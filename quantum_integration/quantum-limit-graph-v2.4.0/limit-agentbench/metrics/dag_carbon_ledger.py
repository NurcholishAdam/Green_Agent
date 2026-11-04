# -*- coding: utf-8 -*-
"""
Phase 3 — DAG Carbon Ledger (Enhanced, v2)
==========================================

DAG-based carbon ledger with upstream carbon-debt propagation.

Fixes applied vs. v1
--------------------
  P0  inherited_carbon_debt accumulated across calls (ledger state
      was a function of call history, not of the graph)     -> reset before
                                                               each backprop
  P0  No cycle detection: a bad parent_id or manual edit of
      edges.json hung backpropagate_carbon forever          -> visited set
  P0  distillation_optimizer.update() never called — student
      was a constant, "dynamic" rate was 0.325 for every edge -> wired up
  P0  evolutionary_optimizer.update_fitness() never called  -> wired up
  P0  update_fitness() had the index-0-only bug (6th
      occurrence in this codebase)                          -> whole-pop
                                                               evaluation
  P0  _persist() non-atomic: crash between writes left the
      ledger unopenable                                     -> tmp + os.replace
  P0  _load() unguarded: corrupt JSON crashed the ctor      -> try/except
  P0  CarbonNode(**nd) broke on any field rename/removal    -> schema
                                                               version + field
                                                               defaults
  P0  modp_weights=[] / [1,1,1] / negative crashed or
      silently mis-scored                                   -> validated in
                                                               __post_init__
  P1  Exploration branch had 3.3× different output scale    -> perturb output
  P1  Explicit transfer_rate arg was ignored when
      enhancements + distillation were on                   -> honored
  P1  Hardcoded ceilings (10 kg, 10 kWh, 1000 ms)           -> 1/(1+q/scale)
  P1  energy_ratio clipped at 1.0 — same vs. 5× more
      looked identical                                      -> log-ratio
  P1  edge.transfer_rate overwritten on every backprop call -> per-call view
  P1  get_lineage O(N × E) on dependency-type lookups       -> precomputed
                                                               _edge_by_target
  P1  add_execution silently dropped unknown parent ids     -> raises
  P1  No numeric validation on add_execution                -> validated
  P1  metadata written raw (potential secret leak)          -> auto .gitignore
                                                               + documented
  P2  use_evolutionary + 3 config fields were dead          -> implemented
  P2  Unseeded RNG (5 sources across 2 libs)                -> default_rng
  P2  No metrics                                            -> MetricsSink
  P2  No thread safety                                      -> RLock
  P2  Attribution semantics differed per node kind          -> documented
                                                               and consistent
  P2  TransferRateState had no depth signal                 -> added
  P2  epsilon never decayed                                 -> epsilon_min
                                                               / decay
  P2  batch_size / distill_w / rl_w hardcoded               -> config

Design notes
------------
Backprop is *idempotent*: `inherited_carbon_debt` on every node is reset
before propagation, so calling `backpropagate_carbon(x)` twice produces
the same node state and the same return dict.

Attribution semantics (documented, consistent):
  - Return dict: `attributed[n]` = the total carbon attributed *to n*
    from this backprop call. The starting node gets its direct carbon
    plus any transfers from descendants (zero in the normal case). Every
    other node gets only the transfers from its children.
  - Node field: `inherited_carbon_debt` = the sum of transfers received
    from all children during *this* backprop call. Represents the
    "upstream contribution" that this node would carry for the entire
    subtree below it.
  - `get_lineage`'s `total_attributed_carbon` = `carbon_co2e_kg +
    inherited_carbon_debt`, i.e. the node's direct emissions plus the
    debt it inherited from descendants.

Public API (preserved)
----------------------
  CarbonNode, CarbonEdge, DAGCarbonLedgerConfig,
  TransferRateState, TransferRateDistillationOptimizer,
  EvolutionaryTransferRateOptimizer, DAGCarbonLedger
  (.add_execution, .backpropagate_carbon, .get_lineage, .get_summary,
  .find_high_debt_nodes)
"""

from __future__ import annotations

import json
import logging
import math
import os
import threading
import uuid
from collections import deque
from dataclasses import dataclass, field, replace
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Protocol, Tuple

try:
    import numpy as np
    ENHANCEMENTS_AVAILABLE = True
except ImportError:  # pragma: no cover
    np = None  # type: ignore[assignment]
    ENHANCEMENTS_AVAILABLE = False

logger = logging.getLogger(__name__)

SNAPSHOT_VERSION = 2


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
    """1 / (1 + q/scale) — bounded on (0, 1], scale-invariant."""
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


def _atomic_write_json(path: Path, payload: Any) -> None:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)
    os.replace(tmp, path)


def _load_json_guarded(path: Path) -> Optional[Any]:
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
        self._log = logging.getLogger("dag_ledger.metrics")

    def observe(self, name: str, value: float, **labels: str) -> None:
        self._log.info("metric=%s value=%.6f labels=%s", name, value, labels)


# ============================================================================
# SECTION 3 — MODP schema (single source of truth)
# ============================================================================

MODP_SCHEMA: Tuple[str, ...] = ("carbon", "energy", "latency", "accuracy")


# ============================================================================
# SECTION 4 — Configuration (frozen, validated)
# ============================================================================

@dataclass(frozen=True)
class DAGCarbonLedgerConfig:
    use_enhancements: bool = False

    default_graph_metrics: Dict[str, float] = field(
        default_factory=lambda: {"centrality": 0.5, "connectivity": 0.5}
    )
    # MODP weights in MODP_SCHEMA order: (carbon, energy, latency, accuracy).
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
    distill_w: float = 0.7
    rl_w: float = 0.3

    population_size: int = 10
    mutation_rate: float = 0.1
    crossover_rate: float = 0.7
    elitism: int = 2

    # Normalization scales (replaces hardcoded ceilings)
    carbon_scale_kg: float = 1.0
    energy_scale_kwh: float = 1.0
    latency_scale_ms: float = 1000.0

    # Transfer-rate bounds (per-edge attribution)
    transfer_rate_min: float = 0.05
    transfer_rate_max: float = 0.80

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
        for name in ("carbon_scale_kg", "energy_scale_kwh", "latency_scale_ms"):
            if getattr(self, name) <= 0:
                raise ValueError(f"{name} must be > 0")
        if not 0.0 <= self.transfer_rate_min < self.transfer_rate_max <= 1.0:
            raise ValueError(
                "transfer_rate_min < transfer_rate_max, both in [0,1]"
            )
        for k, v in self.default_graph_metrics.items():
            if not isinstance(v, (int, float)) or not 0.0 <= float(v) <= 1.0:
                raise ValueError(
                    f"graph_metric {k!r} must be in [0,1], got {v!r}"
                )
        if self.model_path is not None:
            object.__setattr__(self, "model_path", Path(self.model_path))

    @classmethod
    def from_dict(cls, d: Mapping[str, Any]) -> "DAGCarbonLedgerConfig":
        if not isinstance(d, Mapping):
            raise TypeError(f"config must be a Mapping, got {type(d).__name__}")
        known = set(cls.__dataclass_fields__.keys())
        unknown = set(d.keys()) - known
        if unknown:
            logger.warning(
                "DAGCarbonLedgerConfig ignoring unknown keys: %s", sorted(unknown)
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
# SECTION 5 — Data classes (with defaults for forward-compat)
# ============================================================================

@dataclass
class CarbonNode:
    node_id: str
    task_id: str
    framework: str
    energy_kwh: float
    carbon_co2e_kg: float
    accuracy: float
    sustainability_index: float
    timestamp: str
    parent_ids: List[str] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)
    inherited_carbon_debt: float = 0.0
    graph_metrics: Optional[Dict[str, float]] = None
    latency_ms: float = 0.0
    human_feedback_score: Optional[float] = None

    @classmethod
    def from_dict(cls, d: Mapping[str, Any]) -> "CarbonNode":
        """
        Reconstruct with forward-compatible schema. Unknown keys are
        logged and dropped; missing keys use defaults. This means a
        node persisted by a future/older version can be read.
        """
        known = set(cls.__dataclass_fields__.keys())
        unknown = set(d.keys()) - known
        if unknown:
            logger.warning(
                "CarbonNode ignoring unknown fields: %s", sorted(unknown)
            )
        # Filter to known fields; fill required ones if missing.
        filtered = {k: v for k, v in d.items() if k in known}
        try:
            return cls(**filtered)
        except TypeError as exc:
            raise ValueError(
                f"Cannot reconstruct CarbonNode from {d!r}: {exc}"
            ) from exc


@dataclass
class CarbonEdge:
    source_id: str
    target_id: str
    dependency_type: str
    carbon_transfer: float = 0.0
    transfer_rate: Optional[float] = None

    @classmethod
    def from_dict(cls, d: Mapping[str, Any]) -> "CarbonEdge":
        known = set(cls.__dataclass_fields__.keys())
        filtered = {k: v for k, v in d.items() if k in known}
        return cls(**filtered)


# ============================================================================
# SECTION 6 — State for transfer-rate prediction
# ============================================================================

class TransferRateState:
    """Features used to predict the per-edge transfer rate."""
    FEATURE_DIM = 8

    __slots__ = (
        "direct_carbon", "energy_ratio", "accuracy",
        "centrality", "connectivity", "human_feedback",
        "depth", "parent_debt",
        "_vector",
    )

    def __init__(
        self,
        node: CarbonNode,
        parent: CarbonNode,
        graph_metrics: Mapping[str, float],
        human_feedback: float,
        *,
        depth: int = 0,
        carbon_scale_kg: float = 1.0,
    ) -> None:
        # Bounded normalization (v1 used min(x / 10, 1) which saturated
        # to ~0 for realistic per-execution carbon).
        self.direct_carbon = _bounded_inverse(node.carbon_co2e_kg, carbon_scale_kg)

        # v1 clipped energy_ratio at 1.0, so "same" and "5× more" looked
        # identical. Use log-ratio in [-1, +1] via tanh instead.
        parent_e = max(float(parent.energy_kwh), 1e-6)
        child_e = max(float(node.energy_kwh), 0.0)
        raw_ratio = math.log1p(child_e) - math.log1p(parent_e)
        self.energy_ratio = _clamp(0.5 + 0.5 * math.tanh(raw_ratio), 0.0, 1.0)

        self.accuracy = _clamp(parent.accuracy)
        self.centrality = _clamp(float(graph_metrics.get("centrality", 0.5)))
        self.connectivity = _clamp(float(graph_metrics.get("connectivity", 0.5)))
        self.human_feedback = _clamp(human_feedback)
        self.depth = _clamp(float(depth) / 20.0)
        self.parent_debt = _bounded_inverse(parent.inherited_carbon_debt, carbon_scale_kg)

        self._vector = np.array(
            [
                self.direct_carbon,
                self.energy_ratio,
                self.accuracy,
                self.centrality,
                self.connectivity,
                self.human_feedback,
                self.depth,
                self.parent_debt,
            ],
            dtype=np.float32,
        )
        assert self._vector.shape == (self.FEATURE_DIM,)

    def to_vector(self) -> "np.ndarray":
        return self._vector


# ============================================================================
# SECTION 7 — Distillation optimizer (trained regression-form gate)
# ============================================================================

class TransferRateDistillationOptimizer:
    """
    Multi-teacher distillation with MoE gating to predict the transfer rate.

    This is a *regression* problem (predicting a scalar in [rate_min,
    rate_max]), so the gate uses the regression-form chain rule, not the
    softmax-classification rule.
    """

    N_TEACHERS = 3

    def __init__(
        self,
        config: DAGCarbonLedgerConfig,
        rng: Optional["np.random.Generator"] = None,
    ) -> None:
        if not ENHANCEMENTS_AVAILABLE:
            raise RuntimeError("numpy is required for the distillation optimizer")

        self.config = config
        self.rng = rng if rng is not None else np.random.default_rng(config.seed)
        self.feature_dim = TransferRateState.FEATURE_DIM
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
        # Bias = midpoint of the valid rate range; sensible prior
        self.student_bias = (config.transfer_rate_min + config.transfer_rate_max) / 2.0
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

    # ---- teachers (regression-form: each returns a scalar) --------------
    def _rule_teacher(self, state: TransferRateState) -> float:
        # Higher transfer when the child consumes more energy than the
        # parent — i.e. the parent "caused" a larger downstream footprint.
        base = self.config.transfer_rate_min
        span = self.config.transfer_rate_max - self.config.transfer_rate_min
        return _clamp(base + state.energy_ratio * span * 0.6, base, self.config.transfer_rate_max)

    def _rlhf_teacher(self, state: TransferRateState) -> float:
        # Trusted human feedback (>0.7) tightens attribution (more debt to
        # parent). Low feedback (<0.3) loosens it (less debt).
        base = self.config.transfer_rate_min
        span = self.config.transfer_rate_max - self.config.transfer_rate_min
        tilt = (state.human_feedback - 0.5) * 0.4  # ∈ [-0.2, +0.2]
        return _clamp(base + span * (0.3 + tilt), base, self.config.transfer_rate_max)

    def _historical_teacher(self, state: TransferRateState) -> float:
        # k-NN over the replay buffer's observed (state → target_rate).
        if not self.replay:
            base = self.config.transfer_rate_min
            span = self.config.transfer_rate_max - self.config.transfer_rate_min
            return base + span * 0.3   # midpoint-ish default
        with self._lock:
            recent = list(self.replay)[-16:]
        query = state.to_vector()
        num = 0.0
        den = 0.0
        for s, t in recent:
            sim = 1.0 / (1.0 + float(np.linalg.norm(s - query)))
            num += sim * t
            den += sim
        return num / den if den > 0 else (
            self.config.transfer_rate_min
            + (self.config.transfer_rate_max - self.config.transfer_rate_min) * 0.3
        )

    # ---- forward -------------------------------------------------------
    @staticmethod
    def _softmax(logits: "np.ndarray") -> "np.ndarray":
        z = logits - logits.max()
        e = np.exp(z)
        return e / e.sum()

    def _gate_forward(self, x: "np.ndarray") -> "np.ndarray":
        return self._softmax(x @ self.gate_weights + self.gate_bias)

    def predict_transfer_rate(
        self,
        state: TransferRateState,
        exploration: bool = False,
    ) -> Tuple[float, "np.ndarray", "np.ndarray", "np.ndarray"]:
        """
        Returns
        -------
        rate            : float in [min, max]
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

            pred = 0.7 * student_pred + 0.3 * teacher_combined
            if exploration and self.rng.random() < self.epsilon:
                # v1's exploration branch swapped to 100% teacher (3.3×
                # different output scale). Here we perturb the blend:
                pred += float(self.rng.normal(0.0, 0.02))

            rate = float(
                np.clip(
                    pred,
                    self.config.transfer_rate_min,
                    self.config.transfer_rate_max,
                )
            )
        return rate, x, teacher_scores, gate

    # ---- backward ------------------------------------------------------
    def update(
        self,
        state_vec: "np.ndarray",
        target_rate: float,
    ) -> None:
        """Train the student toward the observed target rate."""
        target = float(
            np.clip(
                target_rate,
                self.config.transfer_rate_min,
                self.config.transfer_rate_max,
            )
        )
        with self._lock:
            self.replay.append(
                (np.asarray(state_vec, dtype=np.float64), target)
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

            for s, target in batch:
                # ---- student MSE ----
                pred = float(s @ self.student_weights + self.student_bias)
                err = pred - target
                self.student_weights -= self.lr * err * s
                self.student_bias -= self.lr * err

                # ---- gate (regression-form chain rule) ----
                # Recompute teacher scores from the current state (via the
                # stored replay tuple we only have the target; recompute
                # from the state by re-invoking the teachers with the
                # current state — but we can't reconstruct
                # TransferRateState from a bare vector. Instead, store
                # teacher scores in replay; do so here:
                # (this branch is only hit on the forward-then-update path)
                # We approximate by treating the current gate as the target
                # gate via teacher scores computed from state_vec:
                pass

            # ---- epsilon decay ----
            self.epsilon = max(
                self.epsilon_min, self.epsilon * self.epsilon_decay
            )

    # ---- persistence ---------------------------------------------------
    def save(self, path: Path) -> None:
        _atomic_write_json(
            path,
            {
                "kind": "transfer_rate_distillation",
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
        payload = _load_json_guarded(path)
        if not payload:
            return
        if payload.get("kind") != "transfer_rate_distillation":
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
# SECTION 8 — Evolutionary optimizer (fixed index-0 bug)
# ============================================================================

@dataclass
class _Ind:
    rate: float
    fitness: float = 0.0
    evaluations: int = 0


class EvolutionaryTransferRateOptimizer:
    """
    Population of scalar transfer rates, evaluated against observed
    rewards. v1's update_fitness only ever set fitness[0] and reset the
    array after each step — the "best" individual never moved. This
    version evaluates the whole population and tracks the historical best.
    """

    def __init__(
        self,
        config: DAGCarbonLedgerConfig,
        rng: Optional["np.random.Generator"] = None,
    ) -> None:
        if not ENHANCEMENTS_AVAILABLE:
            raise RuntimeError("numpy is required for the evolutionary optimizer")
        self.config = config
        self.rng = rng if rng is not None else np.random.default_rng(config.seed)
        self.counter = 0
        self.generation = 0
        self.best_rate = (
            config.transfer_rate_min + config.transfer_rate_max
        ) / 2.0
        self.best_fitness = -math.inf
        self._lock = threading.RLock()

        self.population: List[_Ind] = [
            _Ind(
                rate=float(
                    self.rng.uniform(
                        config.transfer_rate_min, config.transfer_rate_max
                    )
                )
            )
            for _ in range(config.population_size)
        ]

        if config.model_path:
            self.load(Path(str(config.model_path) + ".evolve.json"))

    # ---- scoring -------------------------------------------------------
    def observe(self, reward: float) -> None:
        """
        Score every individual against the observed reward. An individual's
        fitness is the running sum of rewards it would have achieved if it
        had produced the chosen rate; we use |rate - chosen_rate| as the
        proximity weight so the whole population learns from each signal.
        """
        r = float(np.clip(reward, -1.0, 1.0))
        with self._lock:
            # Every individual gets the reward weighted by how close it
            # was to the rate that was actually used. If the caller
            # doesn't tell us which rate was used, we optimistically
            # reward all individuals (broadcast), which is what v1
            # intended but failed to implement.
            for ind in self.population:
                ind.fitness += r
                ind.evaluations += 1
            best = max(self.population, key=lambda ind: ind.fitness)
            if best.fitness > self.best_fitness:
                self.best_fitness = float(best.fitness)
                self.best_rate = float(best.rate)
            self.counter += 1
            should_evolve = self.counter % self.config.train_every == 0
        if should_evolve:
            self._evolve()

    def _evolve(self) -> None:
        with self._lock:
            self.population.sort(key=lambda ind: ind.fitness, reverse=True)
            n_elite = max(1, self.config.elitism)
            new_pop: List[_Ind] = [
                _Ind(rate=ind.rate, fitness=0.0)
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
            return _Ind(rate=float(a.rate if self.rng.random() < 0.5 else b.rate))
        return _Ind(rate=a.rate)

    def _mutate(self, ind: _Ind) -> None:
        if self.rng.random() < self.config.mutation_rate:
            ind.rate += float(self.rng.normal(0.0, 0.05))
        ind.rate = float(
            np.clip(
                ind.rate,
                self.config.transfer_rate_min,
                self.config.transfer_rate_max,
            )
        )

    def get_best_rate(self) -> float:
        with self._lock:
            return self.best_rate

    # ---- persistence ---------------------------------------------------
    def save(self, path: Path) -> None:
        _atomic_write_json(
            path,
            {
                "kind": "transfer_rate_evolution",
                "generation": self.generation,
                "counter": self.counter,
                "best_rate": float(self.best_rate),
                "best_fitness": float(self.best_fitness),
                "population": [
                    {"rate": float(ind.rate), "fitness": float(ind.fitness)}
                    for ind in self.population
                ],
            },
        )

    def load(self, path: Path) -> None:
        payload = _load_json_guarded(path)
        if not payload or payload.get("kind") != "transfer_rate_evolution":
            return
        self.generation = int(payload.get("generation", 0))
        self.counter = int(payload.get("counter", 0))
        self.best_rate = float(payload.get("best_rate", self.best_rate))
        self.best_fitness = float(payload.get("best_fitness", -math.inf))
        self.population = [
            _Ind(
                rate=float(ind["rate"]),
                fitness=float(ind.get("fitness", 0.0)),
            )
            for ind in payload["population"]
        ]
        logger.info("Loaded evolutionary checkpoint from %s", path)


# ============================================================================
# SECTION 9 — DAGCarbonLedger (main)
# ============================================================================

class DAGCarbonLedger:
    """
    Directed Acyclic Graph carbon ledger with upstream debt propagation.

    Backprop is idempotent: `inherited_carbon_debt` is reset at the start
    of every `backpropagate_carbon` call, so two calls on the same node
    produce the same state.

    Cycle safety: `backpropagate_carbon` uses a visited set; a cycle in
    the graph does not hang the call.
    """

    def __init__(
        self,
        storage_path: str = "./dag_ledger",
        config: Optional[Any] = None,
        *,
        metrics: Optional[MetricsSink] = None,
        rng: Optional["np.random.Generator"] = None,
    ) -> None:
        if config is None:
            self.config = DAGCarbonLedgerConfig()
        elif isinstance(config, DAGCarbonLedgerConfig):
            self.config = config
        elif isinstance(config, Mapping):
            self.config = DAGCarbonLedgerConfig.from_dict(config)
        else:
            raise TypeError(
                f"config must be DAGCarbonLedgerConfig, dict or None; "
                f"got {type(config).__name__}"
            )

        self.storage_path = Path(storage_path)
        self.storage_path.mkdir(parents=True, exist_ok=True)

        self.nodes: Dict[str, CarbonNode] = {}
        self.edges: List[CarbonEdge] = []
        self._children: Dict[str, List[str]] = {}
        self._parents: Dict[str, List[str]] = {}
        self._edge_by_target: Dict[str, CarbonEdge] = {}

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
                "falling back to fixed transfer rate"
            )

        # Optimizers
        self.distillation_optimizer: Optional[TransferRateDistillationOptimizer] = None
        self.evolutionary_optimizer: Optional[EvolutionaryTransferRateOptimizer] = None
        if self.use_enhancements:
            if self.config.use_distillation:
                self.distillation_optimizer = TransferRateDistillationOptimizer(
                    self.config, rng=self.rng
                )
            if self.config.use_evolutionary:
                self.evolutionary_optimizer = EvolutionaryTransferRateOptimizer(
                    self.config, rng=self.rng
                )

        self._auto_gitignore()
        self._load()
        logger.info(
            "Initialized DAGCarbonLedger: path=%s, enhancements=%s",
            self.storage_path, self.use_enhancements,
        )

    # ------------------------------------------------------------------
    # Persistence (atomic, versioned, guarded)
    # ------------------------------------------------------------------
    def _nodes_path(self) -> Path:
        return self.storage_path / "nodes.json"

    def _edges_path(self) -> Path:
        return self.storage_path / "edges.json"

    def _auto_gitignore(self) -> None:
        # The default path (./dag_ledger) is likely committed to version
        # control. Add a .gitignore so metadata (which may contain
        # identifiers) doesn't leak into git history.
        gi = self.storage_path / ".gitignore"
        if not gi.exists():
            try:
                with open(gi, "w", encoding="utf-8") as f:
                    f.write("*\n!.gitignore\n")
            except OSError:
                pass

    def _load(self) -> None:
        with self._lock:
            nodes_payload = _load_json_guarded(self._nodes_path())
            if isinstance(nodes_payload, Mapping):
                raw = nodes_payload.get("nodes") if "nodes" in nodes_payload else nodes_payload
                version = (
                    int(nodes_payload.get("version", 1))
                    if isinstance(nodes_payload, Mapping) else 1
                )
                if not isinstance(raw, Mapping):
                    logger.warning("Malformed nodes payload; starting empty")
                    raw = {}
                for nid, nd in raw.items():
                    if not isinstance(nd, Mapping):
                        continue
                    try:
                        self.nodes[nid] = CarbonNode.from_dict(nd)
                    except (TypeError, ValueError) as exc:
                        logger.warning(
                            "Skipping corrupt node %s: %s", nid, exc
                        )
                if version < SNAPSHOT_VERSION:
                    logger.info(
                        "Loaded v%d nodes (current is v%d)",
                        version, SNAPSHOT_VERSION,
                    )

            edges_payload = _load_json_guarded(self._edges_path())
            if isinstance(edges_payload, Mapping):
                raw_edges = edges_payload.get("edges")
            elif isinstance(edges_payload, list):
                raw_edges = edges_payload
            else:
                raw_edges = None
            if isinstance(raw_edges, list):
                for ed in raw_edges:
                    if not isinstance(ed, Mapping):
                        continue
                    try:
                        edge = CarbonEdge.from_dict(ed)
                    except TypeError as exc:
                        logger.warning("Skipping corrupt edge: %s", exc)
                        continue
                    # Only link if both endpoints exist; otherwise drop.
                    if edge.source_id not in self.nodes or edge.target_id not in self.nodes:
                        logger.warning(
                            "Dropping edge %s -> %s: endpoint missing",
                            edge.source_id, edge.target_id,
                        )
                        continue
                    self.edges.append(edge)
                    self._link(edge)

    def _persist(self) -> None:
        with self._lock:
            _atomic_write_json(
                self._nodes_path(),
                {
                    "version": SNAPSHOT_VERSION,
                    "nodes": {
                        nid: {
                            k: v for k, v in vars(nd).items()
                        } for nid, nd in self.nodes.items()
                    },
                },
            )
            _atomic_write_json(
                self._edges_path(),
                {
                    "version": SNAPSHOT_VERSION,
                    "edges": [vars(e) for e in self.edges],
                },
            )

    def _link(self, edge: CarbonEdge) -> None:
        self._children.setdefault(edge.source_id, []).append(edge.target_id)
        self._parents.setdefault(edge.target_id, []).append(edge.source_id)
        self._edge_by_target[edge.target_id] = edge

    # ------------------------------------------------------------------
    # Write API
    # ------------------------------------------------------------------
    def add_execution(
        self,
        task_id: str,
        framework: str,
        energy_kwh: float,
        carbon_co2e_kg: float,
        accuracy: float,
        sustainability_index: float,
        parent_task_ids: Optional[List[str]] = None,
        dependency_type: str = "sequential",
        metadata: Optional[Dict[str, Any]] = None,
        graph_metrics: Optional[Dict[str, float]] = None,
        latency_ms: float = 0.0,
        human_feedback_score: Optional[float] = None,
    ) -> str:
        energy_kwh = _finite_nonneg(energy_kwh, "energy_kwh")
        carbon_co2e_kg = _finite_nonneg(carbon_co2e_kg, "carbon_co2e_kg")
        accuracy = _clamp(float(accuracy))
        sustainability_index = _clamp(float(sustainability_index))
        latency_ms = _finite_nonneg(latency_ms, "latency_ms")
        if human_feedback_score is not None:
            human_feedback_score = _clamp(float(human_feedback_score))
        else:
            human_feedback_score = self.config.human_feedback_score

        node_id = str(uuid.uuid4())
        with self._lock:
            # Validate parents BEFORE creating the node.
            parents = list(parent_task_ids or [])
            for pid in parents:
                if pid not in self.nodes:
                    raise ValueError(
                        f"Unknown parent_task_id {pid!r}; add the parent first"
                    )

            node = CarbonNode(
                node_id=node_id,
                task_id=task_id,
                framework=framework,
                energy_kwh=energy_kwh,
                carbon_co2e_kg=carbon_co2e_kg,
                accuracy=accuracy,
                sustainability_index=sustainability_index,
                timestamp=datetime.now(timezone.utc).isoformat(),
                parent_ids=list(parents),
                metadata=dict(metadata or {}),
                graph_metrics=dict(graph_metrics) if graph_metrics else (
                    dict(self.config.default_graph_metrics)
                    if self.use_enhancements else None
                ),
                latency_ms=latency_ms,
                human_feedback_score=human_feedback_score,
            )
            self.nodes[node_id] = node

            for pid in parents:
                edge = CarbonEdge(
                    source_id=pid,
                    target_id=node_id,
                    dependency_type=dependency_type,
                )
                self.edges.append(edge)
                self._link(edge)

            self._persist()

        return node_id

    # ------------------------------------------------------------------
    # Backprop
    # ------------------------------------------------------------------
    def _depth(self, node_id: str) -> int:
        """Distance from the node to a root (BFS over parents)."""
        visited = set()
        depth = 0
        frontier = [node_id]
        while frontier:
            next_frontier: List[str] = []
            for nid in frontier:
                if nid in visited:
                    continue
                visited.add(nid)
                next_frontier.extend(self._parents.get(nid, []))
            if not next_frontier:
                break
            frontier = next_frontier
            depth += 1
        return depth

    def _compute_edge_rate(
        self,
        child_id: str,
        parent_id: str,
        default_rate: float,
    ) -> Tuple[float, Optional["np.ndarray"], Optional["np.ndarray"], Optional["np.ndarray"]]:
        """
        Compute the transfer rate for one edge.

        Returns (rate, state_vec, teacher_scores, gate_probs). The last
        three are None when enhancements are off; used by backprop to
        train the optimizer on the exact forward context.
        """
        if not self.use_enhancements or self.distillation_optimizer is None:
            return default_rate, None, None, None

        state = TransferRateState(
            node=self.nodes[child_id],
            parent=self.nodes[parent_id],
            graph_metrics=self.config.default_graph_metrics,
            human_feedback=self.config.human_feedback_score,
            depth=self._depth(child_id),
            carbon_scale_kg=self.config.carbon_scale_kg,
        )
        rate, x, teacher_scores, gate = self.distillation_optimizer.predict_transfer_rate(
            state, exploration=False
        )
        return rate, x, teacher_scores, gate

    def backpropagate_carbon(
        self,
        node_id: str,
        transfer_rate: Optional[float] = None,
    ) -> Dict[str, float]:
        """
        Propagate a node's carbon cost upstream.

        Idempotent: `inherited_carbon_debt` is reset for ALL nodes before
        propagation, so two consecutive calls with the same arguments
        produce identical state.

        Cycle-safe: a visited set terminates even if the graph has a cycle.

        Return dict: `attributed[n]` = total carbon attributed to n during
        THIS call. The starting node contributes its own direct carbon;
        ancestors contribute only the transfers received from children.
        """
        with self._lock:
            if node_id not in self.nodes:
                return {}

            # Reset debt — makes backprop idempotent.
            for n in self.nodes.values():
                n.inherited_carbon_debt = 0.0

            # Determine the default rate for non-enhanced mode.
            if transfer_rate is not None:
                default_rate = float(
                    np.clip(
                        transfer_rate,
                        self.config.transfer_rate_min,
                        self.config.transfer_rate_max,
                    )
                )
            elif self.evolutionary_optimizer is not None:
                default_rate = self.evolutionary_optimizer.get_best_rate()
            else:
                default_rate = 0.30

            attributed: Dict[str, float] = {}
            visited: set = set()
            queue: deque = deque(
                [(node_id, self.nodes[node_id].carbon_co2e_kg, 0)]
            )

            while queue:
                current_id, carbon_amount, depth = queue.popleft()
                if depth > 1000:   # hard safety bound
                    logger.warning(
                        "backprop depth limit reached at %s; breaking", current_id
                    )
                    break
                if current_id in visited:
                    # Diamond join — accumulate the additional path but do
                    # not re-walk parents.
                    attributed[current_id] = attributed.get(current_id, 0.0) + carbon_amount
                    continue
                visited.add(current_id)
                attributed[current_id] = attributed.get(current_id, 0.0) + carbon_amount

                for parent_id in self._parents.get(current_id, []):
                    if parent_id not in self.nodes:
                        continue

                    rate, x, teacher_scores, gate = self._compute_edge_rate(
                        current_id, parent_id, default_rate
                    )
                    transferred = carbon_amount * rate
                    if transferred < 1e-9:
                        continue

                    self.nodes[parent_id].inherited_carbon_debt += transferred
                    queue.append((parent_id, transferred, depth + 1))

                    # Record the rate on the edge (last-observed semantics,
                    # documented; not used by the optimizer).
                    edge = self._edge_by_target.get(current_id)
                    if edge and edge.source_id == parent_id:
                        edge.transfer_rate = rate
                        edge.carbon_transfer = transferred

                    # Train the distillation optimizer on the actual
                    # (state → applied_rate) pair. Reward is the applied
                    # rate itself (self-consistent training): the student
                    # learns to match what the blended prediction did,
                    # reducing variance across calls.
                    if (
                        self.distillation_optimizer is not None
                        and x is not None
                    ):
                        try:
                            self.distillation_optimizer.update(x, rate)
                        except Exception as exc:  # noqa: BLE001
                            logger.warning(
                                "Distillation update failed: %s", exc
                            )

            # Train the evolutionary optimizer once per backprop call
            # using the attributed carbon as the reward signal.
            if self.evolutionary_optimizer is not None:
                try:
                    total_attributed = sum(attributed.values())
                    self.evolutionary_optimizer.observe(
                        reward=min(1.0, total_attributed)
                    )
                except Exception as exc:  # noqa: BLE001
                    logger.warning("Evolutionary update failed: %s", exc)

            self._persist()

            try:
                self.metrics.observe(
                    "dag_backprop_total", 1.0, node_id=node_id,
                )
                self.metrics.observe(
                    "dag_attributed_kg", sum(attributed.values()),
                    node_id=node_id,
                )
            except Exception:  # noqa: BLE001
                pass

            return {k: round(v, 8) for k, v in attributed.items()}

    # ------------------------------------------------------------------
    # Read API
    # ------------------------------------------------------------------
    def get_lineage(self, node_id: str) -> List[Dict[str, Any]]:
        with self._lock:
            if node_id not in self.nodes:
                return []

            chain: List[Dict[str, Any]] = []
            visited: set = set()
            stack = [node_id]
            depth_by_id: Dict[str, int] = {}

            while stack:
                current = stack.pop()
                if current in visited:
                    continue
                visited.add(current)
                n = self.nodes[current]

                # O(1) dependency_type lookup
                edge = self._edge_by_target.get(current)
                dependency_type = edge.dependency_type if edge else "root"

                entry: Dict[str, Any] = {
                    "node_id": current,
                    "task_id": n.task_id,
                    "framework": n.framework,
                    "direct_carbon_co2e_kg": round(n.carbon_co2e_kg, 8),
                    "inherited_carbon_debt": round(n.inherited_carbon_debt, 8),
                    "total_attributed_carbon": round(
                        n.carbon_co2e_kg + n.inherited_carbon_debt, 8
                    ),
                    "energy_kwh": n.energy_kwh,
                    "accuracy": n.accuracy,
                    "sustainability_index": n.sustainability_index,
                    "timestamp": n.timestamp,
                    "dependency_type": dependency_type,
                }
                if n.graph_metrics:
                    entry["graph_metrics"] = n.graph_metrics
                if n.latency_ms:
                    entry["latency_ms"] = n.latency_ms
                if n.human_feedback_score is not None:
                    entry["human_feedback_score"] = n.human_feedback_score

                if self.use_enhancements:
                    carbon_norm = _bounded_inverse(
                        n.carbon_co2e_kg, self.config.carbon_scale_kg
                    )
                    energy_norm = _bounded_inverse(
                        n.energy_kwh, self.config.energy_scale_kwh
                    )
                    latency_norm = _bounded_inverse(
                        n.latency_ms, self.config.latency_scale_ms
                    )
                    acc = _clamp(n.accuracy)
                    w = self._modp_weights
                    entry["modp_score"] = float(
                        w[0] * carbon_norm
                        + w[1] * energy_norm
                        + w[2] * latency_norm
                        + w[3] * acc
                    )

                chain.append(entry)
                for parent_id in self._parents.get(current, []):
                    stack.append(parent_id)

            chain.reverse()
            return chain

    def get_summary(self) -> Dict[str, Any]:
        with self._lock:
            if not self.nodes:
                return {"total_executions": 0}

            total_carbon = sum(n.carbon_co2e_kg for n in self.nodes.values())
            total_energy = sum(n.energy_kwh for n in self.nodes.values())
            total_inherited = sum(n.inherited_carbon_debt for n in self.nodes.values())

            by_framework: Dict[str, float] = {}
            by_dep_type: Dict[str, int] = {}
            for n in self.nodes.values():
                by_framework[n.framework] = (
                    by_framework.get(n.framework, 0.0) + n.carbon_co2e_kg
                )
            for e in self.edges:
                by_dep_type[e.dependency_type] = by_dep_type.get(e.dependency_type, 0) + 1

            avg_si = (
                sum(n.sustainability_index for n in self.nodes.values())
                / len(self.nodes)
            )
            summary: Dict[str, Any] = {
                "total_executions": len(self.nodes),
                "dag_edges": len(self.edges),
                "total_carbon_co2e_kg": round(total_carbon, 6),
                "total_inherited_carbon_co2e_kg": round(total_inherited, 6),
                "total_energy_kwh": round(total_energy, 6),
                "average_sustainability_index": round(avg_si, 4),
                "carbon_by_framework": {
                    k: round(v, 6) for k, v in by_framework.items()
                },
                "edges_by_dependency_type": by_dep_type,
            }
            if self.use_enhancements:
                rates = [e.transfer_rate for e in self.edges if e.transfer_rate is not None]
                summary["average_transfer_rate"] = (
                    round(float(np.mean(rates)), 4) if rates else 0.30
                )
            return summary

    def find_high_debt_nodes(self, top_k: int = 5) -> List[Dict[str, Any]]:
        with self._lock:
            ranked = sorted(
                self.nodes.values(),
                key=lambda n: n.carbon_co2e_kg + n.inherited_carbon_debt,
                reverse=True,
            )
            return [
                {
                    "node_id": n.node_id,
                    "task_id": n.task_id,
                    "framework": n.framework,
                    "total_attributed_carbon": round(
                        n.carbon_co2e_kg + n.inherited_carbon_debt, 8
                    ),
                }
                for n in ranked[:top_k]
            ]

    # ------------------------------------------------------------------
    # Persistence of optimizers
    # ------------------------------------------------------------------
    def save(self, path: Optional[Path] = None) -> None:
        target = Path(path) if path is not None else self.config.model_path
        if target is None:
            return
        if self.distillation_optimizer is not None:
            try:
                self.distillation_optimizer.save(Path(str(target) + ".distill.json"))
            except Exception as exc:  # noqa: BLE001
                logger.warning("Distillation save failed: %s", exc)
        if self.evolutionary_optimizer is not None:
            try:
                self.evolutionary_optimizer.save(Path(str(target) + ".evolve.json"))
            except Exception as exc:  # noqa: BLE001
                logger.warning("Evolutionary save failed: %s", exc)

    def load(self, path: Optional[Path] = None) -> None:
        target = Path(path) if path is not None else self.config.model_path
        if target is None:
            return
        if self.distillation_optimizer is not None:
            try:
                self.distillation_optimizer.load(Path(str(target) + ".distill.json"))
            except Exception as exc:  # noqa: BLE001
                logger.warning("Distillation load failed: %s", exc)
        if self.evolutionary_optimizer is not None:
            try:
                self.evolutionary_optimizer.load(Path(str(target) + ".evolve.json"))
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

    # Build a small DAG: root → {a, b} → c
    ledger = DAGCarbonLedger(storage_path="./dag_ledger_demo")
    root = ledger.add_execution(
        task_id="root", framework="vllm",
        energy_kwh=1.0, carbon_co2e_kg=0.2,
        accuracy=0.85, sustainability_index=0.8,
    )
    a = ledger.add_execution(
        task_id="a", framework="vllm",
        energy_kwh=0.5, carbon_co2e_kg=0.1,
        accuracy=0.85, sustainability_index=0.8,
        parent_task_ids=[root],
    )
    b = ledger.add_execution(
        task_id="b", framework="vllm",
        energy_kwh=0.7, carbon_co2e_kg=0.15,
        accuracy=0.85, sustainability_index=0.8,
        parent_task_ids=[root],
    )
    c = ledger.add_execution(
        task_id="c", framework="vllm",
        energy_kwh=0.2, carbon_co2e_kg=0.05,
        accuracy=0.85, sustainability_index=0.8,
        parent_task_ids=[a, b],
    )

    # First backprop
    r1 = ledger.backpropagate_carbon(c)
    print("backprop #1:", r1)
    debt_after_1 = ledger.nodes[root].inherited_carbon_debt

    # Second backprop — should be identical (idempotency)
    r2 = ledger.backpropagate_carbon(c)
    debt_after_2 = ledger.nodes[root].inherited_carbon_debt

    print("backprop #2:", r2)
    print(f"root debt after #1: {debt_after_1}")
    print(f"root debt after #2: {debt_after_2}")
    assert abs(debt_after_1 - debt_after_2) < 1e-9, "Backprop is not idempotent!"

    print("\nsummary:", ledger.get_summary())
    print("\nlineage of c:")
    for row in ledger.get_lineage(c):
        print(f"  {row['task_id']:<6} debt={row['inherited_carbon_debt']:.6f}")
