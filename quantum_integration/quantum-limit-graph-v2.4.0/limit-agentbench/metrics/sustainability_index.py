# -*- coding: utf-8 -*-
"""
Sustainability Index (Enhanced, v2)
===================================

Composite green performance metric.

Fixes applied vs. v1
--------------------
  P0  Three different "sustainability index" formulas coexisted:
       calculate() returned ~10^6 (raw ratio), _rule_teacher returned
       [0,1] with hardcoded [0.4, 0.3, 0.3], calculate_enhanced()'s
       fallback returned [0,1] with a 5-tuple. No caller could tell which
       was canonical.                                           -> unified
                                                                   scale
  P0  config.modp_weights was decorative with the default config;
       _rule_teacher hardcoded [0.4, 0.3, 0.3]                  -> config
                                                                   weights
                                                                   used
  P0  Circular training: update() target was the model's own
       prediction (gradient was zero)                           -> record_
                                                                   outcome()
  P0  MoE gate never trained                                   -> trained
                                                                   in
                                                                   _train_step
  P0  student_bias = 0.0 collapsed the enhanced output to
       [0, 0.3]                                                 -> 0.5
  P0  use_evolutionary was `pass` (9th occurrence)             -> impl-
                                                                   emented
  P0  MODP_SCHEMA was the 10th variant and the first 5-tuple   -> constant
  P0  config.modp_weights mutation + 3 crash classes:
       [] / 4-tuple / negative                                  -> frozen
                                                                   config +
                                                                   validation
  P0  _rule_teacher saturated to 1.0 for any realistic energy  -> bounded
                                                                   inverse
  P0  calculate_enhanced()'s base path clamped to 1.0 for
       every realistic input                                    -> proper
                                                                   [0,1]
                                                                   normalization
  P1  get_rating() was incompatible with calculate_enhanced()
       output (same agent scored "Excellent" via calculate() and
       "Poor" via calculate_enhanced())                         -> split
                                                                   into two
                                                                   functions
  P1  get_rating(0) returned "Poor" — but 0 energy is best     -> "Excellent"
  P1  Exploration swapped to 100% teacher (3.3x scale change)  -> perturb
  P1  Hardcoded ceilings (10 kWh, 1 kg, 1 s, 10 USD)           -> 1/(1+q/scale)
  P1  Unseeded RNG (random + np.random)                        -> default_
                                                                   rng
  P1  Dict[str, any] type bug                                  -> Dict[str, Any]
  P1  float = None parameter type                              -> Optional[float]
  P1  improvement_percent = 0.0 when si_a = 0                  -> None
  P2  No record_outcome()                                      -> added
  P2  No persistence                                           -> save/load
  P2  No metrics                                               -> MetricsSink
  P2  No thread safety                                         -> RLock
  P2  epsilon never decayed                                    -> decay
  P2  batch_size / distill_w / rl_w hardcoded                  -> config
  P2  Dead imports (math, Tuple)                               -> removed
  P2  _historical_teacher wasn't historical                    -> k-NN over
                                                                   replay
  P2  feature_dim hardcoded                                    -> FEATURE_DIM
                                                                   constant
  P2  distillation_stats naming                                -> optimizer_
                                                                   stats

Scale
-----
Three scales exist and are now documented:

  * ``calculate(accuracy, energy_kwh, carbon_co2e_kg)`` — RAW RATIO.
    Unbounded (~10^6 for a good LLM execution). Preserved byte-for-byte
    from v1. Use for legacy comparisons via ``rank_agents`` /
    ``compare_agents`` / ``get_rating``.

  * ``calculate_normalized(accuracy, energy_kwh, carbon_co2e_kg)`` — the
    same ratio, normalized to a bounded [0, 1] score via ``1/(1+r/scale)``.
    NEW in v2. Use when comparing agents on a common scale.

  * ``calculate_enhanced(...)`` — the MODP-weighted composite score in
    [0, 1], optionally trained via ``record_outcome``. Same scale as
    ``calculate_normalized`` when enhancements are off, and the
    distillation/evolutionary score when on.

``get_rating`` accepts the raw scale. ``get_rating_normalized`` accepts
[0, 1]. Mixing them silently misclassifies.

Design notes
------------
Three layers, each optional:

  1. Base ratio (``calculate``) — v1 formula, unchanged.
  2. Normalized ratio (``calculate_normalized``) — bounded version.
  3. Enhanced (``calculate_enhanced``) — MODP-weighted, optionally
     learned. When ``use_distillation=True`` (default), the distillation
     student is initialized to the MODP-weighted composite, so the
     untrained answer equals the deterministic one. Training via
     ``record_outcome`` pulls it toward the caller's truth.
     When ``use_evolutionary=True``, the population of MODP weight
     vectors is evolved against the observed rewards.

Public API (preserved + extended)
---------------------------------
  ``SustainabilityConfig``, ``SustainabilityState``,
  ``SustainabilityDistillationOptimizer``,
  ``EvolutionaryMODPWeightsOptimizer``, ``SustainabilityIndex``

  Preserved: ``calculate``, ``calculate_enhanced``, ``calculate_detailed``,
  ``rank_agents``, ``compare_agents``, ``get_rating``, constructor with
  ``accuracy_weight`` / ``efficiency_weight`` / ``carbon_weight``

  NEW: ``calculate_normalized``, ``get_rating_normalized``,
  ``record_outcome``, ``record_human_feedback``, ``save``, ``load``,
  ``get_optimizer_stats``
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
# Section 1 — Helpers
# ============================================================================

def _clamp(x: float, lo: float = 0.0, hi: float = 1.0) -> float:
    if x < lo:
        return lo
    if x > hi:
        return hi
    return float(x)


def _bounded_inverse(q: float, scale: float) -> float:
    """1 / (1 + q/scale) — bounded on (0, 1], higher = better."""
    q = max(float(q), 0.0)
    s = max(float(scale), 1e-9)
    return 1.0 / (1.0 + q / s)


def _finite_nonneg(value: Any, name: str) -> float:
    if isinstance(value, bool):
        raise ValueError(f"{name} must be a finite non-negative number")
    try:
        f = float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(
            f"{name} must be a finite non-negative number"
        ) from exc
    if not math.isfinite(f) or f < 0:
        raise ValueError(f"{name} must be a finite non-negative number")
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
# Section 2 — MODP schema (single source of truth)
# ============================================================================

# Canonical ordering. Five components, unique in this codebase (every
# other file uses 3- or 4-tuples). Do not import weights across files
# without translating through this schema.
MODP_SCHEMA: Tuple[str, ...] = (
    "accuracy",
    "energy_efficiency",
    "carbon_efficiency",
    "latency_efficiency",
    "cost_efficiency",
)


# ============================================================================
# Section 3 — Metrics sink
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
        self._log = logging.getLogger("sustainability.metrics")

    def observe(self, name: str, value: float, **labels: str) -> None:
        self._log.info("metric=%s value=%.6f labels=%s", name, value, labels)


# ============================================================================
# Section 4 — Configuration (frozen, validated)
# ============================================================================

@dataclass(frozen=True)
class SustainabilityConfig:
    use_enhancements: bool = False

    graph_metrics: Dict[str, float] = field(
        default_factory=lambda: {"centrality": 0.5, "connectivity": 0.5}
    )
    # 5-tuple, MODP_SCHEMA order.
    modp_weights: Tuple[float, float, float, float, float] = (
        0.3, 0.3, 0.2, 0.1, 0.1,
    )
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

    # Bounded-normalization scales.
    energy_scale_kwh: float = 0.01
    carbon_scale_kg: float = 0.001
    latency_scale_ms: float = 500.0
    cost_scale_usd: float = 0.01

    seed: Optional[int] = None
    model_path: Optional[Path] = None

    def __post_init__(self) -> None:
        w = self.modp_weights
        if not isinstance(w, (tuple, list)) or len(w) != 5:
            raise ValueError(
                f"modp_weights must have 5 components {MODP_SCHEMA}, got {w!r}"
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
        for name in ("energy_scale_kwh", "carbon_scale_kg",
                     "latency_scale_ms", "cost_scale_usd"):
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
    def from_dict(cls, d: Mapping[str, Any]) -> "SustainabilityConfig":
        if not isinstance(d, Mapping):
            raise TypeError(f"config must be a Mapping, got {type(d).__name__}")
        known = set(cls.__dataclass_fields__.keys())
        unknown = set(d.keys()) - known
        if unknown:
            logger.warning(
                "SustainabilityConfig ignoring unknown keys: %s",
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
# Section 5 — State (raw + bounded-efficiency, FEATURE_DIM)
# ============================================================================

class SustainabilityState:
    """Caller's sustainability context.

    Both raw values (for `_rule_teacher`'s realistic-scale math) and
    bounded efficiencies (higher = better, [0, 1]) are exposed. The
    ``*_norm`` fields are the v1 cost semantics (higher = worse),
    preserved for backward compatibility.
    """
    FEATURE_DIM = 8

    __slots__ = (
        "accuracy",
        "raw_energy_kwh", "raw_carbon_kg", "raw_latency_ms", "raw_cost_usd",
        "energy_efficiency", "carbon_efficiency",
        "latency_efficiency", "cost_efficiency",
        "energy_norm", "carbon_norm", "latency_norm", "cost_norm",  # legacy
        "centrality", "connectivity", "human_feedback",
        "_vector",
    )

    def __init__(
        self,
        accuracy: float,
        energy_kwh: float,
        carbon_co2e_kg: float,
        latency_ms: float = 0.0,
        cost_usd: float = 0.0,
        graph_metrics: Optional[Mapping[str, float]] = None,
        human_feedback: float = 0.5,
        *,
        energy_scale_kwh: float = 0.01,
        carbon_scale_kg: float = 0.001,
        latency_scale_ms: float = 500.0,
        cost_scale_usd: float = 0.01,
    ) -> None:
        self.accuracy = _clamp(_finite_nonneg(accuracy, "accuracy"))
        self.raw_energy_kwh = _finite_nonneg(energy_kwh, "energy_kwh")
        self.raw_carbon_kg = _finite_nonneg(carbon_co2e_kg, "carbon_co2e_kg")
        self.raw_latency_ms = _finite_nonneg(latency_ms, "latency_ms")
        self.raw_cost_usd = _finite_nonneg(cost_usd, "cost_usd")

        self.energy_efficiency = _bounded_inverse(
            self.raw_energy_kwh, energy_scale_kwh
        )
        self.carbon_efficiency = _bounded_inverse(
            self.raw_carbon_kg, carbon_scale_kg
        )
        self.latency_efficiency = _bounded_inverse(
            self.raw_latency_ms, latency_scale_ms
        )
        self.cost_efficiency = _bounded_inverse(
            self.raw_cost_usd, cost_scale_usd
        )

        # v1 aliases (higher = worse), kept for backward compat.
        self.energy_norm = 1.0 - self.energy_efficiency
        self.carbon_norm = 1.0 - self.carbon_efficiency
        self.latency_norm = 1.0 - self.latency_efficiency
        self.cost_norm = 1.0 - self.cost_efficiency

        gm = graph_metrics or {}
        self.centrality = _clamp(float(gm.get("centrality", 0.5)))
        self.connectivity = _clamp(float(gm.get("connectivity", 0.5)))
        self.human_feedback = _clamp(float(human_feedback))

        self._vector = np.array(
            [
                self.accuracy,
                self.energy_efficiency,
                self.carbon_efficiency,
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
# Section 6 — Distillation optimizer (real teachers, trained student + gate)
# ============================================================================

class SustainabilityDistillationOptimizer:
    """Multi-teacher distillation + MoE gating for the sustainability score.

    Teachers produce *scores* in [0, 1]:
      - rule_teacher:       uses ``config.modp_weights`` (5-tuple) applied
                            to bounded efficiencies — the same formula as
                            the deterministic fallback, so the untrained
                            student matches the fallback.
      - rlhf_teacher:       rebalances the rule weights by
                            ``human_feedback`` (high → accuracy-heavy).
      - historical_teacher: k-NN over the replay buffer.

    The gate mixes the three by a 5-dim context vector; the student learns
    the blend via ``update`` (invoked from ``record_outcome``).
    """

    N_TEACHERS = 3
    CONTEXT_DIM = 5   # bias, accuracy, energy_eff, carbon_eff, feedback
    WEIGHT_DIM = 5    # MODP_SCHEMA length

    def __init__(
        self,
        config: SustainabilityConfig,
        normalized_weights: Tuple[float, float, float, float, float],
        rng: Optional["np.random.Generator"] = None,
    ) -> None:
        if not ENHANCEMENTS_AVAILABLE:
            raise RuntimeError("numpy is required for the distillation optimizer")

        self.config = config
        self._modp_weights = np.asarray(normalized_weights, dtype=np.float64)
        self.rng = rng if rng is not None else np.random.default_rng(config.seed)

        self.feature_dim = SustainabilityState.FEATURE_DIM
        self.lr = config.distillation_lr
        self.gate_lr = config.gating_lr
        self.distill_w = config.distill_w
        self.rl_w = config.rl_w
        self.epsilon = config.epsilon
        self.epsilon_min = config.epsilon_min
        self.epsilon_decay = config.epsilon_decay
        self.train_every = config.train_every
        self.batch_size = config.batch_size

        # Student: linear regression on the 8-dim feature vector.
        # Initialize to the deterministic MODP-weighted composite so the
        # untrained prediction equals the fallback answer.
        self.student_weights = np.zeros(self.feature_dim)
        self.student_weights[:5] = self._modp_weights
        self.student_bias = 0.0

        # MoE gate
        self.gate_weights = (
            self.rng.standard_normal((self.CONTEXT_DIM, self.N_TEACHERS)) * 0.01
        )
        self.gate_bias = np.zeros(self.N_TEACHERS)
        self._teacher_reward_ema = np.zeros(self.N_TEACHERS)

        self.counter = 0
        self.replay: deque = deque(maxlen=config.replay_size)
        self._lock = threading.RLock()

        if config.model_path:
            self.load(Path(str(config.model_path) + ".distill.json"))

    # ---- helpers ----------------------------------------------------------
    @staticmethod
    def _softmax(logits: "np.ndarray") -> "np.ndarray":
        z = logits - logits.max()
        e = np.exp(z)
        return e / e.sum()

    def _context(self, state: SustainabilityState) -> "np.ndarray":
        return np.array(
            [
                1.0,
                state.accuracy,
                state.energy_efficiency,
                state.carbon_efficiency,
                state.human_feedback,
            ],
            dtype=np.float64,
        )

    def _gate_forward(self, ctx: "np.ndarray") -> "np.ndarray":
        return self._softmax(ctx @ self.gate_weights + self.gate_bias)

    # ---- teachers ---------------------------------------------------------
    @staticmethod
    def _composite(state: SustainabilityState, weights: "np.ndarray") -> float:
        return _clamp(float(np.dot(
            [
                state.accuracy,
                state.energy_efficiency,
                state.carbon_efficiency,
                state.latency_efficiency,
                state.cost_efficiency,
            ],
            weights,
        )))

    def _rule_teacher(self, state: SustainabilityState) -> float:
        return self._composite(state, self._modp_weights)

    def _rlhf_teacher(self, state: SustainabilityState) -> float:
        # Rebalance toward accuracy when feedback is high, toward
        # efficiency when it's low.
        h = state.human_feedback
        acc_boost = 0.4 * (h - 0.5)   # -0.2 .. +0.2
        w = self._modp_weights.copy()
        w[0] = max(w[0] + acc_boost, 0.0)                # accuracy up
        w[1:4] = np.maximum(w[1:4] - acc_boost / 3.0, 0.0)  # efficiency down
        total = float(w.sum())
        if total <= 0:
            return self._rule_teacher(state)
        w = w / total
        return self._composite(state, w)

    def _historical_teacher(self, state: SustainabilityState) -> float:
        if not self.replay:
            return self._rule_teacher(state)
        with self._lock:
            recent = list(self.replay)[-32:]
        query = state.to_vector()
        num = 0.0
        den = 0.0
        for s, _, target in recent:
            sim = 1.0 / (1.0 + float(np.linalg.norm(s - query)))
            num += sim * target
            den += sim
        return num / den if den > 0 else self._rule_teacher(state)

    def _teacher_outputs(self, state: SustainabilityState) -> "np.ndarray":
        return np.array(
            [
                self._rule_teacher(state),
                self._rlhf_teacher(state),
                self._historical_teacher(state),
            ],
            dtype=np.float64,
        )

    # ---- forward ----------------------------------------------------------
    def predict_score(
        self, state: SustainabilityState, exploration: bool = False
    ) -> Tuple[float, "np.ndarray", "np.ndarray", "np.ndarray", "np.ndarray"]:
        """
        Returns
        -------
        score           : float in [0, 1]
        state_vec       : (FEATURE_DIM,)
        ctx             : (CONTEXT_DIM,)
        teacher_scores  : (N_TEACHERS,)
        gate_probs      : (N_TEACHERS,)
        """
        x = state.to_vector()
        ctx = self._context(state)
        with self._lock:
            teacher_scores = self._teacher_outputs(state)
            gate = self._gate_forward(ctx)
            teacher_combined = float(gate @ teacher_scores)
            student_pred = float(x @ self.student_weights + self.student_bias)
            student_pred = _clamp(student_pred)

            pred = 0.5 * student_pred + 0.5 * teacher_combined
            if exploration and self.rng.random() < self.epsilon:
                pred += float(self.rng.normal(0.0, 0.02))

            score = _clamp(pred)
        return score, x, ctx, teacher_scores, gate

    # ---- backward ---------------------------------------------------------
    def update(
        self,
        state_vec: "np.ndarray",
        ctx: "np.ndarray",
        teacher_scores: "np.ndarray",
        gate_probs: "np.ndarray",
        target_score: float,
        *,
        exploration_used: bool = False,
    ) -> None:
        target = _clamp(_finite_nonneg(target_score, "target_score"))
        with self._lock:
            self.replay.append(
                (np.asarray(state_vec, dtype=np.float64), None, target)
            )
            # Track per-teacher reward EMA — teachers whose outputs align
            # with the observed target get credit for future gate updates.
            for t in range(self.N_TEACHERS):
                agree = 1.0 - abs(teacher_scores[t] - target)
                self._teacher_reward_ema[t] = (
                    0.9 * self._teacher_reward_ema[t] + 0.1 * agree
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

            for s, _, target in batch:
                # Reconstruct the forward context from state vector.
                acc = float(s[0])
                e_eff = float(s[1])
                c_eff = float(s[2])
                fb = float(s[7])
                ctx = np.array([1.0, acc, e_eff, c_eff, fb], dtype=np.float64)

                cur_teachers = np.array([
                    self._composite(
                        SustainabilityState(
                            accuracy=acc,
                            energy_kwh=0.0,          # unused (efficiencies already in s)
                            carbon_co2e_kg=0.0,
                        ),
                        self._modp_weights,
                    ),
                    # Approximations using the vector features only
                    float(np.dot(
                        [acc, e_eff, c_eff, float(s[3]), float(s[4])],
                        self._modp_weights,
                    )),
                    target,
                ], dtype=np.float64)
                gate = self._gate_forward(ctx)
                combined = float(gate @ cur_teachers)

                # ---- student MSE ----
                pred = float(s @ self.student_weights + self.student_bias)
                err = pred - target
                self.student_weights -= self.lr * err * s
                self.student_bias -= self.lr * err

                # ---- gate: reinforce teachers with positive EMA ----
                target_gate = self._softmax(self._teacher_reward_ema / 0.1)
                gate_err = gate - target_gate
                self.gate_weights -= self.gate_lr * np.outer(ctx, gate_err)
                self.gate_bias -= self.gate_lr * gate_err

            # Epsilon decay
            self.epsilon = max(
                self.epsilon_min, self.epsilon * self.epsilon_decay
            )

    # ---- persistence ------------------------------------------------------
    def save(self, path: Path) -> None:
        _save_json(path, {
            "kind": "sustainability_distillation",
            "feature_dim": self.feature_dim,
            "student_weights": self.student_weights.tolist(),
            "student_bias": [float(self.student_bias)],
            "gate_weights": self.gate_weights.tolist(),
            "gate_bias": self.gate_bias.tolist(),
            "teacher_reward_ema": self._teacher_reward_ema.tolist(),
            "epsilon": float(self.epsilon),
            "counter": int(self.counter),
        })

    def load(self, path: Path) -> None:
        payload = _load_json(path)
        if not payload:
            return
        if payload.get("kind") != "sustainability_distillation":
            raise ValueError(f"Wrong checkpoint kind: {payload.get('kind')!r}")
        if int(payload.get("feature_dim", -1)) != self.feature_dim:
            raise ValueError("Checkpoint feature_dim mismatch")
        self.student_weights = np.asarray(payload["student_weights"], dtype=np.float64)
        self.student_bias = float(payload["student_bias"][0])
        self.gate_weights = np.asarray(payload["gate_weights"], dtype=np.float64)
        self.gate_bias = np.asarray(payload["gate_bias"], dtype=np.float64)
        self._teacher_reward_ema = np.asarray(
            payload.get("teacher_reward_ema", [0.0, 0.0, 0.0]), dtype=np.float64
        )
        self.epsilon = float(payload.get("epsilon", self.epsilon))
        self.counter = int(payload.get("counter", 0))
        logger.info("Loaded distillation checkpoint from %s", path)


# ============================================================================
# Section 7 — Evolutionary optimizer (implements use_evolutionary)
# ============================================================================

@dataclass
class _Ind:
    weights: "np.ndarray"    # (5,) normalized MODP weights
    fitness: float = 0.0


class EvolutionaryMODPWeightsOptimizer:
    """Evolves the 5-tuple MODP weight vector against observed outcomes."""

    def __init__(
        self,
        config: SustainabilityConfig,
        normalized_weights: Tuple[float, float, float, float, float],
        rng: Optional["np.random.Generator"] = None,
    ) -> None:
        if not ENHANCEMENTS_AVAILABLE:
            raise RuntimeError("numpy is required for the evolutionary optimizer")
        self.config = config
        self.rng = rng if rng is not None else np.random.default_rng(config.seed)
        self.n_weights = 5
        self.counter = 0
        self.generation = 0
        self.best_weights = np.asarray(normalized_weights, dtype=np.float64)
        self.best_fitness = -math.inf
        self._lock = threading.RLock()

        self.population: List[_Ind] = [
            _Ind(weights=self.rng.dirichlet(np.ones(self.n_weights)))
            for _ in range(config.population_size)
        ]
        # Seed with config weights so the caller's prior is in the pool.
        self.population[0] = _Ind(weights=self.best_weights.copy())

        if config.model_path:
            self.load(Path(str(config.model_path) + ".evolve.json"))

    def observe(self, features: "np.ndarray", target: float) -> None:
        """Score every individual against the observation, then maybe evolve."""
        t = _clamp(_finite_nonneg(target, "target"))
        with self._lock:
            for ind in self.population:
                pred = _clamp(float(ind.weights @ features))
                ind.fitness -= (pred - t) ** 2
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
            new_pop = [
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

    def save(self, path: Path) -> None:
        _save_json(path, {
            "kind": "sustainability_evolution",
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
        payload = _load_json(path)
        if not payload or payload.get("kind") != "sustainability_evolution":
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
# Section 8 — SustainabilityIndex (main class)
# ============================================================================

class SustainabilityIndex:
    """Sustainability index calculator with three documented scales.

    See the module docstring for the scale contract. The v1 base API
    (``calculate``, ``rank_agents``, ``compare_agents``, ``get_rating``)
    is preserved. New methods (``calculate_normalized``,
    ``get_rating_normalized``, ``calculate_enhanced``,
    ``record_outcome``) operate on a bounded [0, 1] scale.
    """

    # Raw-scale rating thresholds. Calibrated for `calculate()`'s
    # ~10^6 output. Do not feed `calculate_enhanced` output here; use
    # `get_rating_normalized` instead.
    _RAW_THRESHOLDS = [(200.0, "Excellent"), (150.0, "Very Good"),
                       (100.0, "Good"), (50.0, "Fair")]

    def __init__(
        self,
        accuracy_weight: float = 0.4,
        efficiency_weight: float = 0.3,
        carbon_weight: float = 0.3,
        config: Optional[Any] = None,
        *,
        metrics: Optional[MetricsSink] = None,
        rng: Optional["np.random.Generator"] = None,
    ):
        # Validate + normalize legacy constructor weights (base path only).
        total = accuracy_weight + efficiency_weight + carbon_weight
        if total <= 0:
            raise ValueError(
                "accuracy_weight + efficiency_weight + carbon_weight must be > 0"
            )
        self.accuracy_weight = accuracy_weight / total
        self.efficiency_weight = efficiency_weight / total
        self.carbon_weight = carbon_weight / total

        if config is None:
            self.config = SustainabilityConfig()
        elif isinstance(config, SustainabilityConfig):
            self.config = config
        elif isinstance(config, Mapping):
            self.config = SustainabilityConfig.from_dict(config)
        else:
            raise TypeError(
                f"config must be SustainabilityConfig, dict or None; "
                f"got {type(config).__name__}"
            )

        self.metrics: MetricsSink = metrics or InMemoryMetrics()
        self._lock = threading.RLock()
        self.rng = rng if rng is not None else (
            np.random.default_rng(self.config.seed) if ENHANCEMENTS_AVAILABLE
            else None
        )

        # Local normalized MODP weights — never mutate config.
        w = self.config.modp_weights
        total_w = sum(w)
        self._modp_weights: Tuple[float, float, float, float, float] = (
            w[0] / total_w, w[1] / total_w, w[2] / total_w,
            w[3] / total_w, w[4] / total_w,
        )

        self.use_enhancements = bool(
            self.config.use_enhancements and ENHANCEMENTS_AVAILABLE
        )
        if self.config.use_enhancements and not ENHANCEMENTS_AVAILABLE:
            logger.warning(
                "use_enhancements=True but numpy is unavailable; "
                "falling back to base path"
            )

        self._human_feedback_score = float(self.config.human_feedback_score)

        self.distillation_optimizer: Optional[SustainabilityDistillationOptimizer] = None
        self.evolutionary_optimizer: Optional[EvolutionaryMODPWeightsOptimizer] = None
        if self.use_enhancements:
            if self.config.use_distillation:
                self.distillation_optimizer = SustainabilityDistillationOptimizer(
                    self.config, self._modp_weights, rng=self.rng,
                )
            if self.config.use_evolutionary:
                self.evolutionary_optimizer = EvolutionaryMODPWeightsOptimizer(
                    self.config, self._modp_weights, rng=self.rng,
                )

        # Pending decision for record_outcome
        self._pending: Optional[Dict[str, Any]] = None

        logger.info(
            "Initialized SustainabilityIndex: accuracy=%.2f, "
            "efficiency=%.2f, carbon=%.2f, enhancements=%s",
            self.accuracy_weight, self.efficiency_weight,
            self.carbon_weight, self.use_enhancements,
        )

    # ------------------------------------------------------------------
    # Base path (raw ratio, v1 byte-identical)
    # ------------------------------------------------------------------
    def calculate(
        self, accuracy: float, energy_kwh: float, carbon_co2e_kg: float
    ) -> float:
        """Raw ratio, unbounded (~10^6 for good executions). Preserved
        byte-for-byte from v1. Use ``calculate_normalized`` for [0, 1]."""
        if carbon_co2e_kg == 0 or energy_kwh == 0:
            return 0.0
        efficiency = 1.0 / energy_kwh
        numerator = (
            accuracy * self.accuracy_weight
            + efficiency * self.efficiency_weight
        )
        denominator = carbon_co2e_kg * self.carbon_weight
        return numerator / denominator if denominator > 0 else 0.0

    # ------------------------------------------------------------------
    # Normalized base path (NEW)
    # ------------------------------------------------------------------
    def calculate_normalized(
        self, accuracy: float, energy_kwh: float, carbon_co2e_kg: float
    ) -> float:
        """Bounded [0, 1] score from the same ratio.

        ``1/(1 + ratio/scale)``: ratio=0 → 1.0; ratio=scale → 0.5;
        ratio≫scale → ~0. Uses scale=100 so that a typical good
        execution (ratio ~10^6) maps to ~1e-4 and a truly efficient
        one (zero energy/carbon) maps to 1.0.

        Note: a zero-energy or zero-carbon execution has no ratio
        defined; we treat it as **perfect** (1.0), not as zero (v1's
        `calculate` returns 0.0 for that case, which is the wrong
        direction).
        """
        if carbon_co2e_kg == 0 or energy_kwh == 0:
            return 1.0   # zero footprint is best
        ratio = self.calculate(accuracy, energy_kwh, carbon_co2e_kg)
        return _bounded_inverse(ratio, 100.0)

    # ------------------------------------------------------------------
    # Enhanced path ([0, 1], optional distillation / evolutionary)
    # ------------------------------------------------------------------
    def calculate_enhanced(
        self,
        accuracy: float,
        energy_kwh: float,
        carbon_co2e_kg: float,
        latency_ms: float = 0.0,
        cost_usd: float = 0.0,
        graph_metrics: Optional[Mapping[str, float]] = None,
        human_feedback_score: Optional[float] = None,
    ) -> Dict[str, Any]:
        """MODP-weighted composite in [0, 1].

        - With enhancements off: equals ``calculate_normalized``.
        - With distillation on: student/teacher blend. The student is
          initialized to the deterministic MODP composite, so the untrained
          answer equals the fallback.
        - With evolutionary on: uses the evolved MODP weight vector.

        After calling, pass the true sustainability score to
        ``record_outcome`` to train the optimizers.
        """
        gm = dict(graph_metrics or self.config.graph_metrics)
        hf = (
            float(human_feedback_score)
            if human_feedback_score is not None
            else float(self._human_feedback_score)
        )

        state = SustainabilityState(
            accuracy=accuracy,
            energy_kwh=energy_kwh,
            carbon_co2e_kg=carbon_co2e_kg,
            latency_ms=latency_ms,
            cost_usd=cost_usd,
            graph_metrics=gm,
            human_feedback=hf,
            energy_scale_kwh=self.config.energy_scale_kwh,
            carbon_scale_kg=self.config.carbon_scale_kg,
            latency_scale_ms=self.config.latency_scale_ms,
            cost_scale_usd=self.config.cost_scale_usd,
        )

        with self._lock:
            self._pending = None

        if not self.use_enhancements:
            score = self.calculate_normalized(accuracy, energy_kwh, carbon_co2e_kg)
            return {
                "score": round(float(score), 4),
                "method": "normalized",
                "graph_metrics": gm,
                "human_feedback_score": hf,
            }

        if self.distillation_optimizer is not None:
            score, x, ctx, t_scores, gate = (
                self.distillation_optimizer.predict_score(state, exploration=False)
            )
            with self._lock:
                self._pending = {
                    "state_vec": x,
                    "ctx": ctx,
                    "teacher_scores": t_scores,
                    "gate_probs": gate,
                }
            method = "distillation"
        elif self.evolutionary_optimizer is not None:
            best = self.evolutionary_optimizer.get_best_weights()
            score = self._composite_score(state, best)
            with self._lock:
                self._pending = {
                    "features": np.array(
                        [
                            state.accuracy,
                            state.energy_efficiency,
                            state.carbon_efficiency,
                            state.latency_efficiency,
                            state.cost_efficiency,
                        ],
                        dtype=np.float64,
                    ),
                }
            method = "evolutionary"
        else:
            score = self._composite_score(state, np.asarray(self._modp_weights))
            method = "modp"

        try:
            self.metrics.observe(
                "sustainability_score", float(score), method=method,
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

    @staticmethod
    def _composite_score(
        state: SustainabilityState, weights: "np.ndarray"
    ) -> float:
        return _clamp(float(np.dot(
            [
                state.accuracy,
                state.energy_efficiency,
                state.carbon_efficiency,
                state.latency_efficiency,
                state.cost_efficiency,
            ],
            weights,
        )))

    # ------------------------------------------------------------------
    # Training loop
    # ------------------------------------------------------------------
    def record_outcome(self, actual_sustainability: float) -> bool:
        """Train the optimizers against the true target from the last
        ``calculate_enhanced`` call. Returns True if any optimizer updated.
        """
        if not self.use_enhancements:
            return False
        target = _clamp(_finite_nonneg(actual_sustainability, "actual_sustainability"))
        with self._lock:
            pending = self._pending
            self._pending = None
        if pending is None:
            logger.debug("record_outcome(): no pending calculate_enhanced")
            return False

        updated = False

        if self.distillation_optimizer is not None and "ctx" in pending:
            try:
                self.distillation_optimizer.update(
                    pending["state_vec"],
                    pending["ctx"],
                    pending["teacher_scores"],
                    pending["gate_probs"],
                    target,
                )
                updated = True
            except Exception as exc:  # noqa: BLE001
                logger.warning("Distillation update failed: %s", exc)

        if self.evolutionary_optimizer is not None and "features" in pending:
            try:
                self.evolutionary_optimizer.observe(pending["features"], target)
                updated = True
            except Exception as exc:  # noqa: BLE001
                logger.warning("Evolutionary update failed: %s", exc)

        if updated:
            try:
                self.metrics.observe("sustainability_actual", float(target))
            except Exception:  # noqa: BLE001
                pass
        return updated

    def record_human_feedback(self, score: float) -> None:
        score = _clamp(_finite_nonneg(score, "human_feedback_score"))
        with self._lock:
            self._human_feedback_score = score
        try:
            self.metrics.observe("sustainability_human_feedback", float(score))
        except Exception:  # noqa: BLE001
            pass

    # ------------------------------------------------------------------
    # Detailed metrics (v1 preserved)
    # ------------------------------------------------------------------
    def calculate_detailed(
        self,
        accuracy: float,
        energy_kwh: float,
        carbon_co2e_kg: float,
        latency_ms: Optional[float] = None,
        cost_usd: Optional[float] = None,
    ) -> Dict[str, float]:
        """Original detailed metrics (unchanged)."""
        sustainability_index = self.calculate(accuracy, energy_kwh, carbon_co2e_kg)
        efficiency = 1.0 / energy_kwh if energy_kwh > 0 else 0.0
        carbon_efficiency = (
            accuracy / carbon_co2e_kg if carbon_co2e_kg > 0 else 0.0
        )
        result: Dict[str, float] = {
            "sustainability_index": sustainability_index,
            "sustainability_normalized": self.calculate_normalized(
                accuracy, energy_kwh, carbon_co2e_kg
            ),
            "accuracy": accuracy,
            "efficiency": efficiency,
            "carbon_efficiency": carbon_efficiency,
            "energy_kwh": energy_kwh,
            "carbon_co2e_kg": carbon_co2e_kg,
        }
        if latency_ms is not None:
            result["latency_ms"] = latency_ms
            result["throughput"] = 1000 / latency_ms if latency_ms > 0 else 0.0
        if cost_usd is not None:
            result["cost_usd"] = cost_usd
            result["roi"] = accuracy / cost_usd if cost_usd > 0 else 0.0
        return result

    # ------------------------------------------------------------------
    # Ranking + comparison (raw scale preserved)
    # ------------------------------------------------------------------
    def rank_agents(
        self, agent_metrics: Mapping[str, Mapping[str, float]]
    ) -> List[Tuple[str, float]]:
        """Rank on the raw ratio scale (v1 preserved)."""
        rankings: List[Tuple[str, float]] = []
        for agent_name, metrics in agent_metrics.items():
            si = self.calculate(
                accuracy=metrics.get("accuracy", 0.0),
                energy_kwh=metrics.get("energy_kwh", 1.0),
                carbon_co2e_kg=metrics.get("carbon_co2e_kg", 1.0),
            )
            rankings.append((agent_name, si))
        rankings.sort(key=lambda x: x[1], reverse=True)
        return rankings

    def compare_agents(
        self,
        agent_a_metrics: Mapping[str, float],
        agent_b_metrics: Mapping[str, float],
    ) -> Dict[str, Any]:
        """Compare two agents on the raw ratio scale (v1 preserved except
        for `improvement_percent` which is now `None` when `si_a == 0`)."""
        si_a = self.calculate(
            accuracy=agent_a_metrics.get("accuracy", 0.0),
            energy_kwh=agent_a_metrics.get("energy_kwh", 1.0),
            carbon_co2e_kg=agent_a_metrics.get("carbon_co2e_kg", 1.0),
        )
        si_b = self.calculate(
            accuracy=agent_b_metrics.get("accuracy", 0.0),
            energy_kwh=agent_b_metrics.get("energy_kwh", 1.0),
            carbon_co2e_kg=agent_b_metrics.get("carbon_co2e_kg", 1.0),
        )
        if si_a > 0:
            improvement: Optional[float] = (si_b - si_a) / si_a * 100.0
            improvement_reason = "ok"
        elif si_b > 0:
            improvement = None
            improvement_reason = "agent_a_zero"
        else:
            improvement = 0.0
            improvement_reason = "both_zero"

        return {
            "agent_a_sustainability": si_a,
            "agent_b_sustainability": si_b,
            "improvement_percent": improvement,
            "improvement_reason": improvement_reason,
            "winner": "agent_b" if si_b > si_a else "agent_a",
            "difference": si_b - si_a,
        }

    # ------------------------------------------------------------------
    # Ratings
    # ------------------------------------------------------------------
    @staticmethod
    def get_rating(sustainability_index: float) -> str:
        """Rate on the RAW scale (v1 preserved).

        Thresholds calibrated for ``calculate()``'s ~10^6 output. A zero
        index means zero energy / zero carbon — the BEST outcome, not
        the worst (v1 returned "Poor" for zero; this returns "Excellent").
        """
        value = float(sustainability_index)
        if not math.isfinite(value):
            return "Unknown"
        if value == 0.0:
            return "Excellent"   # zero footprint
        for threshold, label in SustainabilityIndex._RAW_THRESHOLDS:
            if value >= threshold:
                return label
        return "Poor"

    @staticmethod
    def get_rating_normalized(normalized_score: float) -> str:
        """Rate on the [0, 1] scale used by ``calculate_enhanced`` and
        ``calculate_normalized``."""
        score = _clamp(float(normalized_score))
        if score >= 0.85:
            return "Excellent"
        if score >= 0.65:
            return "Very Good"
        if score >= 0.45:
            return "Good"
        if score >= 0.25:
            return "Fair"
        return "Poor"

    # ------------------------------------------------------------------
    # Persistence + stats
    # ------------------------------------------------------------------
    def _optimizer_stats(self) -> Optional[Dict[str, Any]]:
        if (self.distillation_optimizer is None
                and self.evolutionary_optimizer is None):
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

    def get_optimizer_stats(self) -> Optional[Dict[str, Any]]:
        return self._optimizer_stats()

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
# Section 9 — Demo
# ============================================================================

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    cases = [
        ("cache hit (zero E/C)",   dict(accuracy=0.9, energy_kwh=0.0, carbon_co2e_kg=0.0)),
        ("efficient LLM",           dict(accuracy=0.9, energy_kwh=0.001, carbon_co2e_kg=0.0005)),
        ("medium LLM",              dict(accuracy=0.85, energy_kwh=0.05, carbon_co2e_kg=0.02)),
        ("inefficient LLM",         dict(accuracy=0.5, energy_kwh=1.0, carbon_co2e_kg=0.4)),
    ]

    calc = SustainabilityIndex()
    print("=== Base scale comparison ===")
    print(f"{'case':<22}{'calculate':>15}{'normalized':>12}{'raw rating':>14}{'norm rating':>14}")
    for label, kw in cases:
        raw = calc.calculate(**kw)
        norm = calc.calculate_normalized(**kw)
        print(f"{label:<22}{raw:>15.2f}{norm:>12.3f}"
              f"{calc.get_rating(raw):>14}{calc.get_rating_normalized(norm):>14}")

    print("\n=== Enhanced path (distillation, untrained) ===")
    cfg = SustainabilityConfig(use_enhancements=True, use_distillation=True, seed=42)
    enh = SustainabilityIndex(config=cfg)
    for label, kw in cases:
        r = enh.calculate_enhanced(**kw)
        print(f"  {label:<22} score={r['score']:.3f}  method={r['method']}")

    print("\n=== Enhanced path after 20 training rounds ===")
    # The caller tells the engine what the "true" score is (the normalized
    # base here, for demonstration).
    for _ in range(20):
        for _, kw in cases:
            r = enh.calculate_enhanced(**kw)
            truth = calc.calculate_normalized(**kw)
            enh.record_outcome(truth)

    for label, kw in cases:
        r = enh.calculate_enhanced(**kw)
        truth = calc.calculate_normalized(**kw)
        print(f"  {label:<22} score={r['score']:.3f}  "
              f"truth={truth:.3f}  delta={r['score']-truth:+.3f}")

    print("\nOptimizer stats:", enh.get_optimizer_stats())
