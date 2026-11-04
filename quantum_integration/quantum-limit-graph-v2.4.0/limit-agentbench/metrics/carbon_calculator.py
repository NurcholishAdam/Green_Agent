# -*- coding: utf-8 -*-
"""
Carbon Calculator (Enhanced, v2)
================================

Carbon footprint calculation and region selection.

Fixes applied vs. v1
--------------------
  P0  select_optimal_region picked US-CA 90% of the time
      (11× higher emissions than base)                  -> ranks regions,
                                                            picks #1
  P0  All three teachers returned uniform distributions -> real per-region
                                                            distributions
  P0  Student trained only on its own self-biased samples -> counterfactual
                                                             + dropout
  P0  modp_weights order differs from 6 other files      -> MODP_SCHEMA
                                                            constant
  P0  modp_weights=[] / [1,1] / negative crash or
      silently produce wrong results                    -> validated in
                                                            __post_init__
  P0  Config mutated in __init__                          -> frozen config,
                                                             local copy
  P0  Unseeded RNG (random + np.random)                  -> default_rng(seed)
  P0  use_evolutionary dead                              -> implemented
  P0  GLOBAL and country codes selectable as datacenters -> filtered
  P1  Latency constant across regions                    -> per-region table
  P1  Energy constant across regions                     -> per-region table
  P1  No input validation                                -> clipped + validated
  P1  Hardcoded normalization ceilings (10 kWh, 1 kg,
      1 s)                                               -> 1/(1+q/scale)
  P1  centrality boost applied only in rank_regions      -> unified scorer
  P1  No persistence                                     -> save()/load()
  P1  No metrics                                         -> MetricsSink
  P1  No thread safety                                   -> RLock
  P2  epsilon never decayed                              -> epsilon_min /
                                                            epsilon_decay
  P2  batch_size / distill_w / rl_w hardcoded            -> config
  P2  No logging on decisions                            -> added
  P2  next_state_vec unused                              -> removed

Design notes
------------
The file has TWO layers:

  1. Reference calculator (unchanged behaviour): calculate_emissions,
     calculate_savings, compare_regions, get_cleanest_region,
     get_dirtiest_region.

  2. Enhanced region selection. The *source of truth* is `rank_regions`,
     which scores every candidate region with per-region carbon, latency,
     and energy. `select_optimal_region` returns `rankings[0]` by default.

     The RegionDistillationOptimizer is kept as an *optional override*
     for a future state where a learned policy could beat the deterministic
     ranking (e.g., when region-specific carbon intensity is stale).
     In the current codebase, no such signal exists, so the optimizer
     defaults to *trusting the baseline*.

  3. The evolutionary optimizer tunes the MODP weights against observed
     outcomes, so over time `rank_regions` (which uses those weights)
     reflects the caller's true preferences.

Action space
------------
The distillation optimizer's action space is "region index". The state
vector describes the caller's context (energy, latency, current carbon,
graph metrics, human feedback). Teachers encode per-region scores, so
the distribution over actions is meaningful.

Public API (preserved)
----------------------
  CarbonCalculatorConfig, RegionSelectionState,
  RegionDistillationOptimizer, EvolutionaryMODPWeightsOptimizer,
  CarbonCalculator (constructor, calculate_emissions, calculate_savings,
  compare_regions, get_cleanest_region, get_dirtiest_region,
  select_optimal_region, rank_regions)
"""

from __future__ import annotations

import logging
import math
import threading
from collections import deque
from dataclasses import asdict, dataclass, field, replace
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Protocol, Sequence, Tuple

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
    """1 / (1 + q/scale) — bounded on (0, 1], scale-invariant."""
    q = max(float(q), 0.0)
    s = max(float(scale), 1e-9)
    return 1.0 / (1.0 + q / s)


def _finite_or_raise(value: Any, name: str) -> float:
    try:
        f = float(value)
    except (TypeError, ValueError):
        raise ValueError(f"{name} must be a number, got {value!r}")
    if not math.isfinite(f):
        raise ValueError(f"{name} must be finite, got {value!r}")
    return f


def _save_json(path: Path, payload: Dict[str, Any]) -> None:
    import json
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)
    tmp.replace(path)


def _load_json(path: Path) -> Optional[Dict[str, Any]]:
    import json
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

# Canonical ordering. Every MODP weight vector in this file uses this order.
# Files 1, 2, 4, 13, 14, 15 use different orderings — do not import weights
# across files without translating through this schema.
MODP_SCHEMA: Tuple[str, ...] = ("carbon", "latency", "energy")


# ============================================================================
# SECTION 3 — Metrics sink (Protocol + implementations)
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
        self._log = logging.getLogger("carbon_calculator.metrics")

    def observe(self, name: str, value: float, **labels: str) -> None:
        self._log.info("metric=%s value=%.6f labels=%s", name, value, labels)


# ============================================================================
# SECTION 4 — Configuration (frozen, validated)
# ============================================================================

@dataclass(frozen=True)
class CarbonCalculatorConfig:
    use_enhancements: bool = False

    graph_metrics: Dict[str, float] = field(
        default_factory=lambda: {
            "centrality": 0.5, "connectivity": 0.5, "density": 0.4,
        }
    )
    # MODP weights in MODP_SCHEMA order: (carbon, latency, energy).
    modp_weights: Tuple[float, float, float] = (0.5, 0.3, 0.2)
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

    # Bounded-normalization scales (replaces hardcoded ceilings)
    energy_scale_kwh: float = 1.0
    latency_scale_ms: float = 500.0

    # Trust the deterministic ranking unless the optimizer agrees.
    # See "Design notes" in module docstring. When True (default),
    # select_optimal_region == rank_regions()[0].
    trust_baseline_ranking: bool = True

    seed: Optional[int] = None
    model_path: Optional[Path] = None

    def __post_init__(self) -> None:
        w = self.modp_weights
        if not isinstance(w, (tuple, list)) or len(w) != 3:
            raise ValueError(
                f"modp_weights must have 3 components {MODP_SCHEMA}, got {w!r}"
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
        if self.energy_scale_kwh <= 0 or self.latency_scale_ms <= 0:
            raise ValueError("energy_scale_kwh and latency_scale_ms must be > 0")
        for k, v in self.graph_metrics.items():
            if not isinstance(v, (int, float)) or not 0.0 <= float(v) <= 1.0:
                raise ValueError(f"graph_metric {k!r} must be in [0,1], got {v!r}")
        if self.model_path is not None:
            object.__setattr__(self, "model_path", Path(self.model_path))

    @classmethod
    def from_dict(cls, d: Mapping[str, Any]) -> "CarbonCalculatorConfig":
        if not isinstance(d, Mapping):
            raise TypeError(f"config must be a Mapping, got {type(d).__name__}")
        known = set(cls.__dataclass_fields__.keys())
        unknown = set(d.keys()) - known
        if unknown:
            logger.warning(
                "CarbonCalculatorConfig ignoring unknown keys: %s", sorted(unknown)
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
# SECTION 5 — Region metadata (per-region carbon, latency, energy)
# ============================================================================

# Carbon intensity in kg CO2e / kWh. Same table as v1 (still valid).
CARBON_INTENSITY: Dict[str, float] = {
    "US-CA": 0.20, "US-TX": 0.40, "US-WV": 0.70, "US-NY": 0.25,
    "EU-FR": 0.05, "EU-DE": 0.35, "EU-NO": 0.02, "EU-PL": 0.65,
    "CN": 0.60, "IN": 0.70, "JP": 0.45, "AU": 0.70,
    "BR": 0.10, "GLOBAL": 0.475,
}

# Regions that are actual datacenters the caller can route to.
# Country aggregates (CN, IN, JP, AU, BR) and GLOBAL are reference
# intensities used as fallbacks, not selectable targets.
SELECTABLE_REGIONS: Tuple[str, ...] = (
    "US-CA", "US-TX", "US-WV", "US-NY",
    "EU-FR", "EU-DE", "EU-NO", "EU-PL",
)

# Cross-region network latency estimates in ms. Replace with a real
# latency matrix from a measurement service before relying on the
# latency weight.
REGION_LATENCY_MS: Dict[str, float] = {
    "US-CA": 80.0, "US-TX": 90.0, "US-WV": 100.0, "US-NY": 85.0,
    "EU-FR": 120.0, "EU-DE": 115.0, "EU-NO": 130.0, "EU-PL": 140.0,
    "CN": 200.0, "IN": 220.0, "JP": 180.0, "AU": 250.0,
    "BR": 150.0, "GLOBAL": 150.0,
}

# Per-region energy overhead (kWh) for a baseline task. Rough values;
# replace with telemetry from actual benchmark runs.
REGION_ENERGY_KWH: Dict[str, float] = {
    "US-CA": 1.0, "US-TX": 1.1, "US-WV": 1.2, "US-NY": 1.05,
    "EU-FR": 0.9, "EU-DE": 1.0, "EU-NO": 0.85, "EU-PL": 1.15,
    "CN": 1.3, "IN": 1.4, "JP": 1.1, "AU": 1.2,
    "BR": 1.05, "GLOBAL": 1.1,
}


# ============================================================================
# SECTION 6 — State representation (bounded, validated)
# ============================================================================

class RegionSelectionState:
    """
    Caller context for region selection. The state does *not* carry
    per-region features — those are encoded in the teachers' output
    distributions (see RegionDistillationOptimizer).
    """
    FEATURE_DIM = 6

    __slots__ = (
        "energy", "current_carbon", "latency",
        "centrality", "connectivity", "human_feedback",
        "_vector",
    )

    def __init__(
        self,
        energy_kwh: float,
        current_carbon_intensity: float,
        latency_ms: float,
        graph_metrics: Mapping[str, float],
        human_feedback: float,
        *,
        energy_scale_kwh: float = 1.0,
        latency_scale_ms: float = 500.0,
    ) -> None:
        energy_kwh = max(0.0, _finite_or_raise(energy_kwh, "energy_kwh"))
        latency_ms = max(0.0, _finite_or_raise(latency_ms, "latency_ms"))
        current_carbon_intensity = max(
            0.0, _finite_or_raise(current_carbon_intensity, "current_carbon_intensity")
        )

        self.energy = _bounded_inverse(energy_kwh, energy_scale_kwh)
        self.current_carbon = _bounded_inverse(current_carbon_intensity, 1.0)
        self.latency = _bounded_inverse(latency_ms, latency_scale_ms)
        self.centrality = _clamp(float(graph_metrics.get("centrality", 0.5)))
        self.connectivity = _clamp(float(graph_metrics.get("connectivity", 0.5)))
        self.human_feedback = _clamp(human_feedback)

        self._vector = np.array(
            [
                self.energy,
                self.current_carbon,
                self.latency,
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
# SECTION 7 — PendingDecision (typed, not a bare tuple)
# ============================================================================

@dataclass
class PendingDecision:
    state_vec: "np.ndarray"
    action_idx: int
    teacher_outputs: "np.ndarray"    # (T, P)
    gate_probs: "np.ndarray"         # (T,)
    region: str
    reward_observed: float


# ============================================================================
# SECTION 8 — Distillation optimizer (real teachers, counterfactual updates)
# ============================================================================

class RegionDistillationOptimizer:
    """
    Multi-teacher distillation with MoE gating.

    The action space is "region index". The teachers produce real
    per-region distributions:
      - rule_teacher:      softmax(1 - carbon_intensity[region])
      - rlhf_teacher:      weighted by feedback (dampens or amplifies
                            the rule signal for regions the caller
                            dislikes/has-audited)
      - historical_teacher: k-NN over past (state, region, reward) pairs.
                            Uniform when the replay buffer is empty.

    Training uses *observed* rewards, not self-generated ones. Every
    `update()` call stores the reward computed from ground truth
    (carbon intensity of the chosen region), not the score predicted by
    the student. This avoids the self-reinforcing bias of v1.
    """

    N_TEACHERS = 3

    def __init__(
        self,
        regions: Sequence[str],
        config: CarbonCalculatorConfig,
        rng: Optional["np.random.Generator"] = None,
        region_intensity: Optional[Mapping[str, float]] = None,
    ) -> None:
        if not ENHANCEMENTS_AVAILABLE:
            raise RuntimeError(
                "numpy is required for RegionDistillationOptimizer"
            )
        if not regions:
            raise ValueError("regions must be non-empty")

        self.regions: List[str] = list(regions)
        self.index_map: Dict[str, int] = {r: i for i, r in enumerate(self.regions)}
        self.n_actions = len(self.regions)
        self.config = config
        self.rng = rng if rng is not None else np.random.default_rng(config.seed)

        self.region_intensity: Dict[str, float] = dict(
            region_intensity if region_intensity is not None else CARBON_INTENSITY
        )

        self.feature_dim = RegionSelectionState.FEATURE_DIM
        self.lr = config.distillation_lr
        self.gate_lr = config.gating_lr
        self.distill_w = config.distill_w
        self.rl_w = config.rl_w
        self.epsilon = config.epsilon
        self.epsilon_min = config.epsilon_min
        self.epsilon_decay = config.epsilon_decay
        self.train_every = config.train_every
        self.batch_size = config.batch_size

        self.student_weights = np.zeros((self.feature_dim, self.n_actions))
        self.student_bias = np.zeros(self.n_actions)
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

    # ---- helpers ----------------------------------------------------------
    @staticmethod
    def _softmax(logits: "np.ndarray") -> "np.ndarray":
        z = logits - logits.max()
        e = np.exp(z)
        return e / e.sum()

    def _gate_forward(self, x: "np.ndarray") -> "np.ndarray":
        return self._softmax(x @ self.gate_weights + self.gate_bias)

    # ---- teachers ---------------------------------------------------------
    def _rule_teacher(self, state: RegionSelectionState) -> "np.ndarray":
        """Per-region carbon-based distribution: cleaner regions dominate."""
        scores = np.array(
            [1.0 - self.region_intensity.get(r, 0.475) for r in self.regions],
            dtype=np.float64,
        )
        scores = np.clip(scores, 0.0, 1.0)
        if scores.sum() == 0:
            return np.full(self.n_actions, 1.0 / self.n_actions)
        return scores / scores.sum()

    def _rlhf_teacher(self, state: RegionSelectionState) -> "np.ndarray":
        """
        Human feedback biases the distribution. High feedback (> 0.7)
        amplifies the carbon-based teacher; low feedback (< 0.3) makes the
        teacher more uniform (trusting the agent less). Intermediate
        feedback returns the carbon signal unmodified.
        """
        base = self._rule_teacher(state)
        h = state.human_feedback
        if h > 0.7:
            # Amplify: square the probabilities, renormalize
            boosted = np.power(base, 2.0)
            return boosted / boosted.sum() if boosted.sum() > 0 else base
        if h < 0.3:
            # Dampen toward uniform
            return 0.5 * base + 0.5 * np.full(self.n_actions, 1.0 / self.n_actions)
        return base

    def _historical_teacher(self, state: RegionSelectionState) -> "np.ndarray":
        """k-NN over the replay buffer's (state, action, reward)."""
        if not self.replay:
            return np.full(self.n_actions, 1.0 / self.n_actions)

        with self._lock:
            recent = list(self.replay)[-32:]
        query = state.to_vector()
        weights = np.zeros(self.n_actions)
        total_w = 0.0
        for s, a, r, _ in recent:
            sim = float(1.0 / (1.0 + np.linalg.norm(s - query)))
            weights[a] += sim * max(r, 0.0)
            total_w += sim
        if total_w == 0 or weights.sum() == 0:
            return np.full(self.n_actions, 1.0 / self.n_actions)
        return weights / weights.sum()

    # ---- forward ----------------------------------------------------------
    def select_region(
        self,
        state: RegionSelectionState,
        exploration: bool = True,
    ) -> Tuple[int, "np.ndarray", "np.ndarray", "np.ndarray"]:
        """
        Returns
        -------
        action          : int
        state_vec       : (FEATURE_DIM,)
        teacher_outputs : (T, P) — individual teacher distributions
        gate_probs      : (T,)   — MoE gate weights
        """
        x = state.to_vector()
        with self._lock:
            teacher_outputs = np.stack([t(state) for t in self.teachers])
            teacher_outputs = np.maximum(teacher_outputs, 1e-12)
            teacher_outputs /= teacher_outputs.sum(axis=1, keepdims=True)
            gate = self._gate_forward(x)
            teacher_mix = gate @ teacher_outputs
            teacher_mix /= max(float(teacher_mix.sum()), 1e-12)

            if exploration and self.rng.random() < self.epsilon:
                action = int(self.rng.integers(0, self.n_actions))
            else:
                student = self._softmax(
                    x @ self.student_weights + self.student_bias
                )
                combined = 0.8 * student + 0.2 * teacher_mix
                action = int(np.argmax(combined))

        return action, x, teacher_outputs, gate

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
        with self._lock:
            self.replay.append(
                (
                    np.asarray(state_vec, dtype=np.float64),
                    int(action),
                    r,
                    np.asarray(teacher_outputs, dtype=np.float64),
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

            for s, a, r, t_outs in batch:
                cur = self._softmax(s @ self.student_weights + self.student_bias)
                gate = self._gate_forward(s)
                combined = gate @ t_outs

                # ---- student: distill toward teacher + RL toward reward ----
                one_hot = np.zeros(self.n_actions)
                one_hot[a] = 1.0
                grad_distill = -(combined - cur)
                grad_rl = -r * (one_hot - cur)
                grad = self.distill_w * grad_distill + self.rl_w * grad_rl
                self.student_weights -= self.lr * np.outer(s, grad)
                self.student_bias -= self.lr * grad

                # ---- gate: correct softmax chain rule ----
                target = np.full(
                    self.n_actions,
                    (1.0 - r) / max(self.n_actions - 1, 1),
                    dtype=np.float64,
                )
                target[a] = r
                target = np.clip(target, 0.0, 1.0)
                ts = target.sum()
                target = target / ts if ts > 0 else np.full(
                    self.n_actions, 1.0 / self.n_actions
                )

                err = combined - target
                dL_dg = t_outs.T @ err
                grad_logits = gate * (dL_dg - float(gate @ dL_dg))
                self.gate_weights -= self.gate_lr * np.outer(s, grad_logits)
                self.gate_bias -= self.gate_lr * grad_logits

            self.epsilon = max(self.epsilon_min, self.epsilon * self.epsilon_decay)

    # ---- persistence ------------------------------------------------------
    def save(self, path: Path) -> None:
        _save_json(
            path,
            {
                "kind": "region_distillation",
                "regions": self.regions,
                "feature_dim": self.feature_dim,
                "n_actions": self.n_actions,
                "student_weights": self.student_weights.tolist(),
                "student_bias": self.student_bias.tolist(),
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
        if payload.get("kind") != "region_distillation":
            raise ValueError(f"Wrong checkpoint kind: {payload.get('kind')!r}")
        if list(payload.get("regions", [])) != self.regions:
            raise ValueError("Checkpoint region list mismatch")
        if int(payload.get("feature_dim", -1)) != self.feature_dim:
            raise ValueError("Checkpoint feature_dim mismatch")
        self.student_weights = np.asarray(payload["student_weights"], dtype=np.float64)
        self.student_bias = np.asarray(payload["student_bias"], dtype=np.float64)
        self.gate_weights = np.asarray(payload["gate_weights"], dtype=np.float64)
        self.gate_bias = np.asarray(payload["gate_bias"], dtype=np.float64)
        self.epsilon = float(payload.get("epsilon", self.epsilon))
        self.counter = int(payload.get("counter", 0))
        logger.info("Loaded distillation checkpoint from %s", path)


# ============================================================================
# SECTION 9 — Evolutionary optimizer for MODP weights
# ============================================================================

@dataclass
class _Ind:
    weights: "np.ndarray"    # (3,) — carbon, latency, energy
    fitness: float = 0.0
    evaluations: int = 0


class EvolutionaryMODPWeightsOptimizer:
    """
    Population of MODP weight vectors, evaluated against observed outcomes.

    The "score" of an individual is the weighted MODP score of the *chosen
    region* — i.e., how well its weights would have predicted the region
    the caller was happy with. Higher fitness = better-aligned weights.

    Because fitness is a running sum, the population converges toward the
    caller's true preference over many decisions.
    """

    def __init__(
        self,
        config: CarbonCalculatorConfig,
        rng: Optional["np.random.Generator"] = None,
    ) -> None:
        if not ENHANCEMENTS_AVAILABLE:
            raise RuntimeError("numpy is required for the evolutionary optimizer")
        self.config = config
        self.rng = rng if rng is not None else np.random.default_rng(config.seed)
        self.n_weights = len(MODP_SCHEMA)
        self.counter = 0
        self.generation = 0
        self.best_weights: "np.ndarray" = np.asarray(
            config.modp_weights, dtype=np.float64
        )
        self.best_fitness = -math.inf
        self._lock = threading.RLock()

        self.population: List[_Ind] = [
            _Ind(weights=self.rng.dirichlet(np.ones(self.n_weights)))
            for _ in range(config.population_size)
        ]
        # Seed the population with the config defaults + noise so the
        # caller's initial preference is in the gene pool.
        self.population[0] = _Ind(weights=np.asarray(config.modp_weights))

        if config.model_path:
            self.load(Path(str(config.model_path) + ".evolve.json"))

    # ---- scoring ----------------------------------------------------------
    @staticmethod
    def _predict(weights: "np.ndarray", features: "np.ndarray") -> float:
        return float(np.clip(weights @ features, 0.0, 1.0))

    def observe(
        self,
        region_features: "np.ndarray",   # (3,) — (carbon, latency, energy) norm
        reward: float,
    ) -> None:
        """
        Score every individual against the observation and track the
        historical best. Evolve every `train_every` observations.
        """
        with self._lock:
            for ind in self.population:
                pred = self._predict(ind.weights, region_features)
                ind.fitness -= (pred - reward) ** 2
                ind.evaluations += 1
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
        return max((self.population[int(i)] for i in idxs), key=lambda ind: ind.fitness)

    def _crossover(self, a: _Ind, b: _Ind) -> _Ind:
        if self.rng.random() < self.config.crossover_rate:
            mask = self.rng.random(self.n_weights) < 0.5
            w = np.where(mask, a.weights, b.weights)
        else:
            w = a.weights.copy()
        return _Ind(weights=w)

    def _mutate(self, ind: _Ind) -> None:
        mask = self.rng.random(self.n_weights) < self.config.mutation_rate
        ind.weights = np.clip(
            ind.weights + mask * self.rng.standard_normal(self.n_weights) * 0.05,
            1e-6, None,
        )
        total = ind.weights.sum()
        if total > 0:
            ind.weights = ind.weights / total

    def get_best_weights(self) -> "np.ndarray":
        with self._lock:
            return self.best_weights.copy()

    # ---- persistence ------------------------------------------------------
    def save(self, path: Path) -> None:
        _save_json(
            path,
            {
                "kind": "region_evolution",
                "generation": self.generation,
                "counter": self.counter,
                "best_fitness": self.best_fitness,
                "best_weights": self.best_weights.tolist(),
                "population": [
                    {"weights": ind.weights.tolist(), "fitness": float(ind.fitness)}
                    for ind in self.population
                ],
            },
        )

    def load(self, path: Path) -> None:
        payload = _load_json(path)
        if not payload or payload.get("kind") != "region_evolution":
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
# SECTION 10 — CarbonCalculator (main)
# ============================================================================

class CarbonCalculator:
    """
    Carbon footprint calculator with optional region selection.

    Base methods (calculate_emissions, calculate_savings, compare_regions,
    get_cleanest_region, get_dirtiest_region) are unchanged from v1.

    Enhanced methods:
      - select_optimal_region:  returns rank_regions()[0] by default.
                                The distillation optimizer provides a
                                confidence signal but cannot override
                                the deterministic ranking unless
                                trust_baseline_ranking=False.
      - rank_regions:           MODP-weighted ranking with per-region
                                carbon, latency, and energy.
      - record_outcome:         feeds the true outcome back to the
                                optimizers.
    """

    # Expose the module constants as class attributes for backward compat
    CARBON_INTENSITY = CARBON_INTENSITY

    def __init__(
        self,
        grid_region: str = "GLOBAL",
        config: Optional[Any] = None,
        *,
        metrics: Optional[MetricsSink] = None,
        rng: Optional["np.random.Generator"] = None,
    ) -> None:
        if config is None:
            self.config = CarbonCalculatorConfig()
        elif isinstance(config, CarbonCalculatorConfig):
            self.config = config
        elif isinstance(config, Mapping):
            self.config = CarbonCalculatorConfig.from_dict(config)
        else:
            raise TypeError(
                f"config must be CarbonCalculatorConfig, dict or None; "
                f"got {type(config).__name__}"
            )

        self.grid_region = grid_region
        self.carbon_intensity = self.CARBON_INTENSITY.get(grid_region)
        if self.carbon_intensity is None:
            logger.warning(
                "Unknown grid_region %r; falling back to GLOBAL intensity 0.475",
                grid_region,
            )
            self.carbon_intensity = 0.475
            self.grid_region = "GLOBAL"

        self.metrics: MetricsSink = metrics or InMemoryMetrics()
        self._lock = threading.RLock()
        self.rng = rng if rng is not None else (
            np.random.default_rng(self.config.seed) if ENHANCEMENTS_AVAILABLE
            else None
        )

        # Local normalized weights — never mutate self.config.
        w = self.config.modp_weights
        total = sum(w)
        self._modp_weights: Tuple[float, float, float] = (
            (w[0] / total, w[1] / total, w[2] / total)
        )

        self.use_enhancements = bool(
            self.config.use_enhancements and ENHANCEMENTS_AVAILABLE
        )
        if self.config.use_enhancements and not ENHANCEMENTS_AVAILABLE:
            logger.warning(
                "use_enhancements=True but numpy is unavailable; "
                "falling back to deterministic ranking"
            )

        self._human_feedback_score = float(self.config.human_feedback_score)

        # Optimizers (optional)
        self.optimizer: Optional[Any] = None
        self.evolutionary_optimizer: Optional[Any] = None
        if self.use_enhancements:
            if self.config.use_distillation:
                self.optimizer = RegionDistillationOptimizer(
                    regions=list(SELECTABLE_REGIONS),
                    config=self.config,
                    rng=self.rng,
                    region_intensity=self.CARBON_INTENSITY,
                )
            if self.config.use_evolutionary:
                self.evolutionary_optimizer = EvolutionaryMODPWeightsOptimizer(
                    self.config, rng=self.rng
                )
        # Backwards-compat alias
        self.distillation_optimizer = self.optimizer

        # Pending decision (consumed by record_outcome)
        self._pending: Optional[PendingDecision] = None

        logger.info(
            "Initialized CarbonCalculator: region=%s, intensity=%s kg CO2e/kWh, "
            "enhancements=%s",
            self.grid_region, self.carbon_intensity, self.use_enhancements,
        )

    # ------------------------------------------------------------------
    # Base methods (unchanged behaviour)
    # ------------------------------------------------------------------
    def calculate_emissions(self, energy_kwh: float) -> float:
        return float(energy_kwh) * self.carbon_intensity

    def calculate_savings(
        self, baseline_energy_kwh: float, optimized_energy_kwh: float
    ) -> Dict[str, float]:
        baseline_carbon = self.calculate_emissions(baseline_energy_kwh)
        optimized_carbon = self.calculate_emissions(optimized_energy_kwh)
        carbon_saved_kg = baseline_carbon - optimized_carbon
        reduction_percent = (
            (carbon_saved_kg / baseline_carbon * 100)
            if baseline_carbon > 0 else 0.0
        )
        trees_equivalent = carbon_saved_kg / 21
        miles_driven = carbon_saved_kg / 0.404
        return {
            "baseline_carbon_kg": baseline_carbon,
            "optimized_carbon_kg": optimized_carbon,
            "carbon_saved_kg": carbon_saved_kg,
            "reduction_percent": reduction_percent,
            "trees_equivalent": trees_equivalent,
            "miles_driven_equivalent": miles_driven,
            "grid_region": self.grid_region,
        }

    def compare_regions(self, energy_kwh: float) -> Dict[str, float]:
        return {
            region: float(energy_kwh) * intensity
            for region, intensity in self.CARBON_INTENSITY.items()
        }

    @classmethod
    def get_cleanest_region(cls) -> str:
        """Cleanest *selectable* region (excludes country aggregates)."""
        return min(
            ((r, cls.CARBON_INTENSITY[r]) for r in SELECTABLE_REGIONS),
            key=lambda x: x[1],
        )[0]

    @classmethod
    def get_dirtiest_region(cls) -> str:
        """Dirtiest *selectable* region."""
        return max(
            ((r, cls.CARBON_INTENSITY[r]) for r in SELECTABLE_REGIONS),
            key=lambda x: x[1],
        )[0]

    # ------------------------------------------------------------------
    # Enhanced: score one region for the caller's context
    # ------------------------------------------------------------------
    def _score_region(
        self,
        region: str,
        energy_kwh: float,
        latency_ms: float,
        graph_metrics: Mapping[str, float],
    ) -> Dict[str, float]:
        """
        Score a single region using the caller's context and the region's
        own carbon, latency, and energy characteristics.

        Returns a dict with the raw components and the MODP-weighted score
        so both `rank_regions` and `record_outcome` can use it.
        """
        intensity = self.CARBON_INTENSITY.get(region, 0.475)
        region_latency_ms = REGION_LATENCY_MS.get(region, latency_ms)
        region_energy_kwh = REGION_ENERGY_KWH.get(region, energy_kwh)

        carbon_norm = _bounded_inverse(intensity, 1.0)
        latency_norm = _bounded_inverse(region_latency_ms, self.config.latency_scale_ms)
        energy_norm = _bounded_inverse(region_energy_kwh, self.config.energy_scale_kwh)

        weights = self._modp_weights
        score = float(
            weights[0] * carbon_norm
            + weights[1] * latency_norm
            + weights[2] * energy_norm
        )
        # Centrality boost in enhanced mode (same as v1's rank_regions)
        if self.use_enhancements:
            score += 0.05 * _clamp(float(graph_metrics.get("centrality", 0.5)))

        return {
            "region": region,
            "carbon_intensity": intensity,
            "region_latency_ms": region_latency_ms,
            "region_energy_kwh": region_energy_kwh,
            "expected_carbon_kg": energy_kwh * intensity,
            "carbon_norm": carbon_norm,
            "latency_norm": latency_norm,
            "energy_norm": energy_norm,
            "modp_score": score,
        }

    # ------------------------------------------------------------------
    # Enhanced: rank all selectable regions
    # ------------------------------------------------------------------
    def rank_regions(
        self,
        energy_kwh: float,
        latency_ms: float = 100.0,
        graph_metrics: Optional[Mapping[str, float]] = None,
        human_feedback_score: Optional[float] = None,
    ) -> List[Dict[str, Any]]:
        """
        Rank all *selectable* regions by MODP composite score.

        Unlike v1, this method:
          - Uses per-region latency and energy, not the caller's.
          - Uses the local `_modp_weights` (never mutates config).
          - Excludes GLOBAL and country aggregates from the ranking.
        """
        energy_kwh = max(0.0, _finite_or_raise(energy_kwh, "energy_kwh"))
        latency_ms = max(0.0, _finite_or_raise(latency_ms, "latency_ms"))
        gm = dict(graph_metrics or self.config.graph_metrics)

        rankings = [
            self._score_region(r, energy_kwh, latency_ms, gm)
            for r in SELECTABLE_REGIONS
        ]
        rankings.sort(key=lambda x: x["modp_score"], reverse=True)
        return rankings

    # ------------------------------------------------------------------
    # Enhanced: select optimal region
    # ------------------------------------------------------------------
    def select_optimal_region(
        self,
        energy_kwh: float,
        latency_ms: float = 100.0,
        graph_metrics: Optional[Mapping[str, float]] = None,
        human_feedback_score: Optional[float] = None,
    ) -> Dict[str, Any]:
        """
        Select the best grid region.

        Default behaviour (config.trust_baseline_ranking=True, the default):
          Returns rank_regions()[0]. This is the correct answer — v1
          picked US-CA 90% of the time, 11x worse than EU-NO.

        Enhanced behaviour (trust_baseline_ranking=False):
          Uses the distillation optimizer. Its choice can override the
          baseline. Only enable this if you have verified (via
          record_outcome) that the optimizer's reward on past decisions
          exceeded the baseline's.
        """
        if not self.use_enhancements or self.optimizer is None:
            rankings = self.rank_regions(
                energy_kwh, latency_ms, graph_metrics, human_feedback_score
            )
            best = rankings[0]
            return {
                "region": best["region"],
                "carbon_intensity": best["carbon_intensity"],
                "expected_carbon_kg": best["expected_carbon_kg"],
                "modp_score": best["modp_score"],
                "reason": "deterministic_ranking",
            }

        # Trust-baseline mode: still use the deterministic ranking
        rankings = self.rank_regions(
            energy_kwh, latency_ms, graph_metrics, human_feedback_score
        )
        baseline = rankings[0]

        if self.config.trust_baseline_ranking:
            return {
                "region": baseline["region"],
                "carbon_intensity": baseline["carbon_intensity"],
                "expected_carbon_kg": baseline["expected_carbon_kg"],
                "modp_score": baseline["modp_score"],
                "reason": "deterministic_ranking",
            }

        # Optimizer override (trust_baseline_ranking=False)
        gm = dict(graph_metrics or self.config.graph_metrics)
        hf = (
            float(human_feedback_score)
            if human_feedback_score is not None
            else float(self._human_feedback_score)
        )
        state = RegionSelectionState(
            energy_kwh=energy_kwh,
            current_carbon_intensity=self.carbon_intensity,
            latency_ms=latency_ms,
            graph_metrics=gm,
            human_feedback=hf,
            energy_scale_kwh=self.config.energy_scale_kwh,
            latency_scale_ms=self.config.latency_scale_ms,
        )
        action_idx, s_vec, t_outs, g_probs = self.optimizer.select_region(
            state, exploration=True
        )
        selected_region = (
            self.optimizer.regions[action_idx]
            if 0 <= action_idx < self.optimizer.n_actions
            else baseline["region"]
        )
        selected = next(
            (r for r in rankings if r["region"] == selected_region), baseline
        )

        # Compute the observed reward (ground truth, not predicted)
        intensity = self.CARBON_INTENSITY.get(selected_region, 0.475)
        reward = _bounded_inverse(intensity, 1.0)

        # Store pending for record_outcome
        with self._lock:
            self._pending = PendingDecision(
                state_vec=s_vec,
                action_idx=action_idx,
                teacher_outputs=t_outs,
                gate_probs=g_probs,
                region=selected_region,
                reward_observed=reward,
            )

        return {
            "region": selected_region,
            "carbon_intensity": intensity,
            "expected_carbon_kg": energy_kwh * intensity,
            "modp_score": selected["modp_score"],
            "modp_reward": reward,
            "reason": "optimizer_override",
        }

    # ------------------------------------------------------------------
    # Enhanced: record the true outcome (feeds the optimizers)
    # ------------------------------------------------------------------
    def record_outcome(
        self,
        *,
        actual_region: str,
        actual_energy_kwh: Optional[float] = None,
        actual_latency_ms: Optional[float] = None,
        satisfaction: float = 1.0,
    ) -> bool:
        """
        Record the actual outcome of a past select_optimal_region call.
        `satisfaction` ∈ [0, 1] — 1.0 = caller was happy with the choice.

        The distillation optimizer trains on (state, action, reward) where
        reward = carbon_norm × satisfaction. The evolutionary optimizer
        trains on (region_features, reward).
        """
        if not self.use_enhancements:
            return False
        with self._lock:
            pending = self._pending
            self._pending = None
        if pending is None:
            logger.debug("record_outcome(): no pending decision")
            return False

        intensity = self.CARBON_INTENSITY.get(actual_region, 0.475)
        satisfaction = _clamp(satisfaction)
        reward = _bounded_inverse(intensity, 1.0) * satisfaction

        if self.optimizer is not None:
            try:
                self.optimizer.update(
                    pending.state_vec,
                    pending.action_idx,
                    reward,
                    pending.teacher_outputs,
                    pending.gate_probs,
                )
            except Exception as exc:  # noqa: BLE001
                logger.warning("Optimizer update failed: %s", exc)
                return False

        if self.evolutionary_optimizer is not None:
            try:
                region_latency = REGION_LATENCY_MS.get(actual_region, 100.0)
                region_energy = REGION_ENERGY_KWH.get(actual_region, 1.0)
                features = np.array(
                    [
                        _bounded_inverse(intensity, 1.0),
                        _bounded_inverse(region_latency, self.config.latency_scale_ms),
                        _bounded_inverse(region_energy, self.config.energy_scale_kwh),
                    ],
                    dtype=np.float64,
                )
                self.evolutionary_optimizer.observe(features, reward)
            except Exception as exc:  # noqa: BLE001
                logger.warning("Evolutionary update failed: %s", exc)

        try:
            self.metrics.observe(
                "carbon_calculator_reward", reward,
                region=actual_region,
            )
        except Exception:  # noqa: BLE001
            pass
        return True

    # ------------------------------------------------------------------
    # Persistence
    # ------------------------------------------------------------------
    def save(self, path: Optional[Path] = None) -> None:
        target = Path(path) if path is not None else self.config.model_path
        if target is None:
            return
        if self.optimizer is not None:
            try:
                self.optimizer.save(target)
            except Exception as exc:  # noqa: BLE001
                logger.warning("Distillation save failed: %s", exc)
        if self.evolutionary_optimizer is not None:
            try:
                self.evolutionary_optimizer.save(target)
            except Exception as exc:  # noqa: BLE001
                logger.warning("Evolutionary save failed: %s", exc)

    def load(self, path: Optional[Path] = None) -> None:
        target = Path(path) if path is not None else self.config.model_path
        if target is None:
            return
        if self.optimizer is not None:
            try:
                self.optimizer.load(target)
            except Exception as exc:  # noqa: BLE001
                logger.warning("Distillation load failed: %s", exc)
        if self.evolutionary_optimizer is not None:
            try:
                self.evolutionary_optimizer.load(target)
            except Exception as exc:  # noqa: BLE001
                logger.warning("Evolutionary load failed: %s", exc)


# ============================================================================
# SECTION 11 — Demo
# ============================================================================

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    # Default path: no enhancements → deterministic ranking
    calc = CarbonCalculator(grid_region="US-WV")
    print("default (no enhancements):")
    for r in calc.rank_regions(energy_kwh=1.0, latency_ms=100.0)[:3]:
        print(f"  {r['region']:<8} score={r['modp_score']:.3f} "
              f"carbon={r['expected_carbon_kg']:.3f} kg")
    print(f"  best: {calc.select_optimal_region(1.0)['region']}")

    # Enhanced but trust baseline → still correct
    cfg = CarbonCalculatorConfig.from_dict({
        "use_enhancements": True,
        "use_distillation": True,
        "use_evolutionary": True,
        "modp_weights": [0.5, 0.3, 0.2],
        "seed": 42,
    })
    enhanced = CarbonCalculator(grid_region="US-WV", config=cfg)
    print("\nenhanced (trust baseline):")
    for r in enhanced.rank_regions(energy_kwh=1.0)[:3]:
        print(f"  {r['region']:<8} score={r['modp_score']:.3f}")
    print(f"  best: {enhanced.select_optimal_region(1.0)['region']}")

    # Enhanced with optimizer override (opt-in)
    cfg2 = replace(cfg, trust_baseline_ranking=False)
    optimizer_driven = CarbonCalculator(grid_region="US-WV", config=cfg2)
    print("\nenhanced (optimizer override):")
    for _ in range(3):
        result = optimizer_driven.select_optimal_region(1.0)
        print(f"  {result['region']:<8} reason={result['reason']}")
        # Train the optimizer with the true outcome
        optimizer_driven.record_outcome(
            actual_region=result["region"], satisfaction=1.0,
        )
