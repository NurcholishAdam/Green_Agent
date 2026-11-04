# -*- coding: utf-8 -*-
"""
Phase 4 — Policy Graph Engine (Enhanced, v2)
============================================

Encodes green_policy.yaml rules as a weighted directed graph and performs
multi-hop, context-aware traversal to produce decisions.

Fixes applied vs. v1
--------------------
  P0  DEFAULT_POLICY was undefined (blocker)         -> defined in-class
  P0  DECISION_IDS was a set (order-unstable)        -> frozen tuple
  P0  Non-enhanced return: score always 0.0          -> canonical short names
  P0  Non-enhanced return: winning_path always []    -> fixed prefix
  P0  _last_decision_path never set                  -> set in decide()
  P0  MoE gate gradient wrong shape/sign             -> correct softmax chain-rule
  P0  Forward/backward disconnect (alpha ignored)    -> AlphaEvolution included
  P0  Config mutation + empty-list crash             -> frozen config + __post_init__
  P0  Reward escapes [-1,1], one-hot can be zeros    -> clipped + validated
  P0  _last_enhanced_decision not cleared in finally -> try/finally
  P0  use_evolutionary was dead                      -> AlphaEvolution implemented
  P0  Unseeded RNG                                   -> np.random.default_rng(seed)
  P0  No persistence                                 -> save()/load() everywhere
  P1  feature_dim/n_actions hard-coded               -> derived
  P1  epsilon never decays                           -> epsilon_min/epsilon_decay
  P1  batch_size/distill_w/rl_w hard-coded           -> config
  P1  Feature vector not clipped                     -> _clamp on all inputs
  P1  Edge weight clamp [-1,1] strangles the graph   -> clamped to [min, max]
  P2  No thread safety                               -> RLock
  P2  No metrics sink                                -> MetricsSink protocol
  P2  Dict-only return                               -> PolicyDecision dataclass
  P2  feedback returns None                          -> FeedbackResult dataclass
  P2  _meta_zone_override written externally         -> public setter
  P2  graph_metrics static                           -> computed from edges
  P2  No bounded history/summary                     -> deque + summary()

Preserved public surface
------------------------
  PolicyNode, PolicyEdge, PolicyGraphConfig,
  PolicyDecisionState, PolicyDistillationOptimizer, PolicyGraph,
  PolicyGraph.DECISION_IDS (now tuple), PolicyGraph.DEFAULT_POLICY (now defined),
  .decide(), .feedback(), .update_edge_weight(), .export_weights()
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
        self._log = logging.getLogger("policy_graph.metrics")

    def observe(self, name: str, value: float, **labels: str) -> None:
        self._log.info("metric=%s value=%.6f labels=%s", name, value, labels)


# ============================================================================
# SECTION 3 — Graph primitives
# ============================================================================

@dataclass(frozen=True)
class PolicyNode:
    node_id: str
    variable: str
    condition: str
    is_decision: bool = False
    decision: Optional[str] = None


@dataclass(frozen=True)
class PolicyEdge:
    source_id: str
    target_id: str
    weight: float = 1.0
    context_tag: str = ""


# ============================================================================
# SECTION 4 — Configuration (frozen, validated)
# ============================================================================

@dataclass(frozen=True)
class PolicyGraphConfig:
    use_enhancements: bool = False

    graph_metrics: Dict[str, float] = field(
        default_factory=lambda: {"centrality": 0.5, "connectivity": 0.5}
    )

    # MODP weights: (carbon, latency, energy, quality)
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

    initial_alpha: float = 0.6
    max_dfs_depth: int = 6
    edge_weight_min: float = 0.01
    edge_weight_max: float = 1.0

    # Reward shaping (before RLHF tilt)
    reward_correct: float = 1.0
    reward_wrong: float = -0.5
    rlhf_tilt: float = 0.2

    history_maxlen: int = 1000
    seed: Optional[int] = None
    model_path: Optional[Path] = None

    def __post_init__(self) -> None:
        w = self.modp_weights
        if not isinstance(w, (tuple, list)) or len(w) != 4:
            raise ValueError(
                f"modp_weights must have 4 components (carbon, latency, energy, "
                f"quality), got {w!r}"
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
        if not 0.0 <= self.initial_alpha <= 1.0:
            raise ValueError("initial_alpha must be in [0,1]")
        if self.max_dfs_depth < 1:
            raise ValueError("max_dfs_depth must be >= 1")
        if not 0.0 <= self.edge_weight_min < self.edge_weight_max <= 1.0:
            raise ValueError(
                "edge_weight_min < edge_weight_max and both in [0,1]"
            )
        if self.history_maxlen < 1:
            raise ValueError("history_maxlen must be >= 1")
        for k, v in self.graph_metrics.items():
            if not _is_finite(v) or not 0.0 <= float(v) <= 1.0:
                raise ValueError(
                    f"graph_metric {k!r} must be in [0,1], got {v!r}"
                )
        if self.model_path is not None:
            object.__setattr__(self, "model_path", Path(self.model_path))

    @classmethod
    def from_dict(cls, d: Mapping[str, Any]) -> "PolicyGraphConfig":
        if not isinstance(d, Mapping):
            raise TypeError(f"config must be a Mapping, got {type(d).__name__}")
        known = set(cls.__dataclass_fields__.keys())
        unknown = set(d.keys()) - known
        if unknown:
            logger.warning(
                "PolicyGraphConfig ignoring unknown keys: %s", sorted(unknown)
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
class PendingDecision:
    """Captured state from a single decide() call, consumed by feedback()."""
    state_vec: "np.ndarray"
    teacher_outputs: "np.ndarray"
    gate_probs: "np.ndarray"
    teacher_mix: "np.ndarray"
    action_short: str
    action_idx: int
    alpha_used: float
    raw_scores: Dict[str, float]


@dataclass
class PolicyDecision:
    decision: str
    score: float
    all_scores: Dict[str, float]
    active_nodes: List[str]
    winning_path: List[str]
    winning_tags: List[str]
    reasoning: str
    alpha_used: Optional[float] = None
    enhanced: bool = False

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class FeedbackResult:
    edges_updated: int
    reward: float
    optimizer_updated: bool
    optimizer_counter: Optional[int] = None
    alpha: Optional[float] = None


# ============================================================================
# SECTION 6 — State representation (clipped)
# ============================================================================

class PolicyDecisionState:
    FEATURE_DIM = 8

    __slots__ = (
        "carbon", "battery", "priority", "zone", "queue",
        "centrality", "connectivity", "human_feedback",
        "_vector",
    )

    _ZONE_ENCODING: Dict[str, float] = {
        "green": 0.0, "yellow": 0.5, "red": 1.0,
    }

    def __init__(
        self,
        context: Mapping[str, Any],
        graph_metrics: Mapping[str, float],
        human_feedback: float,
    ) -> None:
        self.carbon = _clamp(float(context.get("carbon_g_per_kwh", 200.0)) / 1000.0)
        self.battery = _clamp(float(context.get("battery_pct", 1.0)))
        self.priority = _clamp(float(context.get("task_priority", 0.5)))
        zone_name = str(context.get("zone", "green"))
        self.zone = self._ZONE_ENCODING.get(zone_name)
        if self.zone is None:
            logger.debug("Unknown zone %r; encoding as green", zone_name)
            self.zone = 0.0
        self.queue = _clamp(float(context.get("queue_depth", 0)) / 100.0)
        self.centrality = _clamp(float(graph_metrics.get("centrality", 0.5)))
        self.connectivity = _clamp(float(graph_metrics.get("connectivity", 0.5)))
        self.human_feedback = _clamp(float(human_feedback))

        self._vector = np.array(
            [
                self.carbon, self.battery, self.priority,
                self.zone, self.queue,
                self.centrality, self.connectivity, self.human_feedback,
            ],
            dtype=np.float32,
        )
        assert self._vector.shape == (self.FEATURE_DIM,)

    def to_vector(self) -> "np.ndarray":
        return self._vector


# ============================================================================
# SECTION 7 — Teachers
# ============================================================================

class PolicyTeacher(Protocol):
    def predict(self, state: PolicyDecisionState) -> "np.ndarray": ...


class RuleTeacher:
    """Basic domain rules from the original policy."""
    def __init__(self, decisions: Sequence[str]) -> None:
        self.decisions = list(decisions)
        self._idx = {d: i for i, d in enumerate(self.decisions)}

    def predict(self, state: PolicyDecisionState) -> "np.ndarray":
        probs = np.full(len(self.decisions), 0.1, dtype=np.float64)
        if state.carbon > 0.4 or state.battery < 0.4 or state.zone > 0.5:
            idx = self._idx.get("defer")
            if idx is not None:
                probs[idx] += 0.5
        elif state.priority > 0.8 or state.zone < 0.2:
            idx = self._idx.get("execute")
            if idx is not None:
                probs[idx] += 0.5
        else:
            idx = self._idx.get("throttle")
            if idx is not None:
                probs[idx] += 0.4
        return probs / probs.sum()


class RLHFTeacher:
    def __init__(self, decisions: Sequence[str], human_feedback: float = 0.5) -> None:
        self.decisions = list(decisions)
        self._idx = {d: i for i, d in enumerate(self.decisions)}
        self.h = _clamp(float(human_feedback))

    def predict(self, state: PolicyDecisionState) -> "np.ndarray":
        probs = np.full(len(self.decisions), 1.0 / len(self.decisions))
        if self.h > 0.7:
            idx = self._idx.get("execute")
            if idx is not None:
                probs[idx] += 0.2
        elif self.h < 0.3:
            idx = self._idx.get("defer")
            if idx is not None:
                probs[idx] += 0.2
        return probs / probs.sum()


class HistoricalTeacher:
    def __init__(self, decisions: Sequence[str]) -> None:
        self.decisions = list(decisions)
        self._idx = {d: i for i, d in enumerate(self.decisions)}

    def predict(self, state: PolicyDecisionState) -> "np.ndarray":
        probs = np.full(len(self.decisions), 0.05, dtype=np.float64)
        if state.queue > 0.7:
            idx = self._idx.get("throttle")
        elif state.centrality > 0.7:
            idx = self._idx.get("execute")
        else:
            idx = self._idx.get("defer")
        if idx is not None:
            probs[idx] = 0.6
        return probs / probs.sum()


# ============================================================================
# SECTION 8 — Alpha evolution (blend weight between raw and distillation)
# ============================================================================

class AlphaEvolution:
    """
    Lightweight 1-D evolutionary search for the raw/distillation blend weight.

    Maintains a population of alphas, accumulates per-individual reward
    weighted by how close the *used* alpha was to each individual, and
    evolves (tournament + uniform crossover + Gaussian mutation + elitism).
    """

    def __init__(
        self,
        config: PolicyGraphConfig,
        rng: Optional["np.random.Generator"] = None,
    ) -> None:
        if not _NUMPY_OK:
            raise RuntimeError("numpy is required for AlphaEvolution")
        self.config = config
        self.rng = rng if rng is not None else np.random.default_rng(config.seed)
        # Initialise around `initial_alpha`, spread over [0,1].
        self.population = np.clip(
            self.rng.normal(config.initial_alpha, 0.1, size=config.population_size),
            0.0, 1.0,
        )
        self.fitness = np.zeros(config.population_size)
        self.best_alpha = float(config.initial_alpha)
        self.best_fitness = 0.0
        self.counter = 0

    def select(self) -> float:
        """Return the current best alpha; small ε-noise for exploration."""
        if self.rng.random() < 0.1:
            return float(self.population[int(self.rng.integers(0, len(self.population)))])
        return self.best_alpha

    def update(self, alpha_used: float, reward: float) -> None:
        """Credit the closest individual with the observed reward."""
        r = float(np.clip(reward, -1.0, 1.0))
        idx = int(np.argmin(np.abs(self.population - alpha_used)))
        self.fitness[idx] += r

        best_idx = int(np.argmax(self.fitness))
        if self.fitness[best_idx] > self.best_fitness:
            self.best_fitness = float(self.fitness[best_idx])
            self.best_alpha = float(self.population[best_idx])

        self.counter += 1
        if self.counter % self.config.train_every == 0:
            self._evolve()

    def _evolve(self) -> None:
        order = np.argsort(-self.fitness)
        n_elite = max(1, self.config.elitism)
        new_pop = [float(self.population[i]) for i in order[:n_elite]]
        while len(new_pop) < len(self.population):
            a = self._tournament()
            b = self._tournament()
            child = self._crossover(a, b)
            child = self._mutate(child)
            new_pop.append(float(np.clip(child, 0.0, 1.0)))
        self.population = np.asarray(new_pop, dtype=np.float64)
        self.fitness = np.zeros(len(self.population))

    def _tournament(self, k: int = 3) -> float:
        size = min(k, len(self.population))
        idxs = self.rng.choice(len(self.population), size=size, replace=False)
        best = max(idxs, key=lambda i: self.fitness[int(i)])
        return float(self.population[int(best)])

    def _crossover(self, a: float, b: float) -> float:
        if self.rng.random() < self.config.crossover_rate:
            return float(a if self.rng.random() < 0.5 else b)
        return a

    def _mutate(self, a: float) -> float:
        if self.rng.random() < self.config.mutation_rate:
            a += float(self.rng.normal(0.0, 0.05))
        return float(np.clip(a, 0.0, 1.0))

    # ---- persistence ------------------------------------------------------
    def save(self, path: Path) -> None:
        _save_json_atomic(
            path,
            {
                "kind": "alpha_evolution",
                "population": self.population.tolist(),
                "fitness": self.fitness.tolist(),
                "best_alpha": self.best_alpha,
                "best_fitness": self.best_fitness,
                "counter": self.counter,
            },
        )

    def load(self, path: Path) -> None:
        payload = _load_json_guarded(path)
        if not payload or payload.get("kind") != "alpha_evolution":
            return
        self.population = np.asarray(payload["population"], dtype=np.float64)
        self.fitness = np.asarray(payload["fitness"], dtype=np.float64)
        self.best_alpha = float(payload["best_alpha"])
        self.best_fitness = float(payload["best_fitness"])
        self.counter = int(payload["counter"])


# ============================================================================
# SECTION 9 — Distillation optimizer (fixed MoE, correct chain rule)
# ============================================================================

class PolicyDistillationOptimizer:
    """
    Multi-teacher distillation + MoE gating for policy decisions.

    Forward returns:
        (final_probs, state_vec, teacher_outputs, gate_probs, teacher_mix, alpha)

    Backward trains:
        - student toward teacher_mix (distill) + action_onehot (RL)
        - gate via the correct softmax chain rule on the reward-weighted target
        - alpha via the shared `AlphaEvolution` tuner
    """

    def __init__(
        self,
        decisions: Sequence[str],
        config: PolicyGraphConfig,
        rng: Optional["np.random.Generator"] = None,
    ) -> None:
        if not _NUMPY_OK:
            raise RuntimeError("numpy is required for PolicyDistillationOptimizer")
        if len(decisions) < 2:
            raise ValueError("At least 2 decisions are required")

        self.decisions: List[str] = list(decisions)
        self.decision_index: Dict[str, int] = {
            d: i for i, d in enumerate(self.decisions)
        }
        self.n_actions = len(self.decisions)
        self.config = config
        self.rng = rng if rng is not None else np.random.default_rng(config.seed)

        self.feature_dim = PolicyDecisionState.FEATURE_DIM
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
            self.rng.standard_normal((self.feature_dim, 3)) * 0.01
        )
        self.gate_bias = np.zeros(3)

        self.counter = 0
        self.replay: deque = deque(maxlen=config.replay_size)

        self.teachers: List[PolicyTeacher] = [
            RuleTeacher(self.decisions),
            RLHFTeacher(self.decisions, config.human_feedback_score),
            HistoricalTeacher(self.decisions),
        ]

        self.alpha_evolution = AlphaEvolution(config, rng=self.rng)

        if config.model_path:
            self.load(config.model_path)

    # ---- forward ----------------------------------------------------------
    @staticmethod
    def _softmax(logits: "np.ndarray") -> "np.ndarray":
        z = logits - logits.max()
        e = np.exp(z)
        return e / e.sum()

    def _gate_forward(self, x: "np.ndarray") -> "np.ndarray":
        return self._softmax(x @ self.gate_weights + self.gate_bias)

    def predict_proba(
        self,
        state: PolicyDecisionState,
        raw_scores: Mapping[str, float],
    ) -> Tuple["np.ndarray", "np.ndarray", "np.ndarray", "np.ndarray", "np.ndarray", float]:
        """
        Returns
        -------
        final_probs      : (P,) blend of raw + distillation
        state_vec        : (D,)
        teacher_outputs  : (T, P) individual teacher distributions
        gate_probs       : (T,)   MoE gate weights
        teacher_mix      : (P,)   gated teacher mixture
        alpha_used       : float  weight on raw_scores in the blend
        """
        x = state.to_vector()

        teacher_outputs = np.stack(
            [np.clip(t.predict(state), 1e-10, 1.0) for t in self.teachers]
        )
        teacher_outputs /= teacher_outputs.sum(axis=1, keepdims=True)
        gate_probs = self._gate_forward(x)
        teacher_mix = gate_probs @ teacher_outputs
        s = teacher_mix.sum()
        teacher_mix = teacher_mix / s if s > 0 else np.full(self.n_actions, 1.0 / self.n_actions)

        student_probs = self._softmax(x @ self.student_weights + self.student_bias)

        raw_arr = np.array(
            [raw_scores.get(d, 0.0) for d in self.decisions], dtype=np.float64
        )
        if raw_arr.sum() <= 0:
            raw_arr = np.ones(self.n_actions) / self.n_actions
        else:
            raw_arr = raw_arr / raw_arr.sum()

        alpha = self.alpha_evolution.select()
        distillation = 0.5 * teacher_mix + 0.5 * student_probs
        final = alpha * raw_arr + (1.0 - alpha) * distillation
        s = final.sum()
        final = final / s if s > 0 else np.full(self.n_actions, 1.0 / self.n_actions)

        return final, x, teacher_outputs, gate_probs, teacher_mix, float(alpha)

    # ---- backward ---------------------------------------------------------
    def update(
        self,
        state_vec: "np.ndarray",
        teacher_outputs: "np.ndarray",
        gate_probs: "np.ndarray",
        action_onehot: "np.ndarray",
        reward: float,
        alpha_used: float,
    ) -> None:
        r = float(np.clip(reward, -1.0, 1.0))
        self.replay.append(
            (
                state_vec.astype(np.float64),
                teacher_outputs.astype(np.float64),
                gate_probs.astype(np.float64),
                action_onehot.astype(np.float64),
                r,
                float(alpha_used),
            )
        )
        self.counter += 1
        # Alpha learns from the immediate reward (independent of replay).
        self.alpha_evolution.update(alpha_used, r)

        if (
            self.counter % self.train_every == 0
            and len(self.replay) >= self.batch_size
        ):
            self._train_step()

    def _train_step(self) -> None:
        n = min(self.batch_size, len(self.replay))
        idxs = self.rng.choice(len(self.replay), size=n, replace=False)
        batch = [self.replay[i] for i in idxs]

        for s, t_outs, gate, one_hot, r, _alpha in batch:
            cur = self._softmax(s @ self.student_weights + self.student_bias)

            # ---- student: distill + RL toward the observed outcome ----
            teacher_mix = gate @ t_outs
            grad_distill = -(teacher_mix - cur)
            grad_rl = -r * (one_hot - cur)
            grad = self.distill_w * grad_distill + self.rl_w * grad_rl
            self.student_weights -= self.lr * np.outer(s, grad)
            self.student_bias -= self.lr * grad

            # ---- gate: correct softmax chain rule (FIXES the 6th-occurrence bug) ----
            # Target: reward-weighted one-hot (a valid probability vector for r in [-1,1]
            # is obtained by mapping r -> softmax-like interpolation).
            target = np.full(self.n_actions, (1.0 - r) / max(self.n_actions - 1, 1))
            target[int(np.argmax(one_hot))] = r
            target = np.clip(target, 0.0, 1.0)
            tsum = target.sum()
            if tsum > 0:
                target = target / tsum
            else:
                target = np.ones(self.n_actions) / self.n_actions

            combined = gate @ t_outs                          # (P,)
            err = combined - target                           # (P,)
            # dL/d(gate_logits) = gate * (teacher_outputs @ err - gate · (teacher_outputs @ err))
            dL_dg = t_outs @ err                              # (T,)
            grad_logits = gate * (dL_dg - float(gate @ dL_dg))  # (T,)
            self.gate_weights -= self.gate_lr * np.outer(s, grad_logits)
            self.gate_bias -= self.gate_lr * grad_logits

        # epsilon decay
        self.epsilon = max(self.epsilon_min, self.epsilon * self.epsilon_decay)

    # ---- persistence ------------------------------------------------------
    def save(self, path: Path) -> None:
        _save_json_atomic(
            path,
            {
                "kind": "policy_distillation",
                "decisions": self.decisions,
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
        # Persist alpha as a sibling file.
        self.alpha_evolution.save(Path(str(path) + ".alpha.json"))

    def load(self, path: Path) -> None:
        payload = _load_json_guarded(path)
        if not payload:
            return
        if payload.get("kind") != "policy_distillation":
            logger.warning("Checkpoint kind mismatch: %s", payload.get("kind"))
            return
        if list(payload.get("decisions", [])) != self.decisions:
            raise ValueError(
                f"Checkpoint decisions {payload.get('decisions')} != "
                f"current {self.decisions}"
            )
        if int(payload.get("feature_dim", -1)) != self.feature_dim:
            raise ValueError("Checkpoint feature_dim mismatch")
        self.student_weights = np.asarray(payload["student_weights"], dtype=np.float64)
        self.student_bias = np.asarray(payload["student_bias"], dtype=np.float64)
        self.gate_weights = np.asarray(payload["gate_weights"], dtype=np.float64)
        self.gate_bias = np.asarray(payload["gate_bias"], dtype=np.float64)
        self.epsilon = float(payload.get("epsilon", self.epsilon))
        self.counter = int(payload.get("counter", self.counter))
        self.alpha_evolution.load(Path(str(path) + ".alpha.json"))
        logger.info("Loaded distillation checkpoint from %s", path)


# ============================================================================
# SECTION 10 — PolicyGraph (main)
# ============================================================================

class PolicyGraph:
    """
    Multi-hop weighted policy graph with optional enhanced decision blending.
    """

    DECISION_IDS: Tuple[str, ...] = (
        "Decision:execute",
        "Decision:defer",
        "Decision:throttle",
    )

    # Default policy graph — the missing piece from v1 (which referenced an
    # undefined self.DEFAULT_POLICY and therefore could not be instantiated).
    DEFAULT_POLICY: Tuple[Tuple[str, str, float, str], ...] = (
        # ---- CarbonLevel ----
        ("CarbonLevel:critical", "Decision:defer",    0.95, "carbon-critical"),
        ("CarbonLevel:critical", "Decision:throttle", 0.55, "carbon-critical"),
        ("CarbonLevel:high",     "Decision:defer",    0.80, "carbon-high"),
        ("CarbonLevel:high",     "Decision:throttle", 0.60, "carbon-high"),
        ("CarbonLevel:medium",   "Decision:throttle", 0.70, "carbon-medium"),
        ("CarbonLevel:medium",   "Decision:execute",  0.55, "carbon-medium"),
        ("CarbonLevel:low",      "Decision:execute",  0.85, "carbon-low"),
        # ---- BatteryState ----
        ("BatteryState:low",     "Decision:defer",    0.85, "battery-low"),
        ("BatteryState:low",     "Decision:throttle", 0.60, "battery-low"),
        ("BatteryState:high",    "Decision:execute",  0.75, "battery-high"),
        # ---- TaskPriority ----
        ("TaskPriority:critical","Decision:execute",  0.90, "priority-critical"),
        ("TaskPriority:low",     "Decision:defer",    0.60, "priority-low"),
        # ---- Zone ----
        ("Zone:red",             "Decision:defer",    0.90, "zone-red"),
        ("Zone:red",             "Decision:throttle", 0.55, "zone-red"),
        ("Zone:yellow",          "Decision:throttle", 0.75, "zone-yellow"),
        ("Zone:green",           "Decision:execute",  0.80, "zone-green"),
        # ---- QueueDepth ----
        ("QueueDepth:high",      "Decision:throttle", 0.75, "queue-high"),
        ("QueueDepth:low",       "Decision:execute",  0.60, "queue-low"),
    )

    def __init__(
        self,
        policy_yaml_path: Optional[str] = None,
        config: Optional[Any] = None,
        *,
        metrics: Optional[MetricsSink] = None,
        rng: Optional["np.random.Generator"] = None,
    ) -> None:
        # -- Config ------------------------------------------------
        if config is None:
            self.config = PolicyGraphConfig()
        elif isinstance(config, PolicyGraphConfig):
            self.config = config
        elif isinstance(config, Mapping):
            self.config = PolicyGraphConfig.from_dict(config)
        else:
            raise TypeError(
                f"config must be PolicyGraphConfig, dict or None; "
                f"got {type(config).__name__}"
            )

        self.metrics: MetricsSink = metrics or InMemoryMetrics()
        self.rng = rng if rng is not None else (
            np.random.default_rng(self.config.seed) if _NUMPY_OK else None
        )
        self._lock = threading.RLock()

        # -- Graph storage -----------------------------------------
        self.nodes: Dict[str, PolicyNode] = {}
        self.edges: List[PolicyEdge] = []
        self._adj: Dict[str, List[Tuple[str, float, str]]] = {}
        self._meta_zone_override: Optional[str] = None

        self._build_default()
        if policy_yaml_path:
            self._load_yaml(policy_yaml_path)

        # -- Decision history and pending decision -----------------
        self._history: deque = deque(maxlen=self.config.history_maxlen)
        self._last_decision_path: Optional[List[str]] = None
        self._last_decision: Optional[PendingDecision] = None

        # -- Optimizer ---------------------------------------------
        self.cost_optimizer: Optional[PolicyDistillationOptimizer] = None
        if self.config.use_enhancements and _NUMPY_OK:
            # use_evolutionary now controls the AlphaEvolution inside the
            # distillation optimizer — the entire pipeline runs either way,
            # but AlphaEvolution is the "bio-inspired" piece.
            if self.config.use_distillation or self.config.use_evolutionary:
                decisions = [d.split(":", 1)[1] for d in self.DECISION_IDS]
                self.cost_optimizer = PolicyDistillationOptimizer(
                    decisions, self.config, rng=self.rng
                )
                if not self.config.use_evolutionary:
                    # If the caller doesn't want evolution, freeze alpha at
                    # `initial_alpha` (still allow distillation/MoE).
                    self.cost_optimizer.alpha_evolution.best_alpha = (
                        self.config.initial_alpha
                    )
                    # Prevent updates from moving it.
                    self.cost_optimizer.alpha_evolution.update = (  # type: ignore[assignment]
                        lambda _a, _r: None
                    )
        elif self.config.use_enhancements and not _NUMPY_OK:
            logger.warning(
                "use_enhancements=True but numpy is unavailable; "
                "running in non-enhanced mode"
            )

        # Backwards-compat alias
        self.distillation_optimizer = self.cost_optimizer

        if self.config.model_path:
            self.load(self.config.model_path)

    # ------------------------------------------------------------------
    # Graph construction
    # ------------------------------------------------------------------
    def _build_default(self) -> None:
        all_ids: set = set()
        for src, tgt, _w, _tag in self.DEFAULT_POLICY:
            all_ids.add(src)
            all_ids.add(tgt)

        for nid in all_ids:
            parts = nid.split(":", 1)
            variable = parts[0]
            condition = parts[1] if len(parts) > 1 else ""
            is_dec = nid in self.DECISION_IDS
            self.nodes[nid] = PolicyNode(
                node_id=nid,
                variable=variable,
                condition=condition,
                is_decision=is_dec,
                decision=condition if is_dec else None,
            )

        for src, tgt, weight, tag in self.DEFAULT_POLICY:
            self._add_edge(
                PolicyEdge(source_id=src, target_id=tgt, weight=weight, context_tag=tag)
            )

    def _add_edge(self, edge: PolicyEdge) -> None:
        # Skip exact duplicates; the DFS uses the max-weight edge for a given
        # (source, target, tag) triple, so duplicates are only additive noise.
        for existing in self.edges:
            if (
                existing.source_id == edge.source_id
                and existing.target_id == edge.target_id
                and existing.context_tag == edge.context_tag
            ):
                return
        self.edges.append(edge)
        self._adj.setdefault(edge.source_id, []).append(
            (edge.target_id, edge.weight, edge.context_tag)
        )

    def _load_yaml(self, path: str) -> None:
        """Load a policy from YAML. The original silently no-op'd; we honour it."""
        try:
            import yaml  # type: ignore
        except ImportError as exc:
            raise RuntimeError(
                "pyyaml is required to load policy_yaml_path"
            ) from exc
        with open(path, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f) or {}
        rules = data.get("rules") or data.get("edges") or []
        for rule in rules:
            src = rule.get("source") or rule.get("from")
            tgt = rule.get("target") or rule.get("to")
            if not src or not tgt:
                continue
            weight = float(rule.get("weight", 1.0))
            tag = str(rule.get("tag") or rule.get("context_tag") or "")
            self._add_edge(
                PolicyEdge(source_id=src, target_id=tgt, weight=weight, context_tag=tag)
            )

    # ------------------------------------------------------------------
    # Public setter for the meta zone override
    # ------------------------------------------------------------------
    def set_meta_zone_override(self, zone: Optional[str]) -> None:
        if zone is not None and zone not in ("green", "yellow", "red"):
            raise ValueError(f"zone must be one of green/yellow/red, got {zone!r}")
        with self._lock:
            self._meta_zone_override = zone

    # ------------------------------------------------------------------
    # Decision traversal
    # ------------------------------------------------------------------
    def decide(
        self,
        context: Mapping[str, Any],
        *,
        max_depth: Optional[int] = None,
    ) -> Dict[str, Any]:
        if not isinstance(context, Mapping):
            raise TypeError("context must be a Mapping")
        ctx = dict(context)
        depth_limit = int(max_depth if max_depth is not None else self.config.max_dfs_depth)

        with self._lock:
            if self._meta_zone_override in ("green", "yellow", "red"):
                ctx["zone"] = self._meta_zone_override

            active = self._resolve_active_nodes(ctx)
            scores: Dict[str, float] = {d: 0.0 for d in self.DECISION_IDS}
            best_paths: Dict[str, List[str]] = {d: [] for d in self.DECISION_IDS}
            best_tags: Dict[str, List[str]] = {d: [] for d in self.DECISION_IDS}

            for start in active:
                self._dfs(
                    current=start,
                    path=[start],
                    tags=[],
                    weight=1.0,
                    scores=scores,
                    best_paths=best_paths,
                    best_tags=best_tags,
                    visited=set(),
                    depth=0,
                    max_depth=depth_limit,
                )

            # Canonical short names
            raw_scores: Dict[str, float] = {
                d.split(":", 1)[1]: v for d, v in scores.items()
            }

            alpha_used: Optional[float] = None
            enhanced = False

            if self.cost_optimizer is not None:
                state = PolicyDecisionState(
                    context=ctx,
                    graph_metrics=self._graph_metrics_for_state(),
                    human_feedback=self.config.human_feedback_score,
                )
                (
                    final_probs,
                    state_vec,
                    teacher_outputs,
                    gate_probs,
                    teacher_mix,
                    alpha_used,
                ) = self.cost_optimizer.predict_proba(state, raw_scores)
                decision_map = {
                    d: float(final_probs[i])
                    for i, d in enumerate(self.cost_optimizer.decisions)
                }
                winner_short = max(decision_map, key=decision_map.get)
                final_scores = {
                    d: round(decision_map[d], 4) for d in decision_map
                }
                self._last_decision = PendingDecision(
                    state_vec=state_vec,
                    teacher_outputs=teacher_outputs,
                    gate_probs=gate_probs,
                    teacher_mix=teacher_mix,
                    action_short=winner_short,
                    action_idx=self.cost_optimizer.decision_index[winner_short],
                    alpha_used=float(alpha_used),
                    raw_scores=dict(raw_scores),
                )
                enhanced = True
            else:
                # Non-enhanced path — canonical short names (fixes the v1 bug
                # where score was always 0.0 and winning_path always []).
                winner_short = max(raw_scores, key=raw_scores.get)
                final_scores = {d: round(v, 4) for d, v in raw_scores.items()}
                self._last_decision = None

            # Populate the pending path so feedback() can update edge weights
            # (fixes the v1 bug where _last_decision_path was never set).
            winner_full_id = "Decision:" + winner_short
            self._last_decision_path = list(best_paths.get(winner_full_id, []))

            result = PolicyDecision(
                decision=winner_short,
                score=final_scores.get(winner_short, 0.0),
                all_scores=final_scores,
                active_nodes=list(active),
                winning_path=list(best_paths.get(winner_full_id, [])),
                winning_tags=list(best_tags.get(winner_full_id, [])),
                reasoning=self._reasoning(
                    ctx, winner_short,
                    best_paths.get(winner_full_id, []),
                    best_tags.get(winner_full_id, []),
                ),
                alpha_used=alpha_used,
                enhanced=enhanced,
            )
            self._history.append(result)
            self.metrics.observe(
                "policy_decisions_total", 1.0,
                decision=winner_short, enhanced=str(enhanced),
            )
            return result.to_dict()

    def _dfs(
        self,
        current: str,
        path: List[str],
        tags: List[str],
        weight: float,
        scores: Dict[str, float],
        best_paths: Dict[str, List[str]],
        best_tags: Dict[str, List[str]],
        visited: set,
        depth: int,
        max_depth: int,
    ) -> None:
        if depth > max_depth or current in visited:
            return
        visited = visited | {current}

        if current in self.DECISION_IDS:
            if weight > scores[current]:
                scores[current] = weight
                best_paths[current] = list(path)
                best_tags[current] = list(tags)
            return

        for target, edge_w, tag in self._adj.get(current, []):
            new_w = weight * edge_w if edge_w > 0 else 0.0
            if new_w > 0.01:
                self._dfs(
                    target, path + [target], tags + [tag], new_w,
                    scores, best_paths, best_tags, visited, depth + 1, max_depth,
                )

    def _resolve_active_nodes(self, context: Mapping[str, Any]) -> List[str]:
        active: List[str] = []

        carbon = float(context.get("carbon_g_per_kwh", 200.0))
        if carbon >= 500:
            active.append("CarbonLevel:critical")
        elif carbon >= 400:
            active.append("CarbonLevel:high")
        elif carbon >= 250:
            active.append("CarbonLevel:medium")
        else:
            active.append("CarbonLevel:low")

        battery = float(context.get("battery_pct", 1.0))
        active.append("BatteryState:low" if battery < 0.40 else "BatteryState:high")

        priority = float(context.get("task_priority", 0.5))
        if priority >= 0.80:
            active.append("TaskPriority:critical")
        elif priority <= 0.30:
            active.append("TaskPriority:low")

        zone = str(context.get("zone", "green"))
        active.append(f"Zone:{zone}")

        queue = float(context.get("queue_depth", 0))
        if queue > 80:
            active.append("QueueDepth:high")
        elif queue < 20:
            active.append("QueueDepth:low")

        missing = [n for n in active if n not in self.nodes]
        if missing:
            logger.warning(
                "Active nodes missing from graph (will be skipped): %s", missing
            )
        return [n for n in active if n in self.nodes]

    def _reasoning(
        self,
        context: Mapping[str, Any],
        winner_short: str,
        path: List[str],
        tags: List[str],
    ) -> str:
        path_readable = " → ".join(n.split(":")[-1] for n in path) or "(no path)"
        return (
            f"Context: carbon={context.get('carbon_g_per_kwh', '?')}g/kWh, "
            f"battery={int(float(context.get('battery_pct', 1)) * 100)}%, "
            f"priority={context.get('task_priority', '?')}, "
            f"zone={context.get('zone', '?')}, "
            f"queue={context.get('queue_depth', '?')} | "
            f"Decision: {winner_short} | "
            f"Path: {path_readable} | "
            f"Tags: {','.join(tags) if tags else '—'}"
        )

    def _graph_metrics_for_state(self) -> Dict[str, float]:
        """
        Compute centrality/connectivity on demand if the caller didn't
        supply static graph_metrics. Falls back to config values otherwise.
        """
        cfg_gm = self.config.graph_metrics
        # If the config was left at the default (0.5/0.5), compute from edges.
        if (
            cfg_gm.get("centrality") == 0.5
            and cfg_gm.get("connectivity") == 0.5
            and self.edges
        ):
            weights = [abs(e.weight) for e in self.edges]
            centrality = _clamp(sum(weights) / (len(weights) * max(max(weights), 1e-9)))
            n_nodes = len(self.nodes)
            if n_nodes > 1:
                max_edges = n_nodes * (n_nodes - 1)
                connectivity = _clamp(len(self.edges) / max(max_edges, 1))
            else:
                connectivity = 0.0
            return {"centrality": centrality, "connectivity": connectivity}
        return dict(cfg_gm)

    # ------------------------------------------------------------------
    # Online learning
    # ------------------------------------------------------------------
    def update_edge_weight(
        self,
        source_id: str,
        target_id: str,
        decision_was_correct: bool,
        learning_rate: float = 0.05,
    ) -> bool:
        """Return True if at least one edge was updated."""
        if not _is_finite(learning_rate) or learning_rate <= 0:
            raise ValueError("learning_rate must be a positive finite number")
        delta = learning_rate if decision_was_correct else -learning_rate
        updated = False
        with self._lock:
            lo, hi = self.config.edge_weight_min, self.config.edge_weight_max
            new_edges = []
            for edge in self.edges:
                if edge.source_id == source_id and edge.target_id == target_id:
                    new_weight = _clamp(edge.weight + delta, lo, hi)
                    new_edges.append(
                        PolicyEdge(
                            source_id=edge.source_id,
                            target_id=edge.target_id,
                            weight=new_weight,
                            context_tag=edge.context_tag,
                        )
                    )
                    updated = True
                else:
                    new_edges.append(edge)
            if updated:
                self.edges = new_edges
                # Rebuild adjacency (single source of truth).
                self._adj = {}
                for e in self.edges:
                    self._adj.setdefault(e.source_id, []).append(
                        (e.target_id, e.weight, e.context_tag)
                    )
        return updated

    def feedback(
        self,
        decision_was_correct: bool,
        learning_rate: float = 0.05,
    ) -> Dict[str, Any]:
        with self._lock:
            edges_updated = 0

            # 1. Edge weight updates along the winning path (fixes v1 §2.4)
            path = self._last_decision_path or []
            for a, b in zip(path, path[1:]):
                if self.update_edge_weight(a, b, decision_was_correct, learning_rate):
                    edges_updated += 1

            # 2. Optimizer update
            optimizer_updated = False
            reward = 0.0
            counter: Optional[int] = None
            alpha: Optional[float] = None

            if self.cost_optimizer is not None and self._last_decision is not None:
                try:
                    pending = self._last_decision
                    reward = (
                        self.config.reward_correct
                        if decision_was_correct
                        else self.config.reward_wrong
                    )
                    reward += self.config.rlhf_tilt * self.config.human_feedback_score
                    reward = float(np.clip(reward, -1.0, 1.0))

                    one_hot = np.zeros(self.cost_optimizer.n_actions)
                    if pending.action_short in self.cost_optimizer.decision_index:
                        one_hot[self.cost_optimizer.decision_index[pending.action_short]] = 1.0
                    else:
                        logger.warning(
                            "Unknown decision %r; skipping optimizer update",
                            pending.action_short,
                        )
                        raise _SkipUpdate

                    self.cost_optimizer.update(
                        state_vec=pending.state_vec,
                        teacher_outputs=pending.teacher_outputs,
                        gate_probs=pending.gate_probs,
                        action_onehot=one_hot,
                        reward=reward,
                        alpha_used=pending.alpha_used,
                    )
                    optimizer_updated = True
                    counter = self.cost_optimizer.counter
                    alpha = self.cost_optimizer.alpha_evolution.best_alpha
                except _SkipUpdate:
                    pass
                finally:
                    # Always clear pending (fixes v1 §2.10)
                    self._last_decision = None

            result = FeedbackResult(
                edges_updated=edges_updated,
                reward=float(reward),
                optimizer_updated=optimizer_updated,
                optimizer_counter=counter,
                alpha=alpha,
            )
            self.metrics.observe(
                "policy_feedback_total", 1.0,
                correct=str(bool(decision_was_correct)),
            )
            if optimizer_updated:
                self.metrics.observe("policy_reward", reward)
            return asdict(result)

    # ------------------------------------------------------------------
    # Export
    # ------------------------------------------------------------------
    def export_weights(self) -> List[Dict[str, Any]]:
        with self._lock:
            return [
                {
                    "source": e.source_id,
                    "target": e.target_id,
                    "weight": round(e.weight, 4),
                    "context_tag": e.context_tag,
                }
                for e in self.edges
            ]

    # ------------------------------------------------------------------
    # Summary / persistence
    # ------------------------------------------------------------------
    def summary(self) -> Dict[str, Any]:
        with self._lock:
            total = len(self._history)
            decisions = {}
            enhanced = 0
            for rec in self._history:
                decisions[rec.decision] = decisions.get(rec.decision, 0) + 1
                if rec.enhanced:
                    enhanced += 1
            report: Dict[str, Any] = {
                "total_decisions": total,
                "enhanced_decisions": enhanced,
                "decision_distribution": decisions,
            }
            if self.cost_optimizer is not None:
                report["optimizer"] = {
                    "counter": self.cost_optimizer.counter,
                    "epsilon": self.cost_optimizer.epsilon,
                    "replay_size": len(self.cost_optimizer.replay),
                    "alpha": self.cost_optimizer.alpha_evolution.best_alpha,
                }
            return report

    def save(self, path: Optional[Path] = None) -> None:
        target = Path(path) if path is not None else self.config.model_path
        if target is None or self.cost_optimizer is None:
            return
        self.cost_optimizer.save(target)

    def load(self, path: Optional[Path] = None) -> None:
        target = Path(path) if path is not None else self.config.model_path
        if target is None or self.cost_optimizer is None:
            return
        self.cost_optimizer.load(target)


class _SkipUpdate(Exception):
    """Internal sentinel: raise to skip the optimizer update without erroring."""


# ============================================================================
# SECTION 11 — Demo
# ============================================================================

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    cfg = PolicyGraphConfig.from_dict(
        {
            "use_enhancements": True,
            "use_distillation": True,
            "use_evolutionary": True,
            "modp_weights": [0.4, 0.3, 0.2, 0.1],
            "human_feedback_score": 0.6,
            "seed": 42,
            "model_path": Path("./models/policy_graph.json"),
        }
    )
    graph = PolicyGraph(config=cfg)

    ctx = {
        "carbon_g_per_kwh": 450.0,
        "battery_pct": 0.35,
        "task_priority": 0.85,
        "zone": "yellow",
        "queue_depth": 95,
    }
    for _ in range(3):
        decision = graph.decide(ctx)
        print(json.dumps(decision, indent=2))
        result = graph.feedback(decision_was_correct=(_ % 2 == 0))
        print("feedback:", result)

    print("summary:", json.dumps(graph.summary(), indent=2))
    graph.save()
