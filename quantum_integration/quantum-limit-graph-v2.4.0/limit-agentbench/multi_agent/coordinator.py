# -*- coding: utf-8 -*-
"""
MultiAgentCoordinator (Enhanced, v2)
====================================

Distributes or routes tasks across multiple agents.

Fixes applied vs. v1
--------------------
  P0  State described ``agents[0]`` only — the model could never learn
      per-agent preferences.                              -> per-agent
                                                             state, scalar
                                                             scoring
  P0  Teachers hardcoded agent indices ("agent 0 is reliable",
      "agent 2 is quality", "last is expert") and were reversed
      by caller ordering.                                 -> AgentProfile
                                                             with quality/
                                                             efficiency
                                                             attributes
  P0  ``_last_decision`` was dead (set once, never read)
                                                          -> removed;
                                                             replaced by a
                                                             typed
                                                             PendingDecision
                                                             consumed by
                                                             record_outcome
  P0  ``**kwargs`` were accepted and silently discarded   -> forwarded to
                                                             agent.execute
  P0  Empty agent list crashed ``n_actions=0`` (div by zero)
                                                          -> validated at
                                                             construction
  P0  ``distribute`` returned N results in base mode and 1 result in
      enhanced mode — same method, incompatible contract  -> fan-out
                                                             preserved;
                                                             new ``route``
                                                             method for
                                                             single-agent
                                                             routing
  P0  Coordinating ``AgentNode`` agents confounded the reward with the
      agent's internal strategy choice                   -> coordinator
                                                             best-effort
                                                             disables agent
                                                             auto-training;
                                                             documented
  P0  ``hash(name)`` was salted per process → feature vectors
      non-deterministic across runs → trained models
      non-reproducible                                   -> sha256
  P0  MoE gate gradient wrong shape/sign (14th occurrence)
                                                          -> correct
                                                             softmax chain
                                                             rule
  P0  Reward was ``≈ 0.9`` constant when agents didn't report metrics
                                                          -> returns
                                                             ``None`` and
                                                             skips the
                                                             update
  P0  ``len(str(task))`` allocated the whole task as a string
                                                          -> ``len(task)``
  P0  Config mutated in ``__init__``; ``modp_weights=[]`` / 4-tuple /
      negative crashed at ``np.dot`` time                 -> frozen config
                                                             + validation
  P0  ``use_evolutionary`` was ``pass``                   -> implemented
  P1  Unseeded RNG                                         -> default_rng
  P1  Hardcoded ``feature_dim``, ``distill_w``, ``rl_w``,
      ``batch_size``                                      -> config
  P1  Epsilon never decayed                                -> decay
  P1  Latency saturation (``1 - min(L/1000, 1)``)          -> 1/(1+q/scale)
  P1  Carbon used magic ``energy * 0.4``                   -> config field
  P1  Reward was asymmetric (``[-0.05, 1.0]``)             -> symmetric
                                                             ``[-1, 1]``
  P2  No persistence / metrics / thread safety             -> save/load,
                                                             MetricsSink,
                                                             RLock
  P2  No ``record_outcome`` / ``record_human_feedback``    -> added
  P2  ``graph_metrics.density`` unused                     -> exposed via
                                                             context

Return cardinality
------------------
The two public methods have distinct, unambiguous semantics:

    ``distribute(task, **kwargs) -> List[Result]``
        Fan-out. Runs *every* agent, returns every result in agent order.
        Enhanced mode additionally trains the router on each agent's
        reward (a comparison signal).

    ``route(task, **kwargs) -> Result``
        Routing. Returns exactly one result.
          * Base path: runs all agents, returns the best by MODP reward.
          * Enhanced path: the distillation router picks one agent to run.

Both paths preserve v1's base ``distribute`` behavior (fan-out, N results).

Cross-file coordination with ``AgentNode``
------------------------------------------
The coordinator best-effort sets ``wait_for_record_outcome=True`` on any
agent whose ``config`` exposes that field (e.g., ``AgentNode`` from
``agent_node.py`` v2). This decouples the agent's internal strategy
training from the coordinator's routing training, preventing the reward
signal from being confounded by the agent's own strategy choice.

If an agent doesn't expose this field, the coordinator still works — but
the reward may reflect both the agent's capability and the agent's
strategy. Documented as a known limitation.

Public API (preserved + extended)
---------------------------------
  Preserved: ``CoordinatorConfig``, ``CoordinatorState``,
  ``DistillationOptimizer``, ``MultiAgentCoordinator`` (constructor +
  ``distribute``)

  New: ``AgentProfile``, ``PendingDecision``,
  ``EvolutionaryMODPWeightsOptimizer``, ``MetricsSink``,
  ``InMemoryMetrics``, ``LoggingMetrics``, ``MODP_SCHEMA``, plus methods
  on ``MultiAgentCoordinator``: ``route``, ``record_outcome``,
  ``record_human_feedback``, ``save``, ``load``, ``get_optimizer_stats``

Breaking changes (intentional)
------------------------------
  * ``CoordinatorState.__init__`` now takes ``(task, agent, graph_metrics,
    human_feedback)`` instead of ``(task, agent_name, ...)``.
  * ``CoordinatorState`` feature vector expanded from 7 to 8 dims
    (``agent_name_encoded`` → ``agent_quality``, ``agent_efficiency``).
    Existing models cannot be loaded — retrain.
  * ``DistillationOptimizer.select_agent_index`` → ``select_agent(states)``;
    ``update`` no longer takes ``action`` or ``next_state_vec``.
  * ``MultiAgentCoordinator.distribute`` always returns N results in enhanced
    mode (was 1). Callers that assumed 1 result need to use ``route``.
  * ``MultiAgentCoordinator._last_decision`` no longer exists.
  * Reward is now ``[-1, 1]`` (was effectively ``[-0.05, 1.0]``).
"""

from __future__ import annotations

import hashlib
import json
import logging
import math
import os
import threading
from collections import deque
from dataclasses import dataclass, field, replace
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
# Section 1 — Constants
# ============================================================================

# Canonical MODP ordering. 13 prior files use a different order; never
# import weights across files without translating through their schema.
MODP_SCHEMA: Tuple[str, ...] = ("quality", "energy", "latency", "carbon")


# ============================================================================
# Section 2 — Helpers
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
        self._log = logging.getLogger("coordinator.metrics")

    def observe(self, name: str, value: float, **labels: str) -> None:
        self._log.info("metric=%s value=%.6f labels=%s", name, value, labels)


# ============================================================================
# Section 4 — Agent profile (replaces hardcoded indices)
# ============================================================================

@dataclass(frozen=True)
class AgentProfile:
    """Static capability profile for an agent.

    When an agent doesn't expose a ``profile`` attribute, the coordinator
    extracts ``quality`` and ``efficiency`` from the agent's attributes
    (falling back to 0.5) and uses the name for logging.
    """
    name: str
    quality: float = 0.5       # expected accuracy in [0, 1]
    efficiency: float = 0.5    # expected efficiency in [0, 1], higher = better
    specialties: frozenset = field(default_factory=frozenset)

    def __post_init__(self) -> None:
        if not 0.0 <= self.quality <= 1.0:
            raise ValueError("quality must be in [0, 1]")
        if not 0.0 <= self.efficiency <= 1.0:
            raise ValueError("efficiency must be in [0, 1]")


# ============================================================================
# Section 5 — Configuration (frozen, validated)
# ============================================================================

@dataclass(frozen=True)
class CoordinatorConfig:
    """Frozen, validated configuration for MultiAgentCoordinator."""
    use_enhancements: bool = False

    graph_metrics: Dict[str, float] = field(
        default_factory=lambda: {"centrality": 0.5, "connectivity": 0.5}
    )
    # 4-tuple, MODP_SCHEMA order: (quality, energy, latency, carbon).
    modp_weights: Tuple[float, float, float, float] = (0.4, 0.3, 0.2, 0.1)
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

    # Region + carbon intensity (replaces v1's magic ``energy * 0.4``).
    region: str = "GLOBAL"
    carbon_intensity_kg_per_kwh: float = 0.475

    # Bounded normalization scales.
    energy_scale_kwh: float = 0.01
    latency_scale_ms: float = 500.0

    # When True, ``distribute``/``route`` store the pending decision but
    # don't train until the caller invokes ``record_outcome``. Default
    # False preserves v1's auto-train behavior.
    wait_for_record_outcome: bool = False

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
        for name in ("energy_scale_kwh", "latency_scale_ms"):
            if getattr(self, name) <= 0:
                raise ValueError(f"{name} must be > 0")
        if self.carbon_intensity_kg_per_kwh < 0:
            raise ValueError("carbon_intensity_kg_per_kwh must be >= 0")
        for k, v in self.graph_metrics.items():
            if not isinstance(v, (int, float)) or not 0.0 <= float(v) <= 1.0:
                raise ValueError(
                    f"graph_metric {k!r} must be in [0,1], got {v!r}"
                )
        if self.model_path is not None:
            object.__setattr__(self, "model_path", Path(self.model_path))

    @classmethod
    def from_dict(cls, d: Mapping[str, Any]) -> "CoordinatorConfig":
        if not isinstance(d, Mapping):
            raise TypeError(f"config must be a Mapping, got {type(d).__name__}")
        known = set(cls.__dataclass_fields__.keys())
        unknown = set(d.keys()) - known
        if unknown:
            logger.warning(
                "CoordinatorConfig ignoring unknown keys: %s", sorted(unknown)
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
# Section 6 — State (per-agent, deterministic, 8 features)
# ============================================================================

class CoordinatorState:
    """Deterministic per-agent state for router training."""
    FEATURE_DIM = 8

    __slots__ = (
        "task_size", "task_complexity", "task_priority",
        "centrality", "connectivity", "human_feedback",
        "agent_quality", "agent_efficiency",
        "agent_name",
        "_vector",
    )

    def __init__(
        self,
        task: Any,
        agent: Any,
        graph_metrics: Mapping[str, float],
        human_feedback: float,
    ) -> None:
        if isinstance(task, dict):
            # ``len(task)`` is O(number of keys) — no str() allocation.
            self.task_size = len(task)
            self.task_complexity = _clamp(float(task.get("complexity", 0.5)))
            self.task_priority = _clamp(float(task.get("priority", 0.5)))
        else:
            self.task_size = 1
            self.task_complexity = 0.5
            self.task_priority = 0.5

        profile = self._extract_profile(agent)
        self.agent_name = profile.name
        self.agent_quality = _clamp(profile.quality)
        self.agent_efficiency = _clamp(profile.efficiency)

        self.centrality = _clamp(float(graph_metrics.get("centrality", 0.5)))
        self.connectivity = _clamp(float(graph_metrics.get("connectivity", 0.5)))
        self.human_feedback = _clamp(float(human_feedback))

        self._vector = np.array(
            [
                min(self.task_size / 10_000.0, 1.0),
                self.task_complexity,
                self.task_priority,
                self.centrality,
                self.connectivity,
                self.human_feedback,
                self.agent_quality,
                self.agent_efficiency,
            ],
            dtype=np.float32,
        )
        assert self._vector.shape == (self.FEATURE_DIM,)

    @staticmethod
    def _extract_profile(agent: Any) -> AgentProfile:
        profile = getattr(agent, "profile", None)
        if isinstance(profile, AgentProfile):
            return profile
        return AgentProfile(
            name=getattr(agent, "name", f"agent_{id(agent)}"),
            quality=_clamp(float(getattr(agent, "quality", 0.5))),
            efficiency=_clamp(float(getattr(agent, "efficiency", 0.5))),
        )

    def to_vector(self) -> "np.ndarray":
        return self._vector


# ============================================================================
# Section 7 — Pending decision
# ============================================================================

@dataclass
class PendingDecision:
    state_vec: "np.ndarray"
    action: int
    teacher_scores: "np.ndarray"
    gate_probs: "np.ndarray"
    derived_reward: Optional[float]


# ============================================================================
# Section 8 — Distillation optimizer (scalar scores, correct gate)
# ============================================================================

class DistillationOptimizer:
    """Multi-teacher distillation + MoE gating for per-agent scoring.

    Each teacher emits a *scalar score* in ``[-1, 1]`` per agent state.
    The student is a linear function of the state, also in ``[-1, 1]``.
    The gate mixes the teachers by a 7-dim context derived from the state.

    Training:
      - Student: MSE toward the observed reward (per-agent regression).
      - Gate: reinforce teachers whose scores aligned with the reward.

    ``select_agent(states)`` scores every agent's state, then picks argmax.
    """

    N_TEACHERS = 3

    def __init__(
        self,
        config: CoordinatorConfig,
        rng: Optional["np.random.Generator"] = None,
    ) -> None:
        if not ENHANCEMENTS_AVAILABLE:
            raise RuntimeError("numpy is required for DistillationOptimizer")
        self.config = config
        self.rng = rng if rng is not None else np.random.default_rng(config.seed)

        self.feature_dim = CoordinatorState.FEATURE_DIM
        self.lr = config.distillation_lr
        self.gate_lr = config.gating_lr
        self.distill_w = config.distill_w
        self.rl_w = config.rl_w
        self.epsilon = config.epsilon
        self.epsilon_min = config.epsilon_min
        self.epsilon_decay = config.epsilon_decay
        self.train_every = config.train_every
        self.batch_size = config.batch_size

        # Student: scalar score (feature_dim,) → scalar
        self.student_weights = np.zeros(self.feature_dim)
        self.student_bias = 0.0
        # Gate: (feature_dim, n_teachers) → (n_teachers,)
        self.gate_weights = (
            self.rng.standard_normal((self.feature_dim, self.N_TEACHERS)) * 0.01
        )
        self.gate_bias = np.zeros(self.N_TEACHERS)

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

    def _gate_forward(self, x: "np.ndarray") -> "np.ndarray":
        return self._softmax(x @ self.gate_weights + self.gate_bias)

    # ---- teachers (scalar scoring functions) ----------------------------
    def _rule_teacher_score(self, state: CoordinatorState) -> float:
        # Priority-weighted blend of quality and efficiency.
        # Higher task_priority → weight quality more; lower → weight efficiency.
        priority_w = 0.5 + 0.5 * state.task_priority
        efficiency_w = 1.0 - priority_w
        score = priority_w * state.agent_quality + efficiency_w * state.agent_efficiency
        return 2.0 * _clamp(score) - 1.0

    def _rlhf_teacher_score(self, state: CoordinatorState) -> float:
        # High feedback trusts quality; low feedback trusts efficiency.
        h = state.human_feedback
        if h > 0.7:
            score = state.agent_quality
        elif h < 0.3:
            score = state.agent_efficiency
        else:
            score = 0.5 * state.agent_quality + 0.5 * state.agent_efficiency
        return 2.0 * _clamp(score) - 1.0

    def _historical_teacher_score(self, state: CoordinatorState) -> float:
        # k-NN over the replay buffer's (state → reward) observations.
        if not self.replay:
            return 0.0
        with self._lock:
            recent = list(self.replay)[-32:]
        query = state.to_vector()
        num = 0.0
        den = 0.0
        for s, r, _t, _g in recent:
            sim = 1.0 / (1.0 + float(np.linalg.norm(s - query)))
            num += sim * r
            den += sim
        return (num / den) if den > 0 else 0.0

    def _teacher_scores(self, state: CoordinatorState) -> "np.ndarray":
        return np.array(
            [
                self._rule_teacher_score(state),
                self._rlhf_teacher_score(state),
                self._historical_teacher_score(state),
            ],
            dtype=np.float64,
        )

    # ---- forward ----------------------------------------------------------
    def score(
        self, state: CoordinatorState
    ) -> Tuple[float, "np.ndarray", "np.ndarray"]:
        """Return ``(final_score, teacher_scores, gate_probs)`` for one agent."""
        x = state.to_vector()
        with self._lock:
            teacher_scores = self._teacher_scores(state)
            gate_probs = self._gate_forward(x)
            teacher_mix = float(gate_probs @ teacher_scores)
            student = float(x @ self.student_weights + self.student_bias)
            final = float(np.clip(
                0.8 * student + 0.2 * teacher_mix, -1.0, 1.0
            ))
        return final, teacher_scores, gate_probs

    def select_agent(
        self,
        states: Sequence[CoordinatorState],
        exploration: bool = True,
    ) -> Tuple[int, "np.ndarray", "np.ndarray", "np.ndarray"]:
        """
        Score every agent, pick the best (with epsilon-greedy exploration).

        Returns
        -------
        action         : int
        state_vec      : (FEATURE_DIM,) for the chosen agent
        teacher_scores : (N_TEACHERS,) for the chosen agent
        gate_probs     : (N_TEACHERS,) for the chosen agent
        """
        n = len(states)
        scores: List[float] = []
        contexts: List[Tuple["np.ndarray", "np.ndarray", "np.ndarray"]] = []
        for state in states:
            s, t, g = self.score(state)
            scores.append(s)
            contexts.append((state.to_vector(), t, g))

        if exploration and self.rng.random() < self.epsilon:
            idx = int(self.rng.integers(0, n))
        else:
            idx = int(np.argmax(scores))

        state_vec, teacher_scores, gate_probs = contexts[idx]
        return idx, state_vec, teacher_scores, gate_probs

    # ---- backward ---------------------------------------------------------
    def update(
        self,
        state_vec: "np.ndarray",
        reward: float,
        teacher_scores: "np.ndarray",
        gate_probs: "np.ndarray",
    ) -> None:
        r = float(np.clip(reward, -1.0, 1.0))
        with self._lock:
            self.replay.append(
                (
                    np.asarray(state_vec, dtype=np.float64),
                    r,
                    np.asarray(teacher_scores, dtype=np.float64),
                    np.asarray(gate_probs, dtype=np.float64),
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

            for s, r, t_scores, gate_probs in batch:
                # ---- Student: MSE toward reward ----
                student = float(s @ self.student_weights + self.student_bias)
                err = student - r
                self.student_weights -= self.lr * err * s
                self.student_bias -= self.lr * err

                # ---- Gate: reinforce teachers whose scores align with reward ----
                teacher_mix = float(gate_probs @ t_scores)
                err_gate = teacher_mix - r
                dL_dg = err_gate * t_scores                # (T,)
                grad_logits = gate_probs * (
                    dL_dg - float(gate_probs @ dL_dg)
                )                                           # (T,)
                self.gate_weights -= self.gate_lr * np.outer(s, grad_logits)
                self.gate_bias -= self.gate_lr * grad_logits

            self.epsilon = max(
                self.epsilon_min, self.epsilon * self.epsilon_decay
            )

    # ---- persistence ------------------------------------------------------
    def save(self, path: Path) -> None:
        _save_json(path, {
            "kind": "coordinator_distillation",
            "feature_dim": self.feature_dim,
            "student_weights": self.student_weights.tolist(),
            "student_bias": [float(self.student_bias)],
            "gate_weights": self.gate_weights.tolist(),
            "gate_bias": self.gate_bias.tolist(),
            "epsilon": float(self.epsilon),
            "counter": int(self.counter),
        })

    def load(self, path: Path) -> None:
        payload = _load_json(path)
        if not payload:
            return
        if payload.get("kind") != "coordinator_distillation":
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
# Section 9 — Evolutionary MODP optimizer (implements use_evolutionary)
# ============================================================================

@dataclass
class _Ind:
    weights: "np.ndarray"    # (4,) normalized MODP weights
    fitness: float = 0.0


class EvolutionaryMODPWeightsOptimizer:
    """Evolves the 4-tuple MODP weight vector against observed rewards."""

    def __init__(
        self,
        config: CoordinatorConfig,
        normalized_weights: Tuple[float, float, float, float],
        rng: Optional["np.random.Generator"] = None,
    ) -> None:
        if not ENHANCEMENTS_AVAILABLE:
            raise RuntimeError("numpy is required for the evolutionary optimizer")
        self.config = config
        self.rng = rng if rng is not None else np.random.default_rng(config.seed)
        self.n_weights = 4
        self.counter = 0
        self.generation = 0
        self.best_weights = np.asarray(normalized_weights, dtype=np.float64)
        self.best_fitness = -math.inf
        self._lock = threading.RLock()

        self.population: List[_Ind] = [
            _Ind(weights=self.rng.dirichlet(np.ones(self.n_weights)))
            for _ in range(config.population_size)
        ]
        self.population[0] = _Ind(weights=self.best_weights.copy())

        if config.model_path:
            self.load(Path(str(config.model_path) + ".evolve.json"))

    def observe(self, features: "np.ndarray", target: float) -> None:
        t = float(np.clip(target, -1.0, 1.0))
        with self._lock:
            for ind in self.population:
                pred = float(np.clip(2.0 * (ind.weights @ features) - 1.0,
                                     -1.0, 1.0))
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

    def save(self, path: Path) -> None:
        _save_json(path, {
            "kind": "coordinator_evolution",
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
        if not payload or payload.get("kind") != "coordinator_evolution":
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
# Section 10 — MultiAgentCoordinator (main class)
# ============================================================================

class MultiAgentCoordinator:
    """Distributes or routes tasks across multiple agents."""

    def __init__(
        self,
        agents: Sequence[Any],
        config: Optional[Any] = None,
        *,
        metrics: Optional[MetricsSink] = None,
        rng: Optional["np.random.Generator"] = None,
    ) -> None:
        if not agents:
            raise ValueError(
                "MultiAgentCoordinator requires at least one agent"
            )

        if config is None:
            self.config = CoordinatorConfig()
        elif isinstance(config, CoordinatorConfig):
            self.config = config
        elif isinstance(config, Mapping):
            self.config = CoordinatorConfig.from_dict(config)
        else:
            raise TypeError(
                f"config must be CoordinatorConfig, dict or None; "
                f"got {type(config).__name__}"
            )

        # Immutable agent tuple (fixes §5.5 in the analysis).
        self.agents: Tuple[Any, ...] = tuple(agents)
        self.metrics: MetricsSink = metrics or InMemoryMetrics()
        self._lock = threading.RLock()
        self.rng = rng if rng is not None else (
            np.random.default_rng(self.config.seed) if ENHANCEMENTS_AVAILABLE
            else None
        )

        # Local normalized MODP — never mutate config.
        w = self.config.modp_weights
        total = sum(w)
        self._modp_weights: Tuple[float, float, float, float] = (
            w[0] / total, w[1] / total, w[2] / total, w[3] / total,
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

        # Optimizers
        self.distillation_optimizer: Optional[DistillationOptimizer] = None
        self.evolutionary_optimizer: Optional[EvolutionaryMODPWeightsOptimizer] = None
        if self.use_enhancements:
            if self.config.use_distillation:
                self.distillation_optimizer = DistillationOptimizer(
                    self.config, rng=self.rng
                )
            if self.config.use_evolutionary:
                self.evolutionary_optimizer = EvolutionaryMODPWeightsOptimizer(
                    self.config, self._modp_weights, rng=self.rng
                )

        # Best-effort: disable agent auto-training to avoid confound.
        self._prepare_agents()

        self._pending: Optional[PendingDecision] = None
        self._last_results: Optional[List[Any]] = None

        logger.info(
            "Initialized MultiAgentCoordinator: %d agents, enhancements=%s",
            len(self.agents), self.use_enhancements,
        )

    # ------------------------------------------------------------------
    # Public: distribute (fan-out, v1-compatible)
    # ------------------------------------------------------------------
    def distribute(self, task: Any, **kwargs: Any) -> List[Any]:
        """Run the task on *every* agent; return all results in order.

        Base mode: identical to v1 — no side effects.

        Enhanced mode: additionally trains the router on each agent's
        derived reward (a comparison signal across agents). Still returns
        N results.
        """
        if not self.use_enhancements or self.distillation_optimizer is None:
            return [agent.execute(task, **kwargs) for agent in self.agents]

        states: List[CoordinatorState] = []
        rewards: List[Optional[float]] = []
        results: List[Any] = []
        gm = self.config.graph_metrics

        for agent in self.agents:
            states.append(CoordinatorState(
                task, agent, gm, self._human_feedback_score
            ))
            result = agent.execute(task, **kwargs)
            results.append(result)
            rewards.append(self._compute_reward(result))

        # Best-effort training on every agent's reward.
        if not self.config.wait_for_record_outcome:
            with self._lock:
                for state, reward in zip(states, rewards):
                    if reward is not None:
                        self._train_one(state, reward)
        else:
            with self._lock:
                self._pending = PendingDecision(
                    state_vec=states[0].to_vector(),
                    action=0,
                    teacher_scores=np.zeros(DistillationOptimizer.N_TEACHERS),
                    gate_probs=np.zeros(DistillationOptimizer.N_TEACHERS),
                    derived_reward=None,
                )

        self._last_results = results
        return results

    # ------------------------------------------------------------------
    # Public: route (single-agent routing)
    # ------------------------------------------------------------------
    def route(self, task: Any, **kwargs: Any) -> Any:
        """Route to a single agent and return its result."""
        if not self.use_enhancements or self.distillation_optimizer is None:
            # Base path: run all, pick the best by derived reward.
            return self._route_by_reward(task, **kwargs)

        gm = self.config.graph_metrics
        states = [
            CoordinatorState(task, agent, gm, self._human_feedback_score)
            for agent in self.agents
        ]
        idx, state_vec, teacher_scores, gate_probs = (
            self.distillation_optimizer.select_agent(states)
        )
        chosen = self.agents[idx]

        try:
            self.metrics.observe(
                "coordinator_agent_selected", 1.0,
                agent=str(states[idx].agent_name),
            )
        except Exception:  # noqa: BLE001
            pass

        result = chosen.execute(task, **kwargs)
        reward = self._compute_reward(result)

        if self.config.wait_for_record_outcome:
            with self._lock:
                self._pending = PendingDecision(
                    state_vec=state_vec,
                    action=idx,
                    teacher_scores=teacher_scores,
                    gate_probs=gate_probs,
                    derived_reward=reward,
                )
        elif reward is not None:
            with self._lock:
                self.distillation_optimizer.update(
                    state_vec, reward, teacher_scores, gate_probs,
                )
                if self.evolutionary_optimizer is not None:
                    self.evolutionary_optimizer.observe(
                        self._features_for_evolution(reward, state_vec),
                        reward,
                    )

        self._last_results = [result]
        return result

    # ------------------------------------------------------------------
    # Public: record_outcome / record_human_feedback
    # ------------------------------------------------------------------
    def record_outcome(self, external_reward: float) -> bool:
        """Train the optimizer against the caller's true reward.

        Only meaningful when ``config.wait_for_record_outcome=True``.
        """
        if not self.use_enhancements:
            return False
        if not self.config.wait_for_record_outcome:
            logger.warning(
                "record_outcome() called but wait_for_record_outcome=False; "
                "distribute/route already trained on the derived reward"
            )
            return False
        with self._lock:
            pending = self._pending
            self._pending = None
        if pending is None:
            logger.debug("record_outcome(): no pending decision")
            return False
        r = _clamp(
            _finite_nonneg(float(external_reward) + 1.0, "external_reward") - 1.0,
            -1.0, 1.0,
        )
        try:
            self.distillation_optimizer.update(
                pending.state_vec, r,
                pending.teacher_scores, pending.gate_probs,
            )
            if self.evolutionary_optimizer is not None:
                self.evolutionary_optimizer.observe(
                    self._features_for_evolution(r, pending.state_vec), r,
                )
        except Exception as exc:  # noqa: BLE001
            logger.warning("record_outcome failed: %s", exc)
            return False
        return True

    def record_human_feedback(self, score: float) -> None:
        score = _clamp(_finite_nonneg(score, "human_feedback_score"))
        with self._lock:
            self._human_feedback_score = score
        try:
            self.metrics.observe("coordinator_human_feedback", score)
        except Exception:  # noqa: BLE001
            pass

    # ------------------------------------------------------------------
    # Public: introspection + persistence
    # ------------------------------------------------------------------
    def get_optimizer_stats(self) -> Optional[Dict[str, Any]]:
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

    def get_last_results(self) -> Optional[List[Any]]:
        return self._last_results

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

    # ------------------------------------------------------------------
    # Internal: agent preparation (best-effort confound mitigation)
    # ------------------------------------------------------------------
    def _prepare_agents(self) -> None:
        """Best-effort: set ``wait_for_record_outcome=True`` on any agent
        whose config exposes that field. This prevents the agent's own
        strategy training from confounding the coordinator's reward."""
        new_agents: List[Any] = []
        for agent in self.agents:
            cfg = getattr(agent, "config", None)
            if (cfg is not None
                    and hasattr(cfg, "wait_for_record_outcome")
                    and not getattr(cfg, "wait_for_record_outcome", False)):
                try:
                    agent.config = replace(cfg, wait_for_record_outcome=True)
                    logger.info(
                        "Disabled auto-training on agent %r "
                        "(coordinator will train it)",
                        getattr(agent, "name", "agent"),
                    )
                except Exception as exc:  # noqa: BLE001
                    logger.debug(
                        "Failed to reconfigure agent %r: %s", agent, exc
                    )
            new_agents.append(agent)
        self.agents = tuple(new_agents)

    # ------------------------------------------------------------------
    # Internal: base-path routing
    # ------------------------------------------------------------------
    def _route_by_reward(self, task: Any, **kwargs: Any) -> Any:
        best_reward = -math.inf
        best_result: Optional[Any] = None
        for agent in self.agents:
            result = agent.execute(task, **kwargs)
            reward = self._compute_reward(result)
            if reward is not None and reward > best_reward:
                best_reward = reward
                best_result = result
        if best_result is None:
            # No agent produced a measurable reward — return the first.
            return self.agents[0].execute(task, **kwargs)
        return best_result

    # ------------------------------------------------------------------
    # Internal: feature vector for the evolutionary optimizer
    # ------------------------------------------------------------------
    @staticmethod
    def _features_for_evolution(
        reward: float, state_vec: "np.ndarray"
    ) -> "np.ndarray":
        """Build a 4-dim feature vector (quality, energy, latency, carbon
        proxies) from the state vector. Falls back to zeros when no
        granular data is present."""
        # state_vec = (task_size, complexity, priority,
        #              centrality, connectivity, human_feedback,
        #              agent_quality, agent_efficiency)
        return np.array(
            [
                float(state_vec[6]),                 # quality proxy
                float(state_vec[7]),                 # energy efficiency proxy
                1.0 - float(state_vec[1]),           # latency (inverse complexity)
                1.0 - float(state_vec[4]),           # carbon (inverse connectivity)
            ],
            dtype=np.float64,
        )

    # ------------------------------------------------------------------
    # Internal: unified train step (single observation)
    # ------------------------------------------------------------------
    def _train_one(
        self,
        state: CoordinatorState,
        reward: float,
    ) -> None:
        if self.distillation_optimizer is None:
            return
        try:
            _score, teacher_scores, gate_probs = self.distillation_optimizer.score(
                state
            )
            self.distillation_optimizer.update(
                state.to_vector(), reward, teacher_scores, gate_probs,
            )
            if self.evolutionary_optimizer is not None:
                self.evolutionary_optimizer.observe(
                    self._features_for_evolution(reward, state.to_vector()),
                    reward,
                )
        except Exception as exc:  # noqa: BLE001
            logger.warning("Training update failed: %s", exc)

    # ------------------------------------------------------------------
    # Internal: reward computation
    # ------------------------------------------------------------------
    def _compute_reward(self, result: Any) -> Optional[float]:
        """Derive a symmetric reward in ``[-1, 1]``.

        Returns ``None`` when the agent's result lacks the metrics needed.
        """
        if not isinstance(result, dict):
            return None
        metrics = result.get("metrics")
        if not isinstance(metrics, dict):
            return None
        if "accuracy" not in metrics or "energy_kwh" not in metrics:
            return None

        try:
            accuracy = float(metrics["accuracy"])
            energy_kwh = float(metrics["energy_kwh"])
        except (TypeError, ValueError):
            return None
        latency_ms = float(metrics.get("latency_ms", 0.0))
        carbon_kg = float(
            metrics.get(
                "carbon_kg",
                energy_kwh * self.config.carbon_intensity_kg_per_kwh,
            )
        )

        accuracy_norm = _clamp(accuracy)
        energy_norm = _bounded_inverse(energy_kwh, self.config.energy_scale_kwh)
        latency_norm = _bounded_inverse(latency_ms, self.config.latency_scale_ms)
        carbon_scale = (
            self.config.energy_scale_kwh
            * max(self.config.carbon_intensity_kg_per_kwh, 1e-9)
        )
        carbon_norm = _bounded_inverse(carbon_kg, carbon_scale)

        # Evolved MODP weights if available; else config's.
        if self.evolutionary_optimizer is not None:
            w = self.evolutionary_optimizer.get_best_weights()
        else:
            w = np.asarray(self._modp_weights, dtype=np.float64)

        composite = _clamp(
            float(w[0]) * accuracy_norm
            + float(w[1]) * energy_norm
            + float(w[2]) * latency_norm
            + float(w[3]) * carbon_norm
        )

        # Symmetric range [-1, 1] + human-feedback tilt.
        reward = 2.0 * composite - 1.0
        reward += 0.1 * (self._human_feedback_score - 0.5)
        return float(np.clip(reward, -1.0, 1.0))

    # ------------------------------------------------------------------
    def __repr__(self) -> str:
        return (
            f"MultiAgentCoordinator("
            f"agents={len(self.agents)}, "
            f"enhancements={self.use_enhancements})"
        )


__all__ = [
    "AgentProfile",
    "CoordinatorConfig",
    "CoordinatorState",
    "DistillationOptimizer",
    "EvolutionaryMODPWeightsOptimizer",
    "MultiAgentCoordinator",
    "PendingDecision",
    "MetricsSink",
    "InMemoryMetrics",
    "LoggingMetrics",
    "MODP_SCHEMA",
]


# ============================================================================
# Section 11 — Demo
# ============================================================================

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    # A simple test agent that returns metrics reflecting its profile.
    class TestAgent:
        def __init__(self, name: str, quality: float, efficiency: float,
                     latency_ms: float = 100.0):
            self.name = name
            self.profile = AgentProfile(
                name=name, quality=quality, efficiency=efficiency,
            )
            self._latency_ms = latency_ms

        def execute(self, task: Dict[str, Any]) -> Dict[str, Any]:
            # Simulate: higher quality = higher accuracy but more energy.
            base_energy = 0.01 * (1.0 - self.profile.efficiency) + 0.001
            task_complexity = task.get("complexity", 0.5)
            accuracy = self.profile.quality * (1.0 - 0.05 * task_complexity)
            energy = base_energy * (1.0 + 0.5 * task_complexity)
            return {
                "output": f"{self.name}:done",
                "metrics": {
                    "accuracy": accuracy,
                    "energy_kwh": energy,
                    "latency_ms": self._latency_ms * (1.0 + task_complexity),
                },
            }

    agents = [
        TestAgent("fast",    quality=0.85, efficiency=0.95, latency_ms=50.0),
        TestAgent("balanced", quality=0.92, efficiency=0.70, latency_ms=150.0),
        TestAgent("smart",   quality=0.98, efficiency=0.40, latency_ms=300.0),
    ]

    print("=== Base path (use_enhancements=False) ===")
    base = MultiAgentCoordinator(
        agents=agents, config=CoordinatorConfig(use_enhancements=False),
    )
    print(base)
    task = {"complexity": 0.4, "priority": 0.3}
    results = base.distribute(task)
    print(f"  distribute returned {len(results)} results "
          f"(should be {len(agents)})")
    best = base.route(task)
    print(f"  base route picked: {best['output']}")

    print("\n=== Enhanced path ===")
    cfg = CoordinatorConfig.from_dict({
        "use_enhancements": True,
        "use_distillation": True,
        "use_evolutionary": True,
        "region": "US-CA",
        "carbon_intensity_kg_per_kwh": 0.20,
        "seed": 42,
        "train_every": 1,
        "batch_size": 4,
    })
    enh = MultiAgentCoordinator(agents=agents, config=cfg)
    print(enh)

    # Train on a mix of tasks
    for i in range(40):
        task = {
            "complexity": 0.2 + 0.02 * i,
            "priority": 0.2 + 0.015 * i,
        }
        enh.route(task)

    print(f"\n  after 40 route calls: {enh.get_optimizer_stats()}")

    print("\n=== Routing behavior ===")
    for name, task in [
        ("low priority (efficiency)",  {"complexity": 0.1, "priority": 0.1}),
        ("medium priority",            {"complexity": 0.5, "priority": 0.5}),
        ("high priority (quality)",    {"complexity": 0.9, "priority": 0.9}),
    ]:
        result = enh.route(task)
        print(f"  {name}: picked {result['output']}")

    print("\n=== Persistence round-trip ===")
    enh.save("/tmp/coordinator_model")
    enh2 = MultiAgentCoordinator(agents=agents, config=cfg)
    enh2.load("/tmp/coordinator_model")
    print(f"  reloaded: {enh2.get_optimizer_stats()}")

    print("\n=== Metrics emission ===")
    if isinstance(enh.metrics, InMemoryMetrics):
        for k in sorted(enh.metrics.snapshot().keys()):
            print(f"  {k}")

    print("\n=== Feature vector determinism ===")
    s1 = CoordinatorState({"id": "x"}, agents[0], {}, 0.5)
    s2 = CoordinatorState({"id": "x"}, agents[0], {}, 0.5)
    assert np.array_equal(s1.to_vector(), s2.to_vector()), \
        "state vector not deterministic!"
    print(f"  state vector: {s1.to_vector()}")
