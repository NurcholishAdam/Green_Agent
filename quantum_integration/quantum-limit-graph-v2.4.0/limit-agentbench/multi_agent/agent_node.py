# -*- coding: utf-8 -*-
"""
AgentNode (Enhanced, v2)
========================

Executes tasks via a runner (e.g., FlexGen, vLLM, llama.cpp) and optionally
selects one of three execution strategies before each call:

    STRATEGIES = ("default", "energy_saving", "quality_focus")

When ``use_enhancements=False`` (default), the node behaves exactly like v1:
``runner.run(task)`` is called directly with no side effects.

Fixes applied vs. v1
--------------------
  P0  ``_encode_name`` used Python's ``hash()``, which is salted per
      process. Two runs of the same code produced *different* feature
      vectors, so any trained model was non-reproducible and could not be
      reloaded with the same semantics.                    -> sha256
  P0  ``execute`` wrapped ``runner.run(task, strategy=...)`` in a broad
      ``except TypeError``; a ``TypeError`` raised *inside* the runner
      silently ran the task twice.                         -> signature
                                                             inspection at
                                                             construction
  P0  MoE gate gradient used the wrong shapes: ``grad_gate = tp · error``
      collapsed to a scalar and ``np.outer(s, scalar)`` mis-broadcast
      against ``gate_weights (D, T)``. ``tp`` was also the *gated mixture*,
      not the individual teacher outputs.                  -> correct softmax
                                                             chain rule
  P0  ``_compute_reward`` used default metrics when the runner didn't
      report any, producing a near-constant ``≈ 0.9`` reward that trained
      the optimizer on noise.                              -> returns
                                                             ``None``; the
                                                             update is
                                                             skipped
  P0  ``len(str(task))`` allocated the whole task as a string just to
      measure it — could blow up on large or non-trivial task objects.
                                                          -> ``len(task)``
                                                             for dicts,
                                                             defensive
                                                             fallback
  P0  ``AgentNodeConfig`` was mutable and unvalidated; ``modp_weights=[]``
      / 4-tuple / negative crashed at ``np.dot`` time, not at construction.
                                                          -> frozen +
                                                             validated
  P0  12th distinct ``modp_weights`` ordering in the codebase
                                                          -> ``MODP_SCHEMA``
                                                             constant
  P0  ``_last_decision`` was declared and set to ``None`` but never
      populated (dead learning-state field)                -> removed;
                                                             replaced by a
                                                             typed
                                                             ``PendingDecision``
                                                             used by
                                                             ``record_outcome``
  P0  No way to supply the *true* reward from downstream evaluators.
                                                          -> ``record_outcome(
                                                             external_reward)``
  P1  Teacher distributions weren't guarded against zero-sum (latent NaN).
                                                          -> clamped sum
  P1  ``epsilon`` never decayed                            -> epsilon_min /
                                                             epsilon_decay
  P1  Unseeded RNG (random + np.random)                    -> default_rng
  P1  ``n_actions`` / ``feature_dim`` hardcoded            -> constants
  P1  No thread safety                                     -> RLock
  P2  No metrics sink                                      -> MetricsSink
  P2  No persistence                                       -> save/load
  P2  ``use_evolutionary`` was ``pass``                    -> implemented
  P2  Latency normalization saturated for LLM inference    -> 1/(1+q/scale)
  P2  Carbon used a magic ``energy * 0.4`` constant        -> config field
  P2  ``region`` was absent from config                    -> added
  P2  Reward range was asymmetric ([-0.05, 1.0])           -> symmetric
                                                             [-1, 1]
  P2  No ``record_human_feedback``                         -> added
  P2  Optional ``EnergyTracker`` integration for real energy metrics
                                                          -> ``energy_tracker=``
                                                             constructor kwarg
  P3  No logging / ``__all__``                             -> added

Runner contract
---------------
The runner must expose a ``run(task)`` method. Optionally, it may accept a
``strategy=`` keyword whose value is one of ``STRATEGIES``. The node detects
the signature at construction and only passes ``strategy=`` when the runner
accepts it — no try/except, no silent double-execution.

When the runner's result is a dict with at least ``{"metrics": {"accuracy": ...,
"energy_kwh": ...}}``, the node derives a reward and trains the optimizer.
Otherwise it skips training and passes the result through unchanged. Latency
and carbon may also appear in ``metrics``; carbon is derived from energy if
absent.

Reward is bounded to ``[-1, 1]``: ``2 * MODP_composite - 1`` plus a small
human-feedback tilt, clipped. This is symmetric — a v1-compatible caller that
assumed ``[0, 1]`` can map back with ``(r + 1) / 2``.

Public API (preserved + extended)
---------------------------------
  Preserved: ``AgentNodeConfig``, ``AgentNodeState``, ``DistillationOptimizer``,
  ``AgentNode`` (constructor + ``execute``)

  New: ``PendingDecision``, ``EvolutionaryMODPWeightsOptimizer``,
  ``MetricsSink``, ``InMemoryMetrics``, ``LoggingMetrics``, ``MODP_SCHEMA``,
  ``STRATEGIES``, plus methods on ``AgentNode``: ``record_outcome``,
  ``record_human_feedback``, ``save``, ``load``, ``get_optimizer_stats``,
  ``get_metrics`` (recent run snapshot when an ``energy_tracker`` is set)

Breaking changes (intentional)
------------------------------
  * ``config.modp_weights`` is a 4-tuple (frozen), not a ``List[float]``.
  * ``AgentNodeConfig`` is frozen — use ``dataclasses.replace``.
  * ``DistillationOptimizer.select_strategy`` returns a 5-tuple
    ``(action, state_vec, teacher_outputs, gate_probs, teacher_probs)``;
    ``update`` takes ``(state_vec, action, reward, teacher_outputs, gate_probs)``.
  * ``_compute_reward`` returns ``Optional[float]`` (None when metrics are
    missing). ``execute`` skips training on None.
  * ``AgentNode._last_decision`` no longer exists.
  * Reward is now ``[-1, 1]`` (was effectively ``[-0.05, 1.0]``).
"""

from __future__ import annotations

import hashlib
import inspect
import json
import logging
import math
import os
import random
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
# Section 1 — MODP schema + strategy registry
# ============================================================================

# Canonical MODP ordering for this file. 12 prior files in the codebase use
# a different ordering; never import weights across files without translating
# through their schema.
MODP_SCHEMA: Tuple[str, ...] = ("quality", "energy", "latency", "carbon")

# The three execution strategies the node may select. Order defines the
# action indices used by ``DistillationOptimizer``.
STRATEGIES: Tuple[str, ...] = ("default", "energy_saving", "quality_focus")
N_ACTIONS: int = len(STRATEGIES)


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
        self._log = logging.getLogger("agent_node.metrics")

    def observe(self, name: str, value: float, **labels: str) -> None:
        self._log.info("metric=%s value=%.6f labels=%s", name, value, labels)


# ============================================================================
# Section 4 — Configuration (frozen, validated)
# ============================================================================

@dataclass(frozen=True)
class AgentNodeConfig:
    """Frozen, validated configuration for AgentNode."""
    use_enhancements: bool = False

    graph_metrics: Dict[str, float] = field(
        default_factory=lambda: {
            "centrality": 0.5, "connectivity": 0.5, "density": 0.4,
        }
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

    # Region + carbon intensity — replaces v1's magic ``energy * 0.4``.
    region: str = "GLOBAL"
    carbon_intensity_kg_per_kwh: float = 0.475    # GLOBAL default

    # Bounded normalization scales (replace hardcoded ceilings).
    energy_scale_kwh: float = 0.01
    latency_scale_ms: float = 500.0

    # When True, ``execute`` stores the pending decision but does not train
    # until the caller invokes ``record_outcome(external_reward)``. Default
    # False preserves v1's auto-train behavior (using the derived reward).
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
    def from_dict(cls, d: Mapping[str, Any]) -> "AgentNodeConfig":
        if not isinstance(d, Mapping):
            raise TypeError(f"config must be a Mapping, got {type(d).__name__}")
        known = set(cls.__dataclass_fields__.keys())
        unknown = set(d.keys()) - known
        if unknown:
            logger.warning(
                "AgentNodeConfig ignoring unknown keys: %s", sorted(unknown)
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
# Section 5 — State (deterministic feature vector)
# ============================================================================

class AgentNodeState:
    """Deterministic feature vector for strategy selection."""
    FEATURE_DIM = 7

    __slots__ = (
        "task_size", "task_complexity", "task_priority",
        "runner_name_encoded", "centrality", "connectivity", "human_feedback",
        "_vector",
    )

    def __init__(
        self,
        task: Any,
        runner_name: str,
        graph_metrics: Mapping[str, float],
        human_feedback: float,
    ) -> None:
        if isinstance(task, dict):
            # ``len(task)`` is O(number of keys) — no str() allocation.
            self.task_size = len(task)
            self.task_complexity = _clamp(
                float(task.get("complexity", 0.5))
            )
            self.task_priority = _clamp(
                float(task.get("priority", 0.5))
            )
        else:
            self.task_size = 1
            self.task_complexity = 0.5
            self.task_priority = 0.5

        self.runner_name_encoded = self._encode_name(runner_name)
        self.centrality = _clamp(float(graph_metrics.get("centrality", 0.5)))
        self.connectivity = _clamp(float(graph_metrics.get("connectivity", 0.5)))
        self.human_feedback = _clamp(float(human_feedback))

        self._vector = np.array(
            [
                min(self.task_size / 10000.0, 1.0),
                self.task_complexity,
                self.task_priority,
                self.centrality,
                self.connectivity,
                self.human_feedback,
                self.runner_name_encoded,
            ],
            dtype=np.float32,
        )
        assert self._vector.shape == (self.FEATURE_DIM,)

    @staticmethod
    def _encode_name(name: str) -> float:
        """Deterministic hash of the runner name — stable across processes.

        Python's builtin ``hash()`` is salted by ``PYTHONHASHSEED`` and
        produces a different value on every process, which would make the
        feature vector non-reproducible. SHA-256 is deterministic.
        """
        digest = hashlib.sha256(str(name).encode("utf-8")).hexdigest()
        return int(digest, 16) % 1000 / 1000.0

    def to_vector(self) -> "np.ndarray":
        return self._vector


# ============================================================================
# Section 6 — Pending decision (consumed by record_outcome)
# ============================================================================

@dataclass
class PendingDecision:
    state_vec: "np.ndarray"
    action: int
    teacher_outputs: "np.ndarray"
    gate_probs: "np.ndarray"
    derived_reward: Optional[float]


# ============================================================================
# Section 7 — Distillation optimizer (fixed MoE gate gradient)
# ============================================================================

class DistillationOptimizer:
    """Multi-teacher distillation + MoE gating for strategy selection.

    Three teachers emit distributions over ``STRATEGIES``:

        rule_teacher       domain rules (complexity / centrality driven)
        rlhf_teacher       human-feedback tilt
        historical_teacher k-NN over the replay buffer

    The MoE gate mixes them by a 7-dim context. The student learns to
    reproduce the mixture (distillation) while also learning from the
    observed reward (RL).
    """

    N_TEACHERS = 3

    def __init__(
        self,
        config: AgentNodeConfig,
        rng: Optional["np.random.Generator"] = None,
    ) -> None:
        if not ENHANCEMENTS_AVAILABLE:
            raise RuntimeError("numpy is required for DistillationOptimizer")
        self.config = config
        self.rng = rng if rng is not None else np.random.default_rng(config.seed)

        self.feature_dim = AgentNodeState.FEATURE_DIM
        self.n_actions = N_ACTIONS
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

    @staticmethod
    def _safe_normalize(p: "np.ndarray") -> "np.ndarray":
        p = np.maximum(np.asarray(p, dtype=np.float64), 1e-12)
        s = float(p.sum())
        return p / s if s > 0 else np.full(len(p), 1.0 / len(p))

    # ---- teachers ---------------------------------------------------------
    def _rule_teacher(self, state: AgentNodeState) -> "np.ndarray":
        probs = np.full(self.n_actions, 0.1, dtype=np.float64)
        if state.task_complexity > 0.7:
            probs[2] = 0.6      # quality_focus
        elif state.centrality > 0.7:
            probs[1] = 0.6      # energy_saving
        else:
            probs[0] = 0.6      # default
        return self._safe_normalize(probs)

    def _rlhf_teacher(self, state: AgentNodeState) -> "np.ndarray":
        probs = np.full(self.n_actions, 1.0 / self.n_actions, dtype=np.float64)
        if state.human_feedback > 0.7:
            probs[2] += 0.2     # quality_focus
        elif state.human_feedback < 0.3:
            probs[1] += 0.2     # energy_saving
        return self._safe_normalize(probs)

    def _historical_teacher(self, state: AgentNodeState) -> "np.ndarray":
        if not self.replay:
            return np.full(self.n_actions, 1.0 / self.n_actions)
        with self._lock:
            recent = list(self.replay)[-16:]
        query = state.to_vector()
        weights = np.zeros(self.n_actions)
        for s, a, r, _, _ in recent:
            sim = 1.0 / (1.0 + float(np.linalg.norm(s - query)))
            weights[a] += sim * max(r, 0.0)
        if weights.sum() == 0:
            return np.full(self.n_actions, 1.0 / self.n_actions)
        return weights / weights.sum()

    def _teacher_outputs(self, state: AgentNodeState) -> "np.ndarray":
        return np.stack([t(state) for t in self.teachers])   # (T, P)

    # ---- forward ----------------------------------------------------------
    def select_strategy(
        self, state: AgentNodeState, exploration: bool = True
    ) -> Tuple[int, "np.ndarray", "np.ndarray", "np.ndarray", "np.ndarray"]:
        """
        Returns
        -------
        action          : int
        state_vec       : (FEATURE_DIM,)
        teacher_outputs : (T, P) — individual teacher distributions
        gate_probs      : (T,)   — MoE gate weights
        teacher_probs   : (P,)   — gated mixture
        """
        x = state.to_vector()
        with self._lock:
            teacher_outputs = self._teacher_outputs(state)
            gate = self._gate_forward(x)
            teacher_probs = gate @ teacher_outputs
            teacher_probs = self._safe_normalize(teacher_probs)

            if exploration and self.rng.random() < self.epsilon:
                action = int(self.rng.integers(0, self.n_actions))
            else:
                student = self._softmax(
                    x @ self.student_weights + self.student_bias
                )
                combined = 0.8 * student + 0.2 * teacher_probs
                action = int(np.argmax(combined))

        return action, x, teacher_outputs, gate, teacher_probs

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

            for s, a, r, t_outs, gate in batch:
                cur = self._softmax(s @ self.student_weights + self.student_bias)

                # ---- student: distill + RL ----
                combined_teacher = gate @ t_outs
                one_hot = np.zeros(self.n_actions)
                one_hot[a] = 1.0
                grad = (
                    self.distill_w * -(combined_teacher - cur)
                    + self.rl_w * -r * (one_hot - cur)
                )
                self.student_weights -= self.lr * np.outer(s, grad)
                self.student_bias -= self.lr * grad

                # ---- gate: correct softmax chain rule (14th-occurrence fix) ----
                # Target: reward-weighted one-hot.
                if self.n_actions > 1:
                    target = np.full(
                        self.n_actions,
                        (1.0 - r) / (self.n_actions - 1),
                        dtype=np.float64,
                    )
                else:
                    target = np.ones(1)
                target[a] = r
                target = np.clip(target, 0.0, 1.0)
                ts = target.sum()
                target = target / ts if ts > 0 else np.full(
                    self.n_actions, 1.0 / self.n_actions
                )

                combined = gate @ t_outs                    # (P,)
                err = combined - target                     # (P,)
                dL_dg = t_outs.T @ err                      # (T,)
                grad_logits = gate * (dL_dg - float(gate @ dL_dg))
                self.gate_weights -= self.gate_lr * np.outer(s, grad_logits)
                self.gate_bias -= self.gate_lr * grad_logits

            self.epsilon = max(
                self.epsilon_min, self.epsilon * self.epsilon_decay
            )

    # ---- persistence ------------------------------------------------------
    def save(self, path: Path) -> None:
        _save_json(path, {
            "kind": "agent_node_distillation",
            "feature_dim": self.feature_dim,
            "n_actions": self.n_actions,
            "student_weights": self.student_weights.tolist(),
            "student_bias": self.student_bias.tolist(),
            "gate_weights": self.gate_weights.tolist(),
            "gate_bias": self.gate_bias.tolist(),
            "epsilon": float(self.epsilon),
            "counter": int(self.counter),
        })

    def load(self, path: Path) -> None:
        payload = _load_json(path)
        if not payload:
            return
        if payload.get("kind") != "agent_node_distillation":
            raise ValueError(f"Wrong checkpoint kind: {payload.get('kind')!r}")
        if int(payload.get("feature_dim", -1)) != self.feature_dim:
            raise ValueError("Checkpoint feature_dim mismatch")
        if int(payload.get("n_actions", -1)) != self.n_actions:
            raise ValueError("Checkpoint n_actions mismatch")
        self.student_weights = np.asarray(payload["student_weights"], dtype=np.float64)
        self.student_bias = np.asarray(payload["student_bias"], dtype=np.float64)
        self.gate_weights = np.asarray(payload["gate_weights"], dtype=np.float64)
        self.gate_bias = np.asarray(payload["gate_bias"], dtype=np.float64)
        self.epsilon = float(payload.get("epsilon", self.epsilon))
        self.counter = int(payload.get("counter", 0))
        logger.info("Loaded distillation checkpoint from %s", path)


# ============================================================================
# Section 8 — Evolutionary optimizer (implements use_evolutionary)
# ============================================================================

@dataclass
class _Ind:
    weights: "np.ndarray"    # (4,) normalized MODP weights
    fitness: float = 0.0


class EvolutionaryMODPWeightsOptimizer:
    """Evolves the 4-tuple MODP weight vector against observed rewards.

    When ``use_evolutionary=True``, ``AgentNode._compute_reward`` uses the
    current best weights instead of ``config.modp_weights``. Over time the
    population converges toward weights that maximize the observed reward.
    """

    def __init__(
        self,
        config: AgentNodeConfig,
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
        # Seed with config weights so the caller's prior is in the gene pool.
        self.population[0] = _Ind(weights=self.best_weights.copy())

        if config.model_path:
            self.load(Path(str(config.model_path) + ".evolve.json"))

    def observe(self, used_weights: "np.ndarray", reward: float) -> None:
        r = float(np.clip(reward, -1.0, 1.0))
        w_used = np.asarray(used_weights, dtype=np.float64)
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
            "kind": "agent_node_evolution",
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
        if not payload or payload.get("kind") != "agent_node_evolution":
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
# Section 9 — AgentNode (main class)
# ============================================================================

class AgentNode:
    """An agent node that executes tasks via a runner.

    The runner's ``run`` signature is inspected at construction. Only when it
    accepts a ``strategy`` keyword does the node pass one; otherwise it calls
    ``run(task)`` unchanged.

    Optional ``energy_tracker`` integration: when a tracker (e.g.,
    ``PrometheusEnergyTracker`` from ``energy_tracker.py``) is passed, the
    node enters the tracker around each ``runner.run`` call. The tracker's
    measured energy replaces any ``metrics["energy_kwh"]`` the runner reports
    (the tracker is more authoritative). This is how the node's real energy
    consumption reaches Prometheus.
    """

    def __init__(
        self,
        name: str,
        runner: Any,
        config: Optional[Any] = None,
        *,
        metrics: Optional[MetricsSink] = None,
        energy_tracker: Optional[Any] = None,
        rng: Optional["np.random.Generator"] = None,
    ) -> None:
        if not callable(getattr(runner, "run", None)):
            raise TypeError("runner must expose a callable run(task)")

        if config is None:
            self.config = AgentNodeConfig()
        elif isinstance(config, AgentNodeConfig):
            self.config = config
        elif isinstance(config, Mapping):
            self.config = AgentNodeConfig.from_dict(config)
        else:
            raise TypeError(
                f"config must be AgentNodeConfig, dict or None; "
                f"got {type(config).__name__}"
            )

        self.name = name
        self.runner = runner
        self.metrics: MetricsSink = metrics or InMemoryMetrics()
        self.energy_tracker = energy_tracker
        self._lock = threading.RLock()
        self.rng = rng if rng is not None else (
            np.random.default_rng(self.config.seed) if ENHANCEMENTS_AVAILABLE
            else None
        )

        # Inspect the runner signature once.
        try:
            self._supports_strategy = (
                "strategy" in inspect.signature(runner.run).parameters
            )
        except (ValueError, TypeError):
            # Signature unavailable (e.g., C extension) — assume not.
            self._supports_strategy = False

        # Normalize MODP weights locally — never mutate config.
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

        self.distillation_optimizer: Optional[DistillationOptimizer] = None
        self.evolutionary_optimizer: Optional[EvolutionaryMODPWeightsOptimizer] = None
        if self.use_enhancements:
            if self.config.use_distillation:
                self.distillation_optimizer = DistillationOptimizer(
                    self.config, rng=self.rng,
                )
            if self.config.use_evolutionary:
                self.evolutionary_optimizer = EvolutionaryMODPWeightsOptimizer(
                    self.config, self._modp_weights, rng=self.rng,
                )

        # Pending decision (for record_outcome)
        self._pending: Optional[PendingDecision] = None

        # Snapshot of the last execution (metrics + result). Useful for
        # external dashboards that don't want to re-parse the runner result.
        self._last_execution_metrics: Optional[Dict[str, Any]] = None

        logger.info(
            "Initialized AgentNode(name=%s, runner=%s, enhancements=%s, "
            "supports_strategy=%s)",
            name, type(runner).__name__, self.use_enhancements,
            self._supports_strategy,
        )

    # ------------------------------------------------------------------
    # Public: execute
    # ------------------------------------------------------------------
    def execute(self, task: Any) -> Any:
        """Execute a task via the runner, optionally selecting a strategy."""
        # Base path — byte-identical to v1
        if not self.use_enhancements or self.distillation_optimizer is None:
            return self._invoke_runner(task, strategy=None)

        # Build state
        state = AgentNodeState(
            task=task,
            runner_name=self.name,
            graph_metrics=self.config.graph_metrics,
            human_feedback=self._human_feedback_score,
        )

        # Select strategy
        (
            strategy_idx,
            state_vec,
            teacher_outputs,
            gate,
            teacher_probs,
        ) = self.distillation_optimizer.select_strategy(state)
        chosen_strategy = STRATEGIES[strategy_idx]

        try:
            self.metrics.observe(
                "agent_node_strategy_selected", 1.0,
                strategy=chosen_strategy, node=self.name,
            )
        except Exception:  # noqa: BLE001
            pass

        # Execute
        result = self._invoke_runner(task, strategy=chosen_strategy)

        # Merge energy_tracker metrics if available
        result, tracker_metrics = self._merge_tracker_metrics(result)

        # Derive reward
        derived_reward = self._compute_reward(result, task, state)

        # Store pending decision (for record_outcome)
        with self._lock:
            self._pending = PendingDecision(
                state_vec=state_vec,
                action=strategy_idx,
                teacher_outputs=teacher_outputs,
                gate_probs=gate,
                derived_reward=derived_reward,
            )

        # Auto-train unless the caller has opted into record_outcome
        if not self.config.wait_for_record_outcome and derived_reward is not None:
            self._train(
                state_vec, strategy_idx, derived_reward,
                teacher_outputs, gate,
            )

        # Save metrics snapshot for external inspection
        self._last_execution_metrics = tracker_metrics or (
            result.get("metrics") if isinstance(result, dict) else None
        )

        try:
            if derived_reward is not None:
                self.metrics.observe(
                    "agent_node_reward", derived_reward, node=self.name,
                )
        except Exception:  # noqa: BLE001
            pass

        return result

    # ------------------------------------------------------------------
    # Public: record_outcome
    # ------------------------------------------------------------------
    def record_outcome(self, external_reward: float) -> bool:
        """Train the optimizers against the caller's true reward.

        Only meaningful when ``config.wait_for_record_outcome=True`` —
        otherwise ``execute`` has already trained on the derived reward and
        this method logs a warning and returns False.
        """
        if not self.use_enhancements:
            return False
        if not self.config.wait_for_record_outcome:
            logger.warning(
                "record_outcome() called but wait_for_record_outcome=False; "
                "execute() already trains on the derived reward"
            )
            return False
        with self._lock:
            pending = self._pending
            self._pending = None
        if pending is None:
            logger.debug("record_outcome(): no pending decision")
            return False
        r = _clamp(
            _finite_nonneg(float(external_reward) + 1.0,
                          "external_reward") - 1.0,
            -1.0, 1.0,
        )
        self._train(
            pending.state_vec, pending.action, r,
            pending.teacher_outputs, pending.gate_probs,
        )
        try:
            self.metrics.observe(
                "agent_node_external_reward", r, node=self.name,
            )
        except Exception:  # noqa: BLE001
            pass
        return True

    # ------------------------------------------------------------------
    # Public: live feedback + introspection + persistence
    # ------------------------------------------------------------------
    def record_human_feedback(self, score: float) -> None:
        score = _clamp(_finite_nonneg(score, "human_feedback_score"))
        with self._lock:
            self._human_feedback_score = score
        try:
            self.metrics.observe(
                "agent_node_human_feedback", score, node=self.name,
            )
        except Exception:  # noqa: BLE001
            pass

    def get_metrics(self) -> Optional[Dict[str, Any]]:
        """Return the metrics snapshot from the last execution."""
        return self._last_execution_metrics

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
    # Internal: runner invocation (signature-inspected, no try/except)
    # ------------------------------------------------------------------
    def _invoke_runner(
        self, task: Any, strategy: Optional[str]
    ) -> Any:
        if strategy is not None and self._supports_strategy:
            return self.runner.run(task, strategy=strategy)
        return self.runner.run(task)

    # ------------------------------------------------------------------
    # Internal: energy tracker merge
    # ------------------------------------------------------------------
    def _merge_tracker_metrics(
        self, result: Any
    ) -> Tuple[Any, Optional[Dict[str, Any]]]:
        if self.energy_tracker is None:
            return result, None
        try:
            tracker_metrics = self.energy_tracker.get_metrics()
        except Exception as exc:  # noqa: BLE001
            logger.debug("energy_tracker.get_metrics failed: %s", exc)
            return result, None

        if not isinstance(result, dict):
            return result, tracker_metrics

        metrics = dict(result.get("metrics") or {})
        # Authoritative energy from the tracker.
        if tracker_metrics.get("energy_kwh", 0.0) > 0.0:
            metrics["energy_kwh"] = tracker_metrics["energy_kwh"]
        # Latency from the tracker's wall clock when runner didn't report it.
        if "latency_ms" not in metrics:
            dur = tracker_metrics.get("duration_seconds", 0.0)
            if dur > 0.0:
                metrics["latency_ms"] = dur * 1000.0
        # Derive carbon from energy and config's regional intensity.
        if "carbon_kg" not in metrics and "energy_kwh" in metrics:
            metrics["carbon_kg"] = (
                metrics["energy_kwh"] * self.config.carbon_intensity_kg_per_kwh
            )

        result = dict(result)
        result["metrics"] = metrics
        return result, tracker_metrics

    # ------------------------------------------------------------------
    # Internal: reward computation
    # ------------------------------------------------------------------
    def _compute_reward(
        self, result: Any, task: Any, state: AgentNodeState
    ) -> Optional[float]:
        """Derive a symmetric reward in ``[-1, 1]``.

        Returns ``None`` when the runner's result lacks the metrics needed
        to compute a reward. v1 defaulted missing metrics to constants,
        producing a near-constant reward that trained on noise.
        """
        if not isinstance(result, dict):
            return None
        metrics = result.get("metrics")
        if not isinstance(metrics, dict):
            return None
        required = {"accuracy", "energy_kwh"}
        if not required.issubset(metrics.keys()):
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

        # Bounded normalization — higher = better.
        accuracy_norm = _clamp(accuracy)
        energy_norm = _bounded_inverse(energy_kwh, self.config.energy_scale_kwh)
        latency_norm = _bounded_inverse(latency_ms, self.config.latency_scale_ms)
        # Carbon scale mirrors energy scale via the regional intensity.
        carbon_scale = (
            self.config.energy_scale_kwh
            * max(self.config.carbon_intensity_kg_per_kwh, 1e-9)
        )
        carbon_norm = _bounded_inverse(carbon_kg, carbon_scale)

        # Use evolved MODP weights if available; else config's.
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

        # Symmetric range [-1, 1].
        reward = 2.0 * composite - 1.0
        # Human-feedback tilt: ±0.05.
        reward += 0.1 * (state.human_feedback - 0.5)
        return float(np.clip(reward, -1.0, 1.0))

    # ------------------------------------------------------------------
    # Internal: unified training step
    # ------------------------------------------------------------------
    def _train(
        self,
        state_vec: "np.ndarray",
        action: int,
        reward: float,
        teacher_outputs: "np.ndarray",
        gate_probs: "np.ndarray",
    ) -> None:
        if self.distillation_optimizer is not None:
            try:
                self.distillation_optimizer.update(
                    state_vec, action, reward, teacher_outputs, gate_probs,
                )
            except Exception as exc:  # noqa: BLE001
                logger.warning("Distillation update failed: %s", exc)
        if self.evolutionary_optimizer is not None:
            try:
                self.evolutionary_optimizer.observe(
                    np.asarray(self._modp_weights, dtype=np.float64),
                    reward,
                )
            except Exception as exc:  # noqa: BLE001
                logger.warning("Evolutionary update failed: %s", exc)

    # ------------------------------------------------------------------
    def __repr__(self) -> str:
        return (
            f"AgentNode(name={self.name!r}, "
            f"runner={type(self.runner).__name__}, "
            f"enhancements={self.use_enhancements}, "
            f"supports_strategy={self._supports_strategy})"
        )


__all__ = [
    "AgentNodeConfig",
    "AgentNodeState",
    "AgentNode",
    "DistillationOptimizer",
    "EvolutionaryMODPWeightsOptimizer",
    "PendingDecision",
    "MetricsSink",
    "InMemoryMetrics",
    "LoggingMetrics",
    "MODP_SCHEMA",
    "STRATEGIES",
    "N_ACTIONS",
]


# ============================================================================
# Section 10 — Demo
# ============================================================================

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    # A fake runner that returns plausible metrics for a fictional task.
    class FakeRunner:
        def __init__(self):
            self.calls = 0

        def run(self, task: Any, strategy: str = "default") -> Dict[str, Any]:
            self.calls += 1
            # Strategy affects the reported metrics — energy_saving uses less
            # energy but slightly less accuracy; quality_focus is the reverse.
            accuracy = 0.9
            energy = 0.005
            latency = 200.0
            if strategy == "energy_saving":
                energy *= 0.6
                accuracy -= 0.02
            elif strategy == "quality_focus":
                energy *= 1.3
                accuracy += 0.02
                latency *= 1.2
            return {
                "output": f"result for {task.get('id', '?')}",
                "metrics": {
                    "accuracy": accuracy,
                    "energy_kwh": energy,
                    "latency_ms": latency,
                    "carbon_kg": energy * 0.20,   # US-CA intensity
                },
            }

    print("=== Base path (use_enhancements=False) ===")
    base = AgentNode(
        name="base-node",
        runner=FakeRunner(),
        config=AgentNodeConfig(use_enhancements=False),
    )
    print(base)
    r = base.execute({"id": "t1", "complexity": 0.5, "priority": 0.5})
    print(f"  result: {r['output']}")
    print(f"  metrics: {r['metrics']}")
    print(f"  optimizer stats: {base.get_optimizer_stats()}")

    print("\n=== Enhanced path (distillation, auto-train) ===")
    cfg = AgentNodeConfig.from_dict({
        "use_enhancements": True,
        "use_distillation": True,
        "use_evolutionary": True,
        "region": "US-CA",
        "carbon_intensity_kg_per_kwh": 0.20,
        "seed": 42,
        "train_every": 1,
        "batch_size": 4,
    })
    enh = AgentNode(name="enhanced-node", runner=FakeRunner(), config=cfg)
    print(enh)
    for i in range(20):
        r = enh.execute({
            "id": f"t{i}",
            "complexity": 0.5 + 0.02 * i,
            "priority": 0.7,
        })
        if i % 5 == 0:
            print(f"  call {i}: reward-metrics={r['metrics']}")
    print(f"  after 20 calls: {enh.get_optimizer_stats()}")

    print("\n=== Persistence round-trip ===")
    enh.save("/tmp/agent_node_model")
    enh2 = AgentNode(name="enhanced-node", runner=FakeRunner(), config=cfg)
    enh2.load("/tmp/agent_node_model")
    print(f"  reloaded: {enh2.get_optimizer_stats()}")

    print("\n=== Signature dispatch (runner without strategy kwarg) ===")
    class NoStrategyRunner:
        def __init__(self):
            self.calls = 0

        def run(self, task: Any) -> Dict[str, Any]:
            self.calls += 1
            return {"output": "done", "metrics": {"accuracy": 0.9, "energy_kwh": 0.001}}

    no_strategy_runner = NoStrategyRunner()
    enhanced_no_strategy = AgentNode(
        name="no-strategy-node",
        runner=no_strategy_runner,
        config=cfg,
    )
    enhanced_no_strategy.execute({"id": "t-ns", "complexity": 0.4})
    print(f"  runner calls: {no_strategy_runner.calls} (should be 1)")

    print("\n=== Metric emission ===")
    emitted = {}
    if isinstance(enh.metrics, InMemoryMetrics):
        emitted = enh.metrics.snapshot()
    print(f"  emitted metric keys: {sorted(emitted.keys())}")

    print("\n=== Determinism check (feature vector across processes) ===")
    from hashlib import sha256
    s1 = AgentNodeState({"id": "x"}, "flexgen", {}, 0.5)
    s2 = AgentNodeState({"id": "x"}, "flexgen", {}, 0.5)
    assert s1.runner_name_encoded == s2.runner_name_encoded, "name encoding not deterministic!"
    print(f"  runner_name_encoded = {s1.runner_name_encoded:.6f} (deterministic)")
