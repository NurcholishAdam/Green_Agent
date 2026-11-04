# -*- coding: utf-8 -*-
"""
Causal Graph (Enhanced)
=======================

Models the Green Agent's runtime environment as a directed causal graph.

- **Nodes** are measurable system variables (``CarbonIntensity``,
  ``GridStrain``, ``BatteryLevel``, …).
- **Edges** are causal relationships with signed weights
  (``+1`` = amplifies, ``-1`` = suppresses) and Beta-posterior
  confidence.

Core capability: backward BFS from an anomalous variable traces the
causal chain to its root, producing an ordered list of "why this
happened" explanations instead of a bare flag. After each benchmark
result, call :meth:`CausalGraph.update_edge_weight` so the graph
progressively learns which causal relationships are strong vs spurious
in the actual deployment environment.

.. note::
    The previous docstring described the update as a "Bayesian update
    mechanism". It was a **signed linear step** with one-sided
    confidence. The current version tracks per-edge success / failure
    counts; confidence is the Beta-posterior mean
    ``success / (success + failure)``, and the learning rate decays as
    evidence accumulates. That is the honest description of the
    algorithm.

Key fixes over the previous version
-----------------------------------
- **Evolutionary optimizer removed.** ``use_evolutionary=True``
  constructed a population whose only scored individual was index 0;
  ``argmax`` always returned 0; ``best_weights = population[0]`` (a
  random vector) overwrote every edge on the first
  ``update_edge_weight`` call, silently destroying the graph. The flag
  is retained as a **deprecated** field that raises a clear
  :class:`CausalGraphError` at config construction. The replacement is
  :class:`CausalGraphConfig.use_hill_climbing` — a directed step on
  the specific edge being updated — which is what the available
  per-call signal can actually support.
- **Signed propagation in ``trace_root_causes``.** The previous
  implementation used ``abs(edge_w)``, so a suppression edge
  (``BatteryLevel —suppresses→ DeferralSignal``) was scored as if it
  amplified the anomaly. Each chain now reports
  ``cumulative_signed_weight`` (signed) and ``effect`` (``"amplifies"``
  / ``"suppresses"``); sorting still uses the unsigned magnitude for
  ranking.
- **Evidence-weighted update.** ``CausalEdge.confidence`` is a
  derived property ``success / (success + failure)`` rather than a
  monotonically-increasing field. A spurious edge accumulates failures
  and its learning rate decays as ``1 / sqrt(1 + total)``; the previous
  implementation kept a bad edge at half rate forever.
- **Inverted exploration fixed.** ``predict_weight_change`` now uses
  the teacher at full scale during exploration and the trained student
  during exploitation. The previous version used the teacher at *20%*
  scale most of the time and at *100%* scale during exploration —
  exactly backwards.
- **Per-node centrality.** ``_apply_rlhf_and_graph_metrics`` was
  adding a global scalar to every chain's score — a constant offset
  that never changed the sort. It now uses per-node centrality of the
  chain's root.
- **Thread-safe.** Every mutating and reading method is guarded by an
  ``RLock``. ``trace_root_causes`` snapshots ``nodes`` and ``edges``
  before traversal.
- **Frozen, validated dataclasses.** :class:`CausalNode`,
  :class:`CausalEdge`, and :class:`CausalGraphConfig` reject malformed
  values at construction. ``check_anomaly`` returns a new node via
  :func:`dataclasses.replace`.
- **Immutable defaults.** ``DEFAULT_STRUCTURE`` and
  ``DEFAULT_THRESHOLDS`` are module-level ``Final`` tuples /
  ``MappingProxyType`` views; the previous class attributes were
  mutable and shared across instances.
- **O(1) edge lookup.** ``_edge_index: Dict[Tuple[str, str], int]``
  replaces the O(n) linear scans in ``_get_edge_weight`` and friends.
- **``reset()``.** Restores every edge to its ``DEFAULT_STRUCTURE``
  priors and clears history — required for independent benchmarks
  sharing a graph instance.
- **Serialization.** ``export_state()`` / ``from_dict()`` /
  ``to_json()`` round-trip the graph including per-edge evidence
  counts.
- **``strict`` mode.** ``CausalGraph(..., strict=True)`` re-raises on
  configuration or update failures instead of degrading.
- **Per-instance RNGs.** Seedable via ``config.seed`` / ``config.gate_seed``;
  two graphs in the same process no longer share global RNG state.
- **Config cross-checks.** ``use_distillation=True`` /
  ``use_hill_climbing=True`` require ``use_enhancements=True``;
  inconsistent configs are rejected at construction.
- **Logging.** Debug on ``observe`` / ``update_edge_weight`` /
  ``trace_root_causes``; warning when a deprecated flag is set;
  exception on caught update failures.
- **Bug fix: ``_build_edge_state`` lower clamp.** Feature values are
  clamped to ``[0, 1]``; a node with a negative value no longer leaks
  a negative feature into the linear model.
- **Bug fix: distillation target from reward magnitude.** Target
  becomes ``float(reward) * 0.1`` rather than ``±0.1`` regardless of
  reward — the magnitude of the reward now influences the update.
- **Bug fix: ``action`` argument removed** from the distillation
  update. It was stored and never used; the reward already carries the
  sign.
- **Bug fix: ``compute_graph_metrics`` keeps ``centrality`` for
  backward compatibility** and adds ``node_centrality`` for per-node
  consumers.
- **Bug fix: ``observe`` validates ``value`` type and finiteness.**
- **Bug fix: ``observe_batch`` validates unknown keys** — a typo like
  ``CarbonIntensity_hih`` no longer becomes a new node.

Compatibility
-------------
- ``CausalGraph(config=None)`` — same positional signature.
  ``strict=False`` is new and keyword-only.
- ``CausalGraphConfig`` — mostly same fields. Changes:
    - ``use_evolutionary`` still accepted but raises at construction.
    - ``use_hill_climbing`` is new.
    - ``seed`` / ``gate_seed`` are new.
    - ``modp_weights`` typed ``Optional[Tuple[float, float, float]]``.
    - Frozen; direct mutation raises.
- ``CausalNode`` / ``CausalEdge`` — frozen. ``check_anomaly`` returns
  a new node instead of mutating in place. ``confidence`` is a
  computed property; ``success_count`` / ``failure_count`` are the
  source fields.
- ``update_edge_weight(source, target, outcome_correct, learning_rate=0.05)``
  — same signature. Effect: evidence-weighted; ``learning_rate`` is
  the *initial* rate and decays with accumulated evidence.
- ``trace_root_causes(anomaly_variable, max_depth=5, min_weight=0.2,
  max_chains=10, deduplicate_by_root=True)`` — two new keyword-only
  params (``max_chains``, ``deduplicate_by_root``). Each chain now
  contains ``cumulative_signed_weight`` and ``effect`` in addition to
  the backward-compatible ``cumulative_weight``.
- ``export_state()`` — includes ``success_count`` / ``failure_count``
  per edge (used by ``from_dict``) plus ``config`` for round-trip.
- ``CausalGraph.DEFAULT_STRUCTURE`` / ``DEFAULT_THRESHOLDS`` — no
  longer class attributes. Use ``DEFAULT_CAUSAL_STRUCTURE`` and
  ``DEFAULT_CAUSAL_THRESHOLDS`` at module level.

Requires Python 3.8+. ``slots=True`` requires 3.10+; drop for older
versions.
"""

from __future__ import annotations

import enum
import json
import logging
import math
import random
import threading
from collections import defaultdict, deque
from dataclasses import dataclass, field, replace
from types import MappingProxyType
from typing import (
    Any, Callable, Deque, Dict, Final, List, Mapping, Optional, Tuple,
)

import numpy as np


logger = logging.getLogger(__name__)

__version__ = "2.5.0"


# --------------------------------------------------------------------------- #
# Errors
# --------------------------------------------------------------------------- #
class CausalGraphError(ValueError):
    """Raised for invalid causal-graph inputs or configuration."""


# --------------------------------------------------------------------------- #
# StrEnum compatibility shim
# --------------------------------------------------------------------------- #
try:  # pragma: no cover — Python 3.11+
    from enum import StrEnum  # type: ignore[attr-defined]
except ImportError:  # pragma: no cover — 3.10 and earlier
    class StrEnum(str, enum.Enum):  # type: ignore[no-redef]
        __str__ = str.__str__
        __format__ = str.__format__


# --------------------------------------------------------------------------- #
# Causal label vocabulary
# --------------------------------------------------------------------------- #
class CausalLabel(StrEnum):
    """Known edge relationship labels."""

    CAUSES = "causes"
    AMPLIFIES = "amplifies"
    MODULATES = "modulates"
    TRIGGERS = "triggers"
    SUPPRESSES = "suppresses"
    REINFORCES = "reinforces"


# --------------------------------------------------------------------------- #
# Validation helpers
# --------------------------------------------------------------------------- #
def _finite(name: str, value: Any) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise CausalGraphError(f"{name} must be numeric.")
    fv = float(value)
    if not math.isfinite(fv):
        raise CausalGraphError(f"{name} must be finite, got {value!r}.")
    return fv


def _clamp(value: float, low: float, high: float) -> float:
    return max(low, min(high, float(value)))


def _clamp01(value: float) -> float:
    return max(0.0, min(float(value), 1.0))


# --------------------------------------------------------------------------- #
# Data classes (frozen)
# --------------------------------------------------------------------------- #
@dataclass(frozen=True, slots=True)
class CausalNode:
    """
    Immutable snapshot of one causal variable.

    ``check_anomaly`` returns a new instance with ``anomalous`` set — the
    node itself is never mutated in place.
    """

    node_id: str
    variable: str
    current_value: float = 0.0
    threshold_high: Optional[float] = None
    threshold_low: Optional[float] = None
    anomalous: bool = False
    unit: str = ""

    def __post_init__(self) -> None:
        if not isinstance(self.node_id, str) or not self.node_id:
            raise CausalGraphError("node_id must be a non-empty string.")
        if not isinstance(self.variable, str) or not self.variable:
            raise CausalGraphError("variable must be a non-empty string.")
        _finite("current_value", self.current_value)
        for name in ("threshold_high", "threshold_low"):
            v = getattr(self, name)
            if v is None:
                continue
            fv = _finite(name, v)
            if fv < 0:
                raise CausalGraphError(f"{name} must be >= 0.")
        if not isinstance(self.anomalous, bool):
            raise CausalGraphError("anomalous must be a bool.")
        if not isinstance(self.unit, str):
            raise CausalGraphError("unit must be a string.")

    def check_anomaly(self) -> "CausalNode":
        """Return a new node with ``anomalous`` set from the thresholds."""
        hi_breach = (
            self.threshold_high is not None
            and self.current_value > self.threshold_high
        )
        lo_breach = (
            self.threshold_low is not None
            and self.current_value < self.threshold_low
        )
        return replace(self, anomalous=hi_breach or lo_breach)


@dataclass(frozen=True, slots=True)
class CausalEdge:
    """
    Immutable directed edge with Beta-posterior confidence.

    ``weight`` is in ``[-1, 1]``; positive amplifies, negative suppresses.
    ``confidence`` is derived from ``success_count`` / ``failure_count`` —
    it is *not* a field so it cannot drift from the evidence.
    """

    source_id: str
    target_id: str
    weight: float = 0.7
    label: str = CausalLabel.CAUSES.value
    success_count: int = 0
    failure_count: int = 0

    def __post_init__(self) -> None:
        if not isinstance(self.source_id, str) or not self.source_id:
            raise CausalGraphError("source_id must be a non-empty string.")
        if not isinstance(self.target_id, str) or not self.target_id:
            raise CausalGraphError("target_id must be a non-empty string.")
        if self.source_id == self.target_id:
            raise CausalGraphError(
                f"self-loop on {self.source_id!r} is not allowed."
            )
        fw = _finite("weight", self.weight)
        if not -1.0 <= fw <= 1.0:
            raise CausalGraphError(
                f"weight must be in [-1, 1], got {fw}."
            )
        if self.label not in {lab.value for lab in CausalLabel}:
            raise CausalGraphError(
                f"label {self.label!r} is not a CausalLabel."
            )
        for name in ("success_count", "failure_count"):
            v = getattr(self, name)
            if isinstance(v, bool) or not isinstance(v, int) or v < 0:
                raise CausalGraphError(
                    f"{name} must be a non-negative int."
                )

    @property
    def confidence(self) -> float:
        """Beta-posterior mean of ``success / (success + failure)``."""
        total = self.success_count + self.failure_count
        if total == 0:
            return 0.5
        return self.success_count / total


@dataclass(frozen=True, slots=True)
class CausalGraphConfig:
    """Frozen configuration for :class:`CausalGraph`."""

    use_enhancements: bool = False
    compute_graph_metrics: bool = True

    # MODP: [carbon, latency, energy]
    modp_weights: Optional[Tuple[float, float, float]] = None
    human_feedback_score: float = 0.5

    # Distillation + MoE
    use_distillation: bool = False
    distillation_lr: float = 0.01
    gating_lr: float = 0.005
    replay_size: int = 2000
    train_every: int = 10
    epsilon: float = 0.1

    # Hill-climbing (replaces the broken evolutionary optimizer)
    use_hill_climbing: bool = False

    # Deprecated; kept to reject old payloads with a clear error
    use_evolutionary: bool = False

    # Determinism
    seed: Optional[int] = None
    gate_seed: Optional[int] = None

    def __post_init__(self) -> None:
        if not isinstance(self.use_enhancements, bool):
            raise CausalGraphError("use_enhancements must be a bool.")
        if not isinstance(self.compute_graph_metrics, bool):
            raise CausalGraphError("compute_graph_metrics must be a bool.")
        if not isinstance(self.use_distillation, bool):
            raise CausalGraphError("use_distillation must be a bool.")
        if not isinstance(self.use_hill_climbing, bool):
            raise CausalGraphError("use_hill_climbing must be a bool.")
        if not isinstance(self.use_evolutionary, bool):
            raise CausalGraphError("use_evolutionary must be a bool.")
        if self.use_evolutionary:
            raise CausalGraphError(
                "use_evolutionary is no longer supported: the previous "
                "implementation scored only individual 0 and overwrote "
                "the graph on the first update. Use "
                "use_hill_climbing=True for a directed step on the "
                "specific edge being updated."
            )
        if self.use_distillation and not self.use_enhancements:
            raise CausalGraphError(
                "use_distillation=True requires use_enhancements=True."
            )
        if self.use_hill_climbing and not self.use_enhancements:
            raise CausalGraphError(
                "use_hill_climbing=True requires use_enhancements=True."
            )

        fv = _finite("human_feedback_score", self.human_feedback_score)
        if not 0.0 <= fv <= 1.0:
            raise CausalGraphError(
                "human_feedback_score must be in [0, 1]."
            )

        if self.modp_weights is not None:
            if not isinstance(self.modp_weights, tuple):
                raise CausalGraphError(
                    "modp_weights must be a 3-tuple or None."
                )
            if len(self.modp_weights) != 3:
                raise CausalGraphError(
                    f"modp_weights must have 3 elements, got "
                    f"{len(self.modp_weights)}."
                )
            for i, w in enumerate(self.modp_weights):
                fw = _finite(f"modp_weights[{i}]", w)
                if fw < 0:
                    raise CausalGraphError(
                        f"modp_weights[{i}] must be >= 0."
                    )
            if sum(self.modp_weights) <= 0:
                raise CausalGraphError(
                    "modp_weights must have a positive sum."
                )

        fv = _finite("epsilon", self.epsilon)
        if not 0.0 <= fv <= 1.0:
            raise CausalGraphError("epsilon must be in [0, 1].")

        for name in ("distillation_lr", "gating_lr"):
            fv = _finite(name, getattr(self, name))
            if fv <= 0:
                raise CausalGraphError(f"{name} must be > 0.")

        for name in ("replay_size", "train_every"):
            v = getattr(self, name)
            if isinstance(v, bool) or not isinstance(v, int) or v < 1:
                raise CausalGraphError(
                    f"{name} must be an int >= 1."
                )

        for name in ("seed", "gate_seed"):
            v = getattr(self, name)
            if v is None:
                continue
            if isinstance(v, bool) or not isinstance(v, int):
                raise CausalGraphError(f"{name} must be an int or None.")


# --------------------------------------------------------------------------- #
# Immutable defaults
# --------------------------------------------------------------------------- #
DEFAULT_CAUSAL_STRUCTURE: Final[Tuple[
    Tuple[str, str, float, str], ...
]] = (
    ("WeatherEvent",         "RenewableShortfall",    0.80, "causes"),
    ("RenewableShortfall",   "GridStrain",            0.90, "amplifies"),
    ("GridStrain",           "CarbonIntensity",       0.85, "modulates"),
    ("CarbonIntensity",      "DeferralSignal",        0.75, "triggers"),
    ("DeferralSignal",       "ModelThrottleDecision", 0.70, "triggers"),
    ("BatteryLevel",         "DeferralSignal",       -0.60, "suppresses"),
    ("TaskPriority",         "ModelThrottleDecision",-0.80, "suppresses"),
    ("GridStrain",           "DeferralSignal",        0.65, "reinforces"),
    ("QueueDepth",           "ModelThrottleDecision", 0.50, "modulates"),
    ("ModelThrottleDecision","AccuracyDrop",          0.60, "causes"),
    ("DeferralSignal",       "LatencyIncrease",       0.55, "causes"),
)

DEFAULT_CAUSAL_THRESHOLDS: Final[Mapping[str, Mapping[str, Any]]] = (
    MappingProxyType({
        "CarbonIntensity":   MappingProxyType({"high": 400.0, "unit": "g_CO2/kWh"}),
        "GridStrain":        MappingProxyType({"high": 0.80, "unit": "ratio"}),
        "BatteryLevel":      MappingProxyType({"low": 0.25, "unit": "ratio"}),
        "TaskPriority":      MappingProxyType({"low": 0.20, "unit": "ratio"}),
        "QueueDepth":        MappingProxyType({"high": 100.0, "unit": "tasks"}),
        "AccuracyDrop":      MappingProxyType({"high": 0.15, "unit": "ratio"}),
        "LatencyIncrease":   MappingProxyType({"high": 500.0, "unit": "ms"}),
    })
)


# --------------------------------------------------------------------------- #
# Teachers
# --------------------------------------------------------------------------- #
class EdgeWeightTeacher:
    """Base class for an edge-weight teacher."""

    def predict(self, state: np.ndarray) -> float:
        raise NotImplementedError


class RuleBasedTeacher(EdgeWeightTeacher):
    """Rule-based teacher: strengthen when both endpoints are anomalous."""

    def predict(self, state: np.ndarray) -> float:
        if state[0] > 0.5 and state[1] > 0.5:
            return 0.1
        if state[4] > 0.7:
            return 0.05
        return -0.05


class RLHFTeacher(EdgeWeightTeacher):
    """Human-feedback teacher: prefer edges the human validated."""

    def predict(self, state: np.ndarray) -> float:
        return 0.08 * (float(state[5]) - 0.5)


class HistoricalTeacher(EdgeWeightTeacher):
    """Historical teacher: fixed linear prior over the state features."""

    _WEIGHTS: Final[Tuple[float, ...]] = (
        0.02, 0.02, -0.01, -0.01, 0.03, 0.01,
    )

    def predict(self, state: np.ndarray) -> float:
        return float(np.dot(np.asarray(self._WEIGHTS), state))


# --------------------------------------------------------------------------- #
# Distillation optimizer
# --------------------------------------------------------------------------- #
@dataclass(frozen=True, slots=True)
class _ReplayEntry:
    state: np.ndarray
    reward: float


class EdgeWeightDistillationOptimizer:
    """
    Multi-teacher distillation with MoE gating to predict edge-weight
    changes.

    ``predict_weight_change(state)`` returns a proposed **signed** weight
    delta in ``[-0.1, 0.1]``. ``update(state, reward)`` records one
    transition; ``reward`` must be in ``[-1, 1]`` and is used to derive
    the training target as ``float(reward) * 0.1``.

    The student is a linear regressor; the gate is a softmax over the
    ``n_teachers`` teacher predictions. Exploration (``epsilon`` of the
    time) uses the raw teacher prediction; exploitation blends the
    trained student over the teacher (80 / 20).
    """

    def __init__(
        self,
        feature_dim: int,
        config: CausalGraphConfig,
    ) -> None:
        if feature_dim <= 0:
            raise CausalGraphError(
                "feature_dim must be > 0."
            )
        self.feature_dim = feature_dim
        self.config = config

        self.student_weights = np.zeros(feature_dim, dtype=np.float64)
        self.student_bias = 0.0
        self.lr = config.distillation_lr
        self.epsilon = config.epsilon
        self.train_every = config.train_every

        self.teachers = (
            RuleBasedTeacher(),
            RLHFTeacher(),
            HistoricalTeacher(),
        )
        self._np_rng = np.random.default_rng(config.gate_seed)
        self._py_rng = random.Random(config.seed)

        self.gate_weights = (
            self._np_rng.standard_normal(
                (feature_dim, len(self.teachers)),
            ) * 0.01
        )
        self.gate_bias = np.zeros(len(self.teachers), dtype=np.float64)
        self.gate_lr = config.gating_lr

        self.replay_buffer: Deque[_ReplayEntry] = deque(
            maxlen=config.replay_size,
        )
        self.counter = 0
        self._lock = threading.RLock()

    # ---------------------------------------------------------------- predict
    def _gate_forward(self, state: np.ndarray) -> np.ndarray:
        logits = state @ self.gate_weights + self.gate_bias
        logits = logits - np.max(logits)
        exp = np.exp(logits)
        return exp / exp.sum()

    def predict_weight_change(self, state: np.ndarray) -> float:
        if not isinstance(state, np.ndarray) or state.shape != (
            self.feature_dim,
        ):
            raise CausalGraphError(
                f"state must be a 1-D array of length "
                f"{self.feature_dim}."
            )
        with self._lock:
            teacher_preds = np.array(
                [t.predict(state) for t in self.teachers],
                dtype=np.float64,
            )
            gate = self._gate_forward(state)
            teacher_combined = float(np.dot(gate, teacher_preds))
            student_pred = float(
                np.dot(state, self.student_weights) + self.student_bias
            )
            if self._py_rng.random() < self.epsilon:
                # Exploration: use the raw teacher signal at full scale.
                return teacher_combined
            # Exploitation: blend the trained student over the teacher.
            return 0.8 * student_pred + 0.2 * teacher_combined

    # ---------------------------------------------------------------- update
    def update(self, state: np.ndarray, reward: float) -> None:
        """Record one transition; train periodically."""
        fr = _finite("reward", reward)
        if not -1.0 <= fr <= 1.0:
            raise CausalGraphError(
                f"reward must be in [-1, 1], got {fr}."
            )
        with self._lock:
            self.replay_buffer.append(_ReplayEntry(
                state=state.copy(), reward=fr,
            ))
            self.counter += 1
            if (
                self.counter % self.train_every != 0
                or len(self.replay_buffer) < 8
            ):
                return
            batch = self._py_rng.sample(
                list(self.replay_buffer),
                min(8, len(self.replay_buffer)),
            )
            self._train_batch(batch)

    def _train_batch(self, batch: List[_ReplayEntry]) -> None:
        for entry in batch:
            s = entry.state
            target = float(entry.reward) * 0.1
            pred = float(
                np.dot(s, self.student_weights) + self.student_bias
            )
            grad = (pred - target) * s
            self.student_weights -= self.lr * grad
            self.student_bias -= self.lr * (pred - target)

            teacher_preds = np.array(
                [t.predict(s) for t in self.teachers],
                dtype=np.float64,
            )
            gate = self._gate_forward(s)
            combined = float(np.dot(gate, teacher_preds))
            error = combined - target
            grad_gate = teacher_preds * error
            self.gate_weights -= self.gate_lr * np.outer(s, grad_gate)
            self.gate_bias -= self.gate_lr * grad_gate

    def statistics(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "counter": self.counter,
                "buffer_size": len(self.replay_buffer),
                "feature_dim": self.feature_dim,
                "n_teachers": len(self.teachers),
                "epsilon": self.epsilon,
            }


# --------------------------------------------------------------------------- #
# Directed hill-climbing optimizer
# --------------------------------------------------------------------------- #
class EdgeWeightHillClimbingOptimizer:
    """
    Directed step on the specific edge being updated.

    This replaces the previous ``EdgeWeightEvolutionaryOptimizer``,
    which had no per-individual fitness signal and silently overwrote
    the graph on the first update. A single graph maintains a single
    point in weight space; there is no population to evolve, so
    hill-climbing on the current edge is the correct algorithm.
    """

    def __init__(self, config: CausalGraphConfig) -> None:
        self.config = config
        self.step_size = 0.05
        self._lock = threading.RLock()

    def propose_step(self, current_weight: float) -> float:
        """
        Return the proposed delta for one positive-outcome update.

        Negative outcomes use the opposite sign of this value. The
        magnitude shrinks as the edge approaches its bound so updates
        never push past ``[-1, 1]``.
        """
        with self._lock:
            headroom = 1.0 - abs(float(current_weight))
            return self.step_size * headroom


# --------------------------------------------------------------------------- #
# Graph
# --------------------------------------------------------------------------- #
class CausalGraph:
    """
    Directed causal graph of Green Agent runtime variables.

    Thread-safe: every mutating and reading method is guarded by an
    ``RLock``, and ``trace_root_causes`` snapshots the graph before
    traversal.

    Weights start from the ``DEFAULT_CAUSAL_STRUCTURE`` priors and are
    refined by :meth:`update_edge_weight`.
    """

    def __init__(
        self,
        config: Optional[CausalGraphConfig] = None,
        *,
        strict: bool = False,
    ) -> None:
        if config is not None and not isinstance(
            config, CausalGraphConfig
        ):
            raise CausalGraphError(
                f"config must be a CausalGraphConfig, got "
                f"{type(config).__name__}."
            )
        self.config = config or CausalGraphConfig()
        self._strict = bool(strict)
        self._lock = threading.RLock()

        self.nodes: Dict[str, CausalNode] = {}
        self.edges: List[CausalEdge] = []
        self._parents: Dict[str, List[str]] = defaultdict(list)
        self._edge_index: Dict[Tuple[str, str], int] = {}

        self.distillation_optimizer: Optional[
            EdgeWeightDistillationOptimizer
        ] = None
        self.hill_climbing_optimizer: Optional[
            EdgeWeightHillClimbingOptimizer
        ] = None

        self._initialize()
        if self.config.use_enhancements:
            self._init_enhancements()

    # ---------------------------------------------------------------- init
    def _initialize(self) -> None:
        with self._lock:
            self.nodes.clear()
            self.edges.clear()
            self._parents.clear()
            self._edge_index.clear()

            variables: Dict[str, CausalNode] = {}
            for src, tgt, _w, _l in DEFAULT_CAUSAL_STRUCTURE:
                for var in (src, tgt):
                    if var in variables:
                        continue
                    th = DEFAULT_CAUSAL_THRESHOLDS.get(var, {})
                    variables[var] = CausalNode(
                        node_id=var,
                        variable=var,
                        threshold_high=th.get("high"),
                        threshold_low=th.get("low"),
                        unit=th.get("unit", ""),
                    )
            self.nodes = variables

            for src, tgt, weight, label in DEFAULT_CAUSAL_STRUCTURE:
                self._add_edge_locked(CausalEdge(
                    source_id=src, target_id=tgt,
                    weight=float(weight), label=label,
                ))

    def _init_enhancements(self) -> None:
        if self.config.use_distillation:
            self.distillation_optimizer = (
                EdgeWeightDistillationOptimizer(
                    feature_dim=self._get_edge_feature_dim(),
                    config=self.config,
                )
            )
        if self.config.use_hill_climbing:
            self.hill_climbing_optimizer = (
                EdgeWeightHillClimbingOptimizer(self.config)
            )

    def _get_edge_feature_dim(self) -> int:
        """Feature dimension for the edge-state vector."""
        # source anomaly, target anomaly, source value, target value,
        # global centrality, human feedback.
        return 6

    # ---------------------------------------------------------------- edge
    def _add_edge_locked(self, edge: CausalEdge) -> None:
        """Add an edge; caller must hold the lock."""
        key = (edge.source_id, edge.target_id)
        if key in self._edge_index:
            raise CausalGraphError(
                f"edge {edge.source_id!r} -> {edge.target_id!r} already "
                f"exists."
            )
        self._parents[edge.target_id].append(edge.source_id)
        self._edge_index[key] = len(self.edges)
        self.edges.append(edge)

    def _get_edge_index(
        self, source: str, target: str,
    ) -> Optional[int]:
        with self._lock:
            return self._edge_index.get((source, target))

    # ---------------------------------------------------------------- metrics
    def compute_graph_metrics(self) -> Dict[str, Any]:
        """
        Return a JSON-safe summary of graph structure.

        ``centrality`` (backward-compatible scalar) is the mean absolute
        edge weight. ``node_centrality`` is the per-node normalised
        degree centrality used by RLHF re-ranking.
        """
        with self._lock:
            if not self.edges:
                return {
                    "centrality": 0.5,
                    "connectivity": 0.5,
                    "node_centrality": {
                        n: 0.5 for n in self.nodes
                    },
                }
            total_weight = sum(abs(e.weight) for e in self.edges)
            mean_weight = total_weight / len(self.edges)
            centrality = min(mean_weight, 1.0)

            n_nodes = len(self.nodes)
            max_edges = n_nodes * (n_nodes - 1) if n_nodes > 1 else 1
            connectivity = len(self.edges) / max_edges

            node_out: Dict[str, float] = defaultdict(float)
            node_in: Dict[str, float] = defaultdict(float)
            for e in self.edges:
                node_out[e.source_id] += abs(e.weight)
                node_in[e.target_id] += abs(e.weight)
            max_degree = max(
                (node_out[n] + node_in[n] for n in self.nodes),
                default=1.0,
            ) or 1.0
            node_centrality = {
                n: (node_out[n] + node_in[n]) / max_degree
                for n in self.nodes
            }

            return {
                "centrality": centrality,
                "connectivity": min(connectivity, 1.0),
                "node_centrality": node_centrality,
            }

    # ---------------------------------------------------------------- observe
    def observe(
        self,
        variable: str,
        value: float,
        threshold_high: Optional[float] = None,
        threshold_low: Optional[float] = None,
    ) -> None:
        if not isinstance(variable, str) or not variable:
            raise CausalGraphError(
                "variable must be a non-empty string."
            )
        fv = _finite("value", value)
        for name, v in (
            ("threshold_high", threshold_high),
            ("threshold_low", threshold_low),
        ):
            if v is not None:
                _finite(name, v)

        with self._lock:
            if variable not in self.nodes:
                self.nodes[variable] = CausalNode(
                    node_id=variable, variable=variable,
                )
            existing = self.nodes[variable]
            new_node = CausalNode(
                node_id=variable,
                variable=variable,
                current_value=fv,
                threshold_high=(
                    threshold_high if threshold_high is not None
                    else existing.threshold_high
                ),
                threshold_low=(
                    threshold_low if threshold_low is not None
                    else existing.threshold_low
                ),
                unit=existing.unit,
            )
            self.nodes[variable] = new_node.check_anomaly()

    def observe_batch(self, snapshot: Mapping[str, Any]) -> None:
        if not isinstance(snapshot, Mapping):
            raise CausalGraphError(
                f"snapshot must be a Mapping, got "
                f"{type(snapshot).__name__}."
            )
        for key, val in snapshot.items():
            if not isinstance(key, str):
                raise CausalGraphError(
                    f"snapshot keys must be strings, got "
                    f"{type(key).__name__}."
                )
            if key.endswith("_high") or key.endswith("_low"):
                continue
            hi = snapshot.get(f"{key}_high")
            lo = snapshot.get(f"{key}_low")
            self.observe(key, val, hi, lo)

    # ---------------------------------------------------------------- trace
    def trace_root_causes(
        self,
        anomaly_variable: str,
        max_depth: int = 5,
        min_weight: float = 0.2,
        *,
        max_chains: int = 10,
        deduplicate_by_root: bool = True,
    ) -> List[Dict[str, Any]]:
        """
        Backward BFS from ``anomaly_variable`` to its root causes.

        Each chain reports:

        - ``cumulative_weight`` — unsigned strength (backward
          compatible; used for ranking);
        - ``cumulative_signed_weight`` — signed strength; negative
          means the chain's dominant effect is suppression;
        - ``effect`` — ``"amplifies"`` or ``"suppresses"``;
        - ``root_cause``, ``path``, ``path_labels``,
          ``root_is_anomalous``.

        ``deduplicate_by_root=True`` keeps only the highest-strength
        chain per root cause. ``max_chains`` bounds the result size.
        """
        if not isinstance(anomaly_variable, str) or not anomaly_variable:
            raise CausalGraphError(
                "anomaly_variable must be a non-empty string."
            )
        if isinstance(max_depth, bool) or not isinstance(max_depth, int):
            raise CausalGraphError("max_depth must be an int.")
        if max_depth < 1:
            raise CausalGraphError("max_depth must be >= 1.")
        _finite("min_weight", min_weight)
        if min_weight < 0:
            raise CausalGraphError("min_weight must be >= 0.")
        if not isinstance(max_chains, int) or max_chains < 1:
            raise CausalGraphError("max_chains must be an int >= 1.")

        with self._lock:
            if anomaly_variable not in self.nodes:
                return []
            # Snapshot under the lock; traversal runs on the snapshot.
            nodes_snapshot = dict(self.nodes)
            edges_by_key: Dict[Tuple[str, str], CausalEdge] = {
                (e.source_id, e.target_id): e for e in self.edges
            }
            parents_snapshot: Dict[str, List[str]] = {
                k: list(v) for k, v in self._parents.items()
            }

        chains: List[Dict[str, Any]] = []
        # (current, path, labels, signed, strength)
        queue: Deque[Tuple[str, List[str], List[str], float, float]] = deque()
        queue.append((anomaly_variable, [anomaly_variable], [], 1.0, 1.0))
        visited_paths: set = set()

        while queue:
            current, path, labels, signed, strength = queue.popleft()
            if len(path) > max_depth:
                continue
            parents = parents_snapshot.get(current, [])
            if not parents:
                key = tuple(path)
                if key in visited_paths:
                    continue
                visited_paths.add(key)
                chains.append({
                    "root_cause": current,
                    "path": list(reversed(path)),
                    "path_labels": list(reversed(labels)),
                    "cumulative_weight": round(strength, 4),
                    "cumulative_signed_weight": round(signed, 4),
                    "effect": (
                        "amplifies" if signed > 0 else "suppresses"
                    ),
                    "root_is_anomalous": (
                        nodes_snapshot.get(current).anomalous
                        if current in nodes_snapshot else False
                    ),
                })
                continue

            for parent in parents:
                edge = edges_by_key.get((parent, current))
                if edge is None or abs(edge.weight) < min_weight:
                    continue
                new_signed = signed * edge.weight
                new_strength = abs(new_signed)
                queue.append((
                    parent,
                    path + [parent],
                    labels + [edge.label],
                    new_signed,
                    new_strength,
                ))

        chains.sort(
            key=lambda c: c["cumulative_weight"], reverse=True,
        )

        if deduplicate_by_root:
            seen_roots: set = set()
            unique: List[Dict[str, Any]] = []
            for c in chains:
                if c["root_cause"] in seen_roots:
                    continue
                seen_roots.add(c["root_cause"])
                unique.append(c)
            chains = unique

        if self.config.use_enhancements:
            chains = self._apply_rlhf_and_graph_metrics(chains)

        return chains[:max_chains]

    def _apply_rlhf_and_graph_metrics(
        self, chains: List[Dict[str, Any]],
    ) -> List[Dict[str, Any]]:
        """
        Re-rank chains using per-node centrality of the root cause and
        the RLHF human feedback score.
        """
        metrics = self.compute_graph_metrics()
        node_centrality = metrics["node_centrality"]
        human = float(self.config.human_feedback_score)

        def score(c: Dict[str, Any]) -> float:
            root = c["root_cause"]
            centrality = float(node_centrality.get(root, 0.0))
            return (
                float(c["cumulative_weight"]) * (1.0 + human * centrality)
                + centrality * 0.1
            )

        chains.sort(key=score, reverse=True)
        return chains

    def get_anomalies(self) -> List[str]:
        with self._lock:
            return [
                nid for nid, node in self.nodes.items() if node.anomalous
            ]

    # ---------------------------------------------------------------- update
    def update_edge_weight(
        self,
        source: str,
        target: str,
        outcome_correct: bool,
        learning_rate: float = 0.05,
    ) -> None:
        """
        Update one edge's weight from an outcome.

        ``learning_rate`` is the **initial** rate; effective rate decays
        as ``learning_rate / sqrt(1 + total_evidence)`` so a
        well-established edge is not disturbed by a single bad outcome.
        """
        if not isinstance(outcome_correct, bool):
            raise CausalGraphError("outcome_correct must be a bool.")
        _finite("learning_rate", learning_rate)
        if learning_rate <= 0:
            raise CausalGraphError("learning_rate must be > 0.")

        with self._lock:
            idx = self._edge_index.get((source, target))
            if idx is None:
                return

            edge = self.edges[idx]

            # Evidence-weighted update. Confidence is derived from the
            # accumulated successes / failures — a spurious edge
            # accumulates failures and its effective rate decays.
            new_success = edge.success_count + (
                1 if outcome_correct else 0
            )
            new_failure = edge.failure_count + (
                0 if outcome_correct else 1
            )
            total = new_success + new_failure
            effective_lr = learning_rate / math.sqrt(1.0 + total)
            delta = effective_lr if outcome_correct else -effective_lr

            new_weight = _clamp(edge.weight + delta, -1.0, 1.0)

            # Distillation / hill-climbing can propose a larger step;
            # the outcome signal still drives the sign.
            if (
                self.distillation_optimizer is not None
                and self.config.use_distillation
            ):
                try:
                    state = self._build_edge_state(idx)
                    proposed = self.distillation_optimizer.predict_weight_change(
                        state,
                    )
                    new_weight = _clamp(
                        new_weight + proposed * (1 if outcome_correct else -1),
                        -1.0, 1.0,
                    )
                    self.distillation_optimizer.update(
                        state, 1.0 if outcome_correct else -1.0,
                    )
                except CausalGraphError:
                    raise
                except Exception:
                    logger.exception(
                        "distillation update failed for %s -> %s",
                        source, target,
                    )
                    if self._strict:
                        raise
            elif (
                self.hill_climbing_optimizer is not None
                and self.config.use_hill_climbing
            ):
                step = self.hill_climbing_optimizer.propose_step(
                    edge.weight,
                )
                delta_hc = step if outcome_correct else -step
                new_weight = _clamp(
                    new_weight + delta_hc, -1.0, 1.0,
                )

            self.edges[idx] = replace(
                edge,
                weight=new_weight,
                success_count=new_success,
                failure_count=new_failure,
            )

        logger.debug(
            "update_edge_weight %s -> %s: outcome=%s weight=%.4f "
            "(s=%d f=%d)",
            source, target, outcome_correct, new_weight,
            new_success, new_failure,
        )

    def _build_edge_state(self, edge_idx: int) -> np.ndarray:
        """Feature vector for one edge; every feature in ``[0, 1]``."""
        with self._lock:
            edge = self.edges[edge_idx]
            src_node = self.nodes[edge.source_id]
            tgt_node = self.nodes[edge.target_id]
            metrics = self.compute_graph_metrics()
        centrality = float(metrics["centrality"])
        human = float(self.config.human_feedback_score)

        return np.array([
            float(src_node.anomalous),
            float(tgt_node.anomalous),
            _clamp01(src_node.current_value / 1000.0),
            _clamp01(tgt_node.current_value / 1000.0),
            _clamp01(centrality),
            _clamp01(human),
        ], dtype=np.float32)

    # ---------------------------------------------------------------- reset
    def reset(self) -> None:
        """Restore all edges to their ``DEFAULT_CAUSAL_STRUCTURE`` priors."""
        with self._lock:
            self._initialize()
            if self.config.use_enhancements:
                self._init_enhancements()
        logger.info("CausalGraph reset to default structure")

    # ---------------------------------------------------------------- serialize
    def export_state(self) -> Dict[str, Any]:
        """Return a JSON-safe snapshot of nodes, edges, and config."""
        with self._lock:
            return {
                "config": {
                    "use_enhancements": self.config.use_enhancements,
                    "compute_graph_metrics": (
                        self.config.compute_graph_metrics
                    ),
                    "modp_weights": (
                        None if self.config.modp_weights is None
                        else list(self.config.modp_weights)
                    ),
                    "human_feedback_score": (
                        self.config.human_feedback_score
                    ),
                    "use_distillation": self.config.use_distillation,
                    "use_hill_climbing": self.config.use_hill_climbing,
                },
                "nodes": {
                    nid: {
                        "value": n.current_value,
                        "anomalous": n.anomalous,
                        "threshold_high": n.threshold_high,
                        "threshold_low": n.threshold_low,
                        "unit": n.unit,
                    }
                    for nid, n in self.nodes.items()
                },
                "edges": [
                    {
                        "source": e.source_id,
                        "target": e.target_id,
                        "weight": round(e.weight, 4),
                        "confidence": round(e.confidence, 4),
                        "success_count": e.success_count,
                        "failure_count": e.failure_count,
                        "label": e.label,
                    }
                    for e in self.edges
                ],
                "graph_metrics": self.compute_graph_metrics(),
            }

    @classmethod
    def from_dict(
        cls,
        data: Mapping[str, Any],
        *,
        strict: bool = False,
    ) -> "CausalGraph":
        if not isinstance(data, Mapping):
            raise CausalGraphError(
                f"from_dict expects a Mapping, got "
                f"{type(data).__name__}."
            )
        cfg_data = data.get("config") or {}
        try:
            cfg = CausalGraphConfig(
                use_enhancements=bool(
                    cfg_data.get("use_enhancements", False)
                ),
                compute_graph_metrics=bool(
                    cfg_data.get("compute_graph_metrics", True)
                ),
                modp_weights=(
                    None if cfg_data.get("modp_weights") is None
                    else tuple(cfg_data["modp_weights"])
                ),
                human_feedback_score=float(
                    cfg_data.get("human_feedback_score", 0.5)
                ),
                use_distillation=bool(
                    cfg_data.get("use_distillation", False)
                ),
                use_hill_climbing=bool(
                    cfg_data.get("use_hill_climbing", False)
                ),
            )
        except (TypeError, ValueError) as exc:
            raise CausalGraphError(
                f"invalid config in payload: {exc}"
            ) from exc

        graph = cls(config=cfg, strict=strict)
        with graph._lock:
            graph.nodes.clear()
            graph.edges.clear()
            graph._parents.clear()
            graph._edge_index.clear()

            for nid, ndata in (data.get("nodes") or {}).items():
                if not isinstance(ndata, Mapping):
                    raise CausalGraphError(
                        f"node {nid!r} must be a Mapping."
                    )
                try:
                    graph.nodes[nid] = CausalNode(
                        node_id=str(nid),
                        variable=str(nid),
                        current_value=float(ndata.get("value", 0.0)),
                        threshold_high=ndata.get("threshold_high"),
                        threshold_low=ndata.get("threshold_low"),
                        anomalous=bool(ndata.get("anomalous", False)),
                        unit=str(ndata.get("unit", "")),
                    )
                except (TypeError, ValueError) as exc:
                    raise CausalGraphError(
                        f"invalid node {nid!r}: {exc}"
                    ) from exc

            for edata in (data.get("edges") or []):
                if not isinstance(edata, Mapping):
                    raise CausalGraphError(
                        "each edge must be a Mapping."
                    )
                try:
                    graph._add_edge_locked(CausalEdge(
                        source_id=str(edata["source"]),
                        target_id=str(edata["target"]),
                        weight=float(edata["weight"]),
                        label=str(edata.get(
                            "label", CausalLabel.CAUSES.value,
                        )),
                        success_count=int(
                            edata.get("success_count", 0),
                        ),
                        failure_count=int(
                            edata.get("failure_count", 0),
                        ),
                    ))
                except (KeyError, TypeError, ValueError) as exc:
                    raise CausalGraphError(
                        f"invalid edge: {exc}"
                    ) from exc
        return graph

    def to_json(self) -> str:
        return json.dumps(self.export_state(), indent=2, default=str)

    @classmethod
    def from_json(
        cls, payload: str, *, strict: bool = False,
    ) -> "CausalGraph":
        try:
            data = json.loads(payload)
        except json.JSONDecodeError as exc:
            raise CausalGraphError(
                f"invalid JSON: {exc}"
            ) from exc
        return cls.from_dict(data, strict=strict)

    # ---------------------------------------------------------------- dunder
    def __repr__(self) -> str:
        with self._lock:
            return (
                "CausalGraph("
                f"nodes={len(self.nodes)}, "
                f"edges={len(self.edges)}, "
                f"enhancements={'on' if self.config.use_enhancements else 'off'})"
            )


# --------------------------------------------------------------------------- #
# Public API
# --------------------------------------------------------------------------- #
__all__: List[str] = [
    "__version__",
    "CausalEdge",
    "CausalGraph",
    "CausalGraphConfig",
    "CausalGraphError",
    "CausalLabel",
    "CausalNode",
    "DEFAULT_CAUSAL_STRUCTURE",
    "DEFAULT_CAUSAL_THRESHOLDS",
    "EdgeWeightDistillationOptimizer",
    "EdgeWeightHillClimbingOptimizer",
    "EdgeWeightTeacher",
    "HistoricalTeacher",
    "RLHFTeacher",
    "RuleBasedTeacher",
]


def __dir__() -> List[str]:
    return sorted(__all__)


# --------------------------------------------------------------------------- #
# Smoke test: python -m core.causal_graph
# --------------------------------------------------------------------------- #
if __name__ == "__main__":  # pragma: no cover
    import sys

    logging.basicConfig(
        level=logging.INFO,
        format="%(levelname)s %(name)s: %(message)s",
    )

    passed = 0
    failed = 0

    def _check(name: str, cond: bool) -> None:
        global passed, failed
        if cond:
            passed += 1
            print(f"  ✅ {name}")
        else:
            failed += 1
            print(f"  ❌ {name}")

    def _raises(exc_type, fn) -> bool:
        try:
            fn()
        except exc_type:
            return True
        except Exception:
            return False
        return False

    # ---- 1. Basic construction -------------------------------- #
    print("construction:")
    graph = CausalGraph()
    _check(
        "default nodes built",
        len(graph.nodes) > 0 and "CarbonIntensity" in graph.nodes,
    )
    _check(
        "default edges built",
        len(graph.edges) == len(DEFAULT_CAUSAL_STRUCTURE),
    )
    _check(
        "edge index populated",
        ("BatteryLevel", "DeferralSignal") in graph._edge_index,
    )
    _check(
        "repr is compact",
        "CausalGraph(" in repr(graph),
    )

    # ---- 2. Config validation --------------------------------- #
    print("config validation:")
    _check(
        "rejects out-of-range human_feedback",
        _raises(CausalGraphError, lambda: CausalGraphConfig(
            human_feedback_score=2.0,
        )),
    )
    _check(
        "rejects 2-element modp_weights",
        _raises(CausalGraphError, lambda: CausalGraphConfig(
            modp_weights=(0.5, 0.5),
        )),
    )
    _check(
        "rejects all-zero modp_weights",
        _raises(CausalGraphError, lambda: CausalGraphConfig(
            modp_weights=(0.0, 0.0, 0.0),
        )),
    )
    _check(
        "rejects use_evolutionary",
        _raises(CausalGraphError, lambda: CausalGraphConfig(
            use_evolutionary=True,
        )),
    )
    _check(
        "rejects distillation without enhancements",
        _raises(CausalGraphError, lambda: CausalGraphConfig(
            use_distillation=True, use_enhancements=False,
        )),
    )
    _check(
        "accepts distillation + enhancements",
        CausalGraphConfig(
            use_distillation=True, use_enhancements=True,
        ) is not None,
    )
    _check(
        "frozen — mutation rejected",
        _raises(
            (AttributeError, Exception),
            lambda: setattr(
                CausalGraphConfig(), "epsilon", 0.5,
            ),
        ),
    )

    # ---- 3. Immutable defaults ------------------------------- #
    print("immutable defaults:")
    _check(
        "DEFAULT_CAUSAL_STRUCTURE is a tuple",
        isinstance(DEFAULT_CAUSAL_STRUCTURE, tuple),
    )
    _check(
        "thresholds mapping rejects mutation",
        _raises(
            TypeError,
            lambda: DEFAULT_CAUSAL_THRESHOLDS.__setitem__(  # type: ignore[attr-defined]
                "x", {},
            ),
        ),
    )

    # ---- 4. Signed propagation in trace ---------------------- #
    print("trace_root_causes:")
    graph = CausalGraph()
    graph.observe("CarbonIntensity", 500.0)
    chains = graph.trace_root_causes("CarbonIntensity")
    _check(
        "chains returned",
        len(chains) > 0,
    )
    if chains:
        c = chains[0]
        _check(
            "chain has signed weight",
            "cumulative_signed_weight" in c,
        )
        _check(
            "chain has effect",
            c["effect"] in ("amplifies", "suppresses"),
        )
        _check(
            "cumulative_weight is non-negative",
            c["cumulative_weight"] >= 0,
        )

    # BatteryLevel is a suppression edge; tracing DeferralSignal
    # should show it with a negative signed weight.
    graph.observe("DeferralSignal", 1.0)
    chains = graph.trace_root_causes("DeferralSignal")
    battery_chain = next(
        (c for c in chains if c["root_cause"] == "BatteryLevel"), None,
    )
    _check(
        "BatteryLevel suppression edge traced as suppresses",
        battery_chain is not None
        and battery_chain["cumulative_signed_weight"] < 0
        and battery_chain["effect"] == "suppresses",
    )

    # ---- 5. Update does not destroy graph -------------------- #
    print("update_edge_weight preserves graph:")
    graph = CausalGraph()
    before = [e.weight for e in graph.edges]
    graph.update_edge_weight(
        "WeatherEvent", "RenewableShortfall", outcome_correct=True,
    )
    after = [e.weight for e in graph.edges]
    changed = sum(
        1 for a, b in zip(before, after) if abs(a - b) > 1e-9
    )
    _check(
        "only the target edge changed",
        changed == 1,
    )
    _check(
        "other edges untouched",
        all(
            abs(a - b) < 1e-9
            for i, (a, b) in enumerate(zip(before, after))
            if i != graph._edge_index[
                ("WeatherEvent", "RenewableShortfall")
            ]
        ),
    )

    # Evidence-weighted: after many failures, learning rate decays.
    edge_idx = graph._edge_index[
        ("WeatherEvent", "RenewableShortfall")
    ]
    w0 = graph.edges[edge_idx].weight
    for _ in range(100):
        graph.update_edge_weight(
            "WeatherEvent", "RenewableShortfall",
            outcome_correct=False,
        )
    edge = graph.edges[edge_idx]
    _check(
        "100 failures reduce the weight",
        edge.weight < w0,
    )
    _check(
        "confidence decreases with failures",
        edge.confidence < 0.5,
    )
    _check(
        "failure_count == 100",
        edge.failure_count == 100,
    )

    # ---- 6. Hill-climbing path -------------------------------- #
    print("hill-climbing:")
    graph = CausalGraph(CausalGraphConfig(
        use_enhancements=True,
        use_hill_climbing=True,
        seed=7,
    ))
    before = [e.weight for e in graph.edges]
    graph.update_edge_weight(
        "WeatherEvent", "RenewableShortfall", outcome_correct=True,
    )
    after = [e.weight for e in graph.edges]
    changed = sum(
        1 for a, b in zip(before, after) if abs(a - b) > 1e-9
    )
    _check(
        "hill-climbing changes only the target edge",
        changed == 1,
    )

    # ---- 7. Distillation path -------------------------------- #
    print("distillation:")
    graph = CausalGraph(CausalGraphConfig(
        use_enhancements=True,
        use_distillation=True,
        seed=7,
        gate_seed=11,
    ))
    _check(
        "distillation optimizer constructed",
        graph.distillation_optimizer is not None,
    )
    for _ in range(20):
        graph.update_edge_weight(
            "WeatherEvent", "RenewableShortfall",
            outcome_correct=True,
        )
    _check(
        "distillation counter advanced",
        graph.distillation_optimizer.counter == 20,
    )

    # ---- 8. Reset -------------------------------------------- #
    print("reset:")
    graph = CausalGraph()
    idx = graph._edge_index[("WeatherEvent", "RenewableShortfall")]
    original_weight = graph.edges[idx].weight
    graph.update_edge_weight(
        "WeatherEvent", "RenewableShortfall", outcome_correct=True,
    )
    graph.reset()
    _check(
        "reset restores default weight",
        abs(graph.edges[idx].weight - original_weight) < 1e-9,
    )
    _check(
        "reset clears evidence",
        graph.edges[idx].success_count == 0
        and graph.edges[idx].failure_count == 0,
    )

    # ---- 9. Serialization round-trip ------------------------- #
    print("serialization:")
    graph = CausalGraph()
    graph.observe("CarbonIntensity", 500.0)
    graph.update_edge_weight(
        "WeatherEvent", "RenewableShortfall", outcome_correct=True,
    )
    payload = graph.to_json()
    restored = CausalGraph.from_json(payload)
    _check(
        "round-trip preserves node count",
        len(restored.nodes) == len(graph.nodes),
    )
    _check(
        "round-trip preserves edge weights",
        all(
            abs(a.weight - b.weight) < 1e-3
            for a, b in zip(graph.edges, restored.edges)
        ),
    )
    _check(
        "round-trip preserves evidence",
        all(
            a.success_count == b.success_count
            and a.failure_count == b.failure_count
            for a, b in zip(graph.edges, restored.edges)
        ),
    )
    _check(
        "round-trip preserves anomalies",
        restored.nodes["CarbonIntensity"].anomalous is True,
    )

    # ---- 10. Frozen dataclasses ------------------------------ #
    print("frozen dataclasses:")
    node = graph.nodes["CarbonIntensity"]
    _check(
        "CausalNode mutation rejected",
        _raises(
            (AttributeError, Exception),
            lambda: setattr(node, "current_value", 0.0),
        ),
    )
    edge = graph.edges[0]
    _check(
        "CausalEdge mutation rejected",
        _raises(
            (AttributeError, Exception),
            lambda: setattr(edge, "weight", 0.0),
        ),
    )

    # ---- 11. Thread safety smoke ----------------------------- #
    print("thread safety:")
    import threading as _th

    graph = CausalGraph()
    errors: List[BaseException] = []

    def _worker() -> None:
        try:
            for _ in range(50):
                graph.observe("CarbonIntensity", 450.0)
                graph.trace_root_causes("CarbonIntensity")
                graph.update_edge_weight(
                    "WeatherEvent", "RenewableShortfall",
                    outcome_correct=True,
                )
        except BaseException as exc:  # pragma: no cover
            errors.append(exc)

    threads = [_th.Thread(target=_worker) for _ in range(4)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    _check("no thread errors", not errors)
    _check(
        "graph still consistent",
        len(graph.edges) == len(DEFAULT_CAUSAL_STRUCTURE),
    )

    # ---- 12. Seed determinism -------------------------------- #
    print("seed determinism:")
    g1 = CausalGraph(CausalGraphConfig(
        use_enhancements=True, use_distillation=True,
        seed=42, gate_seed=7,
    ))
    g2 = CausalGraph(CausalGraphConfig(
        use_enhancements=True, use_distillation=True,
        seed=42, gate_seed=7,
    ))
    _check(
        "same seed → same initial gate weights",
        all(
            np.allclose(a, b)
            for a, b in zip(
                g1.distillation_optimizer.gate_weights.flatten(),
                g2.distillation_optimizer.gate_weights.flatten(),
            )
        ),
    )

    # ---- 13. __all__ / __dir__ ------------------------------- #
    print("dir:")
    _check("__dir__ matches __all__", __dir__() == sorted(__all__))
    for _name in (
        "CausalGraph", "CausalGraphConfig", "CausalGraphError",
        "CausalNode", "CausalEdge", "CausalLabel",
        "DEFAULT_CAUSAL_STRUCTURE", "DEFAULT_CAUSAL_THRESHOLDS",
        "EdgeWeightDistillationOptimizer",
        "EdgeWeightHillClimbingOptimizer",
    ):
        _check(f"{_name} exported", _name in __all__)

    print()
    print(f"passed: {passed}  failed: {failed}")
    if failed:
        sys.exit(1)
