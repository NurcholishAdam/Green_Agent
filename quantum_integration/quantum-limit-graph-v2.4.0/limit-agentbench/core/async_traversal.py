# -*- coding: utf-8 -*-
"""
async_traversal.py — Recommendation 2 (Enhanced)
================================================

Async graph traversal for large-scale graphs, with optional advanced
decision-making via distillation, MoE gating, MODP, RLHF, and LIMIT
Graph metrics.

Original behaviour retained
---------------------------
- Hybrid routing: sync for small graphs, async for large graphs.
- Public API remains ``route()`` and ``run_sync()``.

Enhancements (enabled via ``use_enhancements=True``)
----------------------------------------------------
- A distillation optimizer with MoE gating decides sync vs async based
  on graph size, carbon intensity, human feedback, and graph metrics.
- After traversal, a MODP reward is computed and used to update the
  optimizer.
- RLHF teacher influences the decision based on human feedback.
- LIMIT Graph centrality / connectivity are part of the state.

**Persistent optimizer**
------------------------
The prior version constructed a fresh optimizer on every ``route()``
call and discarded it before returning, so ``student_weights`` were
always zero at decision time — the "distillation" reduced to a static
teacher-argmax and never learned. This version threads an optimizer
through ``route()``. Callers who want learning across calls pass one
shared instance::

    optimizer = TraversalDistillationOptimizer({"seed": 7})
    for task in workload:
        result = await route(task.graph, "trace_root_causes",
                             use_enhancements=True,
                             optimizer=optimizer)
        # optimizer's student now learns from each call.

Callers who don't pass an optimizer get a one-shot decision from a
fresh instance (same behaviour as before); the smoke test documents
this difference.

**Strategy honesty**
--------------------
``TraversalResult`` carries ``strategy_requested`` (what the
optimizer/threshold asked for) and ``strategy_used`` (what actually
ran). ``used_async`` is kept for backwards compatibility and equals
``strategy_used == "async"``. ``decision_source`` is ``"threshold"``
when the caller did not request enhancement, or when the enhancement
was requested but unavailable, and ``"distillation"`` when the
optimizer produced the decision.

Key fixes over the previous version
-----------------------------------
- **Persistent optimizer.** ``route()`` accepts ``optimizer=`` and
  ``optimizer_config=``; the optimizer is threaded through the call.
  Without persistence, the "distillation" added overhead and did no
  learning.
- **Working traversal implementations.** ``AsyncBFS``,
  ``AsyncDPAlign``, ``AsyncDFS``, and ``_dp_align_sync`` were
  ``pass``-body placeholders in the previous version; they now
  provide correct reference implementations that run the sync code
  in a thread pool, preserving behaviour while exposing the async
  interface. A truly parallel implementation can be swapped in later.
- **Corrected distillation optimizer.** 4-tuple ``select_action``
  (``action, state_vec, teacher_outputs, teacher_combined``); 5-arg
  ``update`` (no ``next_state_vec``); gate gradient
  ``teacher_outputs @ error`` (correct shape even though
  ``n_actions=2`` and ``n_teachers=3``); per-instance RNGs; config
  validated at construction; ``update`` under an ``RLock``;
  ``_normalize_teacher`` slices over-sized outputs before any
  ``np.pad``.
- **Reward has a success gate and consistent units.**
  ``reward = modp_score * failure_reward_multiplier`` (default
  ``0.0``). Energy is computed in Wh from a named 250 W average
  power draw, then converted to kWh; carbon is ``energy_kwh ×
  carbon_intensity / 1000``. The previous version returned a
  positive reward for any elapsed time and used an arbitrary
  ``0.0001`` multiplier.
- **Named MODP constants.** ``MAX_LATENCY_MS``, ``MAX_ENERGY_KWH``,
  ``MAX_CARBON_KG``; each uses ``_clamp01`` on both sides.
- **Named failure multiplier.** ``FAILURE_REWARD_MULTIPLIER = 0.0``;
  overridable per call via ``config["failure_reward_multiplier"]``.
- **Explicit ``route`` signature.** No more silent ``**kwargs``
  misspellings. Legacy callers that passed ``carbon_intensity`` /
  ``human_feedback_score`` / ``graph_metrics`` / ``use_enhancements``
  through ``**kwargs`` keep working via ``**legacy_kwargs`` with a
  ``DeprecationWarning`` for unknown keys.
- **No ``sys.path`` side effect.** The import-time
  ``sys.path.insert(0, ...)`` was removed; the module uses relative
  imports (``from .causal_graph import ...``).
- **``assert isinstance`` → ``raise TraversalError``.** The
  ``AssertionError`` was stripped under ``python -O``; the explicit
  exception is preserved.
- **``run_sync`` no longer pays async overhead for small graphs.**
  It short-circuits to a pure-sync path when the graph is small and
  no enhancement is requested. Inside a running loop it raises
  :class:`TraversalError` instead of blocking the loop.
- **``graph_size`` in the result matches the value used for the
  decision.** The previous version reported
  ``len(graph_obj.nodes)`` regardless of ``method``; a ``dp_align``
  call reported ``0``.
- **``strategy_requested`` / ``strategy_used`` fields** on
  :class:`TraversalResult`; the previous ``execution_strategy`` was
  ambiguous.
- **``student_counter`` → ``counter``.** The stats key name lied
  about its content.
- **Frozen, validated records.** :class:`TraversalDecisionState` and
  :class:`TraversalResult` reject malformed fields.
- **Config propagated to the optimizer.** The previous version
  constructed the optimizer without a config, so ``epsilon``,
  ``train_every``, ``modp_weights``, etc. were silently ignored.
- **Narrow error type.** :class:`TraversalError` replaces the
  previous ``ValueError`` / ``AssertionError`` / ``KeyError`` leaks.
- **``strict`` mode.** ``route(..., strict=True)`` raises when
  ``use_enhancements=True`` and enhancements are unavailable, and
  when a traversal fails.
- **Lazy ``%s`` logging** via a module logger; ``logger.exception``
  on caught exceptions.
- **``__version__``, ``__all__``, ``__dir__``, and a ``_check()``-based
  smoke test.**

Compatibility
-------------
- ``route(graph_obj, method, **kwargs)`` — **callers** that only passed
  the legacy keyword arguments (``use_enhancements``,
  ``carbon_intensity``, ``human_feedback_score``, ``graph_metrics``)
  continue to work. New keyword-only arguments are ``optimizer``,
  ``optimizer_config``, and ``strict``. Unknown ``**kwargs`` emit a
  ``DeprecationWarning``.
- ``run_sync(graph_obj, method, **kwargs)`` — same signature. Behaviour
  change: small graphs short-circuit to sync without touching
  ``asyncio``. Inside a running loop, raises instead of blocking.
- ``TraversalResult`` gained ``strategy_requested`` and
  ``strategy_used``. ``used_async`` is preserved and derived from
  ``strategy_used``.
- ``TraversalDistillationOptimizer.select_action`` returns 4 values
  (previously 3); ``update`` takes 5 args (no ``next_state_vec``).
- ``AsyncBFS``, ``AsyncDPAlign``, ``AsyncDFS``, ``_dp_align_sync`` are
  now working implementations rather than placeholders.

Requires Python 3.8+. ``slots=True`` on dataclasses requires 3.10+;
drop ``slots=True`` for 3.8/3.9.
"""

from __future__ import annotations

import asyncio
import enum
import logging
import math
import random
import threading
import time
import warnings
from collections import deque
from dataclasses import dataclass, field
from typing import (
    Any, Callable, Deque, Dict, Final, Iterable, List, Mapping, Optional,
    Sequence, Tuple,
)

import numpy as np


logger = logging.getLogger(__name__)

__version__ = "2.5.0"


# --------------------------------------------------------------------------- #
# Errors
# --------------------------------------------------------------------------- #
class TraversalError(ValueError):
    """Raised for invalid traversal inputs or execution failures."""


# --------------------------------------------------------------------------- #
# Constants
# --------------------------------------------------------------------------- #
# Node count above which the threshold heuristic switches to async.
ASYNC_THRESHOLD: Final[int] = 200
# Upper bound for concurrent async tasks.
MAX_CONCURRENCY: Final[int] = 16

# Feature vector dimension: graph_size + carbon + human + centrality
# + connectivity = 5.
FEATURE_DIM: Final[int] = 5
N_ACTIONS: Final[int] = 2

# Reward ceilings for the domain (graph traversal is fast; the
# energy/carbon scale is therefore much smaller than model inference).
MAX_LATENCY_MS: Final[float] = 5_000.0
MAX_ENERGY_KWH: Final[float] = 0.001        # 1 Wh
MAX_CARBON_KG: Final[float] = 0.001         # 1 g CO2
DEFAULT_MODP_WEIGHTS: Final[Tuple[float, ...]] = (0.5, 0.3, 0.2)
FAILURE_REWARD_MULTIPLIER: Final[float] = 0.0

# Average power draw for energy estimates (W).
ASSUMED_POWER_W: Final[float] = 250.0

# Normalisation for the graph-size feature.
GRAPH_SIZE_DENOM: Final[float] = 500.0
CARBON_INTENSITY_DENOM: Final[float] = 500.0

DEFAULT_REPLAY_SIZE: Final[int] = 2_000
DEFAULT_TRAIN_EVERY: Final[int] = 10
DEFAULT_BATCH_SIZE: Final[int] = 8

# Strategy strings; mirror the values used throughout the tree.
STRATEGY_SYNC: Final[str] = "sync"
STRATEGY_ASYNC: Final[str] = "async"
DECISION_SOURCE_THRESHOLD: Final[str] = "threshold"
DECISION_SOURCE_DISTILLATION: Final[str] = "distillation"

DEFAULT_EDIT_COSTS: Final[Mapping[str, float]] = {
    "insert": 1.0, "delete": 1.0, "relabel": 0.7,
}


# --------------------------------------------------------------------------- #
# Validation helpers
# --------------------------------------------------------------------------- #
def _clamp01(value: float) -> float:
    return max(0.0, min(float(value), 1.0))


def _finite(name: str, value: Any) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise TraversalError(f"{name} must be numeric.")
    fv = float(value)
    if not math.isfinite(fv):
        raise TraversalError(f"{name} must be finite, got {value!r}.")
    return fv


def _nonneg(name: str, value: Any) -> float:
    fv = _finite(name, value)
    if fv < 0.0:
        raise TraversalError(f"{name} must be >= 0, got {fv}.")
    return fv


def _unit_interval(name: str, value: Any) -> float:
    fv = _finite(name, value)
    if not 0.0 <= fv <= 1.0:
        raise TraversalError(f"{name} must be in [0, 1], got {fv}.")
    return fv


def _optional_unit_interval(name: str, value: Any) -> Optional[float]:
    if value is None:
        return None
    return _unit_interval(name, value)


def _optional_float_mapping(
    name: str, value: Any,
) -> Optional[Dict[str, float]]:
    if value is None:
        return None
    if not isinstance(value, Mapping):
        raise TraversalError(
            f"{name} must be a Mapping or None, got "
            f"{type(value).__name__}."
        )
    out: Dict[str, float] = {}
    for k, v in value.items():
        if not isinstance(k, str) or not k:
            raise TraversalError(
                f"{name} keys must be non-empty strings, got {k!r}."
            )
        out[k] = _finite(f"{name}[{k!r}]", v)
    return out


# --------------------------------------------------------------------------- #
# Optional enhancement imports
# --------------------------------------------------------------------------- #
class _UnavailableComponent:
    """Placeholder for an unavailable dependency; raises on use."""

    __slots__ = ("_name",)

    def __init__(self, name: str, *a: Any, **kw: Any) -> None:
        self._name = name
        raise TraversalError(
            f"Dependency '{name}' is unavailable; install it to use "
            f"the corresponding feature."
        )


try:  # Relative imports (no sys.path manipulation).
    from .causal_graph import CausalGraph  # type: ignore
except ImportError as _exc:  # pragma: no cover — defensive
    logger.info("CausalGraph unavailable: %s", _exc)
    CausalGraph = _UnavailableComponent  # type: ignore[assignment,misc]


try:
    from src.enhancements.schemas.node_descriptor import (  # noqa: F401
        NodeDescriptor,
    )
    from src.enhancements.schemas.workload_descriptor import (  # noqa: F401
        WorkloadDescriptor,
    )
    from src.enhancements.zero_trust_architecture import (  # noqa: F401
        ZeroTrustArchitecture,
    )
    from src.enhancements.schemas.feedback_event import (  # noqa: F401
        FeedbackEvent,
    )

    ENHANCEMENTS_AVAILABLE: bool = True
except ImportError as _exc:  # pragma: no cover — defensive
    ENHANCEMENTS_AVAILABLE = False
    logger.info("Enhancement schemas unavailable: %s", _exc)
    NodeDescriptor = _UnavailableComponent  # type: ignore[assignment,misc]
    WorkloadDescriptor = _UnavailableComponent  # type: ignore[assignment,misc]
    ZeroTrustArchitecture = _UnavailableComponent  # type: ignore[assignment,misc]
    FeedbackEvent = _UnavailableComponent  # type: ignore[assignment,misc]


# --------------------------------------------------------------------------- #
# Decision state
# --------------------------------------------------------------------------- #
@dataclass(frozen=True, slots=True)
class TraversalDecisionState:
    """Immutable feature vector for the sync-vs-async decision."""

    graph_size: int
    carbon: float
    human: float
    centrality: float
    connectivity: float

    def __post_init__(self) -> None:
        if isinstance(self.graph_size, bool) or not isinstance(
            self.graph_size, int
        ):
            raise TraversalError("graph_size must be an int.")
        if self.graph_size < 0:
            raise TraversalError("graph_size must be >= 0.")
        for name in ("carbon", "human", "centrality", "connectivity"):
            _unit_interval(name, getattr(self, name))

    @classmethod
    def from_raw(
        cls,
        *,
        graph_size: int,
        carbon_intensity: float = 400.0,
        human_feedback: float = 0.5,
        graph_centrality: float = 0.5,
        graph_connectivity: float = 0.5,
    ) -> "TraversalDecisionState":
        return cls(
            graph_size=int(graph_size),
            carbon=_clamp01(
                max(float(carbon_intensity), 0.0)
                / CARBON_INTENSITY_DENOM
            ),
            human=_clamp01(float(human_feedback)),
            centrality=_clamp01(float(graph_centrality)),
            connectivity=_clamp01(float(graph_connectivity)),
        )

    def to_vector(self) -> np.ndarray:
        return np.array([
            min(self.graph_size / GRAPH_SIZE_DENOM, 1.0),
            self.carbon,
            self.human,
            self.centrality,
            self.connectivity,
        ], dtype=np.float32)


# --------------------------------------------------------------------------- #
# Replay entry
# --------------------------------------------------------------------------- #
@dataclass(frozen=True, slots=True)
class _ReplayEntry:
    state_vec: np.ndarray
    action: int
    reward: float
    teacher_outputs: np.ndarray
    teacher_combined: np.ndarray


# --------------------------------------------------------------------------- #
# Distillation optimizer (corrected)
# --------------------------------------------------------------------------- #
class TraversalDistillationOptimizer:
    """
    Multi-teacher distillation with MoE gating to select sync vs async.

    Actions: ``0 = sync``, ``1 = async``.

    ``select_action`` returns ``(action, state_vec, teacher_outputs,
    teacher_combined)``. ``update`` takes
    ``(state_vec, action, reward, teacher_outputs, teacher_combined)``.

    .. note::
        The corrected implementation here matches
        :class:`src.integrations.base_adapter.DistillationOptimizer`.
        A future refactor should extract that class to
        ``src/enhancements/distillation.py`` and have this file import
        it.
    """

    def __init__(
        self, config: Optional[Dict[str, Any]] = None,
    ) -> None:
        self.config = self._validate_config(config or {})

        self.feature_dim: int = FEATURE_DIM
        self.n_actions: int = N_ACTIONS

        self.student_weights = np.zeros(
            (self.feature_dim, self.n_actions), dtype=np.float64,
        )
        self.student_bias = np.zeros(self.n_actions, dtype=np.float64)

        self.lr: float = self.config["distillation_lr"]
        self.epsilon: float = self.config["epsilon"]
        self.distill_w: float = self.config["distill_weight"]
        self.rl_w: float = self.config["rl_weight"]
        self.train_every: int = self.config["train_every"]
        self.batch_size: int = self.config["batch_size"]

        self.replay_buffer: Deque[_ReplayEntry] = deque(
            maxlen=self.config["replay_size"],
        )
        self.counter: int = 0
        self._lock = threading.RLock()

        self._py_rng = random.Random(self.config["seed"])
        self._np_rng = np.random.default_rng(self.config["gate_seed"])

        self.teachers = (
            self._rule_teacher,
            self._rlhf_teacher,
            self._historical_teacher,
        )
        self.gate_weights = (
            self._np_rng.standard_normal(
                (self.feature_dim, len(self.teachers)),
            ) * 0.01
        )
        self.gate_bias = np.zeros(len(self.teachers), dtype=np.float64)
        self.gate_lr: float = self.config["gating_lr"]

    # ---------------------------------------------------------------- config
    @staticmethod
    def _validate_config(config: Dict[str, Any]) -> Dict[str, Any]:
        if not isinstance(config, dict):
            raise TraversalError(
                f"config must be a dict, got {type(config).__name__}."
            )

        def _num(
            name: str, raw: Any, default: float, *, positive: bool,
        ) -> float:
            v = default if raw is None else raw
            if isinstance(v, bool) or not isinstance(v, (int, float)):
                raise TraversalError(f"{name} must be numeric.")
            fv = float(v)
            if not math.isfinite(fv):
                raise TraversalError(f"{name} must be finite.")
            if positive and fv <= 0:
                raise TraversalError(f"{name} must be > 0.")
            if not positive and fv < 0:
                raise TraversalError(f"{name} must be >= 0.")
            return fv

        def _int(
            name: str, raw: Any, default: int, *, positive: bool,
        ) -> int:
            v = default if raw is None else raw
            if isinstance(v, bool) or not isinstance(v, int):
                raise TraversalError(f"{name} must be an int.")
            if positive and v <= 0:
                raise TraversalError(f"{name} must be > 0.")
            if not positive and v < 0:
                raise TraversalError(f"{name} must be >= 0.")
            return v

        out = dict(config)
        out["distillation_lr"] = _num(
            "distillation_lr", config.get("distillation_lr"),
            0.01, positive=True,
        )
        out["gating_lr"] = _num(
            "gating_lr", config.get("gating_lr"),
            0.005, positive=True,
        )
        out["epsilon"] = _num(
            "epsilon", config.get("epsilon"), 0.1, positive=False,
        )
        if out["epsilon"] > 1.0:
            raise TraversalError("epsilon must be <= 1.0.")
        out["distill_weight"] = _num(
            "distill_weight", config.get("distill_weight"),
            0.7, positive=False,
        )
        out["rl_weight"] = _num(
            "rl_weight", config.get("rl_weight"),
            0.3, positive=False,
        )
        if out["distill_weight"] + out["rl_weight"] <= 0:
            raise TraversalError(
                "distill_weight + rl_weight must be > 0."
            )
        out["train_every"] = _int(
            "train_every", config.get("train_every"),
            DEFAULT_TRAIN_EVERY, positive=True,
        )
        out["batch_size"] = _int(
            "batch_size", config.get("batch_size"),
            DEFAULT_BATCH_SIZE, positive=True,
        )
        out["replay_size"] = _int(
            "replay_size", config.get("replay_size"),
            DEFAULT_REPLAY_SIZE, positive=True,
        )
        out["seed"] = config.get("seed")
        out["gate_seed"] = config.get("gate_seed")
        return out

    # ---------------------------------------------------------------- teachers
    def _rule_teacher(self, state: TraversalDecisionState) -> np.ndarray:
        if state.graph_size > ASYNC_THRESHOLD:
            return np.array([0.1, 0.9])
        return np.array([0.8, 0.2])

    def _rlhf_teacher(self, state: TraversalDecisionState) -> np.ndarray:
        probs = np.array([0.5, 0.5])
        if state.human > 0.7:
            probs[1] += 0.2
        elif state.human < 0.3:
            probs[0] += 0.2
        return probs / probs.sum()

    def _historical_teacher(
        self, state: TraversalDecisionState,
    ) -> np.ndarray:
        if state.centrality > 0.7:
            return np.array([0.3, 0.7])
        return np.array([0.6, 0.4])

    # ---------------------------------------------------------------- gating
    def _gate_forward(self, state_vec: np.ndarray) -> np.ndarray:
        logits = state_vec @ self.gate_weights + self.gate_bias
        logits = logits - np.max(logits)
        exp = np.exp(logits)
        return exp / exp.sum()

    @staticmethod
    def _normalize_teacher(prob: Any, n_actions: int) -> np.ndarray:
        arr = np.asarray(prob, dtype=np.float64).reshape(-1)
        if arr.size > n_actions:
            arr = arr[:n_actions]
        elif arr.size < n_actions:
            arr = np.pad(arr, (0, n_actions - arr.size), "constant")
        if not np.all(np.isfinite(arr)):
            raise TraversalError(
                "teacher output contains non-finite values."
            )
        total = arr.sum()
        if total <= 0:
            return np.ones(n_actions, dtype=np.float64) / n_actions
        return arr / total

    # ---------------------------------------------------------------- action
    def select_action(
        self,
        state: TraversalDecisionState,
        exploration: bool = True,
    ) -> Tuple[int, np.ndarray, np.ndarray, np.ndarray]:
        if not isinstance(state, TraversalDecisionState):
            raise TraversalError(
                "state must be a TraversalDecisionState."
            )
        x = state.to_vector()
        if x.shape[0] != self.feature_dim:
            raise TraversalError(
                f"feature vector has {x.shape[0]} elements; expected "
                f"{self.feature_dim}."
            )

        teacher_outputs = np.stack([
            self._normalize_teacher(t(state), self.n_actions)
            for t in self.teachers
        ])
        gate = self._gate_forward(x)
        teacher_combined = (gate[:, None] * teacher_outputs).sum(axis=0)
        teacher_combined = teacher_combined / teacher_combined.sum()

        logits = x @ self.student_weights + self.student_bias
        logits = logits - logits.max()
        student_probs = np.exp(logits)
        student_probs = student_probs / student_probs.sum()

        with self._lock:
            if exploration and self._py_rng.random() < self.epsilon:
                action = self._py_rng.randint(0, self.n_actions - 1)
            else:
                combined = (
                    0.8 * student_probs + 0.2 * teacher_combined
                )
                action = int(np.argmax(combined))

        return action, x, teacher_outputs, teacher_combined

    # ---------------------------------------------------------------- update
    def update(
        self,
        state_vec: np.ndarray,
        action: int,
        reward: float,
        teacher_outputs: np.ndarray,
        teacher_combined: np.ndarray,
    ) -> None:
        if not math.isfinite(float(reward)):
            raise TraversalError(
                f"reward must be finite, got {reward!r}."
            )
        with self._lock:
            self.replay_buffer.append(_ReplayEntry(
                state_vec=state_vec,
                action=int(action),
                reward=float(reward),
                teacher_outputs=teacher_outputs,
                teacher_combined=teacher_combined,
            ))
            self.counter += 1
            if (
                self.counter % self.train_every != 0
                or len(self.replay_buffer) < self.batch_size
            ):
                return
            batch = self._py_rng.sample(
                list(self.replay_buffer),
                min(self.batch_size, len(self.replay_buffer)),
            )
            self._train_batch(batch)

    def _train_batch(self, batch: List[_ReplayEntry]) -> None:
        for entry in batch:
            s = entry.state_vec
            cur_probs = self._student_probs(s)

            one_hot = np.zeros(self.n_actions)
            one_hot[entry.action] = 1.0
            grad_distill = -(entry.teacher_combined - cur_probs)
            grad_rl = -entry.reward * (one_hot - cur_probs)
            grad = self.distill_w * grad_distill + self.rl_w * grad_rl
            self.student_weights -= self.lr * np.outer(s, grad)
            self.student_bias -= self.lr * grad

            # Gate gradient: correct shape (n_teachers,) via
            # teacher_outputs @ error — holds even when
            # n_actions != n_teachers.
            target = self._student_probs(s)
            gate = self._gate_forward(s)
            combined = (
                gate[:, None] * entry.teacher_outputs
            ).sum(axis=0)
            combined = combined / combined.sum()
            error = combined - target
            grad_gate = entry.teacher_outputs @ error
            self.student_weights = self.student_weights  # noqa: PLW0127
            self.gate_weights -= self.gate_lr * np.outer(s, grad_gate)
            self.gate_bias -= self.gate_lr * grad_gate

    def _student_probs(self, state_vec: np.ndarray) -> np.ndarray:
        logits = state_vec @ self.student_weights + self.student_bias
        logits = logits - logits.max()
        probs = np.exp(logits)
        return probs / probs.sum()

    # ---------------------------------------------------------------- stats
    def statistics(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "counter": self.counter,
                "buffer_size": len(self.replay_buffer),
                "feature_dim": self.feature_dim,
                "n_actions": self.n_actions,
                "epsilon": self.epsilon,
            }


# --------------------------------------------------------------------------- #
# Traversal result
# --------------------------------------------------------------------------- #
_VALID_DECISION_SOURCES: Final[frozenset] = frozenset({
    DECISION_SOURCE_THRESHOLD, DECISION_SOURCE_DISTILLATION,
})
_VALID_STRATEGIES: Final[frozenset] = frozenset({
    STRATEGY_SYNC, STRATEGY_ASYNC,
})


@dataclass(frozen=True, slots=True)
class TraversalResult:
    """Immutable result of a routed traversal."""

    method: str
    graph_size: int
    used_async: bool
    elapsed_ms: float
    result: Any
    modp_reward: Optional[float] = None
    decision_source: str = DECISION_SOURCE_THRESHOLD
    distillation_stats: Optional[Dict[str, Any]] = None
    strategy_requested: str = STRATEGY_SYNC
    strategy_used: str = STRATEGY_SYNC

    def __post_init__(self) -> None:
        if not isinstance(self.method, str) or not self.method:
            raise TraversalError("method must be a non-empty string.")
        if isinstance(self.graph_size, bool) or not isinstance(
            self.graph_size, int
        ):
            raise TraversalError("graph_size must be an int.")
        if self.graph_size < 0:
            raise TraversalError("graph_size must be >= 0.")
        if not isinstance(self.used_async, bool):
            raise TraversalError("used_async must be a bool.")
        _nonneg("elapsed_ms", self.elapsed_ms)
        if self.decision_source not in _VALID_DECISION_SOURCES:
            raise TraversalError(
                f"decision_source must be one of "
                f"{sorted(_VALID_DECISION_SOURCES)}, got "
                f"{self.decision_source!r}."
            )
        _optional_unit_interval("modp_reward", self.modp_reward)
        if self.strategy_requested not in _VALID_STRATEGIES:
            raise TraversalError(
                f"strategy_requested must be one of "
                f"{sorted(_VALID_STRATEGIES)}, got "
                f"{self.strategy_requested!r}."
            )
        if self.strategy_used not in _VALID_STRATEGIES:
            raise TraversalError(
                f"strategy_used must be one of "
                f"{sorted(_VALID_STRATEGIES)}, got "
                f"{self.strategy_used!r}."
            )
        # The consistency invariant: used_async is derived from
        # strategy_used, so a caller can rely on either field.
        expected_used_async = self.strategy_used == STRATEGY_ASYNC
        if self.used_async != expected_used_async:
            raise TraversalError(
                "used_async must equal (strategy_used == 'async')."
            )
        if self.distillation_stats is not None and not isinstance(
            self.distillation_stats, Mapping
        ):
            raise TraversalError(
                "distillation_stats must be a Mapping or None."
            )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "method": self.method,
            "graph_size": self.graph_size,
            "used_async": self.used_async,
            "elapsed_ms": float(self.elapsed_ms),
            "result": self.result,
            "modp_reward": self.modp_reward,
            "decision_source": self.decision_source,
            "distillation_stats": (
                None if self.distillation_stats is None
                else dict(self.distillation_stats)
            ),
            "strategy_requested": self.strategy_requested,
            "strategy_used": self.strategy_used,
        }


# --------------------------------------------------------------------------- #
# Async BFS / DPAlign / DFS / sync DP align (working implementations)
# --------------------------------------------------------------------------- #
def _dp_align_sync(
    seq_a: Sequence[str],
    seq_b: Sequence[str],
    edit_costs: Mapping[str, float],
) -> float:
    """
    Compute the minimum edit cost between two sequences.

    Standard DP (Levenshtein-style) with configurable insert / delete /
    relabel costs. Returns the scalar cost as a ``float``.
    """
    m, n = len(seq_a), len(seq_b)
    insert = float(edit_costs["insert"])
    delete = float(edit_costs["delete"])
    relabel = float(edit_costs["relabel"])

    prev = [j * insert for j in range(n + 1)]
    for i in range(1, m + 1):
        curr = [i * delete] + [0.0] * n
        for j in range(1, n + 1):
            match = (
                0.0 if seq_a[i - 1] == seq_b[j - 1] else relabel
            )
            curr[j] = min(
                prev[j] + delete,
                curr[j - 1] + insert,
                prev[j - 1] + match,
            )
        prev = curr
    return float(prev[n])


class AsyncDPAlign:
    """Async wrapper around :func:`_dp_align_sync`."""

    @staticmethod
    async def align(
        seq_a: Sequence[str],
        seq_b: Sequence[str],
        edit_costs: Optional[Mapping[str, float]] = None,
    ) -> float:
        costs = dict(edit_costs or DEFAULT_EDIT_COSTS)
        return await asyncio.to_thread(_dp_align_sync, seq_a, seq_b, costs)


class AsyncBFS:
    """
    Async BFS traversal over a causal graph.

    Uses :meth:`CausalGraph.trace_root_causes` (the graph's sync
    entry point) via :func:`asyncio.to_thread`, so the async interface
    is preserved while the CPU-bound work runs off the event loop.
    A truly parallel implementation can replace ``trace`` later
    without changing the public interface.
    """

    def __init__(self, graph: Any) -> None:
        self.graph = graph

    async def trace(
        self,
        anomaly_variable: str,
        max_depth: int = 5,
        min_weight: float = 0.2,
    ) -> Any:
        if not isinstance(anomaly_variable, str) or not anomaly_variable:
            raise TraversalError(
                "anomaly_variable must be a non-empty string."
            )
        if isinstance(max_depth, bool) or not isinstance(max_depth, int):
            raise TraversalError("max_depth must be an int.")
        if max_depth < 1:
            raise TraversalError("max_depth must be >= 1.")
        _unit_interval("min_weight", min_weight)

        trace_method = getattr(self.graph, "trace_root_causes", None)
        if not callable(trace_method):
            raise TraversalError(
                f"{type(self.graph).__name__} does not implement "
                f"trace_root_causes(); AsyncBFS cannot trace it."
            )
        return await asyncio.to_thread(
            trace_method, anomaly_variable, max_depth, min_weight,
        )


class AsyncDFS:
    """
    Async DFS traversal over a causal graph.

    Symmetric to :class:`AsyncBFS`; the graph's DFS / fallback sync
    method is invoked on a thread so the async interface is preserved.
    """

    def __init__(self, graph: Any) -> None:
        self.graph = graph

    async def trace(
        self,
        anomaly_variable: str,
        max_depth: int = 5,
        min_weight: float = 0.2,
    ) -> Any:
        if not isinstance(anomaly_variable, str) or not anomaly_variable:
            raise TraversalError(
                "anomaly_variable must be a non-empty string."
            )
        if isinstance(max_depth, bool) or not isinstance(max_depth, int):
            raise TraversalError("max_depth must be an int.")
        if max_depth < 1:
            raise TraversalError("max_depth must be >= 1.")
        _unit_interval("min_weight", min_weight)

        # Prefer a DFS-specific method if present; fall back to
        # trace_root_causes so both BFS and DFS resolve through the
        # graph's own implementation.
        for attr in ("trace_root_causes_dfs", "trace_root_causes"):
            method = getattr(self.graph, attr, None)
            if callable(method):
                return await asyncio.to_thread(
                    method, anomaly_variable, max_depth, min_weight,
                )
        raise TraversalError(
            f"{type(self.graph).__name__} does not implement a DFS "
            f"or trace_root_causes method."
        )


# --------------------------------------------------------------------------- #
# Per-call decision record
# --------------------------------------------------------------------------- #
@dataclass(frozen=True, slots=True)
class _Decision:
    state_vec: np.ndarray
    action_idx: int
    teacher_outputs: np.ndarray
    teacher_combined: np.ndarray
    strategy_requested: str


# --------------------------------------------------------------------------- #
# Reward computation
# --------------------------------------------------------------------------- #
def _compute_reward(
    *,
    elapsed_ms: float,
    success: bool,
    carbon_intensity: float,
    weights: Optional[Sequence[float]] = None,
    failure_multiplier: float = FAILURE_REWARD_MULTIPLIER,
) -> float:
    """
    Compute the MODP reward in ``[0, 1]`` for a traversal.

    ``elapsed_ms``, ``carbon_intensity``, and ``success`` are the
    traversal's observable outcomes. Energy is estimated from the
    latency under a 250 W average power draw, converted to kWh; carbon
    from the kWh × grid intensity.
    """
    if isinstance(elapsed_ms, bool) or not isinstance(
        elapsed_ms, (int, float)
    ):
        raise TraversalError("elapsed_ms must be numeric.")
    fe = float(elapsed_ms)
    if not math.isfinite(fe) or fe < 0:
        raise TraversalError("elapsed_ms must be finite and >= 0.")

    # 250 W → Wh → kWh → kg CO2.
    energy_wh = (fe / 1000.0 / 3600.0) * ASSUMED_POWER_W
    energy_kwh = energy_wh / 1000.0
    carbon_kg = energy_kwh * max(float(carbon_intensity), 0.0) / 1000.0

    latency_norm = 1.0 - _clamp01(fe / MAX_LATENCY_MS)
    energy_norm = 1.0 - _clamp01(energy_kwh / MAX_ENERGY_KWH)
    carbon_norm = 1.0 - _clamp01(carbon_kg / MAX_CARBON_KG)

    w = np.asarray(
        list(weights) if weights is not None
        else list(DEFAULT_MODP_WEIGHTS),
        dtype=float,
    )
    if (
        w.shape != (3,)
        or not np.all(np.isfinite(w))
        or w.min() < 0.0
        or w.sum() <= 0.0
    ):
        raise TraversalError(
            f"modp_weights must be 3 finite non-negative values with "
            f"a positive sum; got {weights!r}."
        )
    w = w / w.sum()

    base = float(np.dot(
        [latency_norm, energy_norm, carbon_norm], w,
    ))
    return base if success else base * max(float(failure_multiplier), 0.0)


# --------------------------------------------------------------------------- #
# Graph size computation
# --------------------------------------------------------------------------- #
def _compute_graph_size(
    graph_obj: Any, method: str, kwargs: Mapping[str, Any],
) -> int:
    """Return the ``n`` used both for the decision and the result."""
    if method == "trace_root_causes":
        if not isinstance(graph_obj, CausalGraph):
            raise TraversalError(
                f"trace_root_causes requires a CausalGraph, got "
                f"{type(graph_obj).__name__}."
            )
        return len(graph_obj.nodes)
    if method == "dp_align":
        seq_a = kwargs.get("seq_a")
        seq_b = kwargs.get("seq_b")
        if seq_a is None or seq_b is None:
            raise TraversalError(
                "dp_align requires both 'seq_a' and 'seq_b'."
            )
        return len(seq_a) * len(seq_b)
    nodes = getattr(graph_obj, "nodes", None)
    if isinstance(nodes, Mapping):
        return len(nodes)
    return 0


# --------------------------------------------------------------------------- #
# Route
# --------------------------------------------------------------------------- #
async def route(
    graph_obj: Any,
    method: str,
    *,
    use_enhancements: bool = False,
    carbon_intensity: float = 400.0,
    human_feedback_score: float = 0.5,
    graph_metrics: Optional[Mapping[str, float]] = None,
    optimizer: Optional[TraversalDistillationOptimizer] = None,
    optimizer_config: Optional[Mapping[str, Any]] = None,
    strict: bool = False,
    **legacy_kwargs: Any,
) -> TraversalResult:
    """
    Route a traversal to sync or async.

    Parameters
    ----------
    graph_obj, method
        The graph (or, for ``dp_align``, any object) and the traversal
        method.
    use_enhancements : bool, default False
        When True and the enhancements package is available, a
        distillation optimizer picks sync vs async.
    carbon_intensity, human_feedback_score, graph_metrics
        Context for the enhancement decision. Ignored in legacy mode.
    optimizer : TraversalDistillationOptimizer, optional
        A **persistent** optimizer to reuse across calls. When
        provided, the optimizer's ``select_action`` produces the
        decision and its ``update`` trains it with the resulting
        reward. When omitted, a one-shot optimizer is created from
        ``optimizer_config`` (its learning is discarded).
    optimizer_config : Mapping, optional
        Configuration for the one-shot optimizer. Ignored when
        ``optimizer`` is supplied. Also carries ``modp_weights`` and
        ``failure_reward_multiplier`` for the reward.
    strict : bool, default False
        If True, raise when enhancements are requested but
        unavailable, or when a traversal fails.
    **legacy_kwargs
        Additional method-specific kwargs (``anomaly_variable``,
        ``max_depth``, ``min_weight``, ``seq_a``, ``seq_b``,
        ``edit_costs``). Unknown keys emit a ``DeprecationWarning``.
    """
    if not isinstance(method, str) or not method:
        raise TraversalError("method must be a non-empty string.")
    _nonneg("carbon_intensity", carbon_intensity)
    _unit_interval("human_feedback_score", human_feedback_score)
    gm = _optional_float_mapping("graph_metrics", graph_metrics) or {}

    if legacy_kwargs:
        known = {
            "anomaly_variable", "max_depth", "min_weight",
            "seq_a", "seq_b", "edit_costs",
        }
        unknown = set(legacy_kwargs) - known
        if unknown:
            warnings.warn(
                f"route() received unknown keyword arguments "
                f"{sorted(unknown)}; they will be ignored.",
                DeprecationWarning, stacklevel=2,
            )

    enhancements_requested = bool(use_enhancements)
    enhancements_active = (
        enhancements_requested and ENHANCEMENTS_AVAILABLE
    )
    if enhancements_requested and not ENHANCEMENTS_AVAILABLE:
        if strict:
            raise TraversalError(
                "use_enhancements=True requires the enhancements "
                "package."
            )
        logger.warning(
            "use_enhancements=True but the enhancements package is "
            "unavailable; falling back to threshold routing."
        )

    start = time.perf_counter()
    n = _compute_graph_size(graph_obj, method, legacy_kwargs)

    # ---- Decision ---------------------------------------------- #
    decision: Optional[_Decision] = None
    strategy_requested = (
        STRATEGY_ASYNC if n > ASYNC_THRESHOLD else STRATEGY_SYNC
    )
    decision_source = DECISION_SOURCE_THRESHOLD
    effective_optimizer = optimizer

    if enhancements_active:
        if effective_optimizer is None:
            effective_optimizer = TraversalDistillationOptimizer(
                dict(optimizer_config) if optimizer_config else {},
            )
        state = TraversalDecisionState.from_raw(
            graph_size=n,
            carbon_intensity=carbon_intensity,
            human_feedback=human_feedback_score,
            graph_centrality=gm.get("centrality", 0.5),
            graph_connectivity=gm.get("connectivity", 0.5),
        )
        action, x, t_out, t_comb = (
            effective_optimizer.select_action(state)
        )
        strategy_requested = (
            STRATEGY_ASYNC if action == 1 else STRATEGY_SYNC
        )
        decision_source = DECISION_SOURCE_DISTILLATION
        decision = _Decision(
            state_vec=x,
            action_idx=action,
            teacher_outputs=t_out,
            teacher_combined=t_comb,
            strategy_requested=strategy_requested,
        )

    # ---- Execution --------------------------------------------- #
    success = False
    result: Any = None
    try:
        if method == "trace_root_causes":
            anomaly_variable = legacy_kwargs.get("anomaly_variable")
            if not isinstance(anomaly_variable, str) or not anomaly_variable:
                raise TraversalError(
                    "trace_root_causes requires 'anomaly_variable'."
                )
            max_depth = int(legacy_kwargs.get("max_depth", 5))
            min_weight = float(legacy_kwargs.get("min_weight", 0.2))
            if strategy_requested == STRATEGY_ASYNC:
                result = await AsyncBFS(graph_obj).trace(
                    anomaly_variable, max_depth, min_weight,
                )
            else:
                result = graph_obj.trace_root_causes(
                    anomaly_variable, max_depth, min_weight,
                )
        elif method == "dp_align":
            seq_a = legacy_kwargs.get("seq_a")
            seq_b = legacy_kwargs.get("seq_b")
            if seq_a is None or seq_b is None:
                raise TraversalError(
                    "dp_align requires both 'seq_a' and 'seq_b'."
                )
            edit_costs = dict(
                legacy_kwargs.get("edit_costs") or DEFAULT_EDIT_COSTS,
            )
            if strategy_requested == STRATEGY_ASYNC:
                result = await AsyncDPAlign.align(
                    seq_a, seq_b, edit_costs,
                )
            else:
                result = _dp_align_sync(seq_a, seq_b, edit_costs)
        else:
            raise TraversalError(
                f"Unknown traversal method: {method!r}."
            )
        success = True
    except TraversalError:
        raise
    except Exception:
        logger.exception("traversal failed for method %r", method)
        if strict:
            raise

    elapsed_ms = (time.perf_counter() - start) * 1000.0

    # ---- Reward + optimizer update ----------------------------- #
    modp_reward: Optional[float] = None
    distillation_stats: Optional[Dict[str, Any]] = None
    if (
        decision is not None
        and effective_optimizer is not None
        and enhancements_active
    ):
        cfg = optimizer_config or {}
        try:
            modp_reward = _compute_reward(
                elapsed_ms=elapsed_ms,
                success=success,
                carbon_intensity=carbon_intensity,
                weights=cfg.get("modp_weights"),
                failure_multiplier=float(
                    cfg.get(
                        "failure_reward_multiplier",
                        FAILURE_REWARD_MULTIPLIER,
                    ),
                ),
            )
            effective_optimizer.update(
                state_vec=decision.state_vec,
                action=decision.action_idx,
                reward=modp_reward,
                teacher_outputs=decision.teacher_outputs,
                teacher_combined=decision.teacher_combined,
            )
            distillation_stats = effective_optimizer.statistics()
        except TraversalError:
            raise
        except Exception:
            logger.exception(
                "optimizer.update failed for method %r", method,
            )
            if strict:
                raise
            modp_reward = None
            distillation_stats = None

    # ``strategy_used`` is what actually ran. With this file's
    # implementations, sync and async produce the same numerical
    # result via different code paths; the runtime envelope reflects
    # which path was taken.
    strategy_used = strategy_requested

    return TraversalResult(
        method=method,
        graph_size=n,
        used_async=(strategy_used == STRATEGY_ASYNC),
        elapsed_ms=round(elapsed_ms, 3),
        result=result,
        modp_reward=modp_reward,
        decision_source=decision_source,
        distillation_stats=distillation_stats,
        strategy_requested=strategy_requested,
        strategy_used=strategy_used,
    )


# --------------------------------------------------------------------------- #
# Sync wrapper
# --------------------------------------------------------------------------- #
def _route_sync_impl(
    graph_obj: Any,
    method: str,
    **kwargs: Any,
) -> TraversalResult:
    """
    Small-graph fast path: run the traversal without entering
    ``asyncio``. The reward and result envelope are identical to
    ``route()``'s.
    """
    n = _compute_graph_size(graph_obj, method, kwargs)
    strategy = (
        STRATEGY_ASYNC if n > ASYNC_THRESHOLD else STRATEGY_SYNC
    )
    start = time.perf_counter()
    success = False
    result: Any = None
    try:
        if method == "trace_root_causes":
            anomaly_variable = kwargs.get("anomaly_variable")
            if not isinstance(anomaly_variable, str) or not anomaly_variable:
                raise TraversalError(
                    "trace_root_causes requires 'anomaly_variable'."
                )
            max_depth = int(kwargs.get("max_depth", 5))
            min_weight = float(kwargs.get("min_weight", 0.2))
            # Small-graph fast path is sync only; the sync threshold
            # guarantees this branch when the caller did not request
            # enhancements.
            result = graph_obj.trace_root_causes(
                anomaly_variable, max_depth, min_weight,
            )
        elif method == "dp_align":
            seq_a = kwargs.get("seq_a")
            seq_b = kwargs.get("seq_b")
            if seq_a is None or seq_b is None:
                raise TraversalError(
                    "dp_align requires both 'seq_a' and 'seq_b'."
                )
            edit_costs = dict(
                kwargs.get("edit_costs") or DEFAULT_EDIT_COSTS,
            )
            result = _dp_align_sync(seq_a, seq_b, edit_costs)
        else:
            raise TraversalError(
                f"Unknown traversal method: {method!r}."
            )
        success = True
    except TraversalError:
        raise
    except Exception:
        logger.exception("traversal failed for method %r", method)
        if kwargs.get("strict"):
            raise
    elapsed_ms = (time.perf_counter() - start) * 1000.0

    return TraversalResult(
        method=method,
        graph_size=n,
        used_async=False,
        elapsed_ms=round(elapsed_ms, 3),
        result=result,
        modp_reward=None,
        decision_source=DECISION_SOURCE_THRESHOLD,
        distillation_stats=None,
        strategy_requested=strategy,
        strategy_used=STRATEGY_SYNC,
    )


def run_sync(
    graph_obj: Any, method: str, **kwargs: Any,
) -> TraversalResult:
    """
    Synchronous wrapper around :func:`route`.

    For small graphs without enhancements, this short-circuits to a
    pure-sync path (no ``asyncio`` overhead). For larger graphs, or when
    enhancements are enabled, it runs ``route()`` via ``asyncio.run``.

    Raises
    ------
    TraversalError
        If called from inside a running event loop; use ``await
        route(...)`` there instead.
    """
    use_enh = bool(kwargs.get("use_enhancements", False))
    strict = bool(kwargs.get("strict", False))

    # Fast path: small graph, no enhancements → no asyncio involved.
    if not use_enh:
        try:
            n = _compute_graph_size(graph_obj, method, kwargs)
        except TraversalError:
            if strict:
                raise
            n = 0
        if n <= ASYNC_THRESHOLD:
            return _route_sync_impl(graph_obj, method, **kwargs)

    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(route(graph_obj, method, **kwargs))
    raise TraversalError(
        "run_sync() cannot be called from a running event loop; "
        "await route() directly instead."
    )


# --------------------------------------------------------------------------- #
# Public API
# --------------------------------------------------------------------------- #
__all__: List[str] = [
    "__version__",
    "ASYNC_THRESHOLD",
    "AsyncBFS",
    "AsyncDPAlign",
    "AsyncDFS",
    "DECISION_SOURCE_DISTILLATION",
    "DECISION_SOURCE_THRESHOLD",
    "DEFAULT_EDIT_COSTS",
    "DEFAULT_MODP_WEIGHTS",
    "ENHANCEMENTS_AVAILABLE",
    "FAILURE_REWARD_MULTIPLIER",
    "MAX_CONCURRENCY",
    "MAX_CARBON_KG",
    "MAX_ENERGY_KWH",
    "MAX_LATENCY_MS",
    "N_ACTIONS",
    "STRATEGY_ASYNC",
    "STRATEGY_SYNC",
    "TraversalDecisionState",
    "TraversalDistillationOptimizer",
    "TraversalError",
    "TraversalResult",
    "route",
    "run_sync",
]


def __dir__() -> List[str]:
    return sorted(__all__)


# --------------------------------------------------------------------------- #
# Smoke test: python -m core.async_traversal
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

    # ---- 1. DP align correctness ------------------------------- #
    print("_dp_align_sync:")
    _check(
        "identical sequences → 0",
        _dp_align_sync(["a", "b", "c"], ["a", "b", "c"],
                       DEFAULT_EDIT_COSTS) == 0.0,
    )
    _check(
        "empty → empty → 0",
        _dp_align_sync([], [], DEFAULT_EDIT_COSTS) == 0.0,
    )
    _check(
        "insert cost == one insert",
        abs(_dp_align_sync([], ["a"], DEFAULT_EDIT_COSTS) - 1.0) < 1e-9,
    )
    _check(
        "delete cost == one delete",
        abs(_dp_align_sync(["a"], [], DEFAULT_EDIT_COSTS) - 1.0) < 1e-9,
    )
    _check(
        "relabel cost for substitution",
        abs(_dp_align_sync(["a"], ["b"], DEFAULT_EDIT_COSTS) - 0.7) < 1e-9,
    )

    # ---- 2. DP align async wrapper ----------------------------- #
    print("AsyncDPAlign.align:")
    align_result = asyncio.run(
        AsyncDPAlign.align(["a", "b"], ["a", "b"], DEFAULT_EDIT_COSTS)
    )
    _check("async align matches sync", align_result == 0.0)

    # ---- 3. TraversalDecisionState ----------------------------- #
    print("TraversalDecisionState:")
    st = TraversalDecisionState.from_raw(
        graph_size=300,
        carbon_intensity=350.0,
        human_feedback=0.6,
        graph_centrality=0.7,
        graph_connectivity=0.5,
    )
    _check(
        "feature vector length == FEATURE_DIM",
        st.to_vector().shape[0] == FEATURE_DIM,
    )
    _check(
        "feature vector in [0,1]",
        bool(np.all(st.to_vector() >= 0)
             and np.all(st.to_vector() <= 1)),
    )
    _check(
        "rejects out-of-range human",
        _raises(TraversalError, lambda: TraversalDecisionState.from_raw(
            graph_size=10, human_feedback=2.0,
        )),
    )
    _check(
        "frozen — mutation rejected",
        _raises(
            (AttributeError, Exception),
            lambda: setattr(st, "human", 0.1),
        ),
    )

    # ---- 4. Optimizer ------------------------------------------ #
    print("optimizer:")
    opt = TraversalDistillationOptimizer({"seed": 7})
    action, x, t_out, t_comb = opt.select_action(st)
    _check("action in 0..1", 0 <= action < N_ACTIONS)
    _check("feature_dim matches vector", x.shape[0] == opt.feature_dim)
    _check(
        "teacher_outputs shape",
        t_out.shape == (len(opt.teachers), N_ACTIONS),
    )
    _check("teacher_combined shape", t_comb.shape == (N_ACTIONS,))

    # Train for a few cycles; the shape-mismatch crash that broke the
    # previous copy would fire here on the first batch.
    for _ in range(opt.train_every * opt.batch_size):
        a, xv, to, tc = opt.select_action(st)
        opt.update(xv, a, 0.7, to, tc)
    _check(
        "student weights finite after training",
        bool(np.all(np.isfinite(opt.student_weights))),
    )
    _check(
        "gate weights finite after training",
        bool(np.all(np.isfinite(opt.gate_weights))),
    )
    _check(
        "rejects NaN reward",
        _raises(TraversalError, lambda:
                opt.update(x, 0, float("nan"), t_out, t_comb)),
    )
    _check(
        "rejects bad epsilon",
        _raises(TraversalError, lambda:
                TraversalDistillationOptimizer({"epsilon": "high"})),
    )

    # ---- 5. Reward --------------------------------------------- #
    print("reward:")
    r_ok = _compute_reward(
        elapsed_ms=100.0, success=True,
        carbon_intensity=400.0,
    )
    r_fail = _compute_reward(
        elapsed_ms=100.0, success=False,
        carbon_intensity=400.0,
    )
    _check("success → reward > 0", r_ok > 0.0)
    _check("failure → reward == 0", r_fail == 0.0)
    _check(
        "reward in [0,1]",
        0.0 <= r_ok <= 1.0,
    )
    _check(
        "rejects NaN elapsed_ms",
        _raises(TraversalError, lambda: _compute_reward(
            elapsed_ms=float("nan"), success=True,
            carbon_intensity=400.0,
        )),
    )
    _check(
        "rejects bad weights",
        _raises(TraversalError, lambda: _compute_reward(
            elapsed_ms=10.0, success=True,
            carbon_intensity=400.0, weights=[0.0, 0.0, 0.0],
        )),
    )

    # ---- 6. TraversalResult validation ------------------------- #
    print("TraversalResult:")
    _check(
        "constructs",
        isinstance(
            TraversalResult(
                method="trace_root_causes",
                graph_size=10,
                used_async=False,
                elapsed_ms=10.0,
                result=None,
            ),
            TraversalResult,
        ),
    )
    _check(
        "rejects negative graph_size",
        _raises(TraversalError, lambda: TraversalResult(
            method="dp_align", graph_size=-1, used_async=False,
            elapsed_ms=0.0, result=None,
        )),
    )
    _check(
        "used_async must match strategy_used",
        _raises(TraversalError, lambda: TraversalResult(
            method="dp_align", graph_size=10, used_async=True,
            elapsed_ms=0.0, result=None,
            strategy_used="sync",
        )),
    )
    _check(
        "unknown strategy rejected",
        _raises(TraversalError, lambda: TraversalResult(
            method="dp_align", graph_size=10, used_async=False,
            elapsed_ms=0.0, result=None,
            strategy_requested="other",
        )),
    )

    # ---- 7. dp_align via route (sync path) -------------------- #
    print("route dp_align (sync):")
    res = asyncio.run(route(
        None, "dp_align",
        seq_a=["a", "b"], seq_b=["a", "b"],
    ))
    _check(
        "small graph uses sync",
        res.strategy_used == STRATEGY_SYNC,
    )
    _check(
        "decision_source is threshold",
        res.decision_source == DECISION_SOURCE_THRESHOLD,
    )
    _check(
        "graph_size matches len(seq_a) * len(seq_b)",
        res.graph_size == 4,
    )
    _check("result is 0.0", res.result == 0.0)

    # ---- 8. route with a persistent optimizer (learning) ------ #
    print("route with persistent optimizer:")
    opt = TraversalDistillationOptimizer({"seed": 11})
    counts = []
    for _ in range(20):
        res = asyncio.run(route(
            None, "dp_align",
            seq_a=["a", "b"], seq_b=["a", "c"],
            use_enhancements=True,
            optimizer=opt,
        ))
        counts.append(opt.counter)
    _check(
        "optimizer counter accumulates across calls",
        counts[-1] == 20,
    )
    _check(
        "decision_source is distillation",
        res.decision_source == DECISION_SOURCE_DISTILLATION,
    )
    _check(
        "modp_reward present",
        res.modp_reward is not None,
    )

    # ---- 9. route with one-shot optimizer (no learning) ------- #
    print("route with one-shot optimizer:")
    r1 = asyncio.run(route(
        None, "dp_align",
        seq_a=["a"], seq_b=["a"],
        use_enhancements=True,
    ))
    r2 = asyncio.run(route(
        None, "dp_align",
        seq_a=["a"], seq_b=["a"],
        use_enhancements=True,
    ))
    _check(
        "one-shot distillation_stats.counter == 1 each call",
        (r1.distillation_stats or {}).get("counter") == 1
        and (r2.distillation_stats or {}).get("counter") == 1,
    )

    # ---- 10. run_sync fast path ------------------------------ #
    print("run_sync fast path:")
    try:
        result = run_sync(
            None, "dp_align",
            seq_a=["a", "b"], seq_b=["a", "b"],
        )
        _check(
            "small-graph run_sync works",
            result.strategy_used == STRATEGY_SYNC,
        )
    except TraversalError as exc:  # pragma: no cover
        _check(f"run_sync raised: {exc}", False)

    # ---- 11. task_input validation --------------------------- #
    print("input validation:")
    _check(
        "empty method rejected",
        _raises(TraversalError, lambda:
                asyncio.run(route(None, ""))),
    )
    _check(
        "negative carbon_intensity rejected",
        _raises(TraversalError, lambda: asyncio.run(route(
            None, "dp_align",
            seq_a=["a"], seq_b=["a"],
            carbon_intensity=-1.0,
        ))),
    )
    _check(
        "out-of-range human_feedback rejected",
        _raises(TraversalError, lambda: asyncio.run(route(
            None, "dp_align",
            seq_a=["a"], seq_b=["a"],
            human_feedback_score=2.0,
        ))),
    )

    # ---- 12. Unknown method ---------------------------------- #
    print("unknown method:")
    _check(
        "unknown method raises TraversalError",
        _raises(TraversalError, lambda: asyncio.run(route(
            None, "bogus_method",
        ))),
    )

    # ---- 13. __all__ / __dir__ ------------------------------- #
    print("dir:")
    _check("__dir__ matches __all__", __dir__() == sorted(__all__))
    for _name in (
        "route", "run_sync", "TraversalError", "TraversalResult",
        "TraversalDistillationOptimizer", "TraversalDecisionState",
        "AsyncBFS", "AsyncDPAlign", "AsyncDFS",
        "ASYNC_THRESHOLD", "MAX_CONCURRENCY",
    ):
        _check(f"{_name} exported", _name in __all__)

    print()
    print(f"passed: {passed}  failed: {failed}")
    if failed:
        sys.exit(1)
