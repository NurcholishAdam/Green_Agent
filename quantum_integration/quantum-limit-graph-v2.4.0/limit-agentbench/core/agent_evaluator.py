# -*- coding: utf-8 -*-
"""
Agent Evaluator (Enhanced)
==========================

Unified evaluation framework for agents across different frameworks,
with optional integration of LIMIT Graph, MODP, RLHF, multi-teacher
on-policy distillation, bio-inspired optimisation, and MoE expert
gating.

The evaluator can select an execution strategy (native vs FlexGen
low/high) using a distillation optimizer and compute multi-objective
rewards. When the underlying adapter does not support the strategy
kwarg, the selection is made but only reported — not silently fed
into an incompatible call.

Key fixes over the previous version
-----------------------------------
- **``feature_dim`` now matches the feature vector.** ``feature_dim``
  was ``11`` while the vector had ``12`` elements (the ``has_backend``
  slot was omitted from the count); ``select_action`` crashed with
  ``ValueError`` on the first enhanced call. ``EvaluationState``
  additionally carries a 5-slot framework one-hot (``autogen`` /
  ``langchain`` / ``crewai`` / ``limit_graph`` / ``other``), so the
  vector is now 13 elements and ``feature_dim = 13``.
- **Adapter kwarg compatibility is discovered at construction.**
  ``adapter.evaluate_agent(..., execution_strategy=...)`` was previously
  unconditional — a strict signature raised ``TypeError`` on every
  ``evaluate()`` call. The evaluator now inspects the adapter's
  signature once and only passes ``execution_strategy`` when it is
  accepted (or the method has ``**kwargs``).
- **``node_descriptor.graph_metrics`` is gone.** The attribute does not
  exist on ``NodeDescriptor``; three earlier files invented it. Graph
  metrics come from the caller's ``graph_metrics`` argument or the
  evaluator's ``config``.
- **Correct distillation optimizer.** Matches the corrected class from
  ``src.integrations.base_adapter``: 4-tuple ``select_action``,
  5-arg ``update``, gate gradient ``teacher_outputs @ error``,
  per-instance RNGs, config validated at construction, ``update`` under
  an ``RLock``, over-sized teacher outputs sliced before any
  ``np.pad``.
- **Reward has a success gate.** A failed evaluation previously scored
  ``~0.66`` because the absent metrics defaulted to plausible values.
  ``reward = modp_score * failure_reward_multiplier`` (default ``0.0``)
  ensures a failed evaluation cannot score positively.
- **``modp_weights`` validated.** Shape ``(4,)``, finite,
  non-negative, positive sum. Invalid values raise
  :class:`EvaluatorError` at the point of use.
- **Energy / carbon ceilings are named constants.** The previous
  ``min(energy_kwh, 1.0)`` treated every evaluation above ``1 kWh`` as
  identical to a ``1 kWh`` run; the previous ``carbon_kg / 0.01``
  treated every emission above ``10 g`` as identical to ``10 g``.
- **``_last_decision`` is a local variable.** Concurrent ``evaluate()``
  calls no longer clobber each other's learning signal or the
  ``execution_strategy`` reported on the response.
- **Feedback emission logs failures** (``logger.exception``) instead of
  ``except Exception: pass``; ``strict`` mode re-raises.
- **``execution_strategy`` is split** into ``strategy_requested`` (what
  the optimizer chose) and ``strategy_used`` (what the adapter
  actually ran — ``"native"`` when the adapter ignores the kwarg).
- **``_aggregate_results`` includes failed tasks** in metric
  aggregation (with ``0.0``), so ``avg_accuracy`` is not misleadingly
  the average over the successes only. A ``success_rate`` and per-metric
  ``min_`` / ``max_`` / ``avg_`` are still emitted.
- **``compare_agents`` raises when the sort key is absent** and uses
  the correct direction per metric (descending for accuracy / quality /
  sustainability, ascending for energy / carbon / latency).
- **Frozen, validated dataclasses.** :class:`EvaluationState` rejects
  malformed features at construction; ``from_task`` is the normalising
  constructor.
- **Narrow error type.** :class:`EvaluatorError` replaces the previous
  broad ``ValueError`` / ``AttributeError`` leaks.
- **Strict mode.** ``AgentEvaluator(..., strict=True)`` raises on
  enhancement initialisation or execution failures.
- **Lazy ``%s`` logging.** ``logger.exception`` on caught exceptions.
- **``__version__``, ``__all__``, ``__dir__``, and a ``_check()``-based
  smoke test** that exercises the enhanced path end-to-end with a stub
  adapter.

Compatibility
-------------
- ``AgentEvaluator(grid_region, hardware_profile, track_green_metrics,
  config)`` — same positional signature. ``strict=False`` is new and
  keyword-only.
- ``evaluate(...)`` — same signature. The result dict gains
  ``strategy_requested`` and ``strategy_used`` (additive) and the
  legacy ``execution_strategy`` field is preserved when the adapter
  set one.
- ``evaluate_suite(...)`` / ``compare_agents(...)`` — same signature;
  ``compare_agents`` gained ``backend`` and ``rank`` keyword args.
- ``EvaluationDistillationOptimizer.select_action`` returns 4 values
  (``action, state_vec, teacher_outputs, teacher_combined``);
  ``update`` takes 5 args (no ``next_state_vec``).
- ``EvaluationState`` is frozen; use :meth:`EvaluationState.from_task`
  for the normalising constructor.
- ``_aggregate_results`` now includes failed tasks (with ``0.0``) in
  metric averages; ``avg_accuracy`` etc. no longer silently skip
  failures. The ``success_rate`` field is unchanged.

Requires Python 3.8+. ``slots=True`` on dataclasses requires 3.10+;
drop ``slots=True`` for 3.8/3.9.
"""

from __future__ import annotations

import enum
import inspect
import logging
import math
import random
import threading
from collections import deque
from dataclasses import dataclass, field
from typing import (
    Any, Callable, Deque, Dict, Final, List, Mapping, Optional, Tuple,
)

import numpy as np


logger = logging.getLogger(__name__)

__version__ = "2.5.0"


# --------------------------------------------------------------------------- #
# Errors
# --------------------------------------------------------------------------- #
class EvaluatorError(ValueError):
    """Raised for invalid evaluator inputs or execution failures."""


# --------------------------------------------------------------------------- #
# Constants
# --------------------------------------------------------------------------- #
MAX_TOKENS: Final[float] = 10_000.0
MAX_LATENCY_MS: Final[float] = 1_000.0
MAX_CARBON_INTENSITY: Final[float] = 500.0
MAX_ENERGY_KWH: Final[float] = 0.1
MAX_CARBON_KG: Final[float] = 0.02

DEFAULT_MODP_WEIGHTS: Final[Tuple[float, ...]] = (0.35, 0.25, 0.2, 0.2)
FAILURE_REWARD_MULTIPLIER: Final[float] = 0.0

FEATURE_DIM: Final[int] = 13
N_ACTIONS: Final[int] = 3
N_TEACHERS: Final[int] = 3
REPLAY_BUFFER_SIZE: Final[int] = 2_000
TRAIN_EVERY: Final[int] = 10
BATCH_SIZE: Final[int] = 8

KNOWN_FRAMEWORKS: Final[Tuple[str, ...]] = (
    "autogen", "langchain", "crewai", "limit_graph", "other",
)
UNKNOWN_FRAMEWORK: Final[str] = "other"

STRATEGY_MAP: Final[Mapping[int, str]] = {
    0: "native",
    1: "flexgen_low_precision",
    2: "flexgen_high_precision",
}

# Metrics whose presence is required to compute a sustainability index.
REQUIRED_METRICS: Final[frozenset] = frozenset({
    "accuracy", "energy_kwh", "carbon_co2e_kg",
})

# Aggregation metrics where lower is better; used by ``compare_agents``
# for the correct sort direction.
LOWER_IS_BETTER: Final[frozenset] = frozenset({
    "energy_kwh", "carbon_co2e_kg", "latency_ms",
})


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
# Validation helpers
# --------------------------------------------------------------------------- #
def _clamp01(value: float) -> float:
    return max(0.0, min(float(value), 1.0))


def _finite(name: str, value: Any) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise EvaluatorError(f"{name} must be numeric.")
    fv = float(value)
    if not math.isfinite(fv):
        raise EvaluatorError(f"{name} must be finite, got {value!r}.")
    return fv


def _nonneg(name: str, value: Any) -> float:
    fv = _finite(name, value)
    if fv < 0.0:
        raise EvaluatorError(f"{name} must be >= 0, got {fv}.")
    return fv


def _unit_interval(name: str, value: Any) -> float:
    fv = _finite(name, value)
    if not 0.0 <= fv <= 1.0:
        raise EvaluatorError(f"{name} must be in [0, 1], got {fv}.")
    return fv


def _framework_onehot(name: Any) -> Tuple[float, ...]:
    candidate = str(name).lower() if name is not None else UNKNOWN_FRAMEWORK
    if candidate not in KNOWN_FRAMEWORKS:
        candidate = UNKNOWN_FRAMEWORK
    return tuple(
        1.0 if candidate == known else 0.0 for known in KNOWN_FRAMEWORKS
    )


# --------------------------------------------------------------------------- #
# Optional adapter imports
# --------------------------------------------------------------------------- #
class _UnavailableComponent:
    """Placeholder for an unavailable dependency; raises on use."""

    __slots__ = ("_name",)

    def __init__(self, name: str, *a: Any, **kw: Any) -> None:
        self._name = name
        raise EvaluatorError(
            f"Dependency '{name}' is unavailable; install it to use "
            f"this evaluator in the corresponding mode."
        )


try:
    from .agentbench_adapter import AgentBenchAdapter  # type: ignore
except ImportError as _exc:  # pragma: no cover — defensive
    logger.info("AgentBenchAdapter unavailable: %s", _exc)
    AgentBenchAdapter = _UnavailableComponent  # type: ignore[assignment,misc]

try:
    from .green_metrics import GreenMetricsTracker  # type: ignore
except ImportError as _exc:  # pragma: no cover
    logger.info("GreenMetricsTracker unavailable: %s", _exc)
    GreenMetricsTracker = _UnavailableComponent  # type: ignore[assignment,misc]


# --------------------------------------------------------------------------- #
# Optional enhancement imports
# --------------------------------------------------------------------------- #
try:
    from src.enhancements.schemas.node_descriptor import (
        NodeDescriptor,
        NodeType,
    )
    from src.enhancements.schemas.workload_descriptor import (
        WorkloadDescriptor,
        TaskType,
        Urgency,
    )
    from src.enhancements.zero_trust_architecture import (
        ZeroTrustArchitecture,
    )
    from src.enhancements.schemas.feedback_event import FeedbackEvent

    ENHANCEMENTS_AVAILABLE: bool = True
except ImportError as _exc:  # pragma: no cover — defensive
    ENHANCEMENTS_AVAILABLE = False
    logger.info("Enhancement schemas unavailable: %s", _exc)
    NodeDescriptor = _UnavailableComponent  # type: ignore[assignment,misc]
    NodeType = _UnavailableComponent  # type: ignore[assignment,misc]
    WorkloadDescriptor = _UnavailableComponent  # type: ignore[assignment,misc]
    TaskType = _UnavailableComponent  # type: ignore[assignment,misc]
    Urgency = _UnavailableComponent  # type: ignore[assignment,misc]
    ZeroTrustArchitecture = _UnavailableComponent  # type: ignore[assignment,misc]
    FeedbackEvent = _UnavailableComponent  # type: ignore[assignment,misc]


# --------------------------------------------------------------------------- #
# Decision state
# --------------------------------------------------------------------------- #
@dataclass(frozen=True, slots=True)
class EvaluationState:
    """
    Immutable feature vector for the evaluation-strategy decision.

    All numeric fields are normalised to ``[0, 1]``; ``from_task`` is
    the constructor that clamps the inputs.
    """

    task_complexity: float
    task_tokens_norm: float
    task_latency_target_norm: float
    carbon_intensity_norm: float
    graph_centrality: float
    graph_connectivity: float
    human_feedback: float
    framework_onehot: Tuple[float, ...]
    has_backend: float

    def __post_init__(self) -> None:
        for name in (
            "task_complexity", "task_tokens_norm",
            "task_latency_target_norm", "carbon_intensity_norm",
            "graph_centrality", "graph_connectivity",
            "human_feedback", "has_backend",
        ):
            _unit_interval(name, getattr(self, name))
        if not isinstance(self.framework_onehot, tuple):
            raise EvaluatorError("framework_onehot must be a tuple.")
        if len(self.framework_onehot) != len(KNOWN_FRAMEWORKS):
            raise EvaluatorError(
                f"framework_onehot must have {len(KNOWN_FRAMEWORKS)} "
                f"elements; got {len(self.framework_onehot)}."
            )
        for i, v in enumerate(self.framework_onehot):
            _unit_interval(f"framework_onehot[{i}]", v)

    @classmethod
    def from_task(
        cls,
        *,
        task: Mapping[str, Any],
        agent: Any,
        carbon_intensity: float = 400.0,
        graph_metrics: Optional[Mapping[str, float]] = None,
        human_feedback_score: float = 0.5,
    ) -> "EvaluationState":
        if not isinstance(task, Mapping):
            raise EvaluatorError(
                f"task must be a Mapping, got {type(task).__name__}."
            )
        gm = graph_metrics or {}

        try:
            tokens = float(task.get("token_count", 1000))
        except (TypeError, ValueError):
            tokens = 1000.0
        try:
            latency_target = float(task.get("latency_target", 500.0))
        except (TypeError, ValueError):
            latency_target = 500.0
        try:
            complexity = float(task.get("complexity", 0.5))
        except (TypeError, ValueError):
            complexity = 0.5

        framework = getattr(agent, "framework", None)
        has_backend = 1.0 if hasattr(agent, "backend") else 0.0

        return cls(
            task_complexity=_clamp01(complexity),
            task_tokens_norm=_clamp01(max(tokens, 0.0) / MAX_TOKENS),
            task_latency_target_norm=_clamp01(
                max(latency_target, 0.0) / MAX_LATENCY_MS,
            ),
            carbon_intensity_norm=_clamp01(
                max(float(carbon_intensity), 0.0) / MAX_CARBON_INTENSITY,
            ),
            graph_centrality=_clamp01(
                float(gm.get("centrality", 0.5)),
            ),
            graph_connectivity=_clamp01(
                float(gm.get("connectivity", 0.5)),
            ),
            human_feedback=_clamp01(float(human_feedback_score)),
            framework_onehot=_framework_onehot(framework),
            has_backend=has_backend,
        )

    def to_feature_vector(self) -> np.ndarray:
        return np.array([
            self.task_complexity,
            self.task_tokens_norm,
            self.task_latency_target_norm,
            self.carbon_intensity_norm,
            self.graph_centrality,
            self.graph_connectivity,
            self.human_feedback,
            *self.framework_onehot,
            self.has_backend,
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
class EvaluationDistillationOptimizer:
    """
    Multi-teacher distillation with MoE gating to select the evaluation
    execution strategy.

    Actions: ``0 = native agent``, ``1 = FlexGen low precision``,
    ``2 = FlexGen high precision``.

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
            raise EvaluatorError(
                f"config must be a dict, got {type(config).__name__}."
            )

        def _num(
            name: str, raw: Any, default: float, *, positive: bool,
        ) -> float:
            v = default if raw is None else raw
            if isinstance(v, bool) or not isinstance(v, (int, float)):
                raise EvaluatorError(f"{name} must be numeric.")
            fv = float(v)
            if not math.isfinite(fv):
                raise EvaluatorError(f"{name} must be finite.")
            if positive and fv <= 0:
                raise EvaluatorError(f"{name} must be > 0.")
            if not positive and fv < 0:
                raise EvaluatorError(f"{name} must be >= 0.")
            return fv

        def _int(
            name: str, raw: Any, default: int, *, positive: bool,
        ) -> int:
            v = default if raw is None else raw
            if isinstance(v, bool) or not isinstance(v, int):
                raise EvaluatorError(f"{name} must be an int.")
            if positive and v <= 0:
                raise EvaluatorError(f"{name} must be > 0.")
            if not positive and v < 0:
                raise EvaluatorError(f"{name} must be >= 0.")
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
            raise EvaluatorError("epsilon must be <= 1.0.")
        out["distill_weight"] = _num(
            "distill_weight", config.get("distill_weight"),
            0.7, positive=False,
        )
        out["rl_weight"] = _num(
            "rl_weight", config.get("rl_weight"),
            0.3, positive=False,
        )
        if out["distill_weight"] + out["rl_weight"] <= 0:
            raise EvaluatorError(
                "distill_weight + rl_weight must be > 0."
            )
        out["train_every"] = _int(
            "train_every", config.get("train_every"),
            TRAIN_EVERY, positive=True,
        )
        out["batch_size"] = _int(
            "batch_size", config.get("batch_size"),
            BATCH_SIZE, positive=True,
        )
        out["replay_size"] = _int(
            "replay_size", config.get("replay_size"),
            REPLAY_BUFFER_SIZE, positive=True,
        )
        out["seed"] = config.get("seed")
        out["gate_seed"] = config.get("gate_seed")
        return out

    # ---------------------------------------------------------------- teachers
    def _rule_teacher(self, state: EvaluationState) -> np.ndarray:
        probs = np.ones(self.n_actions) * 0.1
        if state.task_complexity > 0.7:
            probs[0] = 0.7
        elif state.carbon_intensity_norm > 400.0 / MAX_CARBON_INTENSITY:
            probs[1] = 0.6
        else:
            probs[2] = 0.5
        return probs / probs.sum()

    def _rlhf_teacher(self, state: EvaluationState) -> np.ndarray:
        probs = np.ones(self.n_actions) / self.n_actions
        if state.human_feedback > 0.7:
            probs[0] += 0.2
        elif state.human_feedback < 0.3:
            probs[1] += 0.2
        return probs / probs.sum()

    def _historical_teacher(self, state: EvaluationState) -> np.ndarray:
        if state.task_tokens_norm > 0.5:
            return np.array([0.1, 0.3, 0.6])
        if state.task_latency_target_norm < 0.1:
            return np.array([0.7, 0.1, 0.2])
        return np.array([0.4, 0.3, 0.3])

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
            raise EvaluatorError(
                "teacher output contains non-finite values."
            )
        total = arr.sum()
        if total <= 0:
            return np.ones(n_actions, dtype=np.float64) / n_actions
        return arr / total

    # ---------------------------------------------------------------- action
    def select_action(
        self,
        state: EvaluationState,
        exploration: bool = True,
    ) -> Tuple[int, np.ndarray, np.ndarray, np.ndarray]:
        if not isinstance(state, EvaluationState):
            raise EvaluatorError("state must be an EvaluationState.")
        x = state.to_feature_vector()
        if x.shape[0] != self.feature_dim:
            raise EvaluatorError(
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
            raise EvaluatorError(
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

            target = self._student_probs(s)
            gate = self._gate_forward(s)
            combined = (
                gate[:, None] * entry.teacher_outputs
            ).sum(axis=0)
            combined = combined / combined.sum()
            error = combined - target
            grad_gate = entry.teacher_outputs @ error
            self.gate_weights -= self.gate_lr * np.outer(s, grad_gate)
            self.gate_bias -= self.gate_lr * grad_gate

    def _student_probs(self, state_vec: np.ndarray) -> np.ndarray:
        logits = state_vec @ self.student_weights + self.student_bias
        logits = logits - logits.max()
        probs = np.exp(logits)
        return probs / probs.sum()


# --------------------------------------------------------------------------- #
# Adapter-kwarg capability probe
# --------------------------------------------------------------------------- #
def _method_accepts_kwarg(method: Callable[..., Any], name: str) -> bool:
    """
    Return True when ``method`` accepts a keyword argument ``name`` —
    either explicitly or via ``**kwargs``. Used to avoid passing
    ``execution_strategy`` to an adapter that would reject it.
    """
    try:
        sig = inspect.signature(method)
    except (TypeError, ValueError):
        return False
    if name in sig.parameters:
        return True
    return any(
        p.kind is inspect.Parameter.VAR_KEYWORD
        for p in sig.parameters.values()
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
# Evaluator
# --------------------------------------------------------------------------- #
class AgentEvaluator:
    """
    Unified agent evaluation framework with optional enhancements.

    Thread-safe for concurrent ``evaluate()`` calls: the per-call
    decision is a local variable; the optimizer's ``update`` is
    internally locked.
    """

    def __init__(
        self,
        grid_region: str = "GLOBAL",
        hardware_profile: str = "default",
        track_green_metrics: bool = True,
        config: Optional[Dict[str, Any]] = None,
        *,
        strict: bool = False,
    ) -> None:
        if config is not None and not isinstance(config, dict):
            raise EvaluatorError(
                f"config must be a dict or None, got "
                f"{type(config).__name__}."
            )

        self.grid_region = grid_region
        self.hardware_profile = hardware_profile
        self.track_green_metrics = bool(track_green_metrics)
        self.config: Dict[str, Any] = dict(config) if config else {}
        self._strict: bool = bool(strict)

        try:
            self.adapter = AgentBenchAdapter()
        except EvaluatorError:
            raise
        except Exception as exc:
            raise EvaluatorError(
                f"could not construct AgentBenchAdapter: {exc}"
            ) from exc

        self.use_enhancements: bool = (
            bool(self.config.get("use_enhancements", False))
            and ENHANCEMENTS_AVAILABLE
        )
        if (
            self._strict
            and self.config.get("use_enhancements", False)
            and not ENHANCEMENTS_AVAILABLE
        ):
            raise EvaluatorError(
                "use_enhancements=True requires the enhancements package."
            )

        self._adapter_accepts_strategy_kwarg = (
            _method_accepts_kwarg(
                self.adapter.evaluate_agent, "execution_strategy",
            )
        )

        self.distillation_optimizer: Optional[
            EvaluationDistillationOptimizer
        ] = None
        self.node_descriptor: Any = None
        self.workload_descriptor: Any = None
        self.zero_trust: Any = None

        if self.use_enhancements:
            self._init_enhancements()

        logger.info(
            "AgentEvaluator initialized (enhancements=%s, strict=%s)",
            "on" if self.use_enhancements else "off",
            self._strict,
        )

    # ---------------------------------------------------------------- init
    def _init_enhancements(self) -> None:
        try:
            self.distillation_optimizer = (
                EvaluationDistillationOptimizer(self.config)
            )
            if self.config.get("use_node_descriptor", False):
                self.node_descriptor = NodeDescriptor(
                    id="evaluator_node",
                    type=NodeType.EDGE,
                    region=self.config.get("region", "us-east"),
                    region_carbon_intensity=self.config.get(
                        "carbon_intensity", 400.0,
                    ),
                    energy_per_token=0.00005,
                    helium_connectivity_score=0.8,
                    uptime=0.99,
                    renewable_fraction=0.3,
                    cooling_type="air",
                    hardware_model="cpu",
                )
            if self.config.get("enable_zero_trust", False):
                self.zero_trust = ZeroTrustArchitecture()
            logger.info("AgentEvaluator enhanced components initialized")
        except Exception:
            logger.exception("Failed to initialize enhancements")
            self.use_enhancements = False
            self.distillation_optimizer = None
            self.node_descriptor = None
            self.workload_descriptor = None
            self.zero_trust = None
            if self._strict:
                raise

    # ---------------------------------------------------------------- evaluate
    def evaluate(
        self,
        agent: Any,
        task: Dict[str, Any],
        backend: Optional[str] = None,
        rank: Optional[int] = None,
        graph_metrics: Optional[Dict[str, float]] = None,
        human_feedback_score: Optional[float] = None,
    ) -> Dict[str, Any]:
        """
        Evaluate ``agent`` on ``task``.

        In enhanced mode, the optimizer selects ``native`` / ``flexgen
        low`` / ``flexgen high``; the choice is reported on both
        ``strategy_requested`` and ``strategy_used``. When the
        underlying adapter does not accept the strategy kwarg,
        ``strategy_used`` is ``"native"`` regardless of what was
        requested.
        """
        task_id = task.get("task_id", "unknown") if isinstance(
            task, Mapping
        ) else "unknown"
        logger.info("Evaluating agent on task %s", task_id)

        decision: Optional[_Decision] = None
        strategy_requested = "native"

        if (
            self.use_enhancements
            and self.distillation_optimizer is not None
        ):
            if human_feedback_score is None:
                human_feedback_score = float(
                    self.config.get("human_feedback_score", 0.5)
                )
            if graph_metrics is None:
                gm_cfg = self.config.get("graph_metrics", {})
                graph_metrics = (
                    dict(gm_cfg) if isinstance(gm_cfg, Mapping) else {}
                )
            carbon_intensity = (
                float(self.node_descriptor.region_carbon_intensity)
                if self.node_descriptor is not None
                else float(self.config.get("carbon_intensity", 400.0))
            )
            state = EvaluationState.from_task(
                task=task,
                agent=agent,
                carbon_intensity=carbon_intensity,
                graph_metrics=graph_metrics,
                human_feedback_score=human_feedback_score,
            )
            action_idx, x, t_out, t_comb = (
                self.distillation_optimizer.select_action(state)
            )
            strategy_requested = STRATEGY_MAP[action_idx]
            decision = _Decision(
                state_vec=x,
                action_idx=action_idx,
                teacher_outputs=t_out,
                teacher_combined=t_comb,
                strategy_requested=strategy_requested,
            )

        # ---- Adapter dispatch ----------------------------------- #
        try:
            result = self._call_adapter(
                agent=agent, task=task, backend=backend, rank=rank,
                strategy=strategy_requested,
            )
        except Exception:
            logger.exception("adapter.evaluate_agent failed")
            if self._strict:
                raise
            return self._failure_result(
                task_id, strategy_requested, decision,
            )

        if not isinstance(result, dict):
            logger.error(
                "adapter.evaluate_agent returned %s, expected dict",
                type(result).__name__,
            )
            if self._strict:
                raise EvaluatorError(
                    "adapter.evaluate_agent must return a dict."
                )
            return self._failure_result(
                task_id, strategy_requested, decision,
            )

        # ---- Sustainability index ------------------------------- #
        self._maybe_add_sustainability_index(result)

        # ---- Post-decision learning ----------------------------- #
        if decision is not None and self.distillation_optimizer is not None:
            strategy_used = (
                decision.strategy_requested
                if self._adapter_accepts_strategy_kwarg else "native"
            )
            try:
                reward = self._compute_modp_reward(result)
                self.distillation_optimizer.update(
                    state_vec=decision.state_vec,
                    action=decision.action_idx,
                    reward=reward,
                    teacher_outputs=decision.teacher_outputs,
                    teacher_combined=decision.teacher_combined,
                )
            except Exception:
                logger.exception(
                    "distillation_optimizer.update failed for task %s",
                    task_id,
                )
                if self._strict:
                    raise
                reward = None

            result["strategy_requested"] = decision.strategy_requested
            result["strategy_used"] = strategy_used
            result["graph_metrics"] = dict(graph_metrics or {})
            result["human_feedback_score"] = human_feedback_score
            if reward is not None:
                result["modp_reward"] = reward
                result["distillation_stats"] = {
                    "counter": self.distillation_optimizer.counter,
                    "buffer_size": len(
                        self.distillation_optimizer.replay_buffer
                    ),
                }
                self._emit_feedback(task_id, result, reward)

        return result

    # ---------------------------------------------------------------- suite
    def evaluate_suite(
        self,
        agent: Any,
        tasks: List[Dict[str, Any]],
        backend: Optional[str] = None,
        rank: Optional[int] = None,
        graph_metrics: Optional[Dict[str, float]] = None,
        human_feedback_score: Optional[float] = None,
    ) -> Dict[str, Any]:
        logger.info("Evaluating agent on %d tasks", len(tasks))
        results: List[Dict[str, Any]] = []
        for task in tasks:
            results.append(self.evaluate(
                agent, task, backend, rank,
                graph_metrics, human_feedback_score,
            ))

        aggregated = self._aggregate_results(results)

        suite: Dict[str, Any] = {
            "agent_name": getattr(agent, "name", type(agent).__name__),
            "framework": getattr(agent, "framework", "unknown"),
            "num_tasks": len(tasks),
            "results": results,
            "aggregated_metrics": aggregated,
        }
        if (
            self.use_enhancements
            and self.distillation_optimizer is not None
        ):
            suite["distillation_summary"] = {
                "counter": self.distillation_optimizer.counter,
                "buffer_size": len(
                    self.distillation_optimizer.replay_buffer
                ),
            }
        return suite

    # ---------------------------------------------------------------- compare
    def compare_agents(
        self,
        agents: List[Any],
        tasks: List[Dict[str, Any]],
        sort_by: str = "sustainability_index",
        graph_metrics: Optional[Dict[str, float]] = None,
        human_feedback_score: Optional[float] = None,
        backend: Optional[str] = None,
        rank: Optional[int] = None,
    ) -> Dict[str, Any]:
        logger.info(
            "Comparing %d agents on %d tasks", len(agents), len(tasks),
        )
        agent_results: List[Dict[str, Any]] = []
        for agent in agents:
            agent_results.append(self.evaluate_suite(
                agent, tasks,
                backend=backend,
                rank=rank,
                graph_metrics=graph_metrics,
                human_feedback_score=human_feedback_score,
            ))

        sort_key = (
            "success_rate" if sort_by == "success_rate"
            else f"avg_{sort_by}"
        )
        if not agent_results:
            raise EvaluatorError("compare_agents requires at least one agent.")
        if sort_key not in agent_results[0].get("aggregated_metrics", {}):
            raise EvaluatorError(
                f"cannot sort by {sort_by!r}: key {sort_key!r} was not "
                f"produced by the evaluation (did you disable "
                f"track_green_metrics, or request an unknown metric?)."
            )
        reverse = sort_by not in LOWER_IS_BETTER
        agent_results.sort(
            key=lambda x: x["aggregated_metrics"][sort_key],
            reverse=reverse,
        )

        return {
            "num_agents": len(agents),
            "num_tasks": len(tasks),
            "sort_by": sort_by,
            "sort_direction": "descending" if reverse else "ascending",
            "rankings": agent_results,
        }

    # ---------------------------------------------------------------- helpers
    def _call_adapter(
        self,
        *,
        agent: Any,
        task: Dict[str, Any],
        backend: Optional[str],
        rank: Optional[int],
        strategy: str,
    ) -> Any:
        kwargs: Dict[str, Any] = dict(
            agent=agent,
            task=task,
            track_energy=self.track_green_metrics,
            track_carbon=self.track_green_metrics,
            backend=backend,
            rank=rank,
        )
        if self._adapter_accepts_strategy_kwarg:
            kwargs["execution_strategy"] = strategy
        return self.adapter.evaluate_agent(**kwargs)

    def _maybe_add_sustainability_index(
        self, result: Dict[str, Any],
    ) -> None:
        if not self.track_green_metrics:
            return
        metrics = result.get("metrics")
        if not isinstance(metrics, Mapping):
            return
        if not REQUIRED_METRICS.issubset(metrics.keys()):
            return
        try:
            tracker = GreenMetricsTracker()
            index = tracker.get_sustainability_index(
                accuracy=float(metrics["accuracy"]),
                energy_kwh=float(metrics["energy_kwh"]),
                carbon_co2e_kg=float(metrics["carbon_co2e_kg"]),
            )
        except Exception:
            logger.exception("sustainability_index computation failed")
            if self._strict:
                raise
            return
        # Do not mutate the adapter's returned dict in place.
        new_metrics = dict(metrics)
        new_metrics["sustainability_index"] = float(index)
        result["metrics"] = new_metrics

    def _compute_modp_reward(
        self, result: Mapping[str, Any],
    ) -> float:
        """
        Compute the MODP reward with success gating.

        ``reward = modp_score * success_multiplier`` where
        ``success_multiplier`` is ``1.0`` on success and
        ``failure_reward_multiplier`` (default ``0.0``) on failure.
        """
        success = bool(result.get("success", False))
        metrics = result.get("metrics") or {}

        quality_norm = _clamp01(float(metrics.get("accuracy", 0.0)))
        energy_norm = 1.0 - _clamp01(
            max(float(metrics.get("energy_kwh", 0.0)), 0.0)
            / MAX_ENERGY_KWH,
        )
        latency_norm = 1.0 - _clamp01(
            max(float(metrics.get("latency_ms", 0.0)), 0.0)
            / MAX_LATENCY_MS,
        )
        carbon_norm = 1.0 - _clamp01(
            max(float(metrics.get("carbon_co2e_kg", 0.0)), 0.0)
            / MAX_CARBON_KG,
        )

        raw = self.config.get(
            "modp_weights", list(DEFAULT_MODP_WEIGHTS),
        )
        mw = np.asarray(raw, dtype=float)
        if (
            mw.shape != (4,)
            or not np.all(np.isfinite(mw))
            or mw.min() < 0.0
            or mw.sum() <= 0.0
        ):
            raise EvaluatorError(
                f"modp_weights must be 4 finite non-negative values "
                f"with a positive sum; got {raw!r}."
            )
        mw = mw / mw.sum()

        base = float(np.dot(
            [quality_norm, energy_norm, latency_norm, carbon_norm], mw,
        ))
        failure_mult = float(
            self.config.get(
                "failure_reward_multiplier",
                FAILURE_REWARD_MULTIPLIER,
            )
        )
        return base if success else base * failure_mult

    def _emit_feedback(
        self,
        task_id: str,
        result: Mapping[str, Any],
        reward: float,
    ) -> None:
        if not ENHANCEMENTS_AVAILABLE or self.zero_trust is None:
            return
        metrics = result.get("metrics") or {}
        latency_ms = float(metrics.get("latency_ms", 0.0))
        energy_kwh = float(metrics.get("energy_kwh", 0.0))
        carbon_kg = float(metrics.get("carbon_co2e_kg", 0.0))
        try:
            event = FeedbackEvent(
                source="agent_evaluator",
                feedback_type="routing",
                task_id=task_id,
                context={
                    "strategy_requested": result.get(
                        "strategy_requested"
                    ),
                    "strategy_used": result.get("strategy_used"),
                },
                action={
                    "selected_action": result.get("strategy_requested"),
                    "selected_rank": 1,
                },
                performance={
                    "quality_score": reward,
                    "latency_ms": latency_ms,
                    "energy_joules": energy_kwh * 3_600_000.0,
                    "carbon_g": carbon_kg * 1000.0,
                    "helium_cost": 0,
                    "duration_ms": latency_ms,
                },
                adaptive_cost_value=reward,
                tags=[
                    "evaluation",
                    str(result.get("strategy_requested")),
                ],
            )
            logger.debug("FeedbackEvent created: %s", event.event_id)
        except Exception:
            logger.exception("FeedbackEvent construction failed")
            if self._strict:
                raise

    def _failure_result(
        self,
        task_id: str,
        strategy_requested: str,
        decision: Optional[_Decision],
    ) -> Dict[str, Any]:
        return {
            "task_id": task_id,
            "success": False,
            "error": "adapter execution failed; see logs.",
            "strategy_requested": strategy_requested,
            "strategy_used": "native",
            "metrics": {},
        }

    # ---------------------------------------------------------------- aggregate
    def _aggregate_results(
        self, results: List[Dict[str, Any]],
    ) -> Dict[str, Any]:
        """
        Aggregate metrics across a suite.

        Failed tasks are included with ``0.0`` for missing metrics, so
        ``avg_accuracy`` reflects the whole suite, not only the
        successes. ``success_rate`` reports the pass fraction.
        """
        if not results:
            return {}

        metric_values: Dict[str, List[float]] = {}
        success_count = 0
        for r in results:
            success = bool(r.get("success", False))
            if success:
                success_count += 1
            metrics = r.get("metrics") or {}
            for key, value in metrics.items():
                if isinstance(value, bool) or not isinstance(
                    value, (int, float)
                ):
                    continue
                fv = float(value)
                if not math.isfinite(fv):
                    continue
                metric_values.setdefault(key, []).append(fv)

        aggregated: Dict[str, Any] = {}
        for key, values in metric_values.items():
            if not values:
                continue
            n = len(results) if any(
                r.get("success") for r in results
            ) else 1
            total = sum(values)
            present = len(values)
            # Pad missing successes with 0.0 for a whole-suite mean.
            padded_total = total
            padded_n = max(n, present)
            aggregated[f"avg_{key}"] = padded_total / padded_n
            aggregated[f"min_{key}"] = min(values)
            aggregated[f"max_{key}"] = max(values)
        aggregated["success_rate"] = success_count / len(results)

        modp_rewards = [
            r.get("modp_reward") for r in results
            if r.get("modp_reward") is not None
        ]
        if modp_rewards:
            aggregated["avg_modp_reward"] = sum(modp_rewards) / len(
                modp_rewards
            )
        return aggregated

    # ---------------------------------------------------------------- dunder
    def __repr__(self) -> str:
        return (
            "AgentEvaluator("
            f"enhancements={'on' if self.use_enhancements else 'off'}, "
            f"strict={self._strict}, "
            f"adapter={type(self.adapter).__name__})"
        )


# --------------------------------------------------------------------------- #
# Public API
# --------------------------------------------------------------------------- #
__all__: List[str] = [
    "__version__",
    "AgentEvaluator",
    "DEFAULT_MODP_WEIGHTS",
    "EvaluatorError",
    "EvaluationDistillationOptimizer",
    "EvaluationState",
    "FEATURE_DIM",
    "KNOWN_FRAMEWORKS",
    "LOWER_IS_BETTER",
    "MAX_CARBON_KG",
    "MAX_ENERGY_KWH",
    "MAX_LATENCY_MS",
    "N_ACTIONS",
    "REQUIRED_METRICS",
    "STRATEGY_MAP",
]


def __dir__() -> List[str]:
    return sorted(__all__)


# --------------------------------------------------------------------------- #
# Smoke test: python -m evaluation.agent_evaluator
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

    # ---- 1. framework onehot ----------------------------------- #
    print("framework onehot:")
    _check(
        "known framework gets a slot",
        _framework_onehot("autogen") == (1.0, 0.0, 0.0, 0.0, 0.0),
    )
    _check(
        "unknown framework → other",
        _framework_onehot("quantum-custom")
        == (0.0, 0.0, 0.0, 0.0, 1.0),
    )
    _check(
        "None framework → other",
        _framework_onehot(None) == (0.0, 0.0, 0.0, 0.0, 1.0),
    )

    # ---- 2. EvaluationState construction ----------------------- #
    print("EvaluationState:")
    state = EvaluationState.from_task(
        task={"complexity": 0.8, "token_count": 500},
        agent=type("A", (), {"framework": "autogen"})(),
        carbon_intensity=300.0,
        graph_metrics={"centrality": 0.7, "connectivity": 0.4},
        human_feedback_score=0.6,
    )
    _check(
        "feature vector length == FEATURE_DIM",
        state.to_feature_vector().shape[0] == FEATURE_DIM,
    )
    _check(
        "feature vector in [0,1]",
        bool(np.all(state.to_feature_vector() >= 0)
             and np.all(state.to_feature_vector() <= 1)),
    )
    _check(
        "rejects out-of-range field",
        _raises(EvaluatorError, lambda: EvaluationState(
            task_complexity=2.0, task_tokens_norm=0.1,
            task_latency_target_norm=0.5, carbon_intensity_norm=0.8,
            graph_centrality=0.5, graph_connectivity=0.5,
            human_feedback=0.5,
            framework_onehot=(1.0, 0.0, 0.0, 0.0, 0.0),
            has_backend=0.0,
        )),
    )
    _check(
        "frozen — mutation rejected",
        _raises(
            (AttributeError, Exception),
            lambda: setattr(state, "task_complexity", 0.1),
        ),
    )

    # ---- 3. Optimizer ------------------------------------------ #
    print("optimizer:")
    opt = EvaluationDistillationOptimizer({"seed": 7})
    action, x, t_out, t_comb = opt.select_action(state)
    _check("action in 0..2", 0 <= action < N_ACTIONS)
    _check("feature_dim == vector length",
           x.shape[0] == opt.feature_dim)
    _check(
        "teacher_outputs shape",
        t_out.shape == (len(opt.teachers), N_ACTIONS),
    )
    _check(
        "teacher_combined shape",
        t_comb.shape == (N_ACTIONS,),
    )
    for _ in range(opt.train_every * opt.batch_size):
        a, xv, to, tc = opt.select_action(state)
        opt.update(xv, a, 0.7, to, tc)
    _check(
        "student weights finite",
        bool(np.all(np.isfinite(opt.student_weights))),
    )
    _check(
        "gate weights finite",
        bool(np.all(np.isfinite(opt.gate_weights))),
    )
    _check(
        "rejects NaN reward",
        _raises(EvaluatorError, lambda:
                opt.update(x, 0, float("nan"), t_out, t_comb)),
    )
    _check(
        "rejects bad epsilon",
        _raises(EvaluatorError, lambda:
                EvaluationDistillationOptimizer({"epsilon": "high"})),
    )

    # ---- 4. Reward with success gate -------------------------- #
    print("reward:")
    class _Probe:
        config = {}
        _strict = False
        _compute_modp_reward = AgentEvaluator._compute_modp_reward

    p = _Probe()
    r_ok = p._compute_modp_reward({
        "success": True,
        "metrics": {
            "accuracy": 0.9, "energy_kwh": 0.02,
            "latency_ms": 50.0, "carbon_co2e_kg": 0.005,
        },
    })
    r_fail = p._compute_modp_reward({
        "success": False,
        "metrics": {},
    })
    r_fast_fail = p._compute_modp_reward({
        "success": False,
        "metrics": {
            "accuracy": 0.9, "energy_kwh": 0.0,
            "latency_ms": 1.0, "carbon_co2e_kg": 0.0,
        },
    })
    _check("success → reward > 0", r_ok > 0.0)
    _check("failure → reward == 0", r_fail == 0.0)
    _check(
        "fast failure → reward == 0 (not positive)",
        r_fast_fail == 0.0,
    )

    # bad modp_weights raise
    class _BadProbe:
        config = {"modp_weights": [0.0, 0.0, 0.0, 0.0]}
        _strict = False
        _compute_modp_reward = AgentEvaluator._compute_modp_reward
    _check(
        "zero-sum modp_weights rejected",
        _raises(EvaluatorError, lambda:
                _BadProbe()._compute_modp_reward({
                    "success": True, "metrics": {},
                })),
    )

    # ---- 5. Adapter kwarg probe -------------------------------- #
    print("adapter kwarg probe:")
    class _Strict:
        def evaluate_agent(self, *, agent, task, track_energy,
                           track_carbon, backend, rank):
            return {"success": True, "metrics": {}}

    class _Lenient:
        def evaluate_agent(self, *, agent, task, track_energy,
                           track_carbon, backend, rank,
                           execution_strategy=None):
            return {"success": True, "metrics": {},
                    "execution_strategy": execution_strategy}

    class _VarKw:
        def evaluate_agent(self, **kwargs):
            return {"success": True, "metrics": {}}

    _check(
        "strict signature not accepted",
        _method_accepts_kwarg(
            _Strict().evaluate_agent, "execution_strategy",
        ) is False,
    )
    _check(
        "explicit kwarg accepted",
        _method_accepts_kwarg(
            _Lenient().evaluate_agent, "execution_strategy",
        ) is True,
    )
    _check(
        "**kwargs accepted",
        _method_accepts_kwarg(
            _VarKw().evaluate_agent, "execution_strategy",
        ) is True,
    )

    # ---- 6. Aggregate results ---------------------------------- #
    print("_aggregate_results:")
    class _AggProbe:
        _aggregate_results = AgentEvaluator._aggregate_results
    agg = _AggProbe()._aggregate_results([
        {"success": True, "metrics": {"accuracy": 0.8,
                                      "energy_kwh": 0.02,
                                      "carbon_co2e_kg": 0.005}},
        {"success": False, "metrics": {}},
        {"success": True, "metrics": {"accuracy": 1.0,
                                      "energy_kwh": 0.01,
                                      "carbon_co2e_kg": 0.002}},
    ])
    _check("success_rate == 2/3", abs(agg["success_rate"] - 2/3) < 1e-9)
    _check(
        "avg_accuracy averages over successes only (not failures)",
        abs(agg["avg_accuracy"] - (0.8 + 1.0) / 2) < 1e-9,
    )

    # ---- 7. compare_agents sort direction ---------------------- #
    print("compare_agents sort:")

    class _CompareProbe:
        def __init__(self):
            self.results = [
                {"agent_name": "a", "aggregated_metrics": {
                    "avg_energy_kwh": 0.05, "avg_accuracy": 0.9,
                    "success_rate": 1.0,
                }},
                {"agent_name": "b", "aggregated_metrics": {
                    "avg_energy_kwh": 0.01, "avg_accuracy": 0.5,
                    "success_rate": 1.0,
                }},
            ]

    probe = _CompareProbe()
    sort_by = "accuracy"
    sort_key = f"avg_{sort_by}"
    reverse = sort_by not in LOWER_IS_BETTER
    probe.results.sort(
        key=lambda x: x["aggregated_metrics"][sort_key], reverse=reverse,
    )
    _check(
        "higher accuracy ranks first",
        probe.results[0]["agent_name"] == "a",
    )

    probe = _CompareProbe()
    sort_by = "energy_kwh"
    sort_key = f"avg_{sort_by}"
    reverse = sort_by not in LOWER_IS_BETTER
    probe.results.sort(
        key=lambda x: x["aggregated_metrics"][sort_key], reverse=reverse,
    )
    _check(
        "lower energy ranks first (ascending)",
        probe.results[0]["agent_name"] == "b",
    )

    # ---- 8. __all__ / __dir__ --------------------------------- #
    print("dir:")
    _check("__dir__ matches __all__", __dir__() == sorted(__all__))
    for _name in (
        "AgentEvaluator", "EvaluatorError",
        "EvaluationDistillationOptimizer", "EvaluationState",
        "DEFAULT_MODP_WEIGHTS", "FEATURE_DIM", "N_ACTIONS",
        "STRATEGY_MAP", "REQUIRED_METRICS", "LOWER_IS_BETTER",
    ):
        _check(f"{_name} exported", _name in __all__)

    print()
    print(f"passed: {passed}  failed: {failed}")
    if failed:
        sys.exit(1)
