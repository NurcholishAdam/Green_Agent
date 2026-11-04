# -*- coding: utf-8 -*-
"""
AgentBench Protocol Adapter (Enhanced)
======================================

Standardised interface compatible with the AgentBench protocol, with
optional integration of LIMIT Graph, MODP, RLHF, multi-teacher
on-policy distillation, bio-inspired optimisation, and MoE expert
gating.

**Single-optimizer contract**
----------------------------
This adapter accepts an optional ``execution_strategy`` kwarg. When it
is provided (typically by :class:`~evaluation.agent_evaluator.AgentEvaluator`),
the adapter **honours it** and does **not** run its own distillation
optimizer for that call. The adapter's optimizer is only used when no
strategy is provided (i.e., when the adapter is used standalone).

This eliminates the prior dual-optimizer confusion: two optimizers
(the evaluator's and the adapter's) trained on the same reward from
independent strategies, and neither strategy determined the reward.

Every successful ``evaluate_agent`` result carries
``strategy_requested`` (what the caller or adapter asked for) and
``strategy_used`` (what the adapter actually ran). When the adapter
has no FlexGen dispatch (this version does not), ``strategy_used`` is
``"native"`` regardless of what was requested — a caller is never
misled by a cosmetic strategy label.

Key fixes over the previous version
-----------------------------------
- **``feature_dim`` matches the feature vector.** ``feature_dim`` was
  ``11`` while the vector had 12 elements (``has_backend`` was omitted
  from the count); ``select_action`` crashed with ``ValueError`` on the
  first enhanced call. The framework one-hot now also carries an
  ``"other"`` slot (5 dimensions), so the vector is 13 elements and
  ``feature_dim = 13`` — matching the enhanced ``agent_evaluator.py``.
- **``node_descriptor.graph_metrics`` fallback removed.** The attribute
  does not exist on ``NodeDescriptor``. Graph metrics come from the
  caller's ``graph_metrics`` argument or ``config["graph_metrics"]``.
- **Reward has a success gate.** A failed evaluation previously scored
  ``~0.66`` because the absent metrics defaulted to plausible values.
  ``reward = modp_score * failure_reward_multiplier`` (default ``0.0``)
  ensures a failed evaluation cannot score positively.
- **``_compute_hash`` is deterministic.** The previous implementation
  included ``datetime.utcnow().isoformat()`` in the hash input, so two
  runs with identical ``(task, output)`` produced different hashes —
  defeating the purpose of a provenance fingerprint. The hash now
  covers only ``(task_id, output)``; the timestamp moved to a
  separate ``provenance["run_at"]`` field.
- **``execution_strategy`` kwarg accepted.** Resolves the dual-optimizer
  confusion with ``agent_evaluator.py``: the caller's strategy is
  honoured, and the adapter's local optimizer is only used when no
  strategy is passed.
- **Honest strategy reporting.** Every enhanced result carries
  ``strategy_requested`` and ``strategy_used``; the previous
  ``execution_strategy`` field reported the *requested* strategy as if
  it were the one that ran.
- **``_last_decision`` is a local variable.** Concurrent
  ``evaluate_agent`` calls no longer clobber each other's learning
  signal or the reported strategy.
- **Corrected distillation optimizer.** 4-tuple ``select_action``;
  5-arg ``update``; gate gradient ``teacher_outputs @ error``;
  per-instance RNGs; config validated at construction; ``update``
  under an ``RLock``; over-sized teacher outputs sliced before any
  ``np.pad``.
- **Named energy / carbon ceilings.** ``MAX_ENERGY_KWH = 0.1``,
  ``MAX_CARBON_KG = 0.02``, ``MAX_LATENCY_MS = 1000``; each uses
  ``_clamp01`` on both sides.
- **``modp_weights`` validated.** Shape ``(4,)``, finite, non-negative,
  positive sum. Invalid values raise :class:`AgentBenchError`.
- **``callable(getattr(agent, 'run', None))``** instead of
  ``hasattr(agent, 'run')`` — a non-callable ``run`` attribute no
  longer passes the check and then raises ``TypeError``.
- **Aware timestamps.** ``datetime.now(timezone.utc)`` throughout;
  the previous ``datetime.utcnow()`` was deprecated on 3.12 and mixed
  naive/aware timestamps in the same payload.
- **Feedback emission logs failures** (``logger.exception``) instead
  of ``except Exception: pass``; ``strict`` mode re-raises.
- **Frozen, validated dataclasses.** :class:`EvaluationState` rejects
  malformed features at construction; ``from_task`` is the normalising
  constructor.
- **Narrow error type.** :class:`AgentBenchError` replaces the previous
  broad ``ValueError`` / ``AttributeError`` / ``KeyError`` leaks.
- **Bounded task registry.** :class:`AgentBenchAdapter.task_registry`
  is guarded by an ``RLock`` and evicts the oldest entries when the
  configurable cap is reached.
- **``strict`` mode.** ``AgentBenchAdapter(..., strict=True)`` raises
  on enhancement initialisation or execution failures.
- **Structured ``export_to_json`` / ``load_from_json``.** ``OSError``
  and ``JSONDecodeError`` are wrapped in :class:`AgentBenchError`;
  parent directories are created on demand.
- **Lazy ``%s`` logging.** ``logger.exception`` on caught exceptions.
- ``__version__``, ``__all__``, ``__dir__``, and a ``_check()``-based
  smoke test.

Compatibility
-------------
- ``AgentBenchAdapter(protocol_version="1.0", config=None)`` — same
  positional signature. ``strict=False`` is new and keyword-only.
- ``evaluate_agent(agent, task, ...)`` — the ``execution_strategy``
  keyword is new and optional. When provided, the caller's strategy
  is honoured and the adapter's local optimizer is bypassed for that
  call. ``agent_evaluator.py``'s ``_method_accepts_kwarg`` probe will
  now find it and pass it — eliminating the previous dual-optimizer
  ambiguity.
- ``evaluate_agent`` result — adds ``strategy_requested`` and
  ``strategy_used`` (additive). The legacy ``execution_strategy``
  field is **no longer set** by this adapter (it was misleading).
- ``EvaluationDistillationOptimizer.select_action`` returns 4 values
  (``action, state_vec, teacher_outputs, teacher_combined``);
  ``update`` takes 5 args (no ``next_state_vec``).
- ``EvaluationState`` is frozen; use :meth:`EvaluationState.from_task`
  for the normalising constructor.
- ``validate_result`` performs type and value checks, not just presence.
- ``_calculate_accuracy`` returns ``None`` only when the task has no
  ``expected_output`` field at all; empty / falsy values are compared.
- ``task["task_id"]`` — missing keys produce ``"unknown"``, not
  ``KeyError``.

Requires Python 3.8+. ``slots=True`` on dataclasses requires 3.10+;
drop ``slots=True`` for 3.8/3.9.
"""

from __future__ import annotations

import enum
import hashlib
import json
import logging
import math
import random
import threading
from collections import OrderedDict, deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import (
    Any, Deque, Dict, Final, List, Mapping, Optional, Tuple,
)

import numpy as np


logger = logging.getLogger(__name__)

__version__ = "2.5.0"


# --------------------------------------------------------------------------- #
# Errors
# --------------------------------------------------------------------------- #
class AgentBenchError(ValueError):
    """Raised for invalid AgentBench adapter inputs or execution failures."""


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

# 7 base + 5 framework onehot + 1 has_backend = 13. Matches the
# enhanced evaluation agent so the two layers interoperate.
FEATURE_DIM: Final[int] = 13
N_ACTIONS: Final[int] = 3
N_TEACHERS: Final[int] = 3

DEFAULT_REPLAY_SIZE: Final[int] = 2_000
DEFAULT_TRAIN_EVERY: Final[int] = 10
DEFAULT_BATCH_SIZE: Final[int] = 8

KNOWN_FRAMEWORKS: Final[Tuple[str, ...]] = (
    "autogen", "langchain", "crewai", "limit_graph", "other",
)
UNKNOWN_FRAMEWORK: Final[str] = "other"

STRATEGY_MAP: Final[Mapping[int, str]] = {
    0: "native",
    1: "flexgen_low_precision",
    2: "flexgen_high_precision",
}

# Metrics required for a sustainability index.
REQUIRED_METRICS: Final[frozenset] = frozenset({
    "accuracy", "energy_kwh", "carbon_co2e_kg",
})

DEFAULT_TASK_REGISTRY_MAX: Final[int] = 10_000
DEFAULT_EVALUATION_METRICS: Final[Tuple[str, ...]] = (
    "accuracy", "latency", "energy_kwh", "carbon_co2e_kg",
)


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
        raise AgentBenchError(f"{name} must be numeric.")
    fv = float(value)
    if not math.isfinite(fv):
        raise AgentBenchError(f"{name} must be finite, got {value!r}.")
    return fv


def _nonneg(name: str, value: Any) -> float:
    fv = _finite(name, value)
    if fv < 0.0:
        raise AgentBenchError(f"{name} must be >= 0, got {fv}.")
    return fv


def _unit_interval(name: str, value: Any) -> float:
    fv = _finite(name, value)
    if not 0.0 <= fv <= 1.0:
        raise AgentBenchError(f"{name} must be in [0, 1], got {fv}.")
    return fv


def _framework_onehot(name: Any) -> Tuple[float, ...]:
    candidate = str(name).lower() if name is not None else UNKNOWN_FRAMEWORK
    if candidate not in KNOWN_FRAMEWORKS:
        candidate = UNKNOWN_FRAMEWORK
    return tuple(
        1.0 if candidate == known else 0.0 for known in KNOWN_FRAMEWORKS
    )


# --------------------------------------------------------------------------- #
# Optional dependency imports
# --------------------------------------------------------------------------- #
class _UnavailableComponent:
    """Placeholder for an unavailable dependency; raises on use."""

    __slots__ = ("_name",)

    def __init__(self, name: str, *a: Any, **kw: Any) -> None:
        self._name = name
        raise AgentBenchError(
            f"Dependency '{name}' is unavailable; install it to use "
            f"the corresponding feature."
        )


try:
    from .green_metrics import GreenMetricsTracker  # type: ignore
except ImportError as _exc:  # pragma: no cover — defensive
    logger.info("GreenMetricsTracker unavailable: %s", _exc)
    GreenMetricsTracker = _UnavailableComponent  # type: ignore[assignment,misc]


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
            raise AgentBenchError("framework_onehot must be a tuple.")
        if len(self.framework_onehot) != len(KNOWN_FRAMEWORKS):
            raise AgentBenchError(
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
            raise AgentBenchError(
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

    Actions: ``0 = native``, ``1 = FlexGen low precision``,
    ``2 = FlexGen high precision``.

    ``select_action`` returns ``(action, state_vec, teacher_outputs,
    teacher_combined)``. ``update`` takes
    ``(state_vec, action, reward, teacher_outputs, teacher_combined)``.

    .. note::
        Matches :class:`src.integrations.base_adapter.DistillationOptimizer`.
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
            raise AgentBenchError(
                f"config must be a dict, got {type(config).__name__}."
            )

        def _num(
            name: str, raw: Any, default: float, *, positive: bool,
        ) -> float:
            v = default if raw is None else raw
            if isinstance(v, bool) or not isinstance(v, (int, float)):
                raise AgentBenchError(f"{name} must be numeric.")
            fv = float(v)
            if not math.isfinite(fv):
                raise AgentBenchError(f"{name} must be finite.")
            if positive and fv <= 0:
                raise AgentBenchError(f"{name} must be > 0.")
            if not positive and fv < 0:
                raise AgentBenchError(f"{name} must be >= 0.")
            return fv

        def _int(
            name: str, raw: Any, default: int, *, positive: bool,
        ) -> int:
            v = default if raw is None else raw
            if isinstance(v, bool) or not isinstance(v, int):
                raise AgentBenchError(f"{name} must be an int.")
            if positive and v <= 0:
                raise AgentBenchError(f"{name} must be > 0.")
            if not positive and v < 0:
                raise AgentBenchError(f"{name} must be >= 0.")
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
            raise AgentBenchError("epsilon must be <= 1.0.")
        out["distill_weight"] = _num(
            "distill_weight", config.get("distill_weight"),
            0.7, positive=False,
        )
        out["rl_weight"] = _num(
            "rl_weight", config.get("rl_weight"),
            0.3, positive=False,
        )
        if out["distill_weight"] + out["rl_weight"] <= 0:
            raise AgentBenchError(
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
            raise AgentBenchError(
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
            raise AgentBenchError("state must be an EvaluationState.")
        x = state.to_feature_vector()
        if x.shape[0] != self.feature_dim:
            raise AgentBenchError(
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
            raise AgentBenchError(
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
# Adapter
# --------------------------------------------------------------------------- #
class AgentBenchAdapter:
    """
    Adapter for AgentBench protocol compatibility with optional
    enhancements.

    **Single-optimizer contract**: when a caller supplies an explicit
    ``execution_strategy`` to :meth:`evaluate_agent`, this adapter
    honours it and does **not** run its own optimizer for that call.
    The adapter's local optimizer is only used when no strategy is
    provided (i.e., when the adapter is used standalone).

    ``strict`` mode (keyword-only) raises on enhancement initialisation
    or execution failures; the default silently degrades.
    """

    def __init__(
        self,
        protocol_version: str = "1.0",
        config: Optional[Dict[str, Any]] = None,
        *,
        strict: bool = False,
    ) -> None:
        if not isinstance(protocol_version, str) or not protocol_version:
            raise AgentBenchError(
                "protocol_version must be a non-empty string."
            )
        if config is not None and not isinstance(config, dict):
            raise AgentBenchError(
                f"config must be a dict or None, got "
                f"{type(config).__name__}."
            )

        self.protocol_version = protocol_version
        self.config: Dict[str, Any] = dict(config) if config else {}
        self._strict: bool = bool(strict)

        self.use_enhancements: bool = (
            bool(self.config.get("use_enhancements", False))
            and ENHANCEMENTS_AVAILABLE
        )
        if (
            self._strict
            and self.config.get("use_enhancements", False)
            and not ENHANCEMENTS_AVAILABLE
        ):
            raise AgentBenchError(
                "use_enhancements=True requires the enhancements package."
            )

        self._lock = threading.RLock()
        self._task_registry_lock = threading.RLock()
        self._max_tasks: int = int(
            self.config.get(
                "max_tasks", DEFAULT_TASK_REGISTRY_MAX,
            )
        )
        # Bounded, insertion-ordered: oldest entries evicted first.
        self.task_registry: "OrderedDict[str, Dict[str, Any]]" = OrderedDict()

        self.distillation_optimizer: Optional[
            EvaluationDistillationOptimizer
        ] = None
        self.node_descriptor: Any = None
        self.zero_trust: Any = None

        if self.use_enhancements:
            self._init_enhancements()

    # ---------------------------------------------------------------- init
    def _init_enhancements(self) -> None:
        try:
            self.distillation_optimizer = EvaluationDistillationOptimizer(
                self.config,
            )
            if self.config.get("use_node_descriptor", False):
                self.node_descriptor = NodeDescriptor(
                    id="agentbench_node",
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
            logger.info("AgentBenchAdapter enhanced components initialized")
        except Exception:
            logger.exception("Failed to initialize enhancements")
            self.use_enhancements = False
            self.distillation_optimizer = None
            self.node_descriptor = None
            self.zero_trust = None
            if self._strict:
                raise

    # ---------------------------------------------------------------- create
    def create_task(
        self,
        task_id: str,
        suite: str,
        task_type: str,
        input_data: Any,
        expected_output: Any = None,
        evaluation_metrics: Optional[Sequence[str]] = None,
        difficulty: str = "medium",
        timeout_seconds: float = 30,
    ) -> Dict[str, Any]:
        """
        Create a task in AgentBench format.

        The task is registered in the adapter's bounded, thread-safe
        registry. When the registry exceeds ``config["max_tasks"]``,
        the oldest entry is evicted.
        """
        if not isinstance(task_id, str) or not task_id:
            raise AgentBenchError("task_id must be a non-empty string.")
        if not isinstance(suite, str) or not suite:
            raise AgentBenchError("suite must be a non-empty string.")
        if not isinstance(task_type, str) or not task_type:
            raise AgentBenchError("task_type must be a non-empty string.")

        metrics = (
            list(evaluation_metrics)
            if evaluation_metrics is not None
            else list(DEFAULT_EVALUATION_METRICS)
        )

        task: Dict[str, Any] = {
            "task_id": task_id,
            "suite": suite,
            "type": task_type,
            "difficulty": difficulty,
            "input": input_data,
            "expected_output": expected_output,
            "evaluation": {
                "metrics": metrics,
                "timeout_seconds": float(timeout_seconds),
            },
            "protocol_version": self.protocol_version,
            "created_at": datetime.now(timezone.utc).isoformat(),
        }
        with self._task_registry_lock:
            self.task_registry[task_id] = task
            while len(self.task_registry) > self._max_tasks:
                self.task_registry.popitem(last=False)
        logger.info("Created task %s in suite %s", task_id, suite)
        return task

    # ---------------------------------------------------------------- evaluate
    def evaluate_agent(
        self,
        agent: Any,
        task: Mapping[str, Any],
        track_energy: bool = True,
        track_carbon: bool = True,
        backend: Optional[str] = None,
        rank: Optional[int] = None,
        graph_metrics: Optional[Dict[str, float]] = None,
        human_feedback_score: Optional[float] = None,
        execution_strategy: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Evaluate ``agent`` on ``task``.

        Parameters
        ----------
        execution_strategy : str, optional
            When provided (by :class:`~evaluation.agent_evaluator.AgentEvaluator`
            or another caller), the adapter honours it and does **not**
            consult its local optimizer for this call. When omitted, the
            adapter's own optimizer selects the strategy (standalone
            enhanced mode).

        Returns
        -------
        dict
            ``strategy_requested`` and ``strategy_used`` are always
            present on the result; ``strategy_used`` is ``"native"``
            when the adapter has no FlexGen dispatch (this version).
        """
        if not isinstance(task, Mapping):
            raise AgentBenchError(
                f"task must be a Mapping, got {type(task).__name__}."
            )
        task_id = str(task.get("task_id", "unknown"))
        logger.info("Evaluating agent on task %s", task_id)

        # ---- Decide strategy ------------------------------------ #
        decision: Optional[_Decision] = None
        strategy_requested: str
        if execution_strategy is not None:
            # Caller-supplied strategy: honour it; skip local training
            # so two optimizers never train on the same reward.
            strategy_requested = self._validate_strategy(execution_strategy)
        elif self.use_enhancements and self.distillation_optimizer is not None:
            if human_feedback_score is None:
                human_feedback_score = float(
                    self.config.get("human_feedback_score", 0.5)
                )
            if graph_metrics is None:
                gm_cfg = self.config.get("graph_metrics", {})
                graph_metrics = (
                    dict(gm_cfg) if isinstance(gm_cfg, Mapping) else {}
                )
            carbon_intensity = float(
                self.config.get("carbon_intensity", 400.0)
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
        else:
            strategy_requested = "native"

        # The adapter has no FlexGen dispatch today; the effective
        # strategy is always native. This field must not lie.
        strategy_used = "native"

        # ---- Green metrics -------------------------------------- #
        tracker = None
        if track_energy or track_carbon:
            try:
                tracker = GreenMetricsTracker(
                    track_energy=track_energy,
                    track_carbon=track_carbon,
                )
                tracker.start()
            except Exception:
                logger.exception("GreenMetricsTracker.start() failed")
                if self._strict:
                    raise
                tracker = None

        # ---- Execute -------------------------------------------- #
        start_time = datetime.now(timezone.utc)
        success = False
        output: Any = None
        error: Optional[str] = None
        try:
            run_method = getattr(agent, "run", None)
            if callable(run_method):
                output = run_method(task.get("input"))
            elif callable(agent):
                output = agent(task.get("input"))
            else:
                raise AgentBenchError(
                    "Agent must expose a callable 'run' method or be "
                    "callable."
                )
            success = True
        except Exception as exc:
            logger.exception("Agent execution failed")
            error = repr(exc)
            if self._strict:
                if tracker is not None:
                    try:
                        tracker.stop()
                    except Exception:
                        logger.exception(
                            "tracker.stop() failed during strict re-raise"
                        )
                raise

        end_time = datetime.now(timezone.utc)
        latency_ms = (end_time - start_time).total_seconds() * 1000.0

        # ---- Green metrics snapshot ----------------------------- #
        green_metrics: Dict[str, Any] = {}
        if tracker is not None:
            try:
                tracker.stop()
                green_metrics = dict(tracker.get_metrics() or {})
            except Exception:
                logger.exception("GreenMetricsTracker.stop() failed")
                if self._strict:
                    raise

        # Do not let green_metrics silently override our own
        # latency_ms measurement.
        green_metrics.pop("latency_ms", None)

        # ---- Accuracy ------------------------------------------- #
        accuracy: Optional[float] = None
        if "expected_output" in task:
            accuracy = self._calculate_accuracy(
                output, task.get("expected_output"),
            )

        # ---- Result --------------------------------------------- #
        result: Dict[str, Any] = {
            "task_id": task_id,
            "agent_name": getattr(
                agent, "name", type(agent).__name__,
            ),
            "framework": getattr(agent, "framework", "unknown"),
            "success": success,
            "output": output,
            "error": error,
            "strategy_requested": strategy_requested,
            "strategy_used": strategy_used,
            "metrics": {
                "latency_ms": latency_ms,
                **green_metrics,
            },
            "provenance": {
                "hash": self._compute_hash(task, output),
                "run_at": end_time.isoformat(),
                "backend": backend,
                "rank": rank,
            },
            "protocol_version": self.protocol_version,
        }
        if accuracy is not None:
            result["metrics"]["accuracy"] = accuracy

        # ---- Sustainability index ------------------------------- #
        self._maybe_add_sustainability_index(result)

        # ---- Enhanced post-processing (standalone mode only) ---- #
        if decision is not None and self.distillation_optimizer is not None:
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

            if reward is not None:
                result["modp_reward"] = reward
                result["graph_metrics"] = dict(graph_metrics or {})
                result["human_feedback_score"] = human_feedback_score
                result["distillation_stats"] = {
                    "counter": self.distillation_optimizer.counter,
                    "buffer_size": len(
                        self.distillation_optimizer.replay_buffer
                    ),
                }
                self._emit_feedback(task_id, result, reward)

        logger.info("Evaluation complete for task %s", task_id)
        return result

    # ---------------------------------------------------------------- helpers
    @staticmethod
    def _validate_strategy(strategy: Any) -> str:
        if strategy in STRATEGY_MAP.values():
            return strategy
        raise AgentBenchError(
            f"unknown execution_strategy {strategy!r}; expected one of "
            f"{sorted(STRATEGY_MAP.values())}."
        )

    def _maybe_add_sustainability_index(
        self, result: Dict[str, Any],
    ) -> None:
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
        new_metrics = dict(metrics)
        new_metrics["sustainability_index"] = float(index)
        result["metrics"] = new_metrics

    def _compute_modp_reward(
        self, result: Mapping[str, Any],
    ) -> float:
        """
        Compute the MODP reward with success gating.

        ``reward = modp_score * failure_reward_multiplier`` — ``0.0`` on
        failure by default, so a failed evaluation cannot score
        positively.
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
            raise AgentBenchError(
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
                source="agentbench_adapter",
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
                    "agentbench",
                    str(result.get("strategy_requested")),
                ],
            )
            logger.debug("FeedbackEvent created: %s", event.event_id)
        except Exception:
            logger.exception("FeedbackEvent construction failed")
            if self._strict:
                raise

    # ---------------------------------------------------------------- util
    @staticmethod
    def _calculate_accuracy(output: Any, expected: Any) -> float:
        """
        Compare ``output`` to ``expected``.

        Returns ``None`` only when the caller has no ``expected`` to
        compare against (the caller decides whether to skip). An empty
        / falsy ``expected`` is compared, not skipped.
        """
        if isinstance(output, dict) and isinstance(expected, dict):
            if not expected:
                return 1.0 if not output else 0.0
            matches = sum(
                1 for k, v in expected.items()
                if k in output and output[k] == v
            )
            return matches / len(expected)
        if isinstance(output, str) and isinstance(expected, str):
            return (
                1.0
                if output.strip().lower() == expected.strip().lower()
                else 0.0
            )
        return 1.0 if output == expected else 0.0

    @staticmethod
    def _compute_hash(
        task: Mapping[str, Any], output: Any,
    ) -> str:
        """
        Deterministic SHA-256 of the (task_id, output) pair.

        The wall-clock timestamp is **not** included, so two runs with
        the same inputs produce the same hash. The run timestamp lives
        on ``provenance["run_at"]``.
        """
        canonical = json.dumps(
            {
                "task_id": task.get("task_id"),
                "output": output,
            },
            sort_keys=True, default=str,
        )
        return hashlib.sha256(canonical.encode("utf-8")).hexdigest()

    def validate_result(self, result: Mapping[str, Any]) -> bool:
        """Validate the shape and types of an evaluation result."""
        if not isinstance(result, Mapping):
            return False
        required = {
            "task_id": str,
            "agent_name": str,
            "framework": str,
            "success": bool,
            "metrics": Mapping,
            "provenance": Mapping,
            "protocol_version": str,
        }
        for field_name, expected_type in required.items():
            if field_name not in result:
                return False
            if not isinstance(result[field_name], expected_type):
                return False
        if not result["task_id"] or not result["protocol_version"]:
            return False
        return True

    def export_to_json(
        self, result: Mapping[str, Any], filepath: str,
    ) -> Path:
        """Write ``result`` to ``filepath``; return the resolved path."""
        if not isinstance(result, Mapping):
            raise AgentBenchError("result must be a Mapping.")
        path = Path(filepath)
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            with path.open("w", encoding="utf-8") as f:
                json.dump(dict(result), f, indent=2, default=str)
        except OSError as exc:
            raise AgentBenchError(
                f"could not write {path}: {exc}"
            ) from exc
        logger.info("Result exported to %s", path)
        return path

    def load_from_json(self, filepath: str) -> Dict[str, Any]:
        """Read a result from ``filepath``."""
        path = Path(filepath)
        try:
            with path.open("r", encoding="utf-8") as f:
                data = json.load(f)
        except OSError as exc:
            raise AgentBenchError(
                f"could not read {path}: {exc}"
            ) from exc
        except json.JSONDecodeError as exc:
            raise AgentBenchError(
                f"invalid JSON in {path}: {exc}"
            ) from exc
        if not isinstance(data, dict):
            raise AgentBenchError(
                f"root of {path} must be a JSON object, got "
                f"{type(data).__name__}."
            )
        logger.info("Result loaded from %s", path)
        return data

    # ---------------------------------------------------------------- dunder
    def __repr__(self) -> str:
        return (
            "AgentBenchAdapter("
            f"protocol_version={self.protocol_version!r}, "
            f"enhancements={'on' if self.use_enhancements else 'off'}, "
            f"strict={self._strict}, "
            f"tasks={len(self.task_registry)})"
        )


# --------------------------------------------------------------------------- #
# Public API
# --------------------------------------------------------------------------- #
__all__: List[str] = [
    "__version__",
    "AgentBenchAdapter",
    "AgentBenchError",
    "DEFAULT_EVALUATION_METRICS",
    "DEFAULT_MODP_WEIGHTS",
    "EvaluationDistillationOptimizer",
    "EvaluationState",
    "FEATURE_DIM",
    "KNOWN_FRAMEWORKS",
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
# Smoke test: python -m evaluation.agentbench_adapter
# --------------------------------------------------------------------------- #
if __name__ == "__main__":  # pragma: no cover
    import sys
    import tempfile

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

    # ---- 1. Framework onehot --------------------------------- #
    print("framework onehot:")
    _check(
        "known framework gets a slot",
        _framework_onehot("autogen") == (1.0, 0.0, 0.0, 0.0, 0.0),
    )
    _check(
        "unknown framework → other",
        _framework_onehot("custom") == (0.0, 0.0, 0.0, 0.0, 1.0),
    )
    _check(
        "None framework → other",
        _framework_onehot(None) == (0.0, 0.0, 0.0, 0.0, 1.0),
    )

    # ---- 2. EvaluationState --------------------------------- #
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
        _raises(AgentBenchError, lambda: EvaluationState(
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

    # ---- 3. Optimizer --------------------------------------- #
    print("optimizer:")
    opt = EvaluationDistillationOptimizer({"seed": 7})
    action, x, t_out, t_comb = opt.select_action(state)
    _check("action in 0..2", 0 <= action < N_ACTIONS)
    _check("feature_dim matches vector", x.shape[0] == opt.feature_dim)
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
        _raises(AgentBenchError, lambda:
                opt.update(x, 0, float("nan"), t_out, t_comb)),
    )
    _check(
        "rejects bad epsilon",
        _raises(AgentBenchError, lambda:
                EvaluationDistillationOptimizer({"epsilon": "high"})),
    )

    # ---- 4. Reward with success gate ----------------------- #
    print("reward:")
    class _Probe:
        config: Dict[str, Any] = {}
        _strict = False
        _compute_modp_reward = AgentBenchAdapter._compute_modp_reward
        _compute_hash = staticmethod(AgentBenchAdapter._compute_hash)

    p = _Probe()
    r_ok = p._compute_modp_reward({
        "success": True,
        "metrics": {
            "accuracy": 0.9, "energy_kwh": 0.02,
            "latency_ms": 50.0, "carbon_co2e_kg": 0.005,
        },
    })
    r_fail = p._compute_modp_reward({
        "success": False, "metrics": {},
    })
    _check("success → reward > 0", r_ok > 0.0)
    _check("failure → reward == 0", r_fail == 0.0)

    class _BadProbe:
        config = {"modp_weights": [0.0, 0.0, 0.0, 0.0]}
        _strict = False
        _compute_modp_reward = AgentBenchAdapter._compute_modp_reward
    _check(
        "zero-sum modp_weights rejected",
        _raises(AgentBenchError, lambda:
                _BadProbe()._compute_modp_reward({
                    "success": True, "metrics": {},
                })),
    )

    # ---- 5. Deterministic hash ----------------------------- #
    print("deterministic hash:")
    task = {"task_id": "t1", "input": "hello"}
    h1 = AgentBenchAdapter._compute_hash(task, "output")
    h2 = AgentBenchAdapter._compute_hash(task, "output")
    h3 = AgentBenchAdapter._compute_hash(task, "different-output")
    _check("identical inputs → identical hash", h1 == h2)
    _check("different output → different hash", h1 != h3)

    # ---- 6. Export / load round-trip ----------------------- #
    print("export / load:")
    with tempfile.TemporaryDirectory() as tmpdir:
        adapter = AgentBenchAdapter()
        payload = {
            "task_id": "t1", "agent_name": "a", "framework": "x",
            "success": True, "metrics": {"latency_ms": 1.0},
            "provenance": {"hash": "abc"},
            "protocol_version": "1.0",
        }
        path = adapter.export_to_json(payload, str(Path(tmpdir) / "out.json"))
        _check("export created file", path.exists())
        restored = adapter.load_from_json(str(path))
        _check("load returned dict", isinstance(restored, dict))
        _check("load round-trips", restored == payload)

    # ---- 7. validate_result -------------------------------- #
    print("validate_result:")
    adapter = AgentBenchAdapter()
    _check(
        "accepts valid result",
        adapter.validate_result({
            "task_id": "t1", "agent_name": "a", "framework": "x",
            "success": True, "metrics": {}, "provenance": {},
            "protocol_version": "1.0",
        }),
    )
    _check(
        "rejects empty task_id",
        adapter.validate_result({
            "task_id": "", "agent_name": "a", "framework": "x",
            "success": True, "metrics": {}, "provenance": {},
            "protocol_version": "1.0",
        }) is False,
    )
    _check(
        "rejects wrong type",
        adapter.validate_result({
            "task_id": "t1", "agent_name": "a", "framework": "x",
            "success": "yes", "metrics": {}, "provenance": {},
            "protocol_version": "1.0",
        }) is False,
    )

    # ---- 8. create_task with bounded registry ------------- #
    print("create_task:")
    adapter = AgentBenchAdapter(config={"max_tasks": 3})
    for i in range(5):
        adapter.create_task(
            task_id=f"task-{i}", suite="s", task_type="inference",
            input_data={"q": f"hi-{i}"},
        )
    _check("registry bound honoured",
           len(adapter.task_registry) == 3)
    _check(
        "oldest entries evicted",
        "task-0" not in adapter.task_registry
        and "task-4" in adapter.task_registry,
    )

    # ---- 9. Strategy acceptance --------------------------- #
    print("strategy acceptance:")

    class _Agent:
        framework = "custom"
        name = "test-agent"

        def run(self, x: Any) -> str:  # noqa: ARG002
            return "ok"

    adapter = AgentBenchAdapter()
    task_dict = adapter.create_task(
        task_id="t-strat", suite="s", task_type="inference",
        input_data={"q": "hi"},
    )
    # Caller-supplied strategy is honoured; no local optimizer trained.
    result = adapter.evaluate_agent(
        _Agent(), task_dict, execution_strategy="flexgen_low_precision",
    )
    _check(
        "strategy_requested echoed",
        result["strategy_requested"] == "flexgen_low_precision",
    )
    _check(
        "strategy_used is native (adapter has no dispatch)",
        result["strategy_used"] == "native",
    )
    _check(
        "no local modp_reward when caller supplied strategy",
        "modp_reward" not in result,
    )
    _check(
        "rejects unknown strategy",
        _raises(AgentBenchError, lambda:
                adapter.evaluate_agent(
                    _Agent(), task_dict,
                    execution_strategy="bogus",
                )),
    )

    # ---- 10. Agent execution failure --------------------- #
    print("execution failure:")

    class _FailingAgent:
        framework = "custom"
        name = "failing"

        def run(self, x: Any) -> str:
            raise RuntimeError("boom")

    adapter = AgentBenchAdapter()
    task_dict = adapter.create_task(
        task_id="t-fail", suite="s", task_type="inference",
        input_data={"q": "hi"},
    )
    result = adapter.evaluate_agent(_FailingAgent(), task_dict)
    _check("success=False", result["success"] is False)
    _check("error populated", result["error"] is not None)
    _check(
        "strategy_used still present",
        result["strategy_used"] == "native",
    )

    # ---- 11. __all__ / __dir__ --------------------------- #
    print("dir:")
    _check("__dir__ matches __all__", __dir__() == sorted(__all__))
    for _name in (
        "AgentBenchAdapter", "AgentBenchError",
        "EvaluationDistillationOptimizer", "EvaluationState",
        "DEFAULT_MODP_WEIGHTS", "FEATURE_DIM", "N_ACTIONS",
        "STRATEGY_MAP", "REQUIRED_METRICS",
    ):
        _check(f"{_name} exported", _name in __all__)

    print()
    print(f"passed: {passed}  failed: {failed}")
    if failed:
        sys.exit(1)
