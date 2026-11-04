# -*- coding: utf-8 -*-
"""
Base Agent Adapter (Enhanced)
=============================

Abstract base class for framework-specific adapters with optional
integration of LIMIT Graph, MODP, RLHF, multi-teacher on-policy
distillation, bio-inspired optimisation, and MoE expert gating.

Design notes
------------
- ``run()`` accepts an optional ``track_green_metrics`` override **per
  call**; the override does not persist between calls. The
  instance-level default (``self.track_green_metrics``) is set at
  construction and is not mutated by ``run()``.
- ``agent_name`` is a lazily-computed property; subclasses may safely
  reference instance attributes set after ``super().__init__(...)``
  inside ``_get_agent_name()``.
- ``run_with_enhancements`` raises :class:`AdapterError` if the
  enhancements flag is on but the optimizer is missing. The inherited
  pipeline never proceeds with a ``None`` optimizer.
- The distillation optimizer is exposed at module level
  (:class:`DistillationOptimizer`) so it can be imported, subclassed, and
  tested independently. It fixes the gating-gradient and
  teacher-outputs-in-replay-buffer bugs present in the three previous
  copies (this file's nested class, ``autogen_adapter.py``, and
  ``autogen_runtime.py``). A follow-up refactor may move it to
  ``src/enhancements/distillation.py`` and have all three files import
  from there.

Key fixes over the previous version
-----------------------------------
- **Runtime contract enforcement.** ``_execute`` is validated to return
  a ``dict`` at both call sites; a subclass that forgets to override it
  raises :class:`AdapterError` instead of crashing downstream with
  ``TypeError: argument of type 'NoneType' is not iterable``.
- **Module-level ``DistillationOptimizer``.** No longer a nested class
  inside ``_create_distillation_optimizer``; importable, subclassable,
  testable.
- **Corrected optimizer.** Gating gradient uses ``teacher_outputs @
  error`` (individual teacher outputs, correct shape); replay buffer
  stores individual teacher outputs alongside the pre-combined
  distribution; ``_clamp01`` on every feature; ``list(deque)`` before
  ``random.sample``; per-instance RNGs; config validated at
  construction.
- **Success gating.** The MODP reward is multiplied by ``success``
  (0 or 1, or ``failure_reward_multiplier`` from config). A failed run
  cannot score positively.
- **Named features.** The magic indices ``state_vec[3]`` / ``state_vec[7]``
  are replaced by a ``NormalizedFeatures`` dataclass with named fields.
- **``energy_norm`` corrected.** Normalized against
  ``MAX_ENERGY_KWH`` (default 0.1), not ``1.0``.
- **Per-call ``track_green_metrics``.** No longer mutated on ``self``.
- **Deferred ``agent_name``.** A property that lazily computes the name;
  subclasses can reference attributes set after ``super().__init__``.
- **``_try_async`` helper.** Sync-first, running-loop, or
  ``asyncio.run()`` — with ``logger.exception`` on every failure path.
- **Strict mode.** ``BaseAgentAdapter(..., strict=True)`` raises on
  integration or execution failures instead of silently downgrading.
- **Narrow error types** — :class:`AdapterError`,
  :class:`AdapterConfigError`.
- **``ExecutionStrategy`` enum** replaces the raw-string ``strategy_map``.
- **Config hardening** — copies on construction; ``modp_weights``
  validated (finite, non-negative, positive sum) and copied in
  ``get_metadata``.
- Lazy ``%s`` logging; ``logger.exception`` on every caught exception.
- ``__version__``, ``__all__``, ``__dir__``, and a ``_check()``-based
  smoke test that survives ``python -O``.

Compatibility
-------------
- ``BaseAgentAdapter(agent, framework_name, config=None, *, strict=False)``
  — ``strict`` is new and keyword-only. Default preserves legacy
  behaviour.
- ``run(task_input, track_green_metrics=None)`` — the keyword argument
  is honoured **per call** and does not persist (old behaviour persisted
  it on ``self``).
- ``run_with_enhancements(task_input, *, track=None)`` — gains a
  keyword-only ``track`` argument; the no-argument call is unchanged.
- ``DistillationOptimizer.select_action`` returns
  ``(action, state_vec, teacher_outputs, teacher_combined)`` — 4 values.
  Callers of ``run_with_enhancements`` are unaffected; direct callers of
  ``select_action`` must update.
- ``get_metadata()["adapter_version"]`` is now ``__version__``
  (``"2.5.0"``), not the hardcoded ``"2.4.2"``.
- ``_execute`` must return a ``dict``; a subclass returning ``None`` now
  raises :class:`AdapterError` at the call site.

Requires Python 3.8+.
"""

from __future__ import annotations

import asyncio
import logging
import math
import random
import threading
import time
from abc import ABC, abstractmethod
from collections import deque
from dataclasses import dataclass
from enum import Enum
from typing import Any, Deque, Dict, Final, List, Optional, Tuple

import numpy as np


logger = logging.getLogger(__name__)

__version__ = "2.5.0"


# --------------------------------------------------------------------------- #
# Errors
# --------------------------------------------------------------------------- #
class AdapterError(ValueError):
    """Raised for invalid adapter inputs or execution failures."""


class AdapterConfigError(AdapterError):
    """Raised for invalid adapter or optimizer configuration."""


# --------------------------------------------------------------------------- #
# Constants
# --------------------------------------------------------------------------- #
MAX_TOKENS: Final[float] = 10_000.0
MAX_LATENCY_MS: Final[float] = 1_000.0
MAX_CARBON_INTENSITY: Final[float] = 500.0
MAX_COST_BUDGET: Final[float] = 100.0
MAX_ENERGY_KWH: Final[float] = 0.1

FEATURE_DIM: Final[int] = 8
N_ACTIONS: Final[int] = 3
N_TEACHERS: Final[int] = 3
REPLAY_BUFFER_SIZE: Final[int] = 2_000
TRAIN_EVERY: Final[int] = 10
BATCH_SIZE: Final[int] = 8

DEFAULT_MODP_WEIGHTS: Final[Tuple[float, ...]] = (0.4, 0.3, 0.2, 0.1)


# --------------------------------------------------------------------------- #
# Unavailable-component stub
# --------------------------------------------------------------------------- #
class _UnavailableComponent:
    """
    Placeholder for an unavailable enhancement component.

    Fails loudly on construction so accidental use of a missing
    integration does not silently produce ``None`` behaviour.
    """

    __slots__ = ("_name",)

    def __init__(self, name: str, *args: Any, **kwargs: Any) -> None:
        self._name = name
        raise AdapterError(
            f"Enhancement component '{name}' is unavailable; install "
            f"the enhancements package to use this adapter in enhanced "
            f"mode."
        )


# --------------------------------------------------------------------------- #
# Optional enhancement imports
# --------------------------------------------------------------------------- #
try:
    from src.enhancements.schemas.workload_descriptor import (
        WorkloadDescriptor,
        TaskType,
        Urgency,
    )
    from src.enhancements.schemas.node_descriptor import (
        NodeDescriptor,
        NodeType,
    )
    from src.enhancements.zero_trust_architecture import (
        ZeroTrustArchitecture,
    )
    from src.enhancements.schemas.feedback_event import FeedbackEvent

    ENHANCEMENTS_AVAILABLE: bool = True
except ImportError as _exc:  # pragma: no cover — defensive
    ENHANCEMENTS_AVAILABLE = False
    logger.info("Enhancement modules unavailable: %s", _exc)
    WorkloadDescriptor = _UnavailableComponent      # type: ignore[assignment,misc]
    NodeDescriptor = _UnavailableComponent          # type: ignore[assignment,misc]
    ZeroTrustArchitecture = _UnavailableComponent   # type: ignore[assignment,misc]
    FeedbackEvent = _UnavailableComponent           # type: ignore[assignment,misc]
    TaskType = _UnavailableComponent                # type: ignore[assignment,misc]
    Urgency = _UnavailableComponent                 # type: ignore[assignment,misc]
    NodeType = _UnavailableComponent                # type: ignore[assignment,misc]


# --------------------------------------------------------------------------- #
# Enums
# --------------------------------------------------------------------------- #
class ExecutionStrategy(str, Enum):
    """Execution strategies the distillation optimizer can select."""

    NATIVE = "native"
    FLEXGEN_LOW_PRECISION = "flexgen_low_precision"
    FLEXGEN_HIGH_PRECISION = "flexgen_high_precision"

    @classmethod
    def from_action(cls, action_idx: int) -> "ExecutionStrategy":
        if action_idx == 0:
            return cls.NATIVE
        if action_idx == 1:
            return cls.FLEXGEN_LOW_PRECISION
        if action_idx == 2:
            return cls.FLEXGEN_HIGH_PRECISION
        raise AdapterError(
            f"unknown action index {action_idx!r}; expected 0, 1, or 2."
        )


# --------------------------------------------------------------------------- #
# Validation helpers
# --------------------------------------------------------------------------- #
def _clamp01(name: str, value: Any) -> float:
    """Clamp ``value`` to ``[0, 1]``; reject non-numeric / non-finite."""
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise AdapterError(
            f"{name} must be numeric, got {type(value).__name__}."
        )
    fv = float(value)
    if not math.isfinite(fv):
        raise AdapterError(f"{name} must be finite, got {value!r}.")
    return max(0.0, min(fv, 1.0))


def _nonneg(name: str, value: Any) -> float:
    """Reject non-numeric / non-finite / negative values."""
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise AdapterError(
            f"{name} must be numeric, got {type(value).__name__}."
        )
    fv = float(value)
    if not math.isfinite(fv):
        raise AdapterError(f"{name} must be finite, got {value!r}.")
    if fv < 0.0:
        raise AdapterError(f"{name} must be >= 0, got {fv}.")
    return fv


# --------------------------------------------------------------------------- #
# Normalized features
# --------------------------------------------------------------------------- #
@dataclass(frozen=True, slots=True)
class NormalizedFeatures:
    """
    Named, normalized task features.

    Eliminates the magic indices (``state_vec[3]``, ``state_vec[7]``)
    that previously bound the reward computation to the exact ordering
    of ``_build_state_vec``. Every field is in ``[0, 1]`` — guaranteed by
    the builder.
    """

    complexity: float
    token_count_norm: float
    latency_target_norm: float
    carbon_intensity_norm: float
    human_feedback_score: float
    graph_centrality: float
    graph_connectivity: float
    cost_budget_norm: float

    def as_vector(self) -> np.ndarray:
        """Return the features as a ``float32`` array of shape ``(8,)``."""
        return np.array([
            self.complexity,
            self.token_count_norm,
            self.latency_target_norm,
            self.carbon_intensity_norm,
            self.human_feedback_score,
            self.graph_centrality,
            self.graph_connectivity,
            self.cost_budget_norm,
        ], dtype=np.float32)


# --------------------------------------------------------------------------- #
# Replay buffer entry
# --------------------------------------------------------------------------- #
@dataclass(frozen=True, slots=True)
class _ReplayEntry:
    """
    One training sample for the distillation optimizer.

    ``teacher_outputs`` is the individual per-teacher distribution
    (shape ``(N_TEACHERS, N_ACTIONS)``). ``teacher_combined`` is the
    gated mixture at decision time (shape ``(N_ACTIONS,)``). Storing both
    lets the gate update recover the correct gradient.
    """

    state_vec: np.ndarray
    action: int
    reward: float
    teacher_outputs: np.ndarray
    teacher_combined: np.ndarray


# --------------------------------------------------------------------------- #
# Distillation optimizer (module-level, canonical implementation)
# --------------------------------------------------------------------------- #
class DistillationOptimizer:
    """
    Multi-teacher distillation with MoE gating.

    Actions: ``0 = native framework``, ``1 = FlexGen low precision``,
    ``2 = FlexGen high precision``.

    ``select_action`` returns ``(action, state_vec, teacher_outputs,
    teacher_combined)``. ``teacher_outputs`` is the individual
    per-teacher distribution (shape ``(N_TEACHERS, N_ACTIONS)``);
    ``teacher_combined`` is the gated mixture at decision time (shape
    ``(N_ACTIONS,)``).
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

        # Per-instance RNGs (seedable, independent of global state).
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
            raise AdapterConfigError(
                f"config must be a dict, got {type(config).__name__}."
            )

        def _num(
            key: str, default: float, *, positive: bool,
        ) -> float:
            raw = config.get(key, default)
            if isinstance(raw, bool) or not isinstance(raw, (int, float)):
                raise AdapterConfigError(
                    f"{key} must be numeric, got {type(raw).__name__}."
                )
            fv = float(raw)
            if not math.isfinite(fv):
                raise AdapterConfigError(f"{key} must be finite.")
            if positive and fv <= 0:
                raise AdapterConfigError(f"{key} must be > 0.")
            if not positive and fv < 0:
                raise AdapterConfigError(f"{key} must be >= 0.")
            return fv

        def _int(key: str, default: int, *, positive: bool) -> int:
            raw = config.get(key, default)
            if isinstance(raw, bool) or not isinstance(raw, int):
                raise AdapterConfigError(
                    f"{key} must be an int, got {type(raw).__name__}."
                )
            if positive and raw <= 0:
                raise AdapterConfigError(f"{key} must be > 0.")
            if not positive and raw < 0:
                raise AdapterConfigError(f"{key} must be >= 0.")
            return raw

        out = dict(config)
        out["distillation_lr"] = _num(
            "distillation_lr", 0.01, positive=True,
        )
        out["gating_lr"] = _num("gating_lr", 0.005, positive=True)
        out["epsilon"] = _num("epsilon", 0.1, positive=False)
        if out["epsilon"] > 1.0:
            raise AdapterConfigError("epsilon must be <= 1.0.")
        out["distill_weight"] = _num(
            "distill_weight", 0.7, positive=False,
        )
        out["rl_weight"] = _num("rl_weight", 0.3, positive=False)
        if out["distill_weight"] + out["rl_weight"] <= 0:
            raise AdapterConfigError(
                "distill_weight + rl_weight must be > 0."
            )
        out["train_every"] = _int("train_every", TRAIN_EVERY, positive=True)
        out["batch_size"] = _int("batch_size", BATCH_SIZE, positive=True)
        out["replay_size"] = _int(
            "replay_size", REPLAY_BUFFER_SIZE, positive=True,
        )
        out["seed"] = config.get("seed")
        out["gate_seed"] = config.get("gate_seed")
        return out

    # ---------------------------------------------------------------- teachers
    def _rule_teacher(self, state_vec: np.ndarray) -> np.ndarray:
        probs = np.ones(self.n_actions) * 0.1
        if state_vec[0] > 0.7:
            probs[0] = 0.8
        elif state_vec[3] > 0.4:
            probs[1] = 0.7
        else:
            probs[2] = 0.5
        return probs / probs.sum()

    def _rlhf_teacher(self, state_vec: np.ndarray) -> np.ndarray:
        probs = np.ones(self.n_actions) / self.n_actions
        if state_vec[4] > 0.7:
            probs[0] += 0.2
        elif state_vec[4] < 0.3:
            probs[1] += 0.2
        return probs / probs.sum()

    def _historical_teacher(self, state_vec: np.ndarray) -> np.ndarray:
        if state_vec[1] > 0.5:
            return np.array([0.1, 0.3, 0.6])
        if state_vec[2] < 0.1:
            return np.array([0.7, 0.1, 0.2])
        return np.array([0.4, 0.3, 0.3])

    # ---------------------------------------------------------------- gating
    def _gate_forward(self, state_vec: np.ndarray) -> np.ndarray:
        logits = state_vec @ self.gate_weights + self.gate_bias
        logits = logits - np.max(logits)
        exp = np.exp(logits)
        return exp / exp.sum()

    # ---------------------------------------------------------------- action
    def select_action(
        self,
        state_vec: np.ndarray,
        exploration: bool = True,
    ) -> Tuple[int, np.ndarray, np.ndarray, np.ndarray]:
        """
        Choose an action and return the decision context.

        Returns
        -------
        (action, state_vec, teacher_outputs, teacher_combined)
            ``teacher_outputs`` has shape ``(N_TEACHERS, N_ACTIONS)``;
            ``teacher_combined`` has shape ``(N_ACTIONS,)``.
        """
        teacher_outputs = np.stack(
            [t(state_vec) for t in self.teachers],
        )
        gate_weights = self._gate_forward(state_vec)
        teacher_combined = (
            gate_weights[:, None] * teacher_outputs
        ).sum(axis=0)
        teacher_combined = teacher_combined / teacher_combined.sum()

        logits = state_vec @ self.student_weights + self.student_bias
        logits = logits - logits.max()
        student_probs = np.exp(logits)
        student_probs = student_probs / student_probs.sum()

        with self._lock:
            if exploration and self._py_rng.random() < self.epsilon:
                action = self._py_rng.randint(0, self.n_actions - 1)
            else:
                combined = 0.8 * student_probs + 0.2 * teacher_combined
                action = int(np.argmax(combined))

        return action, state_vec, teacher_outputs, teacher_combined

    # ---------------------------------------------------------------- update
    def update(
        self,
        state_vec: np.ndarray,
        action: int,
        reward: float,
        teacher_outputs: np.ndarray,
        teacher_combined: np.ndarray,
    ) -> None:
        """
        Record one transition; every ``train_every`` updates, train a
        batch.
        """
        if not math.isfinite(float(reward)):
            raise AdapterError(
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

            # Student update.
            one_hot = np.zeros(self.n_actions)
            one_hot[entry.action] = 1.0
            grad_distill = -(entry.teacher_combined - cur_probs)
            grad_rl = -entry.reward * (one_hot - cur_probs)
            grad = self.distill_w * grad_distill + self.rl_w * grad_rl
            self.student_weights -= self.lr * np.outer(s, grad)
            self.student_bias -= self.lr * grad

            # Gate update: correct shape (n_teachers,).
            target = self._student_probs(s)
            gate = self._gate_forward(s)
            combined = (gate[:, None] * entry.teacher_outputs).sum(axis=0)
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
# Base adapter
# --------------------------------------------------------------------------- #
class BaseAgentAdapter(ABC):
    """
    Abstract base class for agent adapters.

    Provides a unified interface for agents from different frameworks.
    If ``use_enhancements`` is True and the enhanced modules are
    available, the adapter optionally applies decision-making (via
    distillation with MoE) to choose an execution strategy (e.g.,
    delegate to FlexGen or not), compute multi-objective rewards (MODP),
    incorporate RLHF feedback, and emit ``FeedbackEvent`` for
    cross-module learning.

    The ``strict`` flag (keyword-only) controls whether integration and
    execution failures raise (``strict=True``) or log (``strict=False``,
    default — matching legacy behaviour).
    """

    def __init__(
        self,
        agent: Any,
        framework_name: str,
        config: Optional[Dict[str, Any]] = None,
        *,
        strict: bool = False,
    ) -> None:
        """
        Initialize adapter.

        Parameters
        ----------
        agent : Any
            Agent instance from the specific framework.
        framework_name : str
            Name of the framework.
        config : dict, optional
            Configuration dictionary for enhancements.
        strict : bool, default False
            If True, raise on integration or execution failures. If
            False (default), log and continue.
        """
        if not isinstance(framework_name, str) or not framework_name:
            raise AdapterConfigError(
                "framework_name must be a non-empty string."
            )
        if config is not None and not isinstance(config, dict):
            raise AdapterConfigError(
                f"config must be a dict or None, got "
                f"{type(config).__name__}."
            )

        self.agent = agent
        self.framework_name = framework_name
        self.config: Dict[str, Any] = dict(config) if config else {}
        self._strict: bool = bool(strict)

        # Deferred — computed by the ``agent_name`` property on first use.
        self._agent_name_cache: Optional[str] = None

        # Enhancement flags.
        self.use_enhancements: bool = (
            bool(self.config.get("use_enhancements", False))
            and ENHANCEMENTS_AVAILABLE
        )
        self.track_green_metrics: bool = bool(
            self.config.get("track_green_metrics", True)
        )

        # Enhanced components (populated by _initialize_enhancements).
        self.workload_descriptor: Any = None
        self.node_descriptor: Any = None
        self.zero_trust: Any = None
        self.distillation_optimizer: Optional[DistillationOptimizer] = None

        if self.use_enhancements:
            self._initialize_enhancements()

        logger.info(
            "Initialized %s adapter (enhancements=%s)",
            framework_name,
            "on" if self.use_enhancements else "off",
        )

    # ---------------------------------------------------------------- props
    @property
    def agent_name(self) -> str:
        """
        Framework-specific agent name, computed lazily.

        Subclasses may implement ``_get_agent_name()`` referencing
        attributes set **after** ``super().__init__(...)`` returns;
        those attributes will be available by the time the property is
        first accessed.
        """
        if self._agent_name_cache is None:
            name = self._get_agent_name()
            if not isinstance(name, str) or not name:
                raise AdapterError(
                    f"{type(self).__name__}._get_agent_name must return "
                    f"a non-empty string, got {name!r}."
                )
            self._agent_name_cache = name
        return self._agent_name_cache

    # ---------------------------------------------------------------- init
    def _initialize_enhancements(self) -> None:
        """
        Initialize optional enhanced components.

        In ``strict`` mode, a failure re-raises; otherwise logs and
        rolls back all components.
        """
        try:
            self.distillation_optimizer = self._create_distillation_optimizer()

            if (
                self.config.get("use_workload_descriptor", False)
                and WorkloadDescriptor is not None
            ):
                self.workload_descriptor = WorkloadDescriptor(
                    task_id=self.config.get("task_id", "default_task"),
                    task_type=TaskType.INFERENCE,
                    tokens=self.config.get("default_tokens", 1000),
                    latency_target=self.config.get("default_latency", 500.0),
                    urgency=Urgency.MEDIUM,
                    estimated_energy_joules=0.01,
                    estimated_carbon_kg=0.0001,
                    user_id=self.agent_name,
                    metadata={"adapter": self.framework_name},
                )

            if (
                self.config.get("use_node_descriptor", False)
                and NodeDescriptor is not None
            ):
                self.node_descriptor = NodeDescriptor(
                    id=f"{self.agent_name}_node",
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

            logger.info("Enhanced components initialized")
        except Exception:
            logger.exception("Failed to initialize enhancements")
            # Full rollback — no half-initialized state.
            self.use_enhancements = False
            self.distillation_optimizer = None
            self.workload_descriptor = None
            self.node_descriptor = None
            self.zero_trust = None
            if self._strict:
                raise

    def _create_distillation_optimizer(self) -> DistillationOptimizer:
        """
        Create a distillation optimizer.

        Override to swap implementations. The default returns the
        module-level :class:`DistillationOptimizer`, which is fully
        validated and thread-safe.
        """
        return DistillationOptimizer(self.config)

    # ---------------------------------------------------------------- features
    def _build_features(
        self, task_input: Dict[str, Any]
    ) -> NormalizedFeatures:
        """Build normalized, named features from ``task_input``."""
        if not isinstance(task_input, dict):
            raise AdapterError(
                f"task_input must be a dict, got "
                f"{type(task_input).__name__}."
            )
        complexity = float(task_input.get("complexity", 0.5))
        token_count = float(task_input.get("token_count", 1000))
        latency_target = float(task_input.get("latency_target", 500.0))
        carbon_intensity = float(
            task_input.get(
                "carbon_intensity",
                self.node_descriptor.region_carbon_intensity
                if self.node_descriptor is not None
                else 400.0,
            )
        )
        human_feedback = float(
            task_input.get("human_feedback_score", 0.5)
        )
        gm = task_input.get("graph_metrics", {}) or {}
        centrality = float(gm.get("centrality", 0.5))
        connectivity = float(gm.get("connectivity", 0.5))
        cost_budget = float(task_input.get("cost_budget", 10.0))

        return NormalizedFeatures(
            complexity=_clamp01("complexity", complexity),
            token_count_norm=_clamp01(
                "token_count_norm", token_count / MAX_TOKENS,
            ),
            latency_target_norm=_clamp01(
                "latency_target_norm", latency_target / MAX_LATENCY_MS,
            ),
            carbon_intensity_norm=_clamp01(
                "carbon_intensity_norm",
                carbon_intensity / MAX_CARBON_INTENSITY,
            ),
            human_feedback_score=_clamp01(
                "human_feedback_score", human_feedback,
            ),
            graph_centrality=_clamp01("graph_centrality", centrality),
            graph_connectivity=_clamp01(
                "graph_connectivity", connectivity,
            ),
            cost_budget_norm=_clamp01(
                "cost_budget_norm", cost_budget / MAX_COST_BUDGET,
            ),
        )

    def _build_state_vec(self, task_input: Dict[str, Any]) -> np.ndarray:
        """Backward-compatible wrapper around :meth:`_build_features`."""
        return self._build_features(task_input).as_vector()

    def _estimate_energy_carbon(
        self, execution_time_s: float, token_count: int,
    ) -> Tuple[float, float]:
        """
        **Mock** energy and carbon estimation.

        Assumes a 250 W power draw and a 400 gCO₂/kWh grid intensity;
        scales linearly with token count beyond 10 000. Returns
        ``(energy_kwh, carbon_kg)``.

        .. warning::
            Replace this with measured telemetry or a calibrated model
            for production use.
        """
        energy_kwh = (execution_time_s / 3600.0) * 0.25
        carbon_kg = energy_kwh * 0.4
        token_scale = 1.0 + max(token_count, 0) / 10_000.0
        return energy_kwh * token_scale, carbon_kg * token_scale

    # ---------------------------------------------------------------- async
    @staticmethod
    def _try_async(method: Any, **kwargs: Any) -> None:
        """
        Call ``method`` — sync, schedule on the running loop, or via
        ``asyncio.run``.

        Never raises unless the underlying call does; every failure path
        is ``logger.exception``-ed. This is the shared helper for
        ``record_outcome`` and ``zero_trust.shutdown``.
        """
        if not callable(method):
            return
        try:
            result = method(**kwargs)
        except TypeError:
            # Might be a coroutine function.
            try:
                coro = method(**kwargs)
            except Exception:
                logger.exception("async bridge: cannot call coroutine")
                return
            if asyncio.iscoroutine(coro):
                BaseAgentAdapter._run_coroutine(coro)
            return
        except Exception:
            logger.exception("async bridge: sync call failed")
            return
        if asyncio.iscoroutine(result):
            BaseAgentAdapter._run_coroutine(result)

    @staticmethod
    def _run_coroutine(coro: Any) -> None:
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            try:
                asyncio.run(coro)
            except Exception:
                logger.exception("asyncio.run(...) failed")
        else:
            try:
                loop.create_task(coro)
            except Exception:
                logger.exception("loop.create_task(...) failed")

    # ---------------------------------------------------------------- reward
    def _compute_modp_reward(
        self,
        features: NormalizedFeatures,
        *,
        success: bool,
        energy_kwh: float,
        latency_ms: float,
    ) -> float:
        """
        Compute the MODP reward with success gating.

        The reward is ``modp_score * success_gate`` where the gate is
        ``1.0`` on success and ``failure_reward_multiplier`` (default
        ``0.0``) on failure. This prevents the optimizer from learning
        to "fail fast and cheap".
        """
        carbon_norm = 1.0 - features.carbon_intensity_norm
        energy_norm = 1.0 - _clamp01(
            "energy_norm", energy_kwh / MAX_ENERGY_KWH,
        )
        latency_norm = 1.0 - _clamp01(
            "latency_norm", latency_ms / MAX_LATENCY_MS,
        )
        cost_norm = 1.0 - features.cost_budget_norm

        raw_weights = self.config.get(
            "modp_weights", list(DEFAULT_MODP_WEIGHTS),
        )
        mw = np.asarray(raw_weights, dtype=float)
        if (
            mw.shape != (4,)
            or not np.all(np.isfinite(mw))
            or mw.min() < 0.0
            or mw.sum() <= 0.0
        ):
            raise AdapterConfigError(
                f"modp_weights must be 4 finite non-negative values "
                f"with a positive sum, got {raw_weights!r}."
            )
        mw = mw / mw.sum()

        modp_score = float(np.dot(
            [carbon_norm, energy_norm, latency_norm, cost_norm], mw,
        ))
        failure_multiplier = float(
            self.config.get("failure_reward_multiplier", 0.0)
        )
        return modp_score if success else modp_score * failure_multiplier

    # ---------------------------------------------------------------- pipeline
    def run_with_enhancements(
        self,
        task_input: Dict[str, Any],
        *,
        track: Optional[bool] = None,
    ) -> Dict[str, Any]:
        """
        Run the agent using the enhanced decision pipeline.

        Steps:
            1. Build normalized features.
            2. Select an execution strategy via the distillation
               optimizer.
            3. Execute the agent (via :meth:`_execute`).
            4. Compute MODP reward and update models.
            5. Emit ``FeedbackEvent`` if configured.

        Parameters
        ----------
        task_input : dict
        track : bool, optional
            Per-call override for green-metric tracking. When ``None``,
            ``self.track_green_metrics`` is used.
        """
        if not isinstance(task_input, dict):
            raise AdapterError(
                f"task_input must be a dict, got "
                f"{type(task_input).__name__}."
            )
        if self.distillation_optimizer is None:
            raise AdapterError(
                "run_with_enhancements called with no distillation "
                "optimizer; enhancements failed to initialise."
            )
        track_flag = (
            self.track_green_metrics if track is None else bool(track)
        )

        start_time = time.perf_counter()

        # 1. Features.
        features = self._build_features(task_input)
        state_vec = features.as_vector()

        # 2. Action.
        action_idx, _, teacher_outputs, teacher_combined = (
            self.distillation_optimizer.select_action(
                state_vec, exploration=True,
            )
        )
        strategy = ExecutionStrategy.from_action(action_idx)

        # 3. Execution.
        success = False
        result: Dict[str, Any] = {}
        try:
            result = self._execute(task_input, strategy=strategy.value)
            if not isinstance(result, dict):
                raise AdapterError(
                    f"{type(self).__name__}._execute must return a "
                    f"dict, got {type(result).__name__}."
                )
            success = True
        except Exception:
            logger.exception("agent execution failed")
            if self._strict:
                raise

        execution_time_s = time.perf_counter() - start_time
        latency_ms = execution_time_s * 1000.0
        token_count = int(task_input.get("token_count", 1000))
        energy_kwh, carbon_kg = self._estimate_energy_carbon(
            execution_time_s, token_count,
        )

        # 4. Output.
        output: Dict[str, Any] = {
            "latency": execution_time_s,
            "latency_ms": latency_ms,
            "execution_strategy": strategy.value,
            "success": success,
            **result,
        }
        if track_flag:
            output["energy_kwh"] = energy_kwh
            output["carbon_kg"] = carbon_kg
            output["graph_metrics"] = dict(
                task_input.get("graph_metrics", {}) or {},
            )
            output["human_feedback_score"] = float(
                task_input.get("human_feedback_score", 0.5)
            )

        # 5. Reward + learning.
        reward = self._compute_modp_reward(
            features,
            success=success,
            energy_kwh=energy_kwh,
            latency_ms=latency_ms,
        )
        try:
            self.distillation_optimizer.update(
                state_vec=state_vec,
                action=action_idx,
                reward=reward,
                teacher_outputs=teacher_outputs,
                teacher_combined=teacher_combined,
            )
        except Exception:
            logger.exception("distillation_optimizer.update failed")
            if self._strict:
                raise

        if self.workload_descriptor is not None:
            self._try_async(
                self.workload_descriptor.record_outcome,
                latency_achieved_ms=latency_ms,
                carbon_saved_kg=max(0.0, 0.01 - carbon_kg),
                energy_used_joules=energy_kwh * 3_600_000.0,
            )

        if self.zero_trust is not None and ENHANCEMENTS_AVAILABLE:
            self._emit_feedback(
                success=success,
                latency_ms=latency_ms,
                energy_kwh=energy_kwh,
                carbon_kg=carbon_kg,
                strategy=strategy,
                reward=reward,
                task_input=task_input,
            )

        output["reward"] = reward
        return output

    def _emit_feedback(
        self,
        *,
        success: bool,
        latency_ms: float,
        energy_kwh: float,
        carbon_kg: float,
        strategy: ExecutionStrategy,
        reward: float,
        task_input: Dict[str, Any],
    ) -> None:
        try:
            event = FeedbackEvent(
                source=f"{self.framework_name}_adapter",
                feedback_type="routing",
                task_id=task_input.get("task_id", "default_task"),
                context={
                    "strategy": strategy.value,
                    "success": success,
                },
                action={
                    "selected_action": strategy.value,
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
                tags=[self.framework_name, strategy.value],
            )
            logger.debug("FeedbackEvent created: %s", event.event_id)
        except Exception:
            logger.exception("FeedbackEvent construction failed")
            if self._strict:
                raise

    # ---------------------------------------------------------------- run
    def run(
        self,
        task_input: Dict[str, Any],
        track_green_metrics: Optional[bool] = None,
    ) -> Dict[str, Any]:
        """
        Run the agent on ``task_input``.

        If enhancements are enabled, dispatches to
        :meth:`run_with_enhancements`. Otherwise, runs the legacy path.
        The per-call ``track_green_metrics`` override does not persist
        between calls.
        """
        track = (
            self.track_green_metrics
            if track_green_metrics is None
            else bool(track_green_metrics)
        )
        if self.use_enhancements:
            return self.run_with_enhancements(task_input, track=track)
        return self._run_legacy(task_input, track=track)

    def _run_legacy(
        self, task_input: Dict[str, Any], *, track: bool,
    ) -> Dict[str, Any]:
        if not isinstance(task_input, dict):
            raise AdapterError(
                f"task_input must be a dict, got "
                f"{type(task_input).__name__}."
            )
        start = time.perf_counter()
        result = self._execute(task_input, strategy="native")
        if not isinstance(result, dict):
            raise AdapterError(
                f"{type(self).__name__}._execute must return a dict, "
                f"got {type(result).__name__}."
            )
        latency_s = time.perf_counter() - start
        output: Dict[str, Any] = {
            **result,
            "latency": latency_s,
            "latency_ms": latency_s * 1000.0,
            "success": True,
        }
        if track:
            token_count = int(task_input.get("token_count", 1000))
            energy_kwh, carbon_kg = self._estimate_energy_carbon(
                latency_s, token_count,
            )
            output["energy_kwh"] = energy_kwh
            output["carbon_kg"] = carbon_kg
        return output

    # ---------------------------------------------------------------- metadata
    def get_metadata(self) -> Dict[str, Any]:
        """Return a snapshot of adapter metadata."""
        mw = self.config.get("modp_weights")
        meta: Dict[str, Any] = {
            "agent_name": self.agent_name,
            "framework": self.framework_name,
            "adapter_version": __version__,
            "enhancements_enabled": self.use_enhancements,
        }
        if self.use_enhancements:
            meta["modp_weights"] = (
                list(mw) if mw is not None else None
            )
            meta["use_zero_trust"] = self.zero_trust is not None
            meta["use_workload_descriptor"] = (
                self.workload_descriptor is not None
            )
            meta["use_node_descriptor"] = (
                self.node_descriptor is not None
            )
        return meta

    # ---------------------------------------------------------------- abstract
    @abstractmethod
    def _execute(
        self,
        task_input: Dict[str, Any],
        strategy: str = "native",
    ) -> Dict[str, Any]:
        """
        Execute the underlying agent with the given strategy.

        Subclasses must implement this method. The returned dict may
        contain keys like ``accuracy`` / ``output`` and will be merged
        into the final output. **The return value must be a dict** — a
        ``None`` return raises :class:`AdapterError` at the call site.
        """

    @abstractmethod
    def _get_agent_name(self) -> str:
        """Return the agent name from the framework-specific agent."""


# --------------------------------------------------------------------------- #
# Public API
# --------------------------------------------------------------------------- #
__all__: List[str] = [
    "__version__",
    "AdapterConfigError",
    "AdapterError",
    "BaseAgentAdapter",
    "DistillationOptimizer",
    "ExecutionStrategy",
    "NormalizedFeatures",
]


def __dir__() -> List[str]:
    return sorted(__all__)


# --------------------------------------------------------------------------- #
# Smoke test: python -m integrations.base_adapter
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

    # ---- 1. NormalizedFeatures ------------------------------------- #
    print("NormalizedFeatures:")
    f = NormalizedFeatures(
        complexity=0.5,
        token_count_norm=0.1,
        latency_target_norm=0.5,
        carbon_intensity_norm=0.8,
        human_feedback_score=0.5,
        graph_centrality=0.5,
        graph_connectivity=0.5,
        cost_budget_norm=0.1,
    )
    _check("feature vector shape", f.as_vector().shape == (FEATURE_DIM,))
    _check(
        "feature vector in [0,1]",
        bool(np.all(f.as_vector() >= 0) and np.all(f.as_vector() <= 1)),
    )

    # ---- 2. Optimizer config validation ---------------------------- #
    print("optimizer config:")
    opt = DistillationOptimizer({"epsilon": 0.2, "seed": 42})
    _check("epsilon stored", opt.epsilon == 0.2)
    _check(
        "rejects epsilon='high'",
        _raises(AdapterConfigError, lambda:
                DistillationOptimizer({"epsilon": "high"})),
    )
    _check(
        "rejects negative lr",
        _raises(AdapterConfigError, lambda:
                DistillationOptimizer({"distillation_lr": -1})),
    )
    _check(
        "config=None accepted",
        DistillationOptimizer() is not None,
    )

    # ---- 3. select_action / update shapes -------------------------- #
    print("select_action shapes:")
    x = f.as_vector()
    action, x_out, t_out, t_comb = opt.select_action(x)
    _check("action in range", 0 <= action < N_ACTIONS)
    _check("state_vec preserved", np.array_equal(x, x_out))
    _check(
        "teacher_outputs shape",
        t_out.shape == (N_TEACHERS, N_ACTIONS),
    )
    _check("teacher_combined shape", t_comb.shape == (N_ACTIONS,))

    opt.update(x, action, 0.5, t_out, t_comb)
    for _ in range(opt.train_every * opt.batch_size):
        a, xv, to, tc = opt.select_action(x)
        opt.update(xv, a, 0.5, to, tc)
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
        _raises(AdapterError, lambda:
                opt.update(x, 0, float("nan"), t_out, t_comb)),
    )

    # ---- 4. ExecutionStrategy -------------------------------------- #
    print("ExecutionStrategy:")
    _check(
        "from_action(0) == NATIVE",
        ExecutionStrategy.from_action(0) is ExecutionStrategy.NATIVE,
    )
    _check(
        "from_action(1) == FLEXGEN_LOW",
        ExecutionStrategy.from_action(1)
        is ExecutionStrategy.FLEXGEN_LOW_PRECISION,
    )
    _check(
        "from_action(99) raises",
        _raises(AdapterError, lambda:
                ExecutionStrategy.from_action(99)),
    )

    # ---- 5. Concrete adapter --------------------------------------- #
    print("concrete adapter:")

    class _FakeAdapter(BaseAgentAdapter):
        def __init__(self, config=None, *, strict=False):
            self._label = "fake"  # set BEFORE super().__init__ for safety
            super().__init__(
                agent=object(),
                framework_name="fake",
                config=config,
                strict=strict,
            )

        def _get_agent_name(self) -> str:
            return self._label

        def _execute(self, task_input, strategy="native"):
            return {"accuracy": 0.9, "strategy_used": strategy}

    # ---- 5a. agent_name deferred ---------------------------------- #
    class _DeferredNameAdapter(BaseAgentAdapter):
        def __init__(self):
            self._label = None  # set AFTER super().__init__
            super().__init__(agent=object(), framework_name="deferred")

        def _get_agent_name(self) -> str:
            if self._label is None:
                self._label = "deferred"
            return self._label

        def _execute(self, task_input, strategy="native"):
            return {"accuracy": 1.0}

    d = _DeferredNameAdapter()
    _check("deferred agent_name works", d.agent_name == "deferred")

    # ---- 5b. legacy path ----------------------------------------- #
    a = _FakeAdapter()
    out = a.run({"question": "hi", "token_count": 100})
    _check("legacy success=True", out["success"] is True)
    _check("legacy accuracy", out["accuracy"] == 0.9)
    _check("legacy latency present", "latency_ms" in out)
    _check(
        "legacy energy present by default",
        "energy_kwh" in out,
    )

    # per-call track override
    a2 = _FakeAdapter()
    out_no = a2.run({"question": "hi"}, track_green_metrics=False)
    _check(
        "per-call track=False omits energy",
        "energy_kwh" not in out_no,
    )
    out_yes = a2.run({"question": "hi"})    # previous override must NOT persist
    _check(
        "per-call override does not persist",
        "energy_kwh" in out_yes,
    )

    # ---- 5c. _execute returning None ----------------------------- #
    class _BadAdapter(BaseAgentAdapter):
        def _get_agent_name(self):
            return "bad"

        def _execute(self, task_input, strategy="native"):
            return None  # type: ignore[return-value]

    bad = _BadAdapter(agent=object(), framework_name="bad")
    _check(
        "_execute returning None raises AdapterError",
        _raises(AdapterError, lambda:
                bad.run({"question": "hi"})),
    )

    # ---- 5d. config validation ----------------------------------- #
    _check(
        "rejects non-dict config",
        _raises(AdapterConfigError, lambda: _FakeAdapter("not a dict")),
    )
    _check(
        "rejects empty framework_name",
        _raises(AdapterConfigError, lambda:
                BaseAgentAdapter(object(), "")),
    )

    # ---- 6. get_metadata ------------------------------------------- #
    print("get_metadata:")
    a = _FakeAdapter({"modp_weights": [0.5, 0.3, 0.15, 0.05]})
    meta = a.get_metadata()
    _check(
        "adapter_version == __version__",
        meta["adapter_version"] == __version__,
    )
    _check(
        "modp_weights copied",
        meta["modp_weights"] == [0.5, 0.3, 0.15, 0.05]
        and meta["modp_weights"] is not a.config["modp_weights"],
    )
    # Mutating the copy must not affect the config.
    meta["modp_weights"].append(0.0)
    _check(
        "mutating metadata does not affect config",
        len(a.config["modp_weights"]) == 4,
    )

    # ---- 7. modp_weights validation -------------------------------- #
    print("modp_weights validation:")
    a_bad = _FakeAdapter(
        {"use_enhancements": True, "modp_weights": [0, 0, 0, 0]},
    )
    # Force the optimizer to exist for the pipeline path.
    a_bad.distillation_optimizer = DistillationOptimizer({})
    a_bad.use_enhancements = True
    _check(
        "zero modp_weights raises",
        _raises(AdapterConfigError, lambda:
                a_bad.run({"question": "hi"})),
    )

    # ---- 8. __all__ / __dir__ ------------------------------------- #
    print("dir:")
    _check("__dir__ matches __all__", __dir__() == sorted(__all__))
    for _name in (
        "BaseAgentAdapter", "DistillationOptimizer",
        "ExecutionStrategy", "NormalizedFeatures",
        "AdapterError", "AdapterConfigError",
    ):
        _check(f"{_name} exported", _name in __all__)

    print()
    print(f"passed: {passed}  failed: {failed}")
    if failed:
        sys.exit(1)
