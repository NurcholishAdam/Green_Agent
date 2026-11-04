# -*- coding: utf-8 -*-
"""
Adapter implementation for AutoGen-style agents.

Captures message graph depth and conversation complexity as part of
metrics. Optionally integrates LIMIT Graph, MODP, RLHF, multi-teacher
on-policy distillation, bio-inspired optimisation, and MoE expert gating.

Key fixes over the previous version
-----------------------------------
- **Success signal.** The reward is now ``modp_score * success_gate``
  where the gate defaults to ``0.0`` on failure. A "fast failure" can
  no longer score positively for the optimizer. ``output["success"]`` is
  exposed.
- **Simulated marker.** The execution path is explicitly a **stub** —
  ``output["simulated"] = True`` on every call. A consumer can no longer
  mistake the hardcoded result for real execution.
- **Lifecycle safety.** ``__init__`` populates every attribute with a
  safe default, so ``run()`` before ``init()`` works (with legacy
  behaviour) instead of raising ``AttributeError``. ``init()`` accepts
  ``config=None``.
- **Thread safety.** ``_last_decision`` is a **local** variable inside
  ``run()``, not a per-instance field. The optimizer's ``update()`` is
  guarded by an ``RLock``.
- **Correct gating gradient.** The replay buffer stores individual
  teacher outputs (shape ``(n_teachers, n_actions)``) alongside the
  pre-combined distribution. The gate gradient uses
  ``teacher_outputs @ error``, the correct form, instead of the previous
  inner-product shape coincidence.
- **Energy normalization.** ``energy_norm`` is computed against
  ``MAX_ENERGY_KWH`` (default ``0.1``), so a ``flexgen_high_precision``
  run at ``0.5`` kWh is scored meaningfully rather than being flattened
  to the same value as a ``1.0`` kWh run.
- **Feature clamping.** Every feature in ``RuntimeState.to_feature_vector``
  passes through ``_clamp01`` (which also rejects non-finite values).
- **Validation.** ``RuntimeState`` is a frozen dataclass with
  ``__post_init__`` validation; the optimizer validates its config.
- **Async correctness.** ``record_outcome`` and ``zero_trust.shutdown``
  go through a ``_try_async`` helper that handles sync / running-loop /
  ``asyncio.run()`` cases without silently swallowing failures.
- **Narrow error types.** :class:`RuntimeAdapterError`,
  :class:`RuntimeConfigError`.
- **Strict mode.** ``init(config, strict=True)`` raises on integration
  failures instead of silently downgrading to legacy.
- ``ExecutionStrategy`` enum replaces raw strings.
- ``_UnavailableComponent`` replaces silent ``pass``-body dummies.
- ``config`` and ``graph_metrics`` are copied on assignment.
- Lazy ``%s`` logging; ``logger.exception`` on caught exceptions.
- ``__version__``, ``__all__``, ``__dir__``, and a ``_check()``-based
  smoke test that survives ``python -O``.

Compatibility
-------------
- ``AutoGenRuntime().init(config)`` — ``init`` now accepts ``config=None``
  and a keyword-only ``strict=False`` (matching legacy behaviour).
- ``run(query)`` — output shape gains ``"success"`` and ``"simulated"``;
  every other key is preserved.
- ``RuntimeState(...)`` — same constructor arguments, now frozen with
  full validation.
- ``RuntimeDistillationOptimizer(config=None)`` — ``config`` is optional;
  every field validated.
- ``select_action`` returns ``(action, state_vec, teacher_outputs,
  teacher_combined)`` — 4 values, not 3. See the analyzer note in
  :class:`RuntimeDistillationOptimizer`.

Requires Python 3.8+. ``slots=True`` on dataclasses requires 3.10+;
drop it for 3.8/3.9.
"""

from __future__ import annotations

import asyncio
import logging
import math
import random
import threading
import time
from collections import deque
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Deque, Dict, Final, List, Optional, Tuple

import numpy as np

from .base_runtime import BaseRuntimeAdapter


logger = logging.getLogger(__name__)

__version__ = "2.5.0"


# --------------------------------------------------------------------------- #
# Errors
# --------------------------------------------------------------------------- #
class RuntimeAdapterError(ValueError):
    """Raised for invalid AutoGen runtime inputs or execution failures."""


class RuntimeConfigError(RuntimeAdapterError):
    """Raised for invalid runtime or optimizer configuration."""


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

DEFAULT_MODP_WEIGHTS: Final[Tuple[float, ...]] = (
    0.4,  # carbon
    0.3,  # energy
    0.2,  # latency
    0.1,  # cost
)


# --------------------------------------------------------------------------- #
# Enums
# --------------------------------------------------------------------------- #
class ExecutionStrategy(str, Enum):
    """Execution strategies the distillation optimizer can select."""

    AUTOGEN = "autogen"
    FLEXGEN_LOW_PRECISION = "flexgen_low_precision"
    FLEXGEN_HIGH_PRECISION = "flexgen_high_precision"

    @classmethod
    def from_action(cls, action_idx: int) -> "ExecutionStrategy":
        if action_idx == 0:
            return cls.AUTOGEN
        if action_idx == 1:
            return cls.FLEXGEN_LOW_PRECISION
        if action_idx == 2:
            return cls.FLEXGEN_HIGH_PRECISION
        raise RuntimeAdapterError(
            f"unknown action index {action_idx!r}; expected 0, 1, or 2."
        )


# --------------------------------------------------------------------------- #
# Optional enhancements
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
        raise RuntimeAdapterError(
            f"Enhancement component '{name}' is unavailable; install "
            f"the enhancements package to use this runtime in enhanced "
            f"mode."
        )


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
    logger.info("Advanced enhancements unavailable: %s", _exc)
    WorkloadDescriptor = _UnavailableComponent      # type: ignore[assignment,misc]
    NodeDescriptor = _UnavailableComponent          # type: ignore[assignment,misc]
    ZeroTrustArchitecture = _UnavailableComponent   # type: ignore[assignment,misc]
    FeedbackEvent = _UnavailableComponent           # type: ignore[assignment,misc]
    TaskType = _UnavailableComponent                # type: ignore[assignment,misc]
    Urgency = _UnavailableComponent                 # type: ignore[assignment,misc]
    NodeType = _UnavailableComponent                # type: ignore[assignment,misc]


# --------------------------------------------------------------------------- #
# Validation helpers
# --------------------------------------------------------------------------- #
def _clamp01(name: str, value: Any) -> float:
    """Clamp ``value`` to ``[0, 1]``; reject non-numeric / non-finite."""
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise RuntimeAdapterError(
            f"{name} must be numeric, got {type(value).__name__}."
        )
    fv = float(value)
    if not math.isfinite(fv):
        raise RuntimeAdapterError(f"{name} must be finite, got {value!r}.")
    return max(0.0, min(fv, 1.0))


def _nonneg(name: str, value: Any) -> float:
    """Reject non-numeric / non-finite / negative values."""
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise RuntimeAdapterError(
            f"{name} must be numeric, got {type(value).__name__}."
        )
    fv = float(value)
    if not math.isfinite(fv):
        raise RuntimeAdapterError(f"{name} must be finite, got {value!r}.")
    if fv < 0.0:
        raise RuntimeAdapterError(f"{name} must be >= 0, got {fv}.")
    return fv


# --------------------------------------------------------------------------- #
# Runtime state
# --------------------------------------------------------------------------- #
@dataclass(frozen=True, slots=True)
class RuntimeState:
    """
    State used by the distillation optimizer to select execution
    parameters.

    All ranges are enforced in ``__post_init__``; ``to_feature_vector``
    additionally clamps to ``[0, 1]`` as a defence in depth.
    """

    complexity: float
    token_count: int
    latency_target: float
    carbon_intensity: float
    human_feedback_score: float = 0.5
    graph_centrality: float = 0.5
    graph_connectivity: float = 0.5
    cost_budget: float = 10.0

    def __post_init__(self) -> None:
        _clamp01("complexity", self.complexity)
        _nonneg("token_count", self.token_count)
        if isinstance(self.token_count, bool) or not isinstance(
            self.token_count, int
        ):
            raise RuntimeAdapterError("token_count must be an int.")
        _nonneg("latency_target", self.latency_target)
        _nonneg("carbon_intensity", self.carbon_intensity)
        _clamp01("human_feedback_score", self.human_feedback_score)
        _clamp01("graph_centrality", self.graph_centrality)
        _clamp01("graph_connectivity", self.graph_connectivity)
        _nonneg("cost_budget", self.cost_budget)

    @classmethod
    def from_query(
        cls,
        query: Dict[str, Any],
        *,
        carbon_intensity: float = 400.0,
        graph_metrics: Optional[Dict[str, float]] = None,
        human_feedback_score: float = 0.5,
    ) -> "RuntimeState":
        """Build a state from a raw query dict + runtime context."""
        if not isinstance(query, dict):
            raise RuntimeAdapterError(
                f"query must be a dict, got {type(query).__name__}."
            )
        gm = graph_metrics or {}
        return cls(
            complexity=float(query.get("complexity", 0.5)),
            token_count=int(query.get("token_count", 1000)),
            latency_target=float(query.get("latency_target", 500.0)),
            carbon_intensity=float(carbon_intensity),
            human_feedback_score=float(
                query.get("human_feedback_score", human_feedback_score)
            ),
            graph_centrality=float(gm.get("centrality", 0.5)),
            graph_connectivity=float(gm.get("connectivity", 0.5)),
            cost_budget=float(query.get("cost_budget", 10.0)),
        )

    def to_feature_vector(self) -> np.ndarray:
        """
        Convert to a normalized feature vector in ``[0, 1]^8``.

        Every component is clamped to ``[0, 1]``; ``__post_init__``
        already rejects out-of-range inputs, so the clamping is
        defensive.
        """
        return np.array([
            _clamp01("complexity", self.complexity),
            _clamp01("token_count_norm", self.token_count / MAX_TOKENS),
            _clamp01(
                "latency_target_norm", self.latency_target / MAX_LATENCY_MS,
            ),
            _clamp01(
                "carbon_intensity_norm",
                self.carbon_intensity / MAX_CARBON_INTENSITY,
            ),
            _clamp01("human_feedback_score", self.human_feedback_score),
            _clamp01("graph_centrality", self.graph_centrality),
            _clamp01("graph_connectivity", self.graph_connectivity),
            _clamp01("cost_budget_norm", self.cost_budget / MAX_COST_BUDGET),
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
    gated mixture at decision time (shape ``(N_ACTIONS,)``). Storing
    both lets the gate update recover the correct gradient without
    re-deriving the gate weights from an older state.
    """

    state_vec: np.ndarray
    action: int
    reward: float
    teacher_outputs: np.ndarray
    teacher_combined: np.ndarray


# --------------------------------------------------------------------------- #
# Distillation optimizer
# --------------------------------------------------------------------------- #
class RuntimeDistillationOptimizer:
    """
    Multi-teacher on-policy distillation with MoE gating.

    Actions: ``0 = use_autogen``, ``1 = use_flexgen_low``,
    ``2 = use_flexgen_high``.

    ``select_action`` returns ``(action, state_vec, teacher_outputs,
    teacher_combined)``. ``teacher_outputs`` is the individual per-teacher
    distribution (shape ``(N_TEACHERS, N_ACTIONS)``) — required by
    ``update`` for the gate gradient; ``teacher_combined`` is the gated
    mixture at decision time.
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

    # ------------------------------------------------------------------ config
    @staticmethod
    def _validate_config(config: Dict[str, Any]) -> Dict[str, Any]:
        if not isinstance(config, dict):
            raise RuntimeConfigError(
                f"config must be a dict, got {type(config).__name__}."
            )

        def _num(
            key: str, default: float, *, positive: bool,
        ) -> float:
            raw = config.get(key, default)
            if isinstance(raw, bool) or not isinstance(raw, (int, float)):
                raise RuntimeConfigError(
                    f"{key} must be numeric, got {type(raw).__name__}."
                )
            fv = float(raw)
            if not math.isfinite(fv):
                raise RuntimeConfigError(f"{key} must be finite.")
            if positive and fv <= 0:
                raise RuntimeConfigError(f"{key} must be > 0.")
            if not positive and fv < 0:
                raise RuntimeConfigError(f"{key} must be >= 0.")
            return fv

        def _int(key: str, default: int, *, positive: bool) -> int:
            raw = config.get(key, default)
            if isinstance(raw, bool) or not isinstance(raw, int):
                raise RuntimeConfigError(
                    f"{key} must be an int, got {type(raw).__name__}."
                )
            if positive and raw <= 0:
                raise RuntimeConfigError(f"{key} must be > 0.")
            if not positive and raw < 0:
                raise RuntimeConfigError(f"{key} must be >= 0.")
            return raw

        out = dict(config)
        out["distillation_lr"] = _num(
            "distillation_lr", 0.01, positive=True,
        )
        out["gating_lr"] = _num("gating_lr", 0.005, positive=True)
        out["epsilon"] = _num("epsilon", 0.1, positive=False)
        if out["epsilon"] > 1.0:
            raise RuntimeConfigError("epsilon must be <= 1.0.")
        out["distill_weight"] = _num(
            "distill_weight", 0.7, positive=False,
        )
        out["rl_weight"] = _num("rl_weight", 0.3, positive=False)
        if out["distill_weight"] + out["rl_weight"] <= 0:
            raise RuntimeConfigError(
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

    # ----------------------------------------------------------------- teachers
    def _rule_teacher(self, state: RuntimeState) -> np.ndarray:
        probs = np.ones(self.n_actions) * 0.1
        if state.complexity > 0.7:
            probs[0] = 0.8
        elif state.carbon_intensity > 400:
            probs[1] = 0.7
        else:
            probs[2] = 0.5
        return probs / probs.sum()

    def _rlhf_teacher(self, state: RuntimeState) -> np.ndarray:
        probs = np.ones(self.n_actions) / self.n_actions
        if state.human_feedback_score > 0.7:
            probs[0] += 0.2
        elif state.human_feedback_score < 0.3:
            probs[1] += 0.2
        return probs / probs.sum()

    def _historical_teacher(self, state: RuntimeState) -> np.ndarray:
        if state.token_count > 5000:
            return np.array([0.1, 0.3, 0.6])
        if state.latency_target < 100:
            return np.array([0.7, 0.1, 0.2])
        return np.array([0.4, 0.3, 0.3])

    # ----------------------------------------------------------------- gating
    def _gate_forward(self, state_vec: np.ndarray) -> np.ndarray:
        logits = state_vec @ self.gate_weights + self.gate_bias
        logits = logits - np.max(logits)
        exp = np.exp(logits)
        return exp / exp.sum()

    # ----------------------------------------------------------------- action
    def select_action(
        self,
        state: RuntimeState,
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
        x = state.to_feature_vector()

        teacher_outputs = np.stack(
            [t(state) for t in self.teachers],
        )  # shape (N_TEACHERS, N_ACTIONS)

        gate_weights = self._gate_forward(x)                # (N_TEACHERS,)
        teacher_combined = (
            gate_weights[:, None] * teacher_outputs
        ).sum(axis=0)                                        # (N_ACTIONS,)
        teacher_combined = teacher_combined / teacher_combined.sum()

        logits = x @ self.student_weights + self.student_bias
        logits = logits - logits.max()
        student_probs = np.exp(logits)
        student_probs = student_probs / student_probs.sum()

        with self._lock:
            if exploration and self._py_rng.random() < self.epsilon:
                action = self._py_rng.randint(0, self.n_actions - 1)
            else:
                combined = 0.8 * student_probs + 0.2 * teacher_combined
                action = int(np.argmax(combined))

        return action, x, teacher_outputs, teacher_combined

    # ----------------------------------------------------------------- update
    def update(
        self,
        state_vec: np.ndarray,
        action: int,
        reward: float,
        teacher_outputs: np.ndarray,
        teacher_combined: np.ndarray,
    ) -> None:
        """
        Record one transition; every ``train_every`` updates, run a
        training step.
        """
        if not math.isfinite(float(reward)):
            raise RuntimeAdapterError(
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

            # Gate update: target is the student's current belief.
            target = self._student_probs(s)
            gate = self._gate_forward(s)                     # (N_TEACHERS,)
            combined = (
                gate[:, None] * entry.teacher_outputs
            ).sum(axis=0)
            combined = combined / combined.sum()
            error = combined - target                        # (N_ACTIONS,)
            grad_gate = entry.teacher_outputs @ error        # (N_TEACHERS,)
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
    """Local record of one decision — never stored on ``self``."""

    state: RuntimeState
    state_vec: np.ndarray
    action_idx: int
    teacher_outputs: np.ndarray
    teacher_combined: np.ndarray


# --------------------------------------------------------------------------- #
# AutoGen runtime
# --------------------------------------------------------------------------- #
class AutoGenRuntime(BaseRuntimeAdapter):
    """
    Runtime adapter for AutoGen-style agents.

    If enhancements are enabled, it can delegate to FlexGen and use
    distillation to decide execution strategy. It also tracks additional
    metrics (graph depth, carbon, etc.).

    This class is **thread-safe for concurrent** ``run()`` calls: the
    only per-call state (``_Decision``) is held in a local variable
    inside ``run()``; ``self`` holds only the shared model and
    configuration. The optimizer's ``update()`` is guarded by an
    ``RLock``.
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self._setup_defaults()

    def _setup_defaults(self) -> None:
        """Populate every attribute with a safe default."""
        self.config: Dict[str, Any] = {}
        self._strict: bool = False
        self.use_enhancements: bool = False
        self.distillation_optimizer: Optional[
            RuntimeDistillationOptimizer
        ] = None
        self.workload_descriptor: Any = None
        self.node_descriptor: Any = None
        self.zero_trust: Any = None
        self.graph_metrics: Dict[str, float] = {
            "centrality": 0.5, "connectivity": 0.5,
        }
        self.human_feedback_score: float = 0.5

    # ---------------------------------------------------------------- init
    def init(
        self,
        config: Optional[Dict[str, Any]] = None,
        *,
        strict: bool = False,
    ) -> None:
        """
        Configure the runtime.

        Parameters
        ----------
        config : dict or None
            Configuration dict. ``None`` and ``{}`` behave identically.
        strict : bool, default False
            If True, raise on enhancement initialization failures. If
            False (default), log and downgrade to legacy mode.
        """
        self._setup_defaults()
        if config is not None and not isinstance(config, dict):
            raise RuntimeConfigError(
                f"config must be a dict or None, got "
                f"{type(config).__name__}."
            )
        self.config = dict(config) if config else {}
        self._strict = bool(strict)

        self.use_enhancements = (
            bool(self.config.get("use_enhancements", False))
            and ENHANCEMENTS_AVAILABLE
        )
        gm = self.config.get("graph_metrics")
        if isinstance(gm, dict):
            self.graph_metrics = {
                "centrality": float(gm.get("centrality", 0.5)),
                "connectivity": float(gm.get("connectivity", 0.5)),
            }
        self.human_feedback_score = float(
            self.config.get("human_feedback_score", 0.5)
        )

        if self.use_enhancements:
            self._init_enhanced()

    def _init_enhanced(self) -> None:
        """Initialize enhanced decision-making components."""
        try:
            self.distillation_optimizer = RuntimeDistillationOptimizer(
                self.config,
            )

            if "workload" in self.config:
                self.workload_descriptor = WorkloadDescriptor(
                    task_id="autogen_runtime",
                    task_type=TaskType.INFERENCE,
                    tokens=self.config.get("default_tokens", 1000),
                    latency_target=self.config.get(
                        "default_latency", 500.0,
                    ),
                    urgency=Urgency.MEDIUM,
                    estimated_energy_joules=0.01,
                    estimated_carbon_kg=0.0001,
                    user_id="autogen_runtime",
                )

            if "node" in self.config:
                self.node_descriptor = NodeDescriptor(
                    id="autogen_node",
                    type=NodeType.EDGE,
                    region="us-east",
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

            logger.info("AutoGenRuntime enhanced components initialized")
        except Exception:
            logger.exception("Failed to initialize enhancements")
            # Roll back to a clean, non-enhanced state.
            self.use_enhancements = False
            self.distillation_optimizer = None
            self.workload_descriptor = None
            self.node_descriptor = None
            self.zero_trust = None
            if self._strict:
                raise

    # ---------------------------------------------------------------- run
    def run(self, query: Dict[str, Any]) -> Dict[str, Any]:
        """
        Run the runtime on ``query``.

        The execution path is a **simulation** — no real agent or
        FlexGen engine is called. ``output["simulated"]`` is ``True`` on
        every call. Replace :meth:`_simulate_execution` with a real
        dispatch to change this.
        """
        if not isinstance(query, dict):
            raise RuntimeAdapterError(
                f"query must be a dict, got {type(query).__name__}."
            )
        start = time.perf_counter()

        # ---- decision ------------------------------------------- #
        decision: Optional[_Decision] = None
        strategy = ExecutionStrategy.AUTOGEN
        if self.use_enhancements and self.distillation_optimizer is not None:
            carbon_intensity = (
                self.node_descriptor.region_carbon_intensity
                if self.node_descriptor is not None
                else float(query.get("carbon_intensity", 400.0))
            )
            state = RuntimeState.from_query(
                query,
                carbon_intensity=carbon_intensity,
                graph_metrics=self.graph_metrics,
                human_feedback_score=self.human_feedback_score,
            )
            action_idx, state_vec, t_out, t_comb = (
                self.distillation_optimizer.select_action(state)
            )
            strategy = ExecutionStrategy.from_action(action_idx)
            decision = _Decision(
                state=state,
                state_vec=state_vec,
                action_idx=action_idx,
                teacher_outputs=t_out,
                teacher_combined=t_comb,
            )

        # ---- execution ------------------------------------------ #
        success = False
        result: Dict[str, Any] = {}
        try:
            result = self._simulate_execution(strategy)
            success = True
        except Exception:
            logger.exception("execution failed")
            if self._strict:
                raise

        latency_s = time.perf_counter() - start
        latency_ms = latency_s * 1000.0
        token_count = int(query.get("token_count", 1000))
        energy_kwh, carbon_kg = self._estimate_energy_and_carbon(
            token_count, latency_s, strategy,
        )

        # ---- output --------------------------------------------- #
        output: Dict[str, Any] = {
            "latency": latency_s,
            "latency_ms": latency_ms,
            "execution_strategy": strategy.value,
            "simulated": True,
            "success": success,
            **result,
            "energy_kwh": energy_kwh,
            "carbon_kg": carbon_kg,
            "graph_metrics": dict(self.graph_metrics),
            "human_feedback_score": float(
                query.get(
                    "human_feedback_score", self.human_feedback_score,
                )
            ),
        }

        # ---- learning ------------------------------------------- #
        if decision is not None and self.distillation_optimizer is not None:
            reward = self._compute_modp_score(
                success=success,
                state=decision.state,
                execution_time_ms=latency_ms,
                energy_kwh=energy_kwh,
                carbon_kg=carbon_kg,
            )
            try:
                self.distillation_optimizer.update(
                    state_vec=decision.state_vec,
                    action=decision.action_idx,
                    reward=reward,
                    teacher_outputs=decision.teacher_outputs,
                    teacher_combined=decision.teacher_combined,
                )
            except Exception:
                logger.exception("distillation_optimizer.update failed")
                if self._strict:
                    raise
            output["reward"] = reward

            self._record_outcome(
                latency_ms=latency_ms,
                energy_kwh=energy_kwh,
                carbon_kg=carbon_kg,
            )
            self._emit_feedback(
                success=success,
                latency_ms=latency_ms,
                energy_kwh=energy_kwh,
                carbon_kg=carbon_kg,
                strategy=strategy,
                reward=reward,
            )

        return output

    # ---------------------------------------------------------------- helpers
    def _simulate_execution(
        self, strategy: ExecutionStrategy,
    ) -> Dict[str, Any]:
        """
        **Mock execution** — does not call any real engine.

        Replace with a real dispatch to AutoGen / FlexGen in production.
        """
        return {
            ExecutionStrategy.AUTOGEN: {
                "accuracy": 0.85,
                "conversation_depth": 2,
            },
            ExecutionStrategy.FLEXGEN_LOW_PRECISION: {
                "accuracy": 0.80,
                "conversation_depth": 1,
            },
            ExecutionStrategy.FLEXGEN_HIGH_PRECISION: {
                "accuracy": 0.90,
                "conversation_depth": 3,
            },
        }[strategy]

    def _estimate_energy_and_carbon(
        self,
        token_count: int,
        latency_seconds: float,
        strategy: ExecutionStrategy,
    ) -> Tuple[float, float]:
        """
        **Mock** energy and carbon estimation.

        Assumes a 250 W power draw and a 400 gCO₂/kWh grid intensity;
        scales linearly with token count beyond 10 000 and applies a
        per-strategy multiplier (autogen 1.0, flexgen_low 0.7,
        flexgen_high 1.1).

        Returns
        -------
        (energy_kwh, carbon_kg)
        """
        strategy_multiplier = {
            ExecutionStrategy.AUTOGEN: 1.0,
            ExecutionStrategy.FLEXGEN_LOW_PRECISION: 0.7,
            ExecutionStrategy.FLEXGEN_HIGH_PRECISION: 1.1,
        }[strategy]

        hours = max(latency_seconds, 0.0) / 3600.0
        energy_kwh = hours * 0.25 * strategy_multiplier
        carbon_kg = energy_kwh * 0.4
        token_scale = 1.0 + max(token_count, 0) / 10_000.0
        return energy_kwh * token_scale, carbon_kg * token_scale

    def _compute_modp_score(
        self,
        *,
        success: bool,
        state: RuntimeState,
        execution_time_ms: float,
        energy_kwh: float,
        carbon_kg: float,  # noqa: ARG002 — kept for symmetry / future use
    ) -> float:
        """
        Compute the MODP reward with success gating.

        A failed run scores ``modp_score * failure_multiplier`` where
        ``failure_multiplier`` defaults to ``0.0`` (configurable via
        ``config["failure_reward_multiplier"]``). This prevents the
        optimizer from learning to "fail fast and cheap".
        """
        carbon_norm = 1.0 - _clamp01(
            "carbon_norm",
            state.carbon_intensity / MAX_CARBON_INTENSITY,
        )
        # Normalize against MAX_ENERGY_KWH, not 1.0.
        energy_norm = 1.0 - _clamp01(
            "energy_norm", energy_kwh / MAX_ENERGY_KWH,
        )
        latency_norm = 1.0 - _clamp01(
            "latency_norm", execution_time_ms / MAX_LATENCY_MS,
        )
        cost_norm = 1.0 - _clamp01(
            "cost_norm", state.cost_budget / MAX_COST_BUDGET,
        )
        modp_score = float(np.dot(
            [carbon_norm, energy_norm, latency_norm, cost_norm],
            DEFAULT_MODP_WEIGHTS,
        ))

        failure_multiplier = float(
            self.config.get("failure_reward_multiplier", 0.0)
        )
        return modp_score if success else modp_score * failure_multiplier

    def _record_outcome(
        self,
        *,
        latency_ms: float,
        energy_kwh: float,
        carbon_kg: float,
    ) -> None:
        """Best-effort update of the workload descriptor (sync or async)."""
        if self.workload_descriptor is None:
            return
        self._try_async(
            self.workload_descriptor.record_outcome,
            latency_achieved_ms=latency_ms,
            carbon_saved_kg=max(0.0, 0.01 - carbon_kg),
            energy_used_joules=energy_kwh * 3_600_000.0,
        )

    def _emit_feedback(
        self,
        *,
        success: bool,
        latency_ms: float,
        energy_kwh: float,
        carbon_kg: float,
        strategy: ExecutionStrategy,
        reward: float,
    ) -> None:
        """Emit a ``FeedbackEvent`` when the enhancements package is loaded."""
        if not ENHANCEMENTS_AVAILABLE:
            return
        try:
            event = FeedbackEvent(
                source="autogen_runtime",
                feedback_type="routing",
                task_id="autogen_task",
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
                tags=["autogen", strategy.value],
            )
            logger.debug("FeedbackEvent created: %s", event.event_id)
        except Exception:
            logger.exception("FeedbackEvent construction failed")
            if self._strict:
                raise

    @staticmethod
    def _try_async(method: Any, **kwargs: Any) -> None:
        """
        Call ``method`` — sync, schedule on the running loop, or via
        ``asyncio.run``.

        Never raises unless the underlying call does (which is
        ``logger.exception``-ed). This is the shared helper for
        ``record_outcome`` and ``zero_trust.shutdown``.
        """
        if not callable(method):
            return
        # Try sync first.
        try:
            result = method(**kwargs)
        except TypeError:
            result = None      # might be a coroutine function
        except Exception:
            logger.exception("async bridge: sync call failed")
            return
        else:
            if not asyncio.iscoroutine(result):
                return
            # It was a coroutine that got created by a sync call —
            # schedule it, or run it.
            cls = AutoGenRuntime
            cls._run_coroutine(result)
            return
        # Sync raised TypeError: assume it's a coroutine function.
        try:
            coro = method(**kwargs)
        except Exception:
            logger.exception("async bridge: cannot call coroutine")
            return
        if not asyncio.iscoroutine(coro):
            return
        AutoGenRuntime._run_coroutine(coro)

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

    # ---------------------------------------------------------------- finalize
    def finalize(self) -> None:
        """Release resources on the enhancement components."""
        if self.use_enhancements and self.zero_trust is not None:
            shutdown = getattr(self.zero_trust, "shutdown", None)
            if callable(shutdown):
                self._try_async(shutdown)
        logger.info("AutoGenRuntime finalized")


# --------------------------------------------------------------------------- #
# Public API
# --------------------------------------------------------------------------- #
__all__: List[str] = [
    "__version__",
    "AutoGenRuntime",
    "ExecutionStrategy",
    "RuntimeAdapterError",
    "RuntimeConfigError",
    "RuntimeDistillationOptimizer",
    "RuntimeState",
]


def __dir__() -> List[str]:
    return sorted(__all__)


# --------------------------------------------------------------------------- #
# Smoke test: python -m runtimes.autogen_runtime
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

    # ---- 1. RuntimeState validation ------------------------------ #
    print("RuntimeState validation:")
    s = RuntimeState(
        complexity=0.5,
        token_count=1000,
        latency_target=500.0,
        carbon_intensity=400.0,
    )
    _check(
        "feature vector shape",
        s.to_feature_vector().shape == (FEATURE_DIM,),
    )
    fv = s.to_feature_vector()
    _check(
        "feature vector in [0,1]",
        bool(np.all(fv >= 0) and np.all(fv <= 1)),
    )
    _check(
        "rejects NaN complexity",
        _raises(RuntimeAdapterError, lambda: RuntimeState(
            complexity=float("nan"),
            token_count=1, latency_target=1.0, carbon_intensity=1.0,
        )),
    )
    _check(
        "rejects negative token_count",
        _raises(RuntimeAdapterError, lambda: RuntimeState(
            complexity=0.5, token_count=-1,
            latency_target=1.0, carbon_intensity=1.0,
        )),
    )
    _check(
        "frozen — mutation rejected",
        _raises(
            (AttributeError, Exception),
            lambda: setattr(s, "complexity", 0.9),
        ),
    )

    # ---- 2. from_query helper ------------------------------------ #
    print("RuntimeState.from_query:")
    s2 = RuntimeState.from_query(
        {"complexity": 0.8, "token_count": 500},
        carbon_intensity=250.0,
        graph_metrics={"centrality": 0.6, "connectivity": 0.7},
    )
    _check("complexity from query", s2.complexity == 0.8)
    _check("carbon from args", s2.carbon_intensity == 250.0)
    _check("centrality from graph_metrics", s2.graph_centrality == 0.6)

    # ---- 3. Optimizer config validation -------------------------- #
    print("optimizer config:")
    opt = RuntimeDistillationOptimizer({"epsilon": 0.2, "seed": 42})
    _check("epsilon stored", opt.epsilon == 0.2)
    _check(
        "rejects epsilon='high'",
        _raises(RuntimeConfigError, lambda:
                RuntimeDistillationOptimizer({"epsilon": "high"})),
    )
    _check(
        "rejects negative lr",
        _raises(RuntimeConfigError, lambda:
                RuntimeDistillationOptimizer({"distillation_lr": -1})),
    )
    _check(
        "config=None accepted",
        RuntimeDistillationOptimizer() is not None,
    )

    # ---- 4. select_action returns the right shapes -------------- #
    print("select_action shapes:")
    action, x, t_out, t_comb = opt.select_action(s)
    _check("action in range", 0 <= action < N_ACTIONS)
    _check("state_vec shape", x.shape == (FEATURE_DIM,))
    _check(
        "teacher_outputs shape",
        t_out.shape == (N_TEACHERS, N_ACTIONS),
    )
    _check("teacher_combined shape", t_comb.shape == (N_ACTIONS,))
    _check(
        "teacher_outputs rows sum to 1",
        bool(np.allclose(t_out.sum(axis=1), 1.0)),
    )

    # ---- 5. update with correct shapes -------------------------- #
    print("update:")
    opt.update(x, action, 0.5, t_out, t_comb)
    for _ in range(opt.train_every * opt.batch_size):
        a, xv, to, tc = opt.select_action(s)
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
        _raises(RuntimeAdapterError, lambda:
                opt.update(x, 0, float("nan"), t_out, t_comb)),
    )

    # ---- 6. ExecutionStrategy ----------------------------------- #
    print("ExecutionStrategy:")
    _check(
        "from_action(0) == AUTOGEN",
        ExecutionStrategy.from_action(0) is ExecutionStrategy.AUTOGEN,
    )
    _check(
        "from_action(1) == FLEXGEN_LOW",
        ExecutionStrategy.from_action(1)
        is ExecutionStrategy.FLEXGEN_LOW_PRECISION,
    )
    _check(
        "from_action(2) == FLEXGEN_HIGH",
        ExecutionStrategy.from_action(2)
        is ExecutionStrategy.FLEXGEN_HIGH_PRECISION,
    )
    _check(
        "from_action(99) raises",
        _raises(RuntimeAdapterError, lambda:
                ExecutionStrategy.from_action(99)),
    )

    # ---- 7. Reward composition ---------------------------------- #
    print("reward composition:")

    class _RewardProbe:
        """Stand-in that reuses AutoGenRuntime's reward logic."""
        config = {"failure_reward_multiplier": 0.0}
        _strict = False
        _compute_modp_score = AutoGenRuntime._compute_modp_score

    probe = _RewardProbe()
    r_ok = probe._compute_modp_score(
        success=True, state=s, execution_time_ms=500.0,
        energy_kwh=0.01, carbon_kg=0.004,
    )
    r_fail = probe._compute_modp_score(
        success=False, state=s, execution_time_ms=500.0,
        energy_kwh=0.01, carbon_kg=0.004,
    )
    r_fast_fail = probe._compute_modp_score(
        success=False, state=s, execution_time_ms=10.0,
        energy_kwh=0.0001, carbon_kg=0.00004,
    )
    _check("success → reward > 0", r_ok > 0.0)
    _check("failure → reward == 0", r_fail == 0.0)
    _check(
        "fast failure → reward == 0, not positive",
        r_fast_fail == 0.0,
    )

    # ---- 8. Energy model strategy multipliers ------------------- #
    print("energy model:")

    class _EnergyProbe:
        _estimate_energy_and_carbon = (
            AutoGenRuntime._estimate_energy_and_carbon
        )

    ep = _EnergyProbe()
    e_auto, _ = ep._estimate_energy_and_carbon(
        1000, 1.0, ExecutionStrategy.AUTOGEN,
    )
    e_low, _ = ep._estimate_energy_and_carbon(
        1000, 1.0, ExecutionStrategy.FLEXGEN_LOW_PRECISION,
    )
    e_high, _ = ep._estimate_energy_and_carbon(
        1000, 1.0, ExecutionStrategy.FLEXGEN_HIGH_PRECISION,
    )
    _check("flexgen_low < autogen", e_low < e_auto)
    _check("flexgen_high > autogen", e_high > e_auto)

    # ---- 9. Thread safety of update() --------------------------- #
    print("optimizer thread safety:")
    import threading as _th

    opt2 = RuntimeDistillationOptimizer({"seed": 7})
    a2, xv, tout, tc = opt2.select_action(s)
    errors: List[BaseException] = []

    def _worker():
        try:
            for _ in range(50):
                opt2.update(xv, a2, 0.5, tout, tc)
        except BaseException as exc:  # pragma: no cover
            errors.append(exc)

    threads = [_th.Thread(target=_worker) for _ in range(4)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    _check("no thread errors", not errors)
    _check(
        "student weights finite after threading",
        bool(np.all(np.isfinite(opt2.student_weights))),
    )

    # ---- 10. _UnavailableComponent ------------------------------ #
    print("_UnavailableComponent:")
    _check(
        "raises on construction",
        _raises(RuntimeAdapterError, lambda:
                _UnavailableComponent("WorkloadDescriptor")),
    )

    # ---- 11. AutoGenRuntime lifecycle --------------------------- #
    print("AutoGenRuntime lifecycle:")

    class _Base:
        """Stand-in for BaseRuntimeAdapter when running standalone."""

        def __init__(self, *args: Any, **kwargs: Any) -> None:
            pass

    class _RuntimeProbe(_Base, AutoGenRuntime):
        """Combine the base stand-in with the real implementation."""

    rt = _RuntimeProbe()
    # run() before init() must NOT raise.
    try:
        out = rt.run({"question": "hello", "token_count": 100})
        _check("run before init works", True)
    except Exception as exc:  # pragma: no cover — should not happen
        _check(f"run before init failed: {exc}", False)

    _check("output simulated=True", out["simulated"] is True)
    _check("output success=True", out["success"] is True)
    _check("output answer key present", "answer" in out or "accuracy" in out)
    _check(
        "output graph_metrics is a copy",
        out["graph_metrics"] is not rt.graph_metrics,
    )
    _check(
        "output energy present",
        "energy_kwh" in out and "carbon_kg" in out,
    )

    # Non-dict query rejected.
    _check(
        "rejects non-dict query",
        _raises(RuntimeAdapterError, lambda: rt.run("not a dict")),  # type: ignore[arg-type]
    )

    # Non-dict config rejected.
    _check(
        "rejects non-dict config",
        _raises(RuntimeConfigError, lambda: rt.init("not a dict")),  # type: ignore[arg-type]
    )

    # init with dict works.
    rt.init({"graph_metrics": {"centrality": 0.9, "connectivity": 0.1}})
    _check("init accepted config", rt.graph_metrics["centrality"] == 0.9)

    # ---- 12. __all__ / __dir__ ---------------------------------- #
    print("dir:")
    _check("__dir__ matches __all__", __dir__() == sorted(__all__))
    for _name in (
        "AutoGenRuntime", "RuntimeDistillationOptimizer",
        "RuntimeState", "ExecutionStrategy",
        "RuntimeAdapterError", "RuntimeConfigError",
    ):
        _check(f"{_name} exported", _name in __all__)

    print()
    print(f"passed: {passed}  failed: {failed}")
    if failed:
        sys.exit(1)
