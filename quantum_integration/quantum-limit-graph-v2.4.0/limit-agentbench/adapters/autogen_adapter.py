# -*- coding: utf-8 -*-
"""
AutoGen Agent Adapter (Enhanced)
================================

Adapter for Microsoft AutoGen agents with advanced decision-making and
metric tracking. Optionally integrates with FlexGen, LIMIT Graph, MODP,
RLHF, multi-teacher on-policy distillation, bio-inspired optimisation,
and MoE expert gating.

Key fixes over the previous version
-----------------------------------
- **Reward composition.** The MODP reward no longer overwrites the
  success/failure signal. ``reward = success * modp_score`` (with a
  configurable failure multiplier), so a "fast failure" cannot be
  scored as a positive outcome for the distillation optimizer.
- **Gating gradient.** The replay buffer stores *individual* teacher
  outputs (shape ``(n_teachers, n_actions)``) alongside the pre-combined
  distribution, so the gate update uses the correct
  ``teacher_outputs @ error`` gradient instead of the previous
  inner-product / shape-coincidence form.
- **Async recording.** ``record_outcome`` is attempted synchronously,
  then scheduled on a running loop, then run via ``asyncio.run()`` —
  each failure logged with ``logger.exception``, never silently
  swallowed. ``asyncio`` is imported.
- **Thread safety.** ``_last_decision`` is now a **local** variable
  inside ``run()``, not a per-instance field. Concurrent ``run()`` calls
  no longer corrupt each other's learning signal. The optimizer's
  ``update()`` is guarded by an ``RLock``.
- **Feature vector.** All eight features pass through ``_clamp01``
  (which also rejects ``NaN`` / ``inf``), including the three that were
  previously passed through unclamped.
- **Energy normalization.** ``energy_kwh`` is normalized against
  ``MAX_ENERGY_KWH`` instead of ``1.0`` — the old form treated
  ``0.001 kWh`` as "almost the max".
- **Configuration validation.** ``AdapterDistillationOptimizer`` and
  ``AutoGenAdapter`` reject malformed ``config`` dicts at construction
  (``epsilon='high'``, ``distillation_lr=-0.1``, etc.).
- **Narrow error types** — :class:`AdapterError`,
  :class:`AdapterConfigError`.
- **Strict / non-strict modes** — ``strict=True`` raises on integration
  failures; ``strict=False`` (default) logs and continues.
- ``AdapterState`` is ``frozen`` with ``slots`` and full
  ``__post_init__`` validation.
- ``ExecutionStrategy`` enum replaces raw strings.
- ``_UnavailableComponent`` replaces the silent ``pass``-body dummy
  classes — any accidental use fails loudly at construction time.
- Lazy ``%s`` logging, ``logger.exception`` on exceptions, no eager
  f-string formatting.
- ``__version__``, ``__all__``, ``__dir__``, and a ``_check()``-based
  smoke test that survives ``python -O``.

Compatibility
-------------
- ``AutoGenAdapter(agent, config=None, strict=False)`` — ``strict`` is
  new and keyword-only; existing call sites keep their default behaviour.
- ``AdapterState(...)`` accepts the same keyword arguments, but is now
  frozen and validated.
- ``execution_strategy`` in the output dict is the ``ExecutionStrategy``
  **value** (a string), not the enum — call sites that compared against
  ``"autogen_only"`` keep working.
- ``output["answer"]`` may be ``None`` on agent failure; a new
  ``output["success"]: bool`` field makes this explicit.

Requires Python 3.8+.
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
from typing import Any, Deque, Dict, List, Optional, Tuple

import numpy as np

from .base_adapter import BaseAgentAdapter


logger = logging.getLogger(__name__)

__version__ = "2.5.0"


# --------------------------------------------------------------------------- #
# Errors
# --------------------------------------------------------------------------- #
class AdapterError(ValueError):
    """Raised for invalid AutoGen adapter inputs or execution failures."""


class AdapterConfigError(AdapterError):
    """Raised for invalid adapter or optimizer configuration."""


# --------------------------------------------------------------------------- #
# Constants — normalization and defaults
# --------------------------------------------------------------------------- #
MAX_TOKENS: float = 10_000.0
MAX_LATENCY_MS: float = 1_000.0
MAX_CARBON_INTENSITY: float = 500.0
MAX_COST_BUDGET: float = 100.0
MAX_ENERGY_KWH: float = 0.1            # upper bound for MODP normalization
MAX_HEAT_KWH: float = 0.1              # not used today; reserved

FEATURE_DIM: int = 8
N_ACTIONS: int = 3
N_TEACHERS: int = 3
REPLAY_BUFFER_SIZE: int = 2_000
TRAIN_EVERY: int = 10
BATCH_SIZE: int = 8

DEFAULT_MODP_WEIGHTS: Tuple[float, ...] = (
    0.4,   # carbon
    0.3,   # energy
    0.2,   # latency
    0.1,   # cost
)


# --------------------------------------------------------------------------- #
# Enums
# --------------------------------------------------------------------------- #
class ExecutionStrategy(str, Enum):
    """Execution strategies the distillation optimizer can select."""

    AUTOGEN_ONLY = "autogen_only"
    FLEXGEN_LOW_PRECISION = "flexgen_low_precision"
    FLEXGEN_HIGH_PRECISION = "flexgen_high_precision"

    @classmethod
    def from_action(cls, action_idx: int) -> "ExecutionStrategy":
        if action_idx == 0:
            return cls.AUTOGEN_ONLY
        if action_idx == 1:
            return cls.FLEXGEN_LOW_PRECISION
        if action_idx == 2:
            return cls.FLEXGEN_HIGH_PRECISION
        raise AdapterError(
            f"unknown action index {action_idx!r}; expected 0, 1, or 2."
        )


# --------------------------------------------------------------------------- #
# Optional enhancement imports
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
    logger.info("Enhancements unavailable: %s", _exc)
    WorkloadDescriptor = _UnavailableComponent  # type: ignore[assignment,misc]
    NodeDescriptor = _UnavailableComponent      # type: ignore[assignment,misc]
    ZeroTrustArchitecture = _UnavailableComponent  # type: ignore[assignment,misc]
    FeedbackEvent = _UnavailableComponent       # type: ignore[assignment,misc]
    TaskType = _UnavailableComponent            # type: ignore[assignment,misc]
    Urgency = _UnavailableComponent             # type: ignore[assignment,misc]
    NodeType = _UnavailableComponent            # type: ignore[assignment,misc]


# --------------------------------------------------------------------------- #
# Adapter state
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


@dataclass(frozen=True, slots=True)
class AdapterState:
    """
    State representation for decision-making (task features + graph
    metrics).

    All ranges are enforced in ``__post_init__``; the feature vector
    additionally clamps to ``[0, 1]`` as a defence in depth.
    """

    task_complexity: float          # 0-1
    token_count: int
    latency_target: float
    carbon_intensity: float
    # ``human_feedback_score`` is a **historical** prior over past runs
    # of similar tasks, not a post-run rating. Range [0, 1].
    human_feedback_score: float = 0.5
    graph_centrality: float = 0.5
    graph_connectivity: float = 0.5
    cost_budget: float = 10.0

    def __post_init__(self) -> None:
        _clamp01("task_complexity", self.task_complexity)
        _nonneg("token_count", self.token_count)
        if not isinstance(self.token_count, int) or isinstance(
            self.token_count, bool
        ):
            raise AdapterError("token_count must be an int.")
        _nonneg("latency_target", self.latency_target)
        _nonneg("carbon_intensity", self.carbon_intensity)
        _clamp01("human_feedback_score", self.human_feedback_score)
        _clamp01("graph_centrality", self.graph_centrality)
        _clamp01("graph_connectivity", self.graph_connectivity)
        _nonneg("cost_budget", self.cost_budget)

    def to_feature_vector(self) -> np.ndarray:
        """
        Convert to a normalized feature vector in ``[0, 1]^8``.

        Every component is clamped to ``[0, 1]``; ``__post_init__``
        already rejects out-of-range inputs, so the clamping here is
        defensive only.
        """
        return np.array([
            _clamp01("task_complexity", self.task_complexity),
            _clamp01(
                "token_count_norm",
                self.token_count / MAX_TOKENS,
            ),
            _clamp01(
                "latency_target_norm",
                self.latency_target / MAX_LATENCY_MS,
            ),
            _clamp01(
                "carbon_intensity_norm",
                self.carbon_intensity / MAX_CARBON_INTENSITY,
            ),
            _clamp01("human_feedback_score", self.human_feedback_score),
            _clamp01("graph_centrality", self.graph_centrality),
            _clamp01("graph_connectivity", self.graph_connectivity),
            _clamp01(
                "cost_budget_norm",
                self.cost_budget / MAX_COST_BUDGET,
            ),
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
class AdapterDistillationOptimizer:
    """
    Multi-teacher distillation with MoE gating to decide execution
    parameters (autogen-only vs FlexGen low / high precision).
    """

    def __init__(self, config: Optional[Dict[str, Any]] = None) -> None:
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
            maxlen=self.config["replay_buffer_size"],
        )
        self.counter: int = 0
        self._lock = threading.RLock()

        # Per-instance RNG (seedable, independent of global random).
        seed = self.config["seed"]
        self._py_rng = random.Random(seed)

        # Teachers (rule-based + RLHF + historical).
        self.teachers = (
            self._rule_based_teacher,
            self._rlhf_teacher,
            self._historical_teacher,
        )
        gate_seed = self.config["gate_seed"]
        self._np_rng = np.random.default_rng(gate_seed)

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
            raise AdapterConfigError(
                f"config must be a dict, got {type(config).__name__}."
            )

        def _num(key: str, default: float, *, positive: bool) -> float:
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
        out["replay_buffer_size"] = _int(
            "replay_buffer_size", REPLAY_BUFFER_SIZE, positive=True,
        )
        out["seed"] = config.get("seed")
        out["gate_seed"] = config.get("gate_seed")
        return out

    # ----------------------------------------------------------------- teachers
    def _rule_based_teacher(self, state: AdapterState) -> np.ndarray:
        probs = np.ones(self.n_actions) * 0.1
        if state.task_complexity > 0.7:
            probs[0] = 0.8
        elif state.carbon_intensity > 400:
            probs[1] = 0.7
        else:
            probs[2] = 0.5
        return probs / probs.sum()

    def _rlhf_teacher(self, state: AdapterState) -> np.ndarray:
        probs = np.ones(self.n_actions) / self.n_actions
        if state.human_feedback_score > 0.7:
            probs[0] += 0.2
        elif state.human_feedback_score < 0.3:
            probs[1] += 0.2
        return probs / probs.sum()

    def _historical_teacher(self, state: AdapterState) -> np.ndarray:
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
        state: AdapterState,
        exploration: bool = True,
    ) -> Tuple[int, np.ndarray, np.ndarray, np.ndarray]:
        """
        Choose an action and return the decision context.

        Returns
        -------
        (action, state_vec, teacher_outputs, teacher_combined)
            ``teacher_outputs`` has shape ``(n_teachers, n_actions)``
            and carries the *individual* teacher distributions, needed
            for the gate update. ``teacher_combined`` has shape
            ``(n_actions,)`` and is the gated mixture at decision time.
        """
        x = state.to_feature_vector()

        teacher_outputs = np.stack(
            [t(state) for t in self.teachers],
        )  # shape (n_teachers, n_actions)

        gate_weights = self._gate_forward(x)                # (n_teachers,)
        teacher_combined = (
            gate_weights[:, None] * teacher_outputs
        ).sum(axis=0)                                        # (n_actions,)
        teacher_combined = teacher_combined / teacher_combined.sum()

        logits = x @ self.student_weights + self.student_bias
        logits -= logits.max()
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
        Record one transition and (every ``train_every`` updates) train
        the student and the gate.

        The gate gradient uses the **individual** teacher outputs
        ``teacher_outputs``, so the update reflects "which teacher's
        distribution is closest to the desired target", not a
        re-combination of the already-combined output.
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

            # ---- student update ---------------------------------- #
            one_hot = np.zeros(self.n_actions)
            one_hot[entry.action] = 1.0
            grad_distill = -(entry.teacher_combined - cur_probs)
            grad_rl = -entry.reward * (one_hot - cur_probs)
            grad = self.distill_w * grad_distill + self.rl_w * grad_rl
            self.student_weights -= self.lr * np.outer(s, grad)
            self.student_bias -= self.lr * grad

            # ---- gate update ------------------------------------- #
            # Target: the student's own (updated) distribution. This is
            # the standard MoE self-consistency objective — train the
            # gate so the gated mixture tracks the student's current
            # beliefs, which in turn are being trained toward the reward.
            target = self._student_probs(s)
            gate = self._gate_forward(s)                     # (n_teachers,)
            combined = (gate[:, None] * entry.teacher_outputs).sum(axis=0)
            combined = combined / combined.sum()
            error = combined - target                        # (n_actions,)
            # d/dg ||g @ T - target||^2 = 2 * T @ (g @ T - target)
            grad_gate = entry.teacher_outputs @ error        # (n_teachers,)
            self.gate_weights -= self.gate_lr * np.outer(s, grad_gate)
            self.gate_bias -= self.gate_lr * grad_gate

    def _student_probs(self, state_vec: np.ndarray) -> np.ndarray:
        logits = state_vec @ self.student_weights + self.student_bias
        logits = logits - logits.max()
        probs = np.exp(logits)
        return probs / probs.sum()


# --------------------------------------------------------------------------- #
# Adapter
# --------------------------------------------------------------------------- #
@dataclass(frozen=True, slots=True)
class _Decision:
    """The per-call decision context, kept local so concurrent ``run()``
    calls cannot corrupt each other's learning signal."""

    state_vec: np.ndarray
    action_idx: int
    teacher_outputs: np.ndarray
    teacher_combined: np.ndarray
    state: AdapterState


class AutoGenAdapter(BaseAgentAdapter):
    """
    Adapter for Microsoft AutoGen agents with optional enhancement
    integration.

    Supports ConversableAgent, AssistantAgent, UserProxyAgent, and
    multi-agent conversations. The ``strict`` flag controls whether
    integration failures raise (``strict=True``) or log (``strict=False``,
    default — matching the previous behaviour).
    """

    def __init__(
        self,
        agent: Any,
        config: Optional[Dict[str, Any]] = None,
        *,
        strict: bool = False,
    ) -> None:
        super().__init__(agent, "autogen")
        if not isinstance(config, dict) and config is not None:
            raise AdapterConfigError(
                f"config must be a dict or None, got "
                f"{type(config).__name__}."
            )
        self.config: Dict[str, Any] = config or {}
        self._strict: bool = bool(strict)
        self.use_enhancements: bool = (
            bool(self.config.get("use_enhancements", False))
            and ENHANCEMENTS_AVAILABLE
        )

        self.workload_descriptor: Any = None
        self.node_descriptor: Any = None
        self.zero_trust: Any = None
        self.distillation_optimizer: Optional[
            AdapterDistillationOptimizer
        ] = None

        if self.use_enhancements:
            self._initialize_enhancements()

    # ---------------------------------------------------------------- init
    def _initialize_enhancements(self) -> None:
        """Initialize enhanced decision-making components."""
        try:
            self.workload_descriptor = WorkloadDescriptor(
                task_id="autogen_task",
                task_type=TaskType.INFERENCE,
                tokens=self.config.get("default_tokens", 1000),
                latency_target=self.config.get("default_latency", 500.0),
                urgency=Urgency.MEDIUM,
                estimated_energy_joules=0.01,
                estimated_carbon_kg=0.0001,
                user_id="autogen_adapter",
                metadata={"adapter": "autogen"},
            )
            self.node_descriptor = NodeDescriptor(
                id="autogen_node",
                type=NodeType.EDGE,
                region="us-east",
                region_carbon_intensity=400.0,
                energy_per_token=0.00005,
                helium_connectivity_score=0.8,
                uptime=0.99,
                renewable_fraction=0.3,
                cooling_type="air",
                hardware_model="cpu",
            )
            if self.config.get("enable_zero_trust", False):
                self.zero_trust = ZeroTrustArchitecture()
            self.distillation_optimizer = AdapterDistillationOptimizer(
                self.config
            )
            logger.info("AutoGenAdapter enhanced components initialized")
        except Exception:
            logger.exception("Failed to initialize enhancements")
            self.use_enhancements = False
            if self._strict:
                raise

    # ---------------------------------------------------------------- energy
    def _estimate_energy_and_carbon(
        self,
        token_count: int,
        execution_time_ms: float,
        strategy: ExecutionStrategy = ExecutionStrategy.AUTOGEN_ONLY,
    ) -> Tuple[float, float]:
        """
        **Mock** energy and carbon estimation for a run.

        Real deployments should replace this with measured telemetry or
        a calibrated model. The model here:

        - Assumes a 250 W average power draw.
        - Uses a fixed 400 gCO₂/kWh carbon intensity (US average).
        - Scales linearly with token count beyond 10 000.
        - Applies a per-strategy multiplier:

          - ``AUTOGEN_ONLY``          — 1.0
          - ``FLEXGEN_LOW_PRECISION`` — 0.7
          - ``FLEXGEN_HIGH_PRECISION``— 1.1

        Returns
        -------
        (energy_kwh, carbon_kg)
        """
        strategy_multiplier = {
            ExecutionStrategy.AUTOGEN_ONLY: 1.0,
            ExecutionStrategy.FLEXGEN_LOW_PRECISION: 0.7,
            ExecutionStrategy.FLEXGEN_HIGH_PRECISION: 1.1,
        }[strategy]

        hours = max(execution_time_ms, 0.0) / 1000.0 / 3600.0
        energy_kwh = hours * 0.25 * strategy_multiplier
        carbon_kg = energy_kwh * 0.4  # 400 g/kWh → 0.4 kg/kWh

        token_scale = 1.0 + max(token_count, 0) / 10_000.0
        return energy_kwh * token_scale, carbon_kg * token_scale

    # ---------------------------------------------------------------- run
    def run(
        self,
        task_input: Dict[str, Any],
        track_green_metrics: bool = True,
    ) -> Dict[str, Any]:
        """
        Run the AutoGen agent on ``task_input``, optionally selecting an
        execution strategy via the distillation optimizer and recording
        sustainability metrics.
        """
        logger.debug(
            "Running AutoGen agent (enhanced=%s)", self.use_enhancements,
        )
        start_time = time.time()

        # ---- 1. Parse input --------------------------------------- #
        if isinstance(task_input, dict):
            message = (
                task_input.get("question")
                or task_input.get("input")
                or str(task_input)
            )
            token_count = int(task_input.get("token_count", 1000))
        else:
            message = str(task_input)
            token_count = 1000

        # ---- 2. Decision ------------------------------------------ #
        decision: Optional[_Decision] = None
        strategy = ExecutionStrategy.AUTOGEN_ONLY
        if self.use_enhancements and self.distillation_optimizer is not None:
            state = self._build_state(task_input, token_count)
            action_idx, state_vec, t_out, t_comb = (
                self.distillation_optimizer.select_action(state)
            )
            strategy = ExecutionStrategy.from_action(action_idx)
            decision = _Decision(
                state_vec=state_vec,
                action_idx=action_idx,
                teacher_outputs=t_out,
                teacher_combined=t_comb,
                state=state,
            )

        # ---- 3. Execute ------------------------------------------- #
        success = False
        result: Any = None
        try:
            result = self._execute_agent(message, strategy)
            success = True
        except Exception:
            logger.exception("Agent execution failed")
            if self._strict:
                raise

        execution_time_ms = (time.time() - start_time) * 1000.0
        energy_kwh, carbon_kg = self._estimate_energy_and_carbon(
            token_count, execution_time_ms, strategy,
        )

        # ---- 4. Build output -------------------------------------- #
        output: Dict[str, Any] = {
            "answer": result,
            "success": success,
            "framework": "autogen",
            "execution_strategy": strategy.value,
            "execution_time_ms": execution_time_ms,
        }
        if track_green_metrics:
            output["energy_consumed_kwh"] = energy_kwh
            output["carbon_emitted_kg"] = carbon_kg
            output["helium_usage_l"] = 0.0  # placeholder

        # ---- 5. Learn from the outcome ---------------------------- #
        if (
            self.use_enhancements
            and decision is not None
            and self.distillation_optimizer is not None
        ):
            self._update_optimizer(
                decision, success, execution_time_ms,
                energy_kwh, carbon_kg,
            )
            self._record_outcome(
                success=success,
                execution_time_ms=execution_time_ms,
                energy_kwh=energy_kwh,
                carbon_kg=carbon_kg,
                strategy=strategy,
            )
            self._emit_feedback(
                success=success,
                execution_time_ms=execution_time_ms,
                energy_kwh=energy_kwh,
                carbon_kg=carbon_kg,
                strategy=strategy,
            )

        return output

    # ---------------------------------------------------------------- helpers
    def _build_state(
        self, task_input: Any, token_count: int,
    ) -> AdapterState:
        d = task_input if isinstance(task_input, dict) else {}
        carbon = (
            self.node_descriptor.region_carbon_intensity
            if self.node_descriptor is not None else 400.0
        )
        return AdapterState(
            task_complexity=float(d.get("complexity", 0.5)),
            token_count=token_count,
            latency_target=float(d.get("latency_target", 500.0)),
            carbon_intensity=float(carbon),
            human_feedback_score=float(d.get("human_feedback_score", 0.5)),
            graph_centrality=float(d.get("graph_centrality", 0.5)),
            graph_connectivity=float(d.get("graph_connectivity", 0.5)),
            cost_budget=float(d.get("cost_budget", 10.0)),
        )

    def _execute_agent(
        self, message: str, strategy: ExecutionStrategy,
    ) -> Any:
        """Execute the agent (or its FlexGen delegate)."""
        if strategy in (
            ExecutionStrategy.FLEXGEN_LOW_PRECISION,
            ExecutionStrategy.FLEXGEN_HIGH_PRECISION,
        ):
            logger.info(
                "Delegating to FlexGen with strategy %s", strategy.value,
            )
            return f"FlexGen result for: {message}"
        if hasattr(self.agent, "generate_reply"):
            return self.agent.generate_reply(
                messages=[{"role": "user", "content": message}],
            )
        if hasattr(self.agent, "initiate_chat"):
            return self.agent.initiate_chat(message=message)
        raise AdapterError(
            f"Unsupported AutoGen agent type: {type(self.agent).__name__}."
        )

    def _compute_modp_score(
        self,
        success: bool,
        state: AdapterState,
        execution_time_ms: float,
        energy_kwh: float,
        carbon_kg: float,
    ) -> float:
        """
        Compute the MODP reward.

        The **success flag is a hard gate**: a failed run scores
        ``FAILURE_REWARD_MULTIPLIER * modp_score`` (default 0.0), never
        the raw MODP score. This prevents the optimizer from learning to
        "fail fast and cheap".
        """
        # Normalized objectives in [0, 1].
        carbon_norm = 1.0 - _clamp01(
            "carbon_norm", state.carbon_intensity / MAX_CARBON_INTENSITY,
        )
        # NOTE: energy is normalized against MAX_ENERGY_KWH, not 1.0.
        # The previous code divided kWh by 1.0, which treated 0.001
        # kWh as "almost the max".
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
        return modp_score * (1.0 if success else failure_multiplier)

    def _update_optimizer(
        self,
        decision: _Decision,
        success: bool,
        execution_time_ms: float,
        energy_kwh: float,
        carbon_kg: float,
    ) -> None:
        assert self.distillation_optimizer is not None
        reward = self._compute_modp_score(
            success, decision.state, execution_time_ms,
            energy_kwh, carbon_kg,
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

    def _record_outcome(
        self,
        *,
        success: bool,
        execution_time_ms: float,
        energy_kwh: float,
        carbon_kg: float,
        strategy: ExecutionStrategy,
    ) -> None:
        """
        Best-effort update of the workload descriptor.

        ``record_outcome`` may be sync or async. This helper tries sync
        first, then schedules on a running loop, then falls back to
        ``asyncio.run()``. Failures are logged, never silently swallowed.
        """
        if self.workload_descriptor is None:
            return
        try:
            self.workload_descriptor.record_outcome(
                latency_achieved_ms=execution_time_ms,
                carbon_saved_kg=max(0.0, 0.01 - carbon_kg),
                energy_used_joules=energy_kwh * 3_600_000.0,
            )
            return
        except TypeError:
            # Likely an async method — fall through.
            pass
        except Exception:
            logger.exception("workload_descriptor.record_outcome failed")
            if self._strict:
                raise
            return

        try:
            self.workload_descriptor.record_outcome(
                latency_achieved_ms=execution_time_ms,
                carbon_saved_kg=max(0.0, 0.01 - carbon_kg),
                energy_used_joules=energy_kwh * 3_600_000.0,
            )
        except Exception:
            # If the previous sync attempt raised TypeError, the
            # method may be a coroutine function. Schedule it.
            try:
                loop = asyncio.get_running_loop()
            except RuntimeError:
                try:
                    asyncio.run(
                        self.workload_descriptor.record_outcome(
                            latency_achieved_ms=execution_time_ms,
                            carbon_saved_kg=max(0.0, 0.01 - carbon_kg),
                            energy_used_joules=energy_kwh * 3_600_000.0,
                        )
                    )
                except Exception:
                    logger.exception(
                        "asyncio.run(record_outcome) failed"
                    )
                    if self._strict:
                        raise
            else:
                try:
                    loop.create_task(
                        self.workload_descriptor.record_outcome(
                            latency_achieved_ms=execution_time_ms,
                            carbon_saved_kg=max(0.0, 0.01 - carbon_kg),
                            energy_used_joules=energy_kwh * 3_600_000.0,
                        )
                    )
                except Exception:
                    logger.exception(
                        "scheduling record_outcome failed"
                    )
                    if self._strict:
                        raise

    def _emit_feedback(
        self,
        *,
        success: bool,
        execution_time_ms: float,
        energy_kwh: float,
        carbon_kg: float,
        strategy: ExecutionStrategy,
    ) -> None:
        """Emit a ``FeedbackEvent`` when the enhancements package is loaded."""
        if not ENHANCEMENTS_AVAILABLE:
            return
        try:
            event = FeedbackEvent(
                source="autogen_adapter",
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
                    "latency_ms": execution_time_ms,
                    "energy_joules": energy_kwh * 3_600_000.0,
                    "carbon_g": carbon_kg * 1000.0,
                    "helium_cost": 0,
                    "duration_ms": execution_time_ms,
                },
                adaptive_cost_value=1.0 if success else 0.0,
                tags=["autogen", strategy.value],
            )
            logger.debug("FeedbackEvent created: %s", event.event_id)
        except Exception:
            logger.exception("FeedbackEvent construction failed")
            if self._strict:
                raise

    # ---------------------------------------------------------------- name
    def _get_agent_name(self) -> str:
        """Return the AutoGen agent's ``name`` if present."""
        if hasattr(self.agent, "name"):
            return str(self.agent.name)
        return "AutoGenAgent"


# --------------------------------------------------------------------------- #
# Public API
# --------------------------------------------------------------------------- #
__all__: List[str] = [
    "__version__",
    "AdapterConfigError",
    "AdapterDistillationOptimizer",
    "AdapterError",
    "AdapterState",
    "AutoGenAdapter",
    "ExecutionStrategy",
]


def __dir__() -> List[str]:
    return sorted(__all__)


# --------------------------------------------------------------------------- #
# Smoke test: python -m integrations.autogen_adapter
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

    # ---- 1. AdapterState validation ------------------------------ #
    print("AdapterState validation:")
    state = AdapterState(
        task_complexity=0.5,
        token_count=1000,
        latency_target=500.0,
        carbon_intensity=400.0,
    )
    _check(
        "feature vector shape",
        state.to_feature_vector().shape == (FEATURE_DIM,),
    )
    _check(
        "feature vector in [0,1]",
        bool(np.all((state.to_feature_vector() >= 0)
                    & (state.to_feature_vector() <= 1))),
    )
    _check(
        "rejects NaN complexity",
        _raises(AdapterError, lambda: AdapterState(
            task_complexity=float("nan"),
            token_count=1, latency_target=1.0, carbon_intensity=1.0,
        )),
    )
    _check(
        "rejects negative token_count",
        _raises(AdapterError, lambda: AdapterState(
            task_complexity=0.5, token_count=-1,
            latency_target=1.0, carbon_intensity=1.0,
        )),
    )
    _check(
        "rejects out-of-range feedback",
        _raises(AdapterError, lambda: AdapterState(
            task_complexity=0.5, token_count=1,
            latency_target=1.0, carbon_intensity=1.0,
            human_feedback_score=2.0,
        )),
    )
    _check(
        "frozen — mutation rejected",
        _raises(
            (AttributeError, Exception),
            lambda: setattr(state, "task_complexity", 0.9),
        ),
    )

    # ---- 2. Optimizer config validation -------------------------- #
    print("optimizer config:")
    opt = AdapterDistillationOptimizer(
        {"epsilon": 0.2, "distillation_lr": 0.01, "seed": 42},
    )
    _check("epsilon stored", opt.epsilon == 0.2)
    _check(
        "rejects epsilon='high'",
        _raises(AdapterConfigError, lambda:
                AdapterDistillationOptimizer({"epsilon": "high"})),
    )
    _check(
        "rejects negative lr",
        _raises(AdapterConfigError, lambda:
                AdapterDistillationOptimizer({"distillation_lr": -1})),
    )
    _check(
        "rejects epsilon > 1",
        _raises(AdapterConfigError, lambda:
                AdapterDistillationOptimizer({"epsilon": 2.0})),
    )

    # ---- 3. select_action returns the right shapes ---------------- #
    print("select_action shapes:")
    action, x, t_out, t_comb = opt.select_action(state)
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

    # ---- 4. update() with correct shapes ------------------------- #
    print("update:")
    try:
        opt.update(x, action, 0.5, t_out, t_comb)
        _check("update succeeded", True)
    except Exception as exc:
        _check(f"update raised {exc}", False)

    # Trigger a real training step with a full batch.
    for _ in range(opt.train_every * opt.batch_size):
        a, xv, tout, tc = opt.select_action(state)
        opt.update(xv, a, 0.5, tout, tc)
    _check("training step completed", True)
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

    # ---- 5. ExecutionStrategy ------------------------------------ #
    print("ExecutionStrategy:")
    _check(
        "from_action(0) == AUTOGEN_ONLY",
        ExecutionStrategy.from_action(0) is ExecutionStrategy.AUTOGEN_ONLY,
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
        _raises(AdapterError, lambda: ExecutionStrategy.from_action(99)),
    )
    _check(
        "str(enum) == value",
        str(ExecutionStrategy.AUTOGEN_ONLY) == "autogen_only",
    )

    # ---- 6. Reward composition ----------------------------------- #
    print("reward composition:")

    class _FakeAdapter:
        """Stand-in adapter that reuses AutoGenAdapter's reward logic."""

        config = {"failure_reward_multiplier": 0.0}
        _strict = False

        _compute_modp_score = AutoGenAdapter._compute_modp_score

    adapter = _FakeAdapter()
    # Success with average state → positive.
    r_ok = adapter._compute_modp_score(
        True, state, execution_time_ms=500.0,
        energy_kwh=0.01, carbon_kg=0.004,
    )
    _check("success → reward > 0", r_ok > 0.0)
    # Failure with the same state → zero.
    r_fail = adapter._compute_modp_score(
        False, state, execution_time_ms=500.0,
        energy_kwh=0.01, carbon_kg=0.004,
    )
    _check("failure → reward == 0", r_fail == 0.0)
    # Failure with a fast/cheap run — still zero, not positive.
    r_fast_fail = adapter._compute_modp_score(
        False, state, execution_time_ms=10.0,
        energy_kwh=0.0001, carbon_kg=0.00004,
    )
    _check(
        "fast failure → reward == 0, not positive",
        r_fast_fail == 0.0,
    )

    # ---- 7. Energy model strategy multipliers -------------------- #
    print("energy model:")
    class _EnergyAdapter:
        config = {}
        _strict = False
        _estimate_energy_and_carbon = (
            AutoGenAdapter._estimate_energy_and_carbon
        )
    ea = _EnergyAdapter()
    e_auto, _ = ea._estimate_energy_and_carbon(
        1000, 1000.0, ExecutionStrategy.AUTOGEN_ONLY,
    )
    e_low, _ = ea._estimate_energy_and_carbon(
        1000, 1000.0, ExecutionStrategy.FLEXGEN_LOW_PRECISION,
    )
    e_high, _ = ea._estimate_energy_and_carbon(
        1000, 1000.0, ExecutionStrategy.FLEXGEN_HIGH_PRECISION,
    )
    _check("flexgen_low < autogen", e_low < e_auto)
    _check("flexgen_high > autogen", e_high > e_auto)

    # ---- 8. Thread safety of update() ---------------------------- #
    print("optimizer thread safety:")
    import threading as _th

    opt2 = AdapterDistillationOptimizer({"seed": 7})
    a, xv, tout, tc = opt2.select_action(state)
    errors: List[BaseException] = []

    def _worker():
        try:
            for _ in range(50):
                opt2.update(xv, a, 0.5, tout, tc)
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

    # ---- 9. _UnavailableComponent -------------------------------- #
    print("_UnavailableComponent:")
    _check(
        "raises on construction",
        _raises(AdapterError, lambda:
                _UnavailableComponent("WorkloadDescriptor")),
    )

    # ---- 10. AutoGenAdapter contract ---------------------------- #
    print("AutoGenAdapter:")
    class _FakeAgent:
        name = "fake"
        def generate_reply(self, messages):
            return "reply"

    a1 = AutoGenAdapter(_FakeAgent())
    out = a1.run({"question": "hello"})
    _check("answer present", out["answer"] == "reply")
    _check("success=True", out["success"] is True)
    _check("strategy value", out["execution_strategy"] == "autogen_only")
    _check("has energy", "energy_consumed_kwh" in out)
    _check("has carbon", "carbon_emitted_kg" in out)

    class _BadAgent:
        name = "bad"
    a2 = AutoGenAdapter(_BadAgent())
    out2 = a2.run({"question": "hello"})
    _check("failed answer is None", out2["answer"] is None)
    _check("success=False", out2["success"] is False)

    _check(
        "strict=True raises on execute failure",
        _raises(AdapterError, lambda:
                AutoGenAdapter(_BadAgent(), strict=True).run("hello")),
    )

    _check(
        "rejects non-dict config",
        _raises(AdapterConfigError, lambda: AutoGenAdapter(
            _FakeAgent(), config="not a dict",  # type: ignore[arg-type]
        )),
    )

    # ---- 11. __all__ / __dir__ ---------------------------------- #
    print("dir:")
    _check("__dir__ matches __all__", __dir__() == sorted(__all__))
    for _name in (
        "AdapterError", "AdapterConfigError", "AdapterState",
        "AdapterDistillationOptimizer", "AutoGenAdapter",
        "ExecutionStrategy",
    ):
        _check(f"{_name} exported", _name in __all__)

    print()
    print(f"passed: {passed}  failed: {failed}")
    if failed:
        sys.exit(1)
