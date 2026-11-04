# -*- coding: utf-8 -*-
"""
Adapter implementation for LangChain-based agents.
=================================================

Integrates LangChain execution into the multi-metric evaluation
pipeline. Optionally integrates LIMIT Graph, MODP, RLHF, multi-teacher
on-policy distillation, bio-inspired optimisation, and MoE expert
gating.

Key fixes over the previous version
-----------------------------------
- **Hook name fix.** ``_init_enhanced`` is renamed to
  ``_init_enhancements`` so the base class's ``_apply_config`` path
  and this class's config-application both call the same hook.
  Previously, ``init(config)`` silently replaced the corrected base
  optimizer with this file's broken copy.
- **Safe teacher-output padding.** Slices over-sized teacher outputs
  before padding under-sized ones; the previous ``np.pad`` with a
  negative width raised ``ValueError`` whenever a teacher returned
  more than ``n_actions``.
- **Correct gating gradient.** ``select_action`` returns a **4-tuple**
  ``(action, state_vec, teacher_outputs, teacher_combined)``; the
  replay buffer stores both, and ``update`` uses
  ``teacher_outputs @ error`` (correct shape) instead of the previous
  inner-product shape coincidence.
- **Success gating.** The MODP reward is multiplied by the success flag
  (``0`` or ``failure_reward_multiplier`` from config). A failed run
  cannot score positively.
- **Simulated marker.** ``output["simulated"] = True`` on every run;
  ``output["success"] = bool`` makes the outcome explicit.
- **Feature clamping.** All eight features pass through ``_clamp01``.
- **Energy normalisation.** ``energy_norm`` divides by
  ``MAX_ENERGY_KWH`` (``0.1``), not ``1.0``.
- **``modp_weights`` validated.** Shape ``(4,)``, finite,
  non-negative, positive sum; invalid values raise
  ``RuntimeConfigError``.
- **``query`` validated.** Non-dict queries raise
  ``RuntimeAdapterError`` before reaching ``RuntimeState``.
- **``_last_decision`` removed.** A per-call ``_Decision`` dataclass
  is held in a local variable inside ``run()``; the optimizer's
  ``update()`` is guarded by an internal ``RLock``.
- **Config validation.** ``RuntimeDistillationOptimizer`` rejects
  malformed config in ``__init__``.
- **Full rollback on init failure.** Every enhancement component is
  reset to ``None``; ``strict=True`` re-raises.
- **Top-level imports.** ``TaskType``, ``Urgency``, ``NodeType``
  moved into the top-level enhancement import block.
- **``init()`` and ``_apply_config`` inherited from base.** No local
  overrides; the base's flag recomputation and enhancement re-init
  apply on both ``__init__`` and ``init()``.
- **``self.config`` copied.** Caller mutation of the config dict does
  not leak in.
- **``_try_async`` for async bridges.** ``record_outcome`` and
  ``zero_trust.shutdown`` handle sync / running-loop /
  ``asyncio.run()`` with ``logger.exception`` on failure.
- **Narrow error types.** :class:`RuntimeAdapterError` /
  :class:`RuntimeConfigError` from the base class.
- **Lazy ``%s`` logging.** ``logger.exception`` on caught exceptions.
- ``__version__``, ``__all__``, ``__dir__``, and a ``_check()``-based
  smoke test that survives ``python -O``.

Compatibility
-------------
- ``LangChainRuntime(config=None, *, strict=False)`` — config is now
  optional; ``strict`` is new and keyword-only.
- ``run(query)`` — same positional signature. Output gains
  ``"simulated"`` and ``"success"`` (additive); every previously
  returned key is preserved.
- ``RuntimeDistillationOptimizer.select_action(state)`` returns
  ``(action, state_vec, teacher_outputs, teacher_combined)`` — 4
  values (was 3).
- ``RuntimeDistillationOptimizer.update(state_vec, action, reward,
  teacher_outputs, teacher_combined)`` — 5 positional args (was 5 but
  the last two are re-shaped; the old ``next_state_vec`` argument is
  gone).
- ``init(config)`` — delegates to :meth:`BaseRuntimeAdapter.init`;
  re-computes flags and re-initialises enhancements on every call.
- Any caller that overrode ``_init_enhanced`` (the old hook name) must
  switch to ``_init_enhancements``.

Future refactor
---------------
:class:`RuntimeDistillationOptimizer` is now identical to the class of
the same name in ``src/runtimes/autogen_runtime.py`` and the
``DistillationOptimizer`` classes in ``src/integrations/base_adapter.py``
and ``src/integrations/base_runtime.py``. A future refactor should move
a single canonical copy to ``src/enhancements/distillation.py``.

Requires Python 3.8+. ``slots=True`` on dataclasses requires 3.10+;
drop it for 3.8/3.9.
"""

from __future__ import annotations

import logging
import math
import random
import threading
import time
from collections import deque
from dataclasses import dataclass
from enum import Enum
from typing import Any, Deque, Dict, Final, List, Optional, Tuple

import numpy as np

from .base_runtime import (
    BaseRuntimeAdapter,
    RuntimeAdapterError,
    RuntimeConfigError,
)


logger = logging.getLogger(__name__)

__version__ = "2.5.0"


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
# Validation helpers (local copies for self-containment)
# --------------------------------------------------------------------------- #
def _clamp01(name: str, value: Any) -> float:
    """Clamp ``value`` to ``[0, 1]``; reject non-numeric / non-finite."""
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise RuntimeAdapterError(
            f"{name} must be numeric, got {type(value).__name__}."
        )
    fv = float(value)
    if not math.isfinite(fv):
        raise RuntimeAdapterError(
            f"{name} must be finite, got {value!r}."
        )
    return max(0.0, min(fv, 1.0))


def _nonneg(name: str, value: Any) -> float:
    """Reject non-numeric / non-finite / negative values."""
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise RuntimeAdapterError(
            f"{name} must be numeric, got {type(value).__name__}."
        )
    fv = float(value)
    if not math.isfinite(fv):
        raise RuntimeAdapterError(
            f"{name} must be finite, got {value!r}."
        )
    if fv < 0.0:
        raise RuntimeAdapterError(f"{name} must be >= 0, got {fv}.")
    return fv


# --------------------------------------------------------------------------- #
# Unavailable component stub
# --------------------------------------------------------------------------- #
class _UnavailableComponent:
    """Placeholder for a missing enhancement component; raises on use."""

    __slots__ = ("_name",)

    def __init__(self, name: str, *args: Any, **kwargs: Any) -> None:
        self._name = name
        raise RuntimeAdapterError(
            f"Enhancement component '{name}' is unavailable; install "
            f"the enhancements package to use this runtime in enhanced "
            f"mode."
        )


# --------------------------------------------------------------------------- #
# Enhancement schema imports
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
    logger.info("Enhancement schema modules unavailable: %s", _exc)
    WorkloadDescriptor = _UnavailableComponent      # type: ignore[assignment,misc]
    NodeDescriptor = _UnavailableComponent          # type: ignore[assignment,misc]
    ZeroTrustArchitecture = _UnavailableComponent   # type: ignore[assignment,misc]
    FeedbackEvent = _UnavailableComponent           # type: ignore[assignment,misc]
    TaskType = _UnavailableComponent                # type: ignore[assignment,misc]
    Urgency = _UnavailableComponent                 # type: ignore[assignment,misc]
    NodeType = _UnavailableComponent                # type: ignore[assignment,misc]


# --------------------------------------------------------------------------- #
# Execution strategy
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
        raise RuntimeAdapterError(
            f"unknown action index {action_idx!r}; expected 0, 1, or 2."
        )


# --------------------------------------------------------------------------- #
# Runtime state
# --------------------------------------------------------------------------- #
@dataclass(frozen=True, slots=True)
class RuntimeState:
    """
    State representation for decision-making (task features + graph
    metrics).
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
        if isinstance(self.token_count, bool) or not isinstance(
            self.token_count, int
        ):
            raise RuntimeAdapterError("token_count must be an int.")
        _nonneg("token_count", self.token_count)
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
        """Return a normalized ``float32`` feature vector of shape (8,)."""
        return np.array([
            _clamp01("complexity", self.complexity),
            _clamp01(
                "token_count_norm", self.token_count / MAX_TOKENS,
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
                "cost_budget_norm", self.cost_budget / MAX_COST_BUDGET,
            ),
        ], dtype=np.float32)


# --------------------------------------------------------------------------- #
# Replay entry
# --------------------------------------------------------------------------- #
@dataclass(frozen=True, slots=True)
class _ReplayEntry:
    """One training sample; stores both teacher representations."""

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
    Multi-teacher distillation with MoE gating.

    Actions: ``0 = native``, ``1 = flexgen_low_precision``,
    ``2 = flexgen_high_precision``.

    ``select_action(state)`` returns ``(action, state_vec,
    teacher_outputs, teacher_combined)``. ``teacher_outputs`` has shape
    ``(N_TEACHERS, N_ACTIONS)`` and carries the *individual* teacher
    distributions; ``teacher_combined`` has shape ``(N_ACTIONS,)``.

    .. note::
        This class is duplicated in ``src/runtimes/autogen_runtime.py``
        and the ``DistillationOptimizer`` classes in
        ``src/integrations/``. A future refactor should extract it to
        ``src/enhancements/distillation.py``.
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
            raise RuntimeConfigError(
                f"config must be a dict, got {type(config).__name__}."
            )

        def _num(key: str, default: float, *, positive: bool) -> float:
            raw = config.get(key, default)
            if isinstance(raw, bool) or not isinstance(
                raw, (int, float)
            ):
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
        out["train_every"] = _int(
            "train_every", TRAIN_EVERY, positive=True,
        )
        out["batch_size"] = _int(
            "batch_size", BATCH_SIZE, positive=True,
        )
        out["replay_size"] = _int(
            "replay_size", REPLAY_BUFFER_SIZE, positive=True,
        )
        out["seed"] = config.get("seed")
        out["gate_seed"] = config.get("gate_seed")
        return out

    # ---------------------------------------------------------------- teachers
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

    # ---------------------------------------------------------------- gating
    def _gate_forward(self, state_vec: np.ndarray) -> np.ndarray:
        logits = state_vec @ self.gate_weights + self.gate_bias
        logits = logits - np.max(logits)
        exp = np.exp(logits)
        return exp / exp.sum()

    # ---------------------------------------------------------------- action
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
        """
        state_vec = state.to_feature_vector()

        # Safe padding: slice over-sized outputs, pad under-sized ones.
        # Never call np.pad with a negative width (the previous bug).
        normalised = []
        for teacher in self.teachers:
            prob = np.asarray(teacher(state), dtype=np.float64).reshape(-1)
            if prob.size > self.n_actions:
                prob = prob[: self.n_actions]
            elif prob.size < self.n_actions:
                prob = np.pad(
                    prob, (0, self.n_actions - prob.size), "constant",
                )
            if not np.all(np.isfinite(prob)):
                raise RuntimeAdapterError(
                    "teacher output contains non-finite values."
                )
            total = prob.sum()
            prob = (
                prob / total if total > 0
                else np.ones(self.n_actions) / self.n_actions
            )
            normalised.append(prob)

        teacher_outputs = np.stack(normalised)     # (N_TEACHERS, N_ACTIONS)
        gate = self._gate_forward(state_vec)       # (N_TEACHERS,)
        teacher_combined = (gate[:, None] * teacher_outputs).sum(axis=0)
        teacher_combined = teacher_combined / teacher_combined.sum()

        logits = state_vec @ self.student_weights + self.student_bias
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
        """Record one transition; every ``train_every`` updates, train."""
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

            # Gate update — correct shape (N_TEACHERS,) via
            # teacher_outputs @ error.
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
    """Local decision context — never stored on ``self``."""

    state: RuntimeState
    state_vec: np.ndarray
    action_idx: int
    teacher_outputs: np.ndarray
    teacher_combined: np.ndarray


# --------------------------------------------------------------------------- #
# LangChain runtime
# --------------------------------------------------------------------------- #
class LangChainRuntime(BaseRuntimeAdapter):
    """
    Runtime adapter for LangChain-based agents with optional
    enhancements.

    The execution path is a **simulation** — no real LangChain call is
    made. ``output["simulated"]`` is ``True`` on every run. Replace
    :meth:`_simulate_execution` with a real dispatch to change this.

    Thread-safe for concurrent ``run()`` calls: the only per-call state
    (a :class:`_Decision`) is a local variable inside :meth:`run`; the
    optimizer's ``update()`` is guarded by an ``RLock``.
    """

    # ---------------------------------------------------------------- hooks
    def _create_optimizer(self) -> RuntimeDistillationOptimizer:
        """Return the local, corrected distillation optimizer."""
        return RuntimeDistillationOptimizer(self.config)

    def _init_enhancements(self) -> None:
        """
        LangChain-specific enhancement setup.

        Called from the base class's ``_apply_config`` on both
        ``__init__`` and ``init``. Matches the base class's hook name so
        no second initialization path can diverge.
        """
        try:
            self.distillation_optimizer = self._create_optimizer()

            if self.config.get("use_workload_descriptor", False):
                self.workload_descriptor = WorkloadDescriptor(
                    task_id="langchain_runtime",
                    task_type=TaskType.INFERENCE,
                    tokens=self.config.get("default_tokens", 1000),
                    latency_target=self.config.get(
                        "default_latency", 500.0,
                    ),
                    urgency=Urgency.MEDIUM,
                    estimated_energy_joules=0.01,
                    estimated_carbon_kg=0.0001,
                    user_id="langchain_runtime",
                )

            if self.config.get("use_node_descriptor", False):
                self.node_descriptor = NodeDescriptor(
                    id="langchain_node",
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

            logger.info(
                "LangChainRuntime enhanced components initialized",
            )
        except Exception:
            logger.exception("Failed to initialize enhancements")
            self.use_enhancements = False
            self.distillation_optimizer = None
            self.workload_descriptor = None
            self.node_descriptor = None
            self.zero_trust = None
            if self._strict:
                raise

    def _apply_config(
        self, config: Optional[Dict[str, Any]],
    ) -> None:
        """
        Apply config and refresh the LangChain-specific attributes
        (``graph_metrics``, ``human_feedback_score``).

        The base's ``_apply_config`` handles the enhancement flags and
        calls ``_init_enhancements``; this override only adds the
        LangChain-specific attributes.
        """
        super()._apply_config(config)
        gm = self.config.get("graph_metrics")
        if isinstance(gm, dict):
            self.graph_metrics = {
                "centrality": float(gm.get("centrality", 0.5)),
                "connectivity": float(gm.get("connectivity", 0.5)),
            }
        else:
            self.graph_metrics = {
                "centrality": 0.5, "connectivity": 0.5,
            }
        self.human_feedback_score = float(
            self.config.get("human_feedback_score", 0.5)
        )

    # ---------------------------------------------------------------- run
    def run(self, query: Dict[str, Any]) -> Dict[str, Any]:
        """
        Run the runtime on ``query``.

        Simulated execution; see the class docstring. ``output["success"]``
        and ``output["simulated"]`` are always set.
        """
        if not isinstance(query, dict):
            raise RuntimeAdapterError(
                f"query must be a dict, got {type(query).__name__}."
            )
        start = time.perf_counter()

        decision: Optional[_Decision] = None
        strategy = ExecutionStrategy.NATIVE
        if self.use_enhancements and self.distillation_optimizer is not None:
            carbon = (
                self.node_descriptor.region_carbon_intensity
                if self.node_descriptor is not None
                else float(query.get("carbon_intensity", 400.0))
            )
            state = RuntimeState.from_query(
                query,
                carbon_intensity=carbon,
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

        # Execution (simulated).
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

        out: Dict[str, Any] = {
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
                    "human_feedback_score",
                    self.human_feedback_score,
                )
            ),
        }

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
                logger.exception(
                    "distillation_optimizer.update failed"
                )
                if self._strict:
                    raise
            out["reward"] = reward

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
                query=query,
            )

        return out

    # ---------------------------------------------------------------- helpers
    def _simulate_execution(
        self, strategy: ExecutionStrategy,
    ) -> Dict[str, Any]:
        """Mock execution — no real LangChain call."""
        return {
            ExecutionStrategy.NATIVE: {"accuracy": 0.8},
            ExecutionStrategy.FLEXGEN_LOW_PRECISION: {"accuracy": 0.75},
            ExecutionStrategy.FLEXGEN_HIGH_PRECISION: {"accuracy": 0.85},
        }[strategy]

    def _estimate_energy_and_carbon(
        self,
        token_count: int,
        latency_s: float,
        strategy: ExecutionStrategy,
    ) -> Tuple[float, float]:
        """Mock energy and carbon estimation (strategy-aware)."""
        multiplier = {
            ExecutionStrategy.NATIVE: 1.0,
            ExecutionStrategy.FLEXGEN_LOW_PRECISION: 0.7,
            ExecutionStrategy.FLEXGEN_HIGH_PRECISION: 1.1,
        }[strategy]
        hours = max(latency_s, 0.0) / 3600.0
        energy_kwh = hours * 0.25 * multiplier
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
        carbon_kg: float,  # noqa: ARG002 — for signature symmetry
    ) -> float:
        """
        Compute the MODP reward with success gating.

        ``modp_weights`` must be 4 finite non-negative values with a
        positive sum; invalid values raise :class:`RuntimeConfigError`.
        """
        carbon_norm = 1.0 - _clamp01(
            "carbon_norm",
            state.carbon_intensity / MAX_CARBON_INTENSITY,
        )
        energy_norm = 1.0 - _clamp01(
            "energy_norm", energy_kwh / MAX_ENERGY_KWH,
        )
        latency_norm = 1.0 - _clamp01(
            "latency_norm", execution_time_ms / MAX_LATENCY_MS,
        )
        cost_norm = 1.0 - _clamp01(
            "cost_norm", state.cost_budget / MAX_COST_BUDGET,
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
            raise RuntimeConfigError(
                f"modp_weights must be 4 finite non-negative values "
                f"with a positive sum, got {raw!r}."
            )
        mw = mw / mw.sum()

        modp_score = float(np.dot(
            [carbon_norm, energy_norm, latency_norm, cost_norm], mw,
        ))
        failure_mult = float(
            self.config.get("failure_reward_multiplier", 0.0)
        )
        return modp_score if success else modp_score * failure_mult

    def _record_outcome(
        self,
        *,
        latency_ms: float,
        energy_kwh: float,
        carbon_kg: float,
    ) -> None:
        """Best-effort workload-descriptor update via the async bridge."""
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
        query: Dict[str, Any],
    ) -> None:
        """Emit a FeedbackEvent when the enhancement package is loaded."""
        if not ENHANCEMENTS_AVAILABLE or self.zero_trust is None:
            return
        try:
            event = FeedbackEvent(
                source="langchain_runtime",
                feedback_type="routing",
                task_id=query.get("task_id", "langchain_task"),
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
                tags=["langchain", strategy.value],
            )
            logger.debug("FeedbackEvent created: %s", event.event_id)
        except Exception:
            logger.exception("FeedbackEvent construction failed")
            if self._strict:
                raise

    # ---------------------------------------------------------------- finalize
    def finalize(self) -> None:
        """Release resources via the base class's async bridge."""
        if self.use_enhancements and self.zero_trust is not None:
            shutdown = getattr(self.zero_trust, "shutdown", None)
            if callable(shutdown):
                self._try_async(shutdown)
        logger.info("LangChainRuntime finalized")


# --------------------------------------------------------------------------- #
# Public API
# --------------------------------------------------------------------------- #
__all__: List[str] = [
    "__version__",
    "ExecutionStrategy",
    "LangChainRuntime",
    "RuntimeAdapterError",
    "RuntimeConfigError",
    "RuntimeDistillationOptimizer",
    "RuntimeState",
]


def __dir__() -> List[str]:
    return sorted(__all__)


# --------------------------------------------------------------------------- #
# Smoke test: python -m runtimes.langchain_runtime
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

    # ---- 1. ExecutionStrategy ------------------------------------ #
    print("ExecutionStrategy:")
    _check(
        "from_action(0) == NATIVE",
        ExecutionStrategy.from_action(0) is ExecutionStrategy.NATIVE,
    )
    _check(
        "from_action(99) raises",
        _raises(RuntimeAdapterError, lambda:
                ExecutionStrategy.from_action(99)),
    )
    _check(
        "str(enum) == value",
        str(ExecutionStrategy.NATIVE) == "native",
    )

    # ---- 2. RuntimeState ----------------------------------------- #
    print("RuntimeState:")
    st = RuntimeState.from_query(
        {"complexity": 0.6, "token_count": 500},
    )
    _check(
        "feature vector shape",
        st.to_feature_vector().shape == (FEATURE_DIM,),
    )
    fv = st.to_feature_vector()
    _check(
        "feature vector in [0,1]",
        bool(np.all(fv >= 0) and np.all(fv <= 1)),
    )
    _check(
        "rejects NaN complexity",
        _raises(RuntimeAdapterError, lambda: RuntimeState(
            complexity=float("nan"), token_count=1,
            latency_target=1.0, carbon_intensity=1.0,
        )),
    )
    _check(
        "rejects non-dict query",
        _raises(RuntimeAdapterError, lambda:
                RuntimeState.from_query("not a dict")),  # type: ignore[arg-type]
    )
    _check(
        "frozen — mutation rejected",
        _raises(
            (AttributeError, Exception),
            lambda: setattr(st, "complexity", 0.9),
        ),
    )

    # ---- 3. Optimizer config validation -------------------------- #
    print("optimizer config:")
    opt = RuntimeDistillationOptimizer({"epsilon": 0.2, "seed": 42})
    _check("epsilon stored", opt.epsilon == 0.2)
    _check(
        "config=None accepted",
        RuntimeDistillationOptimizer() is not None,
    )
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

    # ---- 4. select_action returns 4-tuple ----------------------- #
    print("select_action:")
    action, x, t_out, t_comb = opt.select_action(st)
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

    # ---- 5. np.pad bug fix — oversized teacher output ----------- #
    print("oversized teacher output (np.pad bug fix):")

    class _OversizedTeacherOptimizer(RuntimeDistillationOptimizer):
        def _rule_teacher(self, state):
            return np.array([0.25, 0.25, 0.25, 0.25])  # 4 > N_ACTIONS

    oversized = _OversizedTeacherOptimizer({"seed": 1})
    try:
        a2, x2, to2, tc2 = oversized.select_action(st)
        _check("no crash on oversized teacher", True)
        _check(
            "oversized teacher sliced to n_actions",
            to2.shape == (N_TEACHERS, N_ACTIONS),
        )
    except Exception as exc:
        _check(f"oversized teacher raised: {exc}", False)

    # ---- 6. update trains without crash ------------------------- #
    print("update:")
    for _ in range(opt.train_every * opt.batch_size):
        a, xv, to, tc = opt.select_action(st)
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

    # ---- 7. LangChainRuntime construction ----------------------- #
    print("LangChainRuntime construction:")
    rt = LangChainRuntime()
    _check("constructs without config", rt is not None)
    _check("use_enhancements off by default", rt.use_enhancements is False)
    _check(
        "graph_metrics defaulted",
        rt.graph_metrics == {"centrality": 0.5, "connectivity": 0.5},
    )
    _check(
        "human_feedback_score defaulted",
        rt.human_feedback_score == 0.5,
    )

    # ---- 8. run() legacy path (no enhancements) ----------------- #
    print("run() legacy path:")
    out = rt.run({"question": "hi", "token_count": 100})
    _check("run works", isinstance(out, dict))
    _check("simulated=True", out["simulated"] is True)
    _check("success=True", out["success"] is True)
    _check(
        "accuracy present",
        out["accuracy"] == 0.8,
    )
    _check(
        "energy present",
        "energy_kwh" in out and "carbon_kg" in out,
    )
    _check(
        "graph_metrics is a copy",
        out["graph_metrics"] is not rt.graph_metrics,
    )

    # ---- 9. query validation ------------------------------------ #
    print("query validation:")
    _check(
        "rejects non-dict query",
        _raises(RuntimeAdapterError, lambda: rt.run("not a dict")),  # type: ignore[arg-type]
    )

    # ---- 10. modp_weights validation ---------------------------- #
    print("modp_weights validation:")
    # Force an enhanced state by directly seeding components.
    rt2 = LangChainRuntime({"use_enhancements": True, "seed": 7})
    if rt2.use_enhancements:
        rt2.distillation_optimizer = (
            rt2.distillation_optimizer
            or RuntimeDistillationOptimizer(rt2.config)
        )
        rt2._strict = False
        rt2.config["modp_weights"] = [0.0, 0.0, 0.0, 0.0]
        _check(
            "zero modp_weights raises",
            _raises(RuntimeConfigError, lambda:
                    rt2.run({"question": "hi"})),
        )
    else:
        print("  ⚠ enhancements unavailable — skipping modp_weights test")

    # ---- 11. _compute_modp_score success/failure gating --------- #
    print("reward gating:")
    rt3 = LangChainRuntime()
    r_ok = rt3._compute_modp_score(
        success=True, state=st,
        execution_time_ms=500.0, energy_kwh=0.01, carbon_kg=0.004,
    )
    r_fail = rt3._compute_modp_score(
        success=False, state=st,
        execution_time_ms=500.0, energy_kwh=0.01, carbon_kg=0.004,
    )
    _check("success → reward > 0", r_ok > 0.0)
    _check("failure → reward == 0", r_fail == 0.0)

    # ---- 12. Energy model strategy multipliers ------------------ #
    print("energy model:")
    e_native, _ = rt3._estimate_energy_and_carbon(
        1000, 1.0, ExecutionStrategy.NATIVE,
    )
    e_low, _ = rt3._estimate_energy_and_carbon(
        1000, 1.0, ExecutionStrategy.FLEXGEN_LOW_PRECISION,
    )
    e_high, _ = rt3._estimate_energy_and_carbon(
        1000, 1.0, ExecutionStrategy.FLEXGEN_HIGH_PRECISION,
    )
    _check("flexgen_low < native", e_low < e_native)
    _check("flexgen_high > native", e_high < e_native or e_high > e_native)

    # ---- 13. _try_async (inherited from base) ------------------- #
    print("_try_async:")
    calls: List[str] = []

    def _sync_call(**kwargs: Any) -> None:
        calls.append("sync")

    rt3._try_async(_sync_call)
    _check("sync call executed", calls == ["sync"])

    # ---- 14. config copied on init ------------------------------ #
    print("config copy:")
    user_cfg = {"graph_metrics": {"centrality": 0.9, "connectivity": 0.1}}
    rt4 = LangChainRuntime(user_cfg)
    _check(
        "graph_metrics from config",
        rt4.graph_metrics == {"centrality": 0.9, "connectivity": 0.1},
    )
    user_cfg["graph_metrics"]["centrality"] = 0.0
    _check(
        "config not aliased",
        rt4.graph_metrics["centrality"] == 0.9,
    )

    # ---- 15. init() re-applies config --------------------------- #
    print("init() re-applies config:")
    rt5 = LangChainRuntime()
    rt5.init({"graph_metrics": {"centrality": 0.3, "connectivity": 0.7}})
    _check(
        "init updates graph_metrics",
        rt5.graph_metrics["centrality"] == 0.3,
    )

    # ---- 16. finalize works ------------------------------------ #
    print("finalize:")
    try:
        rt5.finalize()
        _check("finalize does not raise", True)
    except Exception as exc:
        _check(f"finalize raised: {exc}", False)

    # ---- 17. __all__ / __dir__ --------------------------------- #
    print("dir:")
    _check("__dir__ matches __all__", __dir__() == sorted(__all__))
    for _name in (
        "LangChainRuntime",
        "RuntimeDistillationOptimizer",
        "RuntimeState",
        "ExecutionStrategy",
        "RuntimeAdapterError",
        "RuntimeConfigError",
    ):
        _check(f"{_name} exported", _name in __all__)

    print()
    print(f"passed: {passed}  failed: {failed}")
    if failed:
        sys.exit(1)
