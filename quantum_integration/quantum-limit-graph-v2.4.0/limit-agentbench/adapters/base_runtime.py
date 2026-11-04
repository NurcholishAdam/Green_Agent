# -*- coding: utf-8 -*-
"""
Base runtime adapter — enhanced.
=================================

Abstract base class for framework runtime adapters. Defines the interface
every runtime adapter must implement and provides the shared enhancement
pipeline (distillation optimizer, MODP reward, async-bridge helpers)
that subclasses use when ``use_enhancements=True``.

Lifecycle
---------
- ``__init__(config=None, *, strict=False)`` — configures the adapter,
  initializes enhancement components.
- ``init(config=None)`` — re-configures the adapter (idempotent). The
  base implementation updates every flag and re-initializes
  enhancements; subclasses that override ``init()`` **should** call
  ``super().init(config)`` first, then do their own setup.
- ``run(query)`` — abstract. Must return a ``dict``. Use
  :meth:`run_safe` from callers that want the return-type contract
  enforced.
- ``finalize()`` — abstract. Subclasses releasing resources (including
  an async ``zero_trust.shutdown()``) should use :meth:`_try_async`.

Key fixes over the previous version
-----------------------------------
- **The ``DistillationRoutingOptimizer`` import is gone.** It was
  imported from ``src.enhancements.schemas.node_descriptor`` — a schema
  module that does not define it — so ``use_enhancements=True`` could
  never produce a working runtime. The canonical
  :class:`DistillationOptimizer` is now defined at module level,
  matching the enhanced ``src/integrations/base_adapter.py``.
- **``init(config)`` recomputes all flags and re-initializes
  enhancements.** Previously ``__init__`` and ``init()`` set different
  things and the two-phase initialization diverged:
  ``MyRuntime({"use_enhancements": False}); rt.init({"use_enhancements":
  True})`` silently stayed in legacy mode.
- **Config is copied and validated.** ``config="not a dict"`` now
  raises :class:`RuntimeConfigError` at the entry point instead of
  ``AttributeError`` deep inside ``.get(...)``.
- **Full rollback on enhancement init failure.** Every component is
  reset to ``None``; ``strict=True`` re-raises. Previously the rollback
  left ``workload_descriptor`` / ``node_descriptor`` / ``zero_trust``
  stale.
- **``_UnavailableComponent`` replaces ``pass``-body dummies.** A
  missing enhancement fails loudly at construction instead of silently
  producing an object with no state.
- **Return-type contract.** ``run_safe()`` validates that ``run()``
  returns a ``dict`` and raises :class:`RuntimeAdapterError` otherwise;
  the abstract ``run()`` docstring states the contract.
- **Async bridge.** :meth:`_try_async` handles sync-first,
  running-loop, and ``asyncio.run()`` — with ``logger.exception`` on
  every failure path. Subclasses use this from ``finalize()``.
- **``track_green_metrics`` has a helper.** :meth:`_maybe_track_metrics`
  adds ``energy_kwh`` / ``carbon_kg`` to an output dict when the flag is
  set; subclasses call it from their ``run()``.
- **``get_metadata()`` keys renamed.** ``distillation_optimizer`` (was
  a bool) → ``has_distillation_optimizer``; adds ``adapter_version``.
- **``strict`` mode.** Keyword-only on ``__init__``; propagates to
  enhancement initialization. Raises on failure instead of silently
  downgrading to legacy.
- **Narrow error types** — :class:`RuntimeAdapterError`,
  :class:`RuntimeConfigError`.
- Lazy ``%s`` logging; ``logger.exception`` on caught exceptions.
- ``__version__``, ``__all__``, ``__dir__``, and a ``_check()``-based
  smoke test that survives ``python -O``.

Future refactor
---------------
:class:`DistillationOptimizer` (and its helpers ``_clamp01``,
``_nonneg``, ``NormalizedFeatures``, ``_ReplayEntry``) should move to
``src/enhancements/distillation.py`` alongside the identical class in
``src/integrations/base_adapter.py``. Both files currently define the
same class locally; extracting it would eliminate the duplication and
guarantee that future fixes land in one place.

Compatibility
-------------
- ``BaseRuntimeAdapter(config=None)`` — signature unchanged.
  ``strict=False`` is new and keyword-only.
- ``init(config)`` — now does real work (flag recomputation,
  enhancement re-init). Subclasses that already override ``init()``
  without calling ``super().init()`` keep their existing behaviour but
  won't get the base's re-init logic — that's their responsibility.
- ``run(query)`` / ``finalize()`` — still abstract. Subclasses unchanged.
- ``get_metadata()`` — ``"distillation_optimizer": bool`` is renamed
  to ``"has_distillation_optimizer": bool``. Callers reading the old
  key must update.
- ``self.distillation_optimizer`` is now a :class:`DistillationOptimizer`
  instance (module-level class), not an instance of the undefined
  ``DistillationRoutingOptimizer``. Any subclass that checks
  ``type(self.distillation_optimizer).__name__`` sees the new name.
- New: :meth:`run_safe`, :meth:`_try_async`, :meth:`_maybe_track_metrics`,
  :class:`RuntimeAdapterError`, :class:`RuntimeConfigError`,
  :class:`ExecutionStrategy`, :class:`DistillationOptimizer`,
  :class:`NormalizedFeatures`, ``__version__``.

Requires Python 3.8+. ``slots=True`` on dataclasses requires 3.10+;
drop it for 3.8/3.9.
"""

from __future__ import annotations

import asyncio
import logging
import math
import random
import threading
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
class RuntimeAdapterError(ValueError):
    """Raised for invalid runtime-adapter inputs or execution failures."""


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
        raise RuntimeAdapterError(
            f"Enhancement component '{name}' is unavailable; install "
            f"the enhancements package to use this runtime in enhanced "
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
    logger.info("Enhancement schema modules unavailable: %s", _exc)
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
# Normalized features
# --------------------------------------------------------------------------- #
@dataclass(frozen=True, slots=True)
class NormalizedFeatures:
    """
    Named, normalized task features in ``[0, 1]``.

    Eliminates the magic indices (``state_vec[3]``, ``state_vec[7]``)
    that bind downstream reward computation to the exact ordering of the
    feature vector.
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
# Replay entry
# --------------------------------------------------------------------------- #
@dataclass(frozen=True, slots=True)
class _ReplayEntry:
    """
    One training sample for the distillation optimizer.

    ``teacher_outputs`` (shape ``(N_TEACHERS, N_ACTIONS)``) is the
    individual per-teacher distribution; ``teacher_combined`` (shape
    ``(N_ACTIONS,)``) is the gated mixture at decision time.
    """

    state_vec: np.ndarray
    action: int
    reward: float
    teacher_outputs: np.ndarray
    teacher_combined: np.ndarray


# --------------------------------------------------------------------------- #
# Distillation optimizer
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

    .. note::
        This class duplicates ``DistillationOptimizer`` in
        ``src/integrations/base_adapter.py``. A future refactor should
        move both to ``src/enhancements/distillation.py``.
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

            one_hot = np.zeros(self.n_actions)
            one_hot[entry.action] = 1.0
            grad_distill = -(entry.teacher_combined - cur_probs)
            grad_rl = -entry.reward * (one_hot - cur_probs)
            grad = self.distill_w * grad_distill + self.rl_w * grad_rl
            self.student_weights -= self.lr * np.outer(s, grad)
            self.student_bias -= self.lr * grad

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
# Base runtime adapter
# --------------------------------------------------------------------------- #
class BaseRuntimeAdapter(ABC):
    """
    Abstract base class for runtime adapters.

    Provides a unified interface for running agents on different
    frameworks. When ``use_enhancements`` is True and the enhanced
    modules are available, the adapter optionally:

    - Uses a distillation optimizer (with MoE gating) to select an
      execution strategy (native framework vs FlexGen low/high
      precision).
    - Computes multi-objective rewards (MODP) after execution.
    - Incorporates RLHF feedback via state features.
    - Integrates LIMIT Graph metrics into the decision state.
    - Emits ``FeedbackEvent`` for cross-module learning.

    The ``strict`` flag (keyword-only) controls whether integration
    failures raise (``strict=True``) or log (``strict=False``,
    default — matching legacy behaviour).
    """

    def __init__(
        self,
        config: Optional[Dict[str, Any]] = None,
        *,
        strict: bool = False,
    ) -> None:
        """
        Initialize the runtime adapter.

        Parameters
        ----------
        config : dict, optional
        strict : bool, default False
            If True, raise on enhancement initialization failures.
        """
        self._strict: bool = bool(strict)
        self._apply_config(config)

        logger.info(
            "BaseRuntimeAdapter initialized (enhancements=%s)",
            "on" if self.use_enhancements else "off",
        )

    # ---------------------------------------------------------------- config
    def _apply_config(
        self, config: Optional[Dict[str, Any]],
    ) -> None:
        """
        Validate, copy, and apply ``config``.

        Called from ``__init__`` and ``init()``; recomputes every flag
        and re-initializes enhancement components.
        """
        if config is not None and not isinstance(config, dict):
            raise RuntimeConfigError(
                f"config must be a dict or None, got "
                f"{type(config).__name__}."
            )
        self.config: Dict[str, Any] = dict(config) if config else {}
        self.use_enhancements: bool = (
            bool(self.config.get("use_enhancements", False))
            and ENHANCEMENTS_AVAILABLE
        )
        self.track_green_metrics: bool = bool(
            self.config.get("track_green_metrics", True)
        )

        # Reset components so re-init is clean.
        self.workload_descriptor: Any = None
        self.node_descriptor: Any = None
        self.zero_trust: Any = None
        self.distillation_optimizer: Optional[DistillationOptimizer] = None

        if self.use_enhancements:
            self._init_enhancements()

    # ---------------------------------------------------------------- init
    def init(self, config: Optional[Dict[str, Any]] = None) -> None:
        """
        (Re-)configure the runtime.

        Subclasses that override this method **should** call
        ``super().init(config)`` before their own setup so the base
        class handles flag recomputation and enhancement
        re-initialization.
        """
        self._apply_config(config)

    # ---------------------------------------------------------------- enhance
    def _init_enhancements(self) -> None:
        """
        Initialize enhanced components.

        In ``strict`` mode, a failure re-raises; otherwise logs and
        rolls back all components.
        """
        try:
            self.distillation_optimizer = self._create_optimizer()

            if self.config.get("use_workload_descriptor", False):
                self.workload_descriptor = WorkloadDescriptor(
                    task_id=self.config.get("task_id", "default_task"),
                    task_type=TaskType.INFERENCE,
                    tokens=self.config.get("default_tokens", 1000),
                    latency_target=self.config.get("default_latency", 500.0),
                    urgency=Urgency.MEDIUM,
                    estimated_energy_joules=0.01,
                    estimated_carbon_kg=0.0001,
                    user_id="base_runtime",
                    metadata={"adapter": "base_runtime"},
                )

            if self.config.get("use_node_descriptor", False):
                self.node_descriptor = NodeDescriptor(
                    id="runtime_node",
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

    def _create_optimizer(self) -> DistillationOptimizer:
        """
        Construct the distillation optimizer.

        Override in a subclass to swap implementations. The default
        returns the module-level :class:`DistillationOptimizer`.
        """
        return DistillationOptimizer(self.config)

    # ---------------------------------------------------------------- abstract
    @abstractmethod
    def run(self, query: Dict[str, Any]) -> Dict[str, Any]:
        """
        Execute a single query and return result metrics.

        **The return value must be a ``dict``.** A subclass that returns
        ``None`` or a non-dict will be caught by :meth:`run_safe`; a
        direct caller of ``run()`` sees the raw return.
        """

    @abstractmethod
    def finalize(self) -> None:
        """Release resources. Called by the runtime framework on shutdown."""

    # ---------------------------------------------------------------- helpers
    def run_safe(self, query: Dict[str, Any]) -> Dict[str, Any]:
        """
        Call :meth:`run` and validate the return type.

        Raises :class:`RuntimeAdapterError` when ``run()`` returns a
        non-dict.
        """
        result = self.run(query)
        if not isinstance(result, dict):
            raise RuntimeAdapterError(
                f"{type(self).__name__}.run must return a dict, got "
                f"{type(result).__name__}."
            )
        return result

    def _maybe_track_metrics(
        self,
        output: Dict[str, Any],
        *,
        execution_time_s: float,
        token_count: int,
    ) -> None:
        """
        Add energy / carbon estimates to ``output`` when
        ``self.track_green_metrics`` is set.

        Subclasses call this from their ``run()`` implementation.
        """
        if not self.track_green_metrics:
            return
        energy_kwh = (execution_time_s / 3600.0) * 0.25
        carbon_kg = energy_kwh * 0.4
        token_scale = 1.0 + max(token_count, 0) / 10_000.0
        output["energy_kwh"] = energy_kwh * token_scale
        output["carbon_kg"] = carbon_kg * token_scale

    @staticmethod
    def _try_async(method: Any, **kwargs: Any) -> None:
        """
        Call ``method`` — sync, schedule on the running loop, or via
        ``asyncio.run``.

        Never raises unless the underlying call does; every failure
        path is ``logger.exception``-ed. Subclasses use this from
        ``finalize()`` (e.g. for ``zero_trust.shutdown()``).
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
                BaseRuntimeAdapter._run_coroutine(coro)
            return
        except Exception:
            logger.exception("async bridge: sync call failed")
            return
        if asyncio.iscoroutine(result):
            BaseRuntimeAdapter._run_coroutine(result)

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

    # ---------------------------------------------------------------- metadata
    def get_metadata(self) -> Dict[str, Any]:
        """Return metadata about the runtime adapter and enhancement status."""
        meta: Dict[str, Any] = {
            "adapter_type": type(self).__name__,
            "adapter_version": __version__,
            "enhancements_enabled": self.use_enhancements,
        }
        if self.use_enhancements:
            meta["has_distillation_optimizer"] = (
                self.distillation_optimizer is not None
            )
            meta["use_workload_descriptor"] = (
                self.workload_descriptor is not None
            )
            meta["use_node_descriptor"] = (
                self.node_descriptor is not None
            )
            meta["use_zero_trust"] = self.zero_trust is not None
        return meta


# --------------------------------------------------------------------------- #
# Public API
# --------------------------------------------------------------------------- #
__all__: List[str] = [
    "__version__",
    "BaseRuntimeAdapter",
    "DistillationOptimizer",
    "ExecutionStrategy",
    "NormalizedFeatures",
    "RuntimeAdapterError",
    "RuntimeConfigError",
]


def __dir__() -> List[str]:
    return sorted(__all__)


# --------------------------------------------------------------------------- #
# Smoke test: python -m runtimes.base_runtime
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

    # ---- 1. Concrete subclass for testing ------------------------ #
    print("concrete subclass:")

    class _FakeRuntime(BaseRuntimeAdapter):
        def __init__(self, config=None, *, strict=False):
            self._init_order: List[str] = []
            super().__init__(config, strict=strict)

        def init(self, config=None):
            self._init_order.append("init")
            super().init(config)

        def run(self, query):
            if not isinstance(query, dict):
                raise RuntimeAdapterError("query must be a dict")
            self._init_order.append("run")
            return {"answer": "ok", "accuracy": 0.9}

        def finalize(self):
            self._init_order.append("finalize")

    # ---- 2. Basic construction ---------------------------------- #
    print("construction:")
    rt = _FakeRuntime()
    _check("_apply_config ran", rt.config == {})
    _check("enhancements off by default", rt.use_enhancements is False)
    _check(
        "track_green_metrics on by default",
        rt.track_green_metrics is True,
    )
    _check("optimizer None when enhancements off",
           rt.distillation_optimizer is None)

    # ---- 3. init() re-applies config ---------------------------- #
    print("init() re-applies config:")
    rt2 = _FakeRuntime()
    rt2.init({"track_green_metrics": False})
    _check(
        "init sets track_green_metrics",
        rt2.track_green_metrics is False,
    )
    _check("init order recorded", rt2._init_order == ["init"])

    # ---- 4. Enhancements honoured on init() -------------------- #
    print("enhancements via init():")
    rt3 = _FakeRuntime()
    rt3.init({"use_enhancements": True})
    _check("use_enhancements set by init", rt3.use_enhancements is True)
    _check(
        "distillation_optimizer constructed",
        isinstance(rt3.distillation_optimizer, DistillationOptimizer),
    )

    # ---- 5. Re-init resets state ------------------------------- #
    print("re-init resets state:")
    rt3.init({"use_enhancements": False})
    _check(
        "use_enhancements cleared",
        rt3.use_enhancements is False,
    )
    _check(
        "distillation_optimizer reset to None",
        rt3.distillation_optimizer is None,
    )

    # ---- 6. Config validation ---------------------------------- #
    print("config validation:")
    _check(
        "rejects non-dict config in __init__",
        _raises(RuntimeConfigError, lambda:
                _FakeRuntime("not a dict")),
    )
    _check(
        "rejects non-dict config in init",
        _raises(RuntimeConfigError, lambda:
                rt2.init("not a dict")),
    )

    # ---- 7. run_safe validates return type --------------------- #
    print("run_safe:")
    rt4 = _FakeRuntime()
    out = rt4.run_safe({"q": "hi"})
    _check("run_safe returns the dict", out["answer"] == "ok")

    class _BadRuntime(BaseRuntimeAdapter):
        def run(self, query):
            return None  # type: ignore[return-value]

        def finalize(self):
            pass

    bad = _BadRuntime()
    _check(
        "run_safe rejects None return",
        _raises(RuntimeAdapterError, lambda:
                bad.run_safe({"q": "hi"})),
    )

    # ---- 8. Optimizer config validation ------------------------ #
    print("optimizer config validation:")
    opt = DistillationOptimizer({"epsilon": 0.2, "seed": 42})
    _check("epsilon stored", opt.epsilon == 0.2)
    _check(
        "rejects epsilon='high'",
        _raises(RuntimeConfigError, lambda:
                DistillationOptimizer({"epsilon": "high"})),
    )
    _check(
        "rejects negative lr",
        _raises(RuntimeConfigError, lambda:
                DistillationOptimizer({"distillation_lr": -1})),
    )

    # ---- 9. Optimizer shapes ---------------------------------- #
    print("optimizer shapes:")
    x = np.zeros(FEATURE_DIM, dtype=np.float32)
    action, x_out, t_out, t_comb = opt.select_action(x)
    _check("action in range", 0 <= action < N_ACTIONS)
    _check("state_vec preserved", np.array_equal(x, x_out))
    _check(
        "teacher_outputs shape",
        t_out.shape == (N_TEACHERS, N_ACTIONS),
    )
    _check(
        "teacher_combined shape",
        t_comb.shape == (N_ACTIONS,),
    )

    # Train a few steps to verify no crash.
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
        _raises(RuntimeAdapterError, lambda:
                opt.update(x, 0, float("nan"), t_out, t_comb)),
    )

    # ---- 10. _try_async sync path ------------------------------ #
    print("_try_async:")
    calls: List[str] = []

    def _sync_call(**kwargs: Any) -> None:
        calls.append(f"sync:{kwargs}")

    BaseRuntimeAdapter._try_async(_sync_call, foo="bar")
    _check("sync call executed", calls == ["sync:{'foo': 'bar'}"])

    async def _async_call(**kwargs: Any) -> None:
        calls.append(f"async:{kwargs}")

    BaseRuntimeAdapter._try_async(_async_call, foo="baz")
    _check(
        "async call executed",
        any("async:" in c for c in calls),
    )

    # ---- 11. _maybe_track_metrics ------------------------------ #
    print("_maybe_track_metrics:")
    rt5 = _FakeRuntime({"track_green_metrics": False})
    out: Dict[str, Any] = {"answer": "x"}
    rt5._maybe_track_metrics(out, execution_time_s=1.0, token_count=1000)
    _check("no metrics when disabled", "energy_kwh" not in out)

    rt6 = _FakeRuntime({"track_green_metrics": True})
    out2: Dict[str, Any] = {"answer": "x"}
    rt6._maybe_track_metrics(out2, execution_time_s=1.0, token_count=1000)
    _check("energy present when enabled", "energy_kwh" in out2)
    _check("carbon present when enabled", "carbon_kg" in out2)

    # ---- 12. get_metadata ------------------------------------- #
    print("get_metadata:")
    meta = rt4.get_metadata()
    _check(
        "adapter_version == __version__",
        meta["adapter_version"] == __version__,
    )
    _check(
        "has_distillation_optimizer key",
        "has_distillation_optimizer" in meta,
    )
    _check(
        "distillation_optimizer key removed",
        "distillation_optimizer" not in meta,
    )

    # ---- 13. __all__ / __dir__ --------------------------------- #
    print("dir:")
    _check("__dir__ matches __all__", __dir__() == sorted(__all__))
    for _name in (
        "BaseRuntimeAdapter",
        "DistillationOptimizer",
        "ExecutionStrategy",
        "NormalizedFeatures",
        "RuntimeAdapterError",
        "RuntimeConfigError",
    ):
        _check(f"{_name} exported", _name in __all__)

    # ---- 14. Error hierarchy ---------------------------------- #
    print("error hierarchy:")
    _check(
        "RuntimeConfigError subclasses RuntimeAdapterError",
        issubclass(RuntimeConfigError, RuntimeAdapterError),
    )
    _check(
        "RuntimeAdapterError subclasses ValueError",
        issubclass(RuntimeAdapterError, ValueError),
    )

    print()
    print(f"passed: {passed}  failed: {failed}")
    if failed:
        sys.exit(1)
