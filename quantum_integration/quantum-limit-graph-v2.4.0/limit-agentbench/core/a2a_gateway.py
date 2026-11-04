# -*- coding: utf-8 -*-
"""
A2A Protocol Gateway — AgentBeats Compliance Layer (Enhanced)
============================================================

Validates and transforms agent I/O to a task-completion payload, with
optional adaptive transformation-strategy selection.

**Honest A2A compliance note**
-----------------------------
The internal :class:`A2AResponse` dataclass is **not** wire-compatible
with the real Agent-to-Agent (A2A) protocol: the internal ``TaskStatus``
values (``success`` / ``failure`` / …) are a simplification of the A2A
task states (``completed`` / ``failed`` / …), and the internal payload
does not carry the JSON-RPC 2.0 envelope required on the A2A wire.

The :meth:`A2AResponse.to_jsonrpc_response` method produces a
**wire-compatible** A2A JSON-RPC response. Callers who need to speak to
a real A2A endpoint should:

1. Build an :class:`A2AResponse` via the gateway.
2. Emit :meth:`A2AResponse.to_jsonrpc_response` (passing the original
   request ``id``) over the transport.

Callers who only need the internal status shape can keep using
:meth:`A2AResponse.to_dict`.

Enhancements (enabled via ``config["use_enhancements"]=True``)
-------------------------------------------------------------
- **Adaptive transformation strategy.** A distillation student trained
  from three teachers (rule-based, RLHF, historical) with MoE gating
  chooses between standard / compressed / detailed output per request.
- **MODP reward.** Per-request reward is the weighted sum of a latency
  score and an information-preservation score derived from the
  transformation's actual outputs. Not a constant.
- **MODP score in the response.** The reward is surfaced on
  ``A2AResponse.modp_score`` — a consumer can rank transformations.
- **Corrected distillation optimizer.** 4-tuple ``select_action``;
  5-arg ``update``; per-instance RNGs; config validated at construction;
  ``update`` under an ``RLock``; over-sized teacher outputs sliced
  before any ``np.pad``; gate gradient via ``teacher_outputs @ error``.
- **Strict mode.** ``A2AGateway(..., strict=True)`` raises on
  enhancement initialization or execution failures instead of silently
  degrading.
- **Frozen, validated dataclasses.** :class:`A2ARequest`,
  :class:`A2AResponse`, and :class:`A2ADecisionState` reject malformed
  fields at construction; both request and response round-trip via
  ``to_dict`` / ``from_dict``.
- **Thread-safe counters.** ``request_count`` and ``error_count`` are
  incremented under a lock.
- **Narrow error type.** :class:`A2AGatewayError` replaces the previous
  broad ``ValueError`` / ``AttributeError`` leaks.
- **Real compression.** The ``compressed`` strategy picks one canonical
  value and trims it to a documented maximum — not ``str(output)[:200]``.
- **Aware timestamps.** ``datetime.now(timezone.utc).isoformat()`` —
  the previous naive ``datetime.utcnow()`` broke comparison across
  processes with mixed timezones.

Compatibility
-------------
- ``A2AGateway(default_version=A2AVersion.V1_1, config=None)`` — same
  positional signature. ``strict=False`` is new and keyword-only.
- ``A2AGateway.transform_agent_output(...)`` — same signature. The
  return is now a validated :class:`A2AResponse` with ``modp_score``
  populated (a real number, not ``0.7``).
- ``A2AResponse.to_dict()`` — same keys, plus ``modp_score`` /
  ``distillation_stats`` when present. Failure responses now carry
  ``modp_score`` too.
- ``A2ARequest`` and ``A2AResponse`` are frozen and validated; direct
  attribute mutation raises.
- ``A2ADistillationOptimizer.select_action`` returns 4 values
  (``action, state_vec, teacher_outputs, teacher_combined``);
  ``update`` takes 5 args (no ``next_state_vec``).
- ``create_a2a_task(...)`` now returns an :class:`A2ARequest`
  instance, not a raw ``dict``. Callers that passed its output straight
  into ``gateway.validate_request(...)`` should pass it as
  ``request_data`` via ``request.to_dict()``, or use the new
  :meth:`A2AGateway.submit` shortcut.
- :meth:`A2AResponse.to_jsonrpc_response(request_id, ...)` is new and
  produces the actual A2A wire format.

Requires Python 3.8+. ``slots=True`` on dataclasses requires 3.10+;
drop ``slots=True`` for 3.8/3.9.
"""

from __future__ import annotations

import enum
import json
import logging
import math
import random
import threading
from collections import deque
from dataclasses import dataclass, field, replace
from datetime import datetime, timezone
from typing import (
    Any, Deque, Dict, Final, List, Mapping, Optional, Tuple,
)

import numpy as np


logger = logging.getLogger(__name__)

__version__ = "2.5.0"


# --------------------------------------------------------------------------- #
# Errors
# --------------------------------------------------------------------------- #
class A2AGatewayError(ValueError):
    """Raised for invalid A2A gateway inputs or execution failures."""


# --------------------------------------------------------------------------- #
# Constants
# --------------------------------------------------------------------------- #
# Latency ceiling used for reward normalisation. A request that takes
# longer than this is treated as "zero latency score".
MAX_LATENCY_S: Final[float] = 5.0

# Compression target for the "compressed" strategy.
COMPRESS_MAX_CHARS: Final[int] = 200

# Decision feature vector: task_type_len(1) + input_size(1)
# + has_constraints(1) + centrality(1) + connectivity(1) + human(1)
# + version_num(1) = 7.
A2A_FEATURE_DIM: Final[int] = 7
A2A_N_ACTIONS: Final[int] = 3

DEFAULT_REPLAY_SIZE: Final[int] = 2000
DEFAULT_TRAIN_EVERY: Final[int] = 10
DEFAULT_BATCH_SIZE: Final[int] = 8

# Supported top-level A2A versions (informational; the wire format for
# `to_jsonrpc_response` always emits v1.1-compatible fields).
SUPPORTED_VERSIONS: Final[Tuple[str, ...]] = ("1.0", "1.1")


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
# Enums
# --------------------------------------------------------------------------- #
class A2AVersion(StrEnum):
    """A2A protocol versions supported by this gateway."""

    V1_0 = "1.0"
    V1_1 = "1.1"

    @classmethod
    def from_str(cls, raw: Any) -> "A2AVersion":
        try:
            return cls(str(raw))
        except ValueError as exc:
            raise A2AGatewayError(
                f"unsupported A2A version {raw!r}; expected one of "
                f"{list(SUPPORTED_VERSIONS)}."
            ) from exc


class TaskStatus(StrEnum):
    """
    Internal task-completion status.

    These are the statuses this gateway uses internally. They are
    **not** the A2A wire statuses; use :meth:`A2AResponse.to_jsonrpc_response`
    for the A2A-compliant mapping via :meth:`a2a_state`.
    """

    SUCCESS = "success"
    FAILURE = "failure"
    TIMEOUT = "timeout"
    OOM = "out_of_memory"
    CRASH = "crash"
    INVALID_OUTPUT = "invalid_output"

    def a2a_state(self) -> str:
        """Map the internal status to the A2A wire state string."""
        return {
            TaskStatus.SUCCESS: "completed",
            TaskStatus.FAILURE: "failed",
            TaskStatus.TIMEOUT: "failed",
            TaskStatus.OOM: "failed",
            TaskStatus.CRASH: "failed",
            TaskStatus.INVALID_OUTPUT: "failed",
        }[self]


# --------------------------------------------------------------------------- #
# Validation helpers
# --------------------------------------------------------------------------- #
def _require_nonempty_str(name: str, value: Any) -> str:
    if not isinstance(value, str) or not value:
        raise A2AGatewayError(f"{name} must be a non-empty string.")
    return value


def _require_mapping(name: str, value: Any) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise A2AGatewayError(
            f"{name} must be a Mapping, got {type(value).__name__}."
        )
    return value


def _require_optional_mapping(
    name: str, value: Any,
) -> Optional[Mapping[str, Any]]:
    if value is None:
        return None
    return _require_mapping(name, value)


def _require_optional_float_mapping(
    name: str, value: Any,
) -> Optional[Dict[str, float]]:
    if value is None:
        return None
    m = _require_mapping(name, value)
    out: Dict[str, float] = {}
    for k, v in m.items():
        if not isinstance(k, str) or not k:
            raise A2AGatewayError(
                f"{name} keys must be non-empty strings, got {k!r}."
            )
        if isinstance(v, bool) or not isinstance(v, (int, float)):
            raise A2AGatewayError(
                f"{name}[{k!r}] must be numeric, got {type(v).__name__}."
            )
        fv = float(v)
        if not math.isfinite(fv):
            raise A2AGatewayError(
                f"{name}[{k!r}] must be finite, got {v!r}."
            )
        out[k] = fv
    return out


def _require_optional_unit_interval(
    name: str, value: Any,
) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise A2AGatewayError(f"{name} must be numeric.")
    fv = float(value)
    if not math.isfinite(fv) or not 0.0 <= fv <= 1.0:
        raise A2AGatewayError(
            f"{name} must be finite and in [0, 1], got {value!r}."
        )
    return fv


def _require_optional_nonneg(name: str, value: Any) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise A2AGatewayError(f"{name} must be numeric.")
    fv = float(value)
    if not math.isfinite(fv) or fv < 0.0:
        raise A2AGatewayError(
            f"{name} must be finite and >= 0, got {value!r}."
        )
    return fv


def _clamp01(value: float) -> float:
    return max(0.0, min(float(value), 1.0))


# --------------------------------------------------------------------------- #
# Optional enhancement imports
# --------------------------------------------------------------------------- #
class _UnavailableComponent:
    """Placeholder for an unavailable enhancement component; raises on use."""

    __slots__ = ("_name",)

    def __init__(self, name: str, *a: Any, **kw: Any) -> None:
        self._name = name
        raise A2AGatewayError(
            f"Enhancement component '{name}' is unavailable; install "
            f"the enhancements package to enable enhancements."
        )


try:
    from src.enhancements.schemas.node_descriptor import (
        NodeDescriptor,
        NodeType,
    )
    from src.enhancements.schemas.workload_descriptor import (
        WorkloadDescriptor,
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
    ZeroTrustArchitecture = _UnavailableComponent  # type: ignore[assignment,misc]
    FeedbackEvent = _UnavailableComponent  # type: ignore[assignment,misc]


# --------------------------------------------------------------------------- #
# Records
# --------------------------------------------------------------------------- #
@dataclass(frozen=True, slots=True)
class A2ARequest:
    """Immutable A2A task request with optional enhancement fields."""

    task_id: str
    task_type: str
    input_data: Dict[str, Any]
    constraints: Optional[Dict[str, Any]] = None
    version: str = "1.1"
    graph_metrics: Optional[Dict[str, float]] = None
    human_feedback_score: Optional[float] = None
    modp_weights: Optional[Dict[str, float]] = None

    def __post_init__(self) -> None:
        _require_nonempty_str("task_id", self.task_id)
        _require_nonempty_str("task_type", self.task_type)
        _require_mapping("input_data", self.input_data)
        _require_optional_mapping("constraints", self.constraints)
        _require_optional_float_mapping(
            "graph_metrics", self.graph_metrics,
        )
        _require_optional_unit_interval(
            "human_feedback_score", self.human_feedback_score,
        )
        _require_optional_float_mapping(
            "modp_weights", self.modp_weights,
        )
        if self.version not in SUPPORTED_VERSIONS:
            raise A2AGatewayError(
                f"version {self.version!r} is not supported; "
                f"expected one of {list(SUPPORTED_VERSIONS)}."
            )

    def validate(self) -> bool:
        """
        Return ``True`` — the ``__post_init__`` validation above already
        rejects every malformed field. Retained for backward
        compatibility with callers who probe ``request.validate()``.
        """
        return True

    def to_dict(self) -> Dict[str, Any]:
        out: Dict[str, Any] = {
            "task_id": self.task_id,
            "task_type": self.task_type,
            "input_data": dict(self.input_data),
            "version": self.version,
        }
        if self.constraints is not None:
            out["constraints"] = dict(self.constraints)
        if self.graph_metrics is not None:
            out["graph_metrics"] = dict(self.graph_metrics)
        if self.human_feedback_score is not None:
            out["human_feedback_score"] = self.human_feedback_score
        if self.modp_weights is not None:
            out["modp_weights"] = dict(self.modp_weights)
        return out

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "A2ARequest":
        if not isinstance(data, Mapping):
            raise A2AGatewayError(
                "A2ARequest.from_dict expects a Mapping."
            )
        try:
            return cls(
                task_id=str(data["task_id"]),
                task_type=str(data["task_type"]),
                input_data=dict(data["input_data"]),
                constraints=(
                    None if data.get("constraints") is None
                    else dict(data["constraints"])
                ),
                version=str(data.get("version", "1.1")),
                graph_metrics=(
                    None if data.get("graph_metrics") is None
                    else dict(data["graph_metrics"])
                ),
                human_feedback_score=(
                    None if data.get("human_feedback_score") is None
                    else float(data["human_feedback_score"])
                ),
                modp_weights=(
                    None if data.get("modp_weights") is None
                    else dict(data["modp_weights"])
                ),
            )
        except A2AGatewayError:
            raise
        except (KeyError, TypeError, ValueError) as exc:
            raise A2AGatewayError(
                f"invalid A2ARequest payload: {exc}"
            ) from exc


@dataclass(frozen=True, slots=True)
class A2AResponse:
    """
    Immutable A2A task response with optional enhancement fields.

    ``to_dict()`` returns the internal simplified shape; for a
    wire-compatible A2A JSON-RPC response use
    :meth:`to_jsonrpc_response`.
    """

    task_id: str
    status: TaskStatus
    output: Optional[Dict[str, Any]] = None
    error_message: Optional[str] = None
    execution_time: Optional[float] = None
    green_metrics: Optional[Dict[str, Any]] = None
    reasoning_trace: Optional[List[Dict[str, Any]]] = None
    metadata: Optional[Dict[str, Any]] = None
    version: str = "1.1"
    graph_metrics: Optional[Dict[str, float]] = None
    modp_score: Optional[float] = None
    human_feedback_score: Optional[float] = None
    distillation_stats: Optional[Dict[str, Any]] = None

    def __post_init__(self) -> None:
        _require_nonempty_str("task_id", self.task_id)
        if not isinstance(self.status, TaskStatus):
            try:
                object.__setattr__(
                    self, "status", TaskStatus(str(self.status).lower()),
                )
            except ValueError as exc:
                raise A2AGatewayError(
                    f"invalid status {self.status!r}."
                ) from exc
        _require_optional_mapping("output", self.output)
        _require_optional_mapping("green_metrics", self.green_metrics)
        _require_optional_mapping("metadata", self.metadata)
        _require_optional_mapping(
            "distillation_stats", self.distillation_stats,
        )
        _require_optional_float_mapping(
            "graph_metrics", self.graph_metrics,
        )
        _require_optional_nonneg("execution_time", self.execution_time)
        _require_optional_unit_interval(
            "modp_score", self.modp_score,
        )
        _require_optional_unit_interval(
            "human_feedback_score", self.human_feedback_score,
        )
        if self.version not in SUPPORTED_VERSIONS:
            raise A2AGatewayError(
                f"version {self.version!r} is not supported."
            )
        if self.error_message is not None and not isinstance(
            self.error_message, str
        ):
            raise A2AGatewayError("error_message must be a string or None.")
        if self.reasoning_trace is not None:
            if not isinstance(self.reasoning_trace, list):
                raise A2AGatewayError(
                    "reasoning_trace must be a list or None."
                )

    # ---------------------------------------------------------------- serial
    def to_dict(self) -> Dict[str, Any]:
        result: Dict[str, Any] = {
            "task_id": self.task_id,
            "status": self.status.value,
            "version": self.version,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
        if self.output is not None:
            result["output"] = dict(self.output)
        if self.error_message:
            result["error"] = self.error_message
        if self.execution_time is not None:
            result["execution_time_seconds"] = float(self.execution_time)
        if self.green_metrics:
            result["green_metrics"] = dict(self.green_metrics)
        if self.reasoning_trace:
            result["reasoning_trace"] = list(self.reasoning_trace)
        if self.metadata:
            result["metadata"] = dict(self.metadata)
        if self.graph_metrics:
            result["graph_metrics"] = dict(self.graph_metrics)
        if self.modp_score is not None:
            result["modp_score"] = self.modp_score
        if self.human_feedback_score is not None:
            result["human_feedback_score"] = self.human_feedback_score
        if self.distillation_stats:
            result["distillation_stats"] = dict(self.distillation_stats)
        return result

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "A2AResponse":
        if not isinstance(data, Mapping):
            raise A2AGatewayError(
                "A2AResponse.from_dict expects a Mapping."
            )
        try:
            return cls(
                task_id=str(data["task_id"]),
                status=TaskStatus(str(data["status"]).lower()),
                output=(
                    None if data.get("output") is None
                    else dict(data["output"])
                ),
                error_message=data.get("error"),
                execution_time=(
                    None if data.get("execution_time_seconds") is None
                    else float(data["execution_time_seconds"])
                ),
                green_metrics=(
                    None if data.get("green_metrics") is None
                    else dict(data["green_metrics"])
                ),
                reasoning_trace=(
                    None if data.get("reasoning_trace") is None
                    else list(data["reasoning_trace"])
                ),
                metadata=(
                    None if data.get("metadata") is None
                    else dict(data["metadata"])
                ),
                version=str(data.get("version", "1.1")),
                graph_metrics=(
                    None if data.get("graph_metrics") is None
                    else dict(data["graph_metrics"])
                ),
                modp_score=(
                    None if data.get("modp_score") is None
                    else float(data["modp_score"])
                ),
                human_feedback_score=(
                    None if data.get("human_feedback_score") is None
                    else float(data["human_feedback_score"])
                ),
                distillation_stats=(
                    None if data.get("distillation_stats") is None
                    else dict(data["distillation_stats"])
                ),
            )
        except A2AGatewayError:
            raise
        except (KeyError, TypeError, ValueError) as exc:
            raise A2AGatewayError(
                f"invalid A2AResponse payload: {exc}"
            ) from exc

    # ---------------------------------------------------------------- A2A
    def to_jsonrpc_response(
        self, request_id: str,
    ) -> Dict[str, Any]:
        """
        Render this response in A2A-compliant JSON-RPC 2.0 form.

        Parameters
        ----------
        request_id : str
            The ``id`` from the originating JSON-RPC request. A2A uses
            the request id to correlate the response; it is **not** the
            same as ``task_id``.

        Returns
        -------
        dict
            ``{"jsonrpc": "2.0", "id": ..., "result": {...}}`` — a
            wire-compatible A2A task response. On failure, returns a
            JSON-RPC error envelope instead of a ``result``.
        """
        _require_nonempty_str("request_id", request_id)
        if self.status == TaskStatus.SUCCESS:
            artifacts: List[Dict[str, Any]] = []
            if self.output is not None:
                text = json.dumps(
                    self.output, sort_keys=True, default=str,
                )
                artifacts.append({
                    "name": "output",
                    "parts": [{"type": "text", "text": text}],
                })
            return {
                "jsonrpc": "2.0",
                "id": request_id,
                "result": {
                    "id": self.task_id,
                    "status": {
                        "state": self.status.a2a_state(),
                        "timestamp": datetime.now(
                            timezone.utc
                        ).isoformat(),
                    },
                    "artifacts": artifacts,
                },
            }
        return {
            "jsonrpc": "2.0",
            "id": request_id,
            "error": {
                "code": -32000,
                "message": self.error_message or self.status.a2a_state(),
                "data": {
                    "task_id": self.task_id,
                    "state": self.status.a2a_state(),
                    "execution_time_seconds": self.execution_time,
                },
            },
        }


# --------------------------------------------------------------------------- #
# Decision state
# --------------------------------------------------------------------------- #
@dataclass(frozen=True, slots=True)
class A2ADecisionState:
    """Immutable state for the transformation-strategy decision."""

    task_type_len: float
    input_size: float
    has_constraints: float
    graph_centrality: float
    graph_connectivity: float
    human_feedback: float
    version_num: float

    def __post_init__(self) -> None:
        for name in (
            "task_type_len", "input_size", "has_constraints",
            "graph_centrality", "graph_connectivity",
            "human_feedback", "version_num",
        ):
            v = getattr(self, name)
            if isinstance(v, bool) or not isinstance(v, (int, float)):
                raise A2AGatewayError(f"{name} must be numeric.")
            fv = float(v)
            if not math.isfinite(fv) or not 0.0 <= fv <= 1.0:
                raise A2AGatewayError(
                    f"{name} must be finite and in [0, 1]."
                )

    @classmethod
    def from_request(cls, request: A2ARequest) -> "A2ADecisionState":
        gm = request.graph_metrics or {}
        # ``input_size`` from a deterministic serialization: two
        # semantically equivalent dicts have the same size regardless
        # of insertion order.
        try:
            input_bytes = len(json.dumps(
                request.input_data, sort_keys=True, default=str,
            ))
        except (TypeError, ValueError):
            input_bytes = len(repr(request.input_data))
        return cls(
            task_type_len=_clamp01(len(request.task_type) / 50.0),
            input_size=_clamp01(input_bytes / 10_000.0),
            has_constraints=1.0 if request.constraints else 0.0,
            graph_centrality=_clamp01(
                float(gm.get("centrality", 0.5)),
            ),
            graph_connectivity=_clamp01(
                float(gm.get("connectivity", 0.5)),
            ),
            human_feedback=_clamp01(float(
                request.human_feedback_score
                if request.human_feedback_score is not None
                else 0.5
            )),
            version_num=(
                1.0 if request.version == A2AVersion.V1_1.value else 0.0
            ),
        )

    def to_vector(self) -> np.ndarray:
        return np.array([
            self.task_type_len,
            self.input_size,
            self.has_constraints,
            self.graph_centrality,
            self.graph_connectivity,
            self.human_feedback,
            self.version_num,
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
class A2ADistillationOptimizer:
    """
    Multi-teacher distillation with MoE gating to select the
    transformation strategy.

    Actions: ``0 = standard``, ``1 = compressed``, ``2 = detailed``.

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

        self.feature_dim: int = A2A_FEATURE_DIM
        self.n_actions: int = A2A_N_ACTIONS

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
            raise A2AGatewayError(
                f"config must be a dict, got {type(config).__name__}."
            )

        def _num(
            name: str, raw: Any, default: float, *, positive: bool,
        ) -> float:
            v = default if raw is None else raw
            if isinstance(v, bool) or not isinstance(v, (int, float)):
                raise A2AGatewayError(f"{name} must be numeric.")
            fv = float(v)
            if not math.isfinite(fv):
                raise A2AGatewayError(f"{name} must be finite.")
            if positive and fv <= 0:
                raise A2AGatewayError(f"{name} must be > 0.")
            if not positive and fv < 0:
                raise A2AGatewayError(f"{name} must be >= 0.")
            return fv

        def _int(
            name: str, raw: Any, default: int, *, positive: bool,
        ) -> int:
            v = default if raw is None else raw
            if isinstance(v, bool) or not isinstance(v, int):
                raise A2AGatewayError(f"{name} must be an int.")
            if positive and v <= 0:
                raise A2AGatewayError(f"{name} must be > 0.")
            if not positive and v < 0:
                raise A2AGatewayError(f"{name} must be >= 0.")
            return v

        out = dict(config)
        out["distillation_lr"] = _num(
            "distillation_lr",
            config.get("distillation_lr", config.get("lr")),
            0.01, positive=True,
        )
        out["gating_lr"] = _num(
            "gating_lr",
            config.get("gating_lr", config.get("gate_lr")),
            0.005, positive=True,
        )
        out["epsilon"] = _num(
            "epsilon", config.get("epsilon"), 0.1, positive=False,
        )
        if out["epsilon"] > 1.0:
            raise A2AGatewayError("epsilon must be <= 1.0.")
        out["distill_weight"] = _num(
            "distill_weight",
            config.get("distill_weight", config.get("distill_w")),
            0.7, positive=False,
        )
        out["rl_weight"] = _num(
            "rl_weight",
            config.get("rl_weight", config.get("rl_w")),
            0.3, positive=False,
        )
        if out["distill_weight"] + out["rl_weight"] <= 0:
            raise A2AGatewayError(
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
    def _rule_teacher(self, state: A2ADecisionState) -> np.ndarray:
        probs = np.ones(self.n_actions) * 0.1
        if state.input_size > 0.5:
            probs[1] = 0.6
        elif state.human_feedback > 0.7:
            probs[2] = 0.6
        else:
            probs[0] = 0.6
        return probs / probs.sum()

    def _rlhf_teacher(self, state: A2ADecisionState) -> np.ndarray:
        probs = np.ones(self.n_actions) / self.n_actions
        if state.human_feedback > 0.7:
            probs[2] += 0.2
        elif state.human_feedback < 0.3:
            probs[1] += 0.2
        return probs / probs.sum()

    def _historical_teacher(self, state: A2ADecisionState) -> np.ndarray:
        if state.graph_centrality > 0.7:
            return np.array([0.2, 0.2, 0.6])
        if state.has_constraints:
            return np.array([0.1, 0.7, 0.2])
        return np.array([0.6, 0.2, 0.2])

    # ---------------------------------------------------------------- gating
    def _gate_forward(self, x: np.ndarray) -> np.ndarray:
        logits = x @ self.gate_weights + self.gate_bias
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
            raise A2AGatewayError(
                "teacher output contains non-finite values."
            )
        total = arr.sum()
        if total <= 0:
            return np.ones(n_actions, dtype=np.float64) / n_actions
        return arr / total

    # ---------------------------------------------------------------- action
    def select_action(
        self,
        state: A2ADecisionState,
        exploration: bool = True,
    ) -> Tuple[int, np.ndarray, np.ndarray, np.ndarray]:
        if not isinstance(state, A2ADecisionState):
            raise A2AGatewayError(
                "state must be an A2ADecisionState."
            )
        x = state.to_vector()
        if x.shape[0] != self.feature_dim:
            raise A2AGatewayError(
                f"feature vector has {x.shape[0]} elements; "
                f"expected {self.feature_dim}."
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
            raise A2AGatewayError(
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
# Gateway
# --------------------------------------------------------------------------- #
class A2AGateway:
    """
    Task-completion gateway with optional adaptive transformation
    strategy selection.

    For internal use, :meth:`transform_agent_output` returns a
    validated :class:`A2AResponse`. For A2A transport, use
    :meth:`A2AResponse.to_jsonrpc_response` (see the module docstring).

    ``strict`` mode (keyword-only) raises on enhancement initialization
    or execution failures; the default silently degrades, matching the
    previous behaviour.
    """

    def __init__(
        self,
        default_version: A2AVersion = A2AVersion.V1_1,
        config: Optional[Dict[str, Any]] = None,
        *,
        strict: bool = False,
    ) -> None:
        if not isinstance(default_version, A2AVersion):
            raise A2AGatewayError(
                "default_version must be an A2AVersion."
            )
        if config is not None and not isinstance(config, dict):
            raise A2AGatewayError(
                f"config must be a dict or None, got "
                f"{type(config).__name__}."
            )

        self.default_version = default_version
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
            raise A2AGatewayError(
                "use_enhancements=True requires the enhancements package."
            )

        self._lock = threading.RLock()
        self.request_count: int = 0
        self.error_count: int = 0
        self.validation_error_count: int = 0

        self.distillation_optimizer: Optional[
            A2ADistillationOptimizer
        ] = None
        self.node_descriptor: Any = None
        self.zero_trust: Any = None

        if self.use_enhancements:
            self._init_enhancements()

    # ---------------------------------------------------------------- init
    def _init_enhancements(self) -> None:
        try:
            self.distillation_optimizer = A2ADistillationOptimizer(
                self.config,
            )
            if self.config.get("use_node_descriptor", False):
                self.node_descriptor = NodeDescriptor(
                    id="a2a_gateway_node",
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
            logger.info("A2AGateway enhanced components initialized")
        except Exception:
            logger.exception("Failed to initialize enhancements")
            self.use_enhancements = False
            self.distillation_optimizer = None
            self.node_descriptor = None
            self.zero_trust = None
            if self._strict:
                raise

    # ---------------------------------------------------------------- validate
    def validate_request(
        self, request_data: Mapping[str, Any],
    ) -> A2ARequest:
        """
        Validate and construct an :class:`A2ARequest`.

        Raises
        ------
        A2AGatewayError
            If the request is missing required fields, has malformed
            types, or references an unsupported A2A version.
        """
        with self._lock:
            self.request_count += 1
        try:
            request = A2ARequest.from_dict(request_data)
            return request
        except A2AGatewayError as exc:
            with self._lock:
                self.error_count += 1
                self.validation_error_count += 1
            logger.exception("A2A request validation failed")
            raise A2AGatewayError(
                f"A2A request validation failed: {exc}"
            ) from exc

    def submit(
        self, request_data: Mapping[str, Any],
    ) -> A2ARequest:
        """
        Convenience wrapper around :meth:`validate_request`.

        Accepts either a raw ``Mapping`` or an :class:`A2ARequest`
        instance. Retained so a caller can pass ``create_a2a_task(...)``
        output directly.
        """
        if isinstance(request_data, A2ARequest):
            with self._lock:
                self.request_count += 1
            return request_data
        if not isinstance(request_data, Mapping):
            raise A2AGatewayError(
                "request_data must be a Mapping or A2ARequest, got "
                f"{type(request_data).__name__}."
            )
        return self.validate_request(request_data)

    # ---------------------------------------------------------------- responses
    def create_success_response(
        self,
        task_id: str,
        output: Dict[str, Any],
        execution_time: float,
        *,
        green_metrics: Optional[Dict[str, Any]] = None,
        reasoning_trace: Optional[List[Dict[str, Any]]] = None,
        metadata: Optional[Dict[str, Any]] = None,
        graph_metrics: Optional[Dict[str, float]] = None,
        modp_score: Optional[float] = None,
        human_feedback_score: Optional[float] = None,
        distillation_stats: Optional[Dict[str, Any]] = None,
    ) -> A2AResponse:
        return A2AResponse(
            task_id=task_id,
            status=TaskStatus.SUCCESS,
            output=output,
            execution_time=execution_time,
            green_metrics=green_metrics,
            reasoning_trace=reasoning_trace,
            metadata=metadata,
            version=self.default_version.value,
            graph_metrics=graph_metrics,
            modp_score=modp_score,
            human_feedback_score=human_feedback_score,
            distillation_stats=distillation_stats,
        )

    def create_failure_response(
        self,
        task_id: str,
        status: TaskStatus,
        error_message: str,
        *,
        execution_time: Optional[float] = None,
        partial_output: Optional[Dict[str, Any]] = None,
        reasoning_trace: Optional[List[Dict[str, Any]]] = None,
        graph_metrics: Optional[Dict[str, float]] = None,
        modp_score: Optional[float] = None,
        human_feedback_score: Optional[float] = None,
        distillation_stats: Optional[Dict[str, Any]] = None,
    ) -> A2AResponse:
        return A2AResponse(
            task_id=task_id,
            status=status,
            output=partial_output,
            error_message=error_message,
            execution_time=execution_time,
            reasoning_trace=reasoning_trace,
            version=self.default_version.value,
            graph_metrics=graph_metrics,
            modp_score=modp_score,
            human_feedback_score=human_feedback_score,
            distillation_stats=distillation_stats,
        )

    # ---------------------------------------------------------------- reward
    @staticmethod
    def _compute_modp_score(
        *,
        latency_s: float,
        input_bytes: int,
        output_bytes: int,
        action: int,
        weights: Optional[Mapping[str, float]],
    ) -> float:
        """
        Compute a MODP-style reward in ``[0, 1]`` for a transformation.

        Components
        ----------
        - **latency_score**: ``1.0`` at zero latency, ``0.0`` at or
          above :data:`MAX_LATENCY_S`.
        - **info_score**: whether the chosen action's compression
          behaviour is appropriate for the output size.

        The two are combined by ``weights["latency"]`` and
        ``weights["quality"]`` (defaults ``0.3`` / ``0.7``).
        """
        latency_score = 1.0 - _clamp01(
            max(float(latency_s), 0.0) / MAX_LATENCY_S,
        )
        if input_bytes > 0 and output_bytes > 0:
            ratio = output_bytes / input_bytes
        else:
            ratio = 1.0

        if action == 1:  # compressed
            if ratio < 0.1:
                info_score = 0.3
            elif ratio > 0.9:
                info_score = 0.5
            else:
                info_score = 1.0
        elif action == 2:  # detailed
            info_score = 1.0 if ratio < 5.0 else 0.6
        else:  # standard
            info_score = 1.0

        w = weights or {}
        w_latency = _clamp01(float(w.get("latency", 0.3)))
        w_quality = _clamp01(float(w.get("quality", 0.7)))
        total = w_latency + w_quality
        if total <= 0:
            w_latency, w_quality, total = 0.3, 0.7, 1.0
        w_latency /= total
        w_quality /= total

        return float(
            w_latency * latency_score + w_quality * info_score
        )

    # ---------------------------------------------------------------- transform
    def transform_agent_output(
        self,
        task_id: str,
        agent_output: Any,
        execution_time: float,
        *,
        green_metrics: Optional[Dict[str, Any]] = None,
        reasoning_trace: Optional[List[Dict[str, Any]]] = None,
        request: Optional[A2ARequest] = None,
    ) -> A2AResponse:
        """
        Transform ``agent_output`` to an :class:`A2AResponse`.

        When ``use_enhancements=True`` and ``request`` is provided, a
        distillation optimizer chooses the strategy (standard /
        compressed / detailed) and the per-request reward is recorded
        on ``A2AResponse.modp_score``.
        """
        try:
            output = self._normalize_output(agent_output)
            distill_stats: Optional[Dict[str, Any]] = None
            modp_score: Optional[float] = None

            if (
                self.use_enhancements
                and self.distillation_optimizer is not None
                and request is not None
            ):
                state = A2ADecisionState.from_request(request)
                action, x, t_out, t_comb = (
                    self.distillation_optimizer.select_action(state)
                )
                if action == 1:
                    output = self._compress_output(output)
                elif action == 2 and reasoning_trace is None:
                    reasoning_trace = [{
                        "step": "A2A transformation",
                        "note": "detailed mode",
                    }]

                # Compute MODP reward from the transformation's
                # observable effect.
                input_bytes = self._json_size(request.input_data)
                output_bytes = self._json_size(output)
                modp_score = self._compute_modp_score(
                    latency_s=execution_time,
                    input_bytes=input_bytes,
                    output_bytes=output_bytes,
                    action=action,
                    weights=request.modp_weights,
                )
                self.distillation_optimizer.update(
                    state_vec=x,
                    action=action,
                    reward=modp_score,
                    teacher_outputs=t_out,
                    teacher_combined=t_comb,
                )
                distill_stats = {
                    "action": action,
                    "counter": self.distillation_optimizer.counter,
                    "buffer_size": len(
                        self.distillation_optimizer.replay_buffer,
                    ),
                }

            return self.create_success_response(
                task_id=task_id,
                output=output,
                execution_time=execution_time,
                green_metrics=green_metrics,
                reasoning_trace=reasoning_trace,
                graph_metrics=(
                    dict(request.graph_metrics)
                    if request and request.graph_metrics else None
                ),
                human_feedback_score=(
                    request.human_feedback_score if request else None
                ),
                distillation_stats=distill_stats,
                modp_score=modp_score,
            )
        except A2AGatewayError:
            raise
        except Exception:
            with self._lock:
                self.error_count += 1
            logger.exception(
                "Failed to transform agent output for task %r", task_id,
            )
            if self._strict:
                raise
            return self.create_failure_response(
                task_id=task_id,
                status=TaskStatus.INVALID_OUTPUT,
                error_message=(
                    "Failed to transform agent output; see logs."
                ),
                execution_time=execution_time,
                graph_metrics=(
                    dict(request.graph_metrics)
                    if request and request.graph_metrics else None
                ),
                human_feedback_score=(
                    request.human_feedback_score if request else None
                ),
            )

    # ---------------------------------------------------------------- helpers
    @staticmethod
    def _normalize_output(agent_output: Any) -> Dict[str, Any]:
        if isinstance(agent_output, dict):
            return dict(agent_output)
        if isinstance(agent_output, str):
            return {"result": agent_output}
        if isinstance(agent_output, (list, tuple)):
            return {"results": list(agent_output)}
        return {"value": str(agent_output)}

    @staticmethod
    def _compress_output(output: Dict[str, Any]) -> Dict[str, Any]:
        """
        Reduce ``output`` to a single canonical value, truncated to
        :data:`COMPRESS_MAX_CHARS`.

        Prefers known keys (``result`` / ``answer`` / ``value`` /
        ``output``); falls back to the first primitive-valued entry;
        otherwise returns an empty ``result``.
        """
        for key in ("result", "answer", "value", "output"):
            v = output.get(key)
            if isinstance(v, str) and v:
                return {"result": v[:COMPRESS_MAX_CHARS]}
        for v in output.values():
            if isinstance(v, (str, int, float, bool)):
                return {"result": str(v)[:COMPRESS_MAX_CHARS]}
        return {"result": ""}

    @staticmethod
    def _json_size(data: Any) -> int:
        try:
            return len(json.dumps(
                data, sort_keys=True, default=str,
            ))
        except (TypeError, ValueError):
            return len(repr(data))

    # ---------------------------------------------------------------- stats
    def get_statistics(self) -> Dict[str, Any]:
        with self._lock:
            stats: Dict[str, Any] = {
                "total_requests": self.request_count,
                "total_errors": self.error_count,
                "validation_errors": self.validation_error_count,
                "error_rate": self.error_count / max(
                    self.request_count, 1,
                ),
                "version": self.default_version.value,
                "enhancements_enabled": self.use_enhancements,
            }
        if self.use_enhancements and self.distillation_optimizer is not None:
            stats["distillation"] = {
                "counter": self.distillation_optimizer.counter,
                "buffer_size": len(
                    self.distillation_optimizer.replay_buffer,
                ),
                "feature_dim": self.distillation_optimizer.feature_dim,
                "n_actions": self.distillation_optimizer.n_actions,
            }
        return stats

    def __repr__(self) -> str:
        return (
            "A2AGateway("
            f"version={self.default_version.value}, "
            f"enhancements={'on' if self.use_enhancements else 'off'}, "
            f"strict={self._strict})"
        )


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #
def create_a2a_task(
    task_id: str,
    task_type: str,
    query: str,
    *,
    max_tokens: Optional[int] = None,
    timeout_seconds: Optional[int] = None,
    graph_metrics: Optional[Dict[str, float]] = None,
    human_feedback_score: Optional[float] = None,
) -> A2ARequest:
    """
    Build an :class:`A2ARequest` for a single-query task.

    Returns a typed object — callers can pass it directly to
    :meth:`A2AGateway.submit` / :meth:`A2AGateway.transform_agent_output`.
    """
    constraints: Optional[Dict[str, Any]] = None
    if max_tokens is not None or timeout_seconds is not None:
        constraints = {}
        if max_tokens is not None:
            constraints["max_tokens"] = int(max_tokens)
        if timeout_seconds is not None:
            constraints["timeout_seconds"] = int(timeout_seconds)
    return A2ARequest(
        task_id=task_id,
        task_type=task_type,
        input_data={"query": str(query)},
        constraints=constraints,
        version=A2AVersion.V1_1.value,
        graph_metrics=(
            None if graph_metrics is None else dict(graph_metrics)
        ),
        human_feedback_score=human_feedback_score,
    )


# --------------------------------------------------------------------------- #
# Public API
# --------------------------------------------------------------------------- #
__all__: List[str] = [
    "__version__",
    "A2ADecisionState",
    "A2ADistillationOptimizer",
    "A2AGateway",
    "A2AGatewayError",
    "A2ARequest",
    "A2AResponse",
    "A2AVersion",
    "COMPRESS_MAX_CHARS",
    "ENHANCEMENTS_AVAILABLE",
    "MAX_LATENCY_S",
    "SUPPORTED_VERSIONS",
    "TaskStatus",
    "create_a2a_task",
]


def __dir__() -> List[str]:
    return sorted(__all__)


# --------------------------------------------------------------------------- #
# Smoke test: python -m protocols.a2a_gateway
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

    # ---- 1. A2ARequest validation ------------------------------- #
    print("A2ARequest:")
    req = A2ARequest(
        task_id="t1", task_type="inference",
        input_data={"q": "hi"},
    )
    _check("constructs", req.task_id == "t1")
    _check(
        "rejects empty task_id",
        _raises(A2AGatewayError, lambda: A2ARequest(
            task_id="", task_type="x", input_data={},
        )),
    )
    _check(
        "rejects non-Mapping input_data",
        _raises(A2AGatewayError, lambda: A2ARequest(
            task_id="t", task_type="x",
            input_data="not a mapping",  # type: ignore[arg-type]
        )),
    )
    _check(
        "rejects unsupported version",
        _raises(A2AGatewayError, lambda: A2ARequest(
            task_id="t", task_type="x", input_data={},
            version="2.0",
        )),
    )
    _check(
        "rejects NaN human_feedback_score",
        _raises(A2AGatewayError, lambda: A2ARequest(
            task_id="t", task_type="x", input_data={},
            human_feedback_score=float("nan"),
        )),
    )
    _check(
        "frozen — mutation rejected",
        _raises(
            (AttributeError, Exception),
            lambda: setattr(req, "task_id", "t2"),
        ),
    )
    _check(
        "round-trips via to_dict/from_dict",
        A2ARequest.from_dict(req.to_dict()).to_dict() == req.to_dict(),
    )

    # ---- 2. A2AResponse ----------------------------------------- #
    print("A2AResponse:")
    resp = A2AResponse(
        task_id="t1", status=TaskStatus.SUCCESS,
        output={"result": "ok"},
    )
    _check("constructs", resp.status == TaskStatus.SUCCESS)
    _check(
        "coerces string status",
        A2AResponse(
            task_id="t1", status="success",  # type: ignore[arg-type]
        ).status == TaskStatus.SUCCESS,
    )
    _check(
        "rejects unknown status",
        _raises(A2AGatewayError, lambda: A2AResponse(
            task_id="t1", status="bogus",  # type: ignore[arg-type]
        )),
    )
    _check(
        "round-trips via to_dict/from_dict",
        A2AResponse.from_dict(resp.to_dict()).to_dict()
        == resp.to_dict(),
    )

    # ---- 3. A2AResponse.to_jsonrpc_response --------------------- #
    print("to_jsonrpc_response:")
    rpc = resp.to_jsonrpc_response("req-1")
    _check(
        "jsonrpc envelope",
        rpc["jsonrpc"] == "2.0" and rpc["id"] == "req-1",
    )
    _check(
        "result.state == completed",
        rpc["result"]["status"]["state"] == "completed",
    )
    _check(
        "result.artifacts populated",
        len(rpc["result"]["artifacts"]) == 1,
    )
    fail_resp = A2AResponse(
        task_id="t1", status=TaskStatus.TIMEOUT,
        error_message="deadline exceeded",
    )
    rpc_fail = fail_resp.to_jsonrpc_response("req-2")
    _check(
        "failure → jsonrpc error envelope",
        "error" in rpc_fail and "result" not in rpc_fail,
    )
    _check(
        "error data.state == failed",
        rpc_fail["error"]["data"]["state"] == "failed",
    )

    # ---- 4. A2ADecisionState ------------------------------------ #
    print("A2ADecisionState:")
    state = A2ADecisionState.from_request(req)
    _check(
        "feature vector length",
        state.to_vector().shape[0] == A2A_FEATURE_DIM,
    )
    _check(
        "feature vector in [0,1]",
        bool(np.all(state.to_vector() >= 0)
             and np.all(state.to_vector() <= 1)),
    )

    # ---- 5. Optimizer ------------------------------------------- #
    print("optimizer:")
    opt = A2ADistillationOptimizer({"seed": 7})
    action, x, t_out, t_comb = opt.select_action(state)
    _check("action in 0..2", 0 <= action < A2A_N_ACTIONS)
    _check(
        "teacher_outputs shape",
        t_out.shape == (len(opt.teachers), A2A_N_ACTIONS),
    )
    _check(
        "teacher_combined shape",
        t_comb.shape == (A2A_N_ACTIONS,),
    )

    # Train without crashing.
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
        _raises(A2AGatewayError, lambda:
                opt.update(x, 0, float("nan"), t_out, t_comb)),
    )
    _check(
        "rejects bad epsilon",
        _raises(A2AGatewayError, lambda:
                A2ADistillationOptimizer({"epsilon": "high"})),
    )

    # ---- 6. MODP score ------------------------------------------ #
    print("MODP reward:")
    # Standard action, moderate latency → high score.
    s1 = A2AGateway._compute_modp_score(
        latency_s=0.1, input_bytes=1000, output_bytes=1000,
        action=0, weights=None,
    )
    # Over-compression → penalised.
    s2 = A2AGateway._compute_modp_score(
        latency_s=0.1, input_bytes=1000, output_bytes=10,
        action=1, weights=None,
    )
    # Ideal compression → max score.
    s3 = A2AGateway._compute_modp_score(
        latency_s=0.01, input_bytes=1000, output_bytes=500,
        action=1, weights=None,
    )
    _check("standard action reward in [0,1]", 0.0 <= s1 <= 1.0)
    _check("over-compression penalised", s2 < s3)
    _check("ideal compression reward > 0.5", s3 > 0.5)

    # ---- 7. Gateway construction / validation ------------------- #
    print("gateway:")
    gw = A2AGateway()
    _check("constructs", gw is not None)
    _check(
        "rejects non-dict config",
        _raises(A2AGatewayError, lambda: A2AGateway(
            config="not a dict",  # type: ignore[arg-type]
        )),
    )
    request = gw.validate_request(req.to_dict())
    _check("validate_request returns A2ARequest",
           isinstance(request, A2ARequest))
    _check("request_count incremented", gw.request_count == 1)
    _check(
        "invalid request raises",
        _raises(A2AGatewayError, lambda:
                gw.validate_request({"task_id": "t"})),
    )
    _check(
        "error_count incremented",
        gw.error_count == 1,
    )

    # ---- 8. transform_agent_output (legacy) --------------------- #
    print("transform (legacy):")
    resp = gw.transform_agent_output(
        task_id="t1",
        agent_output={"answer": "42"},
        execution_time=0.05,
        request=request,
    )
    _check("returns A2AResponse", isinstance(resp, A2AResponse))
    _check("status SUCCESS", resp.status == TaskStatus.SUCCESS)
    _check(
        "legacy path does not set modp_score",
        resp.modp_score is None,
    )

    # ---- 9. transform_agent_output (enhanced) ------------------- #
    print("transform (enhanced):")
    gw_enh = A2AGateway(config={"use_enhancements": True, "seed": 11})
    if gw_enh.use_enhancements:
        r_enh = gw_enh.transform_agent_output(
            task_id="t2",
            agent_output={"answer": "x" * 5000},
            execution_time=0.05,
            request=gw_enh.validate_request({
                "task_id": "t2", "task_type": "inference",
                "input_data": {"q": "hi"},
                "constraints": {"max_tokens": 100},
            }),
        )
        _check(
            "enhanced response has modp_score",
            r_enh.modp_score is not None,
        )
        _check(
            "modp_score in [0,1]",
            0.0 <= (r_enh.modp_score or 0.0) <= 1.0,
        )
        _check(
            "distillation_stats present",
            r_enh.distillation_stats is not None,
        )
        if r_enh.distillation_stats:
            _check(
                "stats key is 'counter' (not 'student_counter')",
                "counter" in r_enh.distillation_stats,
            )

        # Stats reflect the enhanced state.
        stats = gw_enh.get_statistics()
        _check(
            "stats includes distillation",
            "distillation" in stats,
        )
        _check(
            "enhancements_enabled reported",
            stats["enhancements_enabled"] is True,
        )
    else:
        print("  (enhancements unavailable — skipping enhanced path)")

    # ---- 10. create_a2a_task ----------------------------------- #
    print("create_a2a_task:")
    task = create_a2a_task(
        task_id="t3", task_type="inference", query="hello",
        max_tokens=100, human_feedback_score=0.7,
    )
    _check("returns A2ARequest", isinstance(task, A2ARequest))
    _check("constraints set", task.constraints == {"max_tokens": 100})
    _check(
        "human_feedback_score preserved",
        task.human_feedback_score == 0.7,
    )

    # ---- 11. Failure path ------------------------------------- #
    print("failure path:")

    class _BadOptimizer:
        def select_action(self, _state: Any) -> Any:
            raise RuntimeError("boom")

    gw_bad = A2AGateway(config={"use_enhancements": True, "seed": 1})
    if gw_bad.use_enhancements:
        gw_bad.distillation_optimizer = _BadOptimizer()  # type: ignore[assignment]
        resp = gw_bad.transform_agent_output(
            task_id="t4",
            agent_output={"answer": "ok"},
            execution_time=0.1,
            request=gw_bad.validate_request({
                "task_id": "t4", "task_type": "inference",
                "input_data": {"q": "hi"},
            }),
        )
        _check(
            "unexpected exception → INVALID_OUTPUT",
            resp.status == TaskStatus.INVALID_OUTPUT,
        )
        _check(
            "failure path logged an error",
            gw_bad.error_count >= 1,
        )

    # ---- 12. __all__ / __dir__ --------------------------------- #
    print("dir:")
    _check("__dir__ matches __all__", __dir__() == sorted(__all__))
    for _name in (
        "A2AGateway", "A2ARequest", "A2AResponse",
        "A2ADistillationOptimizer", "A2ADecisionState",
        "A2AVersion", "TaskStatus",
        "create_a2a_task", "A2AGatewayError",
    ):
        _check(f"{_name} exported", _name in __all__)

    print()
    print(f"passed: {passed}  failed: {failed}")
    if failed:
        sys.exit(1)
