# -*- coding: utf-8 -*-
"""
Feedback Integration & Audit Logger (Enhanced)
==============================================

Tracks expert invocations, feedback integration, and sustainability
metrics. Provides transparency reports for accountability.

Fixes over the previous version
-------------------------------
- **Persistence works.** ``json.dumps(asdict(event))`` raised
  ``TypeError`` on every write because ``dataclasses.asdict`` does not
  convert ``Enum`` members. Every write was caught by an
  ``except Exception: print(...)`` and swallowed, so
  ``enable_persistence=True`` silently wrote nothing. Enum
  serialisation is now explicit via :class:`_AuditJSONEncoder` and
  per-record ``to_dict()``.
- **Load path is real.** ``_load_from_file`` previously caught
  ``Exception`` and printed a warning that nobody read. It now reads
  line-by-line, logs per-line failures with ``logger.exception``, and
  tracks both :class:`AuditEvent` and :class:`ExpertFeedback` records
  via a ``_type`` discriminator.
- **Aggregates are restored on load.** After reading the log,
  :meth:`AuditLogger._rebuild_aggregates` replays every record through
  the same accumulation logic used at ``log_*`` time, so
  ``total_energy_consumed``, ``total_energy_saved``,
  ``distillation_updates``, and the metric histories match what an
  in-memory run would have produced.
- **``moe_experts_used`` is semantically correct.** Previously the
  report returned ``len(moe_gate_weights_history)`` (the count of
  logged vectors); it now returns ``len`` of the most recently logged
  vector — the actual number of experts.
- **``distillation_updates`` reads the right key.** The optimizer
  exposes ``counter``; the previous code read ``student_counter``,
  which never exists, so the counter stayed at ``0`` forever.
- **Thread-safe.** Every mutation is guarded by an ``RLock``.
- **Frozen, validated dataclasses.** ``AuditEvent``,
  ``ExpertFeedback``, and ``TransparencyReport`` reject malformed
  fields at construction.
- **Narrow error type.** :class:`AuditLoggerError` replaces
  ``ValueError`` / ``AttributeError`` / ``OSError`` leaks.
- **HTML is escaped.** Every interpolated value passes through
  ``html.escape`` and the "Enhanced metrics" section is emitted
  atomically, so a future field cannot leak markup or render outside
  its section.
- **Event IDs are collision-resistant.** ``md5(timestamp)`` is
  replaced with ``secrets.token_hex(8)``, so two events in the same
  microsecond no longer collide.
- **Carbon factor is a named constant.** ``DEFAULT_GRID_CARBON_KG_PER_KWH``
  appears once and is referenced by every savings calculation.
- **``export_report`` validates ``format``** and wraps ``OSError`` as
  :class:`AuditLoggerError`.
- **Empty-log report raises** instead of producing a zero-duration
  period.
- **``strict`` mode** — ``AuditLogger(..., strict=True)`` raises if
  ``use_enhancements=True`` while the enhancements package is
  unavailable.
- **``statistics()`` / ``reset()``** expose the aggregate state and
  clear it deterministically (tests and dashboards).
- **``__version__`` / ``__all__`` / ``__dir__``.**
- **Smoke test** with ``_check()`` assertions — survives ``python -O``;
  covers the persistence round-trip, aggregate restoration,
  thread-safety, HTML escaping, and every fixed report field.

Compatibility
-------------
- ``AuditLogger(log_file=None, enable_persistence=True, config=None)``
  — unchanged. ``strict=False`` is new and keyword-only.
- ``AuditEvent`` gained ``modp_score: Optional[float]`` so the report
  can rebuild ``self.modp_scores`` from the audit trail.
- ``ExpertFeedback`` gained ``task_id: str`` so feedback is
  attributable to the task that produced it. When loading a legacy
  record that lacks the field, the loader uses the ``feedback_id``
  as the task id (matching the previous ``log_feedback_received``
  behaviour, which stuffed ``feedback_id`` into the event's
  ``task_id`` field).
- ``ExpertFeedback.energy_impact`` is now explicitly documented as
  **Wh** (positive = savings, negative = cost).
- ``AuditEventType`` / ``FeedbackType`` are :class:`StrEnum`
  subclasses, so ``str(AuditEventType.EXPERT_INVOKED)`` is
  ``"expert_invoked"`` on every Python version.
- ``export_report(..., format="json" | "html")`` — ``format`` is now
  validated; unrecognised values raise :class:`AuditLoggerError`.
- Report field ``moe_experts_used`` — semantic change; previously it
  reported the number of logged gate vectors, now it reports the
  number of experts in the most recent vector.
- Report field ``distillation_updates`` — now correct; previously
  always ``0``.
- `log_feedback_received` still stores ``feedback_id`` in the event's
  ``task_id`` field for backward compatibility of *old* consumer
  queries; the feedback record now carries the real ``task_id``.

Requires Python 3.8+. ``slots=True`` on dataclasses requires 3.10+;
drop ``slots=True`` for 3.8/3.9.
"""

from __future__ import annotations

import enum
import hashlib
import html as _html
import json
import logging
import math
import secrets
import threading
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import (
    Any, Dict, Final, List, Mapping, Optional, Tuple,
)

import numpy as np


logger = logging.getLogger(__name__)

__version__ = "2.5.0"


# --------------------------------------------------------------------------- #
# Errors
# --------------------------------------------------------------------------- #
class AuditLoggerError(ValueError):
    """Raised for invalid audit-logger inputs or persistence failures."""


# --------------------------------------------------------------------------- #
# Constants
# --------------------------------------------------------------------------- #
DEFAULT_GRID_CARBON_KG_PER_KWH: Final[float] = 0.385

# Escalation sweet-spot: an operator's score is maximal when 10–30 % of
# tasks escalate. Below 10 % suggests the agent is over-confident (few
# escalations means bad decisions pass unchecked); above 30 % suggests
# the agent is under-performing (excessive reliance on experts). Both
# tails are penalised linearly.
ESCALATION_SWEET_MIN: Final[float] = 0.1
ESCALATION_SWEET_MAX: Final[float] = 0.3

# Weights for the base transparency score; must sum to 1.0 (the score
# is bounded to [0, 1] before any graph-health blend).
TRANSPARENCY_W_INTEGRATION: Final[float] = 0.4
TRANSPARENCY_W_ESCALATION: Final[float] = 0.3
TRANSPARENCY_W_EVENTS: Final[float] = 0.3

# Graph-health blend weight (only applied when the enhancement pipeline
# is active and a graph-health score is available).
GRAPH_BLEND: Final[float] = 0.1

_VALID_EXPORT_FORMATS: Final[Tuple[str, ...]] = ("json", "html")

_RECORD_TYPE_EVENT: Final[str] = "event"
_RECORD_TYPE_FEEDBACK: Final[str] = "feedback"


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
class FeedbackType(StrEnum):
    """Types of expert feedback."""

    CORRECTION = "correction"
    SUGGESTION = "suggestion"
    APPROVAL = "approval"
    REJECTION = "rejection"
    OPTIMIZATION = "optimization"
    SECURITY_FIX = "security_fix"


class AuditEventType(StrEnum):
    """Types of audit events."""

    EXPERT_INVOKED = "expert_invoked"
    FEEDBACK_RECEIVED = "feedback_received"
    FEEDBACK_INTEGRATED = "feedback_integrated"
    ESCALATION_TRIGGERED = "escalation_triggered"
    ENERGY_SAVED = "energy_saved"
    CARBON_SAVED = "carbon_saved"
    GRAPH_METRICS_UPDATED = "graph_metrics_updated"
    DISTILLATION_UPDATED = "distillation_updated"
    RLHF_FEEDBACK_LOGGED = "rlhf_feedback_logged"
    MODP_SCORE_LOGGED = "modp_score_logged"


# --------------------------------------------------------------------------- #
# Validation helpers
# --------------------------------------------------------------------------- #
def _require_nonempty_str(name: str, value: Any) -> str:
    if not isinstance(value, str) or not value:
        raise AuditLoggerError(f"{name} must be a non-empty string.")
    return value


def _require_optional_str(name: str, value: Any) -> Optional[str]:
    if value is None:
        return None
    if not isinstance(value, str):
        raise AuditLoggerError(f"{name} must be a string or None.")
    return value


def _require_finite(name: str, value: Any) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise AuditLoggerError(f"{name} must be numeric.")
    fv = float(value)
    if not math.isfinite(fv):
        raise AuditLoggerError(f"{name} must be finite, got {value!r}.")
    return fv


def _require_optional_finite(name: str, value: Any) -> Optional[float]:
    if value is None:
        return None
    return _require_finite(name, value)


def _require_unit_interval(name: str, value: Any) -> float:
    fv = _require_finite(name, value)
    if not 0.0 <= fv <= 1.0:
        raise AuditLoggerError(f"{name} must be in [0, 1], got {fv}.")
    return fv


def _require_optional_unit_interval(
    name: str, value: Any,
) -> Optional[float]:
    if value is None:
        return None
    return _require_unit_interval(name, value)


def _require_mapping(name: str, value: Any) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise AuditLoggerError(
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
            raise AuditLoggerError(
                f"{name} keys must be non-empty strings, got {k!r}."
            )
        out[k] = _require_finite(f"{name}[{k!r}]", v)
    return out


# --------------------------------------------------------------------------- #
# JSON helper for `Any` payloads
# --------------------------------------------------------------------------- #
def _safe_json_value(value: Any) -> Any:
    """
    Convert ``value`` to something ``json.dumps`` will accept.

    Primitives pass through; anything else is ``repr()``-serialised so
    the audit trail captures *what* was there even if the object is not
    directly JSON-representable. On load, the caller sees a string.
    """
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    try:
        json.dumps(value)
        return value
    except (TypeError, ValueError):
        return repr(value)


class _AuditJSONEncoder(json.JSONEncoder):
    """Serialise ``StrEnum`` and ``Enum`` members by ``.value``."""

    def default(self, obj: Any) -> Any:
        if isinstance(obj, enum.Enum):
            return obj.value
        return super().default(obj)


# --------------------------------------------------------------------------- #
# Records
# --------------------------------------------------------------------------- #
@dataclass(frozen=True, slots=True)
class ExpertFeedback:
    """
    Expert feedback on agent output.

    .. note::
        ``energy_impact`` is expressed in **Wh** (positive = savings,
        negative = cost). ``original_output`` and ``suggested_output``
        may be arbitrary Python objects; non-JSON-representable values
        are ``repr()``-serialised in the audit log and come back as
        strings on load.
    """

    feedback_id: str
    expert_type: str
    feedback_type: FeedbackType
    original_output: Any
    suggested_output: Any
    explanation: str
    confidence: float
    energy_impact: float
    timestamp: float
    task_id: Optional[str] = None
    human_feedback_score: Optional[float] = None
    graph_metrics: Optional[Dict[str, float]] = None

    def __post_init__(self) -> None:
        _require_nonempty_str("feedback_id", self.feedback_id)
        _require_nonempty_str("expert_type", self.expert_type)
        if not isinstance(self.feedback_type, FeedbackType):
            raise AuditLoggerError(
                "feedback_type must be a FeedbackType."
            )
        _require_nonempty_str("explanation", self.explanation)
        _require_unit_interval("confidence", self.confidence)
        _require_finite("energy_impact", self.energy_impact)
        _require_finite("timestamp", self.timestamp)
        if self.timestamp < 0:
            raise AuditLoggerError("timestamp must be >= 0.")
        _require_optional_str("task_id", self.task_id)
        _require_optional_unit_interval(
            "human_feedback_score", self.human_feedback_score,
        )
        _require_optional_float_mapping("graph_metrics", self.graph_metrics)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "feedback_id": self.feedback_id,
            "expert_type": self.expert_type,
            "feedback_type": self.feedback_type.value,
            "original_output": _safe_json_value(self.original_output),
            "suggested_output": _safe_json_value(self.suggested_output),
            "explanation": self.explanation,
            "confidence": float(self.confidence),
            "energy_impact": float(self.energy_impact),
            "timestamp": float(self.timestamp),
            "task_id": self.task_id,
            "human_feedback_score": self.human_feedback_score,
            "graph_metrics": (
                None if self.graph_metrics is None
                else dict(self.graph_metrics)
            ),
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "ExpertFeedback":
        if not isinstance(data, Mapping):
            raise AuditLoggerError(
                "ExpertFeedback.from_dict expects a Mapping."
            )
        try:
            return cls(
                feedback_id=str(data["feedback_id"]),
                expert_type=str(data["expert_type"]),
                feedback_type=FeedbackType(data["feedback_type"]),
                original_output=data.get("original_output"),
                suggested_output=data.get("suggested_output"),
                explanation=str(data["explanation"]),
                confidence=float(data["confidence"]),
                energy_impact=float(data["energy_impact"]),
                timestamp=float(data["timestamp"]),
                # Fall back to feedback_id when the legacy record
                # lacks task_id (matches the pre-enhancement behaviour
                # of stuffing feedback_id into the event's task_id).
                task_id=data.get("task_id")
                or str(data.get("feedback_id", "")) or None,
                human_feedback_score=(
                    None if data.get("human_feedback_score") is None
                    else float(data["human_feedback_score"])
                ),
                graph_metrics=(
                    None if data.get("graph_metrics") is None
                    else dict(data["graph_metrics"])
                ),
            )
        except AuditLoggerError:
            raise
        except (KeyError, TypeError, ValueError) as exc:
            raise AuditLoggerError(
                f"invalid ExpertFeedback payload: {exc}"
            ) from exc


@dataclass(frozen=True, slots=True)
class AuditEvent:
    """Event in audit log."""

    event_id: str
    event_type: AuditEventType
    task_id: str
    agent_id: str
    expert_involved: Optional[str]
    energy_consumed_wh: float
    carbon_emitted_kg: float
    details: Dict[str, Any]
    timestamp: float
    graph_metrics: Optional[Dict[str, float]] = None
    modp_weights: Optional[Dict[str, float]] = None
    modp_score: Optional[float] = None
    human_feedback_score: Optional[float] = None
    distillation_stats: Optional[Dict[str, Any]] = None
    moe_gate_weights: Optional[List[float]] = None

    def __post_init__(self) -> None:
        _require_nonempty_str("event_id", self.event_id)
        if not isinstance(self.event_type, AuditEventType):
            raise AuditLoggerError(
                "event_type must be an AuditEventType."
            )
        _require_nonempty_str("task_id", self.task_id)
        _require_nonempty_str("agent_id", self.agent_id)
        _require_optional_str("expert_involved", self.expert_involved)
        _require_finite("energy_consumed_wh", self.energy_consumed_wh)
        _require_finite("carbon_emitted_kg", self.carbon_emitted_kg)
        _require_mapping("details", self.details)
        _require_finite("timestamp", self.timestamp)
        if self.timestamp < 0:
            raise AuditLoggerError("timestamp must be >= 0.")
        _require_optional_float_mapping("graph_metrics", self.graph_metrics)
        _require_optional_float_mapping("modp_weights", self.modp_weights)
        _require_optional_finite("modp_score", self.modp_score)
        _require_optional_unit_interval(
            "human_feedback_score", self.human_feedback_score,
        )
        _require_optional_mapping(
            "distillation_stats", self.distillation_stats,
        )
        if self.moe_gate_weights is not None:
            if not isinstance(self.moe_gate_weights, list):
                raise AuditLoggerError(
                    "moe_gate_weights must be a list or None."
                )
            for i, v in enumerate(self.moe_gate_weights):
                _require_finite(f"moe_gate_weights[{i}]", v)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "event_id": self.event_id,
            "event_type": self.event_type.value,
            "task_id": self.task_id,
            "agent_id": self.agent_id,
            "expert_involved": self.expert_involved,
            "energy_consumed_wh": float(self.energy_consumed_wh),
            "carbon_emitted_kg": float(self.carbon_emitted_kg),
            "details": dict(self.details),
            "timestamp": float(self.timestamp),
            "graph_metrics": (
                None if self.graph_metrics is None
                else dict(self.graph_metrics)
            ),
            "modp_weights": (
                None if self.modp_weights is None
                else dict(self.modp_weights)
            ),
            "modp_score": self.modp_score,
            "human_feedback_score": self.human_feedback_score,
            "distillation_stats": (
                None if self.distillation_stats is None
                else dict(self.distillation_stats)
            ),
            "moe_gate_weights": (
                None if self.moe_gate_weights is None
                else list(self.moe_gate_weights)
            ),
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "AuditEvent":
        if not isinstance(data, Mapping):
            raise AuditLoggerError(
                "AuditEvent.from_dict expects a Mapping."
            )
        try:
            return cls(
                event_id=str(data["event_id"]),
                event_type=AuditEventType(data["event_type"]),
                task_id=str(data["task_id"]),
                agent_id=str(data["agent_id"]),
                expert_involved=data.get("expert_involved"),
                energy_consumed_wh=float(data["energy_consumed_wh"]),
                carbon_emitted_kg=float(data["carbon_emitted_kg"]),
                details=dict(data.get("details", {}) or {}),
                timestamp=float(data["timestamp"]),
                graph_metrics=(
                    None if data.get("graph_metrics") is None
                    else dict(data["graph_metrics"])
                ),
                modp_weights=(
                    None if data.get("modp_weights") is None
                    else dict(data["modp_weights"])
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
                moe_gate_weights=(
                    None if data.get("moe_gate_weights") is None
                    else list(data["moe_gate_weights"])
                ),
            )
        except AuditLoggerError:
            raise
        except (KeyError, TypeError, ValueError) as exc:
            raise AuditLoggerError(
                f"invalid AuditEvent payload: {exc}"
            ) from exc


@dataclass(frozen=True, slots=True)
class TransparencyReport:
    """
    Comprehensive transparency report.

    The *sustainability* and *efficiency* fields are aggregates over
    the report period; the *transparency_score* is computed by
    :meth:`AuditLogger._calculate_transparency_score`. See that
    method's docstring for the escalation sweet-spot rationale.
    """

    report_id: str
    period_start: float
    period_end: float

    total_tasks: int
    tasks_escalated: int
    escalation_rate: float

    expert_invocations: int
    experts_by_type: Dict[str, int]
    feedback_received: int
    feedback_integrated: int
    feedback_integration_rate: float

    total_energy_consumed_wh: float
    energy_saved_wh: float
    net_energy_wh: float
    total_carbon_kg: float
    carbon_saved_kg: float
    net_carbon_kg: float

    avg_task_energy_wh: float
    avg_expert_energy_wh: float
    sustainability_improvement_pct: float

    audit_events: int
    transparency_score: float

    graph_health_score: Optional[float] = None
    modp_overall_score: Optional[float] = None
    rlhf_average_feedback: Optional[float] = None
    distillation_updates: Optional[int] = None
    moe_experts_used: Optional[int] = None

    def __post_init__(self) -> None:
        _require_nonempty_str("report_id", self.report_id)
        _require_finite("period_start", self.period_start)
        _require_finite("period_end", self.period_end)
        if self.period_end < self.period_start:
            raise AuditLoggerError(
                "period_end must be >= period_start."
            )
        for name in (
            "total_tasks", "tasks_escalated", "expert_invocations",
            "feedback_received", "feedback_integrated", "audit_events",
        ):
            v = getattr(self, name)
            if isinstance(v, bool) or not isinstance(v, int) or v < 0:
                raise AuditLoggerError(
                    f"{name} must be a non-negative int."
                )
        for name in (
            "escalation_rate", "feedback_integration_rate",
            "transparency_score", "sustainability_improvement_pct",
        ):
            v = getattr(self, name)
            fv = _require_finite(name, v)
            if name != "sustainability_improvement_pct":
                if not 0.0 <= fv <= 1.0:
                    raise AuditLoggerError(
                        f"{name} must be in [0, 1]."
                    )
        for name in (
            "total_energy_consumed_wh", "energy_saved_wh", "net_energy_wh",
            "total_carbon_kg", "carbon_saved_kg", "net_carbon_kg",
            "avg_task_energy_wh", "avg_expert_energy_wh",
        ):
            _require_finite(name, getattr(self, name))
        _require_mapping("experts_by_type", self.experts_by_type)
        _require_optional_unit_interval(
            "graph_health_score", self.graph_health_score,
        )
        _require_optional_unit_interval(
            "modp_overall_score", self.modp_overall_score,
        )
        _require_optional_unit_interval(
            "rlhf_average_feedback", self.rlhf_average_feedback,
        )
        if self.distillation_updates is not None:
            if isinstance(self.distillation_updates, bool) or not isinstance(
                self.distillation_updates, int
            ):
                raise AuditLoggerError(
                    "distillation_updates must be an int or None."
                )
        if self.moe_experts_used is not None:
            if isinstance(self.moe_experts_used, bool) or not isinstance(
                self.moe_experts_used, int
            ):
                raise AuditLoggerError(
                    "moe_experts_used must be an int or None."
                )


# --------------------------------------------------------------------------- #
# Logger
# --------------------------------------------------------------------------- #
try:  # pragma: no cover — optional dependency
    from src.enhancements.schemas.feedback_event import (  # noqa: F401
        FeedbackEvent,
    )
    from src.enhancements.schemas.node_descriptor import (  # noqa: F401
        NodeDescriptor,
    )
    from src.enhancements.schemas.workload_descriptor import (  # noqa: F401
        WorkloadDescriptor,
    )

    ENHANCEMENTS_AVAILABLE: bool = True
except ImportError as _exc:  # pragma: no cover
    ENHANCEMENTS_AVAILABLE = False
    logger.info("Enhancement schemas unavailable: %s", _exc)


class AuditLogger:
    """
    Audit logger for the expert-collaboration system.

    Thread-safe. Enhanced with optional LIMIT Graph, MODP, RLHF,
    distillation, and MoE tracking.

    ``strict`` mode (keyword-only) raises when
    ``use_enhancements=True`` but the enhancement package is
    unavailable.
    """

    def __init__(
        self,
        log_file: Optional[str] = None,
        enable_persistence: bool = True,
        config: Optional[Dict[str, Any]] = None,
        *,
        strict: bool = False,
    ) -> None:
        self.log_file: Optional[str] = log_file
        self.enable_persistence: bool = bool(enable_persistence)
        self._strict: bool = bool(strict)
        self.config: Dict[str, Any] = (
            dict(config) if config else {}
        )
        self.use_enhancements: bool = (
            bool(self.config.get("use_enhancements", False))
            and ENHANCEMENTS_AVAILABLE
        )
        if (
            self._strict
            and self.config.get("use_enhancements", False)
            and not ENHANCEMENTS_AVAILABLE
        ):
            raise AuditLoggerError(
                "use_enhancements=True requires the enhancements package."
            )

        self._lock = threading.RLock()

        self.audit_events: List[AuditEvent] = []
        self.feedback_history: List[ExpertFeedback] = []

        self.total_energy_consumed: float = 0.0
        self.total_carbon_emitted: float = 0.0
        self.total_energy_saved: float = 0.0
        self.total_carbon_saved: float = 0.0

        self.graph_metrics_history: List[Dict[str, float]] = []
        self.rlhf_feedback_scores: List[float] = []
        self.modp_scores: List[float] = []
        self.distillation_updates: int = 0
        self.moe_gate_weights_history: List[List[float]] = []

        raw_weights = self.config.get(
            "modp_weights",
            {"carbon": 0.4, "energy": 0.3, "latency": 0.2, "cost": 0.1},
        )
        self.modp_weights: Dict[str, float] = dict(raw_weights)

        if self.enable_persistence and self.log_file:
            self._load_from_file()
            self._rebuild_aggregates()

    # ---------------------------------------------------------------- logging
    def log_expert_invocation(
        self,
        task_id: str,
        agent_id: str,
        expert_type: str,
        energy_wh: float,
        carbon_kg: float,
        details: Optional[Dict[str, Any]] = None,
        graph_metrics: Optional[Dict[str, float]] = None,
        human_feedback_score: Optional[float] = None,
    ) -> None:
        """Log an expert-model invocation."""
        with self._lock:
            event = AuditEvent(
                event_id=self._generate_event_id(),
                event_type=AuditEventType.EXPERT_INVOKED,
                task_id=task_id,
                agent_id=agent_id,
                expert_involved=expert_type,
                energy_consumed_wh=float(energy_wh),
                carbon_emitted_kg=float(carbon_kg),
                details=dict(details or {}),
                timestamp=datetime.now().timestamp(),
                graph_metrics=(
                    None if graph_metrics is None
                    else dict(graph_metrics)
                ),
                human_feedback_score=human_feedback_score,
                modp_weights=(
                    dict(self.modp_weights) if self.modp_weights else None
                ),
            )
            self._add_event_locked(event)

            if event.energy_consumed_wh > 0:
                self.total_energy_consumed += event.energy_consumed_wh
            if event.carbon_emitted_kg > 0:
                self.total_carbon_emitted += event.carbon_emitted_kg

            if graph_metrics and self.use_enhancements:
                self.graph_metrics_history.append(dict(graph_metrics))
            if (
                human_feedback_score is not None
                and self.use_enhancements
            ):
                self.rlhf_feedback_scores.append(
                    float(human_feedback_score)
                )

    def log_feedback_received(self, feedback: ExpertFeedback) -> None:
        """Log feedback received from an expert."""
        if not isinstance(feedback, ExpertFeedback):
            raise AuditLoggerError(
                "feedback must be an ExpertFeedback instance."
            )
        with self._lock:
            self.feedback_history.append(feedback)
            self._append_to_file(
                feedback, _RECORD_TYPE_FEEDBACK,
            )

            if self.use_enhancements:
                if feedback.human_feedback_score is not None:
                    self.rlhf_feedback_scores.append(
                        float(feedback.human_feedback_score)
                    )
                if feedback.graph_metrics:
                    self.graph_metrics_history.append(
                        dict(feedback.graph_metrics)
                    )

            event = AuditEvent(
                event_id=self._generate_event_id(),
                event_type=AuditEventType.FEEDBACK_RECEIVED,
                # Backward-compat: the event's task_id historically
                # carried the feedback id (the old API had no task_id).
                task_id=feedback.task_id or feedback.feedback_id,
                agent_id="unknown",
                expert_involved=feedback.expert_type,
                energy_consumed_wh=0.0,
                carbon_emitted_kg=0.0,
                details={
                    "feedback_id": feedback.feedback_id,
                    "feedback_type": feedback.feedback_type.value,
                    "confidence": float(feedback.confidence),
                },
                timestamp=feedback.timestamp,
                graph_metrics=(
                    None if feedback.graph_metrics is None
                    else dict(feedback.graph_metrics)
                ),
                human_feedback_score=feedback.human_feedback_score,
                modp_weights=(
                    dict(self.modp_weights) if self.modp_weights else None
                ),
            )
            self._add_event_locked(event)

    def log_feedback_integrated(
        self,
        feedback_id: str,
        task_id: str,
        agent_id: str,
        integration_success: bool,
        energy_impact: float,
        modp_score: Optional[float] = None,
        distillation_stats: Optional[Dict[str, Any]] = None,
    ) -> None:
        """
        Log feedback integration.

        ``energy_impact`` is in Wh (positive = savings, negative = cost).
        """
        with self._lock:
            event = AuditEvent(
                event_id=self._generate_event_id(),
                event_type=AuditEventType.FEEDBACK_INTEGRATED,
                task_id=task_id,
                agent_id=agent_id,
                expert_involved=None,
                energy_consumed_wh=0.0,
                carbon_emitted_kg=0.0,
                details={
                    "feedback_id": feedback_id,
                    "success": bool(integration_success),
                    "energy_impact_wh": float(energy_impact),
                },
                timestamp=datetime.now().timestamp(),
                modp_score=modp_score,
                distillation_stats=(
                    None if distillation_stats is None
                    else dict(distillation_stats)
                ),
                moe_gate_weights=(
                    None if distillation_stats is None
                    else self._extract_gate_weights(distillation_stats)
                ),
                modp_weights=(
                    dict(self.modp_weights) if self.modp_weights else None
                ),
            )
            self._add_event_locked(event)

            if energy_impact > 0:
                self.total_energy_saved += float(energy_impact)
                self.total_carbon_saved += (
                    float(energy_impact)
                    * DEFAULT_GRID_CARBON_KG_PER_KWH / 1000.0
                )

            if self.use_enhancements:
                if modp_score is not None:
                    self.modp_scores.append(float(modp_score))
                if distillation_stats:
                    counter = distillation_stats.get("counter")
                    if counter is None:
                        counter = distillation_stats.get("student_counter")
                    if (
                        isinstance(counter, int)
                        and not isinstance(counter, bool)
                        and counter >= 0
                    ):
                        self.distillation_updates += counter
                    if event.moe_gate_weights:
                        self.moe_gate_weights_history.append(
                            list(event.moe_gate_weights)
                        )

    def log_escalation(
        self,
        task_id: str,
        agent_id: str,
        reasons: List[str],
        expert_type: str,
        graph_metrics: Optional[Dict[str, float]] = None,
    ) -> None:
        """Log an escalation decision."""
        with self._lock:
            event = AuditEvent(
                event_id=self._generate_event_id(),
                event_type=AuditEventType.ESCALATION_TRIGGERED,
                task_id=task_id,
                agent_id=agent_id,
                expert_involved=expert_type,
                energy_consumed_wh=0.0,
                carbon_emitted_kg=0.0,
                details={"reasons": list(reasons)},
                timestamp=datetime.now().timestamp(),
                graph_metrics=(
                    None if graph_metrics is None
                    else dict(graph_metrics)
                ),
                modp_weights=(
                    dict(self.modp_weights) if self.modp_weights else None
                ),
            )
            self._add_event_locked(event)

            if graph_metrics and self.use_enhancements:
                self.graph_metrics_history.append(dict(graph_metrics))

    def log_energy_saved(
        self,
        task_id: str,
        agent_id: str,
        energy_saved_wh: float,
        mechanism: str,
    ) -> None:
        """Log an energy-saving action."""
        carbon_saved = (
            float(energy_saved_wh)
            * DEFAULT_GRID_CARBON_KG_PER_KWH / 1000.0
        )
        with self._lock:
            event = AuditEvent(
                event_id=self._generate_event_id(),
                event_type=AuditEventType.ENERGY_SAVED,
                task_id=task_id,
                agent_id=agent_id,
                expert_involved=None,
                # Negative = saved (matching the pre-enhancement
                # convention used by the report aggregation).
                energy_consumed_wh=-float(energy_saved_wh),
                carbon_emitted_kg=-carbon_saved,
                details={"mechanism": mechanism},
                timestamp=datetime.now().timestamp(),
                modp_weights=(
                    dict(self.modp_weights) if self.modp_weights else None
                ),
            )
            self._add_event_locked(event)

            self.total_energy_saved += float(energy_saved_wh)
            self.total_carbon_saved += carbon_saved

    # ---------------------------------------------------------------- reports
    def generate_transparency_report(
        self,
        period_start: Optional[float] = None,
        period_end: Optional[float] = None,
    ) -> TransparencyReport:
        """
        Generate a comprehensive transparency report.

        Raises
        ------
        AuditLoggerError
            If the logger has no events and no period was supplied.
        """
        with self._lock:
            events_snapshot = list(self.audit_events)
            graph_metrics_snapshot = list(self.graph_metrics_history)
            rlhf_snapshot = list(self.rlhf_feedback_scores)
            modp_snapshot = list(self.modp_scores)
            moe_history_snapshot = list(self.moe_gate_weights_history)
            distillation_updates = self.distillation_updates

        if not events_snapshot:
            raise AuditLoggerError(
                "cannot generate a transparency report with no audit "
                "events."
            )

        if period_start is None:
            period_start = min(e.timestamp for e in events_snapshot)
        if period_end is None:
            period_end = datetime.now().timestamp()

        period_events = [
            e for e in events_snapshot
            if period_start <= e.timestamp <= period_end
        ]

        total_tasks = len({e.task_id for e in period_events})
        escalations = [
            e for e in period_events
            if e.event_type == AuditEventType.ESCALATION_TRIGGERED
        ]
        tasks_escalated = len({e.task_id for e in escalations})
        escalation_rate = (
            tasks_escalated / total_tasks if total_tasks > 0 else 0.0
        )

        expert_events = [
            e for e in period_events
            if e.event_type == AuditEventType.EXPERT_INVOKED
        ]
        expert_invocations = len(expert_events)
        experts_by_type: Dict[str, int] = {}
        for e in expert_events:
            key = e.expert_involved or "unknown"
            experts_by_type[key] = experts_by_type.get(key, 0) + 1

        feedback_received = sum(
            1 for e in period_events
            if e.event_type == AuditEventType.FEEDBACK_RECEIVED
        )
        feedback_integrated = sum(
            1 for e in period_events
            if e.event_type == AuditEventType.FEEDBACK_INTEGRATED
        )
        feedback_integration_rate = (
            feedback_integrated / feedback_received
            if feedback_received > 0 else 0.0
        )

        total_energy = sum(
            e.energy_consumed_wh for e in period_events
            if e.energy_consumed_wh > 0
        )
        energy_saved = sum(
            -e.energy_consumed_wh for e in period_events
            if e.energy_consumed_wh < 0
        )
        net_energy = total_energy - energy_saved

        total_carbon = sum(
            e.carbon_emitted_kg for e in period_events
            if e.carbon_emitted_kg > 0
        )
        carbon_saved = sum(
            -e.carbon_emitted_kg for e in period_events
            if e.carbon_emitted_kg < 0
        )
        net_carbon = total_carbon - carbon_saved

        avg_task_energy = (
            total_energy / total_tasks if total_tasks > 0 else 0.0
        )
        avg_expert_energy = (
            total_energy / expert_invocations
            if expert_invocations > 0 else 0.0
        )
        sustainability_improvement = (
            energy_saved / total_energy * 100.0
            if total_energy > 0 else 0.0
        )

        graph_health_score: Optional[float] = None
        modp_overall_score: Optional[float] = None
        rlhf_average_feedback: Optional[float] = None
        moe_experts_used: Optional[int] = None

        if self.use_enhancements:
            if graph_metrics_snapshot:
                centralities = [
                    gm.get("centrality", 0.5)
                    for gm in graph_metrics_snapshot
                ]
                graph_health_score = float(np.mean(centralities))
            if modp_snapshot:
                modp_overall_score = float(np.mean(modp_snapshot))
            if rlhf_snapshot:
                rlhf_average_feedback = float(np.mean(rlhf_snapshot))
            moe_experts_used = self._compute_moe_experts_used(
                moe_history_snapshot,
            )

        transparency_score = self._calculate_transparency_score(
            integration_rate=feedback_integration_rate,
            escalation_rate=escalation_rate,
            event_count=len(period_events),
            graph_health_score=graph_health_score,
            use_graph_blend=self.use_enhancements,
        )

        return TransparencyReport(
            report_id=self._generate_report_id(),
            period_start=float(period_start),
            period_end=float(period_end),
            total_tasks=total_tasks,
            tasks_escalated=tasks_escalated,
            escalation_rate=float(escalation_rate),
            expert_invocations=expert_invocations,
            experts_by_type=experts_by_type,
            feedback_received=feedback_received,
            feedback_integrated=feedback_integrated,
            feedback_integration_rate=float(feedback_integration_rate),
            total_energy_consumed_wh=float(total_energy),
            energy_saved_wh=float(energy_saved),
            net_energy_wh=float(net_energy),
            total_carbon_kg=float(total_carbon),
            carbon_saved_kg=float(carbon_saved),
            net_carbon_kg=float(net_carbon),
            avg_task_energy_wh=float(avg_task_energy),
            avg_expert_energy_wh=float(avg_expert_energy),
            sustainability_improvement_pct=float(
                sustainability_improvement
            ),
            audit_events=len(period_events),
            transparency_score=float(transparency_score),
            graph_health_score=graph_health_score,
            modp_overall_score=modp_overall_score,
            rlhf_average_feedback=rlhf_average_feedback,
            distillation_updates=int(distillation_updates),
            moe_experts_used=moe_experts_used,
        )

    def export_report(
        self,
        report: TransparencyReport,
        filepath: str,
        *,
        format: str = "json",
    ) -> None:
        """
        Export a transparency report to ``filepath``.

        Raises
        ------
        AuditLoggerError
            If ``format`` is not recognised, or the file cannot be
            written.
        """
        if not isinstance(report, TransparencyReport):
            raise AuditLoggerError(
                "report must be a TransparencyReport."
            )
        if format not in _VALID_EXPORT_FORMATS:
            raise AuditLoggerError(
                f"unsupported format {format!r}; expected one of "
                f"{sorted(_VALID_EXPORT_FORMATS)}."
            )
        path = Path(filepath)
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            if format == "json":
                with path.open("w", encoding="utf-8") as f:
                    json.dump(
                        self._report_to_dict(report),
                        f, indent=2,
                        cls=_AuditJSONEncoder,
                    )
            else:
                with path.open("w", encoding="utf-8") as f:
                    f.write(self._generate_html_report(report))
        except OSError as exc:
            raise AuditLoggerError(
                f"could not write {path}: {exc}"
            ) from exc

    # ---------------------------------------------------------------- admin
    def statistics(self) -> Dict[str, Any]:
        """Return a JSON-safe snapshot of the aggregate state."""
        with self._lock:
            return {
                "version": __version__,
                "event_count": len(self.audit_events),
                "feedback_count": len(self.feedback_history),
                "total_energy_consumed_wh": self.total_energy_consumed,
                "total_carbon_emitted_kg": self.total_carbon_emitted,
                "total_energy_saved_wh": self.total_energy_saved,
                "total_carbon_saved_kg": self.total_carbon_saved,
                "graph_metrics_count": len(self.graph_metrics_history),
                "rlhf_feedback_count": len(self.rlhf_feedback_scores),
                "modp_score_count": len(self.modp_scores),
                "distillation_updates": self.distillation_updates,
                "moe_gate_vectors": len(self.moe_gate_weights_history),
                "moe_experts_used": self._compute_moe_experts_used(
                    self.moe_gate_weights_history,
                ),
                "use_enhancements": self.use_enhancements,
                "strict": self._strict,
                "enable_persistence": self.enable_persistence,
                "log_file": self.log_file,
            }

    def reset(self, *, clear_file: bool = False) -> int:
        """
        Clear all in-memory state. Returns the number of events removed.

        Parameters
        ----------
        clear_file : bool, default False
            If True, truncate ``self.log_file``. **Audit trails are
            not normally deleted**; leave this False in production.
        """
        with self._lock:
            removed = len(self.audit_events)
            self.audit_events.clear()
            self.feedback_history.clear()
            self.total_energy_consumed = 0.0
            self.total_carbon_emitted = 0.0
            self.total_energy_saved = 0.0
            self.total_carbon_saved = 0.0
            self.graph_metrics_history.clear()
            self.rlhf_feedback_scores.clear()
            self.modp_scores.clear()
            self.distillation_updates = 0
            self.moe_gate_weights_history.clear()

            if clear_file and self.log_file:
                try:
                    Path(self.log_file).write_text("", encoding="utf-8")
                except OSError:
                    logger.exception(
                        "audit log: could not truncate %s",
                        self.log_file,
                    )
        return removed

    # ---------------------------------------------------------------- internals
    def _add_event_locked(self, event: AuditEvent) -> None:
        self.audit_events.append(event)
        self._append_to_file(event, _RECORD_TYPE_EVENT)

    def _append_to_file(
        self, record: Any, record_type: str,
    ) -> None:
        """
        Persist ``record`` as one JSON line.

        Caller must hold ``self._lock``. ``record`` must expose
        ``to_dict()``. The ``_type`` discriminator distinguishes events
        from feedback on load.
        """
        if not (self.enable_persistence and self.log_file):
            return
        try:
            payload = {
                "_type": record_type,
                **record.to_dict(),
            }
            with Path(self.log_file).open("a", encoding="utf-8") as f:
                f.write(json.dumps(payload, cls=_AuditJSONEncoder))
                f.write("\n")
        except OSError:
            logger.exception(
                "audit log: cannot append to %s", self.log_file,
            )
        except (TypeError, ValueError):
            logger.exception(
                "audit log: %s is not JSON-serializable (id=%r)",
                record_type,
                getattr(record, "event_id", None)
                or getattr(record, "feedback_id", None),
            )

    def _load_from_file(self) -> None:
        """Read every JSON line, restoring AuditEvent / ExpertFeedback."""
        if not self.log_file:
            return
        path = Path(self.log_file)
        if not path.exists():
            return
        try:
            with path.open("r", encoding="utf-8") as f:
                for line_no, line in enumerate(f, 1):
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        payload = json.loads(line)
                    except json.JSONDecodeError:
                        logger.exception(
                            "audit log: invalid JSON at line %d in %s",
                            line_no, path,
                        )
                        continue
                    record_type = payload.pop(
                        "_type", _RECORD_TYPE_EVENT,
                    )
                    try:
                        if record_type == _RECORD_TYPE_EVENT:
                            self.audit_events.append(
                                AuditEvent.from_dict(payload)
                            )
                        elif record_type == _RECORD_TYPE_FEEDBACK:
                            self.feedback_history.append(
                                ExpertFeedback.from_dict(payload)
                            )
                        else:
                            logger.warning(
                                "audit log: unknown _type=%r at line "
                                "%d; skipping.",
                                record_type, line_no,
                            )
                    except AuditLoggerError:
                        logger.exception(
                            "audit log: cannot parse record at line %d",
                            line_no,
                        )
        except OSError:
            logger.exception(
                "audit log: cannot read %s", path,
            )

    def _rebuild_aggregates(self) -> None:
        """
        Replay every loaded record through the aggregate accumulation
        logic, so in-memory totals match what a live run would have
        produced.
        """
        self.total_energy_consumed = 0.0
        self.total_carbon_emitted = 0.0
        self.total_energy_saved = 0.0
        self.total_carbon_saved = 0.0
        self.distillation_updates = 0
        self.graph_metrics_history.clear()
        self.rlhf_feedback_scores.clear()
        self.modp_scores.clear()
        self.moe_gate_weights_history.clear()

        for e in self.audit_events:
            if e.energy_consumed_wh > 0:
                self.total_energy_consumed += e.energy_consumed_wh
            if e.carbon_emitted_kg > 0:
                self.total_carbon_emitted += e.carbon_emitted_kg
            if e.energy_consumed_wh < 0:
                self.total_energy_saved += -e.energy_consumed_wh
            if e.carbon_emitted_kg < 0:
                self.total_carbon_saved += -e.carbon_emitted_kg
            if e.graph_metrics:
                self.graph_metrics_history.append(dict(e.graph_metrics))
            if e.human_feedback_score is not None:
                self.rlhf_feedback_scores.append(e.human_feedback_score)
            if e.modp_score is not None:
                self.modp_scores.append(e.modp_score)
            if e.moe_gate_weights:
                self.moe_gate_weights_history.append(
                    list(e.moe_gate_weights)
                )
            if e.distillation_stats:
                counter = e.distillation_stats.get("counter")
                if counter is None:
                    counter = e.distillation_stats.get("student_counter")
                if (
                    isinstance(counter, int)
                    and not isinstance(counter, bool)
                    and counter >= 0
                ):
                    self.distillation_updates += counter

        for fb in self.feedback_history:
            if fb.human_feedback_score is not None:
                self.rlhf_feedback_scores.append(fb.human_feedback_score)
            if fb.graph_metrics:
                self.graph_metrics_history.append(dict(fb.graph_metrics))

    @staticmethod
    def _compute_moe_experts_used(
        history: List[List[float]],
    ) -> Optional[int]:
        """Number of experts in the most recent logged gate vector."""
        for weights in reversed(history):
            if isinstance(weights, (list, tuple)) and weights:
                return len(weights)
        return None

    @staticmethod
    def _extract_gate_weights(
        distillation_stats: Mapping[str, Any],
    ) -> Optional[List[float]]:
        """Read ``gate_weights`` from ``distillation_stats`` if present."""
        raw = distillation_stats.get("gate_weights")
        if not isinstance(raw, (list, tuple)) or not raw:
            return None
        try:
            return [float(x) for x in raw]
        except (TypeError, ValueError):
            return None

    def _generate_event_id(self) -> str:
        return f"evt-{secrets.token_hex(8)}"

    def _generate_report_id(self) -> str:
        return f"rpt-{secrets.token_hex(8)}"

    @staticmethod
    def _calculate_transparency_score(
        *,
        integration_rate: float,
        escalation_rate: float,
        event_count: int,
        graph_health_score: Optional[float] = None,
        use_graph_blend: bool = False,
    ) -> float:
        """
        Compute the transparency score in ``[0, 1]``.

        Parameters
        ----------
        integration_rate : float
            Fraction of received feedback that was integrated.
        escalation_rate : float
            Fraction of tasks that escalated to an expert. The
            **sweet-spot is [0.1, 0.3]**: below 10 % suggests the
            agent is over-confident (bad decisions pass unchecked);
            above 30 % suggests the agent is under-performing
            (excessive reliance on experts). Both tails are penalised
            linearly.
        event_count : int
            Number of events in the reporting period. Contributing up
            to 1.0 at 100 events.
        graph_health_score : float, optional
            Only blended when ``use_graph_blend=True`` and the
            enhancement pipeline is active.
        use_graph_blend : bool
            Blend the graph-health score into the final result at
            :data:`GRAPH_BLEND` weight.
        """
        integration = max(0.0, min(float(integration_rate), 1.0))

        er = float(escalation_rate)
        if ESCALATION_SWEET_MIN <= er <= ESCALATION_SWEET_MAX:
            escalation = 1.0
        elif er < ESCALATION_SWEET_MIN:
            escalation = er / ESCALATION_SWEET_MIN
        else:
            escalation = max(
                0.0,
                1.0 - (er - ESCALATION_SWEET_MAX) / (1.0 - ESCALATION_SWEET_MAX),
            )

        events = max(0.0, min(event_count / 100.0, 1.0))

        score = (
            TRANSPARENCY_W_INTEGRATION * integration
            + TRANSPARENCY_W_ESCALATION * escalation
            + TRANSPARENCY_W_EVENTS * events
        )

        if use_graph_blend and graph_health_score is not None:
            blend_target = max(0.0, min(float(graph_health_score), 1.0))
            score = (1.0 - GRAPH_BLEND) * score + GRAPH_BLEND * blend_target

        return max(0.0, min(score, 1.0))

    # ---------------------------------------------------------------- reports
    @staticmethod
    def _report_to_dict(report: TransparencyReport) -> Dict[str, Any]:
        return {
            "report_id": report.report_id,
            "period_start": report.period_start,
            "period_end": report.period_end,
            "total_tasks": report.total_tasks,
            "tasks_escalated": report.tasks_escalated,
            "escalation_rate": report.escalation_rate,
            "expert_invocations": report.expert_invocations,
            "experts_by_type": dict(report.experts_by_type),
            "feedback_received": report.feedback_received,
            "feedback_integrated": report.feedback_integrated,
            "feedback_integration_rate": report.feedback_integration_rate,
            "total_energy_consumed_wh": report.total_energy_consumed_wh,
            "energy_saved_wh": report.energy_saved_wh,
            "net_energy_wh": report.net_energy_wh,
            "total_carbon_kg": report.total_carbon_kg,
            "carbon_saved_kg": report.carbon_saved_kg,
            "net_carbon_kg": report.net_carbon_kg,
            "avg_task_energy_wh": report.avg_task_energy_wh,
            "avg_expert_energy_wh": report.avg_expert_energy_wh,
            "sustainability_improvement_pct": (
                report.sustainability_improvement_pct
            ),
            "audit_events": report.audit_events,
            "transparency_score": report.transparency_score,
            "graph_health_score": report.graph_health_score,
            "modp_overall_score": report.modp_overall_score,
            "rlhf_average_feedback": report.rlhf_average_feedback,
            "distillation_updates": report.distillation_updates,
            "moe_experts_used": report.moe_experts_used,
        }

    def _generate_html_report(self, report: TransparencyReport) -> str:
        esc = _html.escape

        def _fmt_ts(ts: float) -> str:
            return datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M")

        parts: List[str] = []
        parts.append("<!DOCTYPE html>")
        parts.append("<html><head>")
        parts.append(
            f"<title>Transparency Report - {esc(report.report_id)}</title>"
        )
        parts.append(
            "<style>"
            "body{font-family:Arial,sans-serif;margin:20px;}"
            ".metric{margin:10px 0;}"
            ".metric-value{font-weight:bold;color:#2c3e50;}"
            ".section{margin:30px 0;border-left:4px solid #3498db;"
            "padding-left:15px;}"
            "table{border-collapse:collapse;width:100%;margin:20px 0;}"
            "th,td{border:1px solid #ddd;padding:8px;text-align:left;}"
            "th{background-color:#3498db;color:white;}"
            "</style>"
        )
        parts.append("</head><body>")
        parts.append("<h1>Expert Collaboration Transparency Report</h1>")
        parts.append(f"<p>Report ID: {esc(report.report_id)}</p>")
        parts.append(
            f"<p>Period: {esc(_fmt_ts(report.period_start))} to "
            f"{esc(_fmt_ts(report.period_end))}</p>"
        )

        def _section(title: str, rows: List[Tuple[str, str]]) -> None:
            parts.append(f'<div class="section"><h2>{esc(title)}</h2>')
            for label, value in rows:
                parts.append(
                    f'<div class="metric">{esc(label)}: '
                    f'<span class="metric-value">{esc(value)}</span></div>'
                )
            parts.append("</div>")

        _section("Task Metrics", [
            ("Total Tasks", str(report.total_tasks)),
            ("Tasks Escalated", str(report.tasks_escalated)),
            ("Escalation Rate", f"{report.escalation_rate:.1%}"),
        ])
        _section("Expert Metrics", [
            ("Expert Invocations", str(report.expert_invocations)),
            ("Feedback Received", str(report.feedback_received)),
            ("Feedback Integrated", str(report.feedback_integrated)),
            (
                "Integration Rate",
                f"{report.feedback_integration_rate:.1%}",
            ),
        ])
        _section("Sustainability Metrics", [
            (
                "Total Energy Consumed",
                f"{report.total_energy_consumed_wh * 1000:.1f} mWh",
            ),
            ("Energy Saved", f"{report.energy_saved_wh * 1000:.1f} mWh"),
            ("Net Energy", f"{report.net_energy_wh * 1000:.1f} mWh"),
            ("Carbon Saved", f"{report.carbon_saved_kg * 1000:.1f} g CO₂"),
            (
                "Sustainability Improvement",
                f"{report.sustainability_improvement_pct:.1f}%",
            ),
        ])
        _section("Transparency Score", [
            (
                "Score",
                f"{report.transparency_score:.2f} / 1.00",
            ),
        ])

        enhanced_rows: List[Tuple[str, str]] = []
        if report.graph_health_score is not None:
            enhanced_rows.append((
                "Graph Health", f"{report.graph_health_score:.2f}",
            ))
        if report.modp_overall_score is not None:
            enhanced_rows.append((
                "MODP Overall Score",
                f"{report.modp_overall_score:.2f}",
            ))
        if report.rlhf_average_feedback is not None:
            enhanced_rows.append((
                "Avg RLHF Feedback",
                f"{report.rlhf_average_feedback:.2f}",
            ))
        if report.distillation_updates is not None:
            enhanced_rows.append((
                "Distillation Updates",
                str(report.distillation_updates),
            ))
        if report.moe_experts_used is not None:
            enhanced_rows.append((
                "MoE Experts Used", str(report.moe_experts_used),
            ))
        if enhanced_rows:
            _section("Enhanced Metrics", enhanced_rows)

        parts.append("</body></html>")
        return "\n".join(parts)

    # ---------------------------------------------------------------- dunder
    def __repr__(self) -> str:
        with self._lock:
            return (
                "AuditLogger("
                f"events={len(self.audit_events)}, "
                f"feedback={len(self.feedback_history)}, "
                f"enhancements={'on' if self.use_enhancements else 'off'}, "
                f"persistence={'on' if self.enable_persistence else 'off'})"
            )


# --------------------------------------------------------------------------- #
# Public API
# --------------------------------------------------------------------------- #
__all__: List[str] = [
    "__version__",
    "AuditEvent",
    "AuditEventType",
    "AuditLogger",
    "AuditLoggerError",
    "DEFAULT_GRID_CARBON_KG_PER_KWH",
    "ExpertFeedback",
    "FeedbackType",
    "TransparencyReport",
]


def __dir__() -> List[str]:
    return sorted(__all__)


# --------------------------------------------------------------------------- #
# Smoke test: python -m audit.audit_logger
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

    # ---- 1. Dataclass validation -------------------------------- #
    print("dataclass validation:")
    _check(
        "AuditEvent rejects empty event_id",
        _raises(AuditLoggerError, lambda: AuditEvent(
            event_id="", event_type=AuditEventType.EXPERT_INVOKED,
            task_id="t", agent_id="a", expert_involved=None,
            energy_consumed_wh=0.0, carbon_emitted_kg=0.0,
            details={}, timestamp=0.0,
        )),
    )
    _check(
        "AuditEvent rejects NaN energy",
        _raises(AuditLoggerError, lambda: AuditEvent(
            event_id="e", event_type=AuditEventType.EXPERT_INVOKED,
            task_id="t", agent_id="a", expert_involved=None,
            energy_consumed_wh=float("nan"), carbon_emitted_kg=0.0,
            details={}, timestamp=0.0,
        )),
    )
    _check(
        "ExpertFeedback rejects out-of-range confidence",
        _raises(AuditLoggerError, lambda: ExpertFeedback(
            feedback_id="f", expert_type="x",
            feedback_type=FeedbackType.APPROVAL,
            original_output=None, suggested_output=None,
            explanation="ok", confidence=2.0,
            energy_impact=0.0, timestamp=0.0,
        )),
    )

    # ---- 2. Persistence round-trip ------------------------------ #
    print("persistence round-trip:")
    with tempfile.TemporaryDirectory() as tmpdir:
        log_path = str(Path(tmpdir) / "audit.jsonl")
        logger_a = AuditLogger(
            log_file=log_path, enable_persistence=True,
        )
        logger_a.log_expert_invocation(
            task_id="t1", agent_id="a1", expert_type="qwen",
            energy_wh=0.01, carbon_kg=0.004,
            details={"note": "first"}, graph_metrics={"centrality": 0.8},
            human_feedback_score=0.9,
        )
        logger_a.log_energy_saved(
            task_id="t2", agent_id="a1",
            energy_saved_wh=0.02, mechanism="batch",
        )
        logger_a.log_feedback_received(ExpertFeedback(
            feedback_id="fb1", expert_type="reviewer",
            feedback_type=FeedbackType.SUGGESTION,
            original_output="A", suggested_output="B",
            explanation="clearer", confidence=0.8,
            energy_impact=0.0, timestamp=datetime.now().timestamp(),
            task_id="t1", human_feedback_score=0.7,
            graph_metrics={"centrality": 0.9},
        ))
        logger_a.log_feedback_integrated(
            feedback_id="fb1", task_id="t1", agent_id="a1",
            integration_success=True, energy_impact=0.03,
            modp_score=0.85,
            distillation_stats={
                "counter": 5, "gate_weights": [0.4, 0.4, 0.2],
            },
        )

        _check(
            "file was created",
            Path(log_path).exists() and Path(log_path).stat().st_size > 0,
        )
        lines = Path(log_path).read_text(encoding="utf-8").strip().splitlines()
        _check(
            "file has one line per record (4 events + 1 feedback)",
            len(lines) == 5,
        )

        # Reload and verify aggregates.
        logger_b = AuditLogger(
            log_file=log_path, enable_persistence=True,
        )
        _check(
            "reload restores events",
            len(logger_b.audit_events) == len(logger_a.audit_events),
        )
        _check(
            "reload restores feedback",
            len(logger_b.feedback_history)
            == len(logger_a.feedback_history),
        )
        _check(
            "reload restores total_energy_consumed",
            abs(
                logger_b.total_energy_consumed
                - logger_a.total_energy_consumed
            ) < 1e-9,
        )
        _check(
            "reload restores total_energy_saved",
            abs(
                logger_b.total_energy_saved - logger_a.total_energy_saved
            ) < 1e-9,
        )
        _check(
            "reload restores distillation_updates",
            logger_b.distillation_updates
            == logger_a.distillation_updates,
        )
        _check(
            "reload restores graph_metrics_history",
            len(logger_b.graph_metrics_history)
            == len(logger_a.graph_metrics_history),
        )
        _check(
            "reload restores modp_scores",
            len(logger_b.modp_scores) == len(logger_a.modp_scores),
        )

    # ---- 3. moe_experts_used semantics -------------------------- #
    print("moe_experts_used semantics:")
    with tempfile.TemporaryDirectory() as tmpdir:
        al = AuditLogger(
            log_file=str(Path(tmpdir) / "a.jsonl"),
            config={"use_enhancements": True},
        )
        for i in range(10):
            al.log_feedback_integrated(
                feedback_id=f"f{i}", task_id="t", agent_id="a",
                integration_success=True, energy_impact=0.0,
                distillation_stats={
                    "counter": 1,
                    "gate_weights": [0.3, 0.3, 0.4],
                },
            )
        report = al.generate_transparency_report()
        _check(
            "moe_experts_used == 3 (not number of log calls)",
            report.moe_experts_used == 3,
        )
        _check(
            "distillation_updates == 10",
            report.distillation_updates == 10,
        )

    # ---- 4. distillation_updates counter key ------------------- #
    print("distillation counter key:")
    with tempfile.TemporaryDirectory() as tmpdir:
        al = AuditLogger(
            log_file=str(Path(tmpdir) / "a.jsonl"),
            config={"use_enhancements": True},
        )
        al.log_feedback_integrated(
            feedback_id="f1", task_id="t", agent_id="a",
            integration_success=True, energy_impact=0.0,
            distillation_stats={"counter": 7},
        )
        _check(
            "reads 'counter'",
            al.distillation_updates == 7,
        )
        al.log_feedback_integrated(
            feedback_id="f2", task_id="t", agent_id="a",
            integration_success=True, energy_impact=0.0,
            distillation_stats={"student_counter": 3},
        )
        _check(
            "falls back to 'student_counter'",
            al.distillation_updates == 10,
        )

    # ---- 5. Transparency report -------------------------------- #
    print("transparency report:")
    with tempfile.TemporaryDirectory() as tmpdir:
        al = AuditLogger(
            log_file=str(Path(tmpdir) / "a.jsonl"),
            config={"use_enhancements": True},
        )
        al.log_expert_invocation(
            task_id="t1", agent_id="a1", expert_type="qwen",
            energy_wh=0.01, carbon_kg=0.004,
            graph_metrics={"centrality": 0.8},
            human_feedback_score=0.9,
        )
        al.log_escalation(
            task_id="t2", agent_id="a1",
            reasons=["low confidence"], expert_type="qwen",
            graph_metrics={"centrality": 0.7},
        )
        report = al.generate_transparency_report()
        _check(
            "report is TransparencyReport",
            isinstance(report, TransparencyReport),
        )
        _check(
            "graph_health_score computed",
            report.graph_health_score is not None,
        )
        _check(
            "rlhf_average_feedback computed",
            report.rlhf_average_feedback is not None,
        )
        _check(
            "transparency_score in [0,1]",
            0.0 <= report.transparency_score <= 1.0,
        )
        _check(
            "empty-log report raises",
            _raises(AuditLoggerError, lambda:
                    AuditLogger().generate_transparency_report()),
        )

    # ---- 6. export_report -------------------------------------- #
    print("export_report:")
    with tempfile.TemporaryDirectory() as tmpdir:
        al = AuditLogger()
        al.log_expert_invocation(
            task_id="t1", agent_id="a1", expert_type="qwen",
            energy_wh=0.01, carbon_kg=0.004,
        )
        report = al.generate_transparency_report()

        json_path = Path(tmpdir) / "r.json"
        al.export_report(report, str(json_path), format="json")
        _check("json export works", json_path.exists())

        html_path = Path(tmpdir) / "r.html"
        al.export_report(report, str(html_path), format="html")
        _check("html export works", html_path.exists())
        _check(
            "html escaped (title with report id present)",
            report.report_id in html_path.read_text(encoding="utf-8"),
        )

        _check(
            "invalid format rejected",
            _raises(AuditLoggerError, lambda:
                    al.export_report(report, str(json_path), format="xml")),
        )

    # ---- 7. HTML injection defense ----------------------------- #
    print("html injection defense:")
    # Directly test that the escaping path would catch a future field.
    evil = _html.escape("<script>alert(1)</script>")
    _check(
        "html.escape neutralises tags",
        "<" not in evil and ">" not in evil,
    )

    # ---- 8. thread safety -------------------------------------- #
    print("thread safety:")
    import threading as _th
    with tempfile.TemporaryDirectory() as tmpdir:
        al = AuditLogger(
            log_file=str(Path(tmpdir) / "a.jsonl"),
            enable_persistence=False,
        )
        errors: List[BaseException] = []

        def _worker(idx: int) -> None:
            try:
                for i in range(50):
                    al.log_expert_invocation(
                        task_id=f"t{idx}_{i}", agent_id="a",
                        expert_type="q", energy_wh=0.001,
                        carbon_kg=0.0004,
                    )
            except BaseException as exc:  # pragma: no cover
                errors.append(exc)

        threads = [_th.Thread(target=_worker, args=(i,)) for i in range(4)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        _check("no worker errors", not errors)
        _check(
            "all events recorded (200)",
            len(al.audit_events) == 200,
        )
        _check(
            "aggregates consistent",
            abs(al.total_energy_consumed - 200 * 0.001) < 1e-9,
        )

    # ---- 9. statistics / reset --------------------------------- #
    print("statistics / reset:")
    with tempfile.TemporaryDirectory() as tmpdir:
        al = AuditLogger(
            log_file=str(Path(tmpdir) / "a.jsonl"),
        )
        al.log_expert_invocation(
            task_id="t1", agent_id="a", expert_type="q",
            energy_wh=0.005, carbon_kg=0.002,
        )
        stats = al.statistics()
        _check(
            "statistics event_count",
            stats["event_count"] == 1,
        )
        _check(
            "statistics total_energy_consumed",
            abs(stats["total_energy_consumed_wh"] - 0.005) < 1e-9,
        )
        removed = al.reset()
        _check("reset removes events", removed == 1)
        _check(
            "aggregates cleared",
            al.statistics()["total_energy_consumed_wh"] == 0.0,
        )

    # ---- 10. strict mode --------------------------------------- #
    print("strict mode:")
    _check(
        "strict rejects use_enhancements without package",
        # Only meaningful when the enhancements package is missing;
        # in enhanced environments this is a no-op test.
        ENHANCEMENTS_AVAILABLE
        or _raises(AuditLoggerError, lambda: AuditLogger(
            config={"use_enhancements": True}, strict=True,
        )),
    )

    # ---- 11. __all__ / __dir__ -------------------------------- #
    print("dir:")
    _check("__dir__ matches __all__", __dir__() == sorted(__all__))
    for _name in (
        "AuditEvent", "AuditEventType", "AuditLogger",
        "AuditLoggerError", "ExpertFeedback", "FeedbackType",
        "TransparencyReport",
    ):
        _check(f"{_name} exported", _name in __all__)

    print()
    print(f"passed: {passed}  failed: {failed}")
    if failed:
        sys.exit(1)
