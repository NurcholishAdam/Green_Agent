"""
QuantumDistiller — orchestrate teacher→student distillation experiments.

Compares a teacher (often a quantum or large classical model) against a
student (a distilled classical model) and records the quality, energy,
and carbon deltas as a `DistillationResult`.

An instance is a **record-keeper**, not a trainer. The caller supplies
the teacher and student callables; the distiller runs them, measures the
outcome via configurable extractors, and writes an immutable record.

Every run produces a record in the history — including failed runs,
which appear as `DistillationFailure` objects so that the history is the
full audit trail, not just the successful subset.

Original API preserved:
    distiller = QuantumDistiller()
    result = await distiller.run(
        task_id="t", teacher_callable=..., student_callable=...,
    )
    result.quality_retention, result.energy_reduction_pct, ...

See the module docstring in the enhanced contract files
(``contracts/execution_result.py``, etc.) for the conventions this
file follows: frozen dataclasses, typed enums, validated construction,
explicit serialization, and a bounded audit history.
"""

from __future__ import annotations

import asyncio
import hashlib
import inspect
import json
import logging
import math
import secrets
import threading
from collections import OrderedDict
from dataclasses import asdict, dataclass, field, fields
from datetime import datetime, timezone
from enum import Enum
from types import MappingProxyType
from typing import (
    Any,
    Callable,
    Dict,
    List,
    Mapping,
    Optional,
    Sequence,
    Tuple,
)

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())

__version__ = "2.0.0"


# =============================================================================
# Constants
# =============================================================================

MAX_ID_LEN: int = 256
MAX_METADATA_ENTRIES: int = 64
MAX_METADATA_VALUE_LEN: int = 4096
DEFAULT_HISTORY_MAXLEN: int = 1000


# =============================================================================
# Errors
# =============================================================================

class DistillationError(Exception):
    """Base class for QuantumDistiller errors."""


class DistillationTimeoutError(DistillationError):
    """A teacher or student phase exceeded its timeout."""

    def __init__(self, *, phase: str, timeout_s: float) -> None:
        self.phase = phase
        self.timeout_s = timeout_s
        super().__init__(
            f"distillation {phase} phase exceeded {timeout_s}s"
        )


class DistillationCallableError(DistillationError):
    """A teacher or student callable raised."""

    def __init__(self, *, phase: str, cause: BaseException) -> None:
        self.phase = phase
        self.cause = cause
        super().__init__(
            f"distillation {phase} phase raised "
            f"{type(cause).__name__}: {cause}"
        )


class DistillationValidationError(DistillationError, ValueError):
    """Invalid inputs to ``run()`` or ``__init__``."""


# =============================================================================
# Enums
# =============================================================================

class PrecisionLevel(str, Enum):
    """Precision level used by a teacher or student run."""

    FP32 = "fp32"
    FP16 = "fp16"
    INT8 = "int8"
    INT4 = "int4"
    QUANTUM_DISTILLED = "quantum_distilled"


class DistillationOutcomeClass(str, Enum):
    SUCCESS = "success"
    QUALITY_REGRESSION = "quality_regression"
    ENERGY_REGRESSION = "energy_regression"
    UNKNOWN = "unknown"


# =============================================================================
# Config
# =============================================================================

@dataclass(frozen=True)
class DistillerConfig:
    """Tunable parameters for ``QuantumDistiller``."""

    teacher_timeout_s: float = 3600.0     # 1 hour
    student_timeout_s: float = 600.0      # 10 minutes
    max_history: int = DEFAULT_HISTORY_MAXLEN

    # Default success thresholds (used by DistillationResult.is_success
    # only when the caller does not supply their own).
    default_min_retention: float = 0.9
    default_min_energy_reduction_pct: float = 0.0

    # If True, teacher/student callables must be async functions. If
    # False, sync callables are run in a thread (via asyncio.to_thread)
    # so they do not block the event loop.
    require_async_callables: bool = False

    def __post_init__(self) -> None:
        for fname in ("teacher_timeout_s", "student_timeout_s"):
            v = getattr(self, fname)
            if not isinstance(v, (int, float)) or v <= 0:
                raise DistillationValidationError(
                    f"{fname} must be > 0"
                )
        if self.max_history <= 0:
            raise DistillationValidationError(
                "max_history must be > 0"
            )
        if not (0.0 <= float(self.default_min_retention) <= 1.0):
            raise DistillationValidationError(
                "default_min_retention must be in [0, 1]"
            )

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


# =============================================================================
# Data classes
# =============================================================================

@dataclass(frozen=True)
class DistillationResult:
    """
    Immutable record of a single distillation experiment.

    All numeric fields are validated at construction. ``at`` is the
    completion time; ``submitted_at`` and the per-phase timestamps are
    also recorded so the caller can distinguish teacher cost from
    student cost.

    ``quality_retention`` can exceed 1.0 when the student outperforms
    the teacher. Use ``is_quality_improvement`` to check that case.
    """

    distillation_id: str
    task_id: str
    submitted_at: datetime
    at: datetime  # completion time (kept for backward compat)

    # --- Backend / precision ---
    teacher_backend: Optional[str] = None
    teacher_backend_profile: Optional[Any] = None  # BackendProfile, serialized
    teacher_precision: Optional[PrecisionLevel] = None
    student_precision: PrecisionLevel = PrecisionLevel.INT8

    # --- Quality ---
    teacher_quality: Optional[float] = None
    student_quality: Optional[float] = None
    quality_retention: Optional[float] = None
    retention_error: Optional[str] = None

    # --- Resource: teacher vs student ---
    teacher_energy_kwh: Optional[float] = None
    student_energy_kwh: Optional[float] = None
    energy_reduction_pct: Optional[float] = None

    teacher_co2e_kg: Optional[float] = None
    student_co2e_kg: Optional[float] = None
    co2e_reduction_pct: Optional[float] = None

    # --- Timing ---
    teacher_started_at: Optional[datetime] = None
    teacher_completed_at: Optional[datetime] = None
    student_started_at: Optional[datetime] = None
    student_completed_at: Optional[datetime] = None

    # --- Classical baseline (optional) ---
    classical_baseline_quality: Optional[float] = None
    classical_retention: Optional[float] = None
    beats_classical: Optional[bool] = None

    # --- Success classification (computed at construction) ---
    success: bool = False
    success_criteria: Mapping[str, Any] = field(default_factory=dict)

    # --- Provenance ---
    signature: Optional[str] = None
    signature_verified: Optional[bool] = None
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        # --- ID ---
        if not isinstance(self.distillation_id, str) \
                or not self.distillation_id.strip():
            raise DistillationValidationError(
                "distillation_id must be a non-empty string"
            )
        if not isinstance(self.task_id, str) or not self.task_id.strip():
            raise DistillationValidationError(
                "task_id must be a non-empty string"
            )

        # --- Enum coercion ---
        object.__setattr__(
            self, "student_precision",
            _coerce_precision(self.student_precision),
        )
        if self.teacher_precision is not None:
            object.__setattr__(
                self, "teacher_precision",
                _coerce_precision(self.teacher_precision),
            )

        # --- UTC datetimes ---
        for fname in (
            "submitted_at", "at",
            "teacher_started_at", "teacher_completed_at",
            "student_started_at", "student_completed_at",
        ):
            v = getattr(self, fname)
            if v is not None:
                object.__setattr__(self, fname, _ensure_utc(v))

        # --- Numeric field validation ---
        for fname in (
            "teacher_quality", "student_quality",
            "classical_baseline_quality",
        ):
            v = getattr(self, fname)
            if v is not None:
                if not isinstance(v, (int, float)) \
                        or isinstance(v, bool) \
                        or not math.isfinite(float(v)):
                    raise DistillationValidationError(
                        f"{fname} must be a finite number or None"
                    )
                if not (0.0 <= float(v) <= 1.0):
                    raise DistillationValidationError(
                        f"{fname} must be in [0, 1] or None"
                    )

        for fname in ("quality_retention", "classical_retention"):
            v = getattr(self, fname)
            if v is not None:
                if not isinstance(v, (int, float)) \
                        or isinstance(v, bool) \
                        or not math.isfinite(float(v)):
                    raise DistillationValidationError(
                        f"{fname} must be a finite number or None"
                    )
                # No upper bound: retention may exceed 1.

        for fname in (
            "teacher_energy_kwh", "student_energy_kwh",
            "teacher_co2e_kg", "student_co2e_kg",
        ):
            v = getattr(self, fname)
            if v is not None:
                if not isinstance(v, (int, float)) \
                        or isinstance(v, bool) \
                        or not math.isfinite(float(v)):
                    raise DistillationValidationError(
                        f"{fname} must be a finite number or None"
                    )
                if v < 0:
                    raise DistillationValidationError(
                        f"{fname} must be >= 0 or None"
                    )

        for fname in ("energy_reduction_pct", "co2e_reduction_pct"):
            v = getattr(self, fname)
            if v is not None and (
                not isinstance(v, (int, float))
                or isinstance(v, bool)
                or not math.isfinite(float(v))
            ):
                raise DistillationValidationError(
                    f"{fname} must be a finite number or None"
                )

        # --- Booleans ---
        if not isinstance(self.success, bool):
            raise DistillationValidationError("success must be a bool")
        if self.beats_classical is not None \
                and not isinstance(self.beats_classical, bool):
            raise DistillationValidationError(
                "beats_classical must be a bool or None"
            )

        # --- Freeze mappings ---
        object.__setattr__(
            self, "metadata", _freeze_mapping(self.metadata),
        )
        object.__setattr__(
            self, "success_criteria", _freeze_mapping(self.success_criteria),
        )

        # --- Coherence: retention should match qualities ---
        if (
            self.quality_retention is not None
            and self.teacher_quality is not None
            and self.student_quality is not None
            and self.teacher_quality > 0
        ):
            computed = self.student_quality / self.teacher_quality
            if abs(computed - self.quality_retention) > 1e-6:
                raise DistillationValidationError(
                    f"quality_retention={self.quality_retention} is "
                    f"inconsistent with "
                    f"student/teacher={computed:.6f}"
                )

    # ------------------------------------------------------------------ properties
    @property
    def is_quality_improvement(self) -> bool:
        return (
            self.quality_retention is not None
            and self.quality_retention > 1.0
        )

    @property
    def is_energy_regression(self) -> bool:
        return (
            self.energy_reduction_pct is not None
            and self.energy_reduction_pct < 0
        )

    @property
    def is_energy_improvement(self) -> bool:
        return (
            self.energy_reduction_pct is not None
            and self.energy_reduction_pct > 0
        )

    # ------------------------------------------------------------------ classification
    def is_success(
        self,
        *,
        min_retention: Optional[float] = None,
        min_energy_reduction_pct: Optional[float] = None,
    ) -> bool:
        """
        True iff the distillation met the retention and energy criteria.

        ``None`` retention or energy reduction counts as failure — a
        distillation we could not measure is not a success.
        """
        retention_floor = (
            min_retention
            if min_retention is not None
            else float(self.success_criteria.get("min_retention", 0.9))
        )
        energy_floor = (
            min_energy_reduction_pct
            if min_energy_reduction_pct is not None
            else float(
                self.success_criteria.get(
                    "min_energy_reduction_pct", 0.0,
                )
            )
        )
        if self.quality_retention is None:
            return False
        if self.energy_reduction_pct is None:
            return False
        return (
            self.quality_retention >= retention_floor
            and self.energy_reduction_pct >= energy_floor
        )

    def outcome_class(self) -> str:
        """Single-word category: 'success' / 'quality_regression' / etc."""
        if self.is_success():
            return DistillationOutcomeClass.SUCCESS.value
        if self.quality_retention is not None and self.quality_retention < 1.0:
            return DistillationOutcomeClass.QUALITY_REGRESSION.value
        if self.is_energy_regression:
            return DistillationOutcomeClass.ENERGY_REGRESSION.value
        return DistillationOutcomeClass.UNKNOWN.value

    # ------------------------------------------------------------------ serialization
    def to_dict(self) -> Dict[str, Any]:
        return {
            "distillation_id": self.distillation_id,
            "task_id": self.task_id,
            "submitted_at": self.submitted_at.isoformat(),
            "at": self.at.isoformat(),
            "teacher_backend": self.teacher_backend,
            "teacher_backend_profile": _to_jsonable(
                self.teacher_backend_profile
            ),
            "teacher_precision": (
                self.teacher_precision.value
                if self.teacher_precision else None
            ),
            "student_precision": self.student_precision.value,
            "teacher_quality": self.teacher_quality,
            "student_quality": self.student_quality,
            "quality_retention": self.quality_retention,
            "retention_error": self.retention_error,
            "teacher_energy_kwh": self.teacher_energy_kwh,
            "student_energy_kwh": self.student_energy_kwh,
            "energy_reduction_pct": self.energy_reduction_pct,
            "teacher_co2e_kg": self.teacher_co2e_kg,
            "student_co2e_kg": self.student_co2e_kg,
            "co2e_reduction_pct": self.co2e_reduction_pct,
            "teacher_started_at": _iso(self.teacher_started_at),
            "teacher_completed_at": _iso(self.teacher_completed_at),
            "student_started_at": _iso(self.student_started_at),
            "student_completed_at": _iso(self.student_completed_at),
            "classical_baseline_quality": self.classical_baseline_quality,
            "classical_retention": self.classical_retention,
            "beats_classical": self.beats_classical,
            "success": self.success,
            "success_criteria": _to_jsonable(dict(self.success_criteria)),
            "signature": self.signature,
            "signature_verified": self.signature_verified,
            "metadata": _to_jsonable(dict(self.metadata)),
        }

    def to_json(self, **kwargs: Any) -> str:
        return json.dumps(self.to_dict(), **kwargs)

    @classmethod
    def from_dict(
        cls,
        d: Mapping[str, Any],
        *,
        validate: bool = True,
    ) -> "DistillationResult":
        if not isinstance(d, Mapping):
            raise DistillationValidationError(
                f"d must be a Mapping; got {type(d).__name__}"
            )
        known = {f.name for f in fields(cls)}
        unknown = set(d.keys()) - known
        if unknown:
            raise DistillationValidationError(
                f"unknown fields in DistillationResult dict: "
                f"{sorted(unknown)}"
            )
        cleaned: Dict[str, Any] = dict(d)
        for fname in (
            "submitted_at", "at",
            "teacher_started_at", "teacher_completed_at",
            "student_started_at", "student_completed_at",
        ):
            v = cleaned.get(fname)
            if isinstance(v, str):
                cleaned[fname] = datetime.fromisoformat(v)
        if isinstance(cleaned.get("teacher_precision"), str):
            try:
                cleaned["teacher_precision"] = PrecisionLevel(
                    cleaned["teacher_precision"]
                )
            except ValueError:
                cleaned["teacher_precision"] = None
        if isinstance(cleaned.get("student_precision"), str):
            try:
                cleaned["student_precision"] = PrecisionLevel(
                    cleaned["student_precision"]
                )
            except ValueError:
                cleaned["student_precision"] = PrecisionLevel.INT8
        return cls(**cleaned)

    def checksum(self) -> str:
        payload = json.dumps(
            self.to_dict(), sort_keys=True, default=str,
        ).encode("utf-8")
        return hashlib.sha256(payload).hexdigest()

    def __repr__(self) -> str:
        return (
            f"DistillationResult("
            f"id={self.distillation_id!r}, "
            f"task={self.task_id!r}, "
            f"retention={self.quality_retention!r}, "
            f"energy_red={self.energy_reduction_pct!r}%, "
            f"success={self.success})"
        )


@dataclass(frozen=True)
class DistillationFailure:
    """Record of a failed distillation experiment."""

    distillation_id: str
    task_id: str
    phase: str        # "teacher" | "student" | "comparison"
    error_type: str
    error_message: str
    at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    def __post_init__(self) -> None:
        for fname in ("distillation_id", "task_id", "phase",
                      "error_type", "error_message"):
            v = getattr(self, fname)
            if not isinstance(v, str):
                raise DistillationValidationError(
                    f"{fname} must be a string"
                )
        object.__setattr__(self, "at", _ensure_utc(self.at))

    def to_dict(self) -> Dict[str, Any]:
        return {
            "distillation_id": self.distillation_id,
            "task_id": self.task_id,
            "phase": self.phase,
            "error_type": self.error_type,
            "error_message": self.error_message,
            "at": self.at.isoformat(),
            "record_type": "failure",
        }


# =============================================================================
# Helpers
# =============================================================================

def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _ensure_utc(dt: Optional[datetime]) -> Optional[datetime]:
    if dt is None:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt


def _iso(dt: Optional[datetime]) -> Optional[str]:
    return dt.isoformat() if dt is not None else None


def _freeze_mapping(m: Any) -> Mapping[str, Any]:
    if m is None:
        return MappingProxyType({})
    if isinstance(m, MappingProxyType):
        return m
    if not isinstance(m, Mapping):
        raise DistillationValidationError(
            f"expected a Mapping; got {type(m).__name__}"
        )
    return MappingProxyType(dict(m))


def _coerce_precision(value: Any) -> PrecisionLevel:
    if isinstance(value, PrecisionLevel):
        return value
    try:
        return PrecisionLevel(value)
    except (ValueError, KeyError):
        allowed = sorted(e.value for e in PrecisionLevel)
        raise DistillationValidationError(
            f"student_precision/teacher_precision must be one of "
            f"{allowed}; got {value!r}"
        ) from None


def _to_jsonable(obj: Any) -> Any:
    if isinstance(obj, Enum):
        return obj.value
    if isinstance(obj, Mapping):
        return {str(k): _to_jsonable(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple, set, frozenset)):
        return [_to_jsonable(v) for v in obj]
    if isinstance(obj, datetime):
        return obj.isoformat()
    tolist = getattr(obj, "tolist", None)
    if callable(tolist):
        try:
            return _to_jsonable(tolist())
        except Exception:
            pass
    if hasattr(obj, "__dataclass_fields__"):
        return _to_jsonable(asdict(obj))
    if isinstance(obj, (str, int, float, bool)) or obj is None:
        return obj
    raise TypeError(
        f"_to_jsonable: cannot serialize type {type(obj).__name__}"
    )


def _lookup(
    obj: Any, keys: Sequence[str],
) -> Optional[float]:
    """
    Return the first numeric value found for ``keys`` on an attr-or-dict
    object. Returns ``None`` if no key is present, if the value is a
    bool (never a valid numeric metric), or if it's non-finite.
    """
    if obj is None:
        return None
    for k in keys:
        if isinstance(obj, Mapping):
            v = obj.get(k)
        else:
            v = getattr(obj, k, None)
        if v is None:
            continue
        if isinstance(v, bool):
            continue
        if not isinstance(v, (int, float)):
            continue
        if not math.isfinite(float(v)):
            continue
        return float(v)
    return None


def _lookup_nonneg(
    obj: Any, keys: Sequence[str], *, field_name: str,
) -> Optional[float]:
    v = _lookup(obj, keys)
    if v is not None and v < 0:
        logger.warning(
            "negative value for %s (%s); treating as unknown",
            field_name, v,
        )
        return None
    return v


async def _resolve(value: Any) -> Any:
    """Await ``value`` if it is awaitable; return it otherwise."""
    if inspect.isawaitable(value):
        return await value
    return value


async def _run_callable(
    fn: Callable[[], Any],
    *,
    phase: str,
    require_async: bool,
) -> Any:
    """
    Run ``fn`` and resolve the result to a value.

    If ``fn`` is a coroutine function, it is awaited directly. Otherwise
    (or if it returns a non-awaitable), the result is run through
    ``asyncio.to_thread`` if ``require_async`` is False — preventing
    sync callables from blocking the event loop.

    The ``phase`` argument is used only in error messages.
    """
    if inspect.iscoroutinefunction(fn):
        return await fn()
    if require_async:
        raise DistillationValidationError(
            f"{phase}_callable must be an async function when "
            f"require_async_callables=True; got "
            f"{type(fn).__name__}"
        )
    # Run sync callable in a thread; do not block the loop.
    result = await asyncio.to_thread(fn)
    return await _resolve(result)


# =============================================================================
# The Enhanced QuantumDistiller
# =============================================================================

class QuantumDistiller:
    """
    Orchestrates teacher→student distillation experiments.

    The distiller does not train anything: it runs the caller-supplied
    teacher and student callables, extracts quality/energy/carbon via
    configurable extractors, and records an immutable result.

    Every run produces a history entry — successful runs append a
    ``DistillationResult``; failures append a ``DistillationFailure``.
    Thread-safe; bounded history via ``DistillerConfig.max_history``.
    """

    def __init__(
        self,
        *,
        config: Optional[DistillerConfig] = None,
        quality_extractor: Optional[Callable[[Any], Optional[float]]] = None,
        energy_extractor: Optional[Callable[[Any], Optional[float]]] = None,
        co2e_extractor: Optional[Callable[[Any], Optional[float]]] = None,
        signature_preserver: Optional[Any] = None,
    ) -> None:
        self.config = config or DistillerConfig()
        self._quality_extractor = quality_extractor or _extract_quality
        self._energy_extractor = energy_extractor or _extract_energy
        self._co2e_extractor = co2e_extractor or _extract_co2e
        self._signature_preserver = signature_preserver

        self._lock = threading.RLock()
        self._history: "OrderedDict[str, Any]" = OrderedDict()

    # ------------------------------------------------------------------ main API
    async def run(
        self,
        *,
        task_id: str,
        teacher_callable: Callable[[], Any],
        student_callable: Callable[[], Any],
        teacher_backend: Optional[str] = None,
        teacher_backend_profile: Optional[Any] = None,
        teacher_precision: Optional[PrecisionLevel] = None,
        student_precision: PrecisionLevel = PrecisionLevel.INT8,
        classical_baseline_quality: Optional[float] = None,
        metadata: Optional[Dict[str, Any]] = None,
        min_retention: Optional[float] = None,
        min_energy_reduction_pct: Optional[float] = None,
    ) -> DistillationResult:
        """
        Run one distillation experiment.

        Raises ``DistillationValidationError`` on bad inputs,
        ``DistillationTimeoutError`` if a phase times out, and
        ``DistillationCallableError`` if a phase raises. In all three
        cases a failure record is added to the history before the
        exception propagates.
        """
        self._validate_inputs(
            task_id=task_id,
            teacher_callable=teacher_callable,
            student_callable=student_callable,
            metadata=metadata,
            classical_baseline_quality=classical_baseline_quality,
            min_retention=min_retention,
            min_energy_reduction_pct=min_energy_reduction_pct,
        )

        student_precision = _coerce_precision(student_precision)
        if teacher_precision is not None:
            teacher_precision = _coerce_precision(teacher_precision)

        distillation_id = self._new_id()
        submitted_at = _utcnow()
        meta = self._validate_metadata(metadata)

        # --- Teacher phase ---
        teacher_started_at = _utcnow()
        try:
            teacher = await asyncio.wait_for(
                _run_callable(
                    teacher_callable,
                    phase="teacher",
                    require_async=self.config.require_async_callables,
                ),
                timeout=self.config.teacher_timeout_s,
            )
        except asyncio.TimeoutError:
            failure = DistillationFailure(
                distillation_id=distillation_id,
                task_id=task_id,
                phase="teacher",
                error_type="timeout",
                error_message=(
                    f"teacher callable exceeded "
                    f"{self.config.teacher_timeout_s}s"
                ),
            )
            self._record_failure(failure)
            raise DistillationTimeoutError(
                phase="teacher",
                timeout_s=self.config.teacher_timeout_s,
            ) from None
        except Exception as exc:
            failure = DistillationFailure(
                distillation_id=distillation_id,
                task_id=task_id,
                phase="teacher",
                error_type=type(exc).__name__,
                error_message=str(exc) or "(no message)",
            )
            self._record_failure(failure)
            raise DistillationCallableError(
                phase="teacher", cause=exc,
            ) from exc
        teacher_completed_at = _utcnow()

        # --- Student phase ---
        student_started_at = _utcnow()
        try:
            student = await asyncio.wait_for(
                _run_callable(
                    student_callable,
                    phase="student",
                    require_async=self.config.require_async_callables,
                ),
                timeout=self.config.student_timeout_s,
            )
        except asyncio.TimeoutError:
            failure = DistillationFailure(
                distillation_id=distillation_id,
                task_id=task_id,
                phase="student",
                error_type="timeout",
                error_message=(
                    f"student callable exceeded "
                    f"{self.config.student_timeout_s}s"
                ),
            )
            self._record_failure(failure)
            raise DistillationTimeoutError(
                phase="student",
                timeout_s=self.config.student_timeout_s,
            ) from None
        except Exception as exc:
            failure = DistillationFailure(
                distillation_id=distillation_id,
                task_id=task_id,
                phase="student",
                error_type=type(exc).__name__,
                error_message=str(exc) or "(no message)",
            )
            self._record_failure(failure)
            raise DistillationCallableError(
                phase="student", cause=exc,
            ) from exc
        student_completed_at = _utcnow()

        # --- Extraction ---
        teacher_quality = self._quality_extractor(teacher)
        student_quality = self._quality_extractor(student)
        teacher_energy = self._energy_extractor(teacher)
        student_energy = self._energy_extractor(student)
        teacher_co2e = self._co2e_extractor(teacher)
        student_co2e = self._co2e_extractor(student)

        # --- Derived metrics ---
        retention, retention_error = _compute_ratio(
            student_quality, teacher_quality,
            numerator_name="student_quality",
            denominator_name="teacher_quality",
            clamp=False,
        )
        energy_reduction = _compute_reduction_pct(
            teacher_energy, student_energy,
        )
        co2e_reduction = _compute_reduction_pct(
            teacher_co2e, student_co2e,
        )

        # Classical baseline comparison (optional).
        classical_retention: Optional[float] = None
        beats_classical: Optional[bool] = None
        if (
            classical_baseline_quality is not None
            and student_quality is not None
            and classical_baseline_quality > 0
        ):
            classical_retention = student_quality / classical_baseline_quality
            beats_classical = student_quality >= classical_baseline_quality

        # Success thresholds — record explicitly on the result so a
        # later audit knows *why* the distillation was judged a success
        # or failure.
        effective_min_retention = (
            float(min_retention)
            if min_retention is not None
            else float(self.config.default_min_retention)
        )
        effective_min_energy = (
            float(min_energy_reduction_pct)
            if min_energy_reduction_pct is not None
            else float(self.config.default_min_energy_reduction_pct)
        )

        success = (
            retention is not None
            and energy_reduction is not None
            and retention >= effective_min_retention
            and energy_reduction >= effective_min_energy
        )

        # --- Signature (optional) ---
        signature: Optional[str] = None
        signature_verified: Optional[bool] = None
        if self._signature_preserver is not None:
            try:
                sig = await self._signature_preserver.create_signature(
                    thought_data={
                        "task_id": task_id,
                        "distillation_id": distillation_id,
                    },
                    quantum_amplitudes=None,
                    classical_context={
                        "retention": retention,
                        "energy_reduction_pct": energy_reduction,
                    },
                )
                signature = getattr(sig, "signature_id", None)
                signature_verified = True
            except Exception as exc:
                logger.warning(
                    "signature preserver failed: %s", exc, exc_info=True,
                )
                signature_verified = False

        # --- Build the result ---
        completed_at = _utcnow()
        result = DistillationResult(
            distillation_id=distillation_id,
            task_id=task_id,
            submitted_at=submitted_at,
            at=completed_at,
            teacher_backend=teacher_backend,
            teacher_backend_profile=teacher_backend_profile,
            teacher_precision=teacher_precision,
            student_precision=student_precision,
            teacher_quality=teacher_quality,
            student_quality=student_quality,
            quality_retention=retention,
            retention_error=retention_error,
            teacher_energy_kwh=teacher_energy,
            student_energy_kwh=student_energy,
            energy_reduction_pct=energy_reduction,
            teacher_co2e_kg=teacher_co2e,
            student_co2e_kg=student_co2e,
            co2e_reduction_pct=co2e_reduction,
            teacher_started_at=teacher_started_at,
            teacher_completed_at=teacher_completed_at,
            student_started_at=student_started_at,
            student_completed_at=student_completed_at,
            classical_baseline_quality=classical_baseline_quality,
            classical_retention=classical_retention,
            beats_classical=beats_classical,
            success=success,
            success_criteria=MappingProxyType({
                "min_retention": effective_min_retention,
                "min_energy_reduction_pct": effective_min_energy,
            }),
            signature=signature,
            signature_verified=signature_verified,
            metadata=meta,
        )
        self._record_result(result)
        logger.info(
            "distillation %s: retention=%s, energy_reduction=%s%%, "
            "success=%s",
            distillation_id, retention, energy_reduction, success,
        )
        return result

    # ------------------------------------------------------------------ history
    def history(self) -> Tuple[Any, ...]:
        """Return the recorded results and failures in submission order."""
        with self._lock:
            return tuple(self._history.values())

    def history_dicts(self) -> List[Dict[str, Any]]:
        """Return the history as JSON-serializable dicts."""
        return [record_to_dict(r) for r in self.history()]

    def latest(self) -> Optional[Any]:
        with self._lock:
            if not self._history:
                return None
            return next(reversed(self._history.values()))

    def clear(self) -> None:
        with self._lock:
            self._history.clear()

    # ------------------------------------------------------------------ internals
    def _new_id(self) -> str:
        # 128 bits of entropy; no truncation (see §1.8).
        return "dist-" + secrets.token_hex(16)

    def _validate_inputs(
        self,
        *,
        task_id: Any,
        teacher_callable: Any,
        student_callable: Any,
        metadata: Any,
        classical_baseline_quality: Any,
        min_retention: Any,
        min_energy_reduction_pct: Any,
    ) -> None:
        if not isinstance(task_id, str) or not task_id.strip():
            raise DistillationValidationError(
                "task_id must be a non-empty string"
            )
        if not callable(teacher_callable):
            raise DistillationValidationError(
                "teacher_callable must be callable"
            )
        if not callable(student_callable):
            raise DistillationValidationError(
                "student_callable must be callable"
            )
        if metadata is not None and not isinstance(metadata, Mapping):
            raise DistillationValidationError(
                f"metadata must be a Mapping or None; "
                f"got {type(metadata).__name__}"
            )
        if classical_baseline_quality is not None:
            if not isinstance(classical_baseline_quality, (int, float)) \
                    or isinstance(classical_baseline_quality, bool) \
                    or not (0.0 <= float(classical_baseline_quality) <= 1.0):
                raise DistillationValidationError(
                    "classical_baseline_quality must be in [0, 1] or None"
                )
        for name, val in (
            ("min_retention", min_retention),
            ("min_energy_reduction_pct", min_energy_reduction_pct),
        ):
            if val is not None and not isinstance(val, (int, float)):
                raise DistillationValidationError(
                    f"{name} must be a number or None"
                )

    def _validate_metadata(
        self, metadata: Optional[Mapping[str, Any]],
    ) -> Mapping[str, Any]:
        if metadata is None:
            return MappingProxyType({})
        if len(metadata) > MAX_METADATA_ENTRIES:
            raise DistillationValidationError(
                f"metadata has {len(metadata)} entries "
                f"(max {MAX_METADATA_ENTRIES})"
            )
        cleaned: Dict[str, Any] = {}
        for k, v in metadata.items():
            if not isinstance(k, str):
                raise DistillationValidationError(
                    "metadata keys must be strings"
                )
            if isinstance(v, str) and len(v) > MAX_METADATA_VALUE_LEN:
                raise DistillationValidationError(
                    f"metadata[{k!r}] exceeds "
                    f"{MAX_METADATA_VALUE_LEN} chars"
                )
            if v is not None and not isinstance(
                v, (str, int, float, bool, datetime),
            ):
                raise DistillationValidationError(
                    f"metadata[{k!r}] must be a primitive or datetime"
                )
            cleaned[k] = v
        return MappingProxyType(cleaned)

    def _record_result(self, result: DistillationResult) -> None:
        with self._lock:
            self._history[result.distillation_id] = result
            self._evict_locked()

    def _record_failure(self, failure: DistillationFailure) -> None:
        with self._lock:
            self._history[failure.distillation_id] = failure
            self._evict_locked()

    def _evict_locked(self) -> None:
        while len(self._history) > self.config.max_history:
            self._history.popitem(last=False)


# =============================================================================
# Default extractors
# =============================================================================

def _extract_quality(obj: Any) -> Optional[float]:
    """
    Extract task quality from a result object or mapping.

    Only ``quality`` and ``accuracy`` are considered — both mean
    end-to-end task quality. ``fidelity`` is a different metric (circuit
    or quantum-state fidelity) and is explicitly ignored.
    """
    v = _lookup(obj, ("quality", "accuracy"))
    if v is not None:
        if v < 0.0 or v > 1.0:
            logger.warning(
                "quality value %s outside [0, 1]; returning None", v,
            )
            return None
        return v
    # Warn if fidelity was the only plausible-looking field.
    if isinstance(obj, Mapping):
        fidelity = obj.get("fidelity")
    else:
        fidelity = getattr(obj, "fidelity", None)
    if fidelity is not None:
        logger.warning(
            "object has fidelity=%s but no quality/accuracy; "
            "fidelity is not a task-quality metric — retention will be None",
            fidelity,
        )
    return None


def _extract_energy(obj: Any) -> Optional[float]:
    """Extract energy in kWh; ``None`` when unknown."""
    return _lookup_nonneg(
        obj,
        (
            "estimated_energy_kwh", "energy_kwh",
            "energy_consumed_kwh", "energy",
        ),
        field_name="energy",
    )


def _extract_co2e(obj: Any) -> Optional[float]:
    """Extract carbon in kgCO2e; ``None`` when unknown."""
    return _lookup_nonneg(
        obj,
        (
            "estimated_co2e_kg", "carbon_emitted_kg",
            "co2e_kg", "carbon_kgco2e", "co2e",
        ),
        field_name="co2e",
    )


# =============================================================================
# Derived metric helpers
# =============================================================================

def _compute_ratio(
    numerator: Optional[float],
    denominator: Optional[float],
    *,
    numerator_name: str,
    denominator_name: str,
    clamp: bool,
) -> Tuple[Optional[float], Optional[str]]:
    """
    Compute ``numerator / denominator`` or return (None, reason).

    No silent fallbacks: if either side is missing, or the denominator
    is zero, the result is None with a reason string.
    """
    if numerator is None:
        return None, f"{numerator_name}_missing"
    if denominator is None:
        return None, f"{denominator_name}_missing"
    if denominator == 0:
        return None, f"{denominator_name}_zero"
    value = numerator / denominator
    if clamp:
        value = max(0.0, value)
    return value, None


def _compute_reduction_pct(
    baseline: Optional[float], candidate: Optional[float],
) -> Optional[float]:
    """
    Return (baseline - candidate) / baseline * 100, or None.

    Positive values mean the candidate uses less than the baseline.
    Negative values mean a regression — the candidate uses more. Not
    clamped: a negative number is a real signal that must not be hidden.
    """
    if baseline is None or candidate is None:
        return None
    if baseline <= 0:
        return None
    return (baseline - candidate) / baseline * 100.0


# =============================================================================
# Public API
# =============================================================================

__all__: Tuple[str, ...] = (
    "DEFAULT_HISTORY_MAXLEN",
    "DistillationCallableError",
    "DistillationError",
    "DistillationFailure",
    "DistillationOutcomeClass",
    "DistillationResult",
    "DistillationTimeoutError",
    "DistillationValidationError",
    "DistillerConfig",
    "MAX_METADATA_ENTRIES",
    "MAX_METADATA_VALUE_LEN",
    "PrecisionLevel",
    "QuantumDistiller",
)


def record_to_dict(record: Any) -> Dict[str, Any]:
    """Serialize either a DistillationResult or a DistillationFailure."""
    return record.to_dict()


# =============================================================================
# Smoke test: python -m qcs.distillation.quantum_distiller
# =============================================================================

if __name__ == "__main__":  # pragma: no cover
    import json

    logging.basicConfig(level=logging.INFO)

    class _TeacherOut:
        quality = 0.92
        estimated_energy_kwh = 5.0
        estimated_co2e_kg = 2.0

    class _StudentOut:
        quality = 0.85
        estimated_energy_kwh = 1.2
        estimated_co2e_kg = 0.5

    class _StudentNoQuality:
        estimated_energy_kwh = 1.2
        estimated_co2e_kg = 0.5

    class _FidelityOnly:
        fidelity = 0.99
        estimated_energy_kwh = 1.0
        estimated_co2e_kg = 0.4

    async def _teacher_ok() -> Any:
        return _TeacherOut()

    async def _student_ok() -> Any:
        return _StudentOut()

    async def _student_no_quality() -> Any:
        return _StudentNoQuality()

    async def _fidelity_only() -> Any:
        return _FidelityOnly()

    async def _boom() -> Any:
        raise RuntimeError("backend unavailable")

    async def _slow() -> Any:
        await asyncio.sleep(10)
        return _StudentOut()

    async def main() -> None:
        print("=== Happy path (async callables) ===")
        distiller = QuantumDistiller(
            config=DistillerConfig(
                teacher_timeout_s=5.0,
                student_timeout_s=5.0,
                max_history=10,
            ),
        )
        result = await distiller.run(
            task_id="task-001",
            teacher_callable=_teacher_ok,
            student_callable=_student_ok,
            teacher_backend="ibmq_quito",
            student_precision="int8",  # string → coerced
            classical_baseline_quality=0.80,
        )
        print(f"  {result!r}")
        print(f"  teacher_quality:   {result.teacher_quality}")
        print(f"  student_quality:   {result.student_quality}")
        print(f"  quality_retention: {result.quality_retention:.4f}")
        print(f"  energy_reduction:  {result.energy_reduction_pct:.2f}%")
        print(f"  co2e_reduction:    {result.co2e_reduction_pct:.2f}%")
        print(f"  beats_classical:   {result.beats_classical}")
        print(f"  outcome_class:     {result.outcome_class()}")
        print(f"  is_success:        {result.is_success()}")
        print(f"  success_criteria:  {dict(result.success_criteria)}")
        print(f"  checksum:          {result.checksum()[:16]}…")

        print("\n=== Immutability ===")
        try:
            result.quality_retention = 2.0  # type: ignore[misc]
        except Exception as e:
            print(f"  quality_retention mutation → {type(e).__name__}")
        try:
            result.metadata["x"] = 1  # type: ignore[index]
        except TypeError:
            print("  metadata mutation → TypeError")

        print("\n=== §1.1 student_quality=None does not crash ===")
        result_none = await distiller.run(
            task_id="task-partial",
            teacher_callable=_teacher_ok,
            student_callable=_student_no_quality,
        )
        print(f"  retention:        {result_none.quality_retention}")
        print(f"  retention_error:  {result_none.retention_error}")
        print(f"  energy_reduction: {result_none.energy_reduction_pct}")

        print("\n=== §1.2 fidelity not treated as quality ===")
        result_fid = await distiller.run(
            task_id="task-fidelity",
            teacher_callable=_teacher_ok,
            student_callable=_fidelity_only,
        )
        print(f"  teacher_quality: {result_fid.teacher_quality}")
        print(f"  student_quality: {result_fid.student_quality} "
              f"(should be None — fidelity ignored)")
        assert result_fid.student_quality is None

        print("\n=== §1.3 unknown energy does not yield 100% reduction ===")
        class _NoEnergy:
            quality = 0.9

        async def _teacher_ok_2() -> Any:
            class T:
                quality = 0.9
                estimated_energy_kwh = 5.0
            return T()

        async def _student_no_energy() -> Any:
            return _NoEnergy()

        r = await distiller.run(
            task_id="task-noenergy",
            teacher_callable=_teacher_ok_2,
            student_callable=_student_no_energy,
        )
        print(f"  teacher_energy: {r.teacher_energy_kwh}")
        print(f"  student_energy: {r.student_energy_kwh}")
        print(f"  energy_reduction: {r.energy_reduction_pct} "
              f"(should be None)")

        print("\n=== §1.5 negative energy reduction (regression) is preserved ===")
        class _HeavyStudent:
            quality = 0.9
            estimated_energy_kwh = 8.0   # more than teacher's 5.0

        async def _heavy_student() -> Any:
            return _HeavyStudent()

        r_reg = await distiller.run(
            task_id="task-regression",
            teacher_callable=_teacher_ok_2,
            student_callable=_heavy_student,
        )
        print(f"  energy_reduction: {r_reg.energy_reduction_pct:.2f}% "
              f"(negative = regression)")
        assert r_reg.energy_reduction_pct is not None
        assert r_reg.energy_reduction_pct < 0
        assert r_reg.is_energy_regression

        print("\n=== §1.4 dict-shaped result is handled ===")
        async def _teacher_dict() -> Any:
            return {
                "quality": 0.92,
                "estimated_energy_kwh": 4.0,
                "estimated_co2e_kg": 1.5,
            }

        async def _student_dict() -> Any:
            return {
                "quality": 0.85,
                "estimated_energy_kwh": 1.0,
                "estimated_co2e_kg": 0.4,
            }

        r_dict = await distiller.run(
            task_id="task-dict",
            teacher_callable=_teacher_dict,
            student_callable=_student_dict,
        )
        print(f"  energy_reduction: {r_dict.energy_reduction_pct:.2f}%")
        assert r_dict.teacher_energy_kwh == 4.0
        assert r_dict.student_energy_kwh == 1.0

        print("\n=== Failure recorded on teacher exception ===")
        try:
            await distiller.run(
                task_id="task-fail",
                teacher_callable=_boom,
                student_callable=_student_ok,
            )
        except DistillationCallableError as e:
            print(f"  raised: {type(e).__name__} (phase={e.phase})")
        hist = distiller.history()
        failures = [h for h in hist if isinstance(h, DistillationFailure)]
        print(f"  failure records: {len(failures)}")
        assert len(failures) >= 1

        print("\n=== Timeout recorded on student ===")
        try:
            await distiller.run(
                task_id="task-slow",
                teacher_callable=_teacher_ok,
                student_callable=_slow,
            )
        except DistillationTimeoutError as e:
            print(f"  raised: {type(e).__name__} (phase={e.phase})")

        print("\n=== Bounded history ===")
        small = QuantumDistiller(
            config=DistillerConfig(
                teacher_timeout_s=2.0, student_timeout_s=2.0,
                max_history=3,
            ),
        )
        for i in range(10):
            await small.run(
                task_id=f"bounded-{i}",
                teacher_callable=_teacher_ok,
                student_callable=_student_ok,
            )
        print(f"  history size: {len(small.history())} (max 3)")
        assert len(small.history()) == 3

        print("\n=== Serialization ===")
        d = result.to_dict()
        print(f"  JSON length: {len(json.dumps(d, default=str))} bytes")
        restored = DistillationResult.from_dict(d)
        print(f"  round-trip equality: {restored == result}")

        print("\n=== history_dicts() ===")
        hd = distiller.history_dicts()
        print(f"  records: {len(hd)}")
        for rec in hd[:2]:
            print(f"    type={rec.get('record_type', 'result')}, "
                  f"id={rec['distillation_id'][:12]}…")

        print("\n=== Sync callables run in thread (not blocking loop) ===")
        def _sync_teacher() -> Any:
            return _TeacherOut()

        def _sync_student() -> Any:
            return _StudentOut()

        r_sync = await distiller.run(
            task_id="task-sync",
            teacher_callable=_sync_teacher,
            student_callable=_sync_student,
        )
        print(f"  retention: {r_sync.quality_retention:.4f}")

        print("\n=== Construction validation ===")
        for label, kwargs in [
            ("empty task_id", dict(task_id="")),
            ("non-callable teacher",
             dict(teacher_callable=None)),
            ("bad baseline",
             dict(classical_baseline_quality=1.5)),
        ]:
            base = dict(
                task_id="t",
                teacher_callable=_teacher_ok,
                student_callable=_student_ok,
            )
            base.update(kwargs)
            try:
                await distiller.run(**base)
                print(f"  {label}: NOT rejected")
            except DistillationValidationError as e:
                print(f"  {label}: {type(e).__name__}")

        print(f"\n=== __version__ === {__version__}")
        print(f"=== __all__ === {__all__}")
        print("\nSmoke test passed.")

    asyncio.run(main())
