"""
QuantumExecutionResult — the immutable outcome of a quantum job.

An immutable, self-validating record of what happened on a quantum
backend (or its classical fallback). Once constructed, no field can be
mutated; use ``dataclasses.replace(...)`` to obtain a modified copy.

Terminal statuses
-----------------
A ``QuantumExecutionResult`` normally represents a **terminal** outcome:

  * ``COMPLETED``          — the QPU returned a usable result.
  * ``FALLBACK``           — the classical fallback produced the result.
  * ``FAILED``             — the QPU errored; no usable result.
  * ``BUDGET_EXCEEDED``    — a budget was hit; execution aborted.
  * ``CANCELLED``          — the caller revoked the request.

The other three statuses (``SUBMITTED``, ``QUEUED``, ``RUNNING``) are
allowed on a result *only* for progress reporting; use
``is_terminal()`` / ``is_in_flight()`` / ``outcome_class()`` to
distinguish. Production code should be aware that such a result is not
yet conclusive.

Quality fields
--------------
  * ``quality``      — end-to-end task accuracy / score against a
    ground-truth metric. Range [0, 1]. ``None`` means "not measured".
  * ``fidelity``     — post-transpilation circuit-level fidelity (gate
    fidelities compounded over the compiled circuit). Range [0, 1].
  * ``error_rate``   — measured error rate of the executed circuit
    (readout error, gate error, or their combination depending on
    backend). Range [0, 1].

``is_success()`` defaults to checking only the status. Pass
``min_quality=0.9`` to require the ``quality`` field to be measured and
above the bar.

Budget fields
-------------
``estimated_*`` are the pre-execution estimates (copied from the
``QuantumExecutionRequest``); ``measured_*`` are the post-execution
actuals if the backend reported them. Both are optional; ``None`` means
"unknown".
"""

from __future__ import annotations

import json
import logging
import re
from dataclasses import asdict, dataclass, field, fields, replace
from datetime import datetime, timedelta, timezone
from enum import Enum
from types import MappingProxyType
from typing import (
    Any,
    Dict,
    List,
    Mapping,
    Optional,
    Tuple,
)

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())

__version__ = "2.0.0"


# =============================================================================
# Constants
# =============================================================================

SEED_MAX: int = 2**64
MAX_SIGNATURE_LEN: int = 512
MAX_ERROR_MESSAGE_LEN: int = 4096


# =============================================================================
# Errors
# =============================================================================

class ExecutionResultError(ValueError):
    """Raised when a QuantumExecutionResult fails validation."""


# =============================================================================
# Enums
# =============================================================================

class ExecutionStatus(str, Enum):
    """Lifecycle status of a quantum execution."""

    SUBMITTED = "submitted"        # accepted by the monitor, not yet dispatched
    QUEUED = "queued"              # dispatched to the backend, waiting in queue
    RUNNING = "running"            # actively executing on the backend
    COMPLETED = "completed"        # QPU returned a usable result
    FAILED = "failed"              # QPU errored, no usable result
    FALLBACK = "fallback"          # classical fallback produced the result
    CANCELLED = "cancelled"        # caller revoked the request
    BUDGET_EXCEEDED = "budget_exceeded"  # a budget was exceeded; aborted


class FallbackReason(str, Enum):
    """Why the quantum route was abandoned in favour of classical."""

    TIMEOUT = "timeout"
    BUDGET_EXCEEDED = "budget_exceeded"
    INVALID_OUTPUT = "invalid_output"
    HIGH_NOISE = "high_noise"
    BACKEND_UNAVAILABLE = "backend_unavailable"
    LOW_QUALITY = "low_quality"
    NONE = "none"


_TERMINAL_STATUSES = frozenset({
    ExecutionStatus.COMPLETED,
    ExecutionStatus.FAILED,
    ExecutionStatus.FALLBACK,
    ExecutionStatus.CANCELLED,
    ExecutionStatus.BUDGET_EXCEEDED,
})

_IN_FLIGHT_STATUSES = frozenset({
    ExecutionStatus.SUBMITTED,
    ExecutionStatus.QUEUED,
    ExecutionStatus.RUNNING,
})


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


def _coerce_enum(
    enum_cls: type, value: Any, field_name: str,
) -> Any:
    if isinstance(value, enum_cls):
        return value
    try:
        return enum_cls(value)
    except (ValueError, KeyError):
        allowed = sorted(e.value for e in enum_cls)
        raise ExecutionResultError(
            f"{field_name} must be one of {allowed}; got {value!r}"
        ) from None


def _coerce_backend_kind(value: Any) -> Any:
    if value is None:
        return None
    try:
        from .backend_profile import BackendKind
    except ImportError:
        return value
    if isinstance(value, BackendKind):
        return value
    try:
        return BackendKind(value)
    except ValueError:
        allowed = sorted(e.value for e in BackendKind)
        raise ExecutionResultError(
            f"backend_kind must be one of {allowed}; got {value!r}"
        ) from None


def _validate_algorithm(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    if not isinstance(value, str) or not value.strip():
        raise ExecutionResultError(
            "algorithm must be a non-empty string or None"
        )
    normalized = value.strip().lower()
    try:
        from .execution_request import AlgorithmRegistry
    except ImportError:
        return normalized
    if not AlgorithmRegistry.is_known(normalized):
        raise ExecutionResultError(
            f"algorithm {normalized!r} is not registered; "
            f"call AlgorithmRegistry.register({normalized!r}) first"
        )
    return normalized


def _freeze_mapping(m: Any) -> Mapping[str, Any]:
    if m is None:
        return MappingProxyType({})
    if isinstance(m, MappingProxyType):
        return m
    if not isinstance(m, Mapping):
        raise ExecutionResultError(
            f"measurement_settings must be a Mapping; "
            f"got {type(m).__name__}"
        )
    return MappingProxyType(dict(m))


_SIGNATURE_HEX_RE = re.compile(r"^[A-Za-z0-9_\-+/=.:]{8,}$")


def _to_jsonable(obj: Any) -> Any:
    """
    Recursively convert to a JSON-serializable form.

    Raises ``TypeError`` for values whose type is not handled — better to
    fail loudly than to silently stringify audit data.
    """
    if isinstance(obj, Enum):
        return obj.value
    if isinstance(obj, Mapping):
        return {str(k): _to_jsonable(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple, set, frozenset)):
        return [_to_jsonable(v) for v in obj]
    if isinstance(obj, datetime):
        return obj.isoformat()
    # numpy-like scalars and arrays.
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


def _serialize_fallback(obj: Any) -> Any:
    """Serialize ``fallback_result``, preserving recursive result nesting."""
    if obj is None:
        return None
    if isinstance(obj, QuantumExecutionResult):
        return obj.to_dict()
    return _to_jsonable(obj)


def _looks_like_result_dict(d: Any) -> bool:
    if not isinstance(d, Mapping):
        return False
    return (
        "request_id" in d
        and "task_id" in d
        and "status" in d
        and d.get("status") in {s.value for s in ExecutionStatus}
    )


# =============================================================================
# QuantumExecutionResult
# =============================================================================

@dataclass(frozen=True)
class QuantumExecutionResult:
    """
    Immutable outcome of a quantum execution request.

    Carries full provenance for reporting and audit. ``fallback_result``
    is populated when the quantum route fails and a classical fallback
    produced the result.

    See the module docstring for the semantics of ``quality``,
    ``fidelity``, ``error_rate``, ``estimated_*``, and ``measured_*``.
    """

    # --- Identity ---
    request_id: str
    task_id: str
    status: ExecutionStatus

    # --- Backend & runtime ---
    backend: Optional[str] = None
    backend_kind: Optional[Any] = None          # coerced to BackendKind
    algorithm: Optional[str] = None             # validated against registry
    shots_used: int = 0
    queue_seconds: float = 0.0
    runtime_seconds: float = 0.0

    # --- Quality & noise ---
    quality: Optional[float] = None
    fidelity: Optional[float] = None
    error_rate: Optional[float] = None
    uncertainty: float = 0.0                    # deprecated; use uncertainty_std
    uncertainty_std: Optional[float] = None

    # --- Resource accounting ---
    estimated_energy_kwh: float = 0.0
    estimated_co2e_kg: float = 0.0
    estimated_cost: float = 0.0
    measured_energy_kwh: Optional[float] = None
    measured_co2e_kg: Optional[float] = None
    measured_cost: Optional[float] = None

    # --- Provenance ---
    signature: Optional[str] = None
    signature_verified: Optional[bool] = None
    circuit_version: Optional[str] = None
    seed: Optional[int] = None
    measurement_settings: Mapping[str, Any] = field(default_factory=dict)
    raw_output: Any = None

    # --- Fallback ---
    fallback_used: bool = False
    fallback_reason: Optional[FallbackReason] = None
    fallback_result: Any = None

    # --- Error info ---
    error_message: Optional[str] = None
    error_code: Optional[str] = None

    # --- Echo from request ---
    idempotency_key: Optional[str] = None
    policy_version: Optional[str] = None

    # --- Timing ---
    submitted_at: Optional[datetime] = None
    started_at: Optional[datetime] = None
    completed_at: datetime = field(default_factory=_utcnow)

    # --- Execution context ---
    executor_id: Optional[str] = None
    executor_version: Optional[str] = None
    audit_event_id: Optional[str] = None

    # --- Internal ---
    _skip_validation: bool = field(
        default=False, repr=False, compare=False, hash=False,
    )

    # ------------------------------------------------------------------ init
    def __post_init__(self) -> None:
        # --- Enum coercion (raises with allowed values on typos) ---
        object.__setattr__(
            self, "status",
            _coerce_enum(ExecutionStatus, self.status, "status"),
        )
        if self.fallback_reason is not None:
            object.__setattr__(
                self, "fallback_reason",
                _coerce_enum(
                    FallbackReason, self.fallback_reason,
                    "fallback_reason",
                ),
            )
        object.__setattr__(
            self, "backend_kind", _coerce_backend_kind(self.backend_kind),
        )
        object.__setattr__(
            self, "algorithm", _validate_algorithm(self.algorithm),
        )

        # --- UTC normalization ---
        object.__setattr__(
            self, "completed_at", _ensure_utc(self.completed_at),
        )
        if self.submitted_at is not None:
            object.__setattr__(
                self, "submitted_at", _ensure_utc(self.submitted_at),
            )
        if self.started_at is not None:
            object.__setattr__(
                self, "started_at", _ensure_utc(self.started_at),
            )

        # --- Freeze measurement_settings (always, even when skipping) ---
        object.__setattr__(
            self, "measurement_settings",
            _freeze_mapping(self.measurement_settings),
        )

        # --- Validation ---
        if not self._skip_validation:
            self._validate()

    def _validate(self) -> None:
        # --- IDs ---
        for fname in ("request_id", "task_id"):
            v = getattr(self, fname)
            if not isinstance(v, str) or not v.strip():
                raise ExecutionResultError(
                    f"{fname} must be a non-empty string"
                )

        # --- Numeric ranges ---
        if not isinstance(self.shots_used, int) or self.shots_used < 0:
            raise ExecutionResultError(
                "shots_used must be a non-negative int"
            )
        for fname in (
            "queue_seconds", "runtime_seconds",
            "estimated_energy_kwh", "estimated_co2e_kg",
            "estimated_cost",
        ):
            v = getattr(self, fname)
            if not isinstance(v, (int, float)) or v < 0:
                raise ExecutionResultError(f"{fname} must be >= 0")

        for fname in (
            "measured_energy_kwh", "measured_co2e_kg", "measured_cost",
            "uncertainty_std",
        ):
            v = getattr(self, fname)
            if v is not None:
                if not isinstance(v, (int, float)) or v < 0:
                    raise ExecutionResultError(
                        f"{fname} must be >= 0 or None"
                    )

        # --- Probability fields in [0, 1] ---
        for fname in ("quality", "fidelity", "error_rate"):
            v = getattr(self, fname)
            if v is not None:
                if not isinstance(v, (int, float)):
                    raise ExecutionResultError(
                        f"{fname} must be a number or None"
                    )
                if not (0.0 <= float(v) <= 1.0):
                    raise ExecutionResultError(
                        f"{fname} must be in [0, 1] or None"
                    )

        if not isinstance(self.uncertainty, (int, float)) \
                or self.uncertainty < 0:
            raise ExecutionResultError("uncertainty must be >= 0")

        # --- Seed ---
        if self.seed is not None:
            if isinstance(self.seed, bool) \
                    or not isinstance(self.seed, int):
                raise ExecutionResultError("seed must be an int or None")
            if not (0 <= self.seed < SEED_MAX):
                raise ExecutionResultError(
                    "seed must be in [0, 2^64) or None"
                )

        # --- Signature ---
        if self.signature is not None:
            if not isinstance(self.signature, str) \
                    or not self.signature:
                raise ExecutionResultError(
                    "signature must be a non-empty string or None"
                )
            if len(self.signature) > MAX_SIGNATURE_LEN:
                raise ExecutionResultError(
                    f"signature exceeds {MAX_SIGNATURE_LEN} chars"
                )
            if not _SIGNATURE_HEX_RE.match(self.signature):
                raise ExecutionResultError(
                    "signature must be a printable ASCII token "
                    "(hex, base64, or sig-prefixed identifier)"
                )

        # --- Error fields ---
        for fname in ("error_message", "error_code"):
            v = getattr(self, fname)
            if v is not None:
                if not isinstance(v, str):
                    raise ExecutionResultError(
                        f"{fname} must be a string or None"
                    )
                if fname == "error_message" \
                        and len(v) > MAX_ERROR_MESSAGE_LEN:
                    raise ExecutionResultError(
                        f"error_message exceeds {MAX_ERROR_MESSAGE_LEN} chars"
                    )

        # --- Timing coherence ---
        if self.submitted_at is not None \
                and self.completed_at < self.submitted_at:
            raise ExecutionResultError(
                "completed_at must be >= submitted_at"
            )
        if self.started_at is not None:
            if self.submitted_at is not None \
                    and self.started_at < self.submitted_at:
                raise ExecutionResultError(
                    "started_at must be >= submitted_at"
                )
            if self.completed_at < self.started_at:
                raise ExecutionResultError(
                    "completed_at must be >= started_at"
                )

        # --- Status / fallback coherence ---
        self._validate_status_fallback_coherence()

    def _validate_status_fallback_coherence(self) -> None:
        status = self.status
        if status == ExecutionStatus.COMPLETED:
            if self.fallback_used:
                raise ExecutionResultError(
                    "status=completed is incompatible with "
                    "fallback_used=True; use status=fallback for a "
                    "successful classical fallback"
                )
            if self.fallback_reason not in (None, FallbackReason.NONE):
                raise ExecutionResultError(
                    "status=completed is incompatible with a "
                    "fallback_reason other than 'none'"
                )
        elif status == ExecutionStatus.FALLBACK:
            if not self.fallback_used:
                raise ExecutionResultError(
                    "status=fallback requires fallback_used=True"
                )
            if self.fallback_reason in (None, FallbackReason.NONE):
                raise ExecutionResultError(
                    "status=fallback requires a meaningful fallback_reason"
                )
        elif status in (
            ExecutionStatus.FAILED,
            ExecutionStatus.BUDGET_EXCEEDED,
            ExecutionStatus.CANCELLED,
        ):
            if self.fallback_used:
                raise ExecutionResultError(
                    f"status={status.value} is incompatible with "
                    f"fallback_used=True; use status=fallback for a "
                    f"successful classical fallback"
                )
        elif status in _IN_FLIGHT_STATUSES:
            # Progress-report results — allowed, but must not claim a
            # fallback outcome that hasn't happened yet.
            if self.fallback_used:
                raise ExecutionResultError(
                    f"status={status.value} cannot have "
                    f"fallback_used=True (execution is not terminal)"
                )

    # ------------------------------------------------------------------ properties
    @property
    def is_in_flight(self) -> bool:
        return self.status in _IN_FLIGHT_STATUSES

    @property
    def is_terminal(self) -> bool:
        return self.status in _TERMINAL_STATUSES

    @property
    def is_fallback(self) -> bool:
        return self.status == ExecutionStatus.FALLBACK

    @property
    def is_budget_exceeded(self) -> bool:
        return self.status == ExecutionStatus.BUDGET_EXCEEDED

    @property
    def is_cancelled(self) -> bool:
        return self.status == ExecutionStatus.CANCELLED

    @property
    def has_signature(self) -> bool:
        return self.signature is not None and bool(self.signature)

    @property
    def effective_uncertainty(self) -> Optional[float]:
        """Prefer uncertainty_std; fall back to legacy uncertainty if set."""
        if self.uncertainty_std is not None:
            return self.uncertainty_std
        if self.uncertainty > 0:
            return self.uncertainty
        return None

    # ------------------------------------------------------------------ classification
    def is_success(self, *, min_quality: Optional[float] = None) -> bool:
        """
        True iff the QPU completed AND the result meets ``min_quality``.

        Without ``min_quality``, only the status is checked. With it, a
        missing ``quality`` measurement is treated as a failure.
        """
        if self.status != ExecutionStatus.COMPLETED:
            return False
        if min_quality is not None:
            if self.quality is None:
                return False
            if self.quality < min_quality:
                return False
        return True

    def is_terminal_failure(self) -> bool:
        """Terminal AND no usable output was produced."""
        return (
            self.is_terminal
            and not self.is_success()
            and not self.is_fallback
        )

    def outcome_class(self) -> str:
        """
        Single-word category for logging / metrics.

        One of: ``"success"``, ``"fallback"``, ``"failure"``, ``"in_progress"``.
        """
        if self.is_success():
            return "success"
        if self.is_fallback:
            return "fallback"
        if self.is_terminal_failure():
            return "failure"
        return "in_progress"

    # ------------------------------------------------------------------ freshness
    def is_stale(self, max_age_seconds: float = 86400.0) -> bool:
        """True if ``completed_at`` is older than ``max_age_seconds``."""
        if max_age_seconds <= 0:
            raise ValueError("max_age_seconds must be > 0")
        age = (_utcnow() - self.completed_at).total_seconds()
        return age > max_age_seconds

    # ------------------------------------------------------------------ resource
    def raw_output_size_bytes(self) -> int:
        """
        Approximate JSON size of ``raw_output`` in bytes.

        Returns 0 for ``None``, -1 if the output is not serializable.
        Useful for audit-log hygiene checks.
        """
        if self.raw_output is None:
            return 0
        try:
            return len(
                json.dumps(_to_jsonable(self.raw_output)).encode("utf-8")
            )
        except (TypeError, ValueError):
            return -1

    def energy_delta_kwh(self) -> Optional[float]:
        """Measured minus estimated energy; None if either is missing."""
        if self.measured_energy_kwh is None:
            return None
        return self.measured_energy_kwh - self.estimated_energy_kwh

    def cost_delta_usd(self) -> Optional[float]:
        if self.measured_cost is None:
            return None
        return self.measured_cost - self.estimated_cost

    def co2e_delta_kg(self) -> Optional[float]:
        if self.measured_co2e_kg is None:
            return None
        return self.measured_co2e_kg - self.estimated_co2e_kg

    # ------------------------------------------------------------------ serialization
    def to_dict(self) -> Dict[str, Any]:
        """JSON-safe dict; enums as strings, datetimes as ISO, tuples as lists."""
        return {
            "request_id": self.request_id,
            "task_id": self.task_id,
            "status": self.status.value,
            "backend": self.backend,
            "backend_kind": (
                self.backend_kind.value
                if self.backend_kind is not None
                and hasattr(self.backend_kind, "value")
                else self.backend_kind
            ),
            "algorithm": self.algorithm,
            "shots_used": self.shots_used,
            "queue_seconds": self.queue_seconds,
            "runtime_seconds": self.runtime_seconds,
            "quality": self.quality,
            "fidelity": self.fidelity,
            "error_rate": self.error_rate,
            "uncertainty": self.uncertainty,
            "uncertainty_std": self.uncertainty_std,
            "estimated_energy_kwh": self.estimated_energy_kwh,
            "estimated_co2e_kg": self.estimated_co2e_kg,
            "estimated_cost": self.estimated_cost,
            "measured_energy_kwh": self.measured_energy_kwh,
            "measured_co2e_kg": self.measured_co2e_kg,
            "measured_cost": self.measured_cost,
            "signature": self.signature,
            "signature_verified": self.signature_verified,
            "circuit_version": self.circuit_version,
            "seed": self.seed,
            "measurement_settings": _to_jsonable(
                dict(self.measurement_settings)
            ),
            "raw_output": _to_jsonable(self.raw_output),
            "fallback_used": self.fallback_used,
            "fallback_reason": (
                self.fallback_reason.value
                if self.fallback_reason else None
            ),
            "fallback_result": _serialize_fallback(self.fallback_result),
            "error_message": self.error_message,
            "error_code": self.error_code,
            "idempotency_key": self.idempotency_key,
            "policy_version": self.policy_version,
            "submitted_at": (
                self.submitted_at.isoformat()
                if self.submitted_at else None
            ),
            "started_at": (
                self.started_at.isoformat()
                if self.started_at else None
            ),
            "completed_at": self.completed_at.isoformat(),
            "executor_id": self.executor_id,
            "executor_version": self.executor_version,
            "audit_event_id": self.audit_event_id,
        }

    def to_json(self, **kwargs: Any) -> str:
        """``json.dumps`` over ``to_dict()``."""
        return json.dumps(self.to_dict(), **kwargs)

    @classmethod
    def from_dict(
        cls,
        d: Mapping[str, Any],
        *,
        validate: bool = True,
    ) -> "QuantumExecutionResult":
        """
        Reconstruct from ``to_dict()`` output.

        - Rejects unknown fields.
        - Parses ISO-format datetimes.
        - Recursively reconstructs a ``fallback_result`` that looks like
          a ``QuantumExecutionResult``.
        - ``validate=False`` bypasses ``__post_init__`` validation, for
          reconstructing records from audit history.
        """
        if not isinstance(d, Mapping):
            raise ExecutionResultError(
                f"d must be a Mapping; got {type(d).__name__}"
            )
        if "_skip_validation" in d:
            raise ExecutionResultError(
                "_skip_validation cannot be set via from_dict; "
                "use the validate= keyword argument"
            )

        known = {f.name for f in fields(cls)} - {"_skip_validation"}
        cleaned: Dict[str, Any] = dict(d)
        unknown = set(cleaned.keys()) - known
        if unknown:
            raise ExecutionResultError(
                f"unknown fields in QuantumExecutionResult dict: "
                f"{sorted(unknown)}"
            )

        for fname in ("submitted_at", "started_at", "completed_at"):
            v = cleaned.get(fname)
            if isinstance(v, str):
                cleaned[fname] = datetime.fromisoformat(v)

        # Recursively reconstruct fallback_result if it looks like a result.
        if _looks_like_result_dict(cleaned.get("fallback_result")):
            cleaned["fallback_result"] = cls.from_dict(
                cleaned["fallback_result"],
            )

        if not validate:
            cleaned["_skip_validation"] = True

        return cls(**cleaned)

    # ------------------------------------------------------------------ bridge
    def to_evidence_dict(self) -> Dict[str, Any]:
        """
        A dict suitable for constructing ``QuantumEvidence`` in the
        governance layer.

        Returns fields relevant to routing justification without
        importing ``quantum_evidence`` (avoids a cyclic import).
        """
        return {
            "task_id": self.task_id,
            "request_id": self.request_id,
            "status": self.status.value,
            "quantum_used": not self.fallback_used,
            "quality": self.quality,
            "fidelity": self.fidelity,
            "error_rate": self.error_rate,
            "estimated_energy_kwh": self.estimated_energy_kwh,
            "estimated_co2e_kg": self.estimated_co2e_kg,
            "measured_energy_kwh": self.measured_energy_kwh,
            "measured_co2e_kg": self.measured_co2e_kg,
            "signature": self.signature,
            "signature_verified": self.signature_verified,
            "fallback_reason": (
                self.fallback_reason.value
                if self.fallback_reason else None
            ),
        }

    # ------------------------------------------------------------------ repr
    def __repr__(self) -> str:
        return (
            f"QuantumExecutionResult("
            f"request={self.request_id!r}, "
            f"task={self.task_id!r}, "
            f"status={self.status.value}, "
            f"quality={self.quality!r}, "
            f"fallback={self.fallback_used})"
        )


# =============================================================================
# Public API
# =============================================================================

__all__: Tuple[str, ...] = (
    "ExecutionResultError",
    "ExecutionStatus",
    "FallbackReason",
    "QuantumExecutionResult",
)
