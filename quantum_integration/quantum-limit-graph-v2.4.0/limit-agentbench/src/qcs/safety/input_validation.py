"""
InputValidator — validation entry point for all QCS inputs.

Wraps the validation surface of the enhanced contracts
(``QuantumTaskSpec``, ``QuantumExecutionRequest``, ``BackendProfile``)
so callers can gate on one uniform ``ValidationResult`` regardless of
which contract is being checked.

What is actually checked
------------------------
  * ``check_task_spec``  — structural + (when the task exposes it)
    ``is_routable(...)``, which adds baseline, deadline, and qubit
    feasibility. Callers wanting a structural-only check pass
    ``require_baseline=False`` and ``at=None``.
  * ``check_request``    — structural + (when the request exposes it)
    ``validate_for_dispatch(at=...)``, which adds expiry. A request
    past ``max_queue_seconds`` or ``expires_at`` is rejected when
    ``config.check_expiry`` is True.
  * ``check_backend``    — the profile's own
    ``validate_consistency()`` when available; otherwise basic field
    checks for legacy profiles.
  * ``check_payload``    — non-``None``, non-empty, JSON-serializable,
    within ``config.max_payload_bytes``.

What is NOT checked
-------------------
  * Semantic correctness of a task's problem (a valid optimisation
    task may still be infeasible for quantum).
  * PII, prompt injection, or policy compliance of a payload.
  * Cross-input consistency (e.g. a task_spec whose problem_size
    exceeds its request's max_shots).

These belong in downstream layers; the module docstring is honest about
what this validator gates.

Backward compatibility
----------------------
The pre-2.0 API — ``InputValidator.validate_task_spec(x)``, etc. —
still works: the static methods are kept as delegation shims that
instantiate a default-config ``InputValidator`` and call the
corresponding instance method. ``ValidationResult(valid=True,
errors=[])`` is still accepted; ``ValidationResult(valid=True,
errors=["x"])`` is now rejected (was silently allowed).

v2 fixes (see changelog in commit message):
  * No more crash on malformed backend fields (``num_qubits="5"``).
  * Backend validation defers to ``validate_consistency()`` for
    enhanced profiles.
  * Task/request validation uses ``is_routable`` / ``validate_for_dispatch``
    when available (was only ``validate`` — didn't catch expiry or
    unroutability).
  * ``validate_payload`` performs real checks (was only ``is None``).
  * Wrong-type inputs fail closed (were "valid=True" through ``hasattr``).
  * ``ValidationResult`` frozen; ``valid`` is consistency-checked
    against ``issues``.
  * Typed ``ValidationCode`` / ``ValidationIssue``.
  * ``ValidatorConfig`` dataclass; metrics; health; ``validate_all``.
  * ``__version__``, ``__all__``, demo.
"""

from __future__ import annotations

import hashlib
import json
import logging
import math
import threading
import time
from dataclasses import dataclass, field, fields, asdict
from datetime import datetime, timezone
from enum import Enum
from types import MappingProxyType
from typing import (
    Any,
    Callable,
    Dict,
    FrozenSet,
    List,
    Mapping,
    Optional,
    Sequence,
    Tuple,
    Union,
)

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())

__version__ = "2.0.0"


# =============================================================================
# Constants
# =============================================================================

DEFAULT_MAX_PAYLOAD_BYTES: int = 1_048_576   # 1 MiB


# =============================================================================
# Enums
# =============================================================================

class ValidationCode(str, Enum):
    """Typed reason codes; stable identifiers for downstream branching."""

    MISSING_FIELD = "missing_field"
    INVALID_TYPE = "invalid_type"
    OUT_OF_RANGE = "out_of_range"
    NEGATIVE_VALUE = "negative_value"
    EXPIRED = "expired"
    NOT_READY = "not_ready"              # routability failure
    UNKNOWN_BACKEND = "unknown_backend"
    UNKNOWN_ALGORITHM = "unknown_algorithm"
    INCONSISTENT = "inconsistent"        # cross-field
    TOO_LARGE = "too_large"
    NOT_SERIALIZABLE = "not_serializable"
    NO_VALIDATE_METHOD = "no_validate_method"
    OTHER = "other"


# =============================================================================
# Data classes
# =============================================================================

@dataclass(frozen=True)
class ValidationIssue:
    """A single typed validation failure."""

    code: ValidationCode
    field: Optional[str]
    message: str

    def __post_init__(self) -> None:
        # Coerce code from string
        if not isinstance(self.code, ValidationCode):
            try:
                object.__setattr__(
                    self, "code", ValidationCode(self.code),
                )
            except (ValueError, KeyError):
                raise ValueError(
                    f"unknown validation code {self.code!r}"
                ) from None
        if self.field is not None:
            if not isinstance(self.field, str) or not self.field.strip():
                raise ValueError(
                    "field must be a non-empty string or None"
                )
        if not isinstance(self.message, str) or not self.message:
            raise ValueError("message must be a non-empty string")

    def to_dict(self) -> Dict[str, Any]:
        return {
            "code": self.code.value,
            "field": self.field,
            "message": self.message,
        }

    def __repr__(self) -> str:
        return (
            f"ValidationIssue(code={self.code.value}, "
            f"field={self.field!r}, message={self.message!r})"
        )


@dataclass(frozen=True, init=False)
class ValidationResult:
    """
    Immutable validation outcome.

    Primary field: ``issues`` (typed). ``valid`` is a stored bool that is
    consistency-checked against ``issues`` in ``__init__``. ``errors`` is
    a convenience property returning the message strings (backward
    compatible with the pre-2.0 ``.errors: List[str]``).

    ``ValidationResult(valid=True, errors=["x"])`` raises. The two views
    can't disagree.
    """

    valid: bool
    issues: Tuple[ValidationIssue, ...]

    def __init__(
        self,
        *,
        issues: Optional[Sequence[Union[ValidationIssue, str]]] = None,
        # Legacy kwargs:
        valid: Optional[bool] = None,
        errors: Optional[Sequence[str]] = None,
    ) -> None:
        if issues is not None and errors is not None:
            raise TypeError(
                "cannot pass both issues= and errors=; they are aliases"
            )

        derived_issues: Tuple[ValidationIssue, ...]

        if issues is not None:
            derived_issues = tuple(
                i if isinstance(i, ValidationIssue) else ValidationIssue(
                    code=ValidationCode.OTHER, field=None, message=str(i),
                )
                for i in issues
            )
        elif errors is not None:
            derived_issues = tuple(
                ValidationIssue(
                    code=ValidationCode.OTHER, field=None, message=str(e),
                )
                for e in errors
            )
        elif valid is not None:
            if valid:
                derived_issues = ()
            else:
                # Invalid with no reason given — synthesize a generic issue.
                derived_issues = (
                    ValidationIssue(
                        code=ValidationCode.OTHER, field=None,
                        message="invalid (no specific reason given)",
                    ),
                )
        else:
            derived_issues = ()

        derived_valid = not derived_issues
        if valid is not None and valid != derived_valid:
            raise ValueError(
                f"valid={valid} contradicts the derived value "
                f"{derived_valid} from issues/errors"
            )

        object.__setattr__(self, "valid", derived_valid)
        object.__setattr__(self, "issues", derived_issues)

    # ------------------------------------------------------------------ props
    @property
    def errors(self) -> Tuple[str, ...]:
        """Message strings; backward compatible with pre-2.0 ``.errors``."""
        return tuple(i.message for i in self.issues)

    def __bool__(self) -> bool:
        return self.valid

    # ------------------------------------------------------------------ serialization
    def to_dict(self) -> Dict[str, Any]:
        return {
            "valid": self.valid,
            "errors": list(self.errors),
            "issues": [i.to_dict() for i in self.issues],
        }

    def to_json(self, **kwargs: Any) -> str:
        return json.dumps(self.to_dict(), **kwargs)

    @classmethod
    def from_dict(cls, d: Mapping[str, Any]) -> "ValidationResult":
        if not isinstance(d, Mapping):
            raise TypeError(
                f"d must be a Mapping; got {type(d).__name__}"
            )
        issues_data = d.get("issues")
        if issues_data is not None:
            issues = tuple(
                ValidationIssue(
                    code=i.get("code", ValidationCode.OTHER.value),
                    field=i.get("field"),
                    message=i.get("message", ""),
                )
                for i in issues_data
            )
            return cls(issues=issues)
        return cls(errors=d.get("errors", []), valid=d.get("valid"))

    def checksum(self) -> str:
        payload = json.dumps(
            self.to_dict(), sort_keys=True, default=str,
        ).encode("utf-8")
        return hashlib.sha256(payload).hexdigest()

    def __repr__(self) -> str:
        if self.valid:
            return "ValidationResult(valid=True)"
        return (
            f"ValidationResult(valid=False, "
            f"n_issues={len(self.issues)})"
        )


# =============================================================================
# Config
# =============================================================================

@dataclass(frozen=True)
class ValidatorConfig:
    """Tunable parameters for ``InputValidator``."""

    max_payload_bytes: int = DEFAULT_MAX_PAYLOAD_BYTES
    require_baseline: bool = True
    check_expiry: bool = True
    treat_consistency_warnings_as_errors: bool = True
    use_extended_validation: bool = True   # prefer is_routable / validate_for_dispatch

    def __post_init__(self) -> None:
        if isinstance(self.max_payload_bytes, bool) \
                or not isinstance(self.max_payload_bytes, int) \
                or self.max_payload_bytes <= 0:
            raise ValueError(
                "max_payload_bytes must be a positive int"
            )

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


# =============================================================================
# Metrics
# =============================================================================

class MetricsRecorder:
    """Drop-in replaceable with Prometheus / OTel."""

    def __init__(self) -> None:
        self.counters: Dict[str, float] = {}
        self.histograms: Dict[str, List[float]] = {}
        self._lock = threading.RLock()

    @staticmethod
    def _key(name: str, labels: Mapping[str, Any]) -> str:
        if not labels:
            return name
        parts = ",".join(f'{k}="{v}"' for k, v in sorted(labels.items()))
        return f"{name}{{{parts}}}"

    def inc(self, name: str, value: float = 1.0, **labels: Any) -> None:
        with self._lock:
            k = self._key(name, labels)
            self.counters[k] = self.counters.get(k, 0.0) + value

    def observe(self, name: str, value: float, **labels: Any) -> None:
        with self._lock:
            k = self._key(name, labels)
            self.histograms.setdefault(k, []).append(float(value))

    def snapshot(self) -> Dict[str, Any]:
        with self._lock:
            hist: Dict[str, Dict[str, float]] = {}
            for k, vals in self.histograms.items():
                if not vals:
                    continue
                s = sorted(vals)
                hist[k] = {
                    "count": len(vals),
                    "mean": sum(vals) / len(vals),
                    "p50": s[len(s) // 2],
                    "p99": s[min(len(s) - 1, int(len(s) * 0.99))],
                    "max": s[-1],
                }
            return {
                "counters": dict(self.counters),
                "histograms": hist,
            }


# =============================================================================
# Helpers
# =============================================================================

def _is_number(v: Any) -> bool:
    return not isinstance(v, bool) and isinstance(v, (int, float)) and math.isfinite(float(v))


def _is_positive_int(v: Any) -> bool:
    return not isinstance(v, bool) and isinstance(v, int) and v > 0


def _is_nonempty_str(v: Any) -> bool:
    return isinstance(v, str) and bool(v.strip())


def _to_jsonable(obj: Any) -> Any:
    """Recursive JSON-safe conversion; raises on unknown types."""
    if isinstance(obj, Enum):
        return obj.value
    if isinstance(obj, Mapping):
        return {str(k): _to_jsonable(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple, set, frozenset)):
        return [_to_jsonable(v) for v in obj]
    if isinstance(obj, datetime):
        return obj.isoformat()
    if isinstance(obj, (str, int, float, bool)) or obj is None:
        return obj
    raise TypeError(
        f"_to_jsonable: cannot serialize type {type(obj).__name__}"
    )


def _map_violation_to_code(v: Any) -> ValidationCode:
    """Best-effort mapping from a contract violation enum to our typed code."""
    text = str(getattr(v, "value", v)).lower()
    if "expired" in text:
        return ValidationCode.EXPIRED
    if "empty" in text or "missing" in text:
        return ValidationCode.MISSING_FIELD
    if "negative" in text:
        return ValidationCode.NEGATIVE_VALUE
    if "out_of_range" in text or "out-of-range" in text or "invalid" in text:
        return ValidationCode.OUT_OF_RANGE
    if "too_long" in text or "too_large" in text or "excessive" in text:
        return ValidationCode.TOO_LARGE
    if "deadline" in text or "not_ready" in text or "not-routable" in text:
        return ValidationCode.NOT_READY
    if "unknown" in text and "algorithm" in text:
        return ValidationCode.UNKNOWN_ALGORITHM
    if "unknown" in text and ("family" in text or "backend" in text):
        return ValidationCode.UNKNOWN_BACKEND
    return ValidationCode.OTHER


# =============================================================================
# Default config singleton
# =============================================================================

_DEFAULT_CONFIG = ValidatorConfig()


# =============================================================================
# The Enhanced InputValidator
# =============================================================================

class InputValidator:
    """
    Validates quantum task specs, execution requests, backend profiles,
    and payloads before they enter the routing and execution pipeline.

    Instantiate with a ``ValidatorConfig`` for stateful use, or call the
    static shims (``InputValidator.validate_task_spec(...)``) for the
    original zero-config API.
    """

    def __init__(
        self,
        *,
        config: Optional[ValidatorConfig] = None,
        metrics: Optional[MetricsRecorder] = None,
    ) -> None:
        self.config = config or _DEFAULT_CONFIG
        self._metrics = metrics or MetricsRecorder()
        self._started_mono = time.monotonic()

    # ------------------------------------------------------------------ task spec
    def check_task_spec(
        self,
        task_spec: Any,
        *,
        at: Optional[datetime] = None,
        require_baseline: Optional[bool] = None,
    ) -> ValidationResult:
        result = self._do_check_task_spec(
            task_spec, at=at, require_baseline=require_baseline,
        )
        self._record("task_spec", result)
        return result

    def _do_check_task_spec(
        self,
        task_spec: Any,
        *,
        at: Optional[datetime],
        require_baseline: Optional[bool],
    ) -> ValidationResult:
        if task_spec is None:
            return self._fail(
                field="task_spec",
                code=ValidationCode.MISSING_FIELD,
                message="task_spec is None",
            )

        # --- Preferred path: is_routable ---
        is_routable = getattr(task_spec, "is_routable", None)
        if (
            self.config.use_extended_validation
            and callable(is_routable)
        ):
            try:
                rb = is_routable(
                    require_baseline=(
                        require_baseline
                        if require_baseline is not None
                        else self.config.require_baseline
                    ),
                    at=at,
                )
            except TypeError:
                # Older signature without `at`
                try:
                    rb = is_routable(
                        require_baseline=(
                            require_baseline
                            if require_baseline is not None
                            else self.config.require_baseline
                        ),
                    )
                except Exception as exc:
                    return self._fail(
                        field="task_spec",
                        code=ValidationCode.OTHER,
                        message=(
                            f"is_routable() raised "
                            f"{type(exc).__name__}: {exc}"
                        ),
                    )
            except Exception as exc:
                return self._fail(
                    field="task_spec",
                    code=ValidationCode.OTHER,
                    message=(
                        f"is_routable() raised "
                        f"{type(exc).__name__}: {exc}"
                    ),
                )

            if not rb:
                failures = tuple(getattr(rb, "failures", ()))
                if failures:
                    issues = tuple(
                        ValidationIssue(
                            code=ValidationCode.NOT_READY,
                            field=None,
                            message=str(f),
                        )
                        for f in failures
                    )
                    return ValidationResult(issues=issues)
                return self._fail(
                    field="task_spec",
                    code=ValidationCode.NOT_READY,
                    message=str(
                        getattr(rb, "reason", "task not routable")
                    ),
                )
            return ValidationResult()

        # --- Fallback: validate() ---
        validate = getattr(task_spec, "validate", None)
        if not callable(validate):
            return self._fail(
                field="task_spec",
                code=ValidationCode.NO_VALIDATE_METHOD,
                message=(
                    "task_spec has neither is_routable() nor validate(); "
                    f"got {type(task_spec).__name__}"
                ),
            )
        try:
            raw = validate()
        except Exception as exc:
            return self._fail(
                field="task_spec",
                code=ValidationCode.OTHER,
                message=(
                    f"validate() raised "
                    f"{type(exc).__name__}: {exc}"
                ),
            )
        return self._coerce_validation_output(raw, "task_spec")

    # ------------------------------------------------------------------ request
    def check_request(
        self,
        request: Any,
        *,
        at: Optional[datetime] = None,
    ) -> ValidationResult:
        result = self._do_check_request(request, at=at)
        self._record("request", result)
        return result

    def _do_check_request(
        self, request: Any, *, at: Optional[datetime],
    ) -> ValidationResult:
        if request is None:
            return self._fail(
                field="request",
                code=ValidationCode.MISSING_FIELD,
                message="request is None",
            )

        # --- Preferred path: validate_for_dispatch (includes expiry) ---
        validate_for_dispatch = getattr(
            request, "validate_for_dispatch", None,
        )
        if (
            self.config.check_expiry
            and self.config.use_extended_validation
            and callable(validate_for_dispatch)
        ):
            try:
                raw = validate_for_dispatch(at=at)
            except TypeError:
                try:
                    raw = validate_for_dispatch()
                except Exception as exc:
                    return self._fail(
                        field="request",
                        code=ValidationCode.OTHER,
                        message=(
                            f"validate_for_dispatch() raised "
                            f"{type(exc).__name__}: {exc}"
                        ),
                    )
            except Exception as exc:
                return self._fail(
                    field="request",
                    code=ValidationCode.OTHER,
                    message=(
                        f"validate_for_dispatch() raised "
                        f"{type(exc).__name__}: {exc}"
                    ),
                )
            return self._coerce_validation_output(raw, "request")

        # --- Fallback: validate() ---
        validate = getattr(request, "validate", None)
        if not callable(validate):
            return self._fail(
                field="request",
                code=ValidationCode.NO_VALIDATE_METHOD,
                message=(
                    "request has neither validate_for_dispatch() nor "
                    f"validate(); got {type(request).__name__}"
                ),
            )
        try:
            raw = validate()
        except Exception as exc:
            return self._fail(
                field="request",
                code=ValidationCode.OTHER,
                message=(
                    f"validate() raised "
                    f"{type(exc).__name__}: {exc}"
                ),
            )
        return self._coerce_validation_output(raw, "request")

    # ------------------------------------------------------------------ backend
    def check_backend(self, profile: Any) -> ValidationResult:
        result = self._do_check_backend(profile)
        self._record("backend", result)
        return result

    def _do_check_backend(self, profile: Any) -> ValidationResult:
        if profile is None:
            return self._fail(
                field="profile",
                code=ValidationCode.MISSING_FIELD,
                message="profile is None",
            )

        issues: List[ValidationIssue] = []

        # --- Required field ---
        if not _is_nonempty_str(getattr(profile, "backend_id", None)):
            issues.append(ValidationIssue(
                code=ValidationCode.MISSING_FIELD,
                field="backend_id",
                message="backend_id must be a non-empty string",
            ))

        # --- Preferred path: the profile's own validate_consistency ---
        validate_consistency = getattr(
            profile, "validate_consistency", None,
        )
        if callable(validate_consistency):
            try:
                warnings = validate_consistency()
            except Exception as exc:
                issues.append(ValidationIssue(
                    code=ValidationCode.OTHER,
                    field=None,
                    message=(
                        f"validate_consistency() raised "
                        f"{type(exc).__name__}: {exc}"
                    ),
                ))
                warnings = []
            for w in warnings or ():
                code = (
                    ValidationCode.INCONSISTENT
                    if self.config.treat_consistency_warnings_as_errors
                    else ValidationCode.OTHER
                )
                # If warnings are meant to be informational only, we skip
                if not self.config.treat_consistency_warnings_as_errors:
                    continue
                issues.append(ValidationIssue(
                    code=code,
                    field=None,
                    message=str(w),
                ))
        else:
            # --- Fallback: basic legacy checks ---
            if not _is_positive_int(getattr(profile, "num_qubits", None)):
                issues.append(ValidationIssue(
                    code=ValidationCode.INVALID_TYPE,
                    field="num_qubits",
                    message=(
                        "num_qubits must be a positive integer "
                        f"(got {getattr(profile, 'num_qubits', None)!r})"
                    ),
                ))
            if not _is_positive_int(getattr(profile, "max_shots_per_job", None)):
                issues.append(ValidationIssue(
                    code=ValidationCode.INVALID_TYPE,
                    field="max_shots_per_job",
                    message=(
                        "max_shots_per_job must be a positive integer "
                        f"(got {getattr(profile, 'max_shots_per_job', None)!r})"
                    ),
                ))

        return (
            ValidationResult(issues=tuple(issues))
            if issues else ValidationResult()
        )

    # ------------------------------------------------------------------ payload
    def check_payload(self, payload: Any) -> ValidationResult:
        result = self._do_check_payload(payload)
        self._record("payload", result)
        return result

    def _do_check_payload(self, payload: Any) -> ValidationResult:
        if payload is None:
            return self._fail(
                field="payload",
                code=ValidationCode.MISSING_FIELD,
                message="payload is None",
            )

        issues: List[ValidationIssue] = []

        # --- Emptiness (only for sized containers) ---
        if isinstance(payload, (str, bytes, bytearray, list, dict, set, tuple)):
            if len(payload) == 0:
                issues.append(ValidationIssue(
                    code=ValidationCode.OUT_OF_RANGE,
                    field=None,
                    message="payload is empty",
                ))

        # --- JSON serializability ---
        try:
            serialized = json.dumps(_to_jsonable(payload))
        except (TypeError, ValueError) as exc:
            issues.append(ValidationIssue(
                code=ValidationCode.NOT_SERIALIZABLE,
                field=None,
                message=(
                    f"payload is not JSON-serializable: {exc}"
                ),
            ))
            return ValidationResult(issues=tuple(issues))

        # --- Size ---
        size_bytes = len(serialized.encode("utf-8"))
        if size_bytes > self.config.max_payload_bytes:
            issues.append(ValidationIssue(
                code=ValidationCode.TOO_LARGE,
                field=None,
                message=(
                    f"payload size {size_bytes} bytes exceeds "
                    f"max {self.config.max_payload_bytes}"
                ),
            ))

        return (
            ValidationResult(issues=tuple(issues))
            if issues else ValidationResult()
        )

    # ------------------------------------------------------------------ batch
    def validate_all(
        self,
        *items: Any,
        at: Optional[datetime] = None,
    ) -> ValidationResult:
        """Validate a mixed sequence of items, dispatching by type."""
        issues: List[ValidationIssue] = []
        for item in items:
            kind = self._detect_kind(item)
            if kind == "task_spec":
                issues.extend(self.check_task_spec(item, at=at).issues)
            elif kind == "request":
                issues.extend(self.check_request(item, at=at).issues)
            elif kind == "backend":
                issues.extend(self.check_backend(item).issues)
            elif kind == "payload":
                issues.extend(self.check_payload(item).issues)
            elif kind == "none":
                issues.append(ValidationIssue(
                    code=ValidationCode.MISSING_FIELD,
                    field=None,
                    message="item is None",
                ))
            else:  # pragma: no cover — every path above returns a kind
                issues.append(ValidationIssue(
                    code=ValidationCode.INVALID_TYPE,
                    field=None,
                    message=f"unrecognized item: {type(item).__name__}",
                ))
        return (
            ValidationResult(issues=tuple(issues))
            if issues else ValidationResult()
        )

    @staticmethod
    def _detect_kind(item: Any) -> str:
        if item is None:
            return "none"
        if hasattr(item, "problem_family"):
            return "task_spec"
        if hasattr(item, "backend_preference"):
            return "request"
        if hasattr(item, "num_qubits") or hasattr(item, "backend_id"):
            return "backend"
        return "payload"

    # ------------------------------------------------------------------ health
    def health(self) -> Dict[str, Any]:
        return {
            "ready": True,
            "config": self.config.to_dict(),
            "uptime_seconds": time.monotonic() - self._started_mono,
            "metrics": self._metrics.snapshot(),
        }

    # ------------------------------------------------------------------ helpers
    def _coerce_validation_output(
        self, raw: Any, field_name: str,
    ) -> ValidationResult:
        if raw is None:
            return ValidationResult()

        # Enhanced contracts: `violations` + `details`
        violations = getattr(raw, "violations", None)
        if violations is not None:
            details = getattr(raw, "details", None) or {}
            issues: List[ValidationIssue] = []
            for v in violations:
                code = _map_violation_to_code(v)
                if isinstance(details, Mapping) and v in details:
                    message = str(details[v])
                else:
                    message = str(getattr(v, "value", v))
                issues.append(ValidationIssue(
                    code=code, field=None, message=message,
                ))
            return (
                ValidationResult(issues=tuple(issues))
                if issues else ValidationResult()
            )

        # Legacy contracts: List[str]
        if isinstance(raw, (list, tuple)):
            issues = [
                ValidationIssue(
                    code=_map_violation_to_code(e),
                    field=None,
                    message=str(e),
                )
                for e in raw
            ]
            return (
                ValidationResult(issues=tuple(issues))
                if issues else ValidationResult()
            )

        return self._fail(
            field=field_name,
            code=ValidationCode.OTHER,
            message=(
                f"unrecognized validation output type: "
                f"{type(raw).__name__}"
            ),
        )

    def _fail(
        self,
        *,
        field: Optional[str],
        code: ValidationCode,
        message: str,
    ) -> ValidationResult:
        return ValidationResult(
            issues=(ValidationIssue(
                code=code, field=field, message=message,
            ),),
        )

    def _record(self, kind: str, result: ValidationResult) -> None:
        self._metrics.inc(
            "validation_calls_total",
            kind=kind,
            valid=str(result.valid).lower(),
        )
        if not result.valid:
            logger.info(
                "validation failed",
                extra={
                    "kind": kind,
                    "issues": [i.to_dict() for i in result.issues],
                },
            )

    # ------------------------------------------------------------------ static shims
    @staticmethod
    def validate_task_spec(task_spec: Any) -> ValidationResult:
        """Backward-compatible shim → ``InputValidator().check_task_spec``."""
        return InputValidator().check_task_spec(task_spec)

    @staticmethod
    def validate_request(request: Any) -> ValidationResult:
        """Backward-compatible shim → ``InputValidator().check_request``."""
        return InputValidator().check_request(request)

    @staticmethod
    def validate_backend(profile: Any) -> ValidationResult:
        """Backward-compatible shim → ``InputValidator().check_backend``."""
        return InputValidator().check_backend(profile)

    @staticmethod
    def validate_payload(payload: Any) -> ValidationResult:
        """Backward-compatible shim → ``InputValidator().check_payload``."""
        return InputValidator().check_payload(payload)


# =============================================================================
# Public API
# =============================================================================

__all__: Tuple[str, ...] = (
    "DEFAULT_MAX_PAYLOAD_BYTES",
    "InputValidator",
    "MetricsRecorder",
    "ValidationCode",
    "ValidationIssue",
    "ValidationResult",
    "ValidatorConfig",
)


# =============================================================================
# Demo: python -m qcs.safety.input_validator
# =============================================================================

if __name__ == "__main__":  # pragma: no cover
    import json
    from datetime import datetime, timedelta, timezone

    logging.basicConfig(level=logging.INFO)

    # --- Mock objects standing in for the enhanced contracts ---

    class _MockRoutabilityCheck:
        def __init__(self, routable, reason="ok", failures=()):
            self.routable = routable
            self.reason = reason
            self.failures = failures
        def __bool__(self):
            return self.routable

    class _MockTaskSpec:
        problem_family = "optimization"
        def __init__(self, failures=()):
            self._failures = failures
        def is_routable(self, *, require_baseline=True, at=None):
            if self._failures:
                return _MockRoutabilityCheck(
                    False, reason=self._failures[0],
                    failures=self._failures,
                )
            return _MockRoutabilityCheck(True)

    class _MockTaskSpecNoBaseline:
        problem_family = "optimization"
        def is_routable(self, *, require_baseline=True, at=None):
            if require_baseline:
                return _MockRoutabilityCheck(
                    False, reason="missing_classical_baseline",
                    failures=("missing_classical_baseline",),
                )
            return _MockRoutabilityCheck(True)

    class _MockLegacyTaskSpec:
        def validate(self):
            return ["task_id must be non-empty"]

    class _MockRequest:
        backend_preference = "auto"
        def __init__(self, expired=False):
            self._expired = expired
        def validate_for_dispatch(self, at=None):
            issues = [] if not self._expired else ["expired"]
            class R:
                violations = tuple(issues)
                details = {v: v for v in issues}
            return R()

    class _MockEnhancedBackend:
        backend_id = "mock-qpu"
        num_qubits = 5
        max_shots_per_job = 100_000
        def __init__(self, warnings=()):
            self._warnings = list(warnings)
        def validate_consistency(self):
            return self._warnings

    class _MockLegacyBackend:
        backend_id = "legacy-qpu"
        num_qubits = "5"           # ← wrong type, would crash the original
        max_shots_per_job = 1000

    class _MockUnknown:
        """No validate / is_routable method at all."""
        pass

    print("=== Happy paths ===")
    validator = InputValidator()
    r = validator.check_task_spec(_MockTaskSpec())
    print(f"  task_spec: {r!r}")

    r = validator.check_request(_MockRequest())
    print(f"  request:   {r!r}")

    r = validator.check_backend(_MockEnhancedBackend())
    print(f"  backend:   {r!r}")

    r = validator.check_payload({"label": "cat", "score": 0.9})
    print(f"  payload:   {r!r}")

    print("\n=== §1.4 Expired request is rejected ===")
    expired = validator.check_request(_MockRequest(expired=True))
    print(f"  {expired!r}")
    print(f"  issues: {[i.message for i in expired.issues]}")
    assert not expired.valid
    assert any(i.code == ValidationCode.EXPIRED or "expired" in i.message.lower() for i in expired.issues)

    print("\n=== §1.4 Unroutable task spec is rejected ===")
    unroutable = validator.check_task_spec(
        _MockTaskSpec(failures=("past_deadline", "infeasible_qubits")),
    )
    print(f"  {unroutable!r}")
    print(f"  issues: {[i.message for i in unroutable.issues]}")
    assert not unroutable.valid
    assert len(unroutable.issues) == 2

    print("\n=== §1.4 require_baseline configurable ===")
    strict = validator.check_task_spec(_MockTaskSpecNoBaseline())
    lenient = validator.check_task_spec(
        _MockTaskSpecNoBaseline(), require_baseline=False,
    )
    print(f"  default:   valid={strict.valid}")
    print(f"  relaxed:   valid={lenient.valid}")
    assert not strict.valid
    assert lenient.valid

    print("\n=== §1.2 Backend consistency warnings surface ===")
    hw_no_vendor = _MockEnhancedBackend(
        warnings=("hardware_backend_without_vendor",
                  "hardware_backend_without_region"),
    )
    r = validator.check_backend(hw_no_vendor)
    print(f"  {r!r}")
    print(f"  issues: {[i.message for i in r.issues]}")
    assert not r.valid

    print("\n=== §1.3 Malformed legacy backend fields don't crash ===")
    r = validator.check_backend(_MockLegacyBackend())
    print(f"  {r!r}")
    print(f"  issues: {[i.message for i in r.issues]}")
    assert not r.valid
    assert any(i.field == "num_qubits" for i in r.issues)

    print("\n=== §1.5 Wrong type fails closed ===")
    r = validator.check_task_spec(_MockUnknown())
    print(f"  {r!r}")
    print(f"  issues: {[i.message for i in r.issues]}")
    assert not r.valid
    assert r.issues[0].code == ValidationCode.NO_VALIDATE_METHOD

    print("\n=== §1.6 Payload validation now does real work ===")
    for label, payload in [
        ("None", None),
        ("empty list", []),
        ("empty dict", {}),
        ("empty string", ""),
        ("object()", object()),
        ("datetime in dict", {"ts": datetime.now(timezone.utc)}),
        ("oversized", b"x" * (2 * 1024 * 1024)),
    ]:
        r = validator.check_payload(payload)
        codes = [i.code.value for i in r.issues]
        print(f"  {label:20} valid={r.valid}  codes={codes}")
        assert not r.valid

    print("\n=== §2.1 valid is not decoupled from issues ===")
    try:
        ValidationResult(valid=True, errors=["nope"])
    except ValueError as e:
        print(f"  inconsistent result rejected: {str(e)[:60]}…")

    print("\n=== §2.2 Immutability ===")
    r_ok = ValidationResult()
    try:
        r_ok.valid = False  # type: ignore[misc]
    except Exception as e:
        print(f"  valid mutation → {type(e).__name__}")
    try:
        r_ok.issues = ()  # type: ignore[misc]
    except Exception as e:
        print(f"  issues mutation → {type(e).__name__}")

    print("\n=== §2.3 Typed issues ===")
    r = validator.check_payload([])
    issue = r.issues[0]
    print(f"  code:    {issue.code.value}")
    print(f"  field:   {issue.field}")
    print(f"  message: {issue.message}")
    assert issue.code == ValidationCode.OUT_OF_RANGE
    assert r.to_dict()["issues"][0]["code"] == "out_of_range"

    print("\n=== §2.4 Serialization round-trip ===")
    d = r.to_dict()
    print(f"  JSON length: {len(json.dumps(d))} bytes")
    restored = ValidationResult.from_dict(d)
    print(f"  round-trip: {restored == r}")
    assert restored == r

    print("\n=== §2.7 Batch validation ===")
    batch = validator.validate_all(
        _MockTaskSpec(),
        _MockRequest(),
        _MockEnhancedBackend(),
        {"ok": True},
    )
    print(f"  mixed good batch: {batch!r}")

    batch_bad = validator.validate_all(
        _MockTaskSpec(failures=("bad",)),
        _MockRequest(expired=True),
        [],
    )
    print(f"  mixed bad batch:  {batch_bad!r}")
    print(f"  issues:           {len(batch_bad.issues)}")
    assert not batch_bad.valid

    print("\n=== §2.6 Stateful validator with custom config ===")
    small_budget = InputValidator(
        config=ValidatorConfig(max_payload_bytes=100),
    )
    r_small = small_budget.check_payload({"data": "x" * 200})
    print(f"  small budget: {r_small!r}")
    assert not r_small.valid
    assert any(i.code == ValidationCode.TOO_LARGE for i in r_small.issues)

    print("\n=== §3.3 Health ===")
    print(f"  {validator.health()['config']}")

    print("\n=== Backward compat: static methods ===")
    r_static = InputValidator.validate_backend(_MockEnhancedBackend())
    print(f"  InputValidator.validate_backend(mock) → {r_static!r}")
    assert r_static.valid

    print(f"\n=== __version__ === {__version__}")
    print(f"=== __all__ === {__all__}")
    print("\nSmoke test passed.")
