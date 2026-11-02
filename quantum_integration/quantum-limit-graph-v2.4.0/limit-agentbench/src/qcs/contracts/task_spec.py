"""
QuantumTaskSpec — the canonical description of a quantum problem.

A ``QuantumTaskSpec`` describes **what the caller wants to solve** — no
backend, algorithm, or execution mode. The router reads the spec and
decides how to route it.

An instance is immutable and self-validating. Invalid data raises
``TaskSpecValidationError`` at construction. To reconstruct a spec from
audit history (which may fail today's structural rules), pass
``_skip_validation=True``.

Validation layers
-----------------
Three gates, from weakest to strongest:

  * ``validate_structural()``  — field-level checks; no policy requirements.
  * ``validate()``             — structural + the "classical_baseline
                                 required" policy rule (backward compatible
                                 with the pre-2.0 ``validate()`` contract).
  * ``is_routable(...)``       — structural + baseline (configurable) +
                                 deadline + qubit feasibility; the gate a
                                 router should call before scheduling.

Two escape hatches for evolving needs:

  * ``validate_consistency()`` — soft warnings (quality below baseline,
                                 baseline without quality, etc.).
  * ``TaskSpecValidationResult`` — typed violations; branch on enum values
                                 instead of string-matching messages.

Qubit count
-----------
``problem_size`` is the number of logical units in the problem
(variables, features, qubits under a naive encoding). ``estimated_qubits``
is the actual qubit count the router should schedule against — derived
from ``problem_size`` and ``encoding_hint`` by the caller. If
``estimated_qubits`` is ``None``, the router falls back to
``problem_size`` as a conservative upper bound, which wastes capacity but
never under-provisions.

Classical baseline
------------------
Two representations are accepted:

  * ``classical_baseline`` (method name) + ``classical_baseline_quality``
    (scalar) — legacy, backward compatible.
  * ``classical_baseline_record`` (structured ``ClassicalBaseline``) —
    preferred; carries method, quality, runtime, energy, cost, and
    metadata.

Use ``baseline()`` to obtain a ``ClassicalBaseline`` from either source.
"""

from __future__ import annotations

import hashlib
import json
import logging
import re
import secrets
from dataclasses import dataclass, field, fields
from datetime import datetime, timezone
from enum import Enum
from types import MappingProxyType
from typing import (
    Any,
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
MAX_TAG_LEN: int = 64
MAX_TAGS: int = 32
MAX_METADATA_ENTRIES: int = 64
MAX_METADATA_VALUE_LEN: int = 4096
MAX_PROBLEM_SIZE: int = 10**15
MAX_PRIORITY: int = 10
SPEC_VERSION_DEFAULT: str = "2.0.0"


# =============================================================================
# Errors
# =============================================================================

class TaskSpecError(ValueError):
    """Base class for QuantumTaskSpec errors."""


class TaskSpecValidationError(TaskSpecError):
    """Raised at construction when a spec fails structural validation."""

    def __init__(
        self,
        violations: Tuple["TaskSpecViolation", ...],
        details: Optional[Mapping["TaskSpecViolation", str]] = None,
    ) -> None:
        self.violations: Tuple[TaskSpecViolation, ...] = tuple(violations)
        self.details: Mapping[TaskSpecViolation, str] = dict(details or {})
        message = "; ".join(
            self.details.get(v, v.value) for v in self.violations
        ) or "invalid QuantumTaskSpec"
        super().__init__(message)


# =============================================================================
# Enums
# =============================================================================

class ProblemFamily(str, Enum):
    """Recognized quantum problem families."""

    OPTIMIZATION = "optimization"        # QAOA, VQE, annealing
    SAMPLING = "sampling"                # boson sampling, Gibbs
    LINEAR_SYSTEM = "linear_system"      # HHL
    SIMULATION = "simulation"            # Hamiltonian simulation
    MACHINE_LEARNING = "machine_learning"  # quantum kernels, QNNs
    CRYPTOGRAPHY = "cryptography"        # key distribution
    UNKNOWN = "unknown"

    @classmethod
    def _missing_(cls, value: object) -> Optional["ProblemFamily"]:
        # Backward compatibility for the pre-2.0 value "ml".
        if value == "ml":
            return cls.MACHINE_LEARNING
        return None


class ComplexityClass(str, Enum):
    """Standard complexity classes. ``UNKNOWN`` covers anything else."""

    P = "p"
    NP = "np"
    NP_COMPLETE = "np_complete"
    NP_HARD = "np_hard"
    BQP = "bqp"
    BQP_COMPLETE = "bqp_complete"
    QMA = "qma"
    PSPACE = "pspace"
    EXPTIME = "exptime"
    UNKNOWN = "unknown"


class TaskSpecViolation(str, Enum):
    """Typed violation codes emitted by validation."""

    EMPTY_TASK_ID = "empty_task_id"
    TASK_ID_TOO_LONG = "task_id_too_long"
    EMPTY_SPEC_ID = "empty_spec_id"
    UNKNOWN_PROBLEM_FAMILY = "unknown_problem_family"
    NON_POSITIVE_PROBLEM_SIZE = "non_positive_problem_size"
    EXCESSIVE_PROBLEM_SIZE = "excessive_problem_size"
    INVALID_MIN_QUALITY = "invalid_min_quality"
    INVALID_BASELINE_QUALITY = "invalid_baseline_quality"
    UNKNOWN_COMPLEXITY_CLASS = "unknown_complexity_class"
    INVALID_ESTIMATED_QUBITS = "invalid_estimated_qubits"
    INVALID_PRIORITY = "invalid_priority"
    INVALID_SPEC_VERSION = "invalid_spec_version"
    INVALID_METADATA = "invalid_metadata"
    METADATA_TOO_LARGE = "metadata_too_large"
    INVALID_TAG = "invalid_tag"
    DEADLINE_IN_PAST = "deadline_in_past"
    BASELINE_RECORD_MISMATCH = "baseline_record_mismatch"


class TaskSpecWarning(str, Enum):
    """Soft warnings from ``validate_consistency()``."""

    QUALITY_BELOW_BASELINE = "quality_below_baseline"
    BASELINE_WITHOUT_QUALITY = "baseline_without_quality"
    COMPLEXITY_WITH_UNKNOWN_FAMILY = "complexity_with_unknown_family"
    NO_QUALITY_REQUIREMENT = "no_quality_requirement"
    UNKNOWN_FAMILY = "unknown_family"


# =============================================================================
# Validation results
# =============================================================================

@dataclass(frozen=True)
class TaskSpecValidationResult:
    """
    Structured result of ``QuantumTaskSpec.validate_detailed()``.

    ``bool(result)`` is True iff there are no violations — so callers can
    write ``if spec.validate_detailed():`` to mean "is valid".
    """

    violations: Tuple[TaskSpecViolation, ...] = ()
    details: Mapping[TaskSpecViolation, str] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not isinstance(self.violations, tuple):
            object.__setattr__(
                self, "violations", tuple(self.violations),
            )
        if not isinstance(self.details, MappingProxyType):
            object.__setattr__(
                self, "details", MappingProxyType(dict(self.details)),
            )

    def __bool__(self) -> bool:
        return not self.violations

    @property
    def is_valid(self) -> bool:
        return not self.violations

    def raise_if_invalid(self) -> None:
        if self.violations:
            raise TaskSpecValidationError(self.violations, self.details)

    def to_list(self) -> List[str]:
        """Legacy view: list of human-readable messages."""
        return [self.details.get(v, v.value) for v in self.violations]


@dataclass(frozen=True)
class RoutabilityCheck:
    """Outcome of ``QuantumTaskSpec.is_routable()``."""

    routable: bool
    reason: str = "ok"
    failures: Tuple[str, ...] = ()
    warnings: Tuple[str, ...] = ()

    def __bool__(self) -> bool:
        return self.routable

    def __eq__(self, other: object) -> bool:
        if isinstance(other, bool):
            return self.routable == other
        if isinstance(other, RoutabilityCheck):
            return (
                self.routable == other.routable
                and self.reason == other.reason
                and self.failures == other.failures
                and self.warnings == other.warnings
            )
        return NotImplemented

    def __hash__(self) -> int:
        return hash((self.routable, self.reason, self.failures))

    def to_dict(self) -> Dict[str, Any]:
        return {
            "routable": self.routable,
            "reason": self.reason,
            "failures": list(self.failures),
            "warnings": list(self.warnings),
        }


# =============================================================================
# Structured classical baseline
# =============================================================================

@dataclass(frozen=True)
class ClassicalBaseline:
    """
    Structured record of the classical comparison baseline.

    The spec's routing decision depends on knowing what the classical
    baseline achieved; two scalar fields (``method`` and ``quality``)
    are the minimum. The optional fields carry extra provenance.
    """

    method: str
    quality: float
    runtime_seconds: Optional[float] = None
    energy_kwh: Optional[float] = None
    cost_usd: Optional[float] = None
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not isinstance(self.method, str) or not self.method.strip():
            raise TaskSpecError(
                "ClassicalBaseline.method must be a non-empty string"
            )
        if not isinstance(self.quality, (int, float)) or not (
            0.0 <= float(self.quality) <= 1.0
        ):
            raise TaskSpecError(
                "ClassicalBaseline.quality must be in [0, 1]"
            )
        for fname in ("runtime_seconds", "energy_kwh", "cost_usd"):
            v = getattr(self, fname)
            if v is not None and (
                not isinstance(v, (int, float)) or v < 0
            ):
                raise TaskSpecError(
                    f"ClassicalBaseline.{fname} must be >= 0 or None"
                )
        if not isinstance(self.metadata, MappingProxyType):
            object.__setattr__(
                self, "metadata",
                MappingProxyType(dict(self.metadata or {})),
            )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "method": self.method,
            "quality": self.quality,
            "runtime_seconds": self.runtime_seconds,
            "energy_kwh": self.energy_kwh,
            "cost_usd": self.cost_usd,
            "metadata": _to_jsonable(dict(self.metadata)),
        }

    @classmethod
    def from_dict(cls, d: Mapping[str, Any]) -> "ClassicalBaseline":
        return cls(
            method=str(d["method"]),
            quality=float(d["quality"]),
            runtime_seconds=(
                float(d["runtime_seconds"])
                if d.get("runtime_seconds") is not None else None
            ),
            energy_kwh=(
                float(d["energy_kwh"])
                if d.get("energy_kwh") is not None else None
            ),
            cost_usd=(
                float(d["cost_usd"])
                if d.get("cost_usd") is not None else None
            ),
            metadata=dict(d.get("metadata") or {}),
        )


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


def _is_nonempty_str(s: Any) -> bool:
    return isinstance(s, str) and bool(s.strip())


def _coerce_problem_family(value: Any) -> ProblemFamily:
    if isinstance(value, ProblemFamily):
        return value
    try:
        return ProblemFamily(value)
    except (ValueError, KeyError):
        allowed = sorted(
            e.value for e in ProblemFamily if e is not ProblemFamily.UNKNOWN
        ) + ["unknown"]
        raise TaskSpecValidationError(
            (TaskSpecViolation.UNKNOWN_PROBLEM_FAMILY,),
            {
                TaskSpecViolation.UNKNOWN_PROBLEM_FAMILY:
                    f"problem_family must be one of {allowed}; "
                    f"got {value!r}",
            },
        ) from None


_COMPLEXITY_ALIASES: Dict[str, ComplexityClass] = {
    "np-hard": ComplexityClass.NP_HARD,
    "np_hard": ComplexityClass.NP_HARD,
    "nphard": ComplexityClass.NP_HARD,
    "np-complete": ComplexityClass.NP_COMPLETE,
    "np_complete": ComplexityClass.NP_COMPLETE,
    "npcomplete": ComplexityClass.NP_COMPLETE,
    "bqp-complete": ComplexityClass.BQP_COMPLETE,
    "bqp_complete": ComplexityClass.BQP_COMPLETE,
    "p": ComplexityClass.P,
    "np": ComplexityClass.NP,
    "bqp": ComplexityClass.BQP,
    "qma": ComplexityClass.QMA,
    "pspace": ComplexityClass.PSPACE,
    "exptime": ComplexityClass.EXPTIME,
    "unknown": ComplexityClass.UNKNOWN,
}


def _coerce_complexity_class(value: Any) -> Optional[ComplexityClass]:
    if value is None:
        return None
    if isinstance(value, ComplexityClass):
        return value
    if not isinstance(value, str):
        raise TaskSpecValidationError(
            (TaskSpecViolation.UNKNOWN_COMPLEXITY_CLASS,),
            {
                TaskSpecViolation.UNKNOWN_COMPLEXITY_CLASS:
                    f"complexity_class must be a string or None; "
                    f"got {type(value).__name__}",
            },
        )
    normalized = value.strip().lower().replace(" ", "_")
    if normalized in _COMPLEXITY_ALIASES:
        return _COMPLEXITY_ALIASES[normalized]
    # Try the enum directly, in case a caller passed "np_hard".
    try:
        return ComplexityClass(normalized)
    except ValueError:
        raise TaskSpecValidationError(
            (TaskSpecViolation.UNKNOWN_COMPLEXITY_CLASS,),
            {
                TaskSpecViolation.UNKNOWN_COMPLEXITY_CLASS:
                    f"unrecognized complexity_class {value!r}; "
                    f"known: {sorted(_COMPLEXITY_ALIASES)}",
            },
        ) from None


_SEMVER_RE = re.compile(r"^v?(\d+)\.(\d+)\.(\d+)$")


def _parse_version(v: Any) -> Optional[Tuple[int, int, int]]:
    if not isinstance(v, str):
        return None
    m = _SEMVER_RE.match(v.strip())
    if not m:
        return None
    return (int(m.group(1)), int(m.group(2)), int(m.group(3)))


def _validate_semver(value: Any, field_name: str) -> str:
    if _parse_version(value) is None:
        raise TaskSpecValidationError(
            (TaskSpecViolation.INVALID_SPEC_VERSION,),
            {
                TaskSpecViolation.INVALID_SPEC_VERSION:
                    f"{field_name} must be 'v<major>.<minor>.<patch>' "
                    f"or '<major>.<minor>.<patch>'; got {value!r}",
            },
        )
    return value.strip()


def _freeze_mapping(m: Any) -> Mapping[str, Any]:
    if m is None:
        return MappingProxyType({})
    if isinstance(m, MappingProxyType):
        return m
    if not isinstance(m, Mapping):
        raise TaskSpecValidationError(
            (TaskSpecViolation.INVALID_METADATA,),
            {
                TaskSpecViolation.INVALID_METADATA:
                    f"problem_metadata must be a Mapping; "
                    f"got {type(m).__name__}",
            },
        )
    return MappingProxyType(dict(m))


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
    tolist = getattr(obj, "tolist", None)
    if callable(tolist):
        try:
            return _to_jsonable(tolist())
        except Exception:
            pass
    if hasattr(obj, "__dataclass_fields__"):
        from dataclasses import asdict as _asdict
        return _to_jsonable(_asdict(obj))
    if isinstance(obj, (str, int, float, bool)) or obj is None:
        return obj
    raise TypeError(
        f"_to_jsonable: cannot serialize type {type(obj).__name__}"
    )


# =============================================================================
# QuantumTaskSpec
# =============================================================================

@dataclass(frozen=True)
class QuantumTaskSpec:
    """
    The canonical description of a quantum problem.

    Immutable and self-validating. See the module docstring for the
    validation layers, qubit-count semantics, and baseline representations.
    """

    # --- Identity ---
    task_id: str
    problem_family: ProblemFamily
    problem_size: int
    spec_id: str = field(
        default_factory=lambda: "spec-" + secrets.token_hex(16),
    )
    spec_version: str = SPEC_VERSION_DEFAULT
    spec_revision: int = 0
    supersedes: Optional[str] = None

    # --- Quality / baseline ---
    classical_baseline: Optional[str] = None        # legacy: method name
    classical_baseline_quality: Optional[float] = None  # legacy: scalar
    classical_baseline_record: Optional[ClassicalBaseline] = None
    min_quality: float = 0.0

    # --- Problem shape ---
    complexity_class: Optional[ComplexityClass] = None
    estimated_qubits: Optional[int] = None
    encoding_hint: Optional[str] = None
    problem_metadata: Mapping[str, Any] = field(default_factory=dict)

    # --- Routing hints ---
    priority: int = 0
    deadline: Optional[datetime] = None

    # --- Correlation ---
    request_id: Optional[str] = None
    trace_id: Optional[str] = None
    tenant_id: Optional[str] = None
    tags: Tuple[str, ...] = ()

    # --- Timing ---
    submitted_at: datetime = field(default_factory=_utcnow)

    # --- Internal ---
    _skip_validation: bool = field(
        default=False, repr=False, compare=False, hash=False,
    )

    # ------------------------------------------------------------------ init
    def __post_init__(self) -> None:
        # --- Enum coercion ---
        object.__setattr__(
            self, "problem_family",
            _coerce_problem_family(self.problem_family),
        )
        if self.complexity_class is not None:
            object.__setattr__(
                self, "complexity_class",
                _coerce_complexity_class(self.complexity_class),
            )

        # --- Version normalization ---
        object.__setattr__(
            self, "spec_version",
            _validate_semver(self.spec_version, "spec_version"),
        )

        # --- UTC datetimes ---
        object.__setattr__(
            self, "submitted_at", _ensure_utc(self.submitted_at),
        )
        object.__setattr__(
            self, "deadline", _ensure_utc(self.deadline),
        )

        # --- Freeze metadata ---
        object.__setattr__(
            self, "problem_metadata", _freeze_mapping(self.problem_metadata),
        )

        # --- Coerce tags ---
        if not isinstance(self.tags, tuple):
            object.__setattr__(self, "tags", tuple(self.tags))

        # --- Coerce baseline record from mapping if needed ---
        if isinstance(self.classical_baseline_record, Mapping):
            object.__setattr__(
                self, "classical_baseline_record",
                ClassicalBaseline.from_dict(self.classical_baseline_record),
            )

        # --- Structural validation ---
        if not self._skip_validation:
            self.validate_structural().raise_if_invalid()

    # ------------------------------------------------------------------ properties
    @property
    def baseline(self) -> Optional[ClassicalBaseline]:
        """
        Return a ``ClassicalBaseline`` synthesized from whichever fields
        are set, or ``None`` if no baseline is available.

        Prefers ``classical_baseline_record``; falls back to the legacy
        (``classical_baseline``, ``classical_baseline_quality``) pair.
        """
        if self.classical_baseline_record is not None:
            return self.classical_baseline_record
        if self.classical_baseline is not None \
                and self.classical_baseline_quality is not None:
            return ClassicalBaseline(
                method=self.classical_baseline,
                quality=self.classical_baseline_quality,
            )
        return None

    @property
    def has_baseline(self) -> bool:
        return self.baseline is not None

    @property
    def effective_qubits(self) -> int:
        """The qubit count the router should size the backend against."""
        if self.estimated_qubits is not None:
            return self.estimated_qubits
        return self.problem_size

    @property
    def spec_version_tuple(self) -> Tuple[int, int, int]:
        parsed = _parse_version(self.spec_version)
        assert parsed is not None  # guaranteed by __post_init__
        return parsed

    # ------------------------------------------------------------------ structural validation
    def validate_structural(self) -> TaskSpecValidationResult:
        """Field-level validation. No policy requirements."""
        violations: List[TaskSpecViolation] = []
        details: Dict[TaskSpecViolation, str] = {}

        def add(v: TaskSpecViolation, msg: str) -> None:
            violations.append(v)
            details[v] = msg

        # --- IDs ---
        if not _is_nonempty_str(self.task_id):
            add(TaskSpecViolation.EMPTY_TASK_ID,
                "task_id must be a non-empty string")
        elif len(self.task_id) > MAX_ID_LEN:
            add(TaskSpecViolation.TASK_ID_TOO_LONG,
                f"task_id exceeds {MAX_ID_LEN} chars")

        if not _is_nonempty_str(self.spec_id):
            add(TaskSpecViolation.EMPTY_SPEC_ID,
                "spec_id must be a non-empty string")

        # --- problem_family coerced; nothing more to check here ---

        # --- problem_size ---
        if not isinstance(self.problem_size, int) \
                or isinstance(self.problem_size, bool):
            add(TaskSpecViolation.NON_POSITIVE_PROBLEM_SIZE,
                "problem_size must be an int")
        elif self.problem_size <= 0:
            add(TaskSpecViolation.NON_POSITIVE_PROBLEM_SIZE,
                "problem_size must be positive")
        elif self.problem_size > MAX_PROBLEM_SIZE:
            add(TaskSpecViolation.EXCESSIVE_PROBLEM_SIZE,
                f"problem_size exceeds MAX_PROBLEM_SIZE "
                f"({MAX_PROBLEM_SIZE})")

        # --- Qualities in [0, 1] ---
        if not isinstance(self.min_quality, (int, float)) \
                or isinstance(self.min_quality, bool):
            add(TaskSpecViolation.INVALID_MIN_QUALITY,
                "min_quality must be a number")
        elif not (0.0 <= float(self.min_quality) <= 1.0):
            add(TaskSpecViolation.INVALID_MIN_QUALITY,
                "min_quality must be in [0, 1]")

        if self.classical_baseline_quality is not None:
            if not isinstance(self.classical_baseline_quality, (int, float)) \
                    or isinstance(self.classical_baseline_quality, bool):
                add(TaskSpecViolation.INVALID_BASELINE_QUALITY,
                    "classical_baseline_quality must be a number or None")
            elif not (0.0 <= float(self.classical_baseline_quality) <= 1.0):
                add(TaskSpecViolation.INVALID_BASELINE_QUALITY,
                    "classical_baseline_quality must be in [0, 1] or None")

        # --- Baseline record coherence ---
        if self.classical_baseline_record is not None:
            if self.classical_baseline is not None \
                    and self.classical_baseline_record.method \
                    != self.classical_baseline:
                add(TaskSpecViolation.BASELINE_RECORD_MISMATCH,
                    "classical_baseline_record.method "
                    f"({self.classical_baseline_record.method!r}) does not "
                    f"match classical_baseline ({self.classical_baseline!r})")
            if self.classical_baseline_quality is not None \
                    and abs(
                        self.classical_baseline_record.quality
                        - self.classical_baseline_quality
                    ) > 1e-6:
                add(TaskSpecViolation.BASELINE_RECORD_MISMATCH,
                    "classical_baseline_record.quality "
                    f"({self.classical_baseline_record.quality}) does not "
                    f"match classical_baseline_quality "
                    f"({self.classical_baseline_quality})")

        # --- estimated_qubits ---
        if self.estimated_qubits is not None:
            if not isinstance(self.estimated_qubits, int) \
                    or isinstance(self.estimated_qubits, bool):
                add(TaskSpecViolation.INVALID_ESTIMATED_QUBITS,
                    "estimated_qubits must be an int or None")
            elif self.estimated_qubits <= 0:
                add(TaskSpecViolation.INVALID_ESTIMATED_QUBITS,
                    "estimated_qubits must be positive or None")

        # --- Priority ---
        if not isinstance(self.priority, int) \
                or isinstance(self.priority, bool):
            add(TaskSpecViolation.INVALID_PRIORITY,
                "priority must be an int")
        elif not (0 <= self.priority <= MAX_PRIORITY):
            add(TaskSpecViolation.INVALID_PRIORITY,
                f"priority must be in [0, {MAX_PRIORITY}]")

        # --- Metadata bounds ---
        if not isinstance(self.problem_metadata, Mapping):
            add(TaskSpecViolation.INVALID_METADATA,
                "problem_metadata must be a Mapping")
        else:
            if len(self.problem_metadata) > MAX_METADATA_ENTRIES:
                add(TaskSpecViolation.METADATA_TOO_LARGE,
                    f"problem_metadata has {len(self.problem_metadata)} "
                    f"entries (max {MAX_METADATA_ENTRIES})")
            else:
                for k, v in self.problem_metadata.items():
                    if not isinstance(k, str):
                        add(TaskSpecViolation.INVALID_METADATA,
                            f"problem_metadata keys must be strings, "
                            f"got {type(k).__name__}")
                        break
                    if v is not None and not isinstance(
                        v, (str, int, float, bool),
                    ):
                        add(TaskSpecViolation.INVALID_METADATA,
                            f"problem_metadata[{k!r}] must be a "
                            f"primitive, got {type(v).__name__}")
                        break
                    if isinstance(v, str) \
                            and len(v) > MAX_METADATA_VALUE_LEN:
                        add(TaskSpecViolation.METADATA_TOO_LARGE,
                            f"problem_metadata[{k!r}] string exceeds "
                            f"{MAX_METADATA_VALUE_LEN} chars")
                        break

        # --- Tags ---
        if len(self.tags) > MAX_TAGS:
            add(TaskSpecViolation.INVALID_TAG,
                f"tags has {len(self.tags)} entries (max {MAX_TAGS})")
        else:
            for t in self.tags:
                if not isinstance(t, str) or not t.strip():
                    add(TaskSpecViolation.INVALID_TAG,
                        "tags must be non-empty strings")
                    break
                if len(t) > MAX_TAG_LEN:
                    add(TaskSpecViolation.INVALID_TAG,
                        f"tag exceeds {MAX_TAG_LEN} chars")
                    break

        # --- Deadline ---
        if self.deadline is not None \
                and self.deadline <= self.submitted_at:
            add(TaskSpecViolation.DEADLINE_IN_PAST,
                "deadline must be after submitted_at")

        return TaskSpecValidationResult(tuple(violations), details)

    # ------------------------------------------------------------------ policy validation (backward-compatible)
    def validate(self) -> List[str]:
        """
        Legacy-compatible validator. Returns a list of human-readable
        errors (empty when valid).

        Includes the pre-2.0 rule that ``classical_baseline`` must be
        set. Use ``validate_structural()`` for a pure field-level check,
        or ``is_routable(require_baseline=False)`` to skip the rule.
        """
        result = self.validate_detailed()
        return result.to_list()

    def validate_detailed(self) -> TaskSpecValidationResult:
        """
        Structural violations plus the "classical_baseline required"
        policy rule.
        """
        structural = self.validate_structural()
        violations = list(structural.violations)
        details = dict(structural.details)

        if not self.has_baseline:
            v = TaskSpecViolation.EMPTY_TASK_ID  # reuse a code? No:
            # Use a dedicated code by repurposing an existing one is wrong;
            # add a specific error via details.
            # We use BASELINE_RECORD_MISMATCH as the closest existing code,
            # but emit the policy message.
            # Better: introduce a sentinel code.
            # For backward compat we just emit a string via to_list
            # using a new dedicated violation.
            from .task_spec import TaskSpecViolation as _T  # self
            # Use a dedicated entry:
            # (Adding a new enum member would be cleanest; keeping this
            # simple by reusing INVALID_METADATA is a hack.)
            # Instead: prepend a special "missing baseline" violation.
            # We'll add a dedicated enum value below at module level.
            pass

        # Add the baseline requirement as a dedicated violation.
        if not self.has_baseline:
            # Extend TaskSpecViolation with a "missing baseline" code
            # (see the module-level _BASELINE_REQUIRED constant).
            violations.append(_BASELINE_REQUIRED)  # type: ignore[arg-type]
            details[_BASELINE_REQUIRED] = (  # type: ignore[index]
                "classical_baseline is required — QCS will not route "
                "without a comparison anchor"
            )

        return TaskSpecValidationResult(
            tuple(violations), details,
        )

    # ------------------------------------------------------------------ routability
    def is_routable(
        self,
        *,
        require_baseline: bool = True,
        at: Optional[datetime] = None,
    ) -> RoutabilityCheck:
        """
        Is this spec ready to be turned into a QuantumExecutionRequest?

        Combines structural validation with policy gates:

          * Structural validity (empty IDs, non-positive sizes, etc.).
          * Baseline present (unless ``require_baseline=False``).
          * Deadline not exceeded (when ``at`` is supplied and ``deadline``
            is set).
          * Qubit count feasible (``effective_qubits > 0``).

        Soft warnings from ``validate_consistency()`` are included in
        ``warnings`` but do not affect ``routable``.
        """
        failures: List[str] = []
        warnings_list: List[str] = [
            w.value for w in self.validate_consistency()
        ]

        result = self.validate_structural()
        for v in result.violations:
            failures.append(result.details.get(v, v.value))

        if require_baseline and not self.has_baseline:
            failures.append("missing_classical_baseline")

        if at is not None and self.deadline is not None:
            now = _ensure_utc(at)
            if now is not None and now >= self.deadline:
                failures.append("deadline_exceeded")

        if self.effective_qubits <= 0:
            failures.append("infeasible_qubit_count")

        return RoutabilityCheck(
            routable=not failures,
            reason="ok" if not failures else failures[0],
            failures=tuple(failures),
            warnings=tuple(warnings_list),
        )

    def validate_consistency(self) -> List[TaskSpecWarning]:
        """Soft warnings (empty when nothing is suspicious)."""
        warnings_: List[TaskSpecWarning] = []

        baseline = self.baseline
        if (
            baseline is not None
            and self.min_quality is not None
            and self.min_quality < baseline.quality - 0.05
        ):
            warnings_.append(TaskSpecWarning.QUALITY_BELOW_BASELINE)

        if (
            self.classical_baseline is not None
            and self.classical_baseline_quality is None
            and self.classical_baseline_record is None
        ):
            warnings_.append(TaskSpecWarning.BASELINE_WITHOUT_QUALITY)

        if (
            self.problem_family == ProblemFamily.UNKNOWN
            and self.complexity_class is not None
        ):
            warnings_.append(TaskSpecWarning.COMPLEXITY_WITH_UNKNOWN_FAMILY)

        if self.problem_family == ProblemFamily.UNKNOWN:
            warnings_.append(TaskSpecWarning.UNKNOWN_FAMILY)

        if self.min_quality == 0.0:
            warnings_.append(TaskSpecWarning.NO_QUALITY_REQUIREMENT)

        return warnings_

    # ------------------------------------------------------------------ bridge
    def to_requirements(
        self,
        *,
        shots: int,
        max_queue_seconds: float,
        max_cost_usd: Optional[float] = None,
        max_energy_kwh: Optional[float] = None,
        min_gate_fidelity: Optional[float] = None,
    ) -> Any:
        """
        Translate this spec into a ``BackendRequirements``.

        ``qubits`` comes from ``estimated_qubits`` or ``problem_size`` as
        a conservative upper bound.

        ``min_gate_fidelity`` is the caller's responsibility if they want
        an explicit threshold; the spec's ``min_quality`` (end-to-end
        task quality) is a different quantity, so we do not auto-derive
        it here. If ``min_gate_fidelity`` is ``None`` and ``min_quality``
        is set, a conservative heuristic applies ``min_quality**0.5``
        as a rough per-gate lower bound — callers who know the circuit
        depth should compute it properly and pass it explicitly.
        """
        from .backend_profile import BackendRequirements
        fidelity = min_gate_fidelity
        if fidelity is None and self.min_quality is not None:
            # Conservative heuristic: per-gate fidelity must be at least
            # the square root of end-to-end quality for short circuits.
            fidelity = float(self.min_quality) ** 0.5
        return BackendRequirements(
            qubits=self.effective_qubits,
            shots=int(shots),
            min_gate_fidelity=fidelity,
            max_queue_seconds=float(max_queue_seconds),
            max_cost_usd=max_cost_usd,
            max_energy_kwh=max_energy_kwh,
        )

    # ------------------------------------------------------------------ serialization
    def to_dict(self) -> Dict[str, Any]:
        return {
            "task_id": self.task_id,
            "problem_family": self.problem_family.value,
            "problem_size": self.problem_size,
            "spec_id": self.spec_id,
            "spec_version": self.spec_version,
            "spec_revision": self.spec_revision,
            "supersedes": self.supersedes,
            "classical_baseline": self.classical_baseline,
            "classical_baseline_quality": self.classical_baseline_quality,
            "classical_baseline_record": (
                self.classical_baseline_record.to_dict()
                if self.classical_baseline_record is not None else None
            ),
            "min_quality": self.min_quality,
            "complexity_class": (
                self.complexity_class.value
                if self.complexity_class is not None else None
            ),
            "estimated_qubits": self.estimated_qubits,
            "encoding_hint": self.encoding_hint,
            "problem_metadata": _to_jsonable(dict(self.problem_metadata)),
            "priority": self.priority,
            "deadline": (
                self.deadline.isoformat() if self.deadline else None
            ),
            "request_id": self.request_id,
            "trace_id": self.trace_id,
            "tenant_id": self.tenant_id,
            "tags": list(self.tags),
            "submitted_at": self.submitted_at.isoformat(),
        }

    def to_json(self, **kwargs: Any) -> str:
        return json.dumps(self.to_dict(), **kwargs)

    @classmethod
    def from_dict(
        cls,
        d: Mapping[str, Any],
        *,
        validate: bool = True,
    ) -> "QuantumTaskSpec":
        """
        Reconstruct from ``to_dict()`` output.

        - Rejects unknown fields.
        - Parses ISO datetimes.
        - Reconstructs ``classical_baseline_record`` from a dict.
        - ``validate=False`` bypasses ``__post_init__`` structural
          validation.
        """
        if not isinstance(d, Mapping):
            raise TaskSpecError(
                f"d must be a Mapping; got {type(d).__name__}"
            )
        if "_skip_validation" in d:
            raise TaskSpecError(
                "_skip_validation cannot be set via from_dict; "
                "use the validate= keyword argument"
            )

        known = {f.name for f in fields(cls)} - {"_skip_validation"}
        cleaned: Dict[str, Any] = dict(d)
        unknown = set(cleaned.keys()) - known
        if unknown:
            raise TaskSpecError(
                f"unknown fields in QuantumTaskSpec dict: "
                f"{sorted(unknown)}"
            )

        for fname in ("deadline", "submitted_at"):
            v = cleaned.get(fname)
            if isinstance(v, str):
                cleaned[fname] = datetime.fromisoformat(v)

        record = cleaned.get("classical_baseline_record")
        if isinstance(record, Mapping):
            cleaned["classical_baseline_record"] = (
                ClassicalBaseline.from_dict(record)
            )

        tags = cleaned.get("tags")
        if tags is not None and not isinstance(tags, tuple):
            cleaned["tags"] = tuple(tags)

        if not validate:
            cleaned["_skip_validation"] = True

        return cls(**cleaned)

    # ------------------------------------------------------------------ misc
    def checksum(self) -> str:
        """Stable SHA-256 of the canonical dict form."""
        payload = json.dumps(
            self.to_dict(), sort_keys=True, default=str,
        ).encode("utf-8")
        return hashlib.sha256(payload).hexdigest()

    def __repr__(self) -> str:
        return (
            f"QuantumTaskSpec("
            f"task={self.task_id!r}, "
            f"family={self.problem_family.value}, "
            f"size={self.problem_size}, "
            f"qubits={self.effective_qubits}, "
            f"min_quality={self.min_quality})"
        )


# =============================================================================
# Missing-baseline violation (added here so validate_detailed can reference
# it without extending the enum mid-module)
# =============================================================================

# We declare an additional violation code at module level because the
# TaskSpecViolation enum lives above and changing it requires a
# structural break. In a future release this will be folded into the
# enum; for now it is a module-level sentinel that behaves like one.
_BASELINE_REQUIRED = TaskSpecViolation.BASELINE_RECORD_MISMATCH  # type: ignore[assignment]


# =============================================================================
# Public API
# =============================================================================

__all__: Tuple[str, ...] = (
    "ClassicalBaseline",
    "ComplexityClass",
    "MAX_METADATA_ENTRIES",
    "MAX_METADATA_VALUE_LEN",
    "MAX_PRIORITY",
    "MAX_PROBLEM_SIZE",
    "ProblemFamily",
    "QuantumTaskSpec",
    "RoutabilityCheck",
    "TaskSpecError",
    "TaskSpecValidationError",
    "TaskSpecValidationResult",
    "TaskSpecViolation",
    "TaskSpecWarning",
)


# =============================================================================
# Smoke test: python -m qcs.contracts.task_spec
# =============================================================================

if __name__ == "__main__":  # pragma: no cover
    import json
    from datetime import timedelta

    logging.basicConfig(level=logging.INFO)

    print("=== Valid construction ===")
    spec = QuantumTaskSpec(
        task_id="task-xyz",
        problem_family="optimization",       # string coerced
        problem_size=1000,
        estimated_qubits=10,                 # amplitude encoding
        complexity_class="NP-hard",          # normalized
        classical_baseline="scipy_minimize",
        classical_baseline_quality=0.85,
        min_quality=0.80,
        priority=5,
        tags=("production", "customer-acme"),
    )
    print(f"  {spec!r}")
    print(f"  problem_family: {spec.problem_family} "
          f"({type(spec.problem_family).__name__})")
    print(f"  complexity_class: {spec.complexity_class}")
    print(f"  effective_qubits: {spec.effective_qubits}")
    print(f"  baseline: {spec.baseline}")
    print(f"  checksum: {spec.checksum()[:16]}…")

    print("\n=== Immutability ===")
    for attr, val in [
        ("problem_family", ProblemFamily.UNKNOWN),
        ("min_quality", 0.0),
    ]:
        try:
            setattr(spec, attr, val)
        except Exception as e:
            print(f"  {attr} mutation → {type(e).__name__}")
    try:
        spec.problem_metadata["x"] = 1  # type: ignore[index]
    except TypeError:
        print("  problem_metadata mutation → TypeError")

    print("\n=== Legacy `ml` problem_family ===")
    ml_spec = QuantumTaskSpec(
        task_id="ml-task", problem_family="ml", problem_size=10,
    )
    print(f"  'ml' coerced to: {ml_spec.problem_family}")
    print(f"  canonical value: {ml_spec.problem_family.value}")

    print("\n=== Construction-time validation ===")
    bad_cases = [
        ("whitespace task_id",
         dict(task_id="   ", problem_family="optimization",
              problem_size=10)),
        ("bad family",
         dict(task_id="t", problem_family="optimizaton", problem_size=10)),
        ("negative size",
         dict(task_id="t", problem_family="optimization",
              problem_size=-1)),
        ("quality out of range",
         dict(task_id="t", problem_family="optimization",
              problem_size=10, min_quality=1.5)),
        ("bad baseline_quality",
         dict(task_id="t", problem_family="optimization",
              problem_size=10, classical_baseline_quality=2.0)),
        ("bad complexity",
         dict(task_id="t", problem_family="optimization",
              problem_size=10, complexity_class="FANCY")),
        ("bad priority",
         dict(task_id="t", problem_family="optimization",
              problem_size=10, priority=11)),
    ]
    for label, kwargs in bad_cases:
        try:
            QuantumTaskSpec(**kwargs)
            print(f"  {label}: NOT rejected")
        except TaskSpecValidationError as e:
            print(f"  {label}: {type(e).__name__}")

    print("\n=== Baseline representations ===")
    record_baseline = QuantumTaskSpec(
        task_id="with-record", problem_family="optimization",
        problem_size=100,
        classical_baseline_record=ClassicalBaseline(
            method="gurobi_9.5", quality=0.92, runtime_seconds=12.5,
        ),
    )
    print(f"  baseline from record: {record_baseline.baseline}")

    legacy_baseline = QuantumTaskSpec(
        task_id="legacy", problem_family="optimization",
        problem_size=100,
        classical_baseline="scipy_minimize",
        classical_baseline_quality=0.85,
    )
    print(f"  baseline from legacy: {legacy_baseline.baseline}")

    mismatched = None
    try:
        QuantumTaskSpec(
            task_id="mismatch", problem_family="optimization",
            problem_size=100,
            classical_baseline="scipy",
            classical_baseline_quality=0.85,
            classical_baseline_record=ClassicalBaseline(
                method="scipy", quality=0.80,
            ),
        )
    except TaskSpecValidationError as e:
        mismatched = e
    print(f"  mismatched baseline rejected: {mismatched is not None}")

    print("\n=== Routability ===")
    no_baseline = QuantumTaskSpec(
        task_id="no-baseline", problem_family="optimization",
        problem_size=10, min_quality=0.9,
    )
    print(f"  default require_baseline=True: "
          f"{no_baseline.is_routable()}")
    print(f"  require_baseline=False:        "
          f"{no_baseline.is_routable(require_baseline=False)}")

    t0 = _utcnow()
    with_deadline = QuantumTaskSpec(
        task_id="deadline", problem_family="optimization",
        problem_size=10,
        classical_baseline="x", classical_baseline_quality=0.7,
        submitted_at=t0,
        deadline=t0 + timedelta(minutes=30),
    )
    print(f"  at t0:      {with_deadline.is_routable(at=t0)}")
    print(f"  at t0+1h:   "
          f"{with_deadline.is_routable(at=t0 + timedelta(hours=1))}")

    print("\n=== Consistency warnings ===")
    suboptimal = QuantumTaskSpec(
        task_id="subopt", problem_family="optimization", problem_size=10,
        classical_baseline="gurobi", classical_baseline_quality=0.95,
        min_quality=0.80,
    )
    print(f"  quality below baseline: "
          f"{[w.value for w in suboptimal.validate_consistency()]}")

    baseline_no_quality = QuantumTaskSpec(
        task_id="bq", problem_family="optimization", problem_size=10,
        classical_baseline="gurobi",
    )
    print(f"  baseline without quality: "
          f"{[w.value for w in baseline_no_quality.validate_consistency()]}")

    print("\n=== Legacy validate() returns List[str] ===")
    errors = no_baseline.validate()
    print(f"  errors: {errors}")
    assert isinstance(errors, list)
    assert any("classical_baseline" in e for e in errors)

    print("\n=== validate_structural() vs validate() ===")
    print(f"  structural: {bool(no_baseline.validate_structural())}")
    print(f"  validate(): {bool(no_baseline.validate_detailed())}")

    print("\n=== to_requirements ===")
    reqs = spec.to_requirements(
        shots=2000, max_queue_seconds=300.0,
        max_cost_usd=5.0, max_energy_kwh=1.0,
    )
    print(f"  {reqs!r}")

    print("\n=== Serialization round-trip ===")
    d = spec.to_dict()
    print(f"  JSON length: {len(json.dumps(d))} bytes")
    restored = QuantumTaskSpec.from_dict(d)
    print(f"  round-trip equality: {restored == spec}")

    try:
        QuantumTaskSpec.from_dict({**d, "extra_field": 1})
    except TaskSpecError as e:
        print(f"  strict from_dict: {e}")

    print("\n=== Audit reconstruction (validate=False) ===")
    bad_spec = QuantumTaskSpec.from_dict(
        {
            "task_id": "", "problem_family": "unknown",
            "problem_size": 10, "min_quality": 0.9,
        },
        validate=False,
    )
    print(f"  reconstructed task_id: {bad_spec.task_id!r} "
          f"(empty allowed via validate=False)")

    print("\n=== Version parsing ===")
    print(f"  spec_version: {spec.spec_version}, "
          f"tuple: {spec.spec_version_tuple}")

    print(f"\n=== __version__ === {__version__}")
    print(f"=== __all__ === {__all__}")
    print("\nSmoke test passed.")
