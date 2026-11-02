"""
QuantumExecutionRequest — the budgeted, reversible quantum job spec.

An immutable, self-validating contract. A ``QuantumExecutionRequest``
instance cannot be constructed with invalid data (the constructor
validates), and cannot be mutated after construction (the dataclass is
frozen).

Budgets
-------
Every budget field uses ``None`` to mean "no budget":

  * ``max_cost_usd=None``         — no monetary cap
  * ``max_energy_kwh=None``       — no energy cap
  * ``max_queue_seconds=300.0``   — 5-minute queue cap (0 = must not queue)

``max_cost_usd=0.0`` explicitly means "zero dollars allowed" (which will
reject every hardware backend — see ``validate_consistency()``).

Reversibility
-------------
A request can be reversed along three dimensions:

  * ``expires_at``        — absolute instant after which the request is void
  * ``max_queue_seconds`` — relative age after which the request is void
  * ``deadline``          — absolute instant by which the task must complete
  * ``idempotency_key``   — stable key for retry deduplication
  * ``supersedes``        — ID of a prior request that this one replaces

Use ``is_expired(at)`` to check, or ``validate_for_dispatch(at)`` for a
combined structural + expiry check suitable for gating submission.

Validation
----------
Construction validates automatically and raises ``InvalidRequestError``
on violation. To reconstruct a request from audit history (which may
contain data that would fail today's validation — e.g. an expired
request), pass ``_skip_validation=True`` via ``from_dict(validate=False)``.
"""

from __future__ import annotations

import hashlib
import json
import logging
import re
import threading
from dataclasses import dataclass, field, fields, replace
from datetime import datetime, timezone
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

# Hard caps — configurable per deployment by overriding these constants
# (e.g. in a sitecustomize or via a QcsLimits dataclass).
MAX_SHOTS_HARD_LIMIT: int = 10_000_000
MAX_QUEUE_SECONDS_HARD_LIMIT: float = 30 * 24 * 3600.0  # 30 days
MAX_PRIORITY: int = 10
MAX_ID_LEN: int = 256
MAX_METADATA_ENTRIES: int = 64
MAX_METADATA_VALUE_LEN: int = 4096
SEED_MAX: int = 2**64


# =============================================================================
# Errors
# =============================================================================

class InvalidRequestError(ValueError):
    """
    Raised when a ``QuantumExecutionRequest`` fails validation.

    Carries the typed violations so callers can branch on them without
    string-matching the message.
    """

    def __init__(
        self,
        violations: Tuple["RequestViolation", ...],
        details: Optional[Mapping["RequestViolation", str]] = None,
    ) -> None:
        self.violations: Tuple[RequestViolation, ...] = tuple(violations)
        self.details: Mapping[RequestViolation, str] = dict(details or {})
        message = "; ".join(
            self.details.get(v, v.value) for v in self.violations
        ) or "invalid QuantumExecutionRequest"
        super().__init__(message)


# =============================================================================
# Enums
# =============================================================================

class BackendPreference(str, Enum):
    """
    Routing preference.

    - ``AUTO``: router chooses based on budgets and availability.
      Honors all constraints; may select hardware or simulator.
    - ``SIMULATOR``: must be a simulator. Falls back to classical
      (if allowed) rather than hardware.
    - ``HARDWARE``: must be real hardware. Falls back to classical
      (if allowed) rather than simulator.
    """

    AUTO = "auto"
    SIMULATOR = "simulator"
    HARDWARE = "hardware"


class EncodingHint(str, Enum):
    """Common encoding hints. Extend with ``EncodingRegistry.register(...)``."""

    AMPLITUDE = "amplitude"
    ANGLE = "angle"
    BASIS = "basis"
    IQP = "iqp"
    DATA_REUPLOADING = "data_reuploading"


class RequestViolation(str, Enum):
    """Typed violation codes; stable identifiers for downstream branching."""

    # Hard violations (construction fails)
    EMPTY_REQUEST_ID = "empty_request_id"
    REQUEST_ID_TOO_LONG = "request_id_too_long"
    EMPTY_TASK_ID = "empty_task_id"
    TASK_ID_TOO_LONG = "task_id_too_long"
    EMPTY_ALGORITHM = "empty_algorithm"
    UNKNOWN_ALGORITHM = "unknown_algorithm"
    UNKNOWN_BACKEND_PREFERENCE = "unknown_backend_preference"
    UNKNOWN_ENCODING_HINT = "unknown_encoding_hint"
    NON_POSITIVE_MAX_SHOTS = "non_positive_max_shots"
    EXCESSIVE_MAX_SHOTS = "excessive_max_shots"
    NEGATIVE_MAX_QUEUE = "negative_max_queue"
    EXCESSIVE_MAX_QUEUE = "excessive_max_queue"
    INVALID_MIN_QUALITY = "invalid_min_quality"
    NEGATIVE_MAX_COST = "negative_max_cost"
    NEGATIVE_MAX_ENERGY = "negative_max_energy"
    INVALID_SEED = "invalid_seed"
    INVALID_POLICY_VERSION = "invalid_policy_version"
    INVALID_PRIORITY = "invalid_priority"
    INVALID_IDEMPOTENCY_KEY = "invalid_idempotency_key"
    INVALID_TRACE_ID = "invalid_trace_id"
    INVALID_TENANT_ID = "invalid_tenant_id"
    METADATA_TOO_LARGE = "metadata_too_large"
    METADATA_INVALID = "metadata_invalid"
    DEADLINE_IN_PAST = "deadline_in_past"
    DEADLINE_EXCEEDS_QUEUE = "deadline_exceeds_queue"

    # Expiry (only reported by validate_for_dispatch / validate(at=...))
    EXPIRED = "expired"

    # Soft warnings (reported by validate_consistency)
    INCONSISTENT_BUDGETS = "inconsistent_budgets"
    IMPOSSIBLE_QUALITY_CHECK = "impossible_quality_check"
    ZERO_QUEUE_BUDGET = "zero_queue_budget"


# =============================================================================
# Registries
# =============================================================================

class AlgorithmRegistry:
    """Registry of known algorithms; plugins can extend it."""

    _known: set = {
        "vqe", "qaoa", "grover", "qnn", "qsvm",
        "amplitude_estimation", "phase_estimation",
        "shor", "simon", "bernstein_vazirani",
    }
    _lock = threading.RLock()

    @classmethod
    def register(cls, name: str) -> None:
        if not isinstance(name, str) or not name.strip():
            raise ValueError("algorithm name must be a non-empty string")
        with cls._lock:
            cls._known.add(name.strip().lower())

    @classmethod
    def is_known(cls, name: str) -> bool:
        if not isinstance(name, str):
            return False
        return name.strip().lower() in cls._known

    @classmethod
    def all(cls) -> List[str]:
        with cls._lock:
            return sorted(cls._known)


class EncodingRegistry:
    """Registry of known encoding hints; plugins can extend it."""

    _known: set = {e.value for e in EncodingHint}
    _lock = threading.RLock()

    @classmethod
    def register(cls, name: str) -> None:
        if not isinstance(name, str) or not name.strip():
            raise ValueError("encoding name must be a non-empty string")
        with cls._lock:
            cls._known.add(name.strip().lower())

    @classmethod
    def is_known(cls, name: str) -> bool:
        if not isinstance(name, str):
            return False
        return name.strip().lower() in cls._known

    @classmethod
    def all(cls) -> List[str]:
        with cls._lock:
            return sorted(cls._known)


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


def _coerce_enum(enum_cls: type, value: Any, field_name: str) -> Enum:
    if isinstance(value, enum_cls):
        return value
    try:
        return enum_cls(value)
    except (ValueError, KeyError):
        allowed = sorted(e.value for e in enum_cls)
        raise ValueError(
            f"{field_name} must be one of {allowed}; got {value!r}"
        ) from None


def _freeze_mapping(m: Any) -> Mapping[str, Any]:
    if m is None:
        return MappingProxyType({})
    if isinstance(m, MappingProxyType):
        return m
    if not isinstance(m, Mapping):
        raise TypeError(
            f"metadata must be a Mapping, got {type(m).__name__}"
        )
    return MappingProxyType(dict(m))


def _to_jsonable(obj: Any) -> Any:
    if isinstance(obj, Enum):
        return obj.value
    if isinstance(obj, Mapping):
        return {k: _to_jsonable(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple, set, frozenset)):
        return [_to_jsonable(v) for v in obj]
    if isinstance(obj, datetime):
        return obj.isoformat()
    if hasattr(obj, "__dataclass_fields__"):
        from dataclasses import asdict
        return _to_jsonable(asdict(obj))
    return obj


_SEMVER_RE = re.compile(r"^v?(\d+)\.(\d+)\.(\d+)$")


def _parse_version(v: Any) -> Optional[Tuple[int, int, int]]:
    if not isinstance(v, str):
        return None
    m = _SEMVER_RE.match(v.strip())
    if not m:
        return None
    return (int(m.group(1)), int(m.group(2)), int(m.group(3)))


def _is_nonempty_str(s: Any) -> bool:
    return isinstance(s, str) and bool(s.strip())


# =============================================================================
# Validation result
# =============================================================================

@dataclass(frozen=True)
class RequestValidationResult:
    """
    Structured outcome of ``QuantumExecutionRequest.validate()``.

    ``bool(result)`` is True iff there are no violations — so callers can
    write ``if request.validate():`` to mean "is valid".
    """

    violations: Tuple[RequestViolation, ...] = ()
    details: Mapping[RequestViolation, str] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not isinstance(self.violations, tuple):
            object.__setattr__(
                self, "violations", tuple(self.violations),
            )
        if not isinstance(self.details, MappingProxyType):
            object.__setattr__(
                self, "details", _freeze_mapping(self.details),
            )

    def __bool__(self) -> bool:
        return not self.violations

    @property
    def is_valid(self) -> bool:
        return not self.violations

    def raise_if_invalid(self) -> None:
        if self.violations:
            raise InvalidRequestError(self.violations, self.details)

    def to_list(self) -> List[str]:
        """Legacy view: list of human-readable messages."""
        return [
            self.details.get(v, v.value) for v in self.violations
        ]


# =============================================================================
# Main class
# =============================================================================

@dataclass(frozen=True)
class QuantumExecutionRequest:
    """
    A budgeted, reversible quantum job request.

    Immutable. Validated at construction. Every budget is explicit and
    uses ``None`` for "no cap".

    See the module docstring for the semantics of ``max_cost_usd=None``
    vs ``max_cost_usd=0.0``, and for the reversibility fields.
    """

    # --- Identity ---
    request_id: str
    task_id: str
    algorithm: str

    # --- Routing ---
    backend_preference: BackendPreference = BackendPreference.AUTO
    encoding_hint: Optional[str] = None

    # --- Budgets ---
    max_shots: int = 1000
    max_queue_seconds: float = 300.0
    max_cost_usd: Optional[float] = None     # None = no monetary budget
    max_energy_kwh: Optional[float] = None   # None = no energy budget
    min_quality: float = 0.8
    allow_classical_fallback: bool = True

    # --- Policy / metadata ---
    policy_version: str = "v5.0.0"
    seed: Optional[int] = None
    requested_at: datetime = field(default_factory=_utcnow)
    metadata: Mapping[str, Any] = field(default_factory=dict)

    # --- Reversibility ---
    expires_at: Optional[datetime] = None
    deadline: Optional[datetime] = None
    supersedes: Optional[str] = None
    idempotency_key: Optional[str] = None

    # --- Accounting / tracing ---
    priority: int = 0
    trace_id: Optional[str] = None
    tenant_id: Optional[str] = None
    user_id: Optional[str] = None

    # --- Internal (not serialized) ---
    _skip_validation: bool = field(
        default=False, repr=False, compare=False,
    )

    # ------------------------------------------------------------------ init
    def __post_init__(self) -> None:
        # --- Coerce enums (raises ValueError on typos) ---
        object.__setattr__(
            self, "backend_preference",
            _coerce_enum(
                BackendPreference, self.backend_preference,
                "backend_preference",
            ),
        )

        # --- Normalize algorithm ---
        if isinstance(self.algorithm, str):
            object.__setattr__(
                self, "algorithm", self.algorithm.strip().lower(),
            )

        # --- Normalize encoding_hint (keep as string; validated against registry) ---
        if self.encoding_hint is not None:
            if isinstance(self.encoding_hint, EncodingHint):
                object.__setattr__(
                    self, "encoding_hint", self.encoding_hint.value,
                )
            elif isinstance(self.encoding_hint, str):
                object.__setattr__(
                    self, "encoding_hint",
                    self.encoding_hint.strip().lower(),
                )
            # else: leave as-is; validate() will flag it

        # --- UTC datetimes ---
        object.__setattr__(
            self, "requested_at", _ensure_utc(self.requested_at),
        )
        if self.expires_at is not None:
            object.__setattr__(
                self, "expires_at", _ensure_utc(self.expires_at),
            )
        if self.deadline is not None:
            object.__setattr__(
                self, "deadline", _ensure_utc(self.deadline),
            )

        # --- Freeze metadata (if it's the right shape) ---
        if isinstance(self.metadata, Mapping):
            object.__setattr__(
                self, "metadata", _freeze_mapping(self.metadata),
            )

        # --- Validate (unless explicitly skipped) ---
        if not self._skip_validation:
            result = self.validate()
            if result.violations:
                result.raise_if_invalid()

    # ------------------------------------------------------------------ properties
    @property
    def max_cost(self) -> float:
        """
        Deprecated alias for ``max_cost_usd``.

        Returns 0.0 when ``max_cost_usd`` is None, preserving the old
        "0 = unlimited" convention for callers that haven't migrated.
        """
        return self.max_cost_usd if self.max_cost_usd is not None else 0.0

    @property
    def policy_version_tuple(self) -> Tuple[int, int, int]:
        """Parsed numeric version — safe for comparison."""
        parsed = _parse_version(self.policy_version)
        if parsed is None:
            raise ValueError(
                f"invalid policy_version: {self.policy_version!r}"
            )
        return parsed

    # ------------------------------------------------------------------ expiry
    def is_expired(self, at: Optional[datetime] = None) -> bool:
        """
        True if the request should no longer be dispatched.

        Checks, in order: ``expires_at``, queue-age vs ``max_queue_seconds``,
        and absolute ``deadline``.
        """
        now = _ensure_utc(at) or _utcnow()
        if self.expires_at is not None and now >= self.expires_at:
            return True
        if self.max_queue_seconds > 0:
            age = (now - self.requested_at).total_seconds()
            if age > self.max_queue_seconds:
                return True
        if self.deadline is not None and now >= self.deadline:
            return True
        return False

    # ------------------------------------------------------------------ validation
    def validate(self) -> RequestValidationResult:
        """
        Structural validation. Does **not** check expiry — use
        :meth:`is_expired` or :meth:`validate_for_dispatch` for that.
        """
        violations: List[RequestViolation] = []
        details: Dict[RequestViolation, str] = {}

        def add(v: RequestViolation, msg: str) -> None:
            violations.append(v)
            details[v] = msg

        # --- IDs ---
        if not _is_nonempty_str(self.request_id):
            add(RequestViolation.EMPTY_REQUEST_ID,
                "request_id must be a non-empty string")
        elif len(self.request_id) > MAX_ID_LEN:
            add(RequestViolation.REQUEST_ID_TOO_LONG,
                f"request_id exceeds {MAX_ID_LEN} characters")

        if not _is_nonempty_str(self.task_id):
            add(RequestViolation.EMPTY_TASK_ID,
                "task_id must be a non-empty string")
        elif len(self.task_id) > MAX_ID_LEN:
            add(RequestViolation.TASK_ID_TOO_LONG,
                f"task_id exceeds {MAX_ID_LEN} characters")

        # --- Algorithm ---
        if not _is_nonempty_str(self.algorithm):
            add(RequestViolation.EMPTY_ALGORITHM,
                "algorithm must be a non-empty string")
        elif not AlgorithmRegistry.is_known(self.algorithm):
            add(RequestViolation.UNKNOWN_ALGORITHM,
                f"unknown algorithm '{self.algorithm}'; "
                f"register via AlgorithmRegistry.register(...)")

        # --- Backend preference (defensive; construction coerces) ---
        if not isinstance(self.backend_preference, BackendPreference):
            add(RequestViolation.UNKNOWN_BACKEND_PREFERENCE,
                f"unknown backend_preference '{self.backend_preference}'")

        # --- Encoding hint ---
        if self.encoding_hint is not None \
                and not EncodingRegistry.is_known(self.encoding_hint):
            add(RequestViolation.UNKNOWN_ENCODING_HINT,
                f"unknown encoding_hint '{self.encoding_hint}'; "
                f"register via EncodingRegistry.register(...)")

        # --- Shots ---
        if not isinstance(self.max_shots, int) or self.max_shots <= 0:
            add(RequestViolation.NON_POSITIVE_MAX_SHOTS,
                "max_shots must be a positive int")
        elif self.max_shots > MAX_SHOTS_HARD_LIMIT:
            add(RequestViolation.EXCESSIVE_MAX_SHOTS,
                f"max_shots exceeds hard limit ({MAX_SHOTS_HARD_LIMIT})")

        # --- Queue ---
        if not isinstance(self.max_queue_seconds, (int, float)):
            add(RequestViolation.NEGATIVE_MAX_QUEUE,
                "max_queue_seconds must be numeric")
        elif self.max_queue_seconds < 0:
            add(RequestViolation.NEGATIVE_MAX_QUEUE,
                "max_queue_seconds must be non-negative")
        elif self.max_queue_seconds > MAX_QUEUE_SECONDS_HARD_LIMIT:
            add(RequestViolation.EXCESSIVE_MAX_QUEUE,
                f"max_queue_seconds exceeds hard limit "
                f"({MAX_QUEUE_SECONDS_HARD_LIMIT}s)")

        # --- Quality ---
        if not isinstance(self.min_quality, (int, float)) \
                or not 0.0 <= float(self.min_quality) <= 1.0:
            add(RequestViolation.INVALID_MIN_QUALITY,
                "min_quality must be a number in [0, 1]")

        # --- Cost budget ---
        if self.max_cost_usd is not None:
            if not isinstance(self.max_cost_usd, (int, float)):
                add(RequestViolation.NEGATIVE_MAX_COST,
                    "max_cost_usd must be numeric or None")
            elif self.max_cost_usd < 0:
                add(RequestViolation.NEGATIVE_MAX_COST,
                    "max_cost_usd must be >= 0 or None")

        # --- Energy budget ---
        if self.max_energy_kwh is not None:
            if not isinstance(self.max_energy_kwh, (int, float)):
                add(RequestViolation.NEGATIVE_MAX_ENERGY,
                    "max_energy_kwh must be numeric or None")
            elif self.max_energy_kwh < 0:
                add(RequestViolation.NEGATIVE_MAX_ENERGY,
                    "max_energy_kwh must be >= 0 or None")

        # --- Seed ---
        if self.seed is not None:
            if isinstance(self.seed, bool) \
                    or not isinstance(self.seed, int):
                add(RequestViolation.INVALID_SEED,
                    "seed must be an int or None")
            elif not (0 <= self.seed < SEED_MAX):
                add(RequestViolation.INVALID_SEED,
                    f"seed must be in [0, 2^64)")

        # --- Policy version ---
        if _parse_version(self.policy_version) is None:
            add(RequestViolation.INVALID_POLICY_VERSION,
                f"invalid policy_version '{self.policy_version}'; "
                f"expected v<major>.<minor>.<patch>")

        # --- Priority ---
        if isinstance(self.priority, bool) \
                or not isinstance(self.priority, int):
            add(RequestViolation.INVALID_PRIORITY,
                "priority must be an int")
        elif not (0 <= self.priority <= MAX_PRIORITY):
            add(RequestViolation.INVALID_PRIORITY,
                f"priority must be in [0, {MAX_PRIORITY}]")

        # --- Idempotency key ---
        if self.idempotency_key is not None \
                and not _is_nonempty_str(self.idempotency_key):
            add(RequestViolation.INVALID_IDEMPOTENCY_KEY,
                "idempotency_key must be a non-empty string or None")

        # --- Trace / tenant ---
        if self.trace_id is not None and not _is_nonempty_str(self.trace_id):
            add(RequestViolation.INVALID_TRACE_ID,
                "trace_id must be a non-empty string or None")
        if self.tenant_id is not None and not _is_nonempty_str(self.tenant_id):
            add(RequestViolation.INVALID_TENANT_ID,
                "tenant_id must be a non-empty string or None")

        # --- Metadata ---
        if not isinstance(self.metadata, Mapping):
            add(RequestViolation.METADATA_INVALID,
                "metadata must be a Mapping")
        else:
            if len(self.metadata) > MAX_METADATA_ENTRIES:
                add(RequestViolation.METADATA_TOO_LARGE,
                    f"metadata has {len(self.metadata)} entries "
                    f"(max {MAX_METADATA_ENTRIES})")
            else:
                for k, v in self.metadata.items():
                    if not isinstance(k, str):
                        add(RequestViolation.METADATA_INVALID,
                            f"metadata keys must be strings, "
                            f"got {type(k).__name__}")
                        break
                    if v is not None \
                            and not isinstance(v, (str, int, float, bool)):
                        add(RequestViolation.METADATA_INVALID,
                            f"metadata[{k!r}] must be a primitive, "
                            f"got {type(v).__name__}")
                        break
                    if isinstance(v, str) \
                            and len(v) > MAX_METADATA_VALUE_LEN:
                        add(RequestViolation.METADATA_TOO_LARGE,
                            f"metadata[{k!r}] string exceeds "
                            f"{MAX_METADATA_VALUE_LEN} chars")
                        break

        # --- Deadline consistency ---
        if self.deadline is not None:
            if self.deadline <= self.requested_at:
                add(RequestViolation.DEADLINE_IN_PAST,
                    "deadline must be after requested_at")
            elif self.max_queue_seconds > 0:
                delta = (
                    self.deadline - self.requested_at
                ).total_seconds()
                if delta > float(self.max_queue_seconds):
                    add(RequestViolation.DEADLINE_EXCEEDS_QUEUE,
                        f"deadline is {delta:.0f}s away but "
                        f"max_queue_seconds is {self.max_queue_seconds:.0f}s")

        return RequestValidationResult(tuple(violations), details)

    def validate_for_dispatch(
        self, at: Optional[datetime] = None,
    ) -> RequestValidationResult:
        """Structural validation + expiry check. Use to gate submission."""
        result = self.validate()
        if self.is_expired(at):
            new_violations = result.violations + (
                RequestViolation.EXPIRED,
            )
            new_details = dict(result.details)
            new_details[RequestViolation.EXPIRED] = (
                f"request expired at {(_ensure_utc(at) or _utcnow()).isoformat()}"
            )
            return RequestValidationResult(new_violations, new_details)
        return result

    def validate_consistency(self) -> List[RequestViolation]:
        """Soft warnings (empty list on success)."""
        warnings_: List[RequestViolation] = []
        if self.max_cost_usd is not None and self.max_cost_usd == 0.0 \
                and not self.allow_classical_fallback:
            warnings_.append(RequestViolation.INCONSISTENT_BUDGETS)
        if self.min_quality == 1.0 and self.max_shots == 1:
            warnings_.append(RequestViolation.IMPOSSIBLE_QUALITY_CHECK)
        if self.max_queue_seconds == 0:
            warnings_.append(RequestViolation.ZERO_QUEUE_BUDGET)
        return warnings_

    # ------------------------------------------------------------------ bridge
    def as_requirements(
        self,
        *,
        required_qubits: int,
        min_gate_fidelity: Optional[float] = None,
    ) -> Any:
        """
        Translate this request into a ``BackendRequirements``.

        ``required_qubits`` and ``min_gate_fidelity`` are not part of the
        request (they depend on the algorithm + encoding_hint + task
        spec); the caller must supply them.
        """
        from .backend_profile import BackendRequirements
        return BackendRequirements(
            qubits=int(required_qubits),
            shots=int(self.max_shots),
            min_gate_fidelity=min_gate_fidelity,
            max_queue_seconds=float(self.max_queue_seconds),
            max_cost_usd=self.max_cost_usd,
            max_energy_kwh=self.max_energy_kwh,
        )

    # ------------------------------------------------------------------ serialization
    def to_dict(self) -> Dict[str, Any]:
        """JSON-safe dict — enums as strings, tuples as lists, datetimes as ISO."""
        return {
            "request_id": self.request_id,
            "task_id": self.task_id,
            "algorithm": self.algorithm,
            "backend_preference": self.backend_preference.value,
            "encoding_hint": self.encoding_hint,
            "max_shots": self.max_shots,
            "max_queue_seconds": self.max_queue_seconds,
            "max_cost_usd": self.max_cost_usd,
            "max_energy_kwh": self.max_energy_kwh,
            "min_quality": self.min_quality,
            "allow_classical_fallback": self.allow_classical_fallback,
            "policy_version": self.policy_version,
            "seed": self.seed,
            "requested_at": self.requested_at.isoformat(),
            "metadata": _to_jsonable(dict(self.metadata)),
            "expires_at": (
                self.expires_at.isoformat()
                if self.expires_at else None
            ),
            "deadline": (
                self.deadline.isoformat() if self.deadline else None
            ),
            "supersedes": self.supersedes,
            "idempotency_key": self.idempotency_key,
            "priority": self.priority,
            "trace_id": self.trace_id,
            "tenant_id": self.tenant_id,
            "user_id": self.user_id,
        }

    @classmethod
    def from_dict(
        cls,
        d: Mapping[str, Any],
        *,
        validate: bool = True,
    ) -> "QuantumExecutionRequest":
        """
        Construct from ``to_dict()`` output.

        - Rejects unknown fields (except a legacy ``max_cost`` alias).
        - Parses ISO-format datetimes.
        - ``validate=False`` bypasses ``__post_init__`` validation, for
          reconstructing possibly-expired records from audit history.
        """
        if not isinstance(d, Mapping):
            raise TypeError(
                f"d must be a Mapping, got {type(d).__name__}"
            )
        if "_skip_validation" in d:
            raise ValueError(
                "_skip_validation cannot be set via from_dict; "
                "use the validate= keyword argument"
            )

        known = {f.name for f in fields(cls)} - {"_skip_validation"}
        cleaned: Dict[str, Any] = dict(d)

        # Legacy compatibility: max_cost → max_cost_usd.
        if "max_cost" in cleaned and "max_cost_usd" not in cleaned:
            legacy = cleaned.pop("max_cost")
            cleaned["max_cost_usd"] = legacy if legacy else None

        unknown = set(cleaned.keys()) - known
        if unknown:
            raise ValueError(
                f"unknown fields in QuantumExecutionRequest dict: "
                f"{sorted(unknown)}"
            )

        for fname in ("requested_at", "expires_at", "deadline"):
            v = cleaned.get(fname)
            if isinstance(v, str):
                cleaned[fname] = datetime.fromisoformat(v)

        if not validate:
            cleaned["_skip_validation"] = True

        return cls(**cleaned)

    # ------------------------------------------------------------------ misc
    def checksum(self) -> str:
        """
        Stable SHA-256 hex digest of the canonical dict form.

        Useful as an idempotency key fallback or for detecting duplicate
        requests without an explicit ``idempotency_key``.
        """
        payload = json.dumps(
            self.to_dict(), sort_keys=True, default=str,
        ).encode("utf-8")
        return hashlib.sha256(payload).hexdigest()

    def __repr__(self) -> str:
        return (
            f"QuantumExecutionRequest("
            f"id={self.request_id!r}, "
            f"task={self.task_id!r}, "
            f"algorithm={self.algorithm!r}, "
            f"pref={self.backend_preference.value}, "
            f"shots={self.max_shots}, "
            f"priority={self.priority})"
        )


# =============================================================================
# Public API
# =============================================================================

__all__: Tuple[str, ...] = (
    "AlgorithmRegistry",
    "BackendPreference",
    "EncodingHint",
    "EncodingRegistry",
    "InvalidRequestError",
    "MAX_METADATA_ENTRIES",
    "MAX_METADATA_VALUE_LEN",
    "MAX_PRIORITY",
    "MAX_QUEUE_SECONDS_HARD_LIMIT",
    "MAX_SHOTS_HARD_LIMIT",
    "QuantumExecutionRequest",
    "RequestValidationResult",
    "RequestViolation",
)


# =============================================================================
# Smoke test: python -m qcs.contracts.execution_request
# =============================================================================

if __name__ == "__main__":  # pragma: no cover
    import json
    from datetime import timedelta

    logging.basicConfig(level=logging.INFO)

    print("=== Valid construction ===")
    req = QuantumExecutionRequest(
        request_id="req-001",
        task_id="task-xyz",
        algorithm="VQE",  # normalized to "vqe"
        backend_preference="hardware",  # coerced
        encoding_hint=EncodingHint.AMPLITUDE,
        max_shots=2000,
        max_queue_seconds=600.0,
        max_cost_usd=2.5,
        max_energy_kwh=0.5,
        min_quality=0.85,
        policy_version="v5.2.0",
        seed=42,
        priority=7,
        idempotency_key="client-req-abc",
        trace_id="trace-123",
        tenant_id="acme",
        metadata={"owner": "ml-team", "tag": 42},
    )
    print(f"  {req!r}")
    print(f"  backend_preference: {req.backend_preference} "
          f"({type(req.backend_preference).__name__})")
    print(f"  algorithm: {req.algorithm}")
    print(f"  encoding_hint: {req.encoding_hint}")
    print(f"  version tuple: {req.policy_version_tuple}")
    print(f"  max_cost_usd: {req.max_cost_usd}  "
          f"(legacy .max_cost: {req.max_cost})")

    print("\n=== Immutability ===")
    for attr, new_val in [
        ("max_shots", 10**9),
        ("backend_preference", BackendPreference.SIMULATOR),
        ("metadata", {"x": 1}),
    ]:
        try:
            setattr(req, attr, new_val)
        except Exception as e:
            print(f"  {attr} mutation → {type(e).__name__}")
    try:
        req.metadata["injected"] = True  # type: ignore[index]
    except TypeError as e:
        print(f"  metadata mutation → TypeError")

    print("\n=== Construction-time validation ===")
    bad_cases = [
        ("empty request_id",
         dict(request_id="", task_id="t", algorithm="vqe")),
        ("unknown algorithm",
         dict(request_id="r", task_id="t", algorithm="vqe_")),
        ("bad backend_pref",
         dict(request_id="r", task_id="t", algorithm="vqe",
              backend_preference="hardware_")),
        ("negative shots",
         dict(request_id="r", task_id="t", algorithm="vqe",
              max_shots=-1)),
        ("bad quality",
         dict(request_id="r", task_id="t", algorithm="vqe",
              min_quality=1.5)),
        ("negative cost",
         dict(request_id="r", task_id="t", algorithm="vqe",
              max_cost_usd=-1.0)),
        ("bad seed",
         dict(request_id="r", task_id="t", algorithm="vqe",
              seed=-1)),
        ("bad version",
         dict(request_id="r", task_id="t", algorithm="vqe",
              policy_version="v5")),
        ("bad priority",
         dict(request_id="r", task_id="t", algorithm="vqe",
              priority=11)),
        ("metadata non-primitive",
         dict(request_id="r", task_id="t", algorithm="vqe",
              metadata={"bad": object()})),
    ]
    for label, kwargs in bad_cases:
        try:
            QuantumExecutionRequest(**kwargs)
            print(f"  {label}: NOT rejected")
        except (InvalidRequestError, ValueError) as e:
            print(f"  {label}: rejected ({type(e).__name__})")

    print("\n=== Budget sentinels ===")
    unlimited = QuantumExecutionRequest(
        request_id="r1", task_id="t", algorithm="vqe",
        max_cost_usd=None, max_energy_kwh=None,
    )
    print(f"  max_cost_usd=None → max_cost={unlimited.max_cost} (legacy 0=unlimited)")
    zero_budget = QuantumExecutionRequest(
        request_id="r2", task_id="t", algorithm="vqe",
        max_cost_usd=0.0,
    )
    print(f"  max_cost_usd=0.0  → max_cost={zero_budget.max_cost}")
    print(f"  consistency: {zero_budget.validate_consistency()}")

    print("\n=== Expiry ===")
    t0 = _utcnow()
    short = QuantumExecutionRequest(
        request_id="r3", task_id="t", algorithm="vqe",
        requested_at=t0,
        max_queue_seconds=60.0,
        expires_at=t0 + timedelta(seconds=120),
    )
    print(f"  at t0:       expired={short.is_expired(at=t0)}")
    print(f"  at t0+90s:   expired={short.is_expired(at=t0 + timedelta(seconds=90))}")
    print(f"  at t0+130s:  expired={short.is_expired(at=t0 + timedelta(seconds=130))}")

    dispatch_check = short.validate_for_dispatch(at=t0 + timedelta(seconds=90))
    print(f"  validate_for_dispatch at t0+90s: {dispatch_check.violations}")

    print("\n=== as_requirements ===")
    reqs = req.as_requirements(required_qubits=6, min_gate_fidelity=0.99)
    print(f"  {reqs!r}")

    print("\n=== Serialization round-trip ===")
    d = req.to_dict()
    print(f"  JSON: {json.dumps(d, default=str)[:90]}…")
    restored = QuantumExecutionRequest.from_dict(d)
    print(f"  round-trip: {restored == req}")

    try:
        QuantumExecutionRequest.from_dict({**d, "extra_field": 1})
    except ValueError as e:
        print(f"  strict from_dict: {e}")

    print("\n=== Legacy max_cost in from_dict ===")
    legacy_dict = {
        "request_id": "legacy", "task_id": "t", "algorithm": "vqe",
        "max_cost": 5.0,
    }
    legacy = QuantumExecutionRequest.from_dict(legacy_dict)
    print(f"  legacy max_cost=5.0 → max_cost_usd={legacy.max_cost_usd}")

    print("\n=== reconstruct via validate=False (audit path) ===")
    t_old = _utcnow() - timedelta(days=30)
    old_dict = {
        "request_id": "old", "task_id": "t", "algorithm": "vqe",
        "requested_at": t_old.isoformat(),
        "max_queue_seconds": 60.0,
    }
    old_req = QuantumExecutionRequest.from_dict(old_dict, validate=False)
    print(f"  reconstructed: is_expired={old_req.is_expired()}")

    print("\n=== checksum / idempotency ===")
    print(f"  checksum: {req.checksum()[:16]}…")
    req_dup = QuantumExecutionRequest(
        **{k: v for k, v in req.to_dict().items()
           if k not in ("requested_at",)},
        requested_at=req.requested_at,
    )
    print(f"  duplicate checksum matches: "
          f"{req.checksum() == req_dup.checksum()}")

    print("\n=== Cross-field warnings ===")
    no_fallback = QuantumExecutionRequest(
        request_id="r4", task_id="t", algorithm="vqe",
        max_cost_usd=0.0, allow_classical_fallback=False,
    )
    print(f"  max_cost=0 + no fallback: "
          f"{no_fallback.validate_consistency()}")

    print("\n=== Registry extension ===")
    AlgorithmRegistry.register("my_custom_algo")
    custom = QuantumExecutionRequest(
        request_id="r5", task_id="t", algorithm="my_custom_algo",
    )
    print(f"  registered algorithm accepted: {custom.algorithm!r}")

    print(f"\n=== __version__ === {__version__}")
    print(f"=== __all__ === {__all__}")
    print("\nSmoke test passed.")
