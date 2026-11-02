"""
QuantumEvidence — the structured route recommendation QCS returns.

A recommendation **with evidence**, not a final decision. The orchestrator,
governance, or policy layer decides whether to act on it.

An instance is immutable and self-validating: invalid combinations (e.g.
``justification="quantum_favored"`` alongside a negative advantage, or
``safety_verdict="pending_review"`` without ``requires_human_review=True``)
raise ``QuantumEvidenceError`` at construction.

What "evidence" means here
--------------------------
The structured-record fields distinguish prediction from evidence:

  * ``expected_*``             — predictions about quality, cost, energy.
  * ``citations``              — references to the data sources that
                                 informed the prediction (historical runs,
                                 forecasts, market snapshots, papers).
  * ``rejected_alternatives``  — the routes that were considered and why
                                 they lost, so an auditor can reconstruct
                                 the decision space.
  * ``guardrail_verdicts``     — which specific safety checks ran, and
                                 whether each passed.
  * ``provenance``             — how much data, from which window, from
                                 which models, went into this record.

Actionability
-------------
``is_actionable()`` collapses the six-condition gate (expiry, justification
not INCONCLUSIVE/BLOCKED, safety_verdict in {PASSED, NOT_REQUIRED}, no
human review required, advantage over a threshold, uncertainty under a
threshold) into one call. Callers who need finer control use the raw fields.

Guidance for governance
-----------------------
  * Treat ``justification`` as a recommendation, not a verdict.
  * Trust ``safety_verdict`` over ``requires_human_review`` — the latter
    is a policy hint, the former is the safety decision.
  * ``BLOCKED`` means do not proceed, regardless of quality/cost.
  * ``is_actionable()`` returns False for anything ambiguous.
"""

from __future__ import annotations

import hashlib
import json
import logging
import re
import secrets
from dataclasses import asdict, dataclass, field, fields
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
# QCS package version (from metadata; falls back to 0.0.0+unknown)
# =============================================================================

def _detect_qcs_version() -> str:
    try:
        from importlib.metadata import (
            version as _pkg_version, PackageNotFoundError,
        )
        try:
            return _pkg_version("green-agent-qcs")
        except PackageNotFoundError:
            return "0.0.0+unknown"
    except ImportError:  # pragma: no cover
        return "0.0.0+unknown"


_QCS_VERSION: str = _detect_qcs_version()


# =============================================================================
# Constants
# =============================================================================

MAX_RATIONALE_ENTRIES: int = 64
MAX_RATIONALE_LINE_LEN: int = 1024
MAX_CITATIONS: int = 128
MAX_REJECTED_ALTERNATIVES: int = 64
MAX_GUARDRAIL_VERDICTS: int = 64
MAX_ID_LEN: int = 256
MAX_DETAIL_LEN: int = 4096


# =============================================================================
# Errors
# =============================================================================

class QuantumEvidenceError(ValueError):
    """Raised when a QuantumEvidence record fails validation."""


# =============================================================================
# Enums
# =============================================================================

class RouteJustification(str, Enum):
    """The QCS recommendation classification."""

    QUANTUM_FAVORED = "quantum_favored"
    DISTILLED_FAVORED = "distilled_favored"
    CLASSICAL_FAVORED = "classical_favored"
    INCONCLUSIVE = "inconclusive"
    BLOCKED = "blocked"


class SafetyVerdict(str, Enum):
    """Outcome of the safety / guardrail evaluation."""

    NOT_REQUIRED = "not_required"      # no guardrail check was needed
    PASSED = "passed"                  # every guardrail passed
    PENDING_REVIEW = "pending_review"  # human review required before acting
    FAILED = "failed"                  # at least one guardrail failed
    BLOCKED = "blocked"                # route forbidden (policy / safety)


class CitationSourceType(str, Enum):
    """What kind of source a citation refers to."""

    HISTORICAL_RUN = "historical_run"
    FORECAST = "forecast"
    MARKET_SNAPSHOT = "market_snapshot"
    PAPER = "paper"
    HEURISTIC = "heuristic"
    GUARDRAIL = "guardrail"
    OTHER = "other"


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


def _coerce_enum(enum_cls: type, value: Any, field_name: str) -> Any:
    if isinstance(value, enum_cls):
        return value
    try:
        return enum_cls(value)
    except (ValueError, KeyError):
        allowed = sorted(e.value for e in enum_cls)
        raise QuantumEvidenceError(
            f"{field_name} must be one of {allowed}; got {value!r}"
        ) from None


def _coerce_bool(value: Any, field_name: str) -> bool:
    if isinstance(value, bool):
        return value
    if value in (0, 1):
        return bool(value)
    raise QuantumEvidenceError(
        f"{field_name} must be a bool; got {type(value).__name__}"
    )


def _coerce_optional_str(
    value: Any,
    field_name: str,
    *,
    max_len: Optional[int] = None,
    allow_none: bool = True,
) -> Optional[str]:
    if value is None:
        if allow_none:
            return None
        raise QuantumEvidenceError(f"{field_name} is required")
    if not isinstance(value, str) or not value.strip():
        raise QuantumEvidenceError(
            f"{field_name} must be a non-empty string"
        )
    if max_len is not None and len(value) > max_len:
        raise QuantumEvidenceError(
            f"{field_name} exceeds {max_len} chars"
        )
    return value


_SEMVER_STRICT_RE = re.compile(r"^v?(\d+)\.(\d+)\.(\d+)$")
_SEMVER_LOOSE_RE = re.compile(r"^v?(\d+)\.(\d+)\.(\d+)(?:[+\-].+)?$")


def _parse_version(v: Any) -> Optional[Tuple[int, int, int]]:
    if not isinstance(v, str):
        return None
    m = _SEMVER_STRICT_RE.match(v.strip())
    if not m:
        return None
    return (int(m.group(1)), int(m.group(2)), int(m.group(3)))


def _validate_policy_version(v: Any) -> Optional[str]:
    if v is None:
        return None
    if not isinstance(v, str) or _parse_version(v) is None:
        raise QuantumEvidenceError(
            f"policy_version must match 'v<major>.<minor>.<patch>'; "
            f"got {v!r}"
        )
    return v.strip()


def _validate_qcs_version(v: Any) -> str:
    if not isinstance(v, str) or not _SEMVER_LOOSE_RE.match(v.strip()):
        raise QuantumEvidenceError(
            f"qcs_version must be a version string; got {v!r}"
        )
    return v.strip()


def _validate_algorithm(value: Any) -> Optional[str]:
    if value is None:
        return None
    if not isinstance(value, str) or not value.strip():
        raise QuantumEvidenceError(
            "recommended_algorithm must be a non-empty string or None"
        )
    normalized = value.strip().lower()
    try:
        from .execution_request import AlgorithmRegistry
    except ImportError:
        return normalized
    if not AlgorithmRegistry.is_known(normalized):
        raise QuantumEvidenceError(
            f"recommended_algorithm {normalized!r} is not registered; "
            f"call AlgorithmRegistry.register({normalized!r}) first"
        )
    return normalized


def _to_jsonable(obj: Any) -> Any:
    """Recursively convert to a JSON-serializable form; raises on unknown."""
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


# =============================================================================
# Structured evidence records
# =============================================================================

@dataclass(frozen=True)
class Citation:
    """A reference to a data source that informed the recommendation."""

    source_type: CitationSourceType
    source_id: str
    weight: float = 1.0
    excerpt: Optional[str] = None

    def __post_init__(self) -> None:
        object.__setattr__(
            self, "source_type",
            _coerce_enum(
                CitationSourceType, self.source_type, "source_type",
            ),
        )
        _coerce_optional_str(
            self.source_id, "source_id",
            max_len=MAX_ID_LEN, allow_none=False,
        )
        if not isinstance(self.weight, (int, float)) \
                or not (0.0 <= float(self.weight) <= 1.0):
            raise QuantumEvidenceError(
                f"weight must be in [0, 1]; got {self.weight!r}"
            )
        _coerce_optional_str(
            self.excerpt, "excerpt", max_len=MAX_DETAIL_LEN,
        )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "source_type": self.source_type.value,
            "source_id": self.source_id,
            "weight": self.weight,
            "excerpt": self.excerpt,
        }

    @classmethod
    def from_dict(cls, d: Mapping[str, Any]) -> "Citation":
        return cls(
            source_type=d["source_type"],
            source_id=d["source_id"],
            weight=float(d.get("weight", 1.0)),
            excerpt=d.get("excerpt"),
        )


@dataclass(frozen=True)
class RejectedAlternative:
    """A route considered and why it lost."""

    reason: str
    backend_id: Optional[str] = None
    algorithm: Optional[str] = None
    expected_quality: Optional[float] = None
    expected_cost_usd: Optional[float] = None
    expected_queue_seconds: Optional[float] = None
    detail: Optional[str] = None

    def __post_init__(self) -> None:
        _coerce_optional_str(
            self.reason, "reason",
            max_len=MAX_DETAIL_LEN, allow_none=False,
        )
        _coerce_optional_str(self.backend_id, "backend_id", max_len=MAX_ID_LEN)
        _coerce_optional_str(self.algorithm, "algorithm", max_len=MAX_ID_LEN)
        _coerce_optional_str(self.detail, "detail", max_len=MAX_DETAIL_LEN)
        if self.expected_quality is not None and not (
            isinstance(self.expected_quality, (int, float))
            and 0.0 <= float(self.expected_quality) <= 1.0
        ):
            raise QuantumEvidenceError(
                "expected_quality must be in [0, 1] or None"
            )
        for fname in ("expected_cost_usd", "expected_queue_seconds"):
            v = getattr(self, fname)
            if v is not None and (
                not isinstance(v, (int, float)) or v < 0
            ):
                raise QuantumEvidenceError(
                    f"{fname} must be >= 0 or None"
                )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "reason": self.reason,
            "backend_id": self.backend_id,
            "algorithm": self.algorithm,
            "expected_quality": self.expected_quality,
            "expected_cost_usd": self.expected_cost_usd,
            "expected_queue_seconds": self.expected_queue_seconds,
            "detail": self.detail,
        }

    @classmethod
    def from_dict(cls, d: Mapping[str, Any]) -> "RejectedAlternative":
        return cls(
            reason=str(d.get("reason", "unspecified")),
            backend_id=d.get("backend_id"),
            algorithm=d.get("algorithm"),
            expected_quality=(
                float(d["expected_quality"])
                if d.get("expected_quality") is not None else None
            ),
            expected_cost_usd=(
                float(d["expected_cost_usd"])
                if d.get("expected_cost_usd") is not None else None
            ),
            expected_queue_seconds=(
                float(d["expected_queue_seconds"])
                if d.get("expected_queue_seconds") is not None else None
            ),
            detail=d.get("detail"),
        )


@dataclass(frozen=True)
class GuardrailVerdict:
    """The outcome of a specific guardrail check."""

    guardrail_id: str
    passed: bool
    detail: Optional[str] = None
    checked_at: datetime = field(default_factory=_utcnow)

    def __post_init__(self) -> None:
        _coerce_optional_str(
            self.guardrail_id, "guardrail_id",
            max_len=MAX_ID_LEN, allow_none=False,
        )
        _coerce_bool(self.passed, "passed")
        _coerce_optional_str(self.detail, "detail", max_len=MAX_DETAIL_LEN)
        object.__setattr__(
            self, "checked_at", _ensure_utc(self.checked_at),
        )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "guardrail_id": self.guardrail_id,
            "passed": self.passed,
            "detail": self.detail,
            "checked_at": self.checked_at.isoformat(),
        }

    @classmethod
    def from_dict(cls, d: Mapping[str, Any]) -> "GuardrailVerdict":
        ts = d.get("checked_at")
        if isinstance(ts, str):
            ts = datetime.fromisoformat(ts)
        return cls(
            guardrail_id=str(d["guardrail_id"]),
            passed=bool(d.get("passed", False)),
            detail=d.get("detail"),
            checked_at=ts if ts is not None else _utcnow(),
        )


@dataclass(frozen=True)
class EvidenceProvenance:
    """What data was consulted to generate this recommendation."""

    historical_window_hours: Optional[float] = None
    sample_count: Optional[int] = None
    models_used: Tuple[str, ...] = ()
    market_snapshot_at: Optional[datetime] = None
    forecast_horizon_hours: Optional[float] = None
    git_commit: Optional[str] = None

    def __post_init__(self) -> None:
        if not isinstance(self.models_used, tuple):
            object.__setattr__(
                self, "models_used", tuple(self.models_used),
            )
        for fname in (
            "historical_window_hours", "forecast_horizon_hours",
        ):
            v = getattr(self, fname)
            if v is not None and (
                not isinstance(v, (int, float)) or v < 0
            ):
                raise QuantumEvidenceError(
                    f"{fname} must be >= 0 or None"
                )
        if self.sample_count is not None and (
            not isinstance(self.sample_count, int)
            or self.sample_count < 0
        ):
            raise QuantumEvidenceError(
                "sample_count must be a non-negative int or None"
            )
        object.__setattr__(
            self, "market_snapshot_at",
            _ensure_utc(self.market_snapshot_at),
        )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "historical_window_hours": self.historical_window_hours,
            "sample_count": self.sample_count,
            "models_used": list(self.models_used),
            "market_snapshot_at": (
                self.market_snapshot_at.isoformat()
                if self.market_snapshot_at else None
            ),
            "forecast_horizon_hours": self.forecast_horizon_hours,
            "git_commit": self.git_commit,
        }

    @classmethod
    def from_dict(cls, d: Mapping[str, Any]) -> "EvidenceProvenance":
        ts = d.get("market_snapshot_at")
        if isinstance(ts, str):
            ts = datetime.fromisoformat(ts)
        models = d.get("models_used", ())
        if not isinstance(models, tuple):
            models = tuple(models)
        return cls(
            historical_window_hours=d.get("historical_window_hours"),
            sample_count=d.get("sample_count"),
            models_used=models,
            market_snapshot_at=ts,
            forecast_horizon_hours=d.get("forecast_horizon_hours"),
            git_commit=d.get("git_commit"),
        )


# =============================================================================
# Coercion helpers for structured lists
# =============================================================================

def _coerce_citation(item: Any) -> Citation:
    if isinstance(item, Citation):
        return item
    if isinstance(item, Mapping):
        return Citation.from_dict(item)
    raise QuantumEvidenceError(
        f"citations entry must be a Mapping or Citation; "
        f"got {type(item).__name__}"
    )


def _coerce_rejected_alternative(item: Any) -> RejectedAlternative:
    if isinstance(item, RejectedAlternative):
        return item
    if isinstance(item, Mapping):
        return RejectedAlternative.from_dict(item)
    raise QuantumEvidenceError(
        f"rejected_alternatives entry must be a Mapping or "
        f"RejectedAlternative; got {type(item).__name__}"
    )


def _coerce_guardrail_verdict(item: Any) -> GuardrailVerdict:
    if isinstance(item, GuardrailVerdict):
        return item
    if isinstance(item, Mapping):
        return GuardrailVerdict.from_dict(item)
    raise QuantumEvidenceError(
        f"guardrail_verdicts entry must be a Mapping or "
        f"GuardrailVerdict; got {type(item).__name__}"
    )


# =============================================================================
# QuantumEvidence
# =============================================================================

@dataclass(frozen=True)
class QuantumEvidence:
    """
    Structured evidence returned by QCS for a quantum route.

    Immutable, self-validating record. See the module docstring for the
    meaning of ``is_actionable()``, ``safety_verdict``, and the structured
    evidence records.
    """

    # --- Identity ---
    task_id: str
    justification: RouteJustification
    recommendation_id: str = field(
        default_factory=lambda: "rec-" + secrets.token_hex(16),
    )
    request_id: Optional[str] = None

    # --- Recommended route ---
    recommended_backend: Optional[str] = None
    recommended_algorithm: Optional[str] = None

    # --- Quality expectation ---
    expected_quality: Optional[float] = None
    classical_baseline_quality: Optional[float] = None
    expected_advantage: Optional[float] = None
    advantage_override_reason: Optional[str] = None
    recommendation_confidence: Optional[float] = None

    # --- Cost expectation (Optional: None = unknown) ---
    expected_queue_seconds: Optional[float] = None
    expected_shots: Optional[int] = None
    expected_energy_kwh: Optional[float] = None
    expected_co2e_kg: Optional[float] = None
    expected_cost_usd: Optional[float] = None

    # --- Uncertainty ---
    quality_uncertainty: Optional[float] = None
    noise_estimate: Optional[float] = None

    # --- Rationale (XAI) and structured evidence ---
    rationale: Tuple[str, ...] = ()
    citations: Tuple[Citation, ...] = ()
    rejected_alternatives: Tuple[RejectedAlternative, ...] = ()
    provenance: Optional[EvidenceProvenance] = None

    # --- Safety & governance ---
    safety_verdict: SafetyVerdict = SafetyVerdict.NOT_REQUIRED
    guardrail_verdicts: Tuple[GuardrailVerdict, ...] = ()
    requires_human_review: bool = False

    # --- Provenance ---
    policy_version: Optional[str] = None
    qcs_version: str = _QCS_VERSION
    generated_at: datetime = field(default_factory=_utcnow)
    expires_at: Optional[datetime] = None

    # --- Echo ---
    trace_id: Optional[str] = None
    idempotency_key: Optional[str] = None

    # --- Internal ---
    _skip_validation: bool = field(
        default=False, repr=False, compare=False, hash=False,
    )

    # ------------------------------------------------------------------ init
    def __post_init__(self) -> None:
        # --- Coerce enums (raises with allowed values on typos) ---
        object.__setattr__(
            self, "justification",
            _coerce_enum(
                RouteJustification, self.justification, "justification",
            ),
        )
        object.__setattr__(
            self, "safety_verdict",
            _coerce_enum(
                SafetyVerdict, self.safety_verdict, "safety_verdict",
            ),
        )

        # --- Coerce structured lists ---
        if not isinstance(self.rationale, tuple):
            object.__setattr__(self, "rationale", tuple(self.rationale))
        if not isinstance(self.citations, tuple):
            object.__setattr__(
                self, "citations",
                tuple(_coerce_citation(x) for x in self.citations),
            )
        if not isinstance(self.rejected_alternatives, tuple):
            object.__setattr__(
                self, "rejected_alternatives",
                tuple(
                    _coerce_rejected_alternative(x)
                    for x in self.rejected_alternatives
                ),
            )
        if not isinstance(self.guardrail_verdicts, tuple):
            object.__setattr__(
                self, "guardrail_verdicts",
                tuple(
                    _coerce_guardrail_verdict(x)
                    for x in self.guardrail_verdicts
                ),
            )

        # --- UTC datetimes ---
        object.__setattr__(
            self, "generated_at", _ensure_utc(self.generated_at),
        )
        object.__setattr__(
            self, "expires_at", _ensure_utc(self.expires_at),
        )

        # --- Coerce bools and validate enums' string values ---
        object.__setattr__(
            self, "requires_human_review",
            _coerce_bool(
                self.requires_human_review, "requires_human_review",
            ),
        )
        object.__setattr__(
            self, "recommended_algorithm",
            _validate_algorithm(self.recommended_algorithm),
        )
        object.__setattr__(
            self, "policy_version",
            _validate_policy_version(self.policy_version),
        )
        object.__setattr__(
            self, "qcs_version",
            _validate_qcs_version(self.qcs_version),
        )

        # --- Validation (unless skipped) ---
        if not self._skip_validation:
            self._validate_basic()
            self._validate_cross_field()

    # ------------------------------------------------------------------ validation
    def _validate_basic(self) -> None:
        # --- Identity ---
        _coerce_optional_str(
            self.task_id, "task_id",
            max_len=MAX_ID_LEN, allow_none=False,
        )
        _coerce_optional_str(
            self.recommendation_id, "recommendation_id",
            max_len=MAX_ID_LEN, allow_none=False,
        )
        _coerce_optional_str(
            self.request_id, "request_id", max_len=MAX_ID_LEN,
        )
        _coerce_optional_str(
            self.recommended_backend, "recommended_backend",
            max_len=MAX_ID_LEN,
        )
        _coerce_optional_str(
            self.advantage_override_reason, "advantage_override_reason",
            max_len=MAX_DETAIL_LEN,
        )
        _coerce_optional_str(self.trace_id, "trace_id", max_len=MAX_ID_LEN)
        _coerce_optional_str(
            self.idempotency_key, "idempotency_key", max_len=MAX_ID_LEN,
        )

        # --- Probability-like values in [0, 1] ---
        for fname in (
            "expected_quality", "classical_baseline_quality",
            "quality_uncertainty", "noise_estimate",
            "recommendation_confidence",
        ):
            v = getattr(self, fname)
            if v is not None:
                if not isinstance(v, (int, float)):
                    raise QuantumEvidenceError(
                        f"{fname} must be a number or None"
                    )
                if not (0.0 <= float(v) <= 1.0):
                    raise QuantumEvidenceError(
                        f"{fname} must be in [0, 1] or None; got {v!r}"
                    )

        # --- Advantage: no strict range, but must be numeric ---
        if self.expected_advantage is not None and not isinstance(
            self.expected_advantage, (int, float),
        ):
            raise QuantumEvidenceError(
                "expected_advantage must be a number or None"
            )

        # --- Non-negative numerics ---
        for fname in (
            "expected_queue_seconds", "expected_energy_kwh",
            "expected_co2e_kg", "expected_cost_usd",
        ):
            v = getattr(self, fname)
            if v is not None and (
                not isinstance(v, (int, float)) or v < 0
            ):
                raise QuantumEvidenceError(
                    f"{fname} must be >= 0 or None"
                )

        # --- Integers ---
        if self.expected_shots is not None:
            if isinstance(self.expected_shots, bool) or not isinstance(
                self.expected_shots, int,
            ):
                raise QuantumEvidenceError(
                    "expected_shots must be an int or None"
                )
            if self.expected_shots < 0:
                raise QuantumEvidenceError(
                    "expected_shots must be >= 0 or None"
                )

        # --- Rationale bounds ---
        if len(self.rationale) > MAX_RATIONALE_ENTRIES:
            raise QuantumEvidenceError(
                f"rationale has {len(self.rationale)} entries "
                f"(max {MAX_RATIONALE_ENTRIES})"
            )
        for line in self.rationale:
            if not isinstance(line, str):
                raise QuantumEvidenceError(
                    f"rationale entries must be strings; "
                    f"got {type(line).__name__}"
                )
            if len(line) > MAX_RATIONALE_LINE_LEN:
                raise QuantumEvidenceError(
                    f"rationale entry exceeds "
                    f"{MAX_RATIONALE_LINE_LEN} chars"
                )

        # --- Structured list bounds ---
        for fname, limit in (
            ("citations", MAX_CITATIONS),
            ("rejected_alternatives", MAX_REJECTED_ALTERNATIVES),
            ("guardrail_verdicts", MAX_GUARDRAIL_VERDICTS),
        ):
            v = getattr(self, fname)
            if len(v) > limit:
                raise QuantumEvidenceError(
                    f"{fname} has {len(v)} entries (max {limit})"
                )

        # --- Expiry coherence ---
        if self.expires_at is not None \
                and self.expires_at <= self.generated_at:
            raise QuantumEvidenceError(
                "expires_at must be after generated_at"
            )

    def _validate_cross_field(self) -> None:
        # --- Advantage coherence with qualities ---
        if (
            self.expected_quality is not None
            and self.classical_baseline_quality is not None
            and self.expected_advantage is not None
            and self.advantage_override_reason is None
        ):
            computed = (
                self.expected_quality - self.classical_baseline_quality
            )
            if abs(self.expected_advantage - computed) > 0.01:
                raise QuantumEvidenceError(
                    f"expected_advantage={self.expected_advantage:.3f} "
                    f"is inconsistent with expected_quality - "
                    f"classical_baseline_quality={computed:.3f}; "
                    f"set advantage_override_reason to override"
                )

        # --- Justification vs. advantage sign ---
        if self.justification == RouteJustification.QUANTUM_FAVORED:
            if (
                self.expected_advantage is not None
                and self.expected_advantage < 0
                and self.advantage_override_reason is None
            ):
                raise QuantumEvidenceError(
                    "justification=quantum_favored contradicts a "
                    "negative expected_advantage; "
                    "set advantage_override_reason to override"
                )
        elif self.justification == RouteJustification.CLASSICAL_FAVORED:
            if (
                self.expected_advantage is not None
                and self.expected_advantage > 0
                and self.advantage_override_reason is None
            ):
                raise QuantumEvidenceError(
                    "justification=classical_favored contradicts a "
                    "positive expected_advantage; "
                    "set advantage_override_reason to override"
                )

        # --- BLOCKED requires the appropriate safety verdict ---
        if self.justification == RouteJustification.BLOCKED:
            if self.safety_verdict not in (
                SafetyVerdict.FAILED, SafetyVerdict.BLOCKED,
            ):
                raise QuantumEvidenceError(
                    "justification=blocked requires "
                    "safety_verdict in {failed, blocked}"
                )

        # --- PENDING_REVIEW requires the review flag ---
        if self.safety_verdict == SafetyVerdict.PENDING_REVIEW \
                and not self.requires_human_review:
            raise QuantumEvidenceError(
                "safety_verdict=pending_review requires "
                "requires_human_review=True"
            )

    # ------------------------------------------------------------------ properties
    @property
    def is_blocked(self) -> bool:
        return self.justification == RouteJustification.BLOCKED

    @property
    def is_inconclusive(self) -> bool:
        return self.justification == RouteJustification.INCONCLUSIVE

    @property
    def policy_version_tuple(self) -> Optional[Tuple[int, int, int]]:
        return _parse_version(self.policy_version) \
            if self.policy_version else None

    @property
    def qcs_version_tuple(self) -> Optional[Tuple[int, int, int]]:
        return _parse_version(self.qcs_version)

    # ------------------------------------------------------------------ expiry
    def is_expired(self, at: Optional[datetime] = None) -> bool:
        """True if the recommendation is past its expiry."""
        if self.expires_at is None:
            return False
        now = _ensure_utc(at) or _utcnow()
        return now >= self.expires_at

    # ------------------------------------------------------------------ actionability
    def is_actionable(
        self,
        *,
        min_advantage: float = 0.05,
        max_uncertainty: float = 0.2,
        at: Optional[datetime] = None,
        require_complete_estimates: bool = True,
    ) -> bool:
        """
        Can the caller act on this recommendation?

        A recommendation is actionable iff:

          * Not expired.
          * ``justification`` is one of the four non-blocked, non-
            inconclusive values.
          * ``safety_verdict`` is ``PASSED`` or ``NOT_REQUIRED``.
          * ``requires_human_review`` is False.
          * ``expected_advantage`` exceeds ``min_advantage`` for quantum
            routes (and is below ``-min_advantage`` for classical routes).
          * ``quality_uncertainty`` (when known) is at most
            ``max_uncertainty``.
          * ``expected_shots`` and ``expected_cost_usd`` are set (when
            ``require_complete_estimates``).
        """
        if self.is_expired(at):
            return False
        if self.justification in (
            RouteJustification.INCONCLUSIVE, RouteJustification.BLOCKED,
        ):
            return False
        if self.safety_verdict not in (
            SafetyVerdict.PASSED, SafetyVerdict.NOT_REQUIRED,
        ):
            return False
        if self.requires_human_review:
            return False
        if (
            self.quality_uncertainty is not None
            and self.quality_uncertainty > max_uncertainty
        ):
            return False
        if require_complete_estimates:
            if self.expected_shots is None \
                    or self.expected_cost_usd is None:
                return False

        if self.justification in (
            RouteJustification.QUANTUM_FAVORED,
            RouteJustification.DISTILLED_FAVORED,
        ):
            if (
                self.expected_advantage is None
                or self.expected_advantage < min_advantage
            ):
                return False
        elif self.justification == RouteJustification.CLASSICAL_FAVORED:
            if (
                self.expected_advantage is None
                or self.expected_advantage > -min_advantage
            ):
                return False
        return True

    # ------------------------------------------------------------------ classification
    def recommendation_class(self) -> str:
        """Single-word category for logging / metrics."""
        if self.justification == RouteJustification.BLOCKED:
            return "blocked"
        if self.safety_verdict == SafetyVerdict.PENDING_REVIEW:
            return "pending_review"
        if self.justification == RouteJustification.INCONCLUSIVE:
            return "inconclusive"
        if self.justification == RouteJustification.CLASSICAL_FAVORED:
            return "classical"
        return "quantum"

    # ------------------------------------------------------------------ bridge
    @classmethod
    def from_execution_result(
        cls,
        result: Any,
        *,
        classical_baseline_quality: Optional[float] = None,
        rationale: Sequence[str] = (),
        safety_verdict: SafetyVerdict = SafetyVerdict.NOT_REQUIRED,
        **kwargs: Any,
    ) -> "QuantumEvidence":
        """
        Build a post-hoc evidence record from a completed execution.

        The justification is derived from the result's outcome class:

          * ``"success"``  → QUANTUM_FAVORED
          * ``"fallback"`` → CLASSICAL_FAVORED
          * ``"failure"``  → INCONCLUSIVE (fallback to classical next time)
          * ``"in_progress"`` → INCONCLUSIVE

        Extra keyword arguments are forwarded to ``cls(...)``.
        """
        outcome = result.outcome_class() if hasattr(
            result, "outcome_class"
        ) else None
        if outcome == "success":
            justification = RouteJustification.QUANTUM_FAVORED
        elif outcome == "fallback":
            justification = RouteJustification.CLASSICAL_FAVORED
        else:
            justification = RouteJustification.INCONCLUSIVE

        return cls(
            task_id=getattr(result, "task_id", ""),
            request_id=getattr(result, "request_id", None),
            justification=justification,
            expected_quality=getattr(result, "quality", None),
            classical_baseline_quality=classical_baseline_quality,
            expected_advantage=(
                (result.quality - classical_baseline_quality)
                if (
                    getattr(result, "quality", None) is not None
                    and classical_baseline_quality is not None
                ) else None
            ),
            expected_shots=getattr(result, "shots_used", None) or None,
            expected_cost_usd=(
                getattr(result, "measured_cost", None)
                or getattr(result, "estimated_cost", None)
            ),
            expected_energy_kwh=(
                getattr(result, "measured_energy_kwh", None)
                or getattr(result, "estimated_energy_kwh", None)
            ),
            expected_co2e_kg=(
                getattr(result, "measured_co2e_kg", None)
                or getattr(result, "estimated_co2e_kg", None)
            ),
            safety_verdict=safety_verdict,
            rationale=tuple(rationale),
            policy_version=getattr(result, "policy_version", None),
            trace_id=getattr(result, "trace_id", None),
            idempotency_key=getattr(result, "idempotency_key", None),
            **kwargs,
        )

    def to_governance_summary(self) -> Dict[str, Any]:
        """A compressed view for governance dashboards."""
        return {
            "recommendation_id": self.recommendation_id,
            "task_id": self.task_id,
            "justification": self.justification.value,
            "safety_verdict": self.safety_verdict.value,
            "expected_advantage": self.expected_advantage,
            "quality_uncertainty": self.quality_uncertainty,
            "requires_human_review": self.requires_human_review,
            "is_actionable": self.is_actionable(),
            "expires_at": (
                self.expires_at.isoformat() if self.expires_at else None
            ),
        }

    # ------------------------------------------------------------------ serialization
    def to_dict(self) -> Dict[str, Any]:
        return {
            "task_id": self.task_id,
            "recommendation_id": self.recommendation_id,
            "request_id": self.request_id,
            "justification": self.justification.value,
            "recommended_backend": self.recommended_backend,
            "recommended_algorithm": self.recommended_algorithm,
            "expected_quality": self.expected_quality,
            "classical_baseline_quality": self.classical_baseline_quality,
            "expected_advantage": self.expected_advantage,
            "advantage_override_reason": self.advantage_override_reason,
            "recommendation_confidence": self.recommendation_confidence,
            "expected_queue_seconds": self.expected_queue_seconds,
            "expected_shots": self.expected_shots,
            "expected_energy_kwh": self.expected_energy_kwh,
            "expected_co2e_kg": self.expected_co2e_kg,
            "expected_cost_usd": self.expected_cost_usd,
            "quality_uncertainty": self.quality_uncertainty,
            "noise_estimate": self.noise_estimate,
            "rationale": list(self.rationale),
            "citations": [c.to_dict() for c in self.citations],
            "rejected_alternatives": [
                a.to_dict() for a in self.rejected_alternatives
            ],
            "provenance": (
                self.provenance.to_dict()
                if self.provenance is not None else None
            ),
            "safety_verdict": self.safety_verdict.value,
            "guardrail_verdicts": [
                g.to_dict() for g in self.guardrail_verdicts
            ],
            "requires_human_review": self.requires_human_review,
            "policy_version": self.policy_version,
            "qcs_version": self.qcs_version,
            "generated_at": self.generated_at.isoformat(),
            "expires_at": (
                self.expires_at.isoformat() if self.expires_at else None
            ),
            "trace_id": self.trace_id,
            "idempotency_key": self.idempotency_key,
        }

    def to_json(self, **kwargs: Any) -> str:
        return json.dumps(self.to_dict(), **kwargs)

    @classmethod
    def from_dict(
        cls,
        d: Mapping[str, Any],
        *,
        validate: bool = True,
    ) -> "QuantumEvidence":
        """
        Reconstruct from ``to_dict()`` output.

        - Rejects unknown fields.
        - Coerces nested structured records (legacy dicts supported).
        - Parses ISO datetimes.
        - ``validate=False`` bypasses ``__post_init__`` validation, for
          reconstructing records from audit history.
        """
        if not isinstance(d, Mapping):
            raise QuantumEvidenceError(
                f"d must be a Mapping; got {type(d).__name__}"
            )
        if "_skip_validation" in d:
            raise QuantumEvidenceError(
                "_skip_validation cannot be set via from_dict; "
                "use the validate= keyword argument"
            )

        known = {f.name for f in fields(cls)} - {"_skip_validation"}
        cleaned: Dict[str, Any] = dict(d)
        unknown = set(cleaned.keys()) - known
        if unknown:
            raise QuantumEvidenceError(
                f"unknown fields in QuantumEvidence dict: "
                f"{sorted(unknown)}"
            )

        for fname in ("generated_at", "expires_at"):
            v = cleaned.get(fname)
            if isinstance(v, str):
                cleaned[fname] = datetime.fromisoformat(v)

        for fname, record_cls in (
            ("citations", Citation),
            ("rejected_alternatives", RejectedAlternative),
            ("guardrail_verdicts", GuardrailVerdict),
        ):
            v = cleaned.get(fname)
            if v is not None and not isinstance(v, tuple):
                cleaned[fname] = tuple(
                    record_cls.from_dict(x)
                    if isinstance(x, Mapping) else x
                    for x in v
                )

        prov = cleaned.get("provenance")
        if isinstance(prov, Mapping):
            cleaned["provenance"] = EvidenceProvenance.from_dict(prov)

        if not validate:
            cleaned["_skip_validation"] = True

        return cls(**cleaned)

    # ------------------------------------------------------------------ misc
    def checksum(self) -> str:
        """
        Stable SHA-256 of the canonical dict form.

        Use as a reference from a governance decision record so audits
        can verify the evidence has not been rewritten after the fact.
        """
        payload = json.dumps(
            self.to_dict(), sort_keys=True, default=str,
        ).encode("utf-8")
        return hashlib.sha256(payload).hexdigest()

    def __repr__(self) -> str:
        return (
            f"QuantumEvidence("
            f"rec={self.recommendation_id!r}, "
            f"task={self.task_id!r}, "
            f"justification={self.justification.value}, "
            f"safety={self.safety_verdict.value}, "
            f"advantage={self.expected_advantage!r})"
        )


# =============================================================================
# Public API
# =============================================================================

__all__: Tuple[str, ...] = (
    "Citation",
    "CitationSourceType",
    "EvidenceProvenance",
    "GuardrailVerdict",
    "MAX_CITATIONS",
    "MAX_DETAIL_LEN",
    "MAX_GUARDRAIL_VERDICTS",
    "MAX_RATIONALE_ENTRIES",
    "MAX_RATIONALE_LINE_LEN",
    "MAX_REJECTED_ALTERNATIVES",
    "QuantumEvidence",
    "QuantumEvidenceError",
    "RejectedAlternative",
    "RouteJustification",
    "SafetyVerdict",
)


# =============================================================================
# Smoke test: python -m qcs.contracts.quantum_evidence
# =============================================================================

if __name__ == "__main__":  # pragma: no cover
    import json
    from datetime import timedelta

    logging.basicConfig(level=logging.INFO)

    print("=== Valid construction ===")
    evidence = QuantumEvidence(
        task_id="task-xyz",
        justification="quantum_favored",  # coerced
        safety_verdict="passed",          # coerced
        recommended_backend="ibmq_quito",
        recommended_algorithm="vqe",
        expected_quality=0.92,
        classical_baseline_quality=0.85,
        expected_advantage=0.07,
        expected_shots=2000,
        expected_cost_usd=0.50,
        expected_energy_kwh=0.10,
        expected_co2e_kg=0.05,
        expected_queue_seconds=120.0,
        quality_uncertainty=0.08,
        rationale=("quantum quality advantage over baseline",),
        citations=(
            Citation(
                source_type="historical_run",
                source_id="run-2024-04-01",
                weight=0.7,
            ),
        ),
        rejected_alternatives=(
            {"reason": "over_budget"},  # legacy dict, coerced
            RejectedAlternative(reason="queue_too_long"),
        ),
        guardrail_verdicts=(
            {"guardrail_id": "budget_check", "passed": True},  # legacy dict
        ),
        provenance=EvidenceProvenance(
            historical_window_hours=168.0,
            sample_count=42,
            models_used=("qcs.forecaster.v2",),
        ),
        policy_version="v5.2.0",
    )
    print(f"  {evidence!r}")
    print(f"  justification: {evidence.justification} "
          f"({type(evidence.justification).__name__})")
    print(f"  safety_verdict: {evidence.safety_verdict} "
          f"({type(evidence.safety_verdict).__name__})")
    print(f"  recommendation_id: {evidence.recommendation_id}")
    print(f"  is_actionable:    {evidence.is_actionable()}")
    print(f"  recommendation_class: {evidence.recommendation_class()}")
    print(f"  checksum: {evidence.checksum()[:16]}…")

    print("\n=== Immutability ===")
    for attr, val in [
        ("justification", RouteJustification.CLASSICAL_FAVORED),
        ("safety_verdict", SafetyVerdict.FAILED),
        ("rationale", ("x",)),
    ]:
        try:
            setattr(evidence, attr, val)
        except Exception as e:
            print(f"  {attr} mutation → {type(e).__name__}")
    try:
        evidence.rationale.append("x")  # type: ignore[attr-defined]
    except AttributeError:
        print(f"  rationale.append → AttributeError")

    print("\n=== Construction-time validation ===")
    bad_cases = [
        ("empty task_id",
         dict(task_id="", justification="quantum_favored")),
        ("bad justification",
         dict(task_id="t", justification="quantum_favord")),
        ("bad safety_verdict",
         dict(task_id="t", justification="quantum_favored",
              safety_verdict="pas")),
        ("quality > 1",
         dict(task_id="t", justification="quantum_favored",
              expected_quality=1.5)),
        ("negative cost",
         dict(task_id="t", justification="quantum_favored",
              expected_cost_usd=-1.0)),
        ("negative shots",
         dict(task_id="t", justification="quantum_favored",
              expected_shots=-10)),
        ("bad policy_version",
         dict(task_id="t", justification="quantum_favored",
              policy_version="v5")),
    ]
    for label, kwargs in bad_cases:
        try:
            QuantumEvidence(**kwargs)
            print(f"  {label}: NOT rejected")
        except QuantumEvidenceError as e:
            print(f"  {label}: {e}")

    print("\n=== Cross-field validation ===")
    for label, kwargs in [
        ("quantum_favored + neg advantage",
         dict(task_id="t", justification="quantum_favored",
              expected_advantage=-0.1)),
        ("classical_favored + pos advantage",
         dict(task_id="t", justification="classical_favored",
              expected_advantage=0.1)),
        ("blocked + passed",
         dict(task_id="t", justification="blocked",
              safety_verdict="passed")),
        ("pending_review without requires_human_review",
         dict(task_id="t", justification="quantum_favored",
              safety_verdict="pending_review",
              requires_human_review=False)),
        ("advantage incoherent",
         dict(task_id="t", justification="quantum_favored",
              expected_quality=0.90, classical_baseline_quality=0.80,
              expected_advantage=0.50)),
    ]:
        try:
            QuantumEvidence(**kwargs)
            print(f"  {label}: NOT rejected")
        except QuantumEvidenceError as e:
            print(f"  {label}: rejected")

    print("\n=== Override escape hatch ===")
    override = QuantumEvidence(
        task_id="t", justification="quantum_favored",
        expected_quality=0.80, classical_baseline_quality=0.85,
        expected_advantage=0.10,  # inconsistent with 0.80 - 0.85
        advantage_override_reason="quantum enables downstream optimizations",
    )
    print(f"  override accepted: advantage={override.expected_advantage}")

    print("\n=== BLOCKED justification ===")
    blocked = QuantumEvidence(
        task_id="t", justification="blocked",
        safety_verdict="blocked",
    )
    print(f"  is_blocked={blocked.is_blocked}, "
          f"is_actionable={blocked.is_actionable()}")

    print("\n=== Semantic default: expected_* is None, not 0.0 ===")
    bare = QuantumEvidence(task_id="t", justification="inconclusive")
    print(f"  expected_shots:       {bare.expected_shots}")
    print(f"  expected_cost_usd:    {bare.expected_cost_usd}")
    print(f"  expected_energy_kwh:  {bare.expected_energy_kwh}")
    print(f"  quality_uncertainty:  {bare.quality_uncertainty}")

    print("\n=== is_actionable with thresholds ===")
    ev = QuantumEvidence(
        task_id="t", justification="quantum_favored",
        safety_verdict="passed",
        expected_quality=0.92, classical_baseline_quality=0.85,
        expected_advantage=0.07,
        expected_shots=1000, expected_cost_usd=0.10,
        quality_uncertainty=0.10,
    )
    print(f"  default thresholds:      {ev.is_actionable()}")
    print(f"  min_advantage=0.10:      "
          f"{ev.is_actionable(min_advantage=0.10)}")
    print(f"  max_uncertainty=0.05:    "
          f"{ev.is_actionable(max_uncertainty=0.05)}")
    print(f"  require_complete=False:  "
          f"{ev.is_actionable(require_complete_estimates=False)}")

    print("\n=== Expiry ===")
    t0 = _utcnow()
    expiring = QuantumEvidence(
        task_id="t", justification="quantum_favored",
        safety_verdict="passed",
        expected_quality=0.92, classical_baseline_quality=0.85,
        expected_advantage=0.07,
        expected_shots=1000, expected_cost_usd=0.10,
        generated_at=t0,
        expires_at=t0 + timedelta(minutes=5),
    )
    print(f"  at t0:            expired={expiring.is_expired(at=t0)}")
    print(f"  at t0+10m:        "
          f"expired={expiring.is_expired(at=t0 + timedelta(minutes=10))}")
    print(f"  actionable@t0+10m: "
          f"{expiring.is_actionable(at=t0 + timedelta(minutes=10))}")

    print("\n=== Serialization round-trip ===")
    d = evidence.to_dict()
    print(f"  JSON length: {len(json.dumps(d))} bytes")
    restored = QuantumEvidence.from_dict(d)
    print(f"  round-trip equality: {restored == evidence}")
    try:
        QuantumEvidence.from_dict({**d, "extra_field": 1})
    except QuantumEvidenceError as e:
        print(f"  strict from_dict: {e}")

    print("\n=== Audit reconstruction (validate=False) ===")
    old = QuantumEvidence.from_dict(
        {
            "task_id": "old", "justification": "quantum_favored",
            "safety_verdict": "passed",
            "expected_advantage": -0.5,  # would fail today's validation
            "advantage_override_reason": "legacy record",
        },
        validate=False,
    )
    print(f"  reconstructed: justification={old.justification.value}, "
          f"advantage={old.expected_advantage}")

    print("\n=== Governance summary ===")
    summary = evidence.to_governance_summary()
    print(f"  keys: {sorted(summary.keys())}")
    print(f"  is_actionable: {summary['is_actionable']}")

    print("\n=== Version parsing ===")
    print(f"  qcs_version: {evidence.qcs_version}")
    print(f"  policy_version: {evidence.policy_version}, "
          f"tuple: {evidence.policy_version_tuple}")

    print(f"\n=== __version__ === {__version__}")
    print(f"=== __all__ === {__all__}")
    print("\nSmoke test passed.")
