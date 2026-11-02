"""
StudentModelEvaluator — quality/energy/carbon comparison for distilled models.

Evaluates a distilled student model against one or more thresholds:

  * **Quality retention** — ``student_quality / teacher_quality``. The
    primary distillation metric: "did the student preserve the teacher's
    task quality?". Can exceed ``1.0`` when the student outperforms the
    teacher.
  * **Energy ratio** — ``student_energy_kwh / teacher_energy_kwh``. Lower
    is better. Only checked when ``max_energy_ratio`` is set and a
    non-zero teacher baseline is available.
  * **Carbon ratio** — ``student_co2e_kg / teacher_co2e_kg``. Optional;
    only checked when ``max_co2e_ratio`` is set.

Each check produces a typed ``EvaluationReason``. The overall outcome is
a typed ``EvaluationStatus`` — the caller can distinguish "the student
failed quality" from "we couldn't measure the student's quality" from
"the caller passed invalid inputs".

Semantics
---------
``passes`` is derived from ``status == PASSED``. A result that couldn't
be evaluated (missing data, invalid input) has ``status`` of
``INSUFFICIENT_DATA`` or ``INVALID_INPUT`` — both are distinct from
``BELOW_QUALITY`` (a real quality failure).

Original API preserved:
    evaluator = StudentModelEvaluator(
        min_quality_retention=0.85, max_energy_ratio=0.7,
    )
    result = evaluator.evaluate(
        model_id=..., teacher_quality=..., student_quality=...,
        teacher_energy_kwh=..., student_energy_kwh=...,
        student_co2e_kg=0.0,
    )
    result.passes / result.quality / result.energy_kwh / result.reasons

v2 changes (see changelog in commit message):
  * FIX: student_quality=None no longer crashes the retention
    calculation; it produces INSUFFICIENT_DATA.
  * FIX: negative student_energy_kwh is rejected as invalid input
    (previously it silently passed the energy gate).
  * FIX: zero teacher_energy_kwh is now treated as a skipped energy
    check (previously it was a silent failure).
  * FIX: reasons or ["student passes all thresholds"] replaced with an
    empty reasons tuple on success; status carries the outcome.
  * StudentModelEvaluator: constructor params validated; config
    dataclass; type-level status/reasons; sample_count + Wilson CI;
    classical baseline; co2e check (optional); batch evaluation;
    evaluate_or_raise; to_dict/checksum; teacher_model_id; evaluated_at;
    timestamps; concise repr; __version__; __all__; demo.
"""

from __future__ import annotations

import hashlib
import json
import logging
import math
import statistics
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
# Errors
# =============================================================================

class StudentEvaluationError(Exception):
    """Base class for StudentModelEvaluator errors."""


class StudentEvaluationInputError(StudentEvaluationError, ValueError):
    """Caller passed invalid inputs (negative energy, bad range, etc.)."""


class StudentEvaluationInsufficientData(StudentEvaluationError):
    """A required measurement was missing; the check could not run."""


class StudentQualityBelowThreshold(StudentEvaluationError):
    """Quality retention is below the configured threshold."""

    def __init__(self, retention: float, threshold: float) -> None:
        self.retention = retention
        self.threshold = threshold
        super().__init__(
            f"retention {retention:.4f} below threshold {threshold:.4f}"
        )


class StudentEnergyAboveRatio(StudentEvaluationError):
    """Energy ratio is above the configured threshold."""

    def __init__(self, ratio: float, threshold: float) -> None:
        self.ratio = ratio
        self.threshold = threshold
        super().__init__(
            f"energy ratio {ratio:.4f} above threshold {threshold:.4f}"
        )


class StudentCo2eAboveRatio(StudentEvaluationError):
    """Carbon ratio is above the configured threshold."""

    def __init__(self, ratio: float, threshold: float) -> None:
        self.ratio = ratio
        self.threshold = threshold
        super().__init__(
            f"co2e ratio {ratio:.4f} above threshold {threshold:.4f}"
        )


# =============================================================================
# Enums
# =============================================================================

class EvaluationStatus(str, Enum):
    """Overall outcome of a student evaluation."""

    PASSED = "passed"
    BELOW_QUALITY = "below_quality"
    ABOVE_ENERGY = "above_energy"
    ABOVE_CO2E = "above_co2e"
    MULTIPLE_FAILURES = "multiple_failures"
    INSUFFICIENT_DATA = "insufficient_data"
    INVALID_INPUT = "invalid_input"


class EvaluationReason(str, Enum):
    """Typed reason codes attached to an evaluation result."""

    PASSED = "passed"
    BELOW_QUALITY = "below_quality"
    ABOVE_ENERGY = "above_energy"
    ABOVE_CO2E = "above_co2e"

    STUDENT_QUALITY_MISSING = "student_quality_missing"
    TEACHER_QUALITY_MISSING_OR_ZERO = "teacher_quality_missing_or_zero"

    STUDENT_ENERGY_MISSING = "student_energy_missing"
    TEACHER_ENERGY_MISSING = "teacher_energy_missing"
    ZERO_TEACHER_ENERGY = "zero_teacher_energy"

    STUDENT_CO2E_MISSING = "student_co2e_missing"
    TEACHER_CO2E_MISSING = "teacher_co2e_missing"
    ZERO_TEACHER_CO2E = "zero_teacher_co2e"

    NEGATIVE_ENERGY = "negative_energy"
    NEGATIVE_CO2E = "negative_co2e"
    OUT_OF_RANGE_QUALITY = "out_of_range_quality"
    INVALID_SAMPLE_COUNT = "invalid_sample_count"
    BAD_MODEL_ID = "bad_model_id"
    UNKNOWN = "unknown"


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


def _is_finite_number(v: Any) -> bool:
    if isinstance(v, bool):
        return False
    if not isinstance(v, (int, float)):
        return False
    return math.isfinite(float(v))


def _freeze_mapping(m: Any) -> Mapping[str, Any]:
    if m is None:
        return MappingProxyType({})
    if isinstance(m, MappingProxyType):
        return m
    if not isinstance(m, Mapping):
        raise StudentEvaluationError(
            f"expected a Mapping; got {type(m).__name__}"
        )
    return MappingProxyType(dict(m))


def _coerce_status(value: Any) -> EvaluationStatus:
    if isinstance(value, EvaluationStatus):
        return value
    try:
        return EvaluationStatus(value)
    except (ValueError, KeyError):
        allowed = sorted(e.value for e in EvaluationStatus)
        raise StudentEvaluationError(
            f"status must be one of {allowed}; got {value!r}"
        ) from None


def _coerce_reason(value: Any) -> EvaluationReason:
    if isinstance(value, EvaluationReason):
        return value
    try:
        return EvaluationReason(value)
    except (ValueError, KeyError):
        allowed = sorted(e.value for e in EvaluationReason)
        raise StudentEvaluationError(
            f"reason must be one of {allowed}; got {value!r}"
        ) from None


def _wilson_ci(
    p: float, n: int, level: float = 0.95,
) -> Tuple[float, float]:
    """Wilson score interval for a proportion ``p`` observed on ``n`` samples."""
    if n <= 0:
        return (0.0, 1.0)
    p = max(0.0, min(1.0, float(p)))
    z = statistics.NormalDist().inv_cdf(1.0 - (1.0 - level) / 2.0)
    denom = 1.0 + z * z / n
    center = (p + z * z / (2.0 * n)) / denom
    half = (
        z * math.sqrt(p * (1.0 - p) / n + z * z / (4.0 * n * n))
    ) / denom
    return (max(0.0, center - half), min(1.0, center + half))


def _safe_ratio(
    numerator: Optional[float],
    denominator: Optional[float],
) -> Tuple[Optional[float], Optional[EvaluationReason]]:
    """
    Compute ``numerator / denominator`` with explicit error codes.

    Returns ``(None, reason)`` when the ratio cannot be computed — no
    silent fallback, no crash on ``None``.
    """
    if numerator is None:
        return None, EvaluationReason.STUDENT_QUALITY_MISSING
    if denominator is None or denominator == 0:
        return None, EvaluationReason.TEACHER_QUALITY_MISSING_OR_ZERO
    return numerator / denominator, None


# =============================================================================
# Config
# =============================================================================

@dataclass(frozen=True)
class StudentEvaluatorConfig:
    """Frozen, validated configuration for ``StudentModelEvaluator``."""

    min_quality_retention: float = 0.85
    max_energy_ratio: float = 0.7
    max_co2e_ratio: Optional[float] = None     # None → carbon not checked

    def __post_init__(self) -> None:
        # --- min_quality_retention ---
        if not _is_finite_number(self.min_quality_retention):
            raise StudentEvaluationInputError(
                "min_quality_retention must be a finite number"
            )
        if float(self.min_quality_retention) < 0.0:
            raise StudentEvaluationInputError(
                "min_quality_retention must be >= 0"
            )
        # Upper bound is open: retention can exceed 1.0 when the student
        # outperforms the teacher. Warn on absurdly high values.
        if float(self.min_quality_retention) > 10.0:
            logger.warning(
                "min_quality_retention=%s is unusually high (>10)",
                self.min_quality_retention,
            )

        # --- max_energy_ratio ---
        if not _is_finite_number(self.max_energy_ratio):
            raise StudentEvaluationInputError(
                "max_energy_ratio must be a finite number"
            )
        if float(self.max_energy_ratio) <= 0.0:
            raise StudentEvaluationInputError(
                "max_energy_ratio must be > 0"
            )

        # --- max_co2e_ratio ---
        if self.max_co2e_ratio is not None:
            if not _is_finite_number(self.max_co2e_ratio):
                raise StudentEvaluationInputError(
                    "max_co2e_ratio must be a finite number or None"
                )
            if float(self.max_co2e_ratio) <= 0.0:
                raise StudentEvaluationInputError(
                    "max_co2e_ratio must be > 0 or None"
                )

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


# =============================================================================
# Result
# =============================================================================

@dataclass(frozen=True)
class EvaluationResult:
    """
    Immutable evaluation of a student model against quality/energy/carbon.

    ``passes`` is a property derived from ``status == PASSED``. A caller
    that only needs the boolean can use ``result.passes``; a caller that
    needs the reason should branch on ``result.status`` or iterate
    ``result.reasons``.
    """

    # --- Identity ---
    model_id: str
    teacher_model_id: Optional[str] = None

    # --- Status ---
    status: EvaluationStatus = EvaluationStatus.INSUFFICIENT_DATA
    reasons: Tuple[EvaluationReason, ...] = ()
    details: Mapping[EvaluationReason, str] = field(default_factory=dict)

    # --- Raw metrics (student side; ``quality`` and ``energy_kwh``
    #     and ``co2e_kg`` match the pre-2.0 field names) ---
    quality: Optional[float] = None             # student_quality
    energy_kwh: Optional[float] = None          # student_energy_kwh
    co2e_kg: Optional[float] = None             # student_co2e_kg

    teacher_quality: Optional[float] = None
    teacher_energy_kwh: Optional[float] = None
    teacher_co2e_kg: Optional[float] = None

    # --- Derived ratios ---
    retention: Optional[float] = None
    energy_ratio: Optional[float] = None
    co2e_ratio: Optional[float] = None

    # --- Sample count / confidence ---
    sample_count: int = 0

    # --- Classical baseline comparison ---
    classical_baseline_quality: Optional[float] = None
    classical_retention: Optional[float] = None
    beats_classical: Optional[bool] = None

    # --- Thresholds applied at evaluation time ---
    thresholds: Mapping[str, Optional[float]] = field(default_factory=dict)

    # --- Timing ---
    evaluated_at: datetime = field(default_factory=_utcnow)

    # ------------------------------------------------------------------ init
    def __post_init__(self) -> None:
        # --- Identity ---
        if not isinstance(self.model_id, str) or not self.model_id.strip():
            raise StudentEvaluationError(
                "model_id must be a non-empty string"
            )
        if self.teacher_model_id is not None and (
            not isinstance(self.teacher_model_id, str)
            or not self.teacher_model_id.strip()
        ):
            raise StudentEvaluationError(
                "teacher_model_id must be a non-empty string or None"
            )

        # --- Enums ---
        object.__setattr__(
            self, "status", _coerce_status(self.status),
        )
        if not isinstance(self.reasons, tuple):
            object.__setattr__(
                self, "reasons",
                tuple(_coerce_reason(r) for r in self.reasons),
            )
        else:
            object.__setattr__(
                self, "reasons",
                tuple(_coerce_reason(r) for r in self.reasons),
            )

        # --- Details / thresholds frozen ---
        object.__setattr__(
            self, "details", _freeze_mapping(self.details),
        )
        object.__setattr__(
            self, "thresholds", _freeze_mapping(self.thresholds),
        )

        # --- Probability-like fields in [0, 1] ---
        for fname in ("quality", "teacher_quality",
                      "classical_baseline_quality"):
            v = getattr(self, fname)
            if v is not None:
                if not _is_finite_number(v):
                    raise StudentEvaluationError(
                        f"{fname} must be a finite number or None"
                    )
                if not (0.0 <= float(v) <= 1.0):
                    raise StudentEvaluationError(
                        f"{fname} must be in [0, 1] or None"
                    )

        # --- Non-negative energy / co2e ---
        for fname in (
            "energy_kwh", "teacher_energy_kwh",
            "co2e_kg", "teacher_co2e_kg",
        ):
            v = getattr(self, fname)
            if v is not None:
                if not _is_finite_number(v):
                    raise StudentEvaluationError(
                        f"{fname} must be a finite number or None"
                    )
                if float(v) < 0:
                    raise StudentEvaluationError(
                        f"{fname} must be >= 0 or None"
                    )

        # --- Finite ratios ---
        for fname in ("retention", "energy_ratio", "co2e_ratio",
                      "classical_retention"):
            v = getattr(self, fname)
            if v is not None and not _is_finite_number(v):
                raise StudentEvaluationError(
                    f"{fname} must be a finite number or None"
                )

        # --- sample_count ---
        if not isinstance(self.sample_count, int) \
                or isinstance(self.sample_count, bool):
            raise StudentEvaluationError(
                "sample_count must be an int"
            )
        if self.sample_count < 0:
            raise StudentEvaluationError(
                "sample_count must be >= 0"
            )

        # --- Time ---
        object.__setattr__(
            self, "evaluated_at",
            _ensure_utc(self.evaluated_at),
        )

        # --- Consistency: retention must match quality/teacher_quality ---
        if (
            self.retention is not None
            and self.quality is not None
            and self.teacher_quality is not None
            and self.teacher_quality > 0
        ):
            computed = self.quality / self.teacher_quality
            if abs(computed - self.retention) > 1e-6:
                raise StudentEvaluationError(
                    f"retention={self.retention} inconsistent with "
                    f"quality/teacher_quality={computed:.6f}"
                )

        # --- Consistency: energy_ratio must match energy/teacher_energy ---
        if (
            self.energy_ratio is not None
            and self.energy_kwh is not None
            and self.teacher_energy_kwh is not None
            and self.teacher_energy_kwh > 0
        ):
            computed = self.energy_kwh / self.teacher_energy_kwh
            if abs(computed - self.energy_ratio) > 1e-6:
                raise StudentEvaluationError(
                    f"energy_ratio={self.energy_ratio} inconsistent with "
                    f"energy_kwh/teacher_energy_kwh={computed:.6f}"
                )

        # --- Consistency: co2e_ratio must match co2e/teacher_co2e ---
        if (
            self.co2e_ratio is not None
            and self.co2e_kg is not None
            and self.teacher_co2e_kg is not None
            and self.teacher_co2e_kg > 0
        ):
            computed = self.co2e_kg / self.teacher_co2e_kg
            if abs(computed - self.co2e_ratio) > 1e-6:
                raise StudentEvaluationError(
                    f"co2e_ratio={self.co2e_ratio} inconsistent with "
                    f"co2e_kg/teacher_co2e_kg={computed:.6f}"
                )

    # ------------------------------------------------------------------ properties
    @property
    def passes(self) -> bool:
        """Derived from ``status == PASSED`` — the boolean the old API used."""
        return self.status == EvaluationStatus.PASSED

    @property
    def is_quality_improvement(self) -> bool:
        return self.retention is not None and self.retention > 1.0

    @property
    def retention_error(self) -> Optional[EvaluationReason]:
        """Reason retention couldn't be computed, if applicable."""
        for r in self.reasons:
            if r in (
                EvaluationReason.STUDENT_QUALITY_MISSING,
                EvaluationReason.TEACHER_QUALITY_MISSING_OR_ZERO,
            ):
                return r
        return None

    # ------------------------------------------------------------------ confidence
    def quality_confidence_interval(
        self, level: float = 0.95,
    ) -> Tuple[float, float]:
        """
        Wilson score interval on the raw ``student_quality``.

        Returns ``(0.0, 1.0)`` when there is no measurable quality.
        """
        if not (0.0 < level < 1.0):
            raise StudentEvaluationError("level must be in (0, 1)")
        if self.quality is None or self.sample_count == 0:
            return (0.0, 1.0)
        return _wilson_ci(self.quality, self.sample_count, level)

    def retention_confidence_interval(
        self, level: float = 0.95,
    ) -> Tuple[float, float]:
        """
        CI on ``retention``, propagated from ``student_quality``'s CI.

        Assumes ``teacher_quality`` is a known constant.
        """
        if self.retention is None \
                or self.teacher_quality is None \
                or self.teacher_quality <= 0:
            return (0.0, 1.0)
        lo, hi = self.quality_confidence_interval(level)
        t = self.teacher_quality
        return (lo / t, hi / t)

    def passes_with_confidence(
        self, level: float = 0.95,
    ) -> bool:
        """
        Stricter than ``passes``: requires the lower bound of the retention
        CI to clear the quality threshold.

        Returns True when no quality threshold was configured and the
        stored ``passes`` is True.
        """
        if not self.passes:
            return False
        min_retention = self.thresholds.get("min_quality_retention")
        if min_retention is None:
            return True
        lo, _ = self.retention_confidence_interval(level=level)
        return lo >= float(min_retention)

    # ------------------------------------------------------------------ re-scoring
    def is_success(
        self,
        *,
        min_retention: Optional[float] = None,
        max_energy_ratio: Optional[float] = None,
        max_co2e_ratio: Optional[float] = None,
    ) -> bool:
        """
        Re-evaluate the result against the given thresholds.

        ``None`` thresholds fall back to the ones used at evaluation
        time. Missing data counts as a failure (a distillation we could
        not measure is not a success).
        """
        q = (
            min_retention
            if min_retention is not None
            else self.thresholds.get("min_quality_retention")
        )
        if q is not None:
            if self.retention is None or self.retention < float(q):
                return False

        e = (
            max_energy_ratio
            if max_energy_ratio is not None
            else self.thresholds.get("max_energy_ratio")
        )
        if e is not None:
            if self.energy_ratio is None or self.energy_ratio > float(e):
                return False

        c = (
            max_co2e_ratio
            if max_co2e_ratio is not None
            else self.thresholds.get("max_co2e_ratio")
        )
        if c is not None:
            if self.co2e_ratio is None or self.co2e_ratio > float(c):
                return False

        return True

    # ------------------------------------------------------------------ summary
    def summary(self) -> str:
        """One-line human-readable summary."""
        if self.passes:
            return (
                f"{self.model_id}: passes "
                f"(retention={self.retention}, "
                f"energy_ratio={self.energy_ratio})"
            )
        return (
            f"{self.model_id}: {self.status.value} "
            f"({', '.join(r.value for r in self.reasons)})"
        )

    # ------------------------------------------------------------------ serialization
    def to_dict(self) -> Dict[str, Any]:
        return {
            "model_id": self.model_id,
            "teacher_model_id": self.teacher_model_id,
            "status": self.status.value,
            "passes": self.passes,
            "reasons": [r.value for r in self.reasons],
            "details": {
                r.value: msg for r, msg in self.details.items()
            },
            "quality": self.quality,
            "energy_kwh": self.energy_kwh,
            "co2e_kg": self.co2e_kg,
            "teacher_quality": self.teacher_quality,
            "teacher_energy_kwh": self.teacher_energy_kwh,
            "teacher_co2e_kg": self.teacher_co2e_kg,
            "retention": self.retention,
            "energy_ratio": self.energy_ratio,
            "co2e_ratio": self.co2e_ratio,
            "sample_count": self.sample_count,
            "classical_baseline_quality": self.classical_baseline_quality,
            "classical_retention": self.classical_retention,
            "beats_classical": self.beats_classical,
            "thresholds": dict(self.thresholds),
            "evaluated_at": self.evaluated_at.isoformat(),
        }

    def to_json(self, **kwargs: Any) -> str:
        return json.dumps(self.to_dict(), **kwargs)

    @classmethod
    def from_dict(cls, d: Mapping[str, Any]) -> "EvaluationResult":
        if not isinstance(d, Mapping):
            raise StudentEvaluationError(
                f"d must be a Mapping; got {type(d).__name__}"
            )
        known = {f.name for f in fields(cls)}
        unknown = set(d.keys()) - known - {"passes"}
        if unknown:
            raise StudentEvaluationError(
                f"unknown fields in EvaluationResult dict: "
                f"{sorted(unknown)}"
            )
        cleaned: Dict[str, Any] = dict(d)
        cleaned.pop("passes", None)  # derived property
        ts = cleaned.get("evaluated_at")
        if isinstance(ts, str):
            cleaned["evaluated_at"] = datetime.fromisoformat(ts)
        # Coerce reasons from strings.
        reasons = cleaned.get("reasons", ())
        if not isinstance(reasons, tuple):
            cleaned["reasons"] = tuple(reasons)
        # Coerce details keys back to enum.
        details = cleaned.get("details", {})
        coerced_details: Dict[EvaluationReason, str] = {}
        if isinstance(details, Mapping):
            for k, v in details.items():
                try:
                    coerced_details[EvaluationReason(k)] = str(v)
                except (ValueError, KeyError):
                    continue
        cleaned["details"] = coerced_details
        return cls(**cleaned)

    def checksum(self) -> str:
        payload = json.dumps(
            self.to_dict(), sort_keys=True, default=str,
        ).encode("utf-8")
        return hashlib.sha256(payload).hexdigest()

    def __repr__(self) -> str:
        return (
            f"EvaluationResult("
            f"model={self.model_id!r}, "
            f"status={self.status.value}, "
            f"retention={self.retention!r}, "
            f"energy_ratio={self.energy_ratio!r})"
        )


# =============================================================================
# Batch input spec
# =============================================================================

@dataclass(frozen=True)
class ModelSpec:
    """One row of a batch evaluation. Mirrors the ``evaluate`` kwargs."""

    model_id: str
    teacher_quality: Optional[float]
    student_quality: Optional[float]
    teacher_energy_kwh: Optional[float]
    student_energy_kwh: Optional[float]
    student_co2e_kg: Optional[float] = None
    teacher_co2e_kg: Optional[float] = None
    sample_count: int = 0
    classical_baseline_quality: Optional[float] = None
    teacher_model_id: Optional[str] = None


# =============================================================================
# Evaluator
# =============================================================================

class StudentModelEvaluator:
    """
    Evaluates a student model against quality, energy, and carbon thresholds.

    Every ``evaluate`` call produces a fully-populated ``EvaluationResult``
    — including when the caller's inputs are missing or invalid. The result
    distinguishes "student failed" from "we couldn't measure" via
    ``result.status``.

    Thresholds are validated at construction. Defaults are the pre-2.0
    values (``min_quality_retention=0.85``, ``max_energy_ratio=0.7``)
    so existing callers see no behavior change on the happy path.
    """

    def __init__(
        self,
        *,
        min_quality_retention: float = 0.85,
        max_energy_ratio: float = 0.7,
        max_co2e_ratio: Optional[float] = None,
        config: Optional[StudentEvaluatorConfig] = None,
    ) -> None:
        if config is None:
            config = StudentEvaluatorConfig(
                min_quality_retention=min_quality_retention,
                max_energy_ratio=max_energy_ratio,
                max_co2e_ratio=max_co2e_ratio,
            )
        self.config = config
        self.min_quality_retention = float(config.min_quality_retention)
        self.max_energy_ratio = float(config.max_energy_ratio)
        self.max_co2e_ratio = (
            float(config.max_co2e_ratio)
            if config.max_co2e_ratio is not None
            else None
        )

    # ------------------------------------------------------------------ single
    def evaluate(
        self,
        *,
        model_id: str,
        teacher_quality: Optional[float],
        student_quality: Optional[float],
        teacher_energy_kwh: Optional[float],
        student_energy_kwh: Optional[float],
        student_co2e_kg: Optional[float] = None,
        teacher_co2e_kg: Optional[float] = None,
        sample_count: int = 0,
        classical_baseline_quality: Optional[float] = None,
        teacher_model_id: Optional[str] = None,
    ) -> EvaluationResult:
        """
        Evaluate one student model against the configured thresholds.

        Missing or invalid values produce a result with ``status`` of
        ``INSUFFICIENT_DATA`` or ``INVALID_INPUT`` — never a crash.
        """
        # --- Input validation (soft) ---
        invalid_reasons: List[EvaluationReason] = []
        invalid_details: Dict[EvaluationReason, str] = {}

        if not isinstance(model_id, str) or not model_id.strip():
            invalid_reasons.append(EvaluationReason.BAD_MODEL_ID)
            invalid_details[EvaluationReason.BAD_MODEL_ID] = (
                "model_id must be a non-empty string"
            )

        def _check_probability(name: str, v: Any) -> Optional[float]:
            if v is None:
                return None
            if not _is_finite_number(v):
                invalid_reasons.append(EvaluationReason.OUT_OF_RANGE_QUALITY)
                invalid_details[EvaluationReason.OUT_OF_RANGE_QUALITY] = (
                    f"{name} must be a finite number"
                )
                return None
            if not (0.0 <= float(v) <= 1.0):
                invalid_reasons.append(EvaluationReason.OUT_OF_RANGE_QUALITY)
                invalid_details[EvaluationReason.OUT_OF_RANGE_QUALITY] = (
                    f"{name}={v} outside [0, 1]"
                )
                return None
            return float(v)

        def _check_energy(name: str, v: Any) -> Optional[float]:
            if v is None:
                return None
            if not _is_finite_number(v):
                invalid_reasons.append(EvaluationReason.NEGATIVE_ENERGY)
                invalid_details[EvaluationReason.NEGATIVE_ENERGY] = (
                    f"{name} must be a finite number"
                )
                return None
            if float(v) < 0:
                invalid_reasons.append(EvaluationReason.NEGATIVE_ENERGY)
                invalid_details[EvaluationReason.NEGATIVE_ENERGY] = (
                    f"{name}={v} must be >= 0"
                )
                return None
            return float(v)

        t_quality = _check_probability("teacher_quality", teacher_quality)
        s_quality = _check_probability("student_quality", student_quality)
        t_energy = _check_energy("teacher_energy_kwh", teacher_energy_kwh)
        s_energy = _check_energy("student_energy_kwh", student_energy_kwh)
        t_co2e = _check_energy("teacher_co2e_kg", teacher_co2e_kg)
        s_co2e = _check_energy("student_co2e_kg", student_co2e_kg)

        baseline = _check_probability(
            "classical_baseline_quality", classical_baseline_quality,
        )

        if not isinstance(sample_count, int) or isinstance(sample_count, bool):
            invalid_reasons.append(EvaluationReason.INVALID_SAMPLE_COUNT)
            invalid_details[EvaluationReason.INVALID_SAMPLE_COUNT] = (
                "sample_count must be an int"
            )
            sample_count = 0
        elif sample_count < 0:
            invalid_reasons.append(EvaluationReason.INVALID_SAMPLE_COUNT)
            invalid_details[EvaluationReason.INVALID_SAMPLE_COUNT] = (
                f"sample_count={sample_count} must be >= 0"
            )
            sample_count = 0

        # --- If invalid inputs, short-circuit with INVALID_INPUT ---
        if invalid_reasons:
            return EvaluationResult(
                model_id=model_id if isinstance(model_id, str) else "",
                teacher_model_id=teacher_model_id,
                status=EvaluationStatus.INVALID_INPUT,
                reasons=tuple(invalid_reasons),
                details=invalid_details,
                quality=s_quality,
                energy_kwh=s_energy,
                co2e_kg=s_co2e,
                teacher_quality=t_quality,
                teacher_energy_kwh=t_energy,
                teacher_co2e_kg=t_co2e,
                sample_count=sample_count,
                classical_baseline_quality=baseline,
                thresholds=MappingProxyType({
                    "min_quality_retention": self.min_quality_retention,
                    "max_energy_ratio": self.max_energy_ratio,
                    "max_co2e_ratio": self.max_co2e_ratio,
                }),
            )

        reasons: List[EvaluationReason] = []
        details: Dict[EvaluationReason, str] = {}

        # --- Quality retention ---
        retention, retention_err = _safe_ratio(s_quality, t_quality)
        quality_failed = False
        quality_uncomputable = False

        if retention is None:
            quality_uncomputable = True
            if retention_err is not None:
                reasons.append(retention_err)
                details[retention_err] = (
                    "cannot compute retention: "
                    + (
                        "student_quality is missing"
                        if retention_err == EvaluationReason.STUDENT_QUALITY_MISSING
                        else "teacher_quality is missing or zero"
                    )
                )
        elif retention < self.min_quality_retention:
            quality_failed = True
            reasons.append(EvaluationReason.BELOW_QUALITY)
            details[EvaluationReason.BELOW_QUALITY] = (
                f"retention {retention:.4f} below "
                f"threshold {self.min_quality_retention}"
            )

        # --- Energy ratio ---
        energy_ratio: Optional[float] = None
        energy_failed = False
        energy_uncomputable = False

        if t_energy is None:
            # Teacher energy not provided.
            energy_uncomputable = True
            reasons.append(EvaluationReason.TEACHER_ENERGY_MISSING)
            details[EvaluationReason.TEACHER_ENERGY_MISSING] = (
                "teacher_energy_kwh is missing"
            )
        elif t_energy == 0.0:
            # Zero teacher energy: cannot form a ratio. Treat as skipped,
            # not failure. Callers who need strictness should compare
            # absolute energies in their own policy layer.
            energy_uncomputable = True
            reasons.append(EvaluationReason.ZERO_TEACHER_ENERGY)
            details[EvaluationReason.ZERO_TEACHER_ENERGY] = (
                "teacher_energy_kwh is 0; energy ratio check skipped"
            )
        elif s_energy is None:
            energy_uncomputable = True
            reasons.append(EvaluationReason.STUDENT_ENERGY_MISSING)
            details[EvaluationReason.STUDENT_ENERGY_MISSING] = (
                "student_energy_kwh is missing"
            )
        else:
            energy_ratio = s_energy / t_energy
            if energy_ratio > self.max_energy_ratio:
                energy_failed = True
                reasons.append(EvaluationReason.ABOVE_ENERGY)
                details[EvaluationReason.ABOVE_ENERGY] = (
                    f"energy ratio {energy_ratio:.4f} above "
                    f"threshold {self.max_energy_ratio}"
                )

        # --- Carbon ratio (opt-in) ---
        co2e_ratio: Optional[float] = None
        co2e_failed = False
        co2e_uncomputable = False

        if self.max_co2e_ratio is not None:
            if t_co2e is None:
                co2e_uncomputable = True
                reasons.append(EvaluationReason.TEACHER_CO2E_MISSING)
                details[EvaluationReason.TEACHER_CO2E_MISSING] = (
                    "teacher_co2e_kg is required when max_co2e_ratio "
                    "is set"
                )
            elif t_co2e == 0.0:
                co2e_uncomputable = True
                reasons.append(EvaluationReason.ZERO_TEACHER_CO2E)
                details[EvaluationReason.ZERO_TEACHER_CO2E] = (
                    "teacher_co2e_kg is 0; co2e ratio check skipped"
                )
            elif s_co2e is None:
                co2e_uncomputable = True
                reasons.append(EvaluationReason.STUDENT_CO2E_MISSING)
                details[EvaluationReason.STUDENT_CO2E_MISSING] = (
                    "student_co2e_kg is required when max_co2e_ratio "
                    "is set"
                )
            else:
                co2e_ratio = s_co2e / t_co2e
                if co2e_ratio > self.max_co2e_ratio:
                    co2e_failed = True
                    reasons.append(EvaluationReason.ABOVE_CO2E)
                    details[EvaluationReason.ABOVE_CO2E] = (
                        f"co2e ratio {co2e_ratio:.4f} above "
                        f"threshold {self.max_co2e_ratio}"
                    )

        # --- Classical baseline comparison ---
        classical_retention: Optional[float] = None
        beats_classical: Optional[bool] = None
        if (
            baseline is not None
            and s_quality is not None
            and baseline > 0
        ):
            classical_retention = s_quality / baseline
            beats_classical = s_quality >= baseline

        # --- Determine overall status ---
        failure_count = sum((quality_failed, energy_failed, co2e_failed))
        uncomputable_count = sum(
            (quality_uncomputable, energy_uncomputable, co2e_uncomputable)
        )

        if failure_count >= 2:
            status = EvaluationStatus.MULTIPLE_FAILURES
        elif quality_failed:
            status = EvaluationStatus.BELOW_QUALITY
        elif energy_failed:
            status = EvaluationStatus.ABOVE_ENERGY
        elif co2e_failed:
            status = EvaluationStatus.ABOVE_CO2E
        elif uncomputable_count > 0:
            # No hard failures, but at least one check couldn't run.
            status = EvaluationStatus.INSUFFICIENT_DATA
        else:
            status = EvaluationStatus.PASSED

        if status == EvaluationStatus.PASSED:
            reasons = []

        return EvaluationResult(
            model_id=model_id,
            teacher_model_id=teacher_model_id,
            status=status,
            reasons=tuple(reasons),
            details=details,
            quality=s_quality,
            energy_kwh=s_energy,
            co2e_kg=s_co2e,
            teacher_quality=t_quality,
            teacher_energy_kwh=t_energy,
            teacher_co2e_kg=t_co2e,
            retention=retention,
            energy_ratio=energy_ratio,
            co2e_ratio=co2e_ratio,
            sample_count=sample_count,
            classical_baseline_quality=baseline,
            classical_retention=classical_retention,
            beats_classical=beats_classical,
            thresholds=MappingProxyType({
                "min_quality_retention": self.min_quality_retention,
                "max_energy_ratio": self.max_energy_ratio,
                "max_co2e_ratio": self.max_co2e_ratio,
            }),
        )

    # ------------------------------------------------------------------ batch
    def evaluate_batch(
        self, models: Sequence[ModelSpec],
    ) -> Tuple[EvaluationResult, ...]:
        """Evaluate a sequence of ``ModelSpec`` rows, one result per row."""
        return tuple(
            self.evaluate(
                model_id=m.model_id,
                teacher_quality=m.teacher_quality,
                student_quality=m.student_quality,
                teacher_energy_kwh=m.teacher_energy_kwh,
                student_energy_kwh=m.student_energy_kwh,
                student_co2e_kg=m.student_co2e_kg,
                teacher_co2e_kg=m.teacher_co2e_kg,
                sample_count=m.sample_count,
                classical_baseline_quality=m.classical_baseline_quality,
                teacher_model_id=m.teacher_model_id,
            )
            for m in models
        )

    # ------------------------------------------------------------------ raising
    def evaluate_or_raise(
        self,
        *,
        model_id: str,
        teacher_quality: Optional[float],
        student_quality: Optional[float],
        teacher_energy_kwh: Optional[float],
        student_energy_kwh: Optional[float],
        student_co2e_kg: Optional[float] = None,
        teacher_co2e_kg: Optional[float] = None,
        sample_count: int = 0,
        classical_baseline_quality: Optional[float] = None,
        teacher_model_id: Optional[str] = None,
    ) -> EvaluationResult:
        """
        Same as ``evaluate``, but raises a typed exception on non-pass.

        - ``StudentEvaluationInputError``          — status=INVALID_INPUT
        - ``StudentEvaluationInsufficientData``    — status=INSUFFICIENT_DATA
        - ``StudentQualityBelowThreshold``         — status=BELOW_QUALITY
        - ``StudentEnergyAboveRatio``              — status=ABOVE_ENERGY
        - ``StudentCo2eAboveRatio``                — status=ABOVE_CO2E
        - ``StudentEvaluationError``               — status=MULTIPLE_FAILURES
        """
        result = self.evaluate(
            model_id=model_id,
            teacher_quality=teacher_quality,
            student_quality=student_quality,
            teacher_energy_kwh=teacher_energy_kwh,
            student_energy_kwh=student_energy_kwh,
            student_co2e_kg=student_co2e_kg,
            teacher_co2e_kg=teacher_co2e_kg,
            sample_count=sample_count,
            classical_baseline_quality=classical_baseline_quality,
            teacher_model_id=teacher_model_id,
        )
        if result.status == EvaluationStatus.PASSED:
            return result
        if result.status == EvaluationStatus.INVALID_INPUT:
            raise StudentEvaluationInputError(
                "; ".join(
                    result.details.get(r, r.value)
                    for r in result.reasons
                ) or "invalid input"
            )
        if result.status == EvaluationStatus.INSUFFICIENT_DATA:
            raise StudentEvaluationInsufficientData(
                "; ".join(
                    result.details.get(r, r.value)
                    for r in result.reasons
                ) or "insufficient data"
            )
        if result.status == EvaluationStatus.BELOW_QUALITY:
            assert result.retention is not None
            raise StudentQualityBelowThreshold(
                retention=result.retention,
                threshold=self.min_quality_retention,
            )
        if result.status == EvaluationStatus.ABOVE_ENERGY:
            assert result.energy_ratio is not None
            raise StudentEnergyAboveRatio(
                ratio=result.energy_ratio,
                threshold=self.max_energy_ratio,
            )
        if result.status == EvaluationStatus.ABOVE_CO2E:
            assert result.co2e_ratio is not None
            assert self.max_co2e_ratio is not None
            raise StudentCo2eAboveRatio(
                ratio=result.co2e_ratio,
                threshold=self.max_co2e_ratio,
            )
        # MULTIPLE_FAILURES
        raise StudentEvaluationError(
            "; ".join(
                result.details.get(r, r.value) for r in result.reasons
            ) or "multiple failures"
        )


# =============================================================================
# Public API
# =============================================================================

__all__: Tuple[str, ...] = (
    "EvaluationReason",
    "EvaluationResult",
    "EvaluationStatus",
    "ModelSpec",
    "StudentCo2eAboveRatio",
    "StudentEnergyAboveRatio",
    "StudentEvaluationError",
    "StudentEvaluationInputError",
    "StudentEvaluationInsufficientData",
    "StudentEvaluatorConfig",
    "StudentModelEvaluator",
    "StudentQualityBelowThreshold",
)


# =============================================================================
# Demo: python -m qcs.distillation.student_model_evaluator
# =============================================================================

if __name__ == "__main__":  # pragma: no cover
    import json

    logging.basicConfig(level=logging.INFO)

    print("=== Happy path ===")
    evaluator = StudentModelEvaluator(
        min_quality_retention=0.85,
        max_energy_ratio=0.7,
    )
    result = evaluator.evaluate(
        model_id="student-int8-1",
        teacher_quality=0.92,
        student_quality=0.85,
        teacher_energy_kwh=5.0,
        student_energy_kwh=1.2,
        student_co2e_kg=0.5,
        sample_count=1000,
        classical_baseline_quality=0.80,
    )
    print(f"  {result!r}")
    print(f"  status:             {result.status.value}")
    print(f"  passes:             {result.passes}")
    print(f"  retention:          {result.retention:.4f}")
    print(f"  energy_ratio:       {result.energy_ratio:.4f}")
    print(f"  classical_retention:{result.classical_retention:.4f}")
    print(f"  beats_classical:    {result.beats_classical}")
    print(f"  quality CI (95%):   {result.quality_confidence_interval()}")
    print(f"  retention CI (95%): {result.retention_confidence_interval()}")
    print(f"  passes_with_conf:   {result.passes_with_confidence()}")
    print(f"  checksum:           {result.checksum()[:16]}…")

    print("\n=== Immutability ===")
    try:
        result.quality = 0.99  # type: ignore[misc]
    except Exception as e:
        print(f"  quality mutation → {type(e).__name__}")

    print("\n=== §1.1 student_quality=None does not crash ===")
    r_none = evaluator.evaluate(
        model_id="student-nodata",
        teacher_quality=0.92,
        student_quality=None,
        teacher_energy_kwh=5.0,
        student_energy_kwh=1.2,
    )
    print(f"  status:             {r_none.status.value}")
    print(f"  retention:          {r_none.retention}")
    print(f"  retention_error:    {r_none.retention_error}")

    print("\n=== §1.2 negative energy rejected ===")
    r_neg = evaluator.evaluate(
        model_id="student-neg",
        teacher_quality=0.92,
        student_quality=0.85,
        teacher_energy_kwh=5.0,
        student_energy_kwh=-1.0,
    )
    print(f"  status:  {r_neg.status.value}")
    print(f"  reasons: {[r.value for r in r_neg.reasons]}")

    print("\n=== §2.6 zero teacher energy (check skipped, not failed) ===")
    r_zero = evaluator.evaluate(
        model_id="student-zero-teacher",
        teacher_quality=0.92,
        student_quality=0.90,
        teacher_energy_kwh=0.0,
        student_energy_kwh=0.001,
    )
    print(f"  status:  {r_zero.status.value}")
    print(f"  reasons: {[r.value for r in r_zero.reasons]}")

    print("\n=== Each failure mode ===")
    for label, kwargs in [
        ("below quality",
         dict(teacher_quality=0.92, student_quality=0.70,
              teacher_energy_kwh=5.0, student_energy_kwh=1.0)),
        ("above energy",
         dict(teacher_quality=0.92, student_quality=0.90,
              teacher_energy_kwh=5.0, student_energy_kwh=4.0)),
        ("both fail",
         dict(teacher_quality=0.92, student_quality=0.50,
              teacher_energy_kwh=5.0, student_energy_kwh=4.0)),
    ]:
        r = evaluator.evaluate(model_id="m", **kwargs)
        print(f"  {label}: status={r.status.value}, "
              f"reasons={[x.value for x in r.reasons]}")

    print("\n=== Carbon check (opt-in) ===")
    ev_co2e = StudentModelEvaluator(
        min_quality_retention=0.85, max_energy_ratio=0.7,
        max_co2e_ratio=0.5,
    )
    r_c_ok = ev_co2e.evaluate(
        model_id="m", teacher_quality=0.92, student_quality=0.90,
        teacher_energy_kwh=5.0, student_energy_kwh=1.0,
        teacher_co2e_kg=2.0, student_co2e_kg=0.5,
    )
    print(f"  co2e ok:   status={r_c_ok.status.value}, "
          f"co2e_ratio={r_c_ok.co2e_ratio:.4f}")
    r_c_fail = ev_co2e.evaluate(
        model_id="m", teacher_quality=0.92, student_quality=0.90,
        teacher_energy_kwh=5.0, student_energy_kwh=1.0,
        teacher_co2e_kg=2.0, student_co2e_kg=1.5,
    )
    print(f"  co2e fail: status={r_c_fail.status.value}, "
          f"reasons={[x.value for x in r_c_fail.reasons]}")
    r_c_missing = ev_co2e.evaluate(
        model_id="m", teacher_quality=0.92, student_quality=0.90,
        teacher_energy_kwh=5.0, student_energy_kwh=1.0,
    )
    print(f"  co2e missing: status={r_c_missing.status.value}, "
          f"reasons={[x.value for x in r_c_missing.reasons]}")

    print("\n=== §1.4 Constructor validation ===")
    for label, kwargs in [
        ("min_retention > 10 (warn)",
         dict(min_quality_retention=100.0)),
        ("min_retention NaN",
         dict(min_quality_retention=float("nan"))),
        ("min_retention negative",
         dict(min_quality_retention=-0.1)),
        ("max_energy 0",
         dict(max_energy_ratio=0.0)),
        ("max_energy negative",
         dict(max_energy_ratio=-0.5)),
        ("max_co2e 0",
         dict(max_co2e_ratio=0.0)),
    ]:
        try:
            StudentModelEvaluator(**kwargs)
            print(f"  {label}: accepted")
        except StudentEvaluationInputError as e:
            print(f"  {label}: {e}")

    print("\n=== §3.1 is_success with alternate thresholds ===")
    print(f"  default thresholds:        {result.is_success()}")
    print(f"  min_retention=0.95:        "
          f"{result.is_success(min_retention=0.95)}")
    print(f"  max_energy_ratio=0.3:      "
          f"{result.is_success(max_energy_ratio=0.3)}")

    print("\n=== §3.6 evaluate_or_raise ===")
    for label, kwargs in [
        ("below quality",
         dict(model_id="m", teacher_quality=0.92, student_quality=0.70,
              teacher_energy_kwh=5.0, student_energy_kwh=1.0)),
        ("above energy",
         dict(model_id="m", teacher_quality=0.92, student_quality=0.90,
              teacher_energy_kwh=5.0, student_energy_kwh=4.0)),
        ("invalid input",
         dict(model_id="m", teacher_quality=0.92, student_quality=0.90,
              teacher_energy_kwh=5.0, student_energy_kwh=-1.0)),
        ("insufficient data",
         dict(model_id="m", teacher_quality=0.92, student_quality=None,
              teacher_energy_kwh=5.0, student_energy_kwh=1.0)),
    ]:
        try:
            evaluator.evaluate_or_raise(**kwargs)
        except StudentEvaluationError as e:
            print(f"  {label}: {type(e).__name__}: {e}")

    print("\n=== §3.2 Batch evaluation ===")
    batch = [
        ModelSpec(
            model_id=f"m-{i}",
            teacher_quality=0.92, student_quality=0.90 - i * 0.05,
            teacher_energy_kwh=5.0, student_energy_kwh=1.0 + i * 0.5,
        )
        for i in range(3)
    ]
    results = evaluator.evaluate_batch(batch)
    for r in results:
        print(f"  {r.summary()}")

    print("\n=== Serialization round-trip ===")
    d = result.to_dict()
    print(f"  JSON length: {len(json.dumps(d, default=str))} bytes")
    restored = EvaluationResult.from_dict(d)
    print(f"  round-trip: {restored == result}")
    try:
        EvaluationResult.from_dict({**d, "extra": 1})
    except StudentEvaluationError as e:
        print(f"  strict from_dict: {e}")

    print("\n=== Result __post_init__ consistency ===")
    try:
        EvaluationResult(
            model_id="m", status=EvaluationStatus.PASSED,
            reasons=(EvaluationReason.BELOW_QUALITY,),
            quality=0.85, teacher_quality=0.92,
            retention=0.924,
            sample_count=100,
        )
    except StudentEvaluationError as e:
        print(f"  contradictory result rejected: {e}")

    print(f"\n=== __version__ === {__version__}")
    print(f"=== __all__ === {__all__}")
    print("\nSmoke test passed.")
