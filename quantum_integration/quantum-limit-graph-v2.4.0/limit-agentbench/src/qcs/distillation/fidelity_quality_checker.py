"""
Distillation top-1 agreement checker.

Measures how often the student's output matches the teacher's, example
by example. This is a **classification agreement** metric — it measures
imitation, not correctness, and it is not the same "fidelity" that
``QuantumExecutionResult.fidelity`` or ``BackendProfile.gate_fidelity_2q``
report elsewhere in QCS.

For quantum state or gate fidelity, see the ``QuantumDistiller`` and
``BackendProfile`` modules.

Original API preserved:
    checker = FidelityQualityChecker(min_fidelity=0.9)
    result = checker.check(
        teacher_outputs=[...], student_outputs=[...],
    )
    result.passes / result.fidelity / result.reasons

v2 — see changelog:
  * Renamed to Top1AgreementChecker (FidelityQualityChecker is a
    deprecated alias).
  * min_fidelity validated in [0, 1]; min_samples guard added.
  * FidelityCheckStatus enum distinguishes PASSED / FAILED /
    INVALID_INPUT / INSUFFICIENT_SAMPLES.
  * FidelityReason enum + structured details for messages.
  * Frozen, self-validating result; Tuple reasons; MappingProxyType
    details.
  * sample_count on result; confidence_interval() (Wilson).
  * Optional normalize + comparator hooks; ready-made normalizers.
  * Optional ground_truth to report per-model accuracy.
  * check_or_raise() with typed exceptions.
  * StreamingFidelityChecker for large / streamed datasets.
  * Top1AgreementConfig dataclass.
  * to_dict() + checksum() for audit references.
  * __version__, __all__, __main__ demo.
"""

from __future__ import annotations

import hashlib
import json
import logging
import math
import statistics
import warnings
from dataclasses import asdict, dataclass, field, fields
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
# Errors
# =============================================================================

class FidelityCheckError(Exception):
    """Base class for exceptions raised by the checkers."""


class InvalidInputError(FidelityCheckError, ValueError):
    """Caller passed invalid inputs (empty lists, length mismatch, etc.)."""


class InsufficientSamplesError(FidelityCheckError):
    """Fewer samples than ``min_samples``."""


class BelowThresholdError(FidelityCheckError):
    """Fidelity is below the configured threshold."""

    def __init__(
        self,
        fidelity: Optional[float],
        threshold: float,
        sample_count: int,
    ) -> None:
        self.fidelity = fidelity
        self.threshold = threshold
        self.sample_count = sample_count
        super().__init__(
            f"fidelity {fidelity} below threshold {threshold} "
            f"(n={sample_count})"
        )


# =============================================================================
# Enums
# =============================================================================

class FidelityCheckStatus(str, Enum):
    """Outcome of a fidelity check."""

    PASSED = "passed"                        # check ran, fidelity >= threshold
    FAILED = "failed"                        # check ran, fidelity < threshold
    INVALID_INPUT = "invalid_input"          # check could not run
    INSUFFICIENT_SAMPLES = "insufficient_samples"


class FidelityReason(str, Enum):
    """Typed reason codes attached to the result."""

    PASSED = "passed"
    BELOW_THRESHOLD = "below_threshold"
    EMPTY_TEACHER = "empty_teacher"
    LENGTH_MISMATCH = "length_mismatch"
    INVALID_TYPE = "invalid_type"
    GROUND_TRUTH_MISMATCH = "ground_truth_mismatch"
    INSUFFICIENT_SAMPLES = "insufficient_samples"


# =============================================================================
# Normalizers
# =============================================================================

def strip_normalize(s: str) -> str:
    """Trim whitespace (common CSV-source fix)."""
    return s.strip()


def casefold_normalize(s: str) -> str:
    """Trim whitespace and casefold (common CSV-source fix)."""
    return s.strip().casefold()


# =============================================================================
# Config
# =============================================================================

@dataclass(frozen=True)
class Top1AgreementConfig:
    """Frozen, validated configuration for ``Top1AgreementChecker``."""

    min_fidelity: float = 0.9
    min_samples: int = 30

    def __post_init__(self) -> None:
        if isinstance(self.min_fidelity, bool) \
                or not isinstance(self.min_fidelity, (int, float)):
            raise ValueError("min_fidelity must be a number")
        if not math.isfinite(float(self.min_fidelity)):
            raise ValueError("min_fidelity must be finite")
        if not (0.0 <= float(self.min_fidelity) <= 1.0):
            raise ValueError(
                f"min_fidelity must be in [0, 1]; got {self.min_fidelity}"
            )
        if isinstance(self.min_samples, bool) \
                or not isinstance(self.min_samples, int):
            raise ValueError("min_samples must be an int")
        if self.min_samples < 1:
            raise ValueError("min_samples must be >= 1")

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


# =============================================================================
# Result
# =============================================================================

@dataclass(frozen=True)
class FidelityCheckResult:
    """
    Outcome of a top-1 agreement check.

    ``passes`` is derived from ``status == PASSED``. The consistency
    check in ``__post_init__`` rejects records where ``passes`` disagrees
    with ``fidelity >= threshold``.
    """

    status: FidelityCheckStatus
    threshold: float
    sample_count: int
    fidelity: Optional[float] = None
    teacher_accuracy: Optional[float] = None
    student_accuracy: Optional[float] = None
    reasons: Tuple[FidelityReason, ...] = ()
    details: Mapping[FidelityReason, str] = field(default_factory=dict)

    def __post_init__(self) -> None:
        # --- Enum coercion ---
        if not isinstance(self.status, FidelityCheckStatus):
            try:
                object.__setattr__(
                    self, "status", FidelityCheckStatus(self.status),
                )
            except ValueError:
                raise ValueError(
                    f"unknown status {self.status!r}"
                ) from None

        # --- Reasons tuple + enum coercion ---
        if not isinstance(self.reasons, tuple):
            object.__setattr__(self, "reasons", tuple(self.reasons))
        coerced: List[FidelityReason] = []
        for r in self.reasons:
            if isinstance(r, FidelityReason):
                coerced.append(r)
            else:
                try:
                    coerced.append(FidelityReason(r))
                except ValueError:
                    raise ValueError(f"unknown reason {r!r}") from None
        object.__setattr__(self, "reasons", tuple(coerced))

        # --- Freeze details ---
        if not isinstance(self.details, MappingProxyType):
            object.__setattr__(
                self, "details",
                MappingProxyType(dict(self.details or {})),
            )

        # --- Numeric validation ---
        if not (0.0 <= float(self.threshold) <= 1.0):
            raise ValueError(
                f"threshold must be in [0, 1]; got {self.threshold}"
            )
        if self.sample_count < 0:
            raise ValueError("sample_count must be >= 0")
        for fname in ("fidelity", "teacher_accuracy", "student_accuracy"):
            v = getattr(self, fname)
            if v is not None:
                if not math.isfinite(float(v)):
                    raise ValueError(f"{fname} must be finite or None")
                if not (0.0 <= float(v) <= 1.0):
                    raise ValueError(
                        f"{fname} must be in [0, 1] or None; got {v}"
                    )

        # --- Consistency: passes/failed must match fidelity comparison ---
        if self.status in (
            FidelityCheckStatus.PASSED, FidelityCheckStatus.FAILED,
        ):
            if self.fidelity is None:
                raise ValueError(
                    f"status={self.status.value} requires a fidelity value"
                )
            expected_passed = self.fidelity >= self.threshold
            if self.status == FidelityCheckStatus.PASSED and not expected_passed:
                raise ValueError(
                    f"status=passed contradicts fidelity={self.fidelity} "
                    f"< threshold={self.threshold}"
                )
            if self.status == FidelityCheckStatus.FAILED and expected_passed:
                raise ValueError(
                    f"status=failed contradicts fidelity={self.fidelity} "
                    f">= threshold={self.threshold}"
                )

    # ------------------------------------------------------------------ props
    @property
    def passes(self) -> bool:
        return self.status == FidelityCheckStatus.PASSED

    @property
    def is_valid(self) -> bool:
        return self.status == FidelityCheckStatus.PASSED

    @property
    def is_failure(self) -> bool:
        return self.status == FidelityCheckStatus.FAILED

    @property
    def is_invalid_input(self) -> bool:
        return self.status == FidelityCheckStatus.INVALID_INPUT

    @property
    def is_insufficient_samples(self) -> bool:
        return self.status == FidelityCheckStatus.INSUFFICIENT_SAMPLES

    # ------------------------------------------------------------------ confidence
    def confidence_interval(
        self, level: float = 0.95,
    ) -> Tuple[float, float]:
        """
        Wilson score interval on ``fidelity``.

        More accurate than the naive normal approximation for proportions
        near 0 or 1. Returns ``(0.0, 1.0)`` when there is no measurable
        fidelity.
        """
        if not (0.0 < level < 1.0):
            raise ValueError("level must be in (0, 1)")
        if self.fidelity is None or self.sample_count == 0:
            return (0.0, 1.0)
        z = statistics.NormalDist().inv_cdf(1 - (1 - level) / 2)
        n = self.sample_count
        p = self.fidelity
        denom = 1 + z * z / n
        center = (p + z * z / (2 * n)) / denom
        half = (
            z * math.sqrt(p * (1 - p) / n + z * z / (4 * n * n))
            / denom
        )
        return (max(0.0, center - half), min(1.0, center + half))

    def passes_with_confidence(
        self, level: float = 0.95,
    ) -> bool:
        """
        True only if the *lower* bound of the CI clears the threshold.

        Stricter than ``passes``; safer for production deployment gates.
        """
        if self.status != FidelityCheckStatus.PASSED:
            return False
        lower, _ = self.confidence_interval(level=level)
        return lower >= self.threshold

    # ------------------------------------------------------------------ serialization
    def to_dict(self) -> Dict[str, Any]:
        return {
            "status": self.status.value,
            "threshold": self.threshold,
            "sample_count": self.sample_count,
            "fidelity": self.fidelity,
            "teacher_accuracy": self.teacher_accuracy,
            "student_accuracy": self.student_accuracy,
            "reasons": [r.value for r in self.reasons],
            "details": {r.value: msg for r, msg in self.details.items()},
        }

    def to_json(self, **kwargs: Any) -> str:
        return json.dumps(self.to_dict(), **kwargs)

    @classmethod
    def from_dict(cls, d: Mapping[str, Any]) -> "FidelityCheckResult":
        known = {f.name for f in fields(cls)}
        unknown = set(d.keys()) - known
        if unknown:
            raise ValueError(
                f"unknown fields in FidelityCheckResult dict: "
                f"{sorted(unknown)}"
            )
        details = d.get("details", {})
        coerced_details: Dict[FidelityReason, str] = {}
        if isinstance(details, Mapping):
            for k, v in details.items():
                try:
                    coerced_details[FidelityReason(k)] = str(v)
                except ValueError:
                    continue
        return cls(
            status=FidelityCheckStatus(d["status"]),
            threshold=float(d["threshold"]),
            sample_count=int(d.get("sample_count", 0)),
            fidelity=(
                float(d["fidelity"])
                if d.get("fidelity") is not None else None
            ),
            teacher_accuracy=(
                float(d["teacher_accuracy"])
                if d.get("teacher_accuracy") is not None else None
            ),
            student_accuracy=(
                float(d["student_accuracy"])
                if d.get("student_accuracy") is not None else None
            ),
            reasons=tuple(
                FidelityReason(r) for r in d.get("reasons", ())
            ),
            details=coerced_details,
        )

    def checksum(self) -> str:
        payload = json.dumps(
            self.to_dict(), sort_keys=True, default=str,
        ).encode("utf-8")
        return hashlib.sha256(payload).hexdigest()

    def __repr__(self) -> str:
        return (
            f"FidelityCheckResult("
            f"status={self.status.value}, "
            f"fidelity={self.fidelity!r}, "
            f"threshold={self.threshold}, "
            f"n={self.sample_count})"
        )


# =============================================================================
# Checker
# =============================================================================

class Top1AgreementChecker:
    """
    Compares the student's top-1 output against the teacher's, per example.

    ``agreement`` = (# teacher/student output pairs that match) / n. This
    is a **classification agreement** metric, not a quantum fidelity.

    Parameters
    ----------
    min_fidelity : float
        Required agreement ratio in [0, 1]. Values outside the range,
        or non-finite values, raise at construction.
    min_samples : int
        Minimum number of samples required before a check is conclusive.
        Checks with fewer samples return
        ``FidelityCheckStatus.INSUFFICIENT_SAMPLES``.
    normalize : Optional[Callable[[str], str]]
        Applied to both outputs before comparison. Use ``strip_normalize``
        or ``casefold_normalize`` for CSV-sourced data. If both
        ``normalize`` and ``comparator`` are set, ``comparator`` wins.
    comparator : Optional[Callable[[str, str], bool]]
        Alternative equality predicate over the normalized strings.
        Defaults to ``==``.
    """

    def __init__(
        self,
        *,
        min_fidelity: float = 0.9,
        min_samples: int = 30,
        normalize: Optional[Callable[[str], str]] = None,
        comparator: Optional[Callable[[str, str], bool]] = None,
        config: Optional[Top1AgreementConfig] = None,
    ) -> None:
        if config is None:
            config = Top1AgreementConfig(
                min_fidelity=min_fidelity, min_samples=min_samples,
            )
        self.config = config
        self.min_fidelity = float(config.min_fidelity)
        self.min_samples = int(config.min_samples)
        self.normalize = normalize or (lambda s: s)
        self.comparator = comparator or (lambda a, b: a == b)

    # ------------------------------------------------------------------ check
    def check(
        self,
        *,
        teacher_outputs: Sequence[str],
        student_outputs: Sequence[str],
        ground_truth: Optional[Sequence[str]] = None,
    ) -> FidelityCheckResult:
        """Compute the top-1 agreement for this batch."""
        # --- Input type guards ---
        self._ensure_str_sequence("teacher_outputs", teacher_outputs)
        self._ensure_str_sequence("student_outputs", student_outputs)

        # --- Empty / length mismatch ---
        if not teacher_outputs:
            return self._invalid_result(
                reason=FidelityReason.EMPTY_TEACHER,
                detail="teacher_outputs is empty",
            )
        if len(teacher_outputs) != len(student_outputs):
            return self._invalid_result(
                reason=FidelityReason.LENGTH_MISMATCH,
                detail=(
                    f"length mismatch: teacher={len(teacher_outputs)} "
                    f"student={len(student_outputs)}"
                ),
            )
        if ground_truth is not None:
            self._ensure_str_sequence("ground_truth", ground_truth)
            if len(ground_truth) != len(teacher_outputs):
                return self._invalid_result(
                    reason=FidelityReason.GROUND_TRUTH_MISMATCH,
                    detail=(
                        f"ground_truth length {len(ground_truth)} != "
                        f"teacher_outputs length {len(teacher_outputs)}"
                    ),
                )

        n = len(teacher_outputs)
        matches = 0
        for t, s in zip(teacher_outputs, student_outputs):
            if self.comparator(self.normalize(t), self.normalize(s)):
                matches += 1

        fidelity = matches / n

        teacher_acc: Optional[float] = None
        student_acc: Optional[float] = None
        if ground_truth is not None:
            teacher_matches = sum(
                1 for t, g in zip(teacher_outputs, ground_truth)
                if self.comparator(self.normalize(t), self.normalize(g))
            )
            student_matches = sum(
                1 for s, g in zip(student_outputs, ground_truth)
                if self.comparator(self.normalize(s), self.normalize(g))
            )
            teacher_acc = teacher_matches / n
            student_acc = student_matches / n

        # --- Insufficient samples (after computing fidelity for reporting) ---
        if n < self.min_samples:
            return FidelityCheckResult(
                status=FidelityCheckStatus.INSUFFICIENT_SAMPLES,
                threshold=self.min_fidelity,
                sample_count=n,
                fidelity=fidelity,
                teacher_accuracy=teacher_acc,
                student_accuracy=student_acc,
                reasons=(FidelityReason.INSUFFICIENT_SAMPLES,),
                details=MappingProxyType({
                    FidelityReason.INSUFFICIENT_SAMPLES:
                        f"only {n} samples; need >= {self.min_samples}",
                }),
            )

        # --- Threshold comparison ---
        if fidelity >= self.min_fidelity:
            status = FidelityCheckStatus.PASSED
            reason = FidelityReason.PASSED
            detail = (
                f"fidelity {fidelity:.3f} ({matches}/{n}) meets "
                f"threshold {self.min_fidelity}"
            )
        else:
            status = FidelityCheckStatus.FAILED
            reason = FidelityReason.BELOW_THRESHOLD
            detail = (
                f"fidelity {fidelity:.3f} ({matches}/{n}) below "
                f"threshold {self.min_fidelity}"
            )

        return FidelityCheckResult(
            status=status,
            threshold=self.min_fidelity,
            sample_count=n,
            fidelity=fidelity,
            teacher_accuracy=teacher_acc,
            student_accuracy=student_acc,
            reasons=(reason,),
            details=MappingProxyType({reason: detail}),
        )

    def check_or_raise(
        self,
        *,
        teacher_outputs: Sequence[str],
        student_outputs: Sequence[str],
        ground_truth: Optional[Sequence[str]] = None,
    ) -> FidelityCheckResult:
        """
        Same as ``check``, but raises a typed exception on non-pass.

        - ``InvalidInputError``         — status=INVALID_INPUT
        - ``InsufficientSamplesError``  — status=INSUFFICIENT_SAMPLES
        - ``BelowThresholdError``       — status=FAILED
        """
        result = self.check(
            teacher_outputs=teacher_outputs,
            student_outputs=student_outputs,
            ground_truth=ground_truth,
        )
        if result.status == FidelityCheckStatus.INVALID_INPUT:
            raise InvalidInputError(
                result.details.get(
                    result.reasons[0] if result.reasons else None,
                    "invalid input",
                )
            )
        if result.status == FidelityCheckStatus.INSUFFICIENT_SAMPLES:
            raise InsufficientSamplesError(
                result.details.get(
                    FidelityReason.INSUFFICIENT_SAMPLES,
                    "insufficient samples",
                )
            )
        if result.status == FidelityCheckStatus.FAILED:
            raise BelowThresholdError(
                fidelity=result.fidelity,
                threshold=result.threshold,
                sample_count=result.sample_count,
            )
        return result

    # ------------------------------------------------------------------ helpers
    def _invalid_result(
        self,
        *,
        reason: FidelityReason,
        detail: str,
    ) -> FidelityCheckResult:
        return FidelityCheckResult(
            status=FidelityCheckStatus.INVALID_INPUT,
            threshold=self.min_fidelity,
            sample_count=0,
            fidelity=None,
            reasons=(reason,),
            details=MappingProxyType({reason: detail}),
        )

    @staticmethod
    def _ensure_str_sequence(name: str, value: Any) -> None:
        if isinstance(value, (str, bytes)):
            raise InvalidInputError(
                f"{name} must be a Sequence[str], not a "
                f"{type(value).__name__}"
            )
        if not isinstance(value, Sequence):
            raise InvalidInputError(
                f"{name} must be a Sequence[str], not "
                f"{type(value).__name__}"
            )
        for i, item in enumerate(value):
            if not isinstance(item, str):
                raise InvalidInputError(
                    f"{name}[{i}] must be a str, got "
                    f"{type(item).__name__}"
                )


# =============================================================================
# Streaming companion
# =============================================================================

class StreamingFidelityChecker:
    """
    Incremental equivalent of ``Top1AgreementChecker`` for streaming data.

    Call ``update(teacher, student)`` for each pair, then ``finalize()``.
    The final result is equivalent to running ``Top1AgreementChecker``
    over the concatenated lists.
    """

    def __init__(
        self,
        *,
        min_fidelity: float = 0.9,
        min_samples: int = 30,
        normalize: Optional[Callable[[str], str]] = None,
        comparator: Optional[Callable[[str, str], bool]] = None,
        config: Optional[Top1AgreementConfig] = None,
    ) -> None:
        self._config = config or Top1AgreementConfig(
            min_fidelity=min_fidelity, min_samples=min_samples,
        )
        self._normalize = normalize or (lambda s: s)
        self._comparator = comparator or (lambda a, b: a == b)
        self._total: int = 0
        self._matches: int = 0

    def update(self, teacher_output: str, student_output: str) -> None:
        if not isinstance(teacher_output, str) \
                or not isinstance(student_output, str):
            raise InvalidInputError(
                "teacher_output and student_output must be strings"
            )
        self._total += 1
        if self._comparator(
            self._normalize(teacher_output),
            self._normalize(student_output),
        ):
            self._matches += 1

    def update_batch(
        self,
        teacher_outputs: Sequence[str],
        student_outputs: Sequence[str],
    ) -> None:
        Top1AgreementChecker._ensure_str_sequence(
            "teacher_outputs", teacher_outputs,
        )
        Top1AgreementChecker._ensure_str_sequence(
            "student_outputs", student_outputs,
        )
        if len(teacher_outputs) != len(student_outputs):
            raise InvalidInputError(
                f"length mismatch: teacher={len(teacher_outputs)} "
                f"student={len(student_outputs)}"
            )
        for t, s in zip(teacher_outputs, student_outputs):
            self.update(t, s)

    def finalize(self) -> FidelityCheckResult:
        if self._total == 0:
            return FidelityCheckResult(
                status=FidelityCheckStatus.INVALID_INPUT,
                threshold=float(self._config.min_fidelity),
                sample_count=0,
                fidelity=None,
                reasons=(FidelityReason.EMPTY_TEACHER,),
                details=MappingProxyType({
                    FidelityReason.EMPTY_TEACHER:
                        "no updates received before finalize()",
                }),
            )
        fidelity = self._matches / self._total
        if self._total < self._config.min_samples:
            return FidelityCheckResult(
                status=FidelityCheckStatus.INSUFFICIENT_SAMPLES,
                threshold=float(self._config.min_fidelity),
                sample_count=self._total,
                fidelity=fidelity,
                reasons=(FidelityReason.INSUFFICIENT_SAMPLES,),
                details=MappingProxyType({
                    FidelityReason.INSUFFICIENT_SAMPLES:
                        f"only {self._total} samples; need >= "
                        f"{self._config.min_samples}",
                }),
            )
        if fidelity >= self._config.min_fidelity:
            status = FidelityCheckStatus.PASSED
            reason = FidelityReason.PASSED
            detail = (
                f"fidelity {fidelity:.3f} "
                f"({self._matches}/{self._total}) meets threshold "
                f"{self._config.min_fidelity}"
            )
        else:
            status = FidelityCheckStatus.FAILED
            reason = FidelityReason.BELOW_THRESHOLD
            detail = (
                f"fidelity {fidelity:.3f} "
                f"({self._matches}/{self._total}) below threshold "
                f"{self._config.min_fidelity}"
            )
        return FidelityCheckResult(
            status=status,
            threshold=float(self._config.min_fidelity),
            sample_count=self._total,
            fidelity=fidelity,
            reasons=(reason,),
            details=MappingProxyType({reason: detail}),
        )


# =============================================================================
# Deprecated alias
# =============================================================================

class FidelityQualityChecker(Top1AgreementChecker):
    """
    Deprecated alias for ``Top1AgreementChecker``.

    The class name "fidelity" conflates top-1 agreement with quantum
    fidelity. Use ``Top1AgreementChecker`` in new code. This alias will
    be removed in v3.0.0.
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        warnings.warn(
            "FidelityQualityChecker is deprecated; use "
            "Top1AgreementChecker. The class measures top-1 agreement, "
            "not quantum fidelity.",
            DeprecationWarning,
            stacklevel=2,
        )
        super().__init__(*args, **kwargs)


# =============================================================================
# Public API
# =============================================================================

__all__: Tuple[str, ...] = (
    "BelowThresholdError",
    "FidelityCheckError",
    "FidelityCheckResult",
    "FidelityCheckStatus",
    "FidelityQualityChecker",  # deprecated alias
    "FidelityReason",
    "InsufficientSamplesError",
    "InvalidInputError",
    "StreamingFidelityChecker",
    "Top1AgreementChecker",
    "Top1AgreementConfig",
    "casefold_normalize",
    "strip_normalize",
)


# =============================================================================
# Smoke test: python -m qcs.distillation.fidelity_quality_checker
# =============================================================================

if __name__ == "__main__":  # pragma: no cover
    import json

    logging.basicConfig(level=logging.INFO)

    print("=== Valid construction ===")
    checker = Top1AgreementChecker(min_fidelity=0.9, min_samples=30)
    print(f"  min_fidelity={checker.min_fidelity}, "
          f"min_samples={checker.min_samples}")

    print("\n=== Construction validation ===")
    for label, kwargs in [
        ("min_fidelity > 1",
         dict(min_fidelity=1.5)),
        ("min_fidelity < 0",
         dict(min_fidelity=-0.1)),
        ("min_fidelity NaN",
         dict(min_fidelity=float("nan"))),
        ("min_fidelity bool",
         dict(min_fidelity=True)),  # type: ignore[arg-type]
        ("min_samples 0",
         dict(min_samples=0)),
        ("min_samples negative",
         dict(min_samples=-1)),
    ]:
        try:
            Top1AgreementChecker(**kwargs)
            print(f"  {label}: NOT rejected")
        except ValueError as e:
            print(f"  {label}: {e}")

    print("\n=== Passing check (100 samples, 92 matches) ===")
    teacher = [f"label_{i % 10}" for i in range(100)]
    student = teacher.copy()
    student[0] = "wrong"
    student[1] = "wrong"
    student[2] = "wrong"
    student[3] = "wrong"
    student[4] = "wrong"
    student[5] = "wrong"
    student[6] = "wrong"
    student[7] = "wrong"
    result = checker.check(
        teacher_outputs=teacher, student_outputs=student,
    )
    print(f"  {result!r}")
    print(f"  status={result.status.value}, "
          f"fidelity={result.fidelity:.3f}, "
          f"passes={result.passes}")
    print(f"  CI95: {result.confidence_interval()}")
    print(f"  passes_with_confidence: "
          f"{result.passes_with_confidence()}")

    print("\n=== Failing check ===")
    student_more_wrong = teacher.copy()
    for i in range(0, 30):
        student_more_wrong[i] = "wrong"
    result_fail = checker.check(
        teacher_outputs=teacher, student_outputs=student_more_wrong,
    )
    print(f"  {result_fail!r}")
    print(f"  fidelity={result_fail.fidelity:.3f}, "
          f"status={result_fail.status.value}")
    print(f"  reasons: {[r.value for r in result_fail.reasons]}")

    print("\n=== Invalid input ===")
    invalid = checker.check(teacher_outputs=[], student_outputs=[])
    print(f"  empty: {invalid!r} "
          f"(status={invalid.status.value})")
    assert invalid.status == FidelityCheckStatus.INVALID_INPUT
    assert invalid.passes is False

    mismatch = checker.check(
        teacher_outputs=["a", "b"], student_outputs=["a"],
    )
    print(f"  mismatch: status={mismatch.status.value}")

    # String-as-list rejection
    try:
        checker.check(
            teacher_outputs="hello",  # type: ignore[arg-type]
            student_outputs="hello",  # type: ignore[arg-type]
        )
    except InvalidInputError as e:
        print(f"  string-as-list rejected: {e}")

    print("\n=== Insufficient samples ===")
    tiny = checker.check(
        teacher_outputs=["a", "b"], student_outputs=["a", "b"],
    )
    print(f"  n=2: status={tiny.status.value}, "
          f"fidelity={tiny.fidelity}, passes={tiny.passes}")
    assert tiny.status == FidelityCheckStatus.INSUFFICIENT_SAMPLES

    print("\n=== Normalization ===")
    checker_norm = Top1AgreementChecker(
        min_fidelity=0.9, min_samples=3,
        normalize=casefold_normalize,
    )
    r = checker_norm.check(
        teacher_outputs=["Cat", "Dog\n", "Fish"],
        student_outputs=["cat", "dog", "FISH"],
    )
    print(f"  fidelity with casefold+strip: {r.fidelity}")
    assert r.fidelity == 1.0

    print("\n=== Comparator (tolerance) ===")
    checker_tol = Top1AgreementChecker(
        min_fidelity=0.9, min_samples=3,
        comparator=lambda a, b: abs(float(a) - float(b)) < 0.01,
    )
    r = checker_tol.check(
        teacher_outputs=["0.50", "0.75", "0.90"],
        student_outputs=["0.501", "0.749", "0.905"],
    )
    print(f"  tolerance matches: {r.fidelity}")
    assert r.fidelity == 1.0

    print("\n=== Ground truth accuracy ===")
    gt = [f"label_{i % 10}" for i in range(100)]
    r_with_gt = checker.check(
        teacher_outputs=teacher,
        student_outputs=student,
        ground_truth=gt,
    )
    print(f"  teacher_accuracy: {r_with_gt.teacher_accuracy:.3f}")
    print(f"  student_accuracy: {r_with_gt.student_accuracy:.3f}")

    print("\n=== check_or_raise ===")
    try:
        checker.check_or_raise(
            teacher_outputs=teacher, student_outputs=student_more_wrong,
        )
    except BelowThresholdError as e:
        print(f"  BelowThresholdError: fidelity={e.fidelity}, "
              f"threshold={e.threshold}, n={e.sample_count}")

    try:
        checker.check_or_raise(
            teacher_outputs=["a"], student_outputs=["a"],
        )
    except InsufficientSamplesError as e:
        print(f"  InsufficientSamplesError: {e}")

    try:
        checker.check_or_raise(
            teacher_outputs=[], student_outputs=[],
        )
    except InvalidInputError as e:
        print(f"  InvalidInputError: {e}")

    print("\n=== Streaming ===")
    streaming = StreamingFidelityChecker(
        min_fidelity=0.9, min_samples=3,
    )
    streaming.update_batch(
        teacher_outputs=["a", "b", "c", "d"],
        student_outputs=["a", "b", "c", "wrong"],
    )
    streamed = streaming.finalize()
    print(f"  {streamed!r}")
    assert streamed.fidelity == 0.75
    assert streamed.status == FidelityCheckStatus.FAILED

    print("\n=== Deprecated alias ===")
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        _ = FidelityQualityChecker(min_fidelity=0.9)
        assert any(
            issubclass(w.category, DeprecationWarning) for w in caught
        )
    print(f"  FidelityQualityChecker emitted DeprecationWarning ✓")

    print("\n=== Serialization round-trip ===")
    d = result_fail.to_dict()
    print(f"  dict keys: {sorted(d.keys())}")
    restored = FidelityCheckResult.from_dict(d)
    print(f"  round-trip: {restored == result_fail}")
    print(f"  checksum: {result_fail.checksum()[:16]}…")

    print("\n=== Result __post_init__ consistency ===")
    try:
        FidelityCheckResult(
            status=FidelityCheckStatus.PASSED,
            threshold=0.9,
            sample_count=100,
            fidelity=0.80,     # below threshold, but status says passed
            reasons=(FidelityReason.PASSED,),
        )
    except ValueError as e:
        print(f"  contradictory result rejected: {e}")

    print(f"\n=== __version__ === {__version__}")
    print(f"=== __all__ === {__all__}")
    print("\nSmoke test passed.")
