"""
QuantumGuardrails — pre-flight safety gates for quantum jobs.

Every quantum job must pass these gates before execution. The guards
that always run (unless disabled in config):

  1. Classical baseline is declared (uses the enhanced
     ``QuantumTaskSpec.has_baseline`` / ``baseline`` accessors when
     available; falls back to ``classical_baseline`` for legacy specs).
  2. Quality threshold is set (``request.min_quality > 0``).
  3. Queue budget is within the org cap.
  4. Cost budget is declared and within the org cap. A request with
     ``max_cost_usd=None`` ("unlimited") is **blocked** by default —
     safety gates should not approve unbudgeted spend.
  5. Backend (when ``backend_id`` is provided) is on the allowlist.
  6. Task spec is routable — uses ``is_routable(at=...)`` when
     available, which checks deadline and qubit feasibility.
  7. Request is dispatchable — uses ``validate_for_dispatch(at=...)``
     when available, which checks expiry.

Optional guards (off by default, enable in config):

  * Energy budget (``require_energy_budget``): a request's
    ``max_energy_kwh`` must be declared and within the org cap.
  * Simulator-first advisory (``warn_on_hardware``): advisory verdict
    when the target backend is hardware.
  * Custom checks: callables added to ``config.custom_checks``.

Each guard emits exactly one ``GuardrailVerdict``. Disabled checks
still emit a verdict with ``skipped=True`` and ``severity=INFO`` so
callers can see the full picture — "passed" and "not checked" are
distinguishable.

Original API preserved:
    guardrails = QuantumGuardrails(
        backend_allowlist=[...], simulator_first=True,
        max_queue_seconds=600.0, max_cost=100.0,
    )
    result = guardrails.check(
        task_spec=..., request=...,
        backend_kind=..., backend_id=...,
    )
    result.passed / result.verdicts / result.blocking

v2 fixes (see changelog in commit message):
  * ``request.max_cost`` (deprecated property returning 0.0 for None)
    no longer silently approves unlimited budgets — reads
    ``max_cost_usd`` directly and blocks ``None`` by default.
  * ``severity: str`` → ``GuardrailSeverity``; typos raise.
  * ``classical_baseline`` check uses ``has_baseline`` / ``baseline``
    on enhanced specs (was blocking valid tasks with a structured
    baseline record).
  * ``validate()`` → ``is_routable()`` / ``validate_for_dispatch()``
    (misses expired requests and past-deadline specs).
  * ``GuardrailResult.passed`` derived from verdicts; no more decoupled
    truth.
  * Energy budget check added (optional).
  * ``backend_allowlist`` frozen and validated.
  * ``GuardrailVerdict`` / ``GuardrailResult`` frozen.
  * ``GuardrailsConfig`` dataclass.
  * ``check()`` reports failures instead of crashing on bad inputs.
  * Skipped checks emit visible ``skipped=True`` verdicts.
  * Metrics; health(); structured logging; ``__version__``; ``__all__``;
    demo.
"""

from __future__ import annotations

import hashlib
import json
import logging
import math
import threading
import time
from dataclasses import dataclass, field, fields
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
)

from ..contracts.execution_request import QuantumExecutionRequest
from ..contracts.task_spec import QuantumTaskSpec

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())

__version__ = "2.0.0"


# =============================================================================
# Constants
# =============================================================================

DEFAULT_MAX_REASON_LEN: int = 512
DEFAULT_MAX_QUEUE_SECONDS: float = 600.0
DEFAULT_MAX_COST_USD: float = 100.0
DEFAULT_MAX_ENERGY_KWH: float = 100.0


# =============================================================================
# Errors
# =============================================================================

class GuardrailsError(Exception):
    """Base class for QuantumGuardrails errors."""


class GuardrailsInputError(GuardrailsError, ValueError):
    """Invalid inputs to the constructor or a method."""


# =============================================================================
# Enums
# =============================================================================

class GuardrailSeverity(str, Enum):
    """Severity of a guardrail verdict."""

    INFO = "info"
    WARNING = "warning"
    BLOCKING = "blocking"


# =============================================================================
# Config
# =============================================================================

@dataclass(frozen=True)
class GuardrailsConfig:
    """Frozen, validated configuration for ``QuantumGuardrails``."""

    # --- Allowlist ---
    backend_allowlist: FrozenSet[str] = frozenset()

    # --- Preference ---
    # Kept for backward compatibility. Prefer the clearer
    # `warn_on_hardware` below; both are accepted and independent.
    simulator_first: bool = True
    warn_on_hardware: bool = True

    # --- Budgets ---
    max_queue_seconds: float = DEFAULT_MAX_QUEUE_SECONDS
    max_cost_usd: float = DEFAULT_MAX_COST_USD
    max_energy_kwh: Optional[float] = None   # None = energy check disabled

    # --- Requirement flags ---
    require_cost_budget: bool = True   # False → unlimited spend allowed
    require_energy_budget: bool = False   # True → unbudgeted energy blocked
    require_baseline: bool = True

    # --- Reason truncation ---
    max_reason_len: int = DEFAULT_MAX_REASON_LEN

    # --- Extensibility ---
    custom_checks: Tuple[Callable[[Any, Any], "GuardrailVerdict"], ...] = ()

    def __post_init__(self) -> None:
        # --- Coerce backend_allowlist ---
        if not isinstance(self.backend_allowlist, frozenset):
            object.__setattr__(
                self, "backend_allowlist",
                frozenset(self.backend_allowlist),
            )
        for entry in self.backend_allowlist:
            if not isinstance(entry, str) or not entry.strip():
                raise GuardrailsInputError(
                    "backend_allowlist entries must be non-empty "
                    "strings"
                )

        # --- Numeric validation ---
        for fname in ("max_queue_seconds", "max_cost_usd"):
            v = getattr(self, fname)
            if isinstance(v, bool) \
                    or not isinstance(v, (int, float)) \
                    or not math.isfinite(float(v)) \
                    or v < 0:
                raise GuardrailsInputError(
                    f"{fname} must be a non-negative finite number"
                )
        if self.max_energy_kwh is not None:
            v = self.max_energy_kwh
            if isinstance(v, bool) \
                    or not isinstance(v, (int, float)) \
                    or not math.isfinite(float(v)) \
                    or v <= 0:
                raise GuardrailsInputError(
                    "max_energy_kwh must be a positive finite number "
                    "or None"
                )

        # --- Reason length ---
        if isinstance(self.max_reason_len, bool) \
                or not isinstance(self.max_reason_len, int) \
                or self.max_reason_len <= 0:
            raise GuardrailsInputError(
                "max_reason_len must be a positive int"
            )

        # --- Custom checks ---
        if not isinstance(self.custom_checks, tuple):
            object.__setattr__(
                self, "custom_checks", tuple(self.custom_checks),
            )
        for i, fn in enumerate(self.custom_checks):
            if not callable(fn):
                raise GuardrailsInputError(
                    f"custom_checks[{i}] must be callable"
                )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "backend_allowlist": sorted(self.backend_allowlist),
            "simulator_first": self.simulator_first,
            "warn_on_hardware": self.warn_on_hardware,
            "max_queue_seconds": self.max_queue_seconds,
            "max_cost_usd": self.max_cost_usd,
            "max_energy_kwh": self.max_energy_kwh,
            "require_cost_budget": self.require_cost_budget,
            "require_energy_budget": self.require_energy_budget,
            "require_baseline": self.require_baseline,
            "max_reason_len": self.max_reason_len,
            "n_custom_checks": len(self.custom_checks),
        }


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
# Verdict
# =============================================================================

@dataclass(frozen=True)
class GuardrailVerdict:
    """The verdict of a single guardrail check."""

    name: str
    passed: bool
    reason: str
    severity: GuardrailSeverity = GuardrailSeverity.WARNING
    skipped: bool = False

    def __post_init__(self) -> None:
        if not isinstance(self.name, str) or not self.name.strip():
            raise GuardrailsInputError(
                "verdict name must be a non-empty string"
            )
        if not isinstance(self.passed, bool):
            raise GuardrailsInputError("passed must be a bool")
        if not isinstance(self.reason, str):
            raise GuardrailsInputError("reason must be a string")
        if not isinstance(self.skipped, bool):
            raise GuardrailsInputError("skipped must be a bool")
        if not isinstance(self.severity, GuardrailSeverity):
            try:
                object.__setattr__(
                    self, "severity", GuardrailSeverity(self.severity),
                )
            except (ValueError, KeyError):
                allowed = sorted(s.value for s in GuardrailSeverity)
                raise GuardrailsInputError(
                    f"severity must be one of {allowed}; "
                    f"got {self.severity!r}"
                ) from None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "passed": self.passed,
            "reason": self.reason,
            "severity": self.severity.value,
            "skipped": self.skipped,
        }

    def __repr__(self) -> str:
        status = "skipped" if self.skipped else (
            "passed" if self.passed else "failed"
        )
        return (
            f"GuardrailVerdict("
            f"name={self.name!r}, "
            f"{status}, "
            f"severity={self.severity.value})"
        )


# =============================================================================
# Result
# =============================================================================

@dataclass(frozen=True, init=False)
class GuardrailResult:
    """
    Aggregate verdict across all guardrails.

    ``passed`` and ``blocking`` are derived from ``verdicts`` — the two
    views cannot disagree. ``blocking`` is the tuple of verdicts that
    failed with ``severity=BLOCKING``.
    """

    verdicts: Tuple[GuardrailVerdict, ...]

    def __init__(
        self,
        *,
        verdicts: Sequence[GuardrailVerdict] = (),
        # Legacy kwargs — verified, not stored:
        passed: Optional[bool] = None,
        blocking: Optional[Sequence[GuardrailVerdict]] = None,
    ) -> None:
        verdicts_tuple = tuple(verdicts)
        derived_blocking = tuple(
            v for v in verdicts_tuple
            if not v.passed and v.severity == GuardrailSeverity.BLOCKING
        )
        derived_passed = len(derived_blocking) == 0

        if passed is not None and bool(passed) != derived_passed:
            raise ValueError(
                f"passed={passed} contradicts derived passed="
                f"{derived_passed} from verdicts"
            )
        if blocking is not None:
            if tuple(blocking) != derived_blocking:
                raise ValueError(
                    "blocking= contradicts the failed blocking "
                    "verdicts in verdicts="
                )

        object.__setattr__(self, "verdicts", verdicts_tuple)

    @property
    def blocking(self) -> Tuple[GuardrailVerdict, ...]:
        return tuple(
            v for v in self.verdicts
            if not v.passed and v.severity == GuardrailSeverity.BLOCKING
        )

    @property
    def passed(self) -> bool:
        return len(self.blocking) == 0

    def __bool__(self) -> bool:
        return self.passed

    def verdict_for(self, name: str) -> Optional[GuardrailVerdict]:
        """Return the verdict for ``name``, or None if not emitted."""
        for v in self.verdicts:
            if v.name == name:
                return v
        return None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "passed": self.passed,
            "verdicts": [v.to_dict() for v in self.verdicts],
            "blocking": [v.to_dict() for v in self.blocking],
        }

    def to_json(self, **kwargs: Any) -> str:
        return json.dumps(self.to_dict(), **kwargs)

    def checksum(self) -> str:
        payload = json.dumps(
            self.to_dict(), sort_keys=True, default=str,
        ).encode("utf-8")
        return hashlib.sha256(payload).hexdigest()

    def __repr__(self) -> str:
        return (
            f"GuardrailResult("
            f"passed={self.passed}, "
            f"verdicts={len(self.verdicts)}, "
            f"blocking={len(self.blocking)})"
        )


# =============================================================================
# Helpers
# =============================================================================

def _to_jsonable(obj: Any) -> Any:
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
    return repr(obj)


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _is_nonempty_str(v: Any) -> bool:
    return isinstance(v, str) and bool(v.strip())


def _is_hardware_kind(kind: Any) -> Optional[bool]:
    """
    Return True/False for recognized kinds, None for unrecognized.

    Handles string ("hardware", "Hardware", "HARDWARE") and enum
    (`BackendKind.HARDWARE`) uniformly.
    """
    if kind is None:
        return False
    if hasattr(kind, "value"):
        kind_str = str(kind.value).strip().lower()
    elif isinstance(kind, str):
        kind_str = kind.strip().lower()
    else:
        return None
    if kind_str == "hardware":
        return True
    if kind_str in ("simulator", "cloud_simulator", "emulator"):
        return False
    return None


def _truncate(s: str, max_len: int) -> str:
    if len(s) <= max_len:
        return s
    return s[: max(0, max_len - 1)] + "…"


# =============================================================================
# The Enhanced QuantumGuardrails
# =============================================================================

class QuantumGuardrails:
    """
    Pre-flight safety gates.

    See the module docstring for which checks run and how to configure
    them. The class is stateless and thread-safe; instantiate once per
    deployment.
    """

    def __init__(
        self,
        *,
        backend_allowlist: Optional[Sequence[str]] = None,
        simulator_first: bool = True,
        max_queue_seconds: float = DEFAULT_MAX_QUEUE_SECONDS,
        max_cost: float = DEFAULT_MAX_COST_USD,
        max_energy_kwh: Optional[float] = None,
        require_cost_budget: bool = True,
        require_energy_budget: bool = False,
        require_baseline: bool = True,
        warn_on_hardware: Optional[bool] = None,
        custom_checks: Optional[
            Sequence[Callable[[Any, Any], GuardrailVerdict]]
        ] = None,
        max_reason_len: int = DEFAULT_MAX_REASON_LEN,
        config: Optional[GuardrailsConfig] = None,
        metrics: Optional[MetricsRecorder] = None,
    ) -> None:
        if config is None:
            config = GuardrailsConfig(
                backend_allowlist=frozenset(backend_allowlist or ()),
                simulator_first=bool(simulator_first),
                warn_on_hardware=(
                    bool(warn_on_hardware) if warn_on_hardware is not None
                    else bool(simulator_first)
                ),
                max_queue_seconds=float(max_queue_seconds),
                max_cost_usd=float(max_cost),
                max_energy_kwh=(
                    float(max_energy_kwh)
                    if max_energy_kwh is not None else None
                ),
                require_cost_budget=bool(require_cost_budget),
                require_energy_budget=bool(require_energy_budget),
                require_baseline=bool(require_baseline),
                max_reason_len=int(max_reason_len),
                custom_checks=tuple(custom_checks or ()),
            )
        self.config = config
        self.backend_allowlist = config.backend_allowlist
        self.simulator_first = config.simulator_first
        self.max_queue_seconds = config.max_queue_seconds
        self.max_cost = config.max_cost_usd
        self._metrics = metrics or MetricsRecorder()
        self._started_mono = time.monotonic()

    # ------------------------------------------------------------------ check
    def check(
        self,
        *,
        task_spec: Any,
        request: Any,
        backend_kind: Optional[str] = None,
        backend_id: Optional[str] = None,
        at: Optional[datetime] = None,
    ) -> GuardrailResult:
        """
        Run every configured guardrail.

        Returns a ``GuardrailResult`` whose ``passed`` is True iff no
        blocking verdict failed. Bad inputs (``None`` task_spec or
        ``request``) produce blocking verdicts, not exceptions.
        """
        t0 = time.monotonic()
        now = at or _utcnow()

        verdicts: List[GuardrailVerdict] = []

        # --- Input validation as verdicts, not crashes ---
        if task_spec is None:
            verdicts.append(self._verdict(
                name="task_spec_provided",
                passed=False,
                reason="task_spec is None",
                severity=GuardrailSeverity.BLOCKING,
            ))
        if request is None:
            verdicts.append(self._verdict(
                name="request_provided",
                passed=False,
                reason="request is None",
                severity=GuardrailSeverity.BLOCKING,
            ))

        # --- Baseline ---
        if self.config.require_baseline:
            verdicts.append(self._check_baseline(task_spec))
        else:
            verdicts.append(self._skipped(
                "classical_baseline_declared",
                "baseline check disabled by configuration",
            ))

        # --- Quality threshold ---
        verdicts.append(self._check_quality(request))

        # --- Queue budget ---
        verdicts.append(self._check_queue(request))

        # --- Cost budget ---
        verdicts.append(self._check_cost(request))

        # --- Energy budget ---
        if self.config.require_energy_budget \
                or self.config.max_energy_kwh is not None:
            verdicts.append(self._check_energy(request))
        else:
            verdicts.append(self._skipped(
                "energy_budget_within_limit",
                "energy check disabled by configuration",
            ))

        # --- Allowlist ---
        if self.config.backend_allowlist:
            verdicts.append(self._check_allowlist(backend_id))
        else:
            verdicts.append(self._skipped(
                "backend_allowlisted",
                "allowlist check disabled (no allowlist configured)",
            ))

        # --- Simulator-first advisory ---
        if self.config.warn_on_hardware:
            verdicts.append(self._check_simulator_first(backend_kind))
        else:
            verdicts.append(self._skipped(
                "simulator_first",
                "simulator-first advisory disabled",
            ))

        # --- Task spec validation ---
        verdicts.append(self._check_task_spec(task_spec, at=now))

        # --- Request validation ---
        verdicts.append(self._check_request(request, at=now))

        # --- Custom checks ---
        for i, fn in enumerate(self.config.custom_checks):
            try:
                custom = fn(task_spec, request)
            except Exception as exc:
                logger.warning(
                    "custom guardrail %d raised", i, exc_info=True,
                )
                custom = self._verdict(
                    name=f"custom_{i}",
                    passed=False,
                    reason=(
                        f"custom check raised "
                        f"{type(exc).__name__}: {exc}"
                    ),
                    severity=GuardrailSeverity.WARNING,
                )
            if custom is not None:
                verdicts.append(custom)

        result = GuardrailResult(verdicts=tuple(verdicts))
        self._record(result, elapsed_s=time.monotonic() - t0)
        return result

    # ------------------------------------------------------------------ individual checks
    def _check_baseline(self, task_spec: Any) -> GuardrailVerdict:
        if task_spec is None:
            return self._verdict(
                name="classical_baseline_declared",
                passed=False,
                reason="task_spec is None",
                severity=GuardrailSeverity.BLOCKING,
            )

        # Enhanced accessors first.
        has_baseline = getattr(task_spec, "has_baseline", None)
        if has_baseline is not None:
            passed = bool(has_baseline)
            return self._verdict(
                name="classical_baseline_declared",
                passed=passed,
                reason=(
                    "task_spec.has_baseline is True"
                    if passed else
                    "task_spec.has_baseline is False"
                ),
                severity=GuardrailSeverity.BLOCKING,
            )

        baseline_obj = getattr(task_spec, "baseline", None)
        if baseline_obj is not None:
            return self._verdict(
                name="classical_baseline_declared",
                passed=True,
                reason=f"baseline record present: {baseline_obj!r}",
                severity=GuardrailSeverity.BLOCKING,
            )

        # Legacy fallback: `classical_baseline` string.
        legacy = getattr(task_spec, "classical_baseline", None)
        passed = _is_nonempty_str(legacy)
        return self._verdict(
            name="classical_baseline_declared",
            passed=passed,
            reason=(
                f"classical_baseline = {legacy!r}"
                if passed else
                "no classical baseline declared"
            ),
            severity=GuardrailSeverity.BLOCKING,
        )

    def _check_quality(self, request: Any) -> GuardrailVerdict:
        if request is None:
            return self._verdict(
                name="quality_threshold_set",
                passed=False,
                reason="request is None",
                severity=GuardrailSeverity.BLOCKING,
            )
        min_quality = getattr(request, "min_quality", None)
        if not isinstance(min_quality, (int, float)) \
                or isinstance(min_quality, bool):
            return self._verdict(
                name="quality_threshold_set",
                passed=False,
                reason=(
                    f"min_quality must be a number; "
                    f"got {type(min_quality).__name__}"
                ),
                severity=GuardrailSeverity.BLOCKING,
            )
        passed = float(min_quality) > 0.0
        return self._verdict(
            name="quality_threshold_set",
            passed=passed,
            reason=(
                f"min_quality = {min_quality}"
                if passed else
                "min_quality must be > 0"
            ),
            severity=GuardrailSeverity.BLOCKING,
        )

    def _check_queue(self, request: Any) -> GuardrailVerdict:
        if request is None:
            return self._verdict(
                name="queue_budget_within_limit",
                passed=False,
                reason="request is None",
                severity=GuardrailSeverity.BLOCKING,
            )
        raw = getattr(request, "max_queue_seconds", None)
        if not isinstance(raw, (int, float)) or isinstance(raw, bool):
            return self._verdict(
                name="queue_budget_within_limit",
                passed=False,
                reason=(
                    f"max_queue_seconds must be a number; "
                    f"got {type(raw).__name__}"
                ),
                severity=GuardrailSeverity.BLOCKING,
            )
        limit = self.config.max_queue_seconds
        passed = float(raw) <= limit
        return self._verdict(
            name="queue_budget_within_limit",
            passed=passed,
            reason=f"max_queue_seconds={raw} vs limit={limit}",
            severity=GuardrailSeverity.BLOCKING,
        )

    def _check_cost(self, request: Any) -> GuardrailVerdict:
        if request is None:
            return self._verdict(
                name="cost_budget_within_limit",
                passed=False,
                reason="request is None",
                severity=GuardrailSeverity.BLOCKING,
            )

        # Enhanced contract: `max_cost_usd` where None means "unlimited".
        has_enhanced = hasattr(request, "max_cost_usd")
        if has_enhanced:
            raw = getattr(request, "max_cost_usd", None)
            if raw is None:
                if not self.config.require_cost_budget:
                    return self._verdict(
                        name="cost_budget_within_limit",
                        passed=True,
                        reason=(
                            "no cost budget declared "
                            "(require_cost_budget=False)"
                        ),
                        severity=GuardrailSeverity.BLOCKING,
                    )
                return self._verdict(
                    name="cost_budget_within_limit",
                    passed=False,
                    reason=(
                        "no cost budget declared; safety gate "
                        "requires an explicit max_cost_usd"
                    ),
                    severity=GuardrailSeverity.BLOCKING,
                )
            if not isinstance(raw, (int, float)) or isinstance(raw, bool):
                return self._verdict(
                    name="cost_budget_within_limit",
                    passed=False,
                    reason=(
                        f"max_cost_usd must be a number or None; "
                        f"got {type(raw).__name__}"
                    ),
                    severity=GuardrailSeverity.BLOCKING,
                )
            passed = float(raw) <= self.config.max_cost_usd
            return self._verdict(
                name="cost_budget_within_limit",
                passed=passed,
                reason=(
                    f"max_cost_usd={raw} vs limit="
                    f"{self.config.max_cost_usd}"
                ),
                severity=GuardrailSeverity.BLOCKING,
            )

        # Legacy request: `max_cost` where 0.0 (or negative) means unlimited.
        raw = getattr(request, "max_cost", None)
        if not isinstance(raw, (int, float)) or isinstance(raw, bool):
            return self._verdict(
                name="cost_budget_within_limit",
                passed=False,
                reason=(
                    f"max_cost must be a number; "
                    f"got {type(raw).__name__}"
                ),
                severity=GuardrailSeverity.BLOCKING,
            )
        if float(raw) <= 0.0:
            if not self.config.require_cost_budget:
                return self._verdict(
                    name="cost_budget_within_limit",
                    passed=True,
                    reason=(
                        "no cost budget declared "
                        "(require_cost_budget=False)"
                    ),
                    severity=GuardrailSeverity.BLOCKING,
                )
            return self._verdict(
                name="cost_budget_within_limit",
                passed=False,
                reason=(
                    "max_cost is 0 or negative (unbudgeted); safety "
                    "gate requires an explicit positive max_cost"
                ),
                severity=GuardrailSeverity.BLOCKING,
            )
        passed = float(raw) <= self.config.max_cost_usd
        return self._verdict(
            name="cost_budget_within_limit",
            passed=passed,
            reason=(
                f"max_cost={raw} vs limit={self.config.max_cost_usd}"
            ),
            severity=GuardrailSeverity.BLOCKING,
        )

    def _check_energy(self, request: Any) -> GuardrailVerdict:
        if request is None:
            return self._verdict(
                name="energy_budget_within_limit",
                passed=False,
                reason="request is None",
                severity=GuardrailSeverity.BLOCKING,
            )
        raw = getattr(request, "max_energy_kwh", None)
        limit = self.config.max_energy_kwh
        if raw is None:
            if not self.config.require_energy_budget:
                return self._skipped(
                    "energy_budget_within_limit",
                    "no energy budget declared; check not required",
                )
            return self._verdict(
                name="energy_budget_within_limit",
                passed=False,
                reason=(
                    "no energy budget declared; require_energy_budget=True"
                ),
                severity=GuardrailSeverity.BLOCKING,
            )
        if not isinstance(raw, (int, float)) or isinstance(raw, bool):
            return self._verdict(
                name="energy_budget_within_limit",
                passed=False,
                reason=(
                    f"max_energy_kwh must be a number; "
                    f"got {type(raw).__name__}"
                ),
                severity=GuardrailSeverity.BLOCKING,
            )
        if limit is None:
            # No org cap; any non-negative budget is within budget.
            passed = float(raw) >= 0.0
            return self._verdict(
                name="energy_budget_within_limit",
                passed=passed,
                reason=(
                    f"max_energy_kwh={raw} (no org cap configured)"
                ),
                severity=GuardrailSeverity.BLOCKING,
            )
        passed = float(raw) <= limit
        return self._verdict(
            name="energy_budget_within_limit",
            passed=passed,
            reason=f"max_energy_kwh={raw} vs limit={limit}",
            severity=GuardrailSeverity.BLOCKING,
        )

    def _check_allowlist(self, backend_id: Optional[str]) -> GuardrailVerdict:
        # Distinguish "backend_id missing" from "not in allowlist".
        if backend_id is None:
            return self._verdict(
                name="backend_allowlisted",
                passed=False,
                reason=(
                    "backend_id is required when an allowlist is "
                    "configured"
                ),
                severity=GuardrailSeverity.BLOCKING,
            )
        if not isinstance(backend_id, str) or not backend_id.strip():
            return self._verdict(
                name="backend_allowlisted",
                passed=False,
                reason="backend_id is empty",
                severity=GuardrailSeverity.BLOCKING,
            )
        passed = backend_id in self.config.backend_allowlist
        return self._verdict(
            name="backend_allowlisted",
            passed=passed,
            reason=(
                f"backend_id={backend_id!r} is in allowlist"
                if passed else
                f"backend_id={backend_id!r} not in allowlist "
                f"{sorted(self.config.backend_allowlist)}"
            ),
            severity=GuardrailSeverity.BLOCKING,
        )

    def _check_simulator_first(
        self, backend_kind: Optional[str],
    ) -> GuardrailVerdict:
        is_hw = _is_hardware_kind(backend_kind)
        if is_hw is None:
            logger.warning(
                "unrecognized backend_kind; treating as non-hardware",
                extra={"backend_kind": str(backend_kind)},
            )
            return self._verdict(
                name="simulator_first",
                passed=False,
                reason=(
                    f"backend_kind={backend_kind!r} is unrecognized; "
                    f"cannot confirm it is not hardware"
                ),
                severity=GuardrailSeverity.WARNING,
            )
        passed = not is_hw
        return self._verdict(
            name="simulator_first",
            passed=passed,
            reason=(
                f"backend_kind={backend_kind!r}; "
                + (
                    "prefer simulator for development"
                    if is_hw else
                    "simulator (advisory check passed)"
                )
            ),
            severity=GuardrailSeverity.WARNING,
        )

    def _check_task_spec(
        self, task_spec: Any, *, at: datetime,
    ) -> GuardrailVerdict:
        if task_spec is None:
            return self._verdict(
                name="task_spec_valid",
                passed=False,
                reason="task_spec is None",
                severity=GuardrailSeverity.BLOCKING,
            )

        # Preferred: `is_routable(at=...)`.
        is_routable = getattr(task_spec, "is_routable", None)
        if callable(is_routable):
            try:
                rb = is_routable(
                    require_baseline=self.config.require_baseline,
                    at=at,
                )
            except TypeError:
                # Older signature without `at`.
                try:
                    rb = is_routable(
                        require_baseline=self.config.require_baseline,
                    )
                except Exception as exc:
                    return self._verdict(
                        name="task_spec_valid",
                        passed=False,
                        reason=(
                            f"is_routable() raised "
                            f"{type(exc).__name__}: {exc}"
                        ),
                        severity=GuardrailSeverity.BLOCKING,
                    )
            except Exception as exc:
                return self._verdict(
                    name="task_spec_valid",
                    passed=False,
                    reason=(
                        f"is_routable() raised "
                        f"{type(exc).__name__}: {exc}"
                    ),
                    severity=GuardrailSeverity.BLOCKING,
                )
            if bool(rb):
                return self._verdict(
                    name="task_spec_valid",
                    passed=True,
                    reason="task spec routable",
                    severity=GuardrailSeverity.BLOCKING,
                )
            reason = (
                "; ".join(getattr(rb, "failures", ()))
                or str(getattr(rb, "reason", "not routable"))
            )
            return self._verdict(
                name="task_spec_valid",
                passed=False,
                reason=reason,
                severity=GuardrailSeverity.BLOCKING,
            )

        # Fallback: legacy `validate()`.
        validate = getattr(task_spec, "validate", None)
        if not callable(validate):
            return self._verdict(
                name="task_spec_valid",
                passed=False,
                reason=(
                    "task_spec has neither is_routable() nor "
                    "validate()"
                ),
                severity=GuardrailSeverity.BLOCKING,
            )
        try:
            errs = validate()
        except Exception as exc:
            return self._verdict(
                name="task_spec_valid",
                passed=False,
                reason=(
                    f"task_spec.validate() raised "
                    f"{type(exc).__name__}: {exc}"
                ),
                severity=GuardrailSeverity.BLOCKING,
            )
        passed = not errs
        return self._verdict(
            name="task_spec_valid",
            passed=passed,
            reason=(
                "; ".join(str(e) for e in errs)
                if errs else "task spec valid"
            ),
            severity=GuardrailSeverity.BLOCKING,
        )

    def _check_request(
        self, request: Any, *, at: datetime,
    ) -> GuardrailVerdict:
        if request is None:
            return self._verdict(
                name="request_valid",
                passed=False,
                reason="request is None",
                severity=GuardrailSeverity.BLOCKING,
            )

        # Preferred: `validate_for_dispatch(at=...)`.
        validate_for_dispatch = getattr(
            request, "validate_for_dispatch", None,
        )
        if callable(validate_for_dispatch):
            try:
                rd = validate_for_dispatch(at=at)
            except TypeError:
                try:
                    rd = validate_for_dispatch()
                except Exception as exc:
                    return self._verdict(
                        name="request_valid",
                        passed=False,
                        reason=(
                            f"validate_for_dispatch() raised "
                            f"{type(exc).__name__}: {exc}"
                        ),
                        severity=GuardrailSeverity.BLOCKING,
                    )
            except Exception as exc:
                return self._verdict(
                    name="request_valid",
                    passed=False,
                    reason=(
                        f"validate_for_dispatch() raised "
                        f"{type(exc).__name__}: {exc}"
                    ),
                    severity=GuardrailSeverity.BLOCKING,
                )
            if bool(rd):
                return self._verdict(
                    name="request_valid",
                    passed=True,
                    reason="request dispatchable",
                    severity=GuardrailSeverity.BLOCKING,
                )
            # Enhanced result: `.to_list()` or `.violations`.
            to_list = getattr(rd, "to_list", None)
            if callable(to_list):
                reason = "; ".join(str(x) for x in to_list())
            else:
                violations = getattr(rd, "violations", ())
                reason = "; ".join(str(v) for v in violations) or (
                    "request not dispatchable"
                )
            return self._verdict(
                name="request_valid",
                passed=False,
                reason=reason,
                severity=GuardrailSeverity.BLOCKING,
            )

        # Fallback: legacy `validate()`.
        validate = getattr(request, "validate", None)
        if not callable(validate):
            return self._verdict(
                name="request_valid",
                passed=False,
                reason=(
                    "request has neither validate_for_dispatch() nor "
                    "validate()"
                ),
                severity=GuardrailSeverity.BLOCKING,
            )
        try:
            errs = validate()
        except Exception as exc:
            return self._verdict(
                name="request_valid",
                passed=False,
                reason=(
                    f"request.validate() raised "
                    f"{type(exc).__name__}: {exc}"
                ),
                severity=GuardrailSeverity.BLOCKING,
            )
        passed = not errs
        return self._verdict(
            name="request_valid",
            passed=passed,
            reason=(
                "; ".join(str(e) for e in errs)
                if errs else "request valid"
            ),
            severity=GuardrailSeverity.BLOCKING,
        )

    # ------------------------------------------------------------------ helpers
    def _verdict(
        self,
        *,
        name: str,
        passed: bool,
        reason: str,
        severity: GuardrailSeverity,
        skipped: bool = False,
    ) -> GuardrailVerdict:
        return GuardrailVerdict(
            name=name,
            passed=passed,
            reason=_truncate(reason, self.config.max_reason_len),
            severity=severity,
            skipped=skipped,
        )

    def _skipped(self, name: str, reason: str) -> GuardrailVerdict:
        return self._verdict(
            name=name,
            passed=True,
            reason=reason,
            severity=GuardrailSeverity.INFO,
            skipped=True,
        )

    def _record(self, result: GuardrailResult, *, elapsed_s: float) -> None:
        self._metrics.inc(
            "guardrail_calls_total",
            passed=str(result.passed).lower(),
        )
        self._metrics.observe("guardrail_duration_s", elapsed_s)
        for v in result.verdicts:
            self._metrics.inc(
                "guardrail_verdicts_total",
                name=v.name,
                passed=str(v.passed).lower(),
                severity=v.severity.value,
                skipped=str(v.skipped).lower(),
            )
        if not result.passed:
            logger.info(
                "guardrails blocked job",
                extra={
                    "blocking": [
                        {"name": v.name, "reason": v.reason}
                        for v in result.blocking
                    ],
                },
            )

    # ------------------------------------------------------------------ observability
    def health(self) -> Dict[str, Any]:
        return {
            "ready": True,
            "config": self.config.to_dict(),
            "uptime_seconds": time.monotonic() - self._started_mono,
            "metrics": self._metrics.snapshot(),
        }


# =============================================================================
# Public API
# =============================================================================

__all__: Tuple[str, ...] = (
    "DEFAULT_MAX_COST_USD",
    "DEFAULT_MAX_ENERGY_KWH",
    "DEFAULT_MAX_QUEUE_SECONDS",
    "DEFAULT_MAX_REASON_LEN",
    "GuardrailResult",
    "GuardrailSeverity",
    "GuardrailVerdict",
    "GuardrailsConfig",
    "GuardrailsError",
    "GuardrailsInputError",
    "MetricsRecorder",
    "QuantumGuardrails",
)


# =============================================================================
# Demo: python -m qcs.safety.quantum_guardrails
# =============================================================================

if __name__ == "__main__":  # pragma: no cover
    import json
    from datetime import datetime, timedelta, timezone

    logging.basicConfig(level=logging.INFO)

    # --- Mock contracts standing in for the enhanced versions ---

    class _MockBaseline:
        method = "scipy_minimize"
        quality = 0.85

    class _MockTaskSpecGood:
        problem_family = "optimization"
        @property
        def has_baseline(self): return True
        @property
        def baseline(self): return _MockBaseline()
        def is_routable(self, *, require_baseline=True, at=None):
            class RB:
                def __bool__(self): return True
                reason = "ok"
                failures = ()
            return RB()

    class _MockTaskSpecNoBaseline:
        problem_family = "optimization"
        @property
        def has_baseline(self): return False
        @property
        def baseline(self): return None
        def is_routable(self, *, require_baseline=True, at=None):
            class RB:
                def __bool__(self): return not require_baseline
                reason = "missing_classical_baseline"
                failures = ("missing_classical_baseline",)
            return RB()

    class _MockTaskSpecExpired:
        problem_family = "optimization"
        @property
        def has_baseline(self): return True
        @property
        def baseline(self): return _MockBaseline()
        def is_routable(self, *, require_baseline=True, at=None):
            class RB:
                def __bool__(self): return False
                reason = "deadline_exceeded"
                failures = ("deadline_exceeded",)
            return RB()

    class _MockRequestGood:
        min_quality = 0.9
        max_queue_seconds = 60.0
        max_cost_usd = 5.0
        max_energy_kwh = 0.5
        def validate_for_dispatch(self, at=None):
            class R:
                def __bool__(self): return True
                violations = ()
                def to_list(self): return []
            return R()

    class _MockRequestUnlimited:
        min_quality = 0.9
        max_queue_seconds = 60.0
        max_cost_usd = None       # ← unlimited; should be blocked
        max_energy_kwh = 0.5
        def validate_for_dispatch(self, at=None):
            class R:
                def __bool__(self): return True
                violations = ()
                def to_list(self): return []
            return R()

    class _MockRequestExpired:
        min_quality = 0.9
        max_queue_seconds = 60.0
        max_cost_usd = 5.0
        max_energy_kwh = 0.5
        def validate_for_dispatch(self, at=None):
            class R:
                def __bool__(self): return False
                violations = ("expired",)
                def to_list(self): return ["expired"]
            return R()

    class _MockLegacyRequest:
        min_quality = 0.9
        max_queue_seconds = 60.0
        max_cost = 0.0     # ← legacy "unlimited"
        def validate(self): return []

    print("=== Happy path ===")
    g = QuantumGuardrails()
    r = g.check(
        task_spec=_MockTaskSpecGood(),
        request=_MockRequestGood(),
        backend_kind="simulator",
        backend_id="sim-A",
    )
    print(f"  {r!r}")
    for v in r.verdicts:
        status = "SKIP" if v.skipped else ("OK" if v.passed else "FAIL")
        print(f"    {v.name:<32} {status:<4} {v.severity.value}")
    assert r.passed

    print("\n=== §1.1 Unlimited cost budget is blocked by default ===")
    r = g.check(
        task_spec=_MockTaskSpecGood(),
        request=_MockRequestUnlimited(),
    )
    cost_verdict = r.verdict_for("cost_budget_within_limit")
    print(f"  cost verdict: passed={cost_verdict.passed}, "
          f"reason={cost_verdict.reason}")
    assert not cost_verdict.passed
    assert not r.passed

    print("\n=== §1.1 require_cost_budget=False allows unlimited ===")
    g_lenient = QuantumGuardrails(require_cost_budget=False)
    r = g_lenient.check(
        task_spec=_MockTaskSpecGood(),
        request=_MockRequestUnlimited(),
    )
    cost_verdict = r.verdict_for("cost_budget_within_limit")
    print(f"  cost verdict: passed={cost_verdict.passed}, "
          f"reason={cost_verdict.reason}")
    assert cost_verdict.passed

    print("\n=== §1.1 Legacy request (max_cost=0.0) is blocked ===")
    r = g.check(
        task_spec=_MockTaskSpecGood(),
        request=_MockLegacyRequest(),
    )
    cost_verdict = r.verdict_for("cost_budget_within_limit")
    print(f"  cost verdict: passed={cost_verdict.passed}")
    assert not cost_verdict.passed

    print("\n=== §1.2 Severity is typed ===")
    for sev in ("blocking", "warning", "info", GuardrailSeverity.BLOCKING):
        v = GuardrailVerdict(
            name="x", passed=True, reason="", severity=sev,
        )
        print(f"  severity={sev!r:>30} → {v.severity.value}")
    try:
        GuardrailVerdict(
            name="x", passed=True, reason="", severity="blockng",
        )
    except GuardrailsInputError as e:
        print(f"  typo severity rejected: {type(e).__name__}")

    print("\n=== §1.3 Missing baseline blocked ===")
    r = g.check(
        task_spec=_MockTaskSpecNoBaseline(),
        request=_MockRequestGood(),
    )
    b = r.verdict_for("classical_baseline_declared")
    print(f"  baseline verdict: passed={b.passed}")
    assert not b.passed

    print("\n=== §1.3 require_baseline=False skips the check ===")
    g_nb = QuantumGuardrails(require_baseline=False)
    r = g_nb.check(
        task_spec=_MockTaskSpecNoBaseline(),
        request=_MockRequestGood(),
    )
    b = r.verdict_for("classical_baseline_declared")
    print(f"  baseline verdict: passed={b.passed}, skipped={b.skipped}")
    assert b.passed and b.skipped

    print("\n=== §1.4 Expired request is blocked ===")
    r = g.check(
        task_spec=_MockTaskSpecGood(),
        request=_MockRequestExpired(),
    )
    req_v = r.verdict_for("request_valid")
    print(f"  request verdict: passed={req_v.passed}, "
          f"reason={req_v.reason}")
    assert not req_v.passed

    print("\n=== §1.4 Past-deadline task spec is blocked ===")
    r = g.check(
        task_spec=_MockTaskSpecExpired(),
        request=_MockRequestGood(),
    )
    spec_v = r.verdict_for("task_spec_valid")
    print(f"  spec verdict: passed={spec_v.passed}, "
          f"reason={spec_v.reason}")
    assert not spec_v.passed

    print("\n=== §1.5 passed derived from verdicts ===")
    try:
        GuardrailResult(
            verdicts=(GuardrailVerdict(
                name="x", passed=False, reason="fail",
                severity=GuardrailSeverity.BLOCKING,
            ),),
            passed=True,
        )
    except ValueError as e:
        print(f"  contradictory result rejected: {str(e)[:50]}…")

    print("\n=== §1.6 Energy budget checked (opt-in) ===")
    g_en = QuantumGuardrails(
        require_energy_budget=True, max_energy_kwh=0.1,
    )
    r = g_en.check(
        task_spec=_MockTaskSpecGood(),
        request=_MockRequestGood(),  # max_energy_kwh=0.5 > 0.1
    )
    en = r.verdict_for("energy_budget_within_limit")
    print(f"  energy verdict: passed={en.passed}, reason={en.reason}")
    assert not en.passed

    print("\n=== §2.1 Allowlist validation ===")
    try:
        QuantumGuardrails(backend_allowlist=[""])
    except GuardrailsInputError as e:
        print(f"  empty allowlist entry rejected: {type(e).__name__}")

    print("\n=== §2.1 Allowlist with backend_id=None ===")
    g_al = QuantumGuardrails(backend_allowlist=["sim-A", "sim-B"])
    r = g_al.check(
        task_spec=_MockTaskSpecGood(),
        request=_MockRequestGood(),
    )
    al = r.verdict_for("backend_allowlisted")
    print(f"  allowlist verdict: passed={al.passed}, reason={al.reason}")
    assert not al.passed

    print("\n=== §2.2 Constructor validation ===")
    for kwargs in [
        dict(max_queue_seconds=float("nan")),
        dict(max_cost=float("inf")),
        dict(max_energy_kwh=-1.0),
        dict(max_reason_len=0),
    ]:
        try:
            QuantumGuardrails(**kwargs)
            print(f"  {kwargs}: NOT rejected")
        except GuardrailsInputError:
            print(f"  {kwargs}: rejected")

    print("\n=== §2.3 Immutability ===")
    r = g.check(
        task_spec=_MockTaskSpecGood(),
        request=_MockRequestGood(),
    )
    try:
        r.verdicts = ()  # type: ignore[misc]
    except Exception as e:
        print(f"  result.verdicts mutation → {type(e).__name__}")
    try:
        r.verdicts[0].passed = False  # type: ignore[misc]
    except Exception as e:
        print(f"  verdict.passed mutation → {type(e).__name__}")

    print("\n=== §2.4 backend_kind case-insensitivity ===")
    for kind in ("hardware", "Hardware", "HARDWARE"):
        r = g.check(
            task_spec=_MockTaskSpecGood(),
            request=_MockRequestGood(),
            backend_kind=kind,
        )
        sf = r.verdict_for("simulator_first")
        print(f"  kind={kind!r:>10}: passed={sf.passed}")

    print("\n=== §2.5 Bad inputs report as verdicts, not crashes ===")
    r = g.check(task_spec=None, request=None)
    print(f"  {r!r}")
    for v in r.verdicts[:2]:
        print(f"    {v.name}: {v.reason}")
    assert not r.passed

    print("\n=== §2.7 Skipped verdicts are visible ===")
    g_no_allowlist = QuantumGuardrails()   # no allowlist, no energy
    r = g_no_allowlist.check(
        task_spec=_MockTaskSpecGood(),
        request=_MockRequestGood(),
    )
    al = r.verdict_for("backend_allowlisted")
    print(f"  allowlist verdict: skipped={al.skipped}, "
          f"severity={al.severity.value}")
    assert al.skipped
    assert al.severity == GuardrailSeverity.INFO

    print("\n=== §3.3 checksum ===")
    r2 = g.check(
        task_spec=_MockTaskSpecGood(),
        request=_MockRequestGood(),
    )
    print(f"  checksum: {r2.checksum()[:16]}…")

    print("\n=== §3.4 Truncated reason ===")
    g_short = QuantumGuardrails(max_reason_len=20)
    r = g_short.check(
        task_spec=_MockTaskSpecNoBaseline(),
        request=_MockRequestGood(),
    )
    b = r.verdict_for("classical_baseline_declared")
    print(f"  reason (truncated): {b.reason}")
    assert len(b.reason) <= 20

    print("\n=== §3.7 Custom checks ===")
    def my_check(task_spec, request):
        return GuardrailVerdict(
            name="my_custom_check",
            passed=True,
            reason="custom check passed",
            severity=GuardrailSeverity.INFO,
        )
    def failing_check(task_spec, request):
        return GuardrailVerdict(
            name="my_failing_check",
            passed=False,
            reason="custom failure",
            severity=GuardrailSeverity.BLOCKING,
        )
    g_custom = QuantumGuardrails(custom_checks=(my_check, failing_check))
    r = g_custom.check(
        task_spec=_MockTaskSpecGood(),
        request=_MockRequestGood(),
    )
    print(f"  total verdicts: {len(r.verdicts)}")
    print(f"  has my_custom_check: "
          f"{r.verdict_for('my_custom_check') is not None}")
    print(f"  has my_failing_check: "
          f"{r.verdict_for('my_failing_check') is not None}")
    assert not r.passed  # failing_check blocks

    print("\n=== §3.3 JSON-safe serialization ===")
    d = r.to_dict()
    print(f"  JSON length: {len(json.dumps(d))} bytes")

    print("\n=== §3.5 Health ===")
    print(f"  ready: {g.health()['ready']}")

    print(f"\n=== __version__ === {__version__}")
    print(f"=== __all__ === {__all__}")
    print("\nSmoke test passed.")
