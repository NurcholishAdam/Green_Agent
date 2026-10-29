"""
BenchmarkEngine — governance-side benchmark orchestration. — v2.0.0

Defines task eligibility, evaluation rules, reproducibility
requirements, anti-gaming checks, and acceptance thresholds.

v2.0.0 changes vs. v1.0.0
-------------------------
**P0 bug fixes**
  * 2.1 ``float('nan')`` / ``float('inf')`` in metrics no longer pass
        thresholds. Previously ``nan < threshold`` and ``nan > x``
        both evaluate to False, so a ``NaN`` metric silently passed
        every acceptance check.
  * 2.2 ``bool`` in metrics is rejected. Previously ``True`` was
        treated as ``1.0`` by ``isinstance(x, (int, float))``.
  * 2.3 ``anti_gaming_checks`` are now **dispatched**. Previously
        the spec declared checks that ``submit()`` never read — a
        fabricated submission passed if it met the thresholds.
  * 2.4 ``reproducibility_rules`` are now **enforced**. Previously
        the spec declared required fields that ``submit()`` never
        checked against the payload.
  * 2.5 Structured ``Threshold`` dataclass with explicit
        ``direction=`` (``"min"``/``"max"``). Numeric thresholds are
        still accepted for backward compat (sign-based convention),
        but the resolved direction is stored unambiguously.
  * 2.6 ``metrics`` is validated as a ``Mapping`` at the top of
        ``submit()``. Previously a non-mapping argument raised
        ``AttributeError`` on ``metrics.get()``.
  * 2.7 ``metrics`` is deep-copied into the result. Previously a
        shallow ``dict(metrics)`` shared nested state with the caller.
  * 2.8 ``BenchmarkResult`` is ``frozen=True``. The docstring
        promised "immutable"; it now is.
  * 2.9 ``BenchmarkSpec.__post_init__`` validates every field
        (non-empty ids, list-of-strings, mapping of thresholds,
        non-empty anti-gaming / reproducibility).
  * 2.10 ``task_type`` matching is case-insensitive and whitespace
         stripped. Previously ``"INFERENCE"`` was rejected by
         ``task_eligibility=["inference"]``.
  * 2.11 ``run_id`` / ``system_version`` / ``policy_version`` are
         validated as non-empty strings.
  * 2.12 ``BenchmarkResult.__post_init__`` validates every field.
         Previously corrupted results (bool-as-passed, naive
         datetime, empty ids) propagated through the system.
  * 2.13 ``BenchmarkEngineError`` base class +
         ``BenchmarkValidationError`` +
         ``BenchmarkAntiGamingError``. Add to
         ``governance/__init__.py``'s ``_ERROR_CLASSES_BY_MODULE``
         for ``except GovernanceError:`` compatibility.
  * 2.14 ``metric_definitions`` enforced: unknown submitted metrics
         are rejected, and defined metrics may be required to be
         present (``require_all_definitions`` config).
  * 2.15 ``BenchmarkResult.to_dict`` emits ISO 8601 for
         ``submitted_at`` — ``json.dumps`` succeeds (was previously
         blocked by ``asdict``'s raw datetime).
  * 2.16 Structured ``RejectionCode`` enum on every rejection — a
         dashboard can group by code without string matching.

**P1 robustness**
  * 3.1 ``BenchmarkResult.simulated`` / ``.source`` — provenance
        (was previously indistinguishable from a real measurement).
  * 3.2 ``metrics_hook(event, fields)`` on every submission.
  * 3.3 Optional ``audit_log`` integration — every decision is
        appended to the injected ``AuditLog`` (duck-typed).
  * 3.4 ``BenchmarkSpec.to_dict`` / ``from_legacy_dict`` — spec
        round-trip (audit trail of "what was required").
  * 3.5 ``__version__`` / ``LEDGER_SCHEMA_VERSION`` / ``__all__``.
  * 3.6 ``statistics()`` — roll-up counters and rejection breakdown.
  * 3.7 ``RLock`` around state.
  * 3.8 Bounded ``history()`` of submissions.
  * 3.9 ``get_result(run_id)`` lookup.
  * 3.10 Empty ``task_eligibility`` rejected at spec construction
         unless ``allow_any_task=True``.
  * 3.11 ``submit(..., dry_run=True)`` — preview without recording.
  * 3.12 Threshold finiteness validated at spec construction
         (``NaN`` threshold would otherwise always pass).
  * 3.13 ``MetricDefinition`` dataclass for structured access.

Backward compatible
-------------------
``BenchmarkSpec(benchmark_id, name, task_eligibility,
metric_definitions, anti_gaming_checks, acceptance_thresholds,
reproducibility_rules)``, ``BenchmarkEngine(spec)``,
``engine.submit(run_id=..., task_type=..., system_version=...,
policy_version=..., metrics=...)`` — every v1.0.0 call site works
unchanged. New kwargs are additive.

Behaviour changes (fixes for silent-corruption bugs)
---------------------------------------------------
  * ``NaN`` / ``Inf`` / ``bool`` metric values are rejected.
  * Anti-gaming checks declared in the spec are executed.
  * Reproducibility rules declared in the spec are enforced.
  * Duplicate ``snapshot`` no longer possible — n/a here.
  * Unknown metric names are rejected.
  * ``BenchmarkResult`` is frozen.
"""

from __future__ import annotations

import copy
import hashlib
import json
import logging
import math
import threading
import uuid
from collections import defaultdict, deque
from dataclasses import dataclass, field, fields, replace
from datetime import datetime, timezone
from enum import Enum
from typing import (
    Any, Callable, Deque, Dict, List, Mapping, Optional, Tuple,
)

logger = logging.getLogger(__name__)

__version__ = "2.0.0"
LEDGER_SCHEMA_VERSION = "5.0.0"


# =============================================================================
# Errors (P0 2.13)
# =============================================================================

class BenchmarkEngineError(ValueError):
    """Base class for every error raised by BenchmarkEngine."""


class BenchmarkValidationError(BenchmarkEngineError):
    """Raised for invalid spec, submission, or configuration."""


class BenchmarkAntiGamingError(BenchmarkEngineError):
    """Raised when a declared anti-gaming check is unknown."""


class BenchmarkDuplicateSubmissionError(BenchmarkEngineError):
    """Raised when a run_id has already been submitted."""


# =============================================================================
# Rejection codes (P0 2.16)
# =============================================================================

class RejectionCode(str, Enum):
    """Stable machine-readable rejection reasons."""
    TASK_NOT_ELIGIBLE = "task_not_eligible"
    METRIC_MISSING = "metric_missing"
    METRIC_NON_NUMERIC = "metric_non_numeric"
    METRIC_NON_FINITE = "metric_non_finite"
    METRIC_UNKNOWN = "metric_unknown"
    THRESHOLD_BELOW_MIN = "threshold_below_min"
    THRESHOLD_ABOVE_MAX = "threshold_above_max"
    ANTI_GAMING_FAILED = "anti_gaming_failed"
    REPRODUCIBILITY_MISSING = "reproducibility_missing"
    DEDUPLICATE = "duplicate_run_id"

    def __str__(self) -> str:
        return self.value


# =============================================================================
# Helpers
# =============================================================================

def _require_non_empty_string(name: str, value: Any) -> str:
    if not isinstance(value, str) or not value.strip():
        raise BenchmarkValidationError(
            f"{name} must be a non-empty string")
    return value.strip()


def _coerce_numeric(value: Any, *, name: str) -> float:
    """Coerce a metric value to a finite float (P0 2.1, 2.2)."""
    if isinstance(value, bool):
        raise BenchmarkValidationError(
            f"{name} must not be a bool")
    if not isinstance(value, (int, float)):
        raise BenchmarkValidationError(
            f"{name} must be numeric, got {type(value).__name__}")
    f = float(value)
    if math.isnan(f) or math.isinf(f):
        raise BenchmarkValidationError(
            f"{name} must be finite, got {value!r}")
    return f


def _normalize_task_type(value: Any) -> str:
    if not isinstance(value, str) or not value.strip():
        raise BenchmarkValidationError(
            "task_type must be a non-empty string")
    return value.strip().lower()


# =============================================================================
# Threshold (P0 2.5)
# =============================================================================

@dataclass(frozen=True)
class Threshold:
    """A single acceptance threshold with explicit direction."""
    value: float
    direction: str = "min"    # "min" | "max"

    def __post_init__(self) -> None:
        if isinstance(self.value, bool) or \
                not isinstance(self.value, (int, float)):
            raise BenchmarkValidationError(
                f"Threshold.value must be numeric, "
                f"got {type(self.value).__name__}")
        v = float(self.value)
        if math.isnan(v) or math.isinf(v):
            raise BenchmarkValidationError(
                f"Threshold.value must be finite, got {self.value!r}")
        object.__setattr__(self, "value", v)
        if self.direction not in ("min", "max"):
            raise BenchmarkValidationError(
                f"Threshold.direction must be 'min' or 'max', "
                f"got {self.direction!r}")

    @classmethod
    def coerce(cls, raw: Any) -> "Threshold":
        """Accept a numeric (v1.0.0 sign convention) or a Threshold."""
        if isinstance(raw, cls):
            return raw
        if isinstance(raw, bool):
            raise BenchmarkValidationError(
                "threshold must not be a bool")
        if isinstance(raw, (int, float)):
            # v1.0.0 convention: positive = minimum, negative = maximum.
            if raw >= 0:
                return cls(value=float(raw), direction="min")
            return cls(value=abs(float(raw)), direction="max")
        if isinstance(raw, Mapping):
            return cls(
                value=raw.get("value"),
                direction=raw.get("direction", "min"),
            )
        raise BenchmarkValidationError(
            f"threshold must be numeric or a Threshold, "
            f"got {type(raw).__name__}")

    def evaluate(self, metric_value: float) -> Optional[RejectionCode]:
        """Return a RejectionCode if the value fails the threshold."""
        if self.direction == "min" and metric_value < self.value:
            return RejectionCode.THRESHOLD_BELOW_MIN
        if self.direction == "max" and metric_value > self.value:
            return RejectionCode.THRESHOLD_ABOVE_MAX
        return None

    def to_dict(self) -> Dict[str, Any]:
        return {"value": self.value, "direction": self.direction}


# =============================================================================
# MetricDefinition (P1 3.13)
# =============================================================================

@dataclass(frozen=True)
class MetricDefinition:
    """Structured metric definition (was a bare string in v1.0.0)."""
    name: str
    unit: str = "unitless"
    description: str = ""

    def __post_init__(self) -> None:
        object.__setattr__(
            self, "name",
            _require_non_empty_string("name", self.name))
        if not isinstance(self.unit, str):
            raise BenchmarkValidationError(
                "unit must be a string")

    def to_dict(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "unit": self.unit,
            "description": self.description,
        }

    @classmethod
    def coerce(cls, raw: Any) -> "MetricDefinition":
        """Accept a MetricDefinition or a string (v1.0.0 name only)."""
        if isinstance(raw, cls):
            return raw
        if isinstance(raw, str):
            return cls(name=raw)
        if isinstance(raw, Mapping):
            return cls(
                name=raw.get("name"),
                unit=raw.get("unit", "unitless"),
                description=raw.get("description", ""),
            )
        raise BenchmarkValidationError(
            f"metric definition must be MetricDefinition, str, or "
            f"Mapping; got {type(raw).__name__}")


# =============================================================================
# BenchmarkSpec (P0 2.9; P1 3.4, 3.10, 3.12)
# =============================================================================

@dataclass(frozen=True)
class BenchmarkSpec:
    """
    Governance-approved benchmark specification.

    Frozen — the spec cannot drift after construction. Callers who
    want to modify a spec should build a new one.
    """
    benchmark_id: str
    name: str
    task_eligibility: List[str]
    metric_definitions: Dict[str, MetricDefinition]
    anti_gaming_checks: List[str]
    acceptance_thresholds: Dict[str, Threshold]
    reproducibility_rules: List[str]
    # P1: allow "every task type is eligible" as an explicit choice.
    allow_any_task: bool = False
    # P1: require every defined metric to appear in the submission.
    require_all_definitions: bool = False
    # P1: reject submissions with run_id already in history.
    deduplicate_runs: bool = True

    def __post_init__(self) -> None:
        object.__setattr__(
            self, "benchmark_id",
            _require_non_empty_string(
                "benchmark_id", self.benchmark_id))
        object.__setattr__(
            self, "name",
            _require_non_empty_string("name", self.name))

        # task_eligibility
        if not isinstance(self.task_eligibility, list):
            raise BenchmarkValidationError(
                "task_eligibility must be a list")
        if not self.task_eligibility and not self.allow_any_task:
            raise BenchmarkValidationError(
                "task_eligibility must not be empty unless "
                "allow_any_task=True; empty list rejects every "
                "submission")
        normalized: List[str] = []
        for i, t in enumerate(self.task_eligibility):
            if not isinstance(t, str) or not t.strip():
                raise BenchmarkValidationError(
                    f"task_eligibility[{i}] must be a non-empty "
                    f"string, got {t!r}")
            normalized.append(t.strip().lower())
        object.__setattr__(self, "task_eligibility", normalized)

        # metric_definitions — coerce any raw form to MetricDefinition
        if not isinstance(self.metric_definitions, Mapping):
            raise BenchmarkValidationError(
                "metric_definitions must be a mapping")
        coerced_defs: Dict[str, MetricDefinition] = {}
        for name, raw in self.metric_definitions.items():
            if not isinstance(name, str) or not name.strip():
                raise BenchmarkValidationError(
                    f"metric name {name!r} must be a non-empty string")
            md = MetricDefinition.coerce(raw)
            if md.name != name.strip():
                # The definition's own name must match the key.
                if md.name.strip() != name.strip():
                    raise BenchmarkValidationError(
                        f"metric_definitions[{name!r}].name="
                        f"{md.name!r} does not match the key")
            coerced_defs[name.strip()] = md
        object.__setattr__(self, "metric_definitions", coerced_defs)

        # anti_gaming_checks
        if not isinstance(self.anti_gaming_checks, list) or \
                not self.anti_gaming_checks:
            raise BenchmarkValidationError(
                "benchmark must declare anti-gaming checks "
                "(non-empty list)")
        for i, c in enumerate(self.anti_gaming_checks):
            if not isinstance(c, str) or not c.strip():
                raise BenchmarkValidationError(
                    f"anti_gaming_checks[{i}] must be a non-empty "
                    f"string, got {c!r}")
            if c.strip() not in _ANTI_GAMING_CHECKS:
                raise BenchmarkValidationError(
                    f"anti_gaming_checks[{i}]={c!r} is not a "
                    f"registered check; available: "
                    f"{sorted(_ANTI_GAMING_CHECKS)}")

        # acceptance_thresholds — coerce any numeric / mapping form
        if not isinstance(self.acceptance_thresholds, Mapping):
            raise BenchmarkValidationError(
                "acceptance_thresholds must be a mapping")
        coerced_th: Dict[str, Threshold] = {}
        for name, raw in self.acceptance_thresholds.items():
            if not isinstance(name, str) or not name.strip():
                raise BenchmarkValidationError(
                    f"threshold name {name!r} must be a non-empty "
                    f"string")
            coerced_th[name.strip()] = Threshold.coerce(raw)
        object.__setattr__(self, "acceptance_thresholds", coerced_th)

        # reproducibility_rules
        if not isinstance(self.reproducibility_rules, list) or \
                not self.reproducibility_rules:
            raise BenchmarkValidationError(
                "benchmark must declare reproducibility rules "
                "(non-empty list)")
        for i, r in enumerate(self.reproducibility_rules):
            if not isinstance(r, str) or not r.strip():
                raise BenchmarkValidationError(
                    f"reproducibility_rules[{i}] must be a "
                    f"non-empty string, got {r!r}")

    # ---------------------------------------------------------------- #
    def to_dict(self) -> Dict[str, Any]:
        """JSON-safe spec representation (P1 3.4)."""
        return {
            "version": __version__,
            "schema_version": LEDGER_SCHEMA_VERSION,
            "benchmark_id": self.benchmark_id,
            "name": self.name,
            "task_eligibility": list(self.task_eligibility),
            "metric_definitions": {
                k: v.to_dict()
                for k, v in self.metric_definitions.items()
            },
            "anti_gaming_checks": list(self.anti_gaming_checks),
            "acceptance_thresholds": {
                k: v.to_dict()
                for k, v in self.acceptance_thresholds.items()
            },
            "reproducibility_rules":
                list(self.reproducibility_rules),
            "allow_any_task": self.allow_any_task,
            "require_all_definitions": self.require_all_definitions,
            "deduplicate_runs": self.deduplicate_runs,
        }

    @classmethod
    def from_legacy_dict(
        cls, data: Mapping[str, Any],
    ) -> "BenchmarkSpec":
        """Build from a flat mapping; unknown keys ignored."""
        if not isinstance(data, Mapping):
            raise BenchmarkValidationError(
                "from_legacy_dict expects a Mapping")
        valid = {f.name for f in fields(cls)}
        kwargs = {k: v for k, v in data.items() if k in valid}
        return cls(**kwargs)


# =============================================================================
# BenchmarkResult (P0 2.8, 2.12, 2.15; P1 3.1)
# =============================================================================

@dataclass(frozen=True)
class BenchmarkResult:
    """
    Immutable record of a benchmark run.

    Frozen — the passed/rejected verdict cannot be flipped after the
    fact.
    """
    benchmark_id: str
    run_id: str
    system_version: str
    policy_version: str
    submitted_at: datetime = field(
        default_factory=lambda: datetime.now(timezone.utc))
    passed: bool = False
    rejection_reasons: List[str] = field(default_factory=list)
    rejection_codes: List[str] = field(default_factory=list)
    metrics: Dict[str, Any] = field(default_factory=dict)
    # P1 3.1
    simulated: bool = False
    source: Optional[str] = None

    def __post_init__(self) -> None:
        for name in ("benchmark_id", "run_id", "system_version",
                     "policy_version"):
            object.__setattr__(
                self, name,
                _require_non_empty_string(name, getattr(self, name)))
        if not isinstance(self.passed, bool):
            raise BenchmarkValidationError("passed must be a bool")
        if not isinstance(self.rejection_reasons, list):
            raise BenchmarkValidationError(
                "rejection_reasons must be a list")
        if not isinstance(self.rejection_codes, list):
            raise BenchmarkValidationError(
                "rejection_codes must be a list")
        if not isinstance(self.metrics, dict):
            raise BenchmarkValidationError("metrics must be a dict")
        if not isinstance(self.submitted_at, datetime):
            raise BenchmarkValidationError(
                "submitted_at must be a datetime")
        if not self.submitted_at.tzinfo:
            object.__setattr__(
                self, "submitted_at",
                self.submitted_at.replace(tzinfo=timezone.utc))
        else:
            object.__setattr__(
                self, "submitted_at",
                self.submitted_at.astimezone(timezone.utc))
        if not isinstance(self.simulated, bool):
            raise BenchmarkValidationError(
                "simulated must be a bool")
        if self.source is not None:
            object.__setattr__(
                self, "source",
                _require_non_empty_string("source", self.source).lower())

    def to_dict(self) -> Dict[str, Any]:
        """JSON-safe representation (P0 2.15)."""
        return {
            "benchmark_id": self.benchmark_id,
            "run_id": self.run_id,
            "system_version": self.system_version,
            "policy_version": self.policy_version,
            "submitted_at": self.submitted_at.isoformat(),
            "passed": self.passed,
            "rejection_reasons": list(self.rejection_reasons),
            "rejection_codes": list(self.rejection_codes),
            "metrics": _safe_json(self.metrics),
            "simulated": self.simulated,
            "source": self.source,
        }


def _safe_json(value: Any) -> Any:
    """Recursively coerce to JSON-safe form."""
    if value is None or isinstance(value, (bool, int, str)):
        return value
    if isinstance(value, float):
        if math.isnan(value) or math.isinf(value):
            return None
        return value
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, Mapping):
        return {str(k): _safe_json(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set, frozenset)):
        return [_safe_json(v) for v in value]
    return str(value)


# =============================================================================
# Anti-gaming check registry (P0 2.3)
# =============================================================================

_ANTI_GAMING_CHECKS: Dict[str, Callable] = {}


def _register_check(name: str):
    def deco(fn):
        _ANTI_GAMING_CHECKS[name] = fn
        return fn
    return deco


@_register_check("plausibility_bounds")
def _check_plausibility(
    metrics: Mapping[str, Any],
    spec: "BenchmarkSpec",
) -> List[Tuple[RejectionCode, str]]:
    """Reject metrics outside plausible ranges."""
    out: List[Tuple[RejectionCode, str]] = []
    for name, value in metrics.items():
        if name not in spec.metric_definitions:
            continue
        try:
            v = float(value) if not isinstance(value, bool) else 1.0
        except (TypeError, ValueError):
            continue
        if math.isnan(v) or math.isinf(v):
            continue   # already rejected by the finite check
        # Plausibility: require |v| <= 1e12 (sanity bound).
        if abs(v) > 1e12:
            out.append((
                RejectionCode.ANTI_GAMING_FAILED,
                f"anti-gaming: metric {name!r}={v} exceeds "
                f"plausibility bound 1e12",
            ))
    return out


@_register_check("accuracy_range")
def _check_accuracy_range(
    metrics: Mapping[str, Any],
    spec: "BenchmarkSpec",
) -> List[Tuple[RejectionCode, str]]:
    """Reject accuracy outside [0, 1]."""
    if "accuracy" not in metrics:
        return []
    try:
        acc = float(metrics["accuracy"])
    except (TypeError, ValueError):
        return []
    if not 0.0 <= acc <= 1.0:
        return [(
            RejectionCode.ANTI_GAMING_FAILED,
            f"anti-gaming: accuracy={acc} outside [0, 1]",
        )]
    return []


@_register_check("carbon_energy_consistency")
def _check_carbon_energy_consistency(
    metrics: Mapping[str, Any],
    spec: "BenchmarkSpec",
) -> List[Tuple[RejectionCode, str]]:
    """Carbon intensity must be plausible for any grid."""
    energy = metrics.get("energy_kwh")
    carbon = metrics.get("carbon_kg")
    if not isinstance(energy, (int, float)) or \
            not isinstance(carbon, (int, float)):
        return []
    if isinstance(energy, bool) or isinstance(carbon, bool):
        return []
    if energy <= 0 or carbon < 0:
        return []
    intensity = carbon / energy * 1000   # gCO2 / kWh
    if intensity > 2000:
        return [(
            RejectionCode.ANTI_GAMING_FAILED,
            f"anti-gaming: carbon intensity {intensity:.0f} "
            f"gCO2/kWh exceeds 2000",
        )]
    return []


# =============================================================================
# BenchmarkEngine
# =============================================================================

class BenchmarkEngine:
    """
    Governance engine for benchmark submissions.

    Rules enforced (all four, at submit time):

      1. **Task eligibility** — the task type must be in
         ``spec.task_eligibility`` (case-insensitive). Unless
         ``spec.allow_any_task=True``.
      2. **Anti-gaming** — every check listed in
         ``spec.anti_gaming_checks`` runs against the submission.
      3. **Acceptance thresholds** — every threshold in
         ``spec.acceptance_thresholds`` must be satisfied.
      4. **Reproducibility** — every rule in
         ``spec.reproducibility_rules`` must be present in the
         submission payload (via ``extra=`` or top-level kwargs).
    """

    def __init__(
        self,
        spec: BenchmarkSpec,
        *,
        audit_log: Optional[Any] = None,
        metrics_hook: Optional[
            Callable[[str, Mapping[str, Any]], None]
        ] = None,
        max_history: int = 1000,
    ) -> None:
        if not isinstance(spec, BenchmarkSpec):
            raise BenchmarkValidationError(
                f"spec must be a BenchmarkSpec, "
                f"got {type(spec).__name__}")
        if metrics_hook is not None and not callable(metrics_hook):
            raise BenchmarkValidationError(
                "metrics_hook must be callable")
        if isinstance(max_history, bool) or \
                not isinstance(max_history, int) or max_history <= 0:
            raise BenchmarkValidationError(
                "max_history must be a positive int")

        self.spec = spec
        self._audit_log = audit_log
        self._metrics_hook = metrics_hook
        self._lock = threading.RLock()

        self._history: Deque[BenchmarkResult] = deque(
            maxlen=max_history)
        self._result_by_run: Dict[str, BenchmarkResult] = {}
        self._submission_count: int = 0
        self._passed_count: int = 0
        self._rejected_count: int = 0
        self._rejection_code_counts: Dict[str, int] = defaultdict(int)

    # ------------------------------------------------------------------
    # Metrics / audit emitters
    # ------------------------------------------------------------------

    def _emit_metric(
        self, event: str, fields_: Mapping[str, Any],
    ) -> None:
        if self._metrics_hook is None:
            return
        try:
            self._metrics_hook(event, dict(fields_))
        except Exception:  # noqa: BLE001
            logger.debug("metrics_hook raised", exc_info=True)

    def _emit_audit(self, payload: Mapping[str, Any]) -> None:
        if self._audit_log is None:
            return
        append_fn = getattr(self._audit_log, "append", None)
        if not callable(append_fn):
            return
        try:
            append_fn(dict(payload))
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "audit_log.append failed for benchmark event: %s",
                exc)

    @staticmethod
    def _safe_log(level: int, fmt: str, *args: Any) -> None:
        try:
            logger.log(level, fmt, *args)
        except Exception:  # noqa: BLE001
            try:
                logger.debug("log handler raised", exc_info=True)
            except Exception:  # noqa: BLE001
                pass

    # ------------------------------------------------------------------
    # Submit
    # ------------------------------------------------------------------

    def submit(
        self,
        *,
        run_id: str,
        task_type: str,
        system_version: str,
        policy_version: str,
        metrics: Dict[str, Any],
        extra: Optional[Dict[str, Any]] = None,
        simulated: bool = False,
        source: Optional[str] = None,
        dry_run: bool = False,
    ) -> BenchmarkResult:
        """
        Validate a benchmark submission.

        Returns a frozen ``BenchmarkResult``. The result is appended
        to the bounded ``history()`` unless ``dry_run=True``.

        Parameters
        ----------
        run_id, task_type, system_version, policy_version : str
            Required identifiers.
        metrics : Dict[str, Any]
            Submitted metric values. Must be a ``Mapping``; every
            declared threshold's metric must be finite numeric.
        extra : Dict[str, Any], optional
            Additional submission fields (used to satisfy
            ``spec.reproducibility_rules``).
        simulated : bool, default False
            Whether the run was simulated (recorded on the result).
        source : str, optional
            Provenance label (e.g. ``"measured"``, ``"simulated"``).
        dry_run : bool, default False
            If True, do not append to history; the ``BenchmarkResult``
            is returned for inspection only.
        """
        # ---- Validate inputs ----
        run_id = _require_non_empty_string("run_id", run_id)
        system_version = _require_non_empty_string(
            "system_version", system_version)
        policy_version = _require_non_empty_string(
            "policy_version", policy_version)
        task_type_norm = _normalize_task_type(task_type)
        if not isinstance(metrics, Mapping):
            raise BenchmarkValidationError(
                f"metrics must be a Mapping, "
                f"got {type(metrics).__name__}")
        if extra is not None and not isinstance(extra, Mapping):
            raise BenchmarkValidationError(
                f"extra must be a Mapping, "
                f"got {type(extra).__name__}")
        extra = dict(extra or {})
        if not isinstance(simulated, bool):
            raise BenchmarkValidationError(
                "simulated must be a bool")
        if not isinstance(dry_run, bool):
            raise BenchmarkValidationError(
                "dry_run must be a bool")
        if source is not None:
            source = _require_non_empty_string("source", source)

        reasons: List[str] = []
        codes: List[str] = []

        def _reject(code: RejectionCode, msg: str) -> None:
            codes.append(code.value)
            reasons.append(msg)

        # ---- 1. Task eligibility (P0 2.10) ----
        if not self.spec.allow_any_task:
            eligible = set(self.spec.task_eligibility)
            if task_type_norm not in eligible:
                _reject(
                    RejectionCode.TASK_NOT_ELIGIBLE,
                    f"task type {task_type!r} not eligible for "
                    f"benchmark {self.spec.benchmark_id!r}")

        # ---- 2. Metrics presence / definition check (P0 2.14) ----
        submitted_names = {str(k).strip() for k in metrics.keys()}
        defined_names = set(self.spec.metric_definitions.keys())
        if self.spec.require_all_definitions:
            missing = defined_names - submitted_names
            if missing:
                _reject(
                    RejectionCode.METRIC_MISSING,
                    f"missing required metrics: {sorted(missing)}")
        if defined_names:
            unknown = submitted_names - defined_names
            if unknown:
                _reject(
                    RejectionCode.METRIC_UNKNOWN,
                    f"unknown metrics submitted: {sorted(unknown)}")

        # ---- 3. Anti-gaming checks (P0 2.3) ----
        for check_name in self.spec.anti_gaming_checks:
            check_fn = _ANTI_GAMING_CHECKS.get(check_name)
            if check_fn is None:
                # Shouldn't happen — spec construction validates.
                raise BenchmarkAntiGamingError(
                    f"unknown anti-gaming check {check_name!r}")
            try:
                check_rejections = check_fn(metrics, self.spec)
            except Exception as exc:  # noqa: BLE001
                _reject(
                    RejectionCode.ANTI_GAMING_FAILED,
                    f"anti-gaming check {check_name!r} failed: "
                    f"{type(exc).__name__}: {exc}")
                continue
            for code, msg in check_rejections or []:
                _reject(code, msg)

        # ---- 4. Acceptance thresholds (P0 2.1, 2.2) ----
        for name, threshold in self.spec.acceptance_thresholds.items():
            raw = metrics.get(name)
            if raw is None:
                _reject(
                    RejectionCode.METRIC_MISSING,
                    f"metric {name!r} missing")
                continue
            if isinstance(raw, bool) or \
                    not isinstance(raw, (int, float)):
                _reject(
                    RejectionCode.METRIC_NON_NUMERIC,
                    f"metric {name!r} must be numeric, "
                    f"got {type(raw).__name__}")
                continue
            v = float(raw)
            if math.isnan(v) or math.isinf(v):
                _reject(
                    RejectionCode.METRIC_NON_FINITE,
                    f"metric {name!r} must be finite, got {raw!r}")
                continue
            code = threshold.evaluate(v)
            if code is not None:
                if code == RejectionCode.THRESHOLD_BELOW_MIN:
                    _reject(
                        code,
                        f"metric {name!r}={v} below threshold "
                        f"{threshold.value}")
                else:
                    _reject(
                        code,
                        f"metric {name!r}={v} above allowed "
                        f"{threshold.value}")

        # ---- 5. Reproducibility rules (P0 2.4) ----
        submission_payload = {
            "run_id": run_id,
            "task_type": task_type_norm,
            "system_version": system_version,
            "policy_version": policy_version,
            **extra,
        }
        missing_fields: List[str] = []
        for rule in self.spec.reproducibility_rules:
            if submission_payload.get(rule) is None:
                missing_fields.append(rule)
        if missing_fields:
            _reject(
                RejectionCode.REPRODUCIBILITY_MISSING,
                f"missing reproducibility fields: "
                f"{missing_fields}")

        # ---- 6. Deduplication (P1 3.10, per-spec) ----
        with self._lock:
            if self.spec.deduplicate_runs and \
                    run_id in self._result_by_run:
                _reject(
                    RejectionCode.DEDUPLICATE,
                    f"run_id {run_id!r} already submitted")
            # ---- Build result ----
            passed = not reasons
            result = BenchmarkResult(
                benchmark_id=self.spec.benchmark_id,
                run_id=run_id,
                system_version=system_version,
                policy_version=policy_version,
                passed=passed,
                rejection_reasons=reasons,
                rejection_codes=codes,
                metrics=_safe_json(dict(metrics)),
                simulated=bool(simulated),
                source=source,
            )

            if not dry_run:
                self._history.append(result)
                self._result_by_run[run_id] = result
                self._submission_count += 1
                if passed:
                    self._passed_count += 1
                else:
                    self._rejected_count += 1
                    for code_str in codes:
                        self._rejection_code_counts[code_str] += 1

        self._emit_metric(
            "submission_passed" if passed else "submission_rejected",
            {
                "benchmark_id": self.spec.benchmark_id,
                "run_id": run_id,
                "task_type": task_type_norm,
                "codes": codes,
            })
        self._emit_audit({
            "event": "benchmark_submitted",
            "benchmark_id": self.spec.benchmark_id,
            "run_id": run_id,
            "task_type": task_type_norm,
            "passed": passed,
            "codes": codes,
            "submitted_at": result.submitted_at.isoformat(),
        })
        self._safe_log(
            logging.INFO,
            "Benchmark %r run %s: %s",
            self.spec.name, run_id,
            "PASSED" if passed else "REJECTED")
        return result

    # ------------------------------------------------------------------
    # History and statistics
    # ------------------------------------------------------------------

    def history(
        self, limit: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        """Return the most recent submission results."""
        with self._lock:
            records = list(self._history)
        if limit is not None:
            if isinstance(limit, bool) or \
                    not isinstance(limit, int) or limit <= 0:
                raise BenchmarkValidationError(
                    "limit must be a positive int or None")
            records = records[-limit:]
        return [r.to_dict() for r in records]

    def get_result(self, run_id: str) -> Optional[BenchmarkResult]:
        """Return the stored result for ``run_id`` or None."""
        if not isinstance(run_id, str) or not run_id:
            return None
        with self._lock:
            return self._result_by_run.get(run_id)

    def statistics(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "version": __version__,
                "schema_version": LEDGER_SCHEMA_VERSION,
                "benchmark_id": self.spec.benchmark_id,
                "submissions": self._submission_count,
                "passed": self._passed_count,
                "rejected": self._rejected_count,
                "by_rejection_code": dict(self._rejection_code_counts),
                "history_size": len(self._history),
                "audit_log_attached": self._audit_log is not None,
            }

    def reset(self, *, clear_history: bool = True) -> None:
        with self._lock:
            if clear_history:
                self._history.clear()
                self._result_by_run.clear()
            self._submission_count = 0
            self._passed_count = 0
            self._rejected_count = 0
            self._rejection_code_counts.clear()

    # ------------------------------------------------------------------
    # Serialization
    # ------------------------------------------------------------------

    def to_dict(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "version": __version__,
                "schema_version": LEDGER_SCHEMA_VERSION,
                "spec": self.spec.to_dict(),
                "statistics": self.statistics(),
                "history": [r.to_dict() for r in self._history],
            }

    def to_json(self, **kwargs: Any) -> str:
        return json.dumps(self.to_dict(), default=str, **kwargs)

    # ------------------------------------------------------------------
    # Pickling
    # ------------------------------------------------------------------

    def __getstate__(self) -> Dict[str, Any]:
        state = self.__dict__.copy()
        state.pop("_lock", None)
        state.pop("_audit_log", None)
        state.pop("_metrics_hook", None)
        return state

    def __setstate__(self, state: Dict[str, Any]) -> None:
        self.__dict__.update(state)
        self._lock = threading.RLock()
        self._audit_log = None
        self._metrics_hook = None

    # ------------------------------------------------------------------
    # Dunder
    # ------------------------------------------------------------------

    def __repr__(self) -> str:
        with self._lock:
            return (
                f"BenchmarkEngine("
                f"version={__version__!r}, "
                f"benchmark={self.spec.benchmark_id!r}, "
                f"submissions={self._submission_count}, "
                f"passed={self._passed_count}, "
                f"rejected={self._rejected_count})")


# =============================================================================
# Public API
# =============================================================================

__all__ = [
    "__version__",
    "LEDGER_SCHEMA_VERSION",
    "BenchmarkAntiGamingError",
    "BenchmarkDuplicateSubmissionError",
    "BenchmarkEngine",
    "BenchmarkEngineError",
    "BenchmarkResult",
    "BenchmarkSpec",
    "BenchmarkValidationError",
    "MetricDefinition",
    "RejectionCode",
    "Threshold",
]


# =============================================================================
# Self-tests
# =============================================================================

def _run_selftests() -> None:
    """Run the module's self-tests."""
    import concurrent.futures
    import pickle
    from dataclasses import FrozenInstanceError

    logging.basicConfig(
        level=logging.WARNING,
        format="%(levelname)s %(name)s: %(message)s")

    print(f"=== benchmark_engine v{__version__} self-tests ===\n")

    def _make_spec(**overrides):
        base = dict(
            benchmark_id="test-bench",
            name="Test Benchmark",
            task_eligibility=["inference"],
            metric_definitions={
                "accuracy": "accuracy",
                "energy_kwh": "energy_kwh",
                "carbon_kg": "carbon_kg",
            },
            anti_gaming_checks=["accuracy_range"],
            acceptance_thresholds={
                "accuracy": 0.85,
                "carbon_kg": -1.0,   # v1.0.0 max convention
            },
            reproducibility_rules=[
                "system_version", "policy_version", "dataset_id",
            ],
        )
        base.update(overrides)
        return BenchmarkSpec(**base)

    def _submit(engine, **overrides):
        base = dict(
            run_id=f"run-{uuid.uuid4().hex[:8]}",
            task_type="inference",
            system_version="5.0.0",
            policy_version="v1.0.0",
            metrics={"accuracy": 0.92, "energy_kwh": 0.5,
                     "carbon_kg": 0.2},
            extra={"dataset_id": "sst2"},
        )
        base.update(overrides)
        return engine.submit(**base)

    # ---------------------------------------------------------------- #
    # P0 2.1 — NaN/Inf in metrics rejected
    # ---------------------------------------------------------------- #
    print("=== P0 2.1: NaN/Inf rejected ===")

    spec = _make_spec()
    engine = BenchmarkEngine(spec)

    result = _submit(engine, metrics={
        "accuracy": float("nan"),
        "energy_kwh": 0.5, "carbon_kg": 0.2,
    })
    assert result.passed is False
    assert RejectionCode.METRIC_NON_FINITE.value in \
        result.rejection_codes, result.rejection_codes
    print(f"  ✓ accuracy=NaN rejected: {result.rejection_codes}")

    result = _submit(engine, metrics={
        "accuracy": float("inf"),
        "energy_kwh": 0.5, "carbon_kg": 0.2,
    })
    assert result.passed is False
    assert RejectionCode.METRIC_NON_FINITE.value in \
        result.rejection_codes
    print(f"  ✓ accuracy=Inf rejected")

    # ---------------------------------------------------------------- #
    # P0 2.2 — bool rejected
    # ---------------------------------------------------------------- #
    print("\n=== P0 2.2: bool rejected ===")

    result = _submit(engine, metrics={
        "accuracy": True,
        "energy_kwh": 0.5, "carbon_kg": 0.2,
    })
    assert result.passed is False
    assert RejectionCode.METRIC_NON_NUMERIC.value in \
        result.rejection_codes
    print(f"  ✓ accuracy=True rejected")

    # ---------------------------------------------------------------- #
    # P0 2.3 — anti-gaming checks actually run
    # ---------------------------------------------------------------- #
    print("\n=== P0 2.3: anti-gaming checks run ===")

    # accuracy=1.5 fails the accuracy_range check
    # (but would pass a >=0.85 threshold)
    result = _submit(engine, metrics={
        "accuracy": 1.5,
        "energy_kwh": 0.5, "carbon_kg": 0.2,
    })
    assert result.passed is False
    assert RejectionCode.ANTI_GAMING_FAILED.value in \
        result.rejection_codes
    print(f"  ✓ accuracy=1.5 rejected by anti-gaming check "
          f"(not just threshold)")

    # ---------------------------------------------------------------- #
    # P0 2.4 — reproducibility rules enforced
    # ---------------------------------------------------------------- #
    print("\n=== P0 2.4: reproducibility enforced ===")

    result = _submit(engine, extra={})   # missing dataset_id
    assert result.passed is False
    assert RejectionCode.REPRODUCIBILITY_MISSING.value in \
        result.rejection_codes
    print(f"  ✓ missing dataset_id rejected")

    # ---------------------------------------------------------------- #
    # P0 2.5 — structured Threshold
    # ---------------------------------------------------------------- #
    print("\n=== P0 2.5: Threshold dataclass ===")

    # Explicit Threshold.max
    spec = _make_spec(acceptance_thresholds={
        "accuracy": Threshold(value=0.85, direction="min"),
        "carbon_kg": Threshold(value=1.0, direction="max"),
    })
    engine = BenchmarkEngine(spec)

    result = _submit(engine, metrics={
        "accuracy": 0.92, "energy_kwh": 0.5,
        "carbon_kg": 1.5,  # exceeds max 1.0
    })
    assert result.passed is False
    assert RejectionCode.THRESHOLD_ABOVE_MAX.value in \
        result.rejection_codes
    print(f"  ✓ Threshold(max=1.0) rejects carbon_kg=1.5")

    # Legacy numeric thresholds still work.
    spec = _make_spec(acceptance_thresholds={
        "accuracy": 0.85,
        "carbon_kg": -1.0,
    })
    engine = BenchmarkEngine(spec)
    result = _submit(engine)
    assert result.passed is True
    print(f"  ✓ legacy numeric thresholds still work")

    # ---------------------------------------------------------------- #
    # P0 2.6 — metrics Mapping validated
    # ---------------------------------------------------------------- #
    print("\n=== P0 2.6: metrics must be Mapping ===")
    engine = BenchmarkEngine(_make_spec())
    try:
        _submit(engine, metrics="not-a-mapping")
    except BenchmarkValidationError:
        print(f"  ✓ string metrics → BenchmarkValidationError")
    else:
        raise AssertionError(
            "P0 2.6 regression: string metrics accepted")

    # ---------------------------------------------------------------- #
    # P0 2.7 — deep copy of metrics
    # ---------------------------------------------------------------- #
    print("\n=== P0 2.7: metrics deep-copied ===")
    engine = BenchmarkEngine(_make_spec())
    metrics = {"accuracy": 0.92, "energy_kwh": 0.5,
               "carbon_kg": 0.2}
    result = _submit(engine, metrics=metrics)
    metrics["accuracy"] = 0.1
    assert result.metrics["accuracy"] == 0.92
    print(f"  ✓ mutating submitted dict does not affect result")

    # ---------------------------------------------------------------- #
    # P0 2.8 — frozen result
    # ---------------------------------------------------------------- #
    print("\n=== P0 2.8: BenchmarkResult frozen ===")
    try:
        result.passed = True   # type: ignore[misc]
    except FrozenInstanceError:
        print(f"  ✓ direct mutation raises FrozenInstanceError")
    else:
        raise AssertionError(
            "P0 2.8 regression: BenchmarkResult is mutable")

    # ---------------------------------------------------------------- #
    # P0 2.9 — BenchmarkSpec validation
    # ---------------------------------------------------------------- #
    print("\n=== P0 2.9: spec validation ===")

    bad_specs = [
        dict(benchmark_id="", name="x",
             task_eligibility=["t"],
             metric_definitions={},
             anti_gaming_checks=["accuracy_range"],
             acceptance_thresholds={},
             reproducibility_rules=["x"]),
        dict(benchmark_id="b", name="",
             task_eligibility=["t"],
             metric_definitions={},
             anti_gaming_checks=["accuracy_range"],
             acceptance_thresholds={},
             reproducibility_rules=["x"]),
        dict(benchmark_id="b", name="n",
             task_eligibility=[],
             metric_definitions={},
             anti_gaming_checks=["accuracy_range"],
             acceptance_thresholds={},
             reproducibility_rules=["x"]),
        dict(benchmark_id="b", name="n",
             task_eligibility=["t"],
             metric_definitions={},
             anti_gaming_checks=[],   # empty
             acceptance_thresholds={},
             reproducibility_rules=["x"]),
        dict(benchmark_id="b", name="n",
             task_eligibility=["t"],
             metric_definitions={},
             anti_gaming_checks=["accuracy_range"],
             acceptance_thresholds={},
             reproducibility_rules=[]),  # empty
        dict(benchmark_id="b", name="n",
             task_eligibility=["t"],
             metric_definitions={},
             anti_gaming_checks=["garbage"],   # unknown
             acceptance_thresholds={},
             reproducibility_rules=["x"]),
        dict(benchmark_id="b", name="n",
             task_eligibility=["t"],
             metric_definitions={},
             anti_gaming_checks=["accuracy_range"],
             acceptance_thresholds={"accuracy": float("nan")},
             reproducibility_rules=["x"]),
    ]
    for kwargs in bad_specs:
        try:
            BenchmarkSpec(**kwargs)
        except BenchmarkValidationError:
            pass
        else:
            raise AssertionError(
                f"P0 2.9 regression: {kwargs!r} accepted")
    print(f"  ✓ {len(bad_specs)} invalid specs rejected")

    # allow_any_task escapes empty eligibility.
    spec = BenchmarkSpec(
        benchmark_id="b", name="n",
        task_eligibility=[],
        allow_any_task=True,
        metric_definitions={},
        anti_gaming_checks=["accuracy_range"],
        acceptance_thresholds={},
        reproducibility_rules=["x"])
    assert spec.allow_any_task is True
    print(f"  ✓ allow_any_task=True permits empty eligibility")

    # ---------------------------------------------------------------- #
    # P0 2.10 — case-insensitive task type
    # ---------------------------------------------------------------- #
    print("\n=== P0 2.10: case-insensitive task type ===")
    engine = BenchmarkEngine(_make_spec(
        task_eligibility=["inference"]))
    result = _submit(engine, task_type="INFERENCE")
    assert result.passed is True
    print(f"  ✓ 'INFERENCE' matches 'inference'")

    # ---------------------------------------------------------------- #
    # P0 2.11 — run_id/versions validated
    # ---------------------------------------------------------------- #
    print("\n=== P0 2.11: run_id/versions validated ===")
    for bad_field, bad_val in (
        ("run_id", ""),
        ("system_version", None),
        ("policy_version", ""),
    ):
        try:
            _submit(engine, **{bad_field: bad_val})
        except BenchmarkValidationError:
            pass
        else:
            raise AssertionError(
                f"P0 2.11 regression: {bad_field}={bad_val!r} "
                f"accepted")
    print(f"  ✓ 3 invalid submissions rejected")

    # ---------------------------------------------------------------- #
    # P0 2.12 — BenchmarkResult validation
    # ---------------------------------------------------------------- #
    print("\n=== P0 2.12: BenchmarkResult validation ===")

    bad_results = [
        dict(benchmark_id="", run_id="r",
             system_version="s", policy_version="p"),
        dict(benchmark_id="b", run_id="",
             system_version="s", policy_version="p"),
        dict(benchmark_id="b", run_id="r",
             system_version="", policy_version="p"),
        dict(benchmark_id="b", run_id="r",
             system_version="s", policy_version="p",
             passed="yes"),
        dict(benchmark_id="b", run_id="r",
             system_version="s", policy_version="p",
             submitted_at="not-a-datetime"),
    ]
    for kwargs in bad_results:
        try:
            BenchmarkResult(**kwargs)
        except BenchmarkValidationError:
            pass
        else:
            raise AssertionError(
                f"P0 2.12 regression: {kwargs!r} accepted")
    print(f"  ✓ {len(bad_results)} invalid results rejected")

    # ---------------------------------------------------------------- #
    # P0 2.13 — error hierarchy
    # ---------------------------------------------------------------- #
    print("\n=== P0 2.13: error hierarchy ===")
    assert issubclass(BenchmarkEngineError, ValueError)
    assert issubclass(
        BenchmarkValidationError, BenchmarkEngineError)
    assert issubclass(
        BenchmarkAntiGamingError, BenchmarkEngineError)
    assert issubclass(
        BenchmarkDuplicateSubmissionError, BenchmarkEngineError)
    print(f"  ✓ 4 error types subclass BenchmarkEngineError")

    # ---------------------------------------------------------------- #
    # P0 2.14 — metric_definitions enforced
    # ---------------------------------------------------------------- #
    print("\n=== P0 2.14: metric_definitions enforced ===")
    engine = BenchmarkEngine(_make_spec())
    result = _submit(engine, metrics={
        "accuracy": 0.92,
        "energy_kwh": 0.5, "carbon_kg": 0.2,
        "garbage_metric": 1.0,
    })
    assert result.passed is False
    assert RejectionCode.METRIC_UNKNOWN.value in \
        result.rejection_codes
    print(f"  ✓ unknown 'garbage_metric' rejected")

    # require_all_definitions
    engine = BenchmarkEngine(_make_spec(require_all_definitions=True))
    result = _submit(engine, metrics={"accuracy": 0.92})
    assert result.passed is False
    assert RejectionCode.METRIC_MISSING.value in \
        result.rejection_codes
    print(f"  ✓ require_all_definitions rejects partial submission")

    # ---------------------------------------------------------------- #
    # P0 2.15 — JSON-safe to_dict
    # ---------------------------------------------------------------- #
    print("\n=== P0 2.15: JSON-safe to_dict ===")
    engine = BenchmarkEngine(_make_spec())
    result = _submit(engine)
    payload = result.to_dict()
    json.dumps(payload)   # must not raise
    assert isinstance(payload["submitted_at"], str)
    print(f"  ✓ result.to_dict() JSON-serializable")

    # ---------------------------------------------------------------- #
    # P0 2.16 — RejectionCode enum
    # ---------------------------------------------------------------- #
    print("\n=== P0 2.16: RejectionCode ===")
    engine = BenchmarkEngine(_make_spec())
    result = _submit(engine, metrics={"accuracy": 0.5,
                                      "energy_kwh": 0.5,
                                      "carbon_kg": 0.2})
    assert result.passed is False
    assert RejectionCode.THRESHOLD_BELOW_MIN.value in \
        result.rejection_codes
    print(f"  ✓ structured code present")

    # ---------------------------------------------------------------- #
    # P1 3.1 — simulated + source
    # ---------------------------------------------------------------- #
    print("\n=== P1 3.1: simulated provenance ===")
    engine = BenchmarkEngine(_make_spec())
    result = _submit(engine, simulated=True, source="chaos")
    assert result.simulated is True
    assert result.source == "chaos"
    print(f"  ✓ simulated=True, source='chaos' recorded")

    # ---------------------------------------------------------------- #
    # P1 3.2 — metrics_hook
    # ---------------------------------------------------------------- #
    print("\n=== P1 3.2: metrics_hook ===")
    events = []
    engine = BenchmarkEngine(
        _make_spec(),
        metrics_hook=lambda ev, f: events.append((ev, f)))
    _submit(engine)
    names = {ev for ev, _ in events}
    assert "submission_passed" in names
    print(f"  ✓ events: {sorted(names)}")

    # ---------------------------------------------------------------- #
    # P1 3.3 — audit_log
    # ---------------------------------------------------------------- #
    print("\n=== P1 3.3: audit_log integration ===")
    audit = []

    class _MockAudit:
        def append(self, payload):
            audit.append(payload)

    engine = BenchmarkEngine(_make_spec(), audit_log=_MockAudit())
    _submit(engine)
    assert len(audit) == 1
    assert audit[0]["event"] == "benchmark_submitted"
    print(f"  ✓ audit_log received event")

    # ---------------------------------------------------------------- #
    # P1 3.4 — spec serialization
    # ---------------------------------------------------------------- #
    print("\n=== P1 3.4: spec serialization ===")
    spec = _make_spec()
    d = spec.to_dict()
    assert d["benchmark_id"] == "test-bench"
    json.dumps(d)   # must not raise
    restored = BenchmarkSpec.from_legacy_dict(d)
    assert restored.benchmark_id == spec.benchmark_id
    print(f"  ✓ spec.to_dict / from_legacy_dict round-trip")

    # ---------------------------------------------------------------- #
    # P1 3.5 — version constants
    # ---------------------------------------------------------------- #
    print("\n=== P1 3.5: version constants ===")
    assert __version__ == "2.0.0"
    assert LEDGER_SCHEMA_VERSION == "5.0.0"
    import sys
    mod = sys.modules[__name__]
    assert hasattr(mod, "__all__")
    assert "__version__" in mod.__all__
    print(f"  ✓ version={__version__!r}, "
          f"schema={LEDGER_SCHEMA_VERSION!r}")

    # ---------------------------------------------------------------- #
    # P1 3.6 — statistics
    # ---------------------------------------------------------------- #
    print("\n=== P1 3.6: statistics ===")
    engine = BenchmarkEngine(_make_spec())
    _submit(engine)   # pass
    _submit(engine, metrics={"accuracy": 0.5,
                             "energy_kwh": 0.5,
                             "carbon_kg": 0.2})   # fail
    stats = engine.statistics()
    assert stats["submissions"] == 2
    assert stats["passed"] == 1
    assert stats["rejected"] == 1
    print(f"  ✓ statistics: {stats['submissions']} sub, "
          f"{stats['passed']} pass, {stats['rejected']} reject")

    # ---------------------------------------------------------------- #
    # P1 3.7 — thread safety
    # ---------------------------------------------------------------- #
    print("\n=== P1 3.7: thread safety ===")
    engine = BenchmarkEngine(_make_spec())

    def _worker(i):
        _submit(engine, run_id=f"r-{i}")

    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as pool:
        list(pool.map(_worker, range(50)))

    stats = engine.statistics()
    assert stats["submissions"] == 50, stats
    print(f"  ✓ 50 concurrent submissions counted correctly")

    # ---------------------------------------------------------------- #
    # P1 3.8 — history
    # ---------------------------------------------------------------- #
    print("\n=== P1 3.8: history ===")
    engine = BenchmarkEngine(_make_spec())
    for i in range(5):
        _submit(engine, run_id=f"h-{i}")
    hist = engine.history(limit=3)
    assert len(hist) == 3
    print(f"  ✓ history(limit=3) returns 3 records")

    # ---------------------------------------------------------------- #
    # P1 3.9 — get_result(run_id)
    # ---------------------------------------------------------------- #
    print("\n=== P1 3.9: get_result ===")
    engine = BenchmarkEngine(_make_spec())
    _submit(engine, run_id="specific-run")
    r = engine.get_result("specific-run")
    assert r is not None
    assert r.run_id == "specific-run"
    print(f"  ✓ get_result('specific-run') returns result")

    # ---------------------------------------------------------------- #
    # P1 3.10 — empty task_eligibility
    # ---------------------------------------------------------------- #
    print("\n=== P1 3.10: empty eligibility ===")
    try:
        _make_spec(task_eligibility=[])
    except BenchmarkValidationError as exc:
        print(f"  ✓ empty eligibility rejected by default")
    else:
        raise AssertionError(
            "P1 3.10 regression: empty eligibility accepted")

    # allow_any_task
    spec = _make_spec(task_eligibility=[], allow_any_task=True)
    engine = BenchmarkEngine(spec)
    result = _submit(engine, task_type="anything")
    assert result.passed is True
    print(f"  ✓ allow_any_task=True permits any task type")

    # ---------------------------------------------------------------- #
    # P1 3.11 — dry_run
    # ---------------------------------------------------------------- #
    print("\n=== P1 3.11: dry_run ===")
    engine = BenchmarkEngine(_make_spec())
    result = _submit(engine, dry_run=True)
    assert result.passed is True
    assert len(engine.history()) == 0
    print(f"  ✓ dry_run previews without recording")

    # ---------------------------------------------------------------- #
    # P1 3.12 — threshold finiteness validated
    # ---------------------------------------------------------------- #
    print("\n=== P1 3.12: threshold finiteness ===")
    try:
        _make_spec(acceptance_thresholds={"accuracy": float("nan")})
    except BenchmarkValidationError:
        print(f"  ✓ NaN threshold rejected at spec construction")

    # ---------------------------------------------------------------- #
    # P1 3.13 — MetricDefinition
    # ---------------------------------------------------------------- #
    print("\n=== P1 3.13: MetricDefinition ===")
    md = MetricDefinition(name="accuracy", unit="ratio",
                          description="model accuracy")
    assert md.unit == "ratio"
    # String coercion
    md2 = MetricDefinition.coerce("energy_kwh")
    assert md2.name == "energy_kwh"
    assert md2.unit == "unitless"
    print(f"  ✓ MetricDefinition structured + coerce from string")

    # ---------------------------------------------------------------- #
    # Deduplication
    # ---------------------------------------------------------------- #
    print("\n=== Deduplication ===")
    engine = BenchmarkEngine(_make_spec())
    _submit(engine, run_id="dup-run")
    result = _submit(engine, run_id="dup-run")
    assert result.passed is False
    assert RejectionCode.DEDUPLICATE.value in result.rejection_codes
    print(f"  ✓ duplicate run_id rejected")

    # ---------------------------------------------------------------- #
    # Backward compat
    # ---------------------------------------------------------------- #
    print("\n=== Backward compat ===")
    # v1.0.0 numeric thresholds.
    spec = BenchmarkSpec(
        benchmark_id="legacy",
        name="Legacy",
        task_eligibility=["inference"],
        metric_definitions={"accuracy": "accuracy"},
        anti_gaming_checks=["accuracy_range"],
        acceptance_thresholds={"accuracy": 0.85},
        reproducibility_rules=["system_version"])
    engine = BenchmarkEngine(spec)
    result = engine.submit(
        run_id="r", task_type="inference",
        system_version="v", policy_version="p",
        metrics={"accuracy": 0.92})
    assert result.passed is True
    print(f"  ✓ v1.0.0 numeric thresholds preserved")

    # ---------------------------------------------------------------- #
    # Pickle
    # ---------------------------------------------------------------- #
    print("\n=== Pickle ===")
    engine = BenchmarkEngine(_make_spec())
    _submit(engine)
    blob = pickle.dumps(engine)
    restored = pickle.loads(blob)
    assert restored._lock is not None
    assert restored.statistics()["submissions"] == 1
    print(f"  ✓ pickle round-trip works")

    print(f"\n✓ All self-tests passed.")


if __name__ == "__main__":  # pragma: no cover
    _run_selftests()
