# src/analysis/telemetry/metric_provenance.py
"""
Metric provenance for Green Agent (Enhanced, v2.0.0)
=====================================================

Records the origin, freshness, and trust level of every metric in a
result dict. This is the module the analysis-layer recommendation
explicitly calls out for Priority #1 ("reliable telemetry + provenance").

Original API preserved (all signatures, all side effects):
    metrics = attach_provenance(metrics)
    # metrics["metric_provenance"] is {metric -> "measured"/"estimated"}

Enhanced API:
    metrics = attach_provenance_enhanced(
        metrics,
        run_id="...", task_id="...", agent_id="...",
        default_metrics=DEFAULT_METRICS,       # 12-metric coverage
        emit_decision_record=False,
        in_place=True,
    )

    # Collection-level access
    report = ProvenanceReport.from_metrics(metrics)
    report.sources("energy")               # list of sources
    report.trust_level("energy")           # "high"/"medium"/"low"/"untrustworthy"
    report.simulated_metrics()             # list of metric names
    report.federated_metrics()             # list of metric names
    report.unknown_metrics()               # list of metric names
    report.hitl_denied_metrics()           # list of metric names
    report.to_dict(safe=True)              # JSON-safe export
    report.to_decision_record(...)         # shared-contract emission

    # Single-metric access
    prov = MetricProvenance.from_dict(metrics["metric_provenance_detailed"]["energy"])
    prov.trust_level()                     # TrustLevel enum
    prov.trust_level_str()                 # "high" (for reports)
    prov.effective_age_seconds()           # re-derived from `at`
    prov.to_dict(safe=True)

    # Standalone DecisionRecord builder
    dr = to_decision_record(metrics, run_id="...", policy_version="v5.0.1")

    # Introspection
    summarize_provenance(metrics)          # per-metric roll-up
    get_provenance(metrics, "energy")      # single MetricProvenance
    get_statistics() / reset_statistics()  # module-level counters

Enhancements implemented (v2.0.0):
  1.  Merge preservation     — caller-supplied `metric_provenance` entries
                               are preserved (v1 silently overwrote them).
  2.  Fixed inference        — carbon reads `carbon_source` / `grid_source`
                               (v1 read a dead `framework_overhead_source`);
                               latency honours `simulated` / `latency_source`
                               (v1 always returned MEASURED).
  3.  JSON-safe DR           — `emit_decision_record=True` stores the
                               serialised form at
                               `metrics["provenance_decision_record"]`
                               (v1 attached the live object → broke
                               `json.dumps`).
  4.  Signature-filtered DR  — `inspect.signature(DecisionRecord)` filters
                               kwargs (v1 raised on contract drift).
  5.  Per-metric flags       — `{metric}_simulated` / `_chaos_injected` /
                               `_hitl_approved` / `_precision` / `_region` /
                               `_federated_from` consulted before falling
                               back to the dict-wide value.
  6.  Federated inference    — `federated_from` forces
                               `source_kind=FEDERATED`.
  7.  Trust level uses       — `TrustLevel` enum; chaos → UNTRUSTWORTHY;
                               HITL denial → UNTRUSTWORTHY;
                               relative_uncertainty > 0.5 → LOW;
                               age >= 3600s → HIGH degrades to MEDIUM;
                               documented DEFAULT → MEDIUM.
  8.  Age recomputation      — `age_seconds` derives from `at` lazily
                               (v1 stored a static 0.0 that made the
                               freshness check impossible).
  9.  `__post_init__`        — validates `source_kind` against the enum;
                               clamps negative `age_seconds` / `uncertainty`;
                               violations recorded on the record.
 10.  `from_dict`            — reconstructs a `MetricProvenance` from
                               `to_dict()` output; warns on invalid
                               source_kind instead of raising.
 11.  `ProvenanceReport`     — collection type; `.sources()`,
                               `.trust_level()`, `.simulated_metrics()`,
                               `.to_dict(safe=True)`, `.to_decision_record()`.
 12.  `to_decision_record()` — stateless standalone builder.
 13.  Per-metric lists       — `summarize_provenance()` returns
                               `simulated_metrics`, `unknown_metrics`,
                               `federated_metrics`, `hitl_denied_metrics`.
 14.  Extended coverage      — pass `default_metrics=DEFAULT_METRICS` for
                               12-metric annotation.
 15.  `in_place=False`       — returns a shallow-copy instead of mutating.
 16.  Thread safety          — `RLock` around stats updates.
 17.  Structured logging     — `provenance.attached`,
                               `provenance.decision_record_failed`,
                               `provenance.reconstruction_failed`.
 18.  `__version__` / `__all__`.

Backward compatibility:
  * `attach_provenance(metrics)` still annotates the v1 four metrics
    (`energy`, `latency`, `carbon`, `framework_overhead`) by default and
    mutates the dict in place. Additive keys
    (`metric_provenance_detailed`, `metric_provenance_summary`) are also
    written — callers asserting on the exact set of keys will see extra
    ones, which the module docstring now states explicitly.
  * `metrics["metric_provenance"]` remains `{metric: string}`.
  * `get_statistics()` fields `attachments` and `metrics_seen` are
    preserved; `metrics_seen` now actually contains the per-metric counts
    (v1 always returned an empty dict due to dead code).
"""

from __future__ import annotations

import inspect
import logging
import math
import threading
from collections import Counter
from dataclasses import dataclass, field, asdict, fields
from datetime import datetime
from enum import Enum
from typing import (
    Any, Dict, Iterable, List, Optional, Tuple,
)

__version__ = "2.0.0"

__all__ = [
    "attach_provenance",
    "attach_provenance_enhanced",
    "MetricProvenance",
    "ProvenanceReport",
    "ProvenanceExplainer",
    "SourceKind",
    "TrustLevel",
    "DEFAULT_METRICS",
    "summarize_provenance",
    "get_provenance",
    "to_decision_record",
    "get_statistics",
    "reset_statistics",
    "__version__",
]

logger = logging.getLogger(__name__)


# =============================================================================
# Enums
# =============================================================================

class SourceKind(Enum):
    """How a metric value was obtained."""
    MEASURED = "measured"
    ESTIMATED = "estimated"
    SIMULATED = "simulated"
    FEDERATED = "federated"
    CACHED = "cached"
    DEFAULT = "default"
    CALIBRATED = "calibrated"
    UNKNOWN = "unknown"


class TrustLevel(Enum):
    """Trust classification derived from source + uncertainty + freshness."""
    HIGH = "high"
    MEDIUM = "medium"
    LOW = "low"
    UNTRUSTWORTHY = "untrustworthy"


_VALID_SOURCE_KINDS = frozenset(s.value for s in SourceKind)
_VALID_TRUST_LEVELS = frozenset(t.value for t in TrustLevel)

#: Relative-uncertainty threshold above which trust degrades to LOW.
_HIGH_RELATIVE_UNCERTAINTY = 0.5
#: Age (seconds) above which a MEASURED value degrades from HIGH to MEDIUM.
_STALE_AFTER_SECONDS = 3600.0


# =============================================================================
# Helpers
# =============================================================================

def _safe_float(v: Any) -> Optional[float]:
    if isinstance(v, bool):
        return None
    try:
        f = float(v)
    except (TypeError, ValueError, OverflowError):
        return None
    return f if math.isfinite(f) else None


def _safe_isoformat(ts: Any) -> Optional[str]:
    try:
        if isinstance(ts, datetime):
            return ts.isoformat()
        return datetime.fromtimestamp(float(ts)).isoformat()
    except (OSError, ValueError, OverflowError, TypeError):
        return None


def _parse_datetime(value: Any) -> datetime:
    if isinstance(value, datetime):
        return value
    if isinstance(value, str):
        try:
            return datetime.fromisoformat(value)
        except (ValueError, TypeError):
            return datetime.now()
    return datetime.now()


def _jsonable(v: Any) -> Any:
    """Recursively coerce to a JSON-serialisable primitive."""
    if v is None or isinstance(v, (bool, int, str)):
        return v
    if isinstance(v, float):
        return v if math.isfinite(v) else None
    if isinstance(v, datetime):
        return v.isoformat()
    if isinstance(v, Enum):
        return v.value
    if isinstance(v, dict):
        return {str(k): _jsonable(x) for k, x in v.items()}
    if isinstance(v, (list, tuple, set, frozenset)):
        return [_jsonable(x) for x in v]
    return repr(v)


def _filter_decision_record_kwargs(
    DecisionRecord: Any, candidates: Dict[str, Any],
) -> Dict[str, Any]:
    try:
        sig = inspect.signature(DecisionRecord)
        accepted = set(sig.parameters.keys())
        return {k: v for k, v in candidates.items() if k in accepted}
    except (ValueError, TypeError):
        return dict(candidates)


def _serialise_decision_record(record: Any) -> Optional[Dict[str, Any]]:
    if record is None:
        return None
    if hasattr(record, "to_dict") and callable(record.to_dict):
        try:
            return record.to_dict()
        except Exception:
            pass
    try:
        return asdict(record)
    except (TypeError, ValueError):
        pass
    try:
        return {
            k: v for k, v in vars(record).items()
            if not k.startswith("_")
        }
    except Exception:
        return None


def _import_decision_record() -> Optional[Any]:
    try:
        from src.analysis import DecisionRecord  # type: ignore
        return DecisionRecord
    except Exception:
        try:
            from analysis import DecisionRecord  # type: ignore
            return DecisionRecord
        except Exception:
            return None


# =============================================================================
# Thread-safe statistics counter
# =============================================================================

class _StatsCounter:
    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._attachments = 0
        self._metrics_attached = 0
        self._by_metric: Counter = Counter()
        self._by_source: Counter = Counter()
        self._by_trust: Counter = Counter()

    def record_attachment(
        self,
        source_kinds: Dict[str, str],
        trust_levels: Dict[str, str],
    ) -> None:
        with self._lock:
            self._attachments += 1
            self._metrics_attached += len(source_kinds)
            for m, sk in source_kinds.items():
                self._by_metric[m] += 1
                self._by_source[sk] += 1
            for tl in trust_levels.values():
                self._by_trust[tl] += 1

    def snapshot(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "attachments": self._attachments,
                "metrics_attached": self._metrics_attached,
                "metrics_seen": dict(self._by_metric),
                "by_source": dict(self._by_source),
                "by_trust": dict(self._by_trust),
            }

    def reset(self) -> None:
        with self._lock:
            self._attachments = 0
            self._metrics_attached = 0
            self._by_metric.clear()
            self._by_source.clear()
            self._by_trust.clear()


_GLOBAL_STATS = _StatsCounter()


# =============================================================================
# MetricProvenance
# =============================================================================

@dataclass
class MetricProvenance:
    """
    Structured provenance for a single metric.

    `from_dict()` accepts both the original 4-key string form and the
    enhanced form so existing `attach_provenance()` output round-trips.
    """
    metric: str
    source_kind: str = SourceKind.UNKNOWN.value
    source_name: Optional[str] = None
    at: datetime = field(default_factory=datetime.now)
    age_seconds: float = 0.0
    uncertainty: Optional[float] = None
    relative_uncertainty: Optional[float] = None
    precision: Optional[str] = None
    region: Optional[str] = None
    simulated: bool = False
    chaos_injected: bool = False
    hitl_approved: Optional[bool] = None
    federated_from: Optional[str] = None
    notes: Optional[str] = None
    bounds_violations: List[str] = field(default_factory=list)

    def __post_init__(self) -> None:
        violations: List[str] = []

        # --- source_kind validation ---
        if self.source_kind not in _VALID_SOURCE_KINDS:
            violations.append(f"invalid_source_kind({self.source_kind!r})")
            self.source_kind = SourceKind.UNKNOWN.value

        # --- at must be a datetime ---
        if not isinstance(self.at, datetime):
            self.at = _parse_datetime(self.at)

        # --- age_seconds must be >= 0 (or None -> derive) ---
        if self.age_seconds is None:
            self.age_seconds = 0.0
        else:
            age = _safe_float(self.age_seconds)
            if age is None or age < 0:
                violations.append("age_seconds_invalid")
                self.age_seconds = 0.0
            else:
                self.age_seconds = age

        # --- uncertainty / relative_uncertainty non-negative ---
        if self.uncertainty is not None:
            u = _safe_float(self.uncertainty)
            if u is None or u < 0:
                violations.append("uncertainty_invalid")
                self.uncertainty = None
            else:
                self.uncertainty = u
        if self.relative_uncertainty is not None:
            ru = _safe_float(self.relative_uncertainty)
            if ru is None or ru < 0:
                violations.append("relative_uncertainty_invalid")
                self.relative_uncertainty = None
            else:
                self.relative_uncertainty = ru

        # --- metric name ---
        if not isinstance(self.metric, str) or not self.metric:
            violations.append("metric_name_invalid")
            self.metric = str(self.metric) if self.metric is not None else "unknown"

        self.bounds_violations = violations

    # ------------------------------------------------------------------
    # Age
    # ------------------------------------------------------------------

    def effective_age_seconds(self, now: Optional[datetime] = None) -> float:
        """
        Derive the age from `at` at read time.

        This makes `trust_level()` correct even when the record was created
        hours ago and the module's freshness gate (3600s) should now
        downgrade a MEASURED value from HIGH to MEDIUM.
        """
        current = now if now is not None else datetime.now()
        try:
            delta = (current - self.at).total_seconds()
        except (TypeError, ValueError):
            return max(0.0, self.age_seconds)
        return max(0.0, delta)

    # ------------------------------------------------------------------
    # Trust
    # ------------------------------------------------------------------

    def trust_level(self) -> TrustLevel:
        """Return a `TrustLevel` enum (see `trust_level_str` for the string)."""
        # --- Hard failures first ---
        if self.chaos_injected:
            return TrustLevel.UNTRUSTWORTHY
        if self.hitl_approved is False:
            return TrustLevel.UNTRUSTWORTHY

        # --- Relative uncertainty downgrade ---
        if (
            self.relative_uncertainty is not None
            and self.relative_uncertainty > _HIGH_RELATIVE_UNCERTAINTY
        ):
            return TrustLevel.LOW

        # --- Source-kind derivation ---
        sk = self.source_kind
        if sk == SourceKind.SIMULATED.value:
            return TrustLevel.LOW
        if sk == SourceKind.UNKNOWN.value:
            return TrustLevel.LOW
        if sk == SourceKind.DEFAULT.value:
            # Documented baseline → MEDIUM; undocumented fallback → LOW
            return (
                TrustLevel.MEDIUM if self.source_name
                else TrustLevel.LOW
            )
        if sk == SourceKind.MEASURED.value:
            age = self.effective_age_seconds()
            if age > _STALE_AFTER_SECONDS:
                return TrustLevel.MEDIUM
            return TrustLevel.HIGH
        if sk == SourceKind.CALIBRATED.value:
            return TrustLevel.HIGH
        if sk in (
            SourceKind.ESTIMATED.value,
            SourceKind.FEDERATED.value,
            SourceKind.CACHED.value,
        ):
            return TrustLevel.MEDIUM
        return TrustLevel.LOW

    def trust_level_str(self) -> str:
        """Return the string form of `trust_level()` for reports."""
        return self.trust_level().value

    # ------------------------------------------------------------------
    # Serialisation
    # ------------------------------------------------------------------

    def to_dict(self, safe: bool = False) -> Dict[str, Any]:
        out = asdict(self)
        out["at"] = self.at.isoformat()
        out["trust_level"] = self.trust_level_str()
        if safe:
            return _jsonable(out)
        return out

    @classmethod
    def from_dict(cls, data: Any) -> Optional["MetricProvenance"]:
        """
        Reconstruct from `to_dict()` output.

        Returns None on non-dict input. Invalid source_kinds are coerced
        to UNKNOWN with a `WARNING` log; other fields that fail validation
        are clamped by `__post_init__`.
        """
        if not isinstance(data, dict):
            logger.warning(
                "provenance.reconstruction_failed",
                extra={"reason": "not_a_dict",
                       "type": type(data).__name__},
            )
            return None
        d = dict(data)

        # Drop derived fields that aren't dataclass inputs
        d.pop("trust_level", None)

        # Coerce `at` back to datetime
        if "at" in d:
            d["at"] = _parse_datetime(d["at"])

        # Restrict to declared fields
        valid_fields = {f.name for f in fields(cls)}
        d = {k: v for k, v in d.items() if k in valid_fields}

        if "source_kind" in d and d["source_kind"] not in _VALID_SOURCE_KINDS:
            logger.warning(
                "provenance.reconstruction_failed",
                extra={
                    "reason": "invalid_source_kind",
                    "source_kind": d["source_kind"],
                },
            )
            d["source_kind"] = SourceKind.UNKNOWN.value

        if "metric" not in d:
            logger.warning(
                "provenance.reconstruction_failed",
                extra={"reason": "missing_metric_name"},
            )
            return None

        try:
            return cls(**d)
        except TypeError as e:
            logger.warning(
                "provenance.reconstruction_failed",
                extra={"reason": str(e)},
            )
            return None

    # ------------------------------------------------------------------
    # DecisionRecord emission
    # ------------------------------------------------------------------

    def to_decision_record(
        self,
        *,
        run_id: Optional[str] = None,
        task_id: Optional[str] = None,
        agent_id: Optional[str] = None,
        policy_version: str = "",
    ) -> Optional[Any]:
        """Emit a single-metric `DecisionRecord`."""
        DecisionRecord = _import_decision_record()
        if DecisionRecord is None:
            return None

        candidates: Dict[str, Any] = {
            "run_id": run_id or "",
            "timestamp": datetime.now(),
            "task_id": task_id or "",
            "selected_action": f"provenance_annotate({self.metric})",
            "alternatives": [],
            "policy_version": policy_version,
            "model_or_agent": agent_id or "",
            "explanation": {
                "metric": self.metric,
                "source_kind": self.source_kind,
                "source_name": self.source_name,
                "trust_level": self.trust_level_str(),
                "simulated": self.simulated,
                "chaos_injected": self.chaos_injected,
                "hitl_approved": self.hitl_approved,
                "federated_from": self.federated_from,
            },
            "provenance": {
                "source": "metric_provenance",
                "analyzer_version": __version__,
                "metric": self.metric,
                "bounds_violations": list(self.bounds_violations),
            },
        }
        filtered = _filter_decision_record_kwargs(DecisionRecord, candidates)
        try:
            return DecisionRecord(**filtered)
        except TypeError as e:
            logger.warning(
                "provenance.decision_record_failed",
                extra={"error": str(e), "metric": self.metric},
            )
            return None


# =============================================================================
# XAI
# =============================================================================

class ProvenanceExplainer:
    @staticmethod
    def explain(prov: MetricProvenance) -> Dict[str, Any]:
        reasons: List[str] = [
            f"'{prov.metric}' provenance: {prov.source_kind}"
            + (f" ({prov.source_name})" if prov.source_name else "")
        ]
        reasons.append(f"Age: {prov.effective_age_seconds():.1f}s.")
        reasons.append(f"Trust level: {prov.trust_level_str()}.")
        if prov.simulated:
            reasons.append(
                "WARNING: value is simulated — exclude from production reports."
            )
        if prov.chaos_injected:
            reasons.append("Value was affected by chaos injection.")
        if prov.hitl_approved is False:
            reasons.append("WARNING: reviewer denied this value.")
        if prov.uncertainty is not None:
            reasons.append(f"Uncertainty: ±{prov.uncertainty}.")
        if prov.relative_uncertainty is not None:
            reasons.append(
                f"Relative uncertainty: {prov.relative_uncertainty:.3f}."
            )
        if prov.federated_from:
            reasons.append(
                f"Received from federated peer: {prov.federated_from}."
            )
        if prov.bounds_violations:
            reasons.append(
                f"Bounds violations: {prov.bounds_violations}."
            )
        return {
            "headline": (
                f"{prov.metric}: {prov.source_kind} "
                f"({prov.trust_level_str()})"
            ),
            "rationale": reasons,
            "trust_level": prov.trust_level_str(),
        }


# =============================================================================
# Source inference
# =============================================================================

def _map_source_label_to_kind(label: str) -> str:
    l = label.lower()
    if "codecarbon" in l or "measured" in l or "sensor" in l:
        return SourceKind.MEASURED.value
    if "fallback" in l or "simulated" in l or "synthetic" in l:
        return SourceKind.SIMULATED.value
    if "calibrated" in l:
        return SourceKind.CALIBRATED.value
    if "federated" in l or "peer" in l:
        return SourceKind.FEDERATED.value
    if "cached" in l or "reuse" in l:
        return SourceKind.CACHED.value
    if "default" in l or "baseline" in l:
        return SourceKind.DEFAULT.value
    if "estimated" in l or "estimate" in l:
        return SourceKind.ESTIMATED.value
    return SourceKind.ESTIMATED.value


def _infer_source_kind(
    metric: str,
    metrics: Dict[str, Any],
) -> Tuple[str, Optional[str]]:
    """
    Infer provenance from existing keys in `metrics`.

    Per-metric hints (`{metric}_source`, `{metric}_federated_from`) are
    consulted before dict-wide flags.
    """
    # --- Per-metric federated override ---
    fed = (
        metrics.get(f"{metric}_federated_from")
        or metrics.get("federated_from")
    )
    if fed:
        return SourceKind.FEDERATED.value, str(fed)

    m_source = metrics.get(f"{metric}_source")

    # --- Energy ---
    if metric == "energy":
        src = m_source or metrics.get("energy_source")
        if src == "codecarbon":
            return SourceKind.MEASURED.value, "codecarbon"
        if src in ("fallback", "estimate"):
            return SourceKind.SIMULATED.value, "energy_fallback"
        if src == "meter":
            return SourceKind.MEASURED.value, "energy_meter"
        if (
            "energy_joules" in metrics or "energy_kwh" in metrics
        ) and not metrics.get("simulated"):
            return SourceKind.MEASURED.value, "energy_meter"
        return SourceKind.ESTIMATED.value, "energy_estimator"

    # --- Latency (v1 returned MEASURED unconditionally) ---
    if metric == "latency":
        src = m_source or metrics.get("latency_source")
        if src == "wall_clock":
            return SourceKind.MEASURED.value, "wall_clock"
        if src in ("estimated", "estimate"):
            return SourceKind.ESTIMATED.value, "latency_estimator"
        if bool(metrics.get(f"{metric}_simulated")) or metrics.get("simulated"):
            return SourceKind.SIMULATED.value, "latency_simulator"
        return SourceKind.MEASURED.value, "timer"

    # --- Carbon (v1 read a dead `framework_overhead_source`) ---
    if metric == "carbon":
        src = m_source or metrics.get("carbon_source")
        grid = metrics.get("grid_source")
        if src == "operational" or grid == "electricitymap":
            return SourceKind.MEASURED.value, "electricitymap"
        if grid == "forecaster" or src == "forecaster":
            return SourceKind.ESTIMATED.value, "carbon_forecaster"
        if src == "fallback" or grid == "grid_average":
            return SourceKind.DEFAULT.value, "grid_average"
        return SourceKind.ESTIMATED.value, "carbon_estimator"

    # --- Framework overhead ---
    if metric == "framework_overhead":
        src = m_source or metrics.get("framework_overhead_source")
        if src == "calibrated":
            return SourceKind.CALIBRATED.value, "framework_overhead"
        if src == "federated":
            return SourceKind.FEDERATED.value, "framework_overhead"
        if src == "hardcoded":
            return SourceKind.DEFAULT.value, "hardcoded_baseline"
        return SourceKind.ESTIMATED.value, "framework_overhead"

    # --- Helium ---
    if metric == "helium":
        src = m_source or metrics.get("helium_source")
        if src == "sensor":
            return SourceKind.MEASURED.value, "helium_monitor"
        return SourceKind.ESTIMATED.value, "helium_estimator"

    # --- Memory ---
    if metric == "memory":
        src = m_source or metrics.get("memory_source")
        if src == "psutil":
            return SourceKind.MEASURED.value, "psutil"
        return SourceKind.MEASURED.value, "process_rss"

    # --- Generic ---
    if m_source:
        return _map_source_label_to_kind(m_source), m_source
    if metric in metrics:
        return SourceKind.ESTIMATED.value, None
    return SourceKind.UNKNOWN.value, None


def _determine_simulated(
    metric: str,
    source_kind: str,
    metrics: Dict[str, Any],
) -> bool:
    """Per-metric override, else source-kind-driven, else dict-wide."""
    per_metric = metrics.get(f"{metric}_simulated")
    if per_metric is not None:
        return bool(per_metric)
    if source_kind == SourceKind.SIMULATED.value:
        return True
    if source_kind in (
        SourceKind.MEASURED.value,
        SourceKind.CALIBRATED.value,
    ):
        return False
    return bool(metrics.get("simulated", False))


def _determine_chaos(
    metric: str, metrics: Dict[str, Any],
) -> bool:
    per_metric = metrics.get(f"{metric}_chaos_injected")
    if per_metric is not None:
        return bool(per_metric)
    return bool(
        metrics.get("chaos_event") or metrics.get("chaos_injected")
    )


def _determine_hitl(
    metric: str, metrics: Dict[str, Any],
) -> Optional[bool]:
    per_metric = metrics.get(f"{metric}_hitl_approved")
    if per_metric is not None:
        return bool(per_metric)
    v = metrics.get("hitl_approved")
    return bool(v) if isinstance(v, bool) else None


def _per_metric_or_global(
    metric: str, field_name: str, metrics: Dict[str, Any],
) -> Any:
    v = metrics.get(f"{metric}_{field_name}")
    if v is not None:
        return v
    return metrics.get(field_name)


# =============================================================================
# Metric coverage
# =============================================================================

DEFAULT_METRICS: Tuple[str, ...] = (
    "energy",
    "carbon",
    "latency",
    "framework_overhead",
    "helium",
    "memory",
    "cost",
    "circuit_depth",
    "variance",
    "precision",
    "tokens",
    "tool_calls",
)

_LEGACY_METRICS: Tuple[str, ...] = (
    "energy",
    "latency",
    "carbon",
    "framework_overhead",
)


# =============================================================================
# ORIGINAL FUNCTION — preserved (v1 behaviour + additive detail)
# =============================================================================

def attach_provenance(metrics: dict) -> dict:
    """
    Attach metric provenance to a metrics dict.

    Backward-compatible: same signature, same return type,
    `metrics["metric_provenance"]` remains a `{metric: string}` dict.

    Additive keys (`metric_provenance_detailed`,
    `metric_provenance_summary`) are written alongside.
    """
    return attach_provenance_enhanced(metrics)


def attach_provenance_enhanced(
    metrics: dict,
    *,
    run_id: Optional[str] = None,
    task_id: Optional[str] = None,
    agent_id: Optional[str] = None,
    default_metrics: Optional[Iterable[str]] = None,
    default_kind: str = SourceKind.ESTIMATED.value,
    emit_decision_record: bool = False,
    policy_version: str = "",
    in_place: bool = True,
) -> dict:
    """
    Enhanced provenance attachment with explicit control.

    `default_metrics=DEFAULT_METRICS` opts into 12-metric coverage;
    the default remains the v1 four-metric list.
    """
    # --- Input validation ---
    if metrics is None or not isinstance(metrics, dict):
        logger.warning(
            "provenance.attached",
            extra={
                "error": "non_dict_metrics",
                "type": type(metrics).__name__,
            },
        )
        return {}

    target = metrics if in_place else dict(metrics)

    # --- Existing provenance (merge source of truth) ---
    existing_simple: Dict[str, Any] = target.get("metric_provenance") or {}
    if not isinstance(existing_simple, dict):
        existing_simple = {}
    existing_detailed: Dict[str, Any] = (
        target.get("metric_provenance_detailed") or {}
    )
    if not isinstance(existing_detailed, dict):
        existing_detailed = {}

    # --- Targets ---
    targets = (
        list(default_metrics) if default_metrics is not None
        else list(_LEGACY_METRICS)
    )

    simple: Dict[str, str] = dict(existing_simple)
    detailed: Dict[str, Any] = dict(existing_detailed)
    new_source_kinds: Dict[str, str] = {}
    new_trust_levels: Dict[str, str] = {}

    now = datetime.now()

    for metric in targets:
        # --- Preserve existing detailed record ---
        if metric in detailed and isinstance(detailed[metric], dict):
            existing_detail = detailed[metric]
            # Sync simple from detailed if missing
            if metric not in simple:
                simple[metric] = existing_detail.get(
                    "source_kind", default_kind,
                )
            # Refresh derived trust level (age may have changed)
            try:
                existing_prov = MetricProvenance.from_dict(existing_detail)
                if existing_prov is not None:
                    detailed[metric] = existing_prov.to_dict()
                    new_trust_levels[metric] = (
                        existing_prov.trust_level_str()
                    )
            except Exception:
                pass
            new_source_kinds[metric] = simple.get(metric, default_kind)
            continue

        # --- Caller-supplied simple value is authoritative ---
        if metric in simple:
            caller_kind = simple[metric]
            if caller_kind not in _VALID_SOURCE_KINDS:
                logger.warning(
                    "provenance.attached",
                    extra={
                        "metric": metric,
                        "reason": "unknown_caller_source_kind",
                        "value": caller_kind,
                    },
                )
                caller_kind = default_kind
                simple[metric] = caller_kind
            # Still infer the source_name for the detailed view
            _, inferred_name = _infer_source_kind(metric, target)
            source_kind = caller_kind
            source_name = inferred_name
        else:
            source_kind, source_name = _infer_source_kind(metric, target)
            simple[metric] = source_kind

        # --- Simulation flag ---
        simulated = _determine_simulated(metric, source_kind, target)

        # --- Chaos flag ---
        chaos_injected = _determine_chaos(metric, target)
        if chaos_injected:
            simulated = True

        # --- HITL flag ---
        hitl_approved = _determine_hitl(metric, target)

        # --- Federated origin ---
        federated_from = (
            target.get(f"{metric}_federated_from")
            or target.get("federated_from")
        )

        # --- Per-metric precision / region / uncertainty ---
        precision = _per_metric_or_global(
            metric, "precision", target,
        )
        region = _per_metric_or_global(metric, "region", target)
        uncertainty = _per_metric_or_global(
            metric, "uncertainty", target,
        )

        # --- Relative uncertainty (when both value + uncertainty numeric) ---
        relative_uncertainty: Optional[float] = None
        value = target.get(metric)
        v_abs = abs(_safe_float(value) or 0.0)
        u = _safe_float(uncertainty)
        if u is not None and v_abs > 0:
            relative_uncertainty = u / v_abs

        # --- Build the record ---
        try:
            prov = MetricProvenance(
                metric=metric,
                source_kind=source_kind,
                source_name=source_name,
                at=now,
                age_seconds=0.0,
                uncertainty=u,
                relative_uncertainty=relative_uncertainty,
                precision=precision if isinstance(precision, str) else None,
                region=region if isinstance(region, str) else None,
                simulated=simulated,
                chaos_injected=chaos_injected,
                hitl_approved=hitl_approved,
                federated_from=(
                    str(federated_from) if federated_from else None
                ),
            )
            detailed[metric] = prov.to_dict()
            new_source_kinds[metric] = source_kind
            new_trust_levels[metric] = prov.trust_level_str()
        except Exception as e:
            logger.warning(
                "provenance.attached",
                extra={"metric": metric, "error": str(e)},
            )

    # --- Attach ---
    target["metric_provenance"] = simple
    target["metric_provenance_detailed"] = detailed

    # --- Summary (recomputed from the current detailed view) ---
    simulated_metrics = sorted(
        m for m, d in detailed.items()
        if isinstance(d, dict) and d.get("simulated")
    )
    unknown_metrics = sorted(
        m for m, d in detailed.items()
        if isinstance(d, dict)
        and d.get("source_kind") == SourceKind.UNKNOWN.value
    )
    summary_lines = [f"{len(simple)} metrics annotated."]
    if simulated_metrics:
        summary_lines.append(
            f"{len(simulated_metrics)} metric(s) simulated: "
            f"{simulated_metrics}."
        )
    if unknown_metrics:
        summary_lines.append(
            f"{len(unknown_metrics)} metric(s) unknown source: "
            f"{unknown_metrics}."
        )
    target["metric_provenance_summary"] = {
        "headline": f"{len(simple)} metrics annotated",
        "rationale": summary_lines,
        "simulated_count": len(simulated_metrics),
        "unknown_count": len(unknown_metrics),
        "simulated_metrics": simulated_metrics,
        "unknown_metrics": unknown_metrics,
    }

    # --- Stats ---
    _GLOBAL_STATS.record_attachment(new_source_kinds, new_trust_levels)

    # --- Optional DecisionRecord emission (JSON-safe) ---
    if emit_decision_record:
        dr = _emit_decision_record(
            metrics=target,
            run_id=run_id,
            task_id=task_id,
            agent_id=agent_id,
            policy_version=policy_version,
            simple=simple,
            detailed=detailed,
        )
        if dr is not None:
            serialised = _serialise_decision_record(dr)
            if serialised is not None:
                target["provenance_decision_record"] = _jsonable(serialised)

    # --- Structured log ---
    logger.info(
        "provenance.attached",
        extra={
            "run_id": run_id,
            "task_id": task_id,
            "agent_id": agent_id,
            "n_metrics": len(simple),
            "simulated_metrics": simulated_metrics,
            "unknown_metrics": unknown_metrics,
            "emit_decision_record": emit_decision_record,
        },
    )
    return target


# =============================================================================
# DecisionRecord emission
# =============================================================================

def _emit_decision_record(
    *,
    metrics: Dict[str, Any],
    run_id: Optional[str],
    task_id: Optional[str],
    agent_id: Optional[str],
    policy_version: str,
    simple: Dict[str, Any],
    detailed: Dict[str, Any],
) -> Optional[Any]:
    DecisionRecord = _import_decision_record()
    if DecisionRecord is None:
        logger.debug("provenance.decision_record_import_failed")
        return None

    candidates: Dict[str, Any] = {
        "run_id": run_id or "",
        "timestamp": datetime.now(),
        "task_id": task_id or "",
        "selected_action": "attach_provenance",
        "alternatives": [],
        "policy_version": policy_version,
        "model_or_agent": agent_id or "",
        "explanation": {
            "metric_provenance": dict(simple),
            "metric_provenance_detailed":
                _jsonable(dict(detailed)),
            "summary":
                _jsonable(metrics.get("metric_provenance_summary", {})),
        },
        "provenance": {
            "source": "metric_provenance",
            "analyzer_version": __version__,
            "n_metrics": len(simple),
        },
    }
    filtered = _filter_decision_record_kwargs(DecisionRecord, candidates)
    try:
        return DecisionRecord(**filtered)
    except TypeError as e:
        logger.warning(
            "provenance.decision_record_failed",
            extra={"error": str(e)},
        )
        return None


def to_decision_record(
    metrics: Dict[str, Any],
    *,
    run_id: Optional[str] = None,
    task_id: Optional[str] = None,
    agent_id: Optional[str] = None,
    policy_version: str = "",
) -> Optional[Any]:
    """
    Stateless `DecisionRecord` builder for the current provenance state
    of `metrics`.

    Uses the serialised form already present at
    `metrics["provenance_decision_record"]` when available, or rebuilds
    from `metric_provenance` / `metric_provenance_detailed` otherwise.
    """
    if not isinstance(metrics, dict):
        return None
    simple = metrics.get("metric_provenance") or {}
    detailed = metrics.get("metric_provenance_detailed") or {}
    if not isinstance(simple, dict) or not isinstance(detailed, dict):
        return None
    return _emit_decision_record(
        metrics=metrics,
        run_id=run_id,
        task_id=task_id,
        agent_id=agent_id,
        policy_version=policy_version,
        simple=simple,
        detailed=detailed,
    )


# =============================================================================
# ProvenanceReport — collection type
# =============================================================================

class ProvenanceReport:
    """
    Collection-level view over a metrics dict's provenance.
    """

    def __init__(self, metrics: Dict[str, Any]) -> None:
        self._simple: Dict[str, Any] = (
            dict(metrics.get("metric_provenance") or {})
            if isinstance(metrics, dict) else {}
        )
        self._detailed: Dict[str, Any] = (
            dict(metrics.get("metric_provenance_detailed") or {})
            if isinstance(metrics, dict) else {}
        )
        self._summary: Dict[str, Any] = (
            dict(metrics.get("metric_provenance_summary") or {})
            if isinstance(metrics, dict) else {}
        )
        self._metrics_dict: Dict[str, Any] = (
            metrics if isinstance(metrics, dict) else {}
        )

    @classmethod
    def from_metrics(cls, metrics: Any) -> "ProvenanceReport":
        return cls(metrics if isinstance(metrics, dict) else {})

    # -- Introspection --------------------------------------------------

    def metric_names(self) -> List[str]:
        return sorted(self._simple.keys())

    def sources(self, metric: str) -> List[str]:
        """Return source names for a metric (source_kind, then source_name)."""
        d = self._detailed.get(metric)
        out: List[str] = []
        if isinstance(d, dict):
            kind = d.get("source_kind")
            name = d.get("source_name")
            if kind:
                out.append(kind)
            if name and name != kind:
                out.append(name)
            if not out:
                simple = self._simple.get(metric)
                if simple:
                    out.append(simple)
            return out
        simple = self._simple.get(metric)
        return [simple] if simple else []

    def trust_level(self, metric: str) -> str:
        d = self._detailed.get(metric)
        if isinstance(d, dict):
            tl = d.get("trust_level")
            if isinstance(tl, str) and tl in _VALID_TRUST_LEVELS:
                return tl
        return TrustLevel.LOW.value

    def simulated_metrics(self) -> List[str]:
        return sorted(
            m for m, d in self._detailed.items()
            if isinstance(d, dict) and d.get("simulated")
        )

    def federated_metrics(self) -> List[str]:
        return sorted(
            m for m, d in self._detailed.items()
            if isinstance(d, dict) and d.get("federated_from")
        )

    def unknown_metrics(self) -> List[str]:
        return sorted(
            m for m in self._simple
            if self._simple[m] == SourceKind.UNKNOWN.value
        )

    def hitl_denied_metrics(self) -> List[str]:
        return sorted(
            m for m, d in self._detailed.items()
            if isinstance(d, dict) and d.get("hitl_approved") is False
        )

    def summary(self) -> Dict[str, Any]:
        return dict(self._summary)

    # -- Serialisation --------------------------------------------------

    def to_dict(self, safe: bool = False) -> Dict[str, Any]:
        out = {
            "metric_provenance": dict(self._simple),
            "metric_provenance_detailed": dict(self._detailed),
            "summary": dict(self._summary),
        }
        if safe:
            return _jsonable(out)
        return out

    # -- DecisionRecord --------------------------------------------------

    def to_decision_record(
        self,
        *,
        run_id: Optional[str] = None,
        task_id: Optional[str] = None,
        agent_id: Optional[str] = None,
        policy_version: str = "",
    ) -> Optional[Any]:
        return _emit_decision_record(
            metrics=self._metrics_dict,
            run_id=run_id,
            task_id=task_id,
            agent_id=agent_id,
            policy_version=policy_version,
            simple=self._simple,
            detailed=self._detailed,
        )


# =============================================================================
# Summaries and accessors
# =============================================================================

def summarize_provenance(metrics: Dict[str, Any]) -> Dict[str, Any]:
    """Summarize provenance attached to a metrics dict."""
    if not isinstance(metrics, dict):
        return {
            "total_metrics": 0,
            "by_source": {},
            "by_trust_level": {},
            "simulated_metrics": [],
            "unknown_metrics": [],
            "federated_metrics": [],
            "hitl_denied_metrics": [],
        }
    report = ProvenanceReport.from_metrics(metrics)
    simple = report._simple
    detailed = report._detailed
    by_source: Counter = Counter(simple.values())
    by_trust: Counter = Counter()
    for d in detailed.values():
        if isinstance(d, dict):
            tl = d.get("trust_level")
            if isinstance(tl, str):
                by_trust[tl] += 1
    return {
        "total_metrics": len(simple),
        "by_source": dict(by_source),
        "by_trust_level": dict(by_trust),
        "simulated_metrics": report.simulated_metrics(),
        "unknown_metrics": report.unknown_metrics(),
        "federated_metrics": report.federated_metrics(),
        "hitl_denied_metrics": report.hitl_denied_metrics(),
    }


def get_provenance(
    metrics: Dict[str, Any], metric: str,
) -> Optional[MetricProvenance]:
    """Return a `MetricProvenance` for one metric, or None."""
    if not isinstance(metrics, dict):
        return None
    detailed = metrics.get("metric_provenance_detailed")
    if not isinstance(detailed, dict):
        return None
    d = detailed.get(metric)
    if not isinstance(d, dict):
        return None
    return MetricProvenance.from_dict(d)


# =============================================================================
# Module-level statistics shims
# =============================================================================

def get_statistics() -> Dict[str, Any]:
    """Module-level statistics for `attach_provenance*` calls."""
    snap = _GLOBAL_STATS.snapshot()
    return {
        "analyzer_version": __version__,
        "attachments": snap["attachments"],
        "metrics_attached": snap["metrics_attached"],
        "metrics_seen": snap["metrics_seen"],
        "by_source": snap["by_source"],
        "by_trust": snap["by_trust"],
    }


def reset_statistics() -> None:
    """Clear module-level statistics."""
    _GLOBAL_STATS.reset()


# =============================================================================
# Demo
# =============================================================================

if __name__ == "__main__":
    import json
    import sys
    import threading
    import types

    logging.basicConfig(level=logging.WARNING)
    reset_statistics()

    # =================================================================
    # 1. Original behaviour (backward compatible)
    # =================================================================
    print("=== 1. Original behaviour ===")
    m = {"energy": 0.05, "latency": 120.0, "carbon": 0.02}
    out = attach_provenance(m)
    print(f"  metric_provenance: {out['metric_provenance']}")
    print(f"  return is the same dict: {out is m}")

    # =================================================================
    # 2. Merge preservation (v1 silently overwrote)
    # =================================================================
    print("\n=== 2. Merge preservation ===")
    m_merge = {
        "energy": 0.05,
        "metric_provenance": {"energy": "calibrated"},
    }
    attach_provenance(m_merge)
    print(f"  preserved: {m_merge['metric_provenance']['energy']}  "
          f"(expected 'calibrated', NOT 'estimated')")
    detail = m_merge["metric_provenance_detailed"]["energy"]
    print(f"  detailed source_kind: {detail['source_kind']}  "
          f"(honours caller)")

    # =================================================================
    # 3. Fallback energy (v1 'measured' → now 'simulated')
    # =================================================================
    print("\n=== 3. Fallback energy ===")
    m_fb = {"energy": 0.05, "energy_source": "fallback"}
    attach_provenance(m_fb)
    prov = m_fb["metric_provenance_detailed"]["energy"]
    print(f"  source_kind: {prov['source_kind']}  "
          f"(expected 'simulated')")
    print(f"  simulated:   {prov['simulated']}")
    print(f"  trust:       {prov['trust_level']}  (expected 'low')")

    # =================================================================
    # 4. Latency respects `simulated` (v1 always MEASURED)
    # =================================================================
    print("\n=== 4. Latency respects `simulated` ===")
    m_lat_sim = {"latency": 100.0, "simulated": True}
    attach_provenance(m_lat_sim)
    lat = m_lat_sim["metric_provenance_detailed"]["latency"]
    print(f"  simulated latency → source_kind: {lat['source_kind']}  "
          f"(expected 'simulated')")
    print(f"  trust: {lat['trust_level']}  (expected 'low')")

    # Real measured latency (no simulated flag)
    m_lat_meas = {"latency": 100.0, "latency_source": "wall_clock"}
    attach_provenance(m_lat_meas)
    lat2 = m_lat_meas["metric_provenance_detailed"]["latency"]
    print(f"  wall_clock latency → trust: {lat2['trust_level']}  "
          f"(expected 'high')")

    # =================================================================
    # 5. Chaos injection → untrustworthy
    # =================================================================
    print("\n=== 5. Chaos injection ===")
    m_chaos = {"energy": 0.15, "chaos_event": "energy_spike",
               "energy_source": "codecarbon"}
    attach_provenance(m_chaos)
    ch = m_chaos["metric_provenance_detailed"]["energy"]
    print(f"  chaos_injected: {ch['chaos_injected']}")
    print(f"  simulated:      {ch['simulated']}  (chaos forces simulated)")
    print(f"  trust:          {ch['trust_level']}  (expected 'untrustworthy')")

    # Per-metric chaos — only energy affected
    m_pchaos = {
        "energy": 0.15, "latency": 100.0,
        "energy_source": "codecarbon",
        "energy_chaos_injected": True,
    }
    attach_provenance(m_pchaos)
    print(f"\n  per-metric chaos:")
    print(f"    energy chaos:  "
          f"{m_pchaos['metric_provenance_detailed']['energy']['chaos_injected']}")
    print(f"    latency chaos: "
          f"{m_pchaos['metric_provenance_detailed']['latency']['chaos_injected']}")

    # =================================================================
    # 6. Federated origin forces FEDERATED
    # =================================================================
    print("\n=== 6. Federated origin ===")
    m_fed = {"energy": 0.05, "federated_from": "peer-us-east-1"}
    attach_provenance(m_fed)
    fed = m_fed["metric_provenance_detailed"]["energy"]
    print(f"  source_kind:    {fed['source_kind']}  "
          f"(expected 'federated')")
    print(f"  federated_from: {fed['federated_from']}")
    print(f"  trust:          {fed['trust_level']}  (expected 'medium')")

    # =================================================================
    # 7. HITL denial → untrustworthy
    # =================================================================
    print("\n=== 7. HITL denial ===")
    m_hitl = {
        "energy": 0.05, "energy_source": "codecarbon",
        "energy_hitl_approved": False,
    }
    attach_provenance(m_hitl)
    hi = m_hitl["metric_provenance_detailed"]["energy"]
    print(f"  hitl_approved: {hi['hitl_approved']}")
    print(f"  trust:         {hi['trust_level']}  "
          f"(expected 'untrustworthy')")

    # =================================================================
    # 8. Per-metric precision / region
    # =================================================================
    print("\n=== 8. Per-metric precision / region ===")
    m_pm = {
        "energy": 0.05,
        "precision": "fp32",         # global default
        "energy_precision": "int8",  # per-metric override
        "region": "US",
        "energy_region": "EU-North",
    }
    attach_provenance(m_pm)
    pm = m_pm["metric_provenance_detailed"]["energy"]
    print(f"  precision: {pm['precision']}  (expected 'int8')")
    print(f"  region:    {pm['region']}  (expected 'EU-North')")

    # =================================================================
    # 9. ProvenanceReport collection API
    # =================================================================
    print("\n=== 9. ProvenanceReport collection API ===")
    m_report = {
        "energy": 0.05,
        "energy_source": "codecarbon",
        "latency": 100.0,
        "carbon": 0.02,
        "helium": 0.001,
        "chaos_event": "partial",
    }
    attach_provenance_enhanced(
        m_report, default_metrics=DEFAULT_METRICS,
    )
    report = ProvenanceReport.from_metrics(m_report)
    print(f"  metric_names:      {report.metric_names()[:5]}...")
    print(f"  sources('energy'): {report.sources('energy')}")
    print(f"  trust('energy'):   {report.trust_level('energy')}")
    print(f"  simulated:         {report.simulated_metrics()[:3]}...")
    print(f"  unknown:           {report.unknown_metrics()}")

    # =================================================================
    # 10. MetricProvenance.from_dict round-trip
    # =================================================================
    print("\n=== 10. from_dict round-trip ===")
    original = MetricProvenance(
        metric="energy",
        source_kind="measured",
        source_name="codecarbon",
        simulated=False,
        uncertainty=0.005,
    )
    rt = MetricProvenance.from_dict(original.to_dict())
    print(f"  reconstructed: {rt.metric} / {rt.source_kind} / "
          f"{rt.trust_level_str()}")
    print(f"  age recompute (fresh): {rt.effective_age_seconds():.2f}s")

    # Age degradation: construct with an old `at`
    old_time = datetime.now() - __import__("datetime").timedelta(hours=2)
    old_prov = MetricProvenance(
        metric="energy", source_kind="measured",
        source_name="codecarbon", at=old_time,
    )
    print(f"  age recompute (2h old): "
          f"{old_prov.effective_age_seconds():.0f}s")
    print(f"  trust after 2h:         {old_prov.trust_level_str()}  "
          f"(expected 'medium' — HIGH → MEDIUM)")

    # =================================================================
    # 11. JSON-safe DecisionRecord emission
    # =================================================================
    print("\n=== 11. JSON-safe DecisionRecord emission ===")

    class _StrictDR:
        def __init__(self, run_id, timestamp, task_id, selected_action,
                     explanation, provenance):
            self.run_id = run_id
            self.timestamp = timestamp
            self.task_id = task_id
            self.selected_action = selected_action
            self.explanation = explanation
            self.provenance = provenance

    fake_mod = types.ModuleType("analysis")
    fake_mod.DecisionRecord = _StrictDR
    sys.modules["analysis"] = fake_mod

    # Force re-import so the module picks up the fake
    import importlib
    import src.analysis.telemetry.metric_provenance as _mp_mod
    importlib.reload(_mp_mod)

    try:
        m_dr = {"energy": 0.05, "energy_source": "codecarbon"}
        _mp_mod.attach_provenance_enhanced(
            m_dr, run_id="run-1", emit_decision_record=True,
        )
        # Prove the metrics dict is JSON-safe
        try:
            json.dumps(m_dr)
            print("  json.dumps(metrics): OK")
        except (TypeError, ValueError) as e:
            print(f"  FAILED: {e}")
        print(f"  serialised DR present: "
              f"{'provenance_decision_record' in m_dr}")
        if "provenance_decision_record" in m_dr:
            print(f"  DR type: "
                  f"{type(m_dr['provenance_decision_record']).__name__}  "
                  f"(expected dict)")
            print(f"  DR selected_action: "
                  f"{m_dr['provenance_decision_record'].get('selected_action')}")
    finally:
        del sys.modules["analysis"]
        importlib.reload(_mp_mod)

    # =================================================================
    # 12. `in_place=False` leaves the caller's dict untouched
    # =================================================================
    print("\n=== 12. in_place=False ===")
    m_copy = {"energy": 0.05}
    out_copy = attach_provenance_enhanced(m_copy, in_place=False)
    print(f"  original keys: {sorted(m_copy.keys())}  (expected ['energy'])")
    print(f"  copy keys:     "
          f"{sorted(k for k in out_copy if k != 'metric_provenance_detailed')}")

    # =================================================================
    # 13. Thread safety
    # =================================================================
    print("\n=== 13. Thread safety ===")
    reset_statistics()
    barrier = threading.Barrier(4)

    def _worker():
        barrier.wait()
        for _ in range(100):
            attach_provenance({"energy": 0.05})

    threads = [threading.Thread(target=_worker) for _ in range(4)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    stats = get_statistics()
    print(f"  attachments: {stats['attachments']}  (expected 400)")
    print(f"  metrics_attached: {stats['metrics_attached']}  "
          f"(expected 4 × 400 × 4 = per-metric count)" if False
          else f"  metrics_attached: {stats['metrics_attached']}")
    print(f"  metrics_seen: {stats['metrics_seen']}")

    # =================================================================
    # 14. Extended 12-metric coverage
    # =================================================================
    print("\n=== 14. Extended 12-metric coverage ===")
    m_ext = {"energy": 0.05, "latency": 100.0, "memory": 512}
    attach_provenance_enhanced(
        m_ext, default_metrics=DEFAULT_METRICS,
    )
    print(f"  annotated metrics: {len(m_ext['metric_provenance'])}  "
          f"(expected 12)")
    print(f"  annotated names:   "
          f"{sorted(m_ext['metric_provenance'].keys())}")

    # =================================================================
    # 15. summarize_provenance with metric lists
    # =================================================================
    print("\n=== 15. summarize_provenance ===")
    summary = summarize_provenance(m_report)
    print(f"  total_metrics:      {summary['total_metrics']}")
    print(f"  by_source:          {summary['by_source']}")
    print(f"  simulated_metrics:  {summary['simulated_metrics']}")
    print(f"  unknown_metrics:    {summary['unknown_metrics']}")

    # =================================================================
    # 16. Invalid input safety
    # =================================================================
    print("\n=== 16. Invalid input safety ===")
    print(f"  attach_provenance(None) → {attach_provenance(None)}")
    print(f"  MetricProvenance.from_dict(None) → "
          f"{MetricProvenance.from_dict(None)}")
    bad = MetricProvenance(
        metric="x", source_kind="banana",
        age_seconds=-5.0, uncertainty=-1.0,
    )
    print(f"  invalid source_kind → {bad.source_kind}  "
          f"(expected 'unknown')")
    print(f"  violations: {bad.bounds_violations}")

    # =================================================================
    # 17. Full statistics
    # =================================================================
    print("\n=== 17. Full statistics ===")
    print(json.dumps(get_statistics(), indent=2, default=str))
