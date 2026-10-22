# src/analysis/telemetry/metrics_collector.py
"""
Metrics collection for Green Agent (Enhanced, v2.0.0)
=====================================================

Collects core execution metrics — CPU time, memory, latency, tool calls,
reasoning depth — plus energy, carbon, helium, precision, tokens, and
per-event timestamps.

Original API preserved (all signatures, all side effects):
    collector = MetricsCollector()
    collector.record_tool()
    collector.record_depth(3)
    metrics = collector.finalize()      # dict superset of the v1 four keys

Enhanced API:
    collector = MetricsCollector(
        run_id="...", task_id="...", agent_id="...",
        deployment_id="us-ca-prod-01",
        precision="int8", region="US-CA",
        energy_meter=meter, carbon_estimator=est, callback=cb,
        energy_meter_measure_unit="joules",   # for non-enhanced meters
        features={...},
    )

    collector.record_llm_call()
    collector.record_tokens(120)
    collector.record_chaos_event("energy_spike")
    collector.record_shorten_context_factor(0.5)

    metrics = collector.finalize(emit_decision_record=False)  # dict
    snap    = collector.finalize_detailed()                   # MetricsSnapshot
    metrics = collector.attach_provenance()                   # bridge
    dr      = collector.to_decision_record(policy_version="v5.0.1")

    collector.set_hitl_callback(cb)     # (snapshot) -> bool | None
    collector.set_trace(trace)          # ExecutionTrace sink
    collector.set_energy_meter(meter)
    collector.set_carbon_estimator(est)
    collector.set_callback(cb)

    collector.get_statistics()
    collector.get_last_snapshot()
    collector.get_last_decision_record()
    collector.reset(full=False)         # counters only
    collector.reset(full=True)          # also clears wiring
    collector.export()

Enhancements implemented (v2.0.0):
  1.  Single-build finalize      — `finalize()` builds a snapshot exactly
                                  once, caches it unconditionally, and
                                  `finalize_detailed()` returns that same
                                  cached object. v1's `_build_snapshot()`
                                  path silently rebuilt a *partial* snapshot
                                  (dropping energy source, depth stats,
                                  severity, HITL, explanation) — that path
                                  is gone.
  2.  Working provenance bridge  — `attach_provenance()` translates the
                                  snapshot's keys (`energy_wh`, `carbon_g`,
                                  `latency_s`, `memory_mb`) into the metric
                                  names `metric_provenance` expects
                                  (`energy`, `carbon`, `latency`,
                                  `memory`), passes the collector's
                                  authoritative source labels, and then
                                  post-patches the inferred `source_kind`
                                  so a caller's measured value cannot be
                                  silently reported as `unknown`.
  3.  `_shorten_factor` lives    — initialised to `1.0` in `__init__`,
                                  propagated into `MetricsSnapshot`,
                                  surfaced in the explanation and the
                                  DecisionRecord provenance, and reset on
                                  `reset()`. v1 stored it nowhere it was
                                  read, and `reset()` left it undefined.
  4.  Thread-safe statistics     — `_StatsCounter` with `RLock`; per-call
                                  deltas; `races_prevented` wired to the
                                  idempotent short-circuit.
  5.  Honest energy meter path   — prefers `measure_detailed()` (uses its
                                  `energy_joules`, `source`, `simulated`,
                                  `precision`, `uncertainty_joules`),
                                  falls back to `joules()` and then
                                  `measure()` with an explicit unit kwarg.
                                  Never reports `energy_source="measured"`
                                  when the meter returned `simulated=True`.
  6.  Signature-filtered DR      — `inspect.signature(DecisionRecord)`
                                  filters kwargs; `TypeError` returns None
                                  with a warning (v1 propagated).
  7.  Extended bounds            — energy/carbon negatives flagged in
                                  `bounds_violations`; severity derives
                                  from bounds + HITL + simulated.
  8.  HITL callback              — `set_hitl_callback(cb)` for extreme
                                  memory / latency / bounds; denial
                                  appends `[HITL: denied]`.
  9.  ExecutionTrace forwarding  — `set_trace(trace)` records a summary
                                  step per `finalize()`.
 10.  Event-timestamp stats      — `event_count`, `mean_inter_event_gap_ms`,
                                  `max_inter_event_gap_ms` on the snapshot
                                  (v1 appended timestamps and never read
                                  them).
 11.  Sample-std alongside std   — `depth_sample_std` (ddof=1) and
                                  `depth_std` (ddof=0) both reported.
 12.  Chaos-aware snapshots      — `record_chaos_event(name)` promotes
                                  `simulated` and flags `chaos_injected`.
 13.  Carbon contract validated  — finite, non-negative; violations noted
                                  rather than propagated silently.
 14.  `psutil_fallback=False`    — strict path re-raises with a WARNING
                                  log; documented as v1-strict.
 15.  Meter precision adoption   — `getattr(meter, "precision", None)`
                                  wins over the constructor default when
                                  the constructor didn't set it.
 16.  `to_dict(safe=True)`       — `_jsonable` recursion on
                                  `MetricsSnapshot`; `json.dumps` always
                                  succeeds.
 17.  Per-instance statistics    — `_StatsCounter` per collector plus
                                  module-level shims.
 18.  `reset(full=False|True)`   — counters vs. wiring.
 19.  Structured logging         — `metrics.finalized`,
                                  `metrics.energy_meter_failed`,
                                  `metrics.carbon_estimator_failed`,
                                  `metrics.psutil_failed`,
                                  `metrics.provenance_bridge_failed`.
 20.  `__version__` / `__all__`.

Backward compatibility:
  * `collector.start` is still a plain attribute (v1 wrote to it); the
    snapshot's `started_at` mirrors it.
  * `record_tool()` / `record_depth(depth)` / `finalize()` signatures
    unchanged.
  * `finalize()` returns a dict; the four v1 keys (`latency_s`,
    `memory_mb`, `tool_calls`, `conversation_depth`) are always present.
    Additional keys are additive.
  * `attach_provenance()` returns a dict; when the bridge is unavailable
    it returns the translated dict with `metric_provenance = {}` and
    `_provenance_error = "<reason>"` so callers can detect the failure
    (v1 returned the input unchanged with no marker).
  * `get_statistics()` keys `collectors_created` / `finalizations` /
    `psutil_failures` / `concurrent_races_prevented` are preserved;
    `concurrent_races_prevented` now actually increments on idempotent
    short-circuits.
"""

from __future__ import annotations

import inspect
import logging
import math
import threading
import time
import uuid
from collections import Counter, deque
from dataclasses import dataclass, field, asdict, is_dataclass
from datetime import datetime
from enum import Enum
from typing import (
    Any, Callable, Deque, Dict, Iterable, List, Optional, Tuple,
)

try:
    import psutil  # type: ignore
    _HAS_PSUTIL = True
except Exception:  # pragma: no cover
    psutil = None  # type: ignore
    _HAS_PSUTIL = False

__version__ = "2.0.0"

__all__ = [
    "MetricsCollector",
    "MetricsSnapshot",
    "MetricsExplainer",
    "MetricSource",
    "Severity",
    "get_statistics",
    "reset_statistics",
    "__version__",
]

logger = logging.getLogger(__name__)


# =============================================================================
# Enums
# =============================================================================

class MetricSource(Enum):
    """Where a metric came from."""
    MEASURED = "measured"
    ESTIMATED = "estimated"
    SIMULATED = "simulated"
    FALLBACK = "fallback"
    UNKNOWN = "unknown"


class Severity(Enum):
    INFO = "info"
    WARNING = "warning"
    CRITICAL = "critical"
    EMERGENCY = "emergency"


_VALID_SOURCES = frozenset(s.value for s in MetricSource)
_VALID_SEVERITIES = frozenset(s.value for s in Severity)
_VALID_ENERGY_UNITS = frozenset({"joules", "j", "wh", "kwh"})


# =============================================================================
# Helpers
# =============================================================================

def _safe_float(v: Any, default: float = 0.0) -> float:
    """Coerce to finite float; return `default` on any failure."""
    if isinstance(v, bool):
        return default
    try:
        f = float(v)
    except (TypeError, ValueError, OverflowError):
        return default
    return f if math.isfinite(f) else default


def _safe_int(v: Any, default: int = 0) -> int:
    """Coerce to non-negative int; return `default` on any failure."""
    if isinstance(v, bool):
        return default
    try:
        n = int(v)
    except (TypeError, ValueError, OverflowError):
        return default
    return n if 0 <= n <= 1_000_000_000 else default


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
    if is_dataclass(v) and not isinstance(v, type):
        try:
            return _jsonable(asdict(v))
        except Exception:
            pass
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


def _to_joules(value: float, unit: str) -> float:
    u = (unit or "joules").strip().lower()
    if u in ("joules", "j"):
        return value
    if u == "wh":
        return value * 3600.0
    if u == "kwh":
        return value * 3_600_000.0
    raise ValueError(f"Unknown energy unit: {unit!r}")


# =============================================================================
# Thread-safe statistics counter
# =============================================================================

class _StatsCounter:
    """Minimal thread-safe counter."""

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._counts: Counter = Counter()

    def increment(self, key: str, amount: int = 1) -> None:
        with self._lock:
            self._counts[key] += amount

    def snapshot(self) -> Dict[str, int]:
        with self._lock:
            return dict(self._counts)

    def reset(self) -> None:
        with self._lock:
            self._counts.clear()


_GLOBAL_STATS = _StatsCounter()


def get_statistics() -> Dict[str, Any]:
    """Module-level statistics for `MetricsCollector` calls."""
    snap = _GLOBAL_STATS.snapshot()
    return {
        "analyzer_version": __version__,
        "collectors_created": snap.get("collectors", 0),
        "finalizations": snap.get("finalizations", 0),
        "psutil_failures": snap.get("psutil_failures", 0),
        "concurrent_races_prevented": snap.get("races_prevented", 0),
    }


def reset_statistics() -> None:
    """Clear module-level statistics."""
    _GLOBAL_STATS.reset()


# =============================================================================
# MetricsSnapshot
# =============================================================================

@dataclass
class MetricsSnapshot:
    """
    Rich result of a completed collection.

    `to_dict()` returns a dict containing all v1 keys plus the enhanced
    fields. `to_dict(safe=True)` sanitises non-primitive values so
    `json.dumps` always succeeds.
    """
    # --- v1 fields (preserved names) ---
    run_id: str
    task_id: Optional[str]
    agent_id: Optional[str]
    latency_s: float
    memory_mb: float
    tool_calls: int
    conversation_depth: int

    # --- Extended metrics ---
    energy_wh: float = 0.0
    carbon_g: float = 0.0
    helium_units: float = 0.0
    llm_calls: int = 0
    tokens: int = 0

    # --- Context ---
    precision: Optional[str] = None
    region: Optional[str] = None
    pue: Optional[float] = None
    hardware_profile: Optional[str] = None
    deployment_id: str = "local"

    # --- Provenance labels ---
    memory_source: str = MetricSource.UNKNOWN.value
    latency_source: str = MetricSource.MEASURED.value
    energy_source: str = MetricSource.UNKNOWN.value
    carbon_source: str = MetricSource.UNKNOWN.value
    simulated: bool = False
    chaos_injected: bool = False

    # --- Depth history stats ---
    depth_min: Optional[int] = None
    depth_max: Optional[int] = None
    depth_mean: Optional[float] = None
    depth_std: Optional[float] = None          # ddof=0
    depth_sample_std: Optional[float] = None   # ddof=1

    # --- Event timestamps (v1 collected but never surfaced) ---
    event_count: int = 0
    mean_inter_event_gap_ms: Optional[float] = None
    max_inter_event_gap_ms: Optional[float] = None

    # --- Throttle state (v1's `_shorten_factor` was never exposed) ---
    shorten_factor: float = 1.0

    # --- Uncertainty (from meter, when available) ---
    energy_uncertainty_wh: Optional[float] = None

    # --- Verification ---
    bounds_ok: bool = True
    bounds_violations: List[str] = field(default_factory=list)
    severity: str = Severity.INFO.value

    # --- XAI ---
    explanation: Optional[Dict[str, Any]] = None

    # --- Safety / HITL ---
    needs_review: bool = False
    review_reason: Optional[str] = None
    hitl_approved: Optional[bool] = None

    # --- Timing ---
    started_at: Optional[float] = None
    finished_at: Optional[float] = None

    # --- Metadata ---
    at: datetime = field(default_factory=datetime.now)

    def to_dict(self, safe: bool = False) -> Dict[str, Any]:
        out = asdict(self)
        out["at"] = self.at.isoformat()
        if safe:
            return _jsonable(out)
        return out


# =============================================================================
# XAI
# =============================================================================

class MetricsExplainer:
    @staticmethod
    def explain(snap: MetricsSnapshot) -> Dict[str, Any]:
        reasons: List[str] = [
            f"Run {snap.run_id}: latency={snap.latency_s:.3f}s, "
            f"memory={snap.memory_mb:.1f} MB ({snap.memory_source}).",
            f"Tool calls: {snap.tool_calls}, LLM calls: {snap.llm_calls}, "
            f"tokens: {snap.tokens}.",
        ]
        if snap.depth_mean is not None:
            reasons.append(
                f"Depth: mean={snap.depth_mean:.2f}, "
                f"max={snap.depth_max}, "
                f"std(pop)={snap.depth_std or 0.0:.2f}, "
                f"std(sample)={snap.depth_sample_std or 0.0:.2f}."
            )
        if snap.event_count:
            reasons.append(
                f"Events: {snap.event_count}"
                + (
                    f", mean gap="
                    f"{snap.mean_inter_event_gap_ms:.1f}ms"
                    if snap.mean_inter_event_gap_ms is not None
                    else ""
                )
            )
        if snap.energy_wh:
            reasons.append(
                f"Energy: {snap.energy_wh:.6f} Wh "
                f"({snap.energy_source}"
                + (
                    f", ±{snap.energy_uncertainty_wh:.6f} Wh"
                    if snap.energy_uncertainty_wh is not None
                    else ""
                )
                + ")."
            )
        if snap.carbon_g:
            reasons.append(
                f"Carbon: {snap.carbon_g:.6f} gCO₂e ({snap.carbon_source})."
            )
        if snap.helium_units:
            reasons.append(
                f"Helium: {snap.helium_units:.8f} units."
            )
        if snap.shorten_factor != 1.0:
            reasons.append(
                f"Context shortening applied: factor={snap.shorten_factor:.3f}."
            )
        if snap.chaos_injected:
            reasons.append(
                "WARNING: values affected by chaos injection."
            )
        if snap.simulated:
            reasons.append(
                "WARNING: snapshot contains simulated values."
            )
        if not snap.bounds_ok:
            reasons.append(
                f"Bounds violations: {snap.bounds_violations}."
            )
        if snap.review_reason:
            reasons.append(f"Review: {snap.review_reason}.")

        return {
            "headline": (
                f"[{snap.run_id}] {snap.latency_s:.3f}s, "
                f"{snap.memory_mb:.1f} MB, {snap.tool_calls} tool calls"
            ),
            "rationale": reasons,
            "contributing_factors": {
                "latency_s": snap.latency_s,
                "memory_mb": snap.memory_mb,
                "tool_calls": float(snap.tool_calls),
                "conversation_depth": float(snap.conversation_depth),
                "energy_wh": snap.energy_wh,
                "carbon_g": snap.carbon_g,
                "shorten_factor": snap.shorten_factor,
            },
            "confidence": 0.5 if snap.simulated else 0.9,
            "severity": snap.severity,
        }


# =============================================================================
# MetricsCollector
# =============================================================================

class MetricsCollector:
    """
    Enhanced metrics collector.

    Backward-compatible: same constructor keys, same public methods, and
    the same dict shape with additive keys.
    """

    #: HITL thresholds.
    HITL_MEMORY_MB = 8192.0
    HITL_LATENCY_S = 600.0

    DEFAULT_FEATURES: Dict[str, bool] = {
        "idempotent_finalize": True,
        "psutil_fallback": True,
        "energy": True,
        "carbon": True,
        "provenance": True,
        "xai": True,
        "hitl": True,
        "extended_metrics": True,
        "thread_safety": True,
        "trace_forwarding": True,
    }

    def __init__(
        self,
        *,
        run_id: Optional[str] = None,
        task_id: Optional[str] = None,
        agent_id: Optional[str] = None,
        deployment_id: str = "local",
        region: Optional[str] = None,
        pue: Optional[float] = None,
        hardware_profile: Optional[str] = None,
        precision: Optional[str] = None,
        energy_meter: Any = None,
        carbon_estimator: Any = None,
        callback: Any = None,
        energy_meter_measure_unit: str = "joules",
        features: Optional[Dict[str, bool]] = None,
    ):
        # --- v1 public state (preserved names) ---
        self.start: float = time.time()
        self.tool_calls: int = 0
        self.conversation_depth: int = 0

        # --- Identity ---
        self.run_id: str = run_id or f"run-{uuid.uuid4().hex[:8]}"
        self.task_id = task_id
        self.agent_id = agent_id
        self.deployment_id = deployment_id

        # --- Context ---
        self.region = region
        self.pue = _safe_float(pue, default=-1.0)
        if self.pue is not None and self.pue < 0:
            self.pue = None
        self.hardware_profile = hardware_profile
        self.precision = precision

        # --- Integrations ---
        self.energy_meter = energy_meter
        self.carbon_estimator = carbon_estimator
        self.callback = callback

        # --- Energy meter unit contract ---
        unit = (energy_meter_measure_unit or "joules").strip().lower()
        if unit not in _VALID_ENERGY_UNITS:
            raise ValueError(
                f"energy_meter_measure_unit must be one of "
                f"{sorted(_VALID_ENERGY_UNITS)}, got "
                f"{energy_meter_measure_unit!r}"
            )
        self._energy_meter_measure_unit = unit

        # --- Additional counters ---
        self.llm_calls: int = 0
        self.tokens: int = 0

        # --- Depth history ---
        self._depth_history: Deque[int] = deque(maxlen=1024)
        self._event_timestamps: Deque[float] = deque(maxlen=4096)
        self._chaos_events: List[str] = []

        # --- Shorten-context state (v1's `_shorten_factor` bug) ---
        self._shorten_factor: float = 1.0

        # --- Concurrency ---
        self._lock = threading.RLock()

        # --- Finalize cache ---
        self._finalized: bool = False
        self._last_snapshot: Optional[MetricsSnapshot] = None
        self._last_decision_record: Optional[Any] = None

        # --- HITL / trace ---
        self._hitl_callback: Optional[
            Callable[[MetricsSnapshot], Optional[bool]]
        ] = None
        self._trace: Any = None

        # --- Feature flags ---
        self.features: Dict[str, bool] = {
            **self.DEFAULT_FEATURES, **(features or {})
        }
        unknown = set((features or {}).keys()) - set(self.DEFAULT_FEATURES)
        if unknown:
            logger.warning(
                "metrics_collector.unknown_features",
                extra={"unknown": sorted(unknown)},
            )

        # --- Statistics ---
        self._stats = _StatsCounter()
        _GLOBAL_STATS.increment("collectors")

        logger.debug(
            "metrics_collector.initialized",
            extra={
                "run_id": self.run_id,
                "deployment_id": deployment_id,
                "energy_meter_wired": energy_meter is not None,
                "carbon_estimator_wired": carbon_estimator is not None,
                "callback_wired": callback is not None,
            },
        )

    # ------------------------------------------------------------------
    # Recording API (v1 + enhanced)
    # ------------------------------------------------------------------

    def record_tool(self) -> None:
        """Increment the tool-call counter (thread-safe)."""
        with self._lock:
            self.tool_calls += 1
            self._event_timestamps.append(time.time())

    def record_depth(self, depth: Any) -> None:
        """Record a conversation depth observation (v1: running max)."""
        depth_int = _safe_int(depth, default=-1)
        if depth_int < 0:
            logger.warning(
                "metrics_collector.record_depth_invalid",
                extra={"value": depth},
            )
            return
        with self._lock:
            self.conversation_depth = max(
                self.conversation_depth, depth_int,
            )
            self._depth_history.append(depth_int)
            self._event_timestamps.append(time.time())

    def record_llm_call(self) -> None:
        """Track an LLM-call count."""
        with self._lock:
            self.llm_calls += 1
            self._event_timestamps.append(time.time())

    def record_tokens(self, tokens: Any) -> None:
        """Add a token count to the running total."""
        t = _safe_int(tokens, default=-1)
        if t < 0:
            logger.warning(
                "metrics_collector.record_tokens_invalid",
                extra={"value": tokens},
            )
            return
        with self._lock:
            self.tokens += t

    def record_chaos_event(self, name: str) -> None:
        """Record a chaos-injection event so `simulated` gets set."""
        with self._lock:
            self._chaos_events.append(str(name))
            self._event_timestamps.append(time.time())

    def record_shorten_context_factor(self, factor: Any) -> None:
        """Record the context-shortening factor applied during the run."""
        try:
            f = float(factor)
        except (TypeError, ValueError):
            logger.warning(
                "metrics_collector.shorten_factor_invalid",
                extra={"value": factor},
            )
            return
        if not math.isfinite(f):
            logger.warning(
                "metrics_collector.shorten_factor_non_finite",
                extra={"value": factor},
            )
            return
        with self._lock:
            self._shorten_factor = max(0.0, min(1.0, f))

    # ------------------------------------------------------------------
    # Finalization
    # ------------------------------------------------------------------

    def finalize(self, *, emit_decision_record: bool = False) -> Dict[str, Any]:
        """
        Finalize the collection and return the metrics dict.

        Idempotent when `features['idempotent_finalize']` is True: repeated
        calls return the same values. Non-idempotent callers get a fresh
        snapshot each call (v1 semantics).
        """
        with self._lock:
            if (
                self.features.get("idempotent_finalize", True)
                and self._finalized
                and self._last_snapshot is not None
            ):
                _GLOBAL_STATS.increment("races_prevented")
                return self._last_snapshot.to_dict()

            snapshot = self._build_snapshot()

            # HITL review (before caching so denial lands in the cache)
            if (
                self.features.get("hitl", True)
                and snapshot.needs_review
                and self._hitl_callback is not None
            ):
                try:
                    approved = self._hitl_callback(snapshot)
                    if approved is False:
                        snapshot.hitl_approved = False
                        snapshot.review_reason = (
                            (snapshot.review_reason or "")
                            + " [HITL: denied]"
                        ).strip()
                    elif approved is True:
                        snapshot.hitl_approved = True
                except Exception as e:
                    logger.warning(
                        "metrics_collector.hitl_callback_failed",
                        extra={"error": str(e)},
                    )

            # Refresh explanation to include HITL annotation, if any
            if (
                self.features.get("xai", True)
                and snapshot.explanation is not None
                and snapshot.review_reason
            ):
                snapshot.explanation = MetricsExplainer.explain(snapshot)

            self._last_snapshot = snapshot
            self._finalized = True
            self._stats.increment("finalizations")

        _GLOBAL_STATS.increment("finalizations")

        # Trace forwarding (outside the lock — the trace may have its own)
        if (
            self.features.get("trace_forwarding", True)
            and self._trace is not None
        ):
            try:
                self._trace.record(
                    {
                        "action": "metrics_finalize",
                        "run_id": snapshot.run_id,
                        "tool_calls": snapshot.tool_calls,
                        "energy_wh": snapshot.energy_wh,
                        "carbon_g": snapshot.carbon_g,
                        "simulated": snapshot.simulated,
                    },
                    kind="metric",
                    agent_id=snapshot.agent_id,
                    task_id=snapshot.task_id,
                    run_id=snapshot.run_id,
                    duration_ms=(snapshot.latency_s * 1000.0),
                    severity=snapshot.severity,
                )
            except Exception as e:
                logger.debug(
                    "metrics_collector.trace_forward_failed",
                    extra={"error": str(e)},
                )

        result = snapshot.to_dict()

        if emit_decision_record:
            dr = self._build_decision_record_from(snapshot)
            if dr is not None:
                with self._lock:
                    self._last_decision_record = dr
                serialised = _serialise_decision_record(dr)
                if serialised is not None:
                    result["decision_record"] = _jsonable(serialised)

        logger.info(
            "metrics.finalized",
            extra={
                "run_id": snapshot.run_id,
                "task_id": snapshot.task_id,
                "agent_id": snapshot.agent_id,
                "tool_calls": snapshot.tool_calls,
                "llm_calls": snapshot.llm_calls,
                "tokens": snapshot.tokens,
                "latency_s": snapshot.latency_s,
                "memory_mb": snapshot.memory_mb,
                "energy_wh": snapshot.energy_wh,
                "energy_source": snapshot.energy_source,
                "carbon_g": snapshot.carbon_g,
                "carbon_source": snapshot.carbon_source,
                "simulated": snapshot.simulated,
                "chaos_injected": snapshot.chaos_injected,
                "shorten_factor": snapshot.shorten_factor,
                "bounds_ok": snapshot.bounds_ok,
                "needs_review": snapshot.needs_review,
                "severity": snapshot.severity,
            },
        )
        return result

    def finalize_detailed(self) -> MetricsSnapshot:
        """
        Return the full `MetricsSnapshot`.

        Calls `finalize()` exactly once per collection (or returns the
        cached snapshot when idempotent). Never rebuilds.
        """
        self.finalize()  # builds or returns cached
        with self._lock:
            # `finalize()` above guarantees this is set.
            assert self._last_snapshot is not None
            return self._last_snapshot

    # ------------------------------------------------------------------
    # Provenance bridge
    # ------------------------------------------------------------------

    def attach_provenance(
        self, metrics: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        Bridge to `metric_provenance.attach_provenance_enhanced`.

        Returns the translated metrics dict with `metric_provenance` /
        `metric_provenance_detailed` attached, or with
        `_provenance_error` set when the bridge is unavailable.
        """
        snapshot = self.finalize_detailed()
        translated = self._translate_for_provenance(snapshot)

        try:
            from src.analysis.telemetry.metric_provenance import (
                attach_provenance_enhanced as _attach,
                SourceKind as _SourceKind,
            )
        except Exception:
            try:
                from analysis.metric_provenance import (  # type: ignore
                    attach_provenance_enhanced as _attach,
                    SourceKind as _SourceKind,
                )
            except Exception as e:
                logger.debug(
                    "metrics_collector.provenance_bridge_failed",
                    extra={"error": str(e)},
                )
                translated["metric_provenance"] = {}
                translated["metric_provenance_detailed"] = {}
                translated["metric_provenance_summary"] = {}
                translated["_provenance_error"] = str(e)
                return translated

        try:
            enriched = _attach(
                translated,
                default_metrics=["energy", "carbon", "latency", "memory"],
                run_id=snapshot.run_id,
                task_id=snapshot.task_id,
                agent_id=snapshot.agent_id,
                in_place=False,
            )
        except Exception as e:
            logger.warning(
                "metrics_collector.provenance_bridge_failed",
                extra={"error": str(e)},
            )
            translated["metric_provenance"] = {}
            translated["metric_provenance_detailed"] = {}
            translated["metric_provenance_summary"] = {}
            translated["_provenance_error"] = str(e)
            return translated

        self._patch_provenance_sources(enriched, snapshot, _SourceKind)
        return enriched

    def _translate_for_provenance(
        self, snapshot: MetricsSnapshot,
    ) -> Dict[str, Any]:
        """
        Convert the snapshot into the key/value pairs `metric_provenance`
        expects (`energy`, `carbon`, `latency`, `memory`, plus source
        labels, simulated/chaos/hitl flags, precision/region).
        """
        return {
            # Metric values under metric_provenance's expected names
            "energy": snapshot.energy_wh,
            "energy_kwh": snapshot.energy_wh / 1000.0,
            "carbon": snapshot.carbon_g,
            "latency": snapshot.latency_s,
            "memory": snapshot.memory_mb,
            # Source labels (metric_provenance consults `{metric}_source`)
            "energy_source": self._source_for_provenance(
                snapshot.energy_source,
            ),
            "carbon_source": self._source_for_provenance(
                snapshot.carbon_source,
            ),
            "latency_source": self._source_for_provenance(
                snapshot.latency_source,
            ),
            "memory_source": self._source_for_provenance(
                snapshot.memory_source,
            ),
            # Propagate per-metric flags
            "simulated": snapshot.simulated,
            "chaos_injected": snapshot.chaos_injected,
            "precision": snapshot.precision,
            "region": snapshot.region,
            "pue": snapshot.pue,
            "hardware_profile": snapshot.hardware_profile,
            "run_id": snapshot.run_id,
            "task_id": snapshot.task_id,
            "agent_id": snapshot.agent_id,
            "shorten_factor": snapshot.shorten_factor,
            # Uncertainties — metric_provenance reads `{metric}_uncertainty`
            "energy_uncertainty": snapshot.energy_uncertainty_wh,
        }

    @staticmethod
    def _source_for_provenance(source: str) -> str:
        """Map the collector's `MetricSource` to a label metric_provenance
        can infer from (`meter` / `fallback` / `estimated` / `unknown`)."""
        mapping = {
            MetricSource.MEASURED.value: "meter",
            MetricSource.SIMULATED.value: "fallback",
            MetricSource.FALLBACK.value: "fallback",
            MetricSource.ESTIMATED.value: "estimated",
            MetricSource.UNKNOWN.value: "unknown",
        }
        return mapping.get(source, "unknown")

    @staticmethod
    def _patch_provenance_sources(
        enriched: Dict[str, Any],
        snapshot: MetricsSnapshot,
        SourceKind: Any,
    ) -> None:
        """
        Overwrite metric_provenance's inferred `source_kind` with the
        collector's authoritative label.
        """
        if not isinstance(enriched, dict):
            return
        simple = enriched.get("metric_provenance")
        detailed = enriched.get("metric_provenance_detailed")
        if not isinstance(simple, dict) or not isinstance(detailed, dict):
            return

        def _kind(collector_source: str) -> str:
            if collector_source == MetricSource.MEASURED.value:
                return SourceKind.MEASURED.value
            if collector_source == MetricSource.ESTIMATED.value:
                return SourceKind.ESTIMATED.value
            if collector_source == MetricSource.SIMULATED.value:
                return SourceKind.SIMULATED.value
            if collector_source == MetricSource.FALLBACK.value:
                return SourceKind.DEFAULT.value
            return SourceKind.UNKNOWN.value

        for metric, source in (
            ("energy", snapshot.energy_source),
            ("carbon", snapshot.carbon_source),
            ("latency", snapshot.latency_source),
            ("memory", snapshot.memory_source),
        ):
            kind = _kind(source)
            simple[metric] = kind
            if metric in detailed and isinstance(detailed[metric], dict):
                detailed[metric]["source_kind"] = kind

    # ------------------------------------------------------------------
    # DecisionRecord emission
    # ------------------------------------------------------------------

    def to_decision_record(
        self,
        *,
        policy_version: str = "",
    ) -> Optional[Any]:
        """Emit a DecisionRecord for the current snapshot."""
        snapshot = self.finalize_detailed()
        dr = self._build_decision_record_from(
            snapshot, policy_version=policy_version,
        )
        with self._lock:
            self._last_decision_record = dr
        return dr

    def _build_decision_record_from(
        self,
        snapshot: MetricsSnapshot,
        *,
        policy_version: str = "",
    ) -> Optional[Any]:
        DecisionRecord = _import_decision_record()
        if DecisionRecord is None:
            logger.debug(
                "metrics_collector.decision_record_import_failed"
            )
            return None

        candidates: Dict[str, Any] = {
            "run_id": snapshot.run_id,
            "timestamp": snapshot.at,
            "task_id": snapshot.task_id or "",
            "selected_action": (
                f"metrics_collection(tools={snapshot.tool_calls})"
            ),
            "alternatives": [],
            "policy_version": policy_version,
            "model_or_agent": snapshot.agent_id or "",
            "hardware_profile": snapshot.hardware_profile or "",
            "precision": snapshot.precision,
            "latency_ms": snapshot.latency_s * 1000.0,
            "energy_kwh": snapshot.energy_wh / 1000.0,
            "carbon_operational_kg": snapshot.carbon_g / 1000.0,
            "helium_units": snapshot.helium_units,
            "explanation": dict(snapshot.explanation or {}),
            "provenance": {
                "source": "metrics_collector",
                "analyzer_version": __version__,
                "memory_source": snapshot.memory_source,
                "latency_source": snapshot.latency_source,
                "energy_source": snapshot.energy_source,
                "carbon_source": snapshot.carbon_source,
                "simulated": snapshot.simulated,
                "chaos_injected": snapshot.chaos_injected,
                "shorten_factor": snapshot.shorten_factor,
                "precision": snapshot.precision,
                "region": snapshot.region,
                "pue": snapshot.pue,
                "hardware_profile": snapshot.hardware_profile,
                "deployment_id": snapshot.deployment_id,
                "severity": snapshot.severity,
                "needs_review": snapshot.needs_review,
                "hitl_approved": snapshot.hitl_approved,
            },
        }
        filtered = _filter_decision_record_kwargs(
            DecisionRecord, candidates,
        )
        try:
            return DecisionRecord(**filtered)
        except TypeError as e:
            logger.warning(
                "metrics_collector.decision_record_failed",
                extra={"error": str(e)},
            )
            return None

    # ------------------------------------------------------------------
    # Configuration
    # ------------------------------------------------------------------

    def set_hitl_callback(
        self,
        callback: Optional[
            Callable[[MetricsSnapshot], Optional[bool]]
        ],
    ) -> None:
        """
        Register a HITL reviewer for `needs_review=True` snapshots.

        Returning `False` sets `hitl_approved=False` and appends
        `[HITL: denied]` to `review_reason`. Passing `None` clears it.
        """
        with self._lock:
            self._hitl_callback = callback

    def set_trace(self, trace: Any) -> None:
        """Wire in an ExecutionTrace sink for per-finalize() summary steps."""
        with self._lock:
            self._trace = trace

    def set_energy_meter(self, meter: Any) -> None:
        """Wire in (or replace) an EnergyMeter-style component."""
        with self._lock:
            self.energy_meter = meter

    def set_carbon_estimator(self, estimator: Any) -> None:
        """Wire in (or replace) a carbon estimator."""
        with self._lock:
            self.carbon_estimator = estimator

    def set_callback(self, callback: Any) -> None:
        """Wire in (or replace) a callback whose snapshot() gets merged."""
        with self._lock:
            self.callback = callback

    def get_last_snapshot(self) -> Optional[MetricsSnapshot]:
        with self._lock:
            return self._last_snapshot

    def get_last_decision_record(self) -> Optional[Any]:
        with self._lock:
            return self._last_decision_record

    # ------------------------------------------------------------------
    # Statistics / export / reset
    # ------------------------------------------------------------------

    def get_statistics(self) -> Dict[str, Any]:
        """Return cumulative collector statistics (instance-scoped)."""
        with self._lock:
            snap = self._stats.snapshot()
            mean_gap, max_gap = self._event_gap_stats_locked()
            return {
                "analyzer_version": __version__,
                "run_id": self.run_id,
                "task_id": self.task_id,
                "agent_id": self.agent_id,
                "deployment_id": self.deployment_id,
                "finalized": self._finalized,
                "tool_calls": self.tool_calls,
                "llm_calls": self.llm_calls,
                "tokens": self.tokens,
                "conversation_depth": self.conversation_depth,
                "depth_observations": len(self._depth_history),
                "event_timestamps": len(self._event_timestamps),
                "mean_inter_event_gap_ms": mean_gap,
                "max_inter_event_gap_ms": max_gap,
                "chaos_events": list(self._chaos_events),
                "shorten_factor": self._shorten_factor,
                "finalizations": snap.get("finalizations", 0),
                "psutil_failures": snap.get("psutil_failures", 0),
                "features": dict(self.features),
                "psutil_available": _HAS_PSUTIL,
                "global_statistics": get_statistics(),
            }

    def export(self) -> Dict[str, Any]:
        """Full serialisable export."""
        with self._lock:
            snapshot = (
                self._last_snapshot.to_dict(safe=True)
                if self._last_snapshot is not None else None
            )
            dr = self._last_decision_record
            serialised_dr = (
                _jsonable(_serialise_decision_record(dr))
                if dr is not None else None
            )
            return {
                "analyzer_version": __version__,
                "statistics": self.get_statistics(),
                "snapshot": snapshot,
                "decision_record": serialised_dr,
            }

    def reset(self, *, full: bool = False) -> None:
        """
        Reset the collector.

        `full=False` (default) clears counters, timings, caches, depth
        history, event timestamps, chaos events, and the shorten factor.
        `full=True` also clears wiring (meters, estimator, callback,
        trace, hitl) and regenerates `run_id`.
        """
        with self._lock:
            self.start = time.time()
            self.tool_calls = 0
            self.conversation_depth = 0
            self.llm_calls = 0
            self.tokens = 0
            self._depth_history.clear()
            self._event_timestamps.clear()
            self._chaos_events.clear()
            self._shorten_factor = 1.0
            self._finalized = False
            self._last_snapshot = None
            self._last_decision_record = None
            self._stats.reset()

            if full:
                self.run_id = f"run-{uuid.uuid4().hex[:8]}"
                self.task_id = None
                self.agent_id = None
                self.deployment_id = "local"
                self.region = None
                self.pue = None
                self.hardware_profile = None
                self.precision = None
                self.energy_meter = None
                self.carbon_estimator = None
                self.callback = None
                self._hitl_callback = None
                self._trace = None
                self.features = dict(self.DEFAULT_FEATURES)

        logger.debug(
            "metrics_collector.reset",
            extra={"run_id": self.run_id, "full": full},
        )

    # ------------------------------------------------------------------
    # Internal — snapshot construction
    # ------------------------------------------------------------------

    def _build_snapshot(self) -> MetricsSnapshot:
        """Build a fresh snapshot. Called exactly once per finalize()."""
        # Everything below is snapshotted under the lock so concurrent
        # record_* calls do not race the build.
        with self._lock:
            now = time.time()
            latency_s = now - self.start
            depths = list(self._depth_history)
            tools = self.tool_calls
            llm_calls = self.llm_calls
            tokens = self.tokens
            depth_max_from_history = max(depths) if depths else 0
            current_max_depth = self.conversation_depth
            shorten_factor = self._shorten_factor
            chaos_injected = len(self._chaos_events) > 0
            event_count = len(self._event_timestamps)
            mean_gap_ms, max_gap_ms = self._event_gap_stats_locked()

        # --- Memory (outside the lock — psutil may block briefly) ---
        memory_mb, memory_source, memory_violations = self._read_memory()

        # --- Energy / carbon / helium ---
        sust = self._read_sustainability_metrics()

        # --- Depth stats ---
        depth_min: Optional[int] = None
        depth_max = (
            max(current_max_depth, depth_max_from_history)
            if depths or current_max_depth else None
        )
        depth_mean: Optional[float] = None
        depth_std: Optional[float] = None
        depth_sample_std: Optional[float] = None
        if depths:
            depth_min = min(depths)
            depth_mean = sum(depths) / len(depths)
            var_pop = sum(
                (d - depth_mean) ** 2 for d in depths
            ) / len(depths)
            depth_std = math.sqrt(var_pop)
            if len(depths) > 1:
                var_sample = sum(
                    (d - depth_mean) ** 2 for d in depths
                ) / (len(depths) - 1)
                depth_sample_std = math.sqrt(var_sample)

        # --- Simulation detection ---
        simulated = (
            memory_source in (
                MetricSource.FALLBACK.value,
                MetricSource.SIMULATED.value,
            )
            or sust["energy_source"] == MetricSource.SIMULATED.value
            or chaos_injected
        )

        # --- Bounds verification (now includes energy/carbon) ---
        bounds_ok, violations = self._check_bounds(
            latency_s=latency_s,
            memory_mb=memory_mb,
            tool_calls=tools,
            depth=depth_max or 0,
            energy_wh=sust["energy_wh"],
            carbon_g=sust["carbon_g"],
        )
        violations.extend(memory_violations)
        violations.extend(sust["violations"])
        bounds_ok = len(violations) == 0

        # --- HITL / severity ---
        needs_review = False
        review_reason: Optional[str] = None
        if self.features.get("hitl", True):
            if memory_mb >= self.HITL_MEMORY_MB:
                needs_review = True
                review_reason = (
                    f"memory {memory_mb:.1f} MB >= "
                    f"{self.HITL_MEMORY_MB} MB"
                )
            elif latency_s >= self.HITL_LATENCY_S:
                needs_review = True
                review_reason = (
                    f"latency {latency_s:.1f}s >= "
                    f"{self.HITL_LATENCY_S}s"
                )
            elif not bounds_ok:
                needs_review = True
                review_reason = f"bounds violations: {violations}"

        if not bounds_ok:
            severity = Severity.CRITICAL.value
        elif needs_review:
            severity = Severity.WARNING.value
        else:
            severity = Severity.INFO.value

        # --- Precision: prefer the meter's if the caller didn't override ---
        precision = self.precision
        if precision is None and sust.get("meter_precision"):
            precision = sust["meter_precision"]

        snapshot = MetricsSnapshot(
            run_id=self.run_id,
            task_id=self.task_id,
            agent_id=self.agent_id,
            latency_s=latency_s,
            memory_mb=memory_mb,
            tool_calls=tools,
            conversation_depth=depth_max or 0,
            energy_wh=sust["energy_wh"],
            carbon_g=sust["carbon_g"],
            helium_units=sust["helium_units"],
            llm_calls=llm_calls,
            tokens=tokens,
            precision=precision,
            region=self.region,
            pue=self.pue,
            hardware_profile=self.hardware_profile,
            deployment_id=self.deployment_id,
            memory_source=memory_source,
            latency_source=MetricSource.MEASURED.value,
            energy_source=sust["energy_source"],
            carbon_source=sust["carbon_source"],
            simulated=simulated,
            chaos_injected=chaos_injected,
            depth_min=depth_min,
            depth_max=depth_max,
            depth_mean=depth_mean,
            depth_std=depth_std,
            depth_sample_std=depth_sample_std,
            event_count=event_count,
            mean_inter_event_gap_ms=mean_gap_ms,
            max_inter_event_gap_ms=max_gap_ms,
            shorten_factor=shorten_factor,
            energy_uncertainty_wh=sust.get("energy_uncertainty_wh"),
            bounds_ok=bounds_ok,
            bounds_violations=violations,
            severity=severity,
            needs_review=needs_review,
            review_reason=review_reason,
            started_at=self.start,
            finished_at=now,
        )

        if self.features.get("xai", True):
            snapshot.explanation = MetricsExplainer.explain(snapshot)

        return snapshot

    # ------------------------------------------------------------------
    # Internal — subsystem readers
    # ------------------------------------------------------------------

    def _event_gap_stats_locked(
        self,
    ) -> Tuple[Optional[float], Optional[float]]:
        """Return (mean_gap_ms, max_gap_ms) from `_event_timestamps`."""
        timestamps = list(self._event_timestamps)
        if len(timestamps) < 2:
            return None, None
        gaps = [
            timestamps[i + 1] - timestamps[i]
            for i in range(len(timestamps) - 1)
        ]
        gaps = [g for g in gaps if g >= 0]
        if not gaps:
            return None, None
        return (sum(gaps) / len(gaps)) * 1000.0, max(gaps) * 1000.0

    def _read_memory(self) -> Tuple[float, str, List[str]]:
        """Return (value_mb, source, violations)."""
        violations: List[str] = []

        if not _HAS_PSUTIL or psutil is None:
            return 0.0, MetricSource.FALLBACK.value, violations

        strict = not self.features.get("psutil_fallback", True)
        try:
            info = psutil.Process().memory_info()
            memory_mb = info.rss / (1024 ** 2)
        except Exception as e:
            _GLOBAL_STATS.increment("psutil_failures")
            self._stats.increment("psutil_failures")
            logger.warning(
                "metrics.psutil_failed",
                extra={"error": str(e), "strict": strict},
            )
            if strict:
                raise
            return 0.0, MetricSource.FALLBACK.value, violations

        if not math.isfinite(memory_mb) or memory_mb < 0:
            violations.append("memory_invalid")
            return 0.0, MetricSource.FALLBACK.value, violations
        return memory_mb, MetricSource.MEASURED.value, violations

    def _read_sustainability_metrics(self) -> Dict[str, Any]:
        """Return a dict with energy/carbon/helium + their source labels."""
        out: Dict[str, Any] = {
            "energy_wh": 0.0,
            "energy_source": MetricSource.UNKNOWN.value,
            "energy_uncertainty_wh": None,
            "meter_precision": None,
            "carbon_g": 0.0,
            "carbon_source": MetricSource.UNKNOWN.value,
            "helium_units": 0.0,
            "violations": [],
        }

        # --- Energy ---
        meter = self.energy_meter
        if self.features.get("energy", True) and meter is not None:
            joules: Optional[float] = None
            meter_source = MetricSource.MEASURED.value

            # 1. measure_detailed() — best interface
            if hasattr(meter, "measure_detailed") and callable(
                meter.measure_detailed,
            ):
                try:
                    rec = meter.measure_detailed()
                    raw = getattr(rec, "energy_joules", None)
                    j = _safe_float(raw, default=-1.0)
                    if j >= 0:
                        joules = j
                        rec_src = getattr(rec, "source", None)
                        if (
                            bool(getattr(rec, "simulated", False))
                            or (isinstance(rec_src, str)
                                and rec_src in ("fallback", "simulated"))
                        ):
                            meter_source = MetricSource.SIMULATED.value
                        rec_precision = getattr(rec, "precision", None)
                        if isinstance(rec_precision, str):
                            out["meter_precision"] = rec_precision
                        u = _safe_float(
                            getattr(rec, "uncertainty_joules", None),
                            default=-1.0,
                        )
                        if u >= 0:
                            out["energy_uncertainty_wh"] = u / 3600.0
                except Exception as e:
                    logger.debug(
                        "metrics.energy_meter_measure_detailed_failed",
                        extra={"error": str(e)},
                    )

            # 2. joules() — explicit unit
            if joules is None and hasattr(meter, "joules") and callable(
                meter.joules,
            ):
                try:
                    j = _safe_float(meter.joules(), default=-1.0)
                    if j < 0:
                        j = 0.0
                        meter_source = MetricSource.UNKNOWN.value
                    joules = j
                except Exception as e:
                    logger.debug(
                        "metrics.energy_meter_joules_failed",
                        extra={"error": str(e)},
                    )

            # 3. measure() — unit from the collector's kwarg
            if joules is None and hasattr(meter, "measure") and callable(
                meter.measure,
            ):
                try:
                    v = _safe_float(meter.measure(), default=-1.0)
                    if v >= 0:
                        joules = _to_joules(
                            v, self._energy_meter_measure_unit,
                        )
                        meter_source = MetricSource.MEASURED.value
                except Exception as e:
                    logger.debug(
                        "metrics.energy_meter_measure_failed",
                        extra={"error": str(e)},
                    )

            if joules is not None:
                if not math.isfinite(joules) or joules < 0:
                    out["violations"].append(
                        f"energy_invalid({joules})"
                    )
                    joules = 0.0
                    meter_source = MetricSource.UNKNOWN.value
                out["energy_wh"] = joules / 3600.0
                out["energy_source"] = meter_source

        # --- Carbon ---
        if (
            self.features.get("carbon", True)
            and self.carbon_estimator is not None
            and out["energy_wh"] > 0
        ):
            try:
                estimate = self.carbon_estimator.estimate(
                    energy_wh=out["energy_wh"],
                )
                kg = _safe_float(estimate, default=-1.0)
                if not math.isfinite(kg) or kg < 0:
                    out["violations"].append(
                        "carbon_estimator_returned_invalid"
                    )
                else:
                    out["carbon_g"] = kg * 1000.0
                    out["carbon_source"] = MetricSource.ESTIMATED.value
            except Exception as e:
                logger.warning(
                    "metrics.carbon_estimator_failed",
                    extra={"error": str(e)},
                )
                out["violations"].append(
                    f"carbon_estimator_exception:"
                    f"{type(e).__name__}"
                )

        # --- Helium ---
        try:
            from src.analysis.telemetry.helium_profiler import (  # type: ignore
                HeliumProfiler,
            )
            out["helium_units"] = HeliumProfiler.estimate_units(
                out["energy_wh"],
            )
        except Exception:
            if out["energy_wh"] > 0:
                out["helium_units"] = out["energy_wh"] * 5e-5

        return out

    @staticmethod
    def _check_bounds(
        *,
        latency_s: float,
        memory_mb: float,
        tool_calls: int,
        depth: int,
        energy_wh: float,
        carbon_g: float,
    ) -> Tuple[bool, List[str]]:
        bad: List[str] = []
        if latency_s < 0:
            bad.append(f"latency_negative({latency_s})")
        if memory_mb < 0:
            bad.append(f"memory_negative({memory_mb})")
        if tool_calls < 0:
            bad.append(f"tool_calls_negative({tool_calls})")
        if depth < 0:
            bad.append(f"depth_negative({depth})")
        if energy_wh < 0:
            bad.append(f"energy_negative({energy_wh})")
        if carbon_g < 0:
            bad.append(f"carbon_negative({carbon_g})")
        return (len(bad) == 0, bad)


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
    mc = MetricsCollector()
    mc.record_tool()
    mc.record_tool()
    mc.record_depth(3)
    mc.record_depth(5)
    result = mc.finalize()
    print(f"  latency_s:           {result['latency_s']:.6f}")
    print(f"  memory_mb:           {result['memory_mb']:.2f}")
    print(f"  tool_calls:          {result['tool_calls']}")
    print(f"  conversation_depth:  {result['conversation_depth']}")
    print(f"  v1 keys all present: "
          f"{all(k in result for k in ('latency_s', 'memory_mb', 'tool_calls', 'conversation_depth'))}")

    # =================================================================
    # 2. Idempotent finalize (v1 grew latency each call)
    # =================================================================
    print("\n=== 2. Idempotent finalize ===")
    mc_id = MetricsCollector()
    mc_id.record_tool()
    r1 = mc_id.finalize()
    time.sleep(0.05)
    r2 = mc_id.finalize()
    print(f"  call 1 latency: {r1['latency_s']:.6f}")
    print(f"  call 2 latency: {r2['latency_s']:.6f}")
    print(f"  identical:      {r1['latency_s'] == r2['latency_s']}")

    # =================================================================
    # 3. finalize_detailed with idempotent_finalize=False (v1 dropped fields)
    # =================================================================
    print("\n=== 3. finalize_detailed with idempotent_finalize=False ===")
    mc_ni = MetricsCollector(
        features={"idempotent_finalize": False},
    )
    mc_ni.record_tool()
    mc_ni.record_depth(2)
    mc_ni.record_depth(4)
    mc_ni.record_shorten_context_factor(0.5)
    snap1 = mc_ni.finalize_detailed()
    snap2 = mc_ni.finalize_detailed()

    # Both snapshots should have all enhanced fields
    print(f"  snap1.energy_source:     {snap1.energy_source}")
    print(f"  snap1.depth_mean:        {snap1.depth_mean:.2f}")
    print(f"  snap1.shorten_factor:    {snap1.shorten_factor}")
    print(f"  snap1.severity:          {snap1.severity}")
    print(f"  snap1.explanation present: {snap1.explanation is not None}")
    print(f"  snap2 has same field set: "
          f"{set(snap1.to_dict().keys()) == set(snap2.to_dict().keys())}")

    # =================================================================
    # 4. attach_provenance bridge (v1 resolved everything to unknown)
    # =================================================================
    print("\n=== 4. attach_provenance bridge ===")

    class _MockMeterDetailed:
        """Mimics the enhanced EnergyMeter: measure_detailed returns a
        MeasurementRecord with .energy_joules, .source, .simulated."""
        class _Record:
            energy_joules = 1800.0     # 0.5 Wh
            source = "codecarbon"
            simulated = False
            precision = "int8"
            uncertainty_joules = 90.0
        def measure_detailed(self):
            return self._Record()

    class _MockCarbonEst:
        def estimate(self, energy_wh):
            # 0.5 Wh × 400 g/kWh = 0.2 g = 0.0002 kg
            return (energy_wh / 1000.0) * 400 / 1000.0

    mc_bridge = MetricsCollector(
        run_id="run-bridge",
        energy_meter=_MockMeterDetailed(),
        carbon_estimator=_MockCarbonEst(),
    )
    mc_bridge.record_tool()
    enriched = mc_bridge.attach_provenance()

    if "metric_provenance" in enriched and enriched["metric_provenance"]:
        print(f"  metric_provenance:")
        for k, v in enriched["metric_provenance"].items():
            print(f"    {k}: {v}")
        print(f"  detailed['energy']:")
        d = enriched.get("metric_provenance_detailed", {}).get("energy", {})
        print(f"    source_kind: {d.get('source_kind')}  "
              f"(expected 'measured', NOT 'unknown')")
        print(f"    trust_level: {d.get('trust_level')}")
    else:
        print(f"  provenance bridge unavailable: "
              f"{enriched.get('_provenance_error', 'no error')}")

    # =================================================================
    # 5. _shorten_factor lives end-to-end
    # =================================================================
    print("\n=== 5. shorten_factor end-to-end ===")
    mc_short = MetricsCollector()
    print(f"  default shorten_factor: "
          f"{mc_short._shorten_factor}  (expected 1.0)")
    mc_short.record_shorten_context_factor(0.5)
    snap = mc_short.finalize_detailed()
    print(f"  snapshot.shorten_factor: {snap.shorten_factor}")
    print(f"  explanation mentions:    "
          f"{any('shortening' in r.lower() for r in (snap.explanation or {}).get('rationale', []))}")
    mc_short.reset()
    print(f"  after reset():           {mc_short._shorten_factor}  "
          f"(expected 1.0)")

    # =================================================================
    # 6. Thread safety (barrier + concurrent)
    # =================================================================
    print("\n=== 6. Thread safety ===")
    mc_conc = MetricsCollector()
    barrier = threading.Barrier(4)

    def _worker():
        barrier.wait()
        for _ in range(100):
            mc_conc.record_tool()

    threads = [threading.Thread(target=_worker) for _ in range(4)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    print(f"  tool_calls: {mc_conc.tool_calls}  (expected 400)")

    # =================================================================
    # 7. Energy meter interface (measure_detailed → measured)
    # =================================================================
    print("\n=== 7. Energy meter interface ===")
    mc_meter = MetricsCollector(energy_meter=_MockMeterDetailed())
    mc_meter.record_tool()
    snap = mc_meter.finalize_detailed()
    print(f"  energy_wh:     {snap.energy_wh:.6f}  "
          f"(1800 J / 3600 = 0.5)")
    print(f"  energy_source: {snap.energy_source}  "
          f"(expected 'measured')")
    print(f"  precision:     {snap.precision}  "
          f"(adopted from meter)")
    print(f"  uncertainty:   {snap.energy_uncertainty_wh} Wh")

    # Meter that reports simulated → source should be simulated
    class _SimulatedMeter:
        class _Record:
            energy_joules = 1800.0
            source = "fallback"
            simulated = True
            precision = None
            uncertainty_joules = None
        def measure_detailed(self):
            return self._Record()

    mc_sim = MetricsCollector(energy_meter=_SimulatedMeter())
    mc_sim.record_tool()
    snap_sim = mc_sim.finalize_detailed()
    print(f"\n  simulated meter → energy_source: "
          f"{snap_sim.energy_source}  (expected 'simulated')")
    print(f"  simulated flag:                  "
          f"{snap_sim.simulated}")

    # =================================================================
    # 8. Carbon contract validation
    # =================================================================
    print("\n=== 8. Carbon contract validation ===")

    class _BadCarbonEst:
        def estimate(self, energy_wh):
            return float("nan")

    mc_bad = MetricsCollector(
        energy_meter=_MockMeterDetailed(),
        carbon_estimator=_BadCarbonEst(),
    )
    mc_bad.record_tool()
    snap_bad = mc_bad.finalize_detailed()
    print(f"  carbon_g:            {snap_bad.carbon_g}  (expected 0.0)")
    print(f"  carbon_source:       {snap_bad.carbon_source}")
    print(f"  violations:          "
          f"{[v for v in snap_bad.bounds_violations if 'carbon' in v]}")

    # =================================================================
    # 9. set_hitl_callback denial
    # =================================================================
    print("\n=== 9. HITL denial ===")
    mc_hitl = MetricsCollector()
    mc_hitl.record_tool()
    mc_hitl.set_hitl_callback(lambda s: False)
    # Force a review by lowering the threshold temporarily
    mc_hitl.HITL_LATENCY_S = 0.0   # any latency triggers
    snap_hitl = mc_hitl.finalize_detailed()
    print(f"  needs_review:  {snap_hitl.needs_review}")
    print(f"  hitl_approved: {snap_hitl.hitl_approved}")
    print(f"  review_reason: {snap_hitl.review_reason}")
    print(f"  denial noted:  "
          f"{'[HITL: denied]' in (snap_hitl.review_reason or '')}")

    # =================================================================
    # 10. ExecutionTrace forwarding
    # =================================================================
    print("\n=== 10. ExecutionTrace forwarding ===")

    class _FakeTrace:
        def __init__(self):
            self.records = []
        def record(self, event, **kwargs):
            self.records.append((event, kwargs))

    fake_trace = _FakeTrace()
    mc_trace = MetricsCollector()
    mc_trace.record_tool()
    mc_trace.set_trace(fake_trace)
    mc_trace.finalize()
    print(f"  trace records: {len(fake_trace.records)}  (expected 1)")
    if fake_trace.records:
        ev, kwargs = fake_trace.records[0]
        print(f"  event.action: {ev.get('action')}")
        print(f"  kind:         {kwargs.get('kind')}")

    # =================================================================
    # 11. depth_std vs depth_sample_std
    # =================================================================
    print("\n=== 11. depth_std (pop) vs depth_sample_std (sample) ===")
    mc_depth = MetricsCollector()
    for d in [1, 2, 3, 4, 5]:
        mc_depth.record_depth(d)
    snap_depth = mc_depth.finalize_detailed()
    print(f"  depth_mean:        {snap_depth.depth_mean:.2f}")
    print(f"  depth_std (pop):   {snap_depth.depth_std:.4f}  "
          f"(expected √2 ≈ 1.4142)")
    print(f"  depth_sample_std:  {snap_depth.depth_sample_std:.4f}  "
          f"(expected √2.5 ≈ 1.5811)")

    # =================================================================
    # 12. Event gap statistics (v1 collected but never surfaced)
    # =================================================================
    print("\n=== 12. Event gap statistics ===")
    mc_events = MetricsCollector()
    for _ in range(5):
        mc_events.record_tool()
        time.sleep(0.005)
    snap_ev = mc_events.finalize_detailed()
    print(f"  event_count:               {snap_ev.event_count}")
    print(f"  mean_inter_event_gap_ms:   "
          f"{snap_ev.mean_inter_event_gap_ms:.2f}")
    print(f"  max_inter_event_gap_ms:    "
          f"{snap_ev.max_inter_event_gap_ms:.2f}")

    # =================================================================
    # 13. DecisionRecord with a strict mock (signature filter)
    # =================================================================
    print("\n=== 13. DecisionRecord with strict mock ===")

    class _StrictDR:
        def __init__(self, run_id, timestamp, task_id, selected_action,
                     latency_ms, energy_kwh, carbon_operational_kg,
                     explanation, provenance):
            self.run_id = run_id
            self.timestamp = timestamp
            self.task_id = task_id
            self.selected_action = selected_action
            self.latency_ms = latency_ms
            self.energy_kwh = energy_kwh
            self.carbon_operational_kg = carbon_operational_kg
            self.explanation = explanation
            self.provenance = provenance

    fake_mod = types.ModuleType("analysis")
    fake_mod.DecisionRecord = _StrictDR
    sys.modules["analysis"] = fake_mod

    import importlib
    import src.analysis.telemetry.metrics_collector as _mc_mod
    importlib.reload(_mc_mod)

    try:
        mc_dr = _mc_mod.MetricsCollector(
            run_id="run-dr",
            energy_meter=_MockMeterDetailed(),
        )
        mc_dr.record_tool()
        dr = mc_dr.to_decision_record(policy_version="v5.0.1")
        print(f"  record constructed: {dr is not None}")
        if dr:
            print(f"  selected_action:    {dr.selected_action}")
            print(f"  energy_kwh:         {dr.energy_kwh:.8f}")
            print(f"  provenance keys:    "
                  f"{sorted(dr.provenance.keys())[:5]}...")
    finally:
        del sys.modules["analysis"]
        importlib.reload(_mc_mod)

    # =================================================================
    # 14. to_dict(safe=True) with non-serializable payload
    # =================================================================
    print("\n=== 14. to_dict(safe=True) ===")
    mc_safe = MetricsCollector()
    mc_safe.record_tool()
    mc_safe.finalize()   # builds snapshot
    snap_safe = mc_safe.get_last_snapshot()
    snap_safe.explanation = {
        "headline": "x",
        "rationale": [object()],   # poison
    }
    try:
        json.dumps(snap_safe.to_dict(safe=True))
        print(f"  json.dumps(to_dict(safe=True)): OK")
    except (TypeError, ValueError) as e:
        print(f"  FAILED: {e}")

    # =================================================================
    # 15. reset(full=False|True)
    # =================================================================
    print("\n=== 15. reset() ===")
    mc_r = MetricsCollector(
        run_id="run-reset", precision="fp32",
        energy_meter=_MockMeterDetailed(),
    )
    mc_r.record_tool()
    mc_r.record_shorten_context_factor(0.25)
    mc_r.finalize()
    print(f"  before reset: tool_calls={mc_r.tool_calls}, "
          f"shorten={mc_r._shorten_factor}, "
          f"finalized={mc_r._finalized}")
    mc_r.reset()
    print(f"  after reset(): tool_calls={mc_r.tool_calls}, "
          f"shorten={mc_r._shorten_factor}, "
          f"finalized={mc_r._finalized}")
    print(f"  wiring preserved: {mc_r.energy_meter is not None}, "
          f"run_id={mc_r.run_id}, precision={mc_r.precision}")
    mc_r.reset(full=True)
    print(f"  after reset(full=True): run_id={mc_r.run_id}, "
          f"wiring={mc_r.energy_meter is not None}, "
          f"precision={mc_r.precision}")

    # =================================================================
    # 16. Statistics
    # =================================================================
    print("\n=== 16. Statistics ===")
    print(json.dumps(mc_bridge.get_statistics(), indent=2, default=str))
