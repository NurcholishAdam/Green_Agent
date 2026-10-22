# src/analysis/telemetry/self_monitor.py
"""
Self-monitoring introspection for Green Agent (Enhanced, v2.0.0)
=================================================================

Tracks CPU, memory, tool usage, and conversation depth over time.
Produces the `last_trend()` dict consumed by `AdaptiveController` to
choose between eco-modes (normal / low_memory / low_energy).

Original API preserved (all signatures, all side effects):
    monitor = SelfMonitor()
    monitor.snapshot(tool_calls=3, depth=2)     # -> dict
    trend = monitor.last_trend()                 # {"cpu_delta": ..., "memory_delta": ...}
    monitor.snapshots                            # bounded deque of Snapshot
    monitor.reset()

Enhanced API:
    monitor = SelfMonitor(
        run_id="...", task_id="...", agent_id="...",
        deployment_id="us-ca-prod-01",
        precision="int8",
        max_snapshots=1000,
        hitl_cpu_spike=50.0,      # configurable thresholds
        hitl_memory_spike=100.0,
    )

    snap  = monitor.snapshot(tool_calls=3, depth=2)   # dict (v1 shape)
    snap  = monitor.snapshot_detailed(...)             # Snapshot object

    trend = monitor.last_trend()                       # dict, cached
    trend = monitor.last_trend_detailed()              # TrendResult, cached
    trend = monitor.get_last_trend()                   # alias
    snap  = monitor.get_last_snapshot()

    monitor.set_hitl_callback(cb)     # (TrendResult) -> bool | None
    monitor.set_hitl_callback(None)   # clear

    dr = monitor.to_decision_record(emit_if_missing=False)
    monitor.attach_provenance(metrics=None)   # bridge to metric_provenance

    monitor.get_statistics()
    monitor.export(safe=True)
    monitor.reset(full=False)         # counters + snapshots + trends
    monitor.reset(full=True)          # also clears config, HITL, metrics

Enhancements implemented (v2.0.0):
  1.  Cached trends        — `last_trend()` / `last_trend_detailed()` /
                              `get_last_trend()` read a cached result and
                              **do not** re-increment counters, re-fire
                              HITL, or append to history on every read.
                              v1's `last_trend_detailed()` called
                              `last_trend()` internally, so a dashboard
                              polling it produced one history entry per
                              refresh. Cache invalidates on
                              `snapshot()` / `reset()`.
  2.  Monotonic clock      — `time.monotonic()` for elapsed. v1 used
                              `time.time()`, exposing it to NTP jumps.
  3.  Locked stats         — `_StatsCounter` with `RLock`; per-call
                              deltas to module counters; instance-scoped
                              counters do not merge with the global shim.
  4.  Locked reads         — `get_statistics()`, `export()`,
                              `get_last_snapshot()`, `attach_provenance()`
                              hold `self._lock`.
  5.  `max_snapshots`      — validated (`>= 1`); a caller passing `0` or
                              a negative value gets a `ValueError` up
                              front (v1 crashed inside `deque`).
  6.  HITL thresholds      — `hitl_cpu_spike` / `hitl_memory_spike` are
                              constructor kwargs. HITL callback's `False`
                              return annotates the trend
                              (`hitl_approved=False`, `[HITL: denied]`).
  7.  Structured logging   — `self_monitor.snapshot_taken` /
                              `.trend_computed` / `.psutil_failed` /
                              `.invalid_input` / `.hitl_denied`.
  8.  `to_dict(safe=True)` — JSON-safe serialisation on `Snapshot` and
                              `TrendResult`.
  9.  Provenance bridge    — `attach_provenance()` translates the
                              snapshot dict into `metric_provenance`'s
                              expected metric names (`memory_mb → memory`,
                              source labels preserved) so the bridge
                              doesn't resolve every entry to `unknown`
                              (v1 bug — the snapshot keys didn't match).
                              Returns `_provenance_error` on failure.
 10.  Signature-filtered DR — `to_decision_record()` uses
                              `inspect.signature(DecisionRecord)` so
                              contract drift returns None with a warning
                              instead of raising.
 11.  `snapshot_detailed()` — returns the `Snapshot` object (v1 only
                              exposed the dict view).
 12.  `get_last_snapshot()` / `get_last_trend()` — deterministic
                              accessors.
 13.  `reset(full=False|True)` — `full=True` clears `_psutil_failures`,
                              `_invalid_inputs`, `_trend_count`, history,
                              HITL callback, deployment metadata.
 14.  `export(safe=True)`  — JSON-safe export.
 15.  Elapsed-zero safety  — `elapsed_delta == 0` yields `None` rates
                              rather than `0.0` (misrepresents
                              instantaneous jumps as no-change).
 16.  `_validate_int`      — warns on float truncation (v1 silently
                              truncated `3.7 → 3`).
 17.  `__version__` / `__all__`.

Backward compatibility notes:
  * `snapshot(tool_calls, depth)` still returns a dict whose first five
    keys are `elapsed`, `cpu_percent`, `memory_mb`, `tool_calls`,
    `conversation_depth` — matching the v1 contract exactly.
  * `last_trend()` still returns `{}` when fewer than two snapshots
    exist. When a trend is available, the returned dict's first two keys
    are `cpu_delta` / `memory_delta`.
  * `elapsed` is now computed from a monotonic clock. Two snapshots'
    `elapsed` fields may differ slightly from a `time.time()`-based
    reading, but their delta is monotonic and NTP-proof.
  * `Snapshot.at` / `TrendResult.at` are timezone-aware UTC.
  * `metrics` aggregation with `max_snapshots=0` raises `ValueError`
    at construction (v1 raised `ValueError` deep inside `deque`).
  * `to_decision_record()` returns None when fewer than two snapshots
    exist. v1 called `last_trend_detailed()` which forced a trend
    computation, incrementing counters as a side-effect.
  * `attach_provenance()` now returns a dict with `metric_provenance`
    populated for the `memory` metric (mapped from `memory_mb`) — v1
    passed the raw snapshot dict which resolved to `unknown`.
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
from datetime import datetime, timezone
from enum import Enum
from typing import (
    Any, Callable, Deque, Dict, List, Optional, Tuple,
)

__version__ = "2.0.0"

__all__ = [
    "SelfMonitor",
    "Snapshot",
    "TrendResult",
    "TrendExplainer",
    "TrendKind",
    "Severity",
    "MetricSource",
    "get_statistics",
    "reset_statistics",
    "__version__",
]

logger = logging.getLogger(__name__)


# =============================================================================
# Optional psutil import
# =============================================================================

try:
    import psutil  # type: ignore
    _HAS_PSUTIL = True
except Exception:  # pragma: no cover
    psutil = None  # type: ignore
    _HAS_PSUTIL = False
    logger.warning(
        "psutil unavailable; SelfMonitor will report fallback values"
    )


# =============================================================================
# Enums
# =============================================================================

class TrendKind(Enum):
    """Classification of a trend between two snapshots."""
    STABLE = "stable"
    RISING = "rising"
    FALLING = "falling"
    SPIKE = "spike"
    RECOVERING = "recovering"
    UNKNOWN = "unknown"


class Severity(Enum):
    INFO = "info"
    WARNING = "warning"
    CRITICAL = "critical"
    EMERGENCY = "emergency"


class MetricSource(Enum):
    """Where a snapshot metric came from."""
    MEASURED = "measured"
    FALLBACK = "fallback"
    UNKNOWN = "unknown"


# =============================================================================
# Helpers
# =============================================================================

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


def _filter_dr_kwargs(
    DecisionRecord: Any, candidates: Dict[str, Any],
) -> Dict[str, Any]:
    try:
        sig = inspect.signature(DecisionRecord)
        accepted = set(sig.parameters.keys())
        return {k: v for k, v in candidates.items() if k in accepted}
    except (ValueError, TypeError):
        return dict(candidates)


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
# Thread-safe statistics
# =============================================================================

class _StatsCounter:
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
    """Module-level statistics for `SelfMonitor` calls."""
    snap = _GLOBAL_STATS.snapshot()
    return {
        "analyzer_version": __version__,
        "monitors_created": snap.get("monitors", 0),
        "snapshots_taken": snap.get("snapshots", 0),
        "psutil_failures": snap.get("psutil_failures", 0),
        "trends_computed": snap.get("trends", 0),
        "invalid_inputs": snap.get("invalid", 0),
    }


def reset_statistics() -> None:
    """Clear the module-level counter (v1 compatibility)."""
    _GLOBAL_STATS.reset()


# =============================================================================
# TrendResult
# =============================================================================

@dataclass
class TrendResult:
    """
    Structured trend between two snapshots.

    `to_dict()` returns the original two keys (`cpu_delta`,
    `memory_delta`) first, then extended fields. `to_dict(safe=True)`
    sanitises for JSON.
    """
    # --- v1 keys ---
    cpu_delta: float = 0.0
    memory_delta: float = 0.0

    # --- Extended deltas ---
    tool_calls_delta: int = 0
    depth_delta: int = 0
    elapsed_delta_s: float = 0.0

    # --- Rates (None when elapsed_delta <= 0) ---
    cpu_rate: Optional[float] = None
    memory_rate: Optional[float] = None
    tool_calls_rate: Optional[float] = None
    depth_rate: Optional[float] = None

    # --- Classification ---
    classification: str = TrendKind.UNKNOWN.value

    # --- Provenance ---
    run_id: Optional[str] = None
    task_id: Optional[str] = None
    agent_id: Optional[str] = None
    deployment_id: Optional[str] = None
    snapshot_index: int = 0

    # --- Trust ---
    simulated: bool = False

    # --- Safety / HITL ---
    needs_review: bool = False
    review_reason: Optional[str] = None
    hitl_approved: Optional[bool] = None

    # --- XAI ---
    explanation: Optional[Dict[str, Any]] = None

    # --- Metadata ---
    at: datetime = field(
        default_factory=lambda: datetime.now(timezone.utc)
    )

    def to_dict(self, safe: bool = False) -> Dict[str, Any]:
        """v1 keys first, then extended. `safe=True` → JSON-safe."""
        out: Dict[str, Any] = {
            "cpu_delta": self.cpu_delta,
            "memory_delta": self.memory_delta,
            "tool_calls_delta": self.tool_calls_delta,
            "depth_delta": self.depth_delta,
            "elapsed_delta_s": self.elapsed_delta_s,
            "cpu_rate": self.cpu_rate,
            "memory_rate": self.memory_rate,
            "tool_calls_rate": self.tool_calls_rate,
            "depth_rate": self.depth_rate,
            "classification": self.classification,
            "run_id": self.run_id,
            "task_id": self.task_id,
            "agent_id": self.agent_id,
            "deployment_id": self.deployment_id,
            "snapshot_index": self.snapshot_index,
            "simulated": self.simulated,
            "needs_review": self.needs_review,
            "review_reason": self.review_reason,
            "hitl_approved": self.hitl_approved,
            "explanation": self.explanation,
            "at": (
                self.at.isoformat()
                if isinstance(self.at, datetime) else self.at
            ),
        }
        if safe:
            return _jsonable(out)
        return out


# =============================================================================
# Snapshot
# =============================================================================

@dataclass
class Snapshot:
    """A single observation with provenance."""
    index: int
    elapsed: float
    cpu_percent: float
    memory_mb: float
    tool_calls: int
    conversation_depth: int
    cpu_source: str = MetricSource.MEASURED.value
    memory_source: str = MetricSource.MEASURED.value
    run_id: Optional[str] = None
    task_id: Optional[str] = None
    agent_id: Optional[str] = None
    deployment_id: Optional[str] = None
    precision: Optional[str] = None
    at: datetime = field(
        default_factory=lambda: datetime.now(timezone.utc)
    )

    def to_dict(self, safe: bool = False) -> Dict[str, Any]:
        """v1 five keys first, then extended. `safe=True` → JSON-safe."""
        out: Dict[str, Any] = {
            "elapsed": self.elapsed,
            "cpu_percent": self.cpu_percent,
            "memory_mb": self.memory_mb,
            "tool_calls": self.tool_calls,
            "conversation_depth": self.conversation_depth,
            "index": self.index,
            "cpu_source": self.cpu_source,
            "memory_source": self.memory_source,
            "run_id": self.run_id,
            "task_id": self.task_id,
            "agent_id": self.agent_id,
            "deployment_id": self.deployment_id,
            "precision": self.precision,
            "at": (
                self.at.isoformat()
                if isinstance(self.at, datetime) else self.at
            ),
        }
        if safe:
            return _jsonable(out)
        return out


# =============================================================================
# XAI
# =============================================================================

class TrendExplainer:
    @staticmethod
    def explain(trend: TrendResult) -> Dict[str, Any]:
        reasons: List[str] = [
            f"Δcpu={trend.cpu_delta:+.2f}%, "
            f"Δmemory={trend.memory_delta:+.2f} MB, "
            f"Δt={trend.elapsed_delta_s:.3f}s."
        ]
        if trend.elapsed_delta_s > 0 and trend.cpu_rate is not None:
            mr = (
                f"{trend.memory_rate:+.2f} MB/s"
                if trend.memory_rate is not None else "n/a"
            )
            reasons.append(
                f"Rates: cpu={trend.cpu_rate:+.2f}%/s, memory={mr}."
            )
        reasons.append(f"Classification: '{trend.classification}'.")
        if trend.tool_calls_delta:
            reasons.append(
                f"Tool calls changed by {trend.tool_calls_delta:+d}."
            )
        if trend.depth_delta:
            reasons.append(
                f"Conversation depth changed by {trend.depth_delta:+d}."
            )
        if trend.needs_review:
            reasons.append(
                f"WARNING: flagged for review ({trend.review_reason})."
            )
        if trend.hitl_approved is False:
            reasons.append("[HITL: denied] reviewer rejected this trend.")
        if trend.simulated:
            reasons.append(
                "WARNING: one or both snapshots contain fallback values."
            )
        return {
            "headline": (
                f"[{trend.classification}] "
                f"cpu={trend.cpu_delta:+.1f}%, "
                f"mem={trend.memory_delta:+.1f} MB"
            ),
            "rationale": reasons,
            "contributing_factors": {
                "cpu_delta": trend.cpu_delta,
                "memory_delta": trend.memory_delta,
                "elapsed_delta_s": trend.elapsed_delta_s,
            },
        }


# =============================================================================
# Trend classification
# =============================================================================

def _classify_trend(
    cpu_delta: float,
    memory_delta: float,
    elapsed_delta_s: float,
    *,
    cpu_spike_pct: float = 50.0,
    memory_spike_mb: float = 100.0,
) -> str:
    """Classify a trend between two snapshots."""
    if cpu_delta >= cpu_spike_pct or memory_delta >= memory_spike_mb:
        return TrendKind.SPIKE.value
    if cpu_delta <= -cpu_spike_pct or memory_delta <= -memory_spike_mb:
        return TrendKind.RECOVERING.value
    if (
        cpu_delta >= 0 and memory_delta >= 0
        and (cpu_delta > 0 or memory_delta > 0)
    ):
        return TrendKind.RISING.value
    if (
        cpu_delta <= 0 and memory_delta <= 0
        and (cpu_delta < 0 or memory_delta < 0)
    ):
        return TrendKind.FALLING.value
    return TrendKind.STABLE.value


# =============================================================================
# Integer validation
# =============================================================================

def _validate_int(
    value: Any, name: str,
) -> Tuple[bool, int, Optional[str]]:
    """
    Return `(ok, coerced_value, error_message)`.

    Truncation from float is allowed but flagged via `error_message`
    so the caller can log it — v1 silently truncated.
    """
    if isinstance(value, bool):
        return False, 0, f"{name} must be int, got bool"
    try:
        f = float(value)
    except (TypeError, ValueError):
        return False, 0, f"{name} must be numeric, got {type(value).__name__}"
    if not math.isfinite(f):
        return False, 0, f"{name} must be finite, got {value!r}"
    if f < 0:
        return False, 0, f"{name} must be non-negative, got {value!r}"
    if f != int(f):
        return True, int(f), f"{name} truncated from {value!r} to {int(f)}"
    return True, int(f), None


# =============================================================================
# SelfMonitor
# =============================================================================

class SelfMonitor:
    """
    Enhanced self-monitor with warm-up, structured trend output,
    provenance, XAI, and cache.

    Backward-compatible: `SelfMonitor()`, `snapshot(tool_calls, depth)`,
    and `last_trend()` preserve their v1 signatures and return shapes.
    """

    DEFAULT_MAX_SNAPSHOTS = 1000
    DEFAULT_HITL_CPU_SPIKE = 50.0
    DEFAULT_HITL_MEMORY_SPIKE = 100.0

    def __init__(
        self,
        *,
        run_id: Optional[str] = None,
        task_id: Optional[str] = None,
        agent_id: Optional[str] = None,
        deployment_id: str = "local",
        precision: Optional[str] = None,
        max_snapshots: int = DEFAULT_MAX_SNAPSHOTS,
        hitl_cpu_spike: Optional[float] = None,
        hitl_memory_spike: Optional[float] = None,
        features: Optional[Dict[str, bool]] = None,
    ):
        # --- Validate max_snapshots (v1 crashed inside deque) ---
        if not isinstance(max_snapshots, int) or isinstance(
            max_snapshots, bool,
        ):
            raise TypeError(
                f"max_snapshots must be an int, got "
                f"{type(max_snapshots).__name__}"
            )
        if max_snapshots < 1:
            raise ValueError(
                f"max_snapshots must be >= 1, got {max_snapshots}"
            )

        # --- Validate precision ---
        if precision is not None and not isinstance(precision, str):
            raise TypeError(
                f"precision must be a string or None, got "
                f"{type(precision).__name__}"
            )

        # --- v1 public state ---
        self.start_time = time.monotonic()
        self.snapshots: Deque[Snapshot] = deque(maxlen=max_snapshots)

        # --- Identity ---
        self.run_id = run_id or f"sm-{uuid.uuid4().hex[:8]}"
        self.task_id = task_id
        self.agent_id = agent_id
        self.deployment_id = deployment_id
        self.precision = precision
        self._max_snapshots = max_snapshots

        # --- HITL thresholds (configurable) ---
        self._hitl_cpu_spike = self._coerce_threshold(
            hitl_cpu_spike, self.DEFAULT_HITL_CPU_SPIKE, "hitl_cpu_spike",
        )
        self._hitl_memory_spike = self._coerce_threshold(
            hitl_memory_spike, self.DEFAULT_HITL_MEMORY_SPIKE,
            "hitl_memory_spike",
        )

        # --- Feature flags ---
        self.features: Dict[str, bool] = {
            "warm_up_cpu": True,
            "cache_process": True,
            "bounded_snapshots": True,
            "validation": True,
            "xai": True,
            "hitl": True,
            "statistics": True,
            "classify_trends": True,
            "thread_safety": True,
        }
        if features:
            self.features.update(features)

        # --- Cached psutil.Process ---
        self._process: Any = None
        if _HAS_PSUTIL and self.features.get("cache_process", True):
            try:
                self._process = psutil.Process()
            except Exception as e:
                logger.warning(
                    "self_monitor.init_process_failed",
                    extra={"error": str(e)},
                )
                self._process = None

        # --- CPU warm-up ---
        if (
            self._process is not None
            and self.features.get("warm_up_cpu", True)
        ):
            try:
                self._process.cpu_percent(interval=None)
            except Exception as e:
                logger.debug(
                    "self_monitor.warm_up_failed",
                    extra={"error": str(e)},
                )

        # --- Concurrency ---
        self._lock = threading.RLock()

        # --- Statistics (instance-scoped) ---
        self._stats = _StatsCounter()
        self._snapshot_count: int = 0
        self._psutil_failures: int = 0
        self._invalid_inputs: int = 0
        self._trend_count: int = 0
        self._history: Deque[TrendResult] = deque(maxlen=256)

        # --- Trend cache ---
        self._cached_trend: Optional[TrendResult] = None
        self._cached_trend_snapshot_index: int = 0
        self._last_decision_record: Optional[Any] = None

        # --- HITL ---
        self._hitl_callback: Optional[
            Callable[[TrendResult], Optional[bool]]
        ] = None

        _GLOBAL_STATS.increment("monitors")

        logger.debug(
            "self_monitor.initialized",
            extra={
                "run_id": self.run_id,
                "max_snapshots": max_snapshots,
                "psutil": _HAS_PSUTIL,
            },
        )

    @staticmethod
    def _coerce_threshold(
        value: Any, default: float, name: str,
    ) -> float:
        if value is None:
            return float(default)
        if isinstance(value, bool):
            raise TypeError(f"{name} must be numeric, got bool")
        try:
            f = float(value)
        except (TypeError, ValueError):
            raise TypeError(
                f"{name} must be numeric, got {type(value).__name__}"
            )
        if math.isnan(f) or math.isinf(f) or f < 0:
            raise ValueError(
                f"{name} must be finite and >= 0, got {value!r}"
            )
        return f

    # ------------------------------------------------------------------
    # Recording
    # ------------------------------------------------------------------

    def snapshot(
        self,
        tool_calls: Any = 0,
        depth: Any = 0,
    ) -> Dict[str, Any]:
        """
        Record a snapshot of the current process state.

        Backward-compatible: same signature, same return shape
        (`elapsed`, `cpu_percent`, `memory_mb`, `tool_calls`,
        `conversation_depth`).
        """
        snap = self.snapshot_detailed(tool_calls, depth)
        return snap.to_dict()

    def snapshot_detailed(
        self,
        tool_calls: Any = 0,
        depth: Any = 0,
    ) -> Snapshot:
        """
        Record a snapshot and return the `Snapshot` object.

        The returned object is the same instance stored in
        `self.snapshots`; treat it as read-only.
        """
        # --- Validate tool_calls / depth ---
        ok_tc, tc_int, warn_tc = _validate_int(tool_calls, "tool_calls")
        ok_dp, dp_int, warn_dp = _validate_int(depth, "depth")

        if self.features.get("validation", True):
            if not ok_tc:
                with self._lock:
                    self._invalid_inputs += 1
                    self._stats.increment("invalid")
                _GLOBAL_STATS.increment("invalid")
                logger.warning(
                    "self_monitor.invalid_input",
                    extra={"field": "tool_calls", "error": warn_tc},
                )
                tc_int = 0
            elif warn_tc:
                logger.debug(
                    "self_monitor.truncated_input",
                    extra={"field": "tool_calls", "message": warn_tc},
                )
            if not ok_dp:
                with self._lock:
                    self._invalid_inputs += 1
                    self._stats.increment("invalid")
                _GLOBAL_STATS.increment("invalid")
                logger.warning(
                    "self_monitor.invalid_input",
                    extra={"field": "depth", "error": warn_dp},
                )
                dp_int = 0
            elif warn_dp:
                logger.debug(
                    "self_monitor.truncated_input",
                    extra={"field": "depth", "message": warn_dp},
                )

        with self._lock:
            self._snapshot_count += 1
            index = self._snapshot_count
            self._stats.increment("snapshots")

        # --- Read CPU and memory (may hold the psutil lock internally) ---
        cpu_percent, cpu_source = self._read_cpu()
        memory_mb, memory_source = self._read_memory()

        elapsed = time.monotonic() - self.start_time

        snap = Snapshot(
            index=index,
            elapsed=elapsed,
            cpu_percent=cpu_percent,
            memory_mb=memory_mb,
            tool_calls=tc_int,
            conversation_depth=dp_int,
            cpu_source=cpu_source,
            memory_source=memory_source,
            run_id=self.run_id,
            task_id=self.task_id,
            agent_id=self.agent_id,
            deployment_id=self.deployment_id,
            precision=self.precision,
        )

        with self._lock:
            self.snapshots.append(snap)
            # Invalidate the trend cache
            self._cached_trend = None
            self._cached_trend_snapshot_index = 0

        _GLOBAL_STATS.increment("snapshots")

        logger.info(
            "self_monitor.snapshot_taken",
            extra={
                "index": index,
                "cpu_percent": cpu_percent,
                "memory_mb": memory_mb,
                "cpu_source": cpu_source,
                "memory_source": memory_source,
            },
        )
        return snap

    # ------------------------------------------------------------------
    # Trend (cached)
    # ------------------------------------------------------------------

    def last_trend(self) -> Dict[str, Any]:
        """
        Return the trend between the last two snapshots.

        Backward-compatible: `{}` when fewer than two snapshots exist;
        otherwise a dict with `cpu_delta` / `memory_delta` first.

        The result is cached; consecutive calls with no new snapshot
        return the same object **without re-incrementing counters,
        re-appending to history, or re-firing HITL**.
        """
        trend = self.get_last_trend()
        if trend is None:
            return {}
        return trend.to_dict()

    def last_trend_detailed(self) -> Optional[TrendResult]:
        """Return the cached `TrendResult`, or None if <2 snapshots."""
        return self.get_last_trend()

    def get_last_trend(self) -> Optional[TrendResult]:
        """
        Return the cached trend, computing it once if needed.

        After the first computation, subsequent calls (without a new
        snapshot) return the same object.
        """
        with self._lock:
            if len(self.snapshots) < 2:
                return None
            latest_index = self.snapshots[-1].index
            if (
                self._cached_trend is not None
                and self._cached_trend_snapshot_index == latest_index
            ):
                return self._cached_trend

        trend = self._compute_trend()
        with self._lock:
            self._cached_trend = trend
            self._cached_trend_snapshot_index = (
                self.snapshots[-1].index if self.snapshots else 0
            )
        return trend

    def get_last_snapshot(self) -> Optional[Snapshot]:
        """Return the most recent `Snapshot`, or None."""
        with self._lock:
            return self.snapshots[-1] if self.snapshots else None

    # ------------------------------------------------------------------
    # Provenance bridge
    # ------------------------------------------------------------------

    def attach_provenance(
        self,
        metrics: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        Bridge to `metric_provenance.attach_provenance_enhanced`.

        When `metrics` is None, uses the latest snapshot. The snapshot's
        `memory_mb` field is mapped to `metric_provenance`'s expected
        `memory` key so the bridge resolves to the correct source
        (v1 passed raw snapshot keys which resolved to `unknown`).
        """
        if metrics is None:
            snap = self.get_last_snapshot()
            if snap is None:
                return {
                    "metric_provenance": {},
                    "metric_provenance_detailed": {},
                    "metric_provenance_summary": {},
                    "_provenance_error": "no_snapshot_available",
                }
            metrics = self._translate_for_provenance(snap)

        try:
            from src.analysis.telemetry.metric_provenance import (
                attach_provenance_enhanced as _attach,
            )
        except Exception:
            try:
                from analysis.metric_provenance import (  # type: ignore
                    attach_provenance_enhanced as _attach,
                )
            except Exception as e:
                logger.debug(
                    "self_monitor.provenance_bridge_failed",
                    extra={"error": str(e)},
                )
                if isinstance(metrics, dict):
                    metrics = dict(metrics)
                    metrics["metric_provenance"] = {}
                    metrics["metric_provenance_detailed"] = {}
                    metrics["metric_provenance_summary"] = {}
                    metrics["_provenance_error"] = str(e)
                return metrics

        try:
            enriched = _attach(
                metrics,
                default_metrics=["memory"],
                in_place=False,
            )
        except Exception as e:
            logger.warning(
                "self_monitor.provenance_bridge_failed",
                extra={"error": str(e)},
            )
            if isinstance(metrics, dict):
                metrics = dict(metrics)
                metrics["metric_provenance"] = {}
                metrics["metric_provenance_detailed"] = {}
                metrics["metric_provenance_summary"] = {}
                metrics["_provenance_error"] = str(e)
            return metrics

        return enriched

    def _translate_for_provenance(
        self, snap: Snapshot,
    ) -> Dict[str, Any]:
        """Map a Snapshot into the key space metric_provenance expects."""
        return {
            "memory": snap.memory_mb,
            "memory_source": (
                "meter" if snap.memory_source
                == MetricSource.MEASURED.value
                else "fallback"
            ),
            "cpu_percent": snap.cpu_percent,
            "cpu_source": snap.cpu_source,
            "run_id": snap.run_id,
            "task_id": snap.task_id,
            "agent_id": snap.agent_id,
            "deployment_id": snap.deployment_id,
            "precision": snap.precision,
            "simulated": (
                snap.cpu_source != MetricSource.MEASURED.value
                or snap.memory_source != MetricSource.MEASURED.value
            ),
        }

    # ------------------------------------------------------------------
    # DecisionRecord
    # ------------------------------------------------------------------

    def to_decision_record(
        self,
        *,
        emit_if_missing: bool = False,
        policy_version: str = "",
    ) -> Optional[Any]:
        """
        Emit a DecisionRecord for the most recent trend.

        `emit_if_missing=True` forces a trend computation when fewer
        than two snapshots exist. Default: returns None rather than
        triggering a side-effecting trend computation (v1 always did).
        """
        trend = self.get_last_trend()
        if trend is None and emit_if_missing:
            trend = self._compute_trend()
            with self._lock:
                self._cached_trend = trend
                self._cached_trend_snapshot_index = (
                    self.snapshots[-1].index
                    if self.snapshots else 0
                )
        if trend is None:
            return None

        DecisionRecord = _import_decision_record()
        if DecisionRecord is None:
            logger.debug(
                "self_monitor.decision_record_import_failed",
            )
            return None

        candidates: Dict[str, Any] = {
            "run_id": trend.run_id or self.run_id,
            "timestamp": trend.at,
            "task_id": trend.task_id or "",
            "selected_action": (
                f"self_monitor_trend({trend.classification})"
            ),
            "alternatives": [],
            "policy_version": policy_version,
            "model_or_agent": trend.agent_id or "",
            "latency_ms": trend.elapsed_delta_s * 1000.0,
            "explanation": _jsonable(trend.explanation or {}),
            "provenance": {
                "source": "self_monitor",
                "analyzer_version": __version__,
                "cpu_delta": trend.cpu_delta,
                "memory_delta": trend.memory_delta,
                "classification": trend.classification,
                "simulated": trend.simulated,
                "needs_review": trend.needs_review,
                "hitl_approved": trend.hitl_approved,
                "deployment_id": self.deployment_id,
            },
        }
        filtered = _filter_dr_kwargs(DecisionRecord, candidates)
        try:
            record = DecisionRecord(**filtered)
        except TypeError as e:
            logger.warning(
                "self_monitor.decision_record_failed",
                extra={"error": str(e)},
            )
            return None

        with self._lock:
            self._last_decision_record = record
        return record

    def get_last_decision_record(self) -> Optional[Any]:
        with self._lock:
            return self._last_decision_record

    # ------------------------------------------------------------------
    # Configuration
    # ------------------------------------------------------------------

    def set_hitl_callback(
        self,
        callback: Optional[
            Callable[[TrendResult], Optional[bool]]
        ],
    ) -> None:
        """
        Register (or clear) a HITL reviewer for extreme trends.

        Returning `False` sets `hitl_approved=False` on the trend and
        appends `[HITL: denied]` to `review_reason`.
        """
        with self._lock:
            self._hitl_callback = callback

    # ------------------------------------------------------------------
    # Statistics / export / reset
    # ------------------------------------------------------------------

    def get_statistics(self) -> Dict[str, Any]:
        """Instance-scoped statistics + configuration."""
        with self._lock:
            snap = self._stats.snapshot()
            snapshots = list(self.snapshots)
            history = list(self._history)
            invalid_inputs = self._invalid_inputs
            psutil_failures = self._psutil_failures
            trend_count = self._trend_count

        stats: Dict[str, Any] = {
            "analyzer_version": __version__,
            "run_id": self.run_id,
            "deployment_id": self.deployment_id,
            "agent_id": self.agent_id,
            "task_id": self.task_id,
            "snapshots_stored": len(snapshots),
            "snapshots_taken": self._snapshot_count,
            "max_snapshots": self._max_snapshots,
            "trends_computed": trend_count,
            "psutil_failures": psutil_failures,
            "invalid_inputs": invalid_inputs,
            "hitl_cpu_spike": self._hitl_cpu_spike,
            "hitl_memory_spike": self._hitl_memory_spike,
            "features": dict(self.features),
            "psutil_available": _HAS_PSUTIL,
            "cache_valid": self._cached_trend is not None,
            "instance_counters": dict(snap),
        }

        if snapshots:
            cpus = [s.cpu_percent for s in snapshots]
            mems = [s.memory_mb for s in snapshots]
            stats["cpu_stats"] = {
                "mean": sum(cpus) / len(cpus),
                "min": min(cpus),
                "max": max(cpus),
            }
            stats["memory_stats"] = {
                "mean": sum(mems) / len(mems),
                "min": min(mems),
                "max": max(mems),
            }

        if history:
            counts: Counter = Counter()
            for t in history:
                counts[t.classification] += 1
            stats["trend_distribution"] = dict(counts)
        return stats

    def export(self, *, safe: bool = False) -> Dict[str, Any]:
        """Full serialisable export. `safe=True` sanitises for JSON."""
        with self._lock:
            payload = {
                "analyzer_version": __version__,
                "statistics": self.get_statistics(),
                "snapshots": [
                    s.to_dict(safe=safe) for s in self.snapshots
                ],
                "trends": [
                    t.to_dict(safe=safe) for t in self._history
                ],
            }
        if safe:
            return _jsonable(payload)
        return payload

    def reset(self, *, full: bool = False) -> None:
        """
        Reset the monitor.

        `full=False` (default) clears snapshots, trends, cache, and
        counters (matching v1's visible behaviour). `full=True` also
        clears `_psutil_failures` / `_invalid_inputs`, the HITL callback,
        identity metadata, and restores default thresholds.
        """
        with self._lock:
            self.snapshots.clear()
            self._history.clear()
            self.start_time = time.monotonic()
            self._snapshot_count = 0
            self._trend_count = 0
            self._cached_trend = None
            self._cached_trend_snapshot_index = 0
            self._last_decision_record = None
            self._stats.reset()
            if full:
                self._psutil_failures = 0
                self._invalid_inputs = 0
                self._hitl_callback = None
                self.run_id = f"sm-{uuid.uuid4().hex[:8]}"
                self.task_id = None
                self.agent_id = None
                self.deployment_id = "local"
                self.precision = None
                self.features = {
                    "warm_up_cpu": True,
                    "cache_process": True,
                    "bounded_snapshots": True,
                    "validation": True,
                    "xai": True,
                    "hitl": True,
                    "statistics": True,
                    "classify_trends": True,
                    "thread_safety": True,
                }
                self._hitl_cpu_spike = self.DEFAULT_HITL_CPU_SPIKE
                self._hitl_memory_spike = self.DEFAULT_HITL_MEMORY_SPIKE

        # Re-warm the CPU baseline after reset
        if (
            self._process is not None
            and self.features.get("warm_up_cpu", True)
        ):
            try:
                self._process.cpu_percent(interval=None)
            except Exception:
                pass

        logger.debug(
            "self_monitor.reset",
            extra={"run_id": self.run_id, "full": full},
        )

    # ------------------------------------------------------------------
    # Trend computation (internal, cached)
    # ------------------------------------------------------------------

    def _compute_trend(self) -> TrendResult:
        """
        Compute a fresh trend. Called by `get_last_trend()` when the
        cache is stale; appends to history and fires HITL as a side
        effect.
        """
        with self._lock:
            if len(self.snapshots) < 2:
                # Callers guard against this, but belt-and-braces:
                return TrendResult(
                    run_id=self.run_id,
                    task_id=self.task_id,
                    agent_id=self.agent_id,
                    deployment_id=self.deployment_id,
                    classification=TrendKind.UNKNOWN.value,
                )
            prev = self.snapshots[-2]
            curr = self.snapshots[-1]
            self._trend_count += 1
            self._stats.increment("trends")

        _GLOBAL_STATS.increment("trends")

        elapsed_delta = max(0.0, curr.elapsed - prev.elapsed)
        cpu_delta = curr.cpu_percent - prev.cpu_percent
        memory_delta = curr.memory_mb - prev.memory_mb
        tool_calls_delta = curr.tool_calls - prev.tool_calls
        depth_delta = curr.conversation_depth - prev.conversation_depth

        # --- Rates: None when elapsed_delta is 0 (v1 returned 0.0,
        #     misrepresenting instantaneous jumps as no-change) ---
        if elapsed_delta > 0:
            cpu_rate: Optional[float] = cpu_delta / elapsed_delta
            memory_rate: Optional[float] = memory_delta / elapsed_delta
            tool_calls_rate: Optional[float] = (
                tool_calls_delta / elapsed_delta
            )
            depth_rate: Optional[float] = depth_delta / elapsed_delta
        else:
            cpu_rate = memory_rate = None
            tool_calls_rate = depth_rate = None

        # --- Classification ---
        if self.features.get("classify_trends", True):
            classification = _classify_trend(
                cpu_delta, memory_delta, elapsed_delta,
                cpu_spike_pct=self._hitl_cpu_spike,
                memory_spike_mb=self._hitl_memory_spike,
            )
        else:
            classification = TrendKind.UNKNOWN.value

        # --- Simulated flag ---
        simulated = (
            prev.cpu_source != MetricSource.MEASURED.value
            or curr.cpu_source != MetricSource.MEASURED.value
            or prev.memory_source != MetricSource.MEASURED.value
            or curr.memory_source != MetricSource.MEASURED.value
        )

        # --- HITL review flag ---
        needs_review = False
        review_reason: Optional[str] = None
        if self.features.get("hitl", True):
            if cpu_delta >= self._hitl_cpu_spike:
                needs_review = True
                review_reason = (
                    f"cpu_delta {cpu_delta:.1f}% >= "
                    f"{self._hitl_cpu_spike:.1f}%"
                )
            elif memory_delta >= self._hitl_memory_spike:
                needs_review = True
                review_reason = (
                    f"memory_delta {memory_delta:.1f} MB >= "
                    f"{self._hitl_memory_spike:.1f} MB"
                )
            elif simulated:
                needs_review = True
                review_reason = (
                    "one or both snapshots contain fallback values"
                )

        trend = TrendResult(
            cpu_delta=cpu_delta,
            memory_delta=memory_delta,
            tool_calls_delta=tool_calls_delta,
            depth_delta=depth_delta,
            elapsed_delta_s=elapsed_delta,
            cpu_rate=cpu_rate,
            memory_rate=memory_rate,
            tool_calls_rate=tool_calls_rate,
            depth_rate=depth_rate,
            classification=classification,
            run_id=self.run_id,
            task_id=self.task_id,
            agent_id=self.agent_id,
            deployment_id=self.deployment_id,
            snapshot_index=curr.index,
            simulated=simulated,
            needs_review=needs_review,
            review_reason=review_reason,
        )

        if self.features.get("xai", True):
            trend.explanation = TrendExplainer.explain(trend)

        # --- HITL callback (denial annotation) ---
        if needs_review and self._hitl_callback is not None:
            try:
                approved = self._hitl_callback(trend)
                if approved is False:
                    trend.hitl_approved = False
                    trend.review_reason = (
                        (trend.review_reason or "")
                        + " [HITL: denied]"
                    ).strip()
                    logger.info(
                        "self_monitor.hitl_denied",
                        extra={
                            "run_id": self.run_id,
                            "classification": classification,
                        },
                    )
                elif approved is True:
                    trend.hitl_approved = True
            except Exception as e:
                logger.warning(
                    "self_monitor.hitl_callback_failed",
                    extra={"error": str(e)},
                )

        with self._lock:
            self._history.append(trend)

        logger.info(
            "self_monitor.trend_computed",
            extra={
                "run_id": self.run_id,
                "classification": classification,
                "cpu_delta": cpu_delta,
                "memory_delta": memory_delta,
                "elapsed_delta_s": elapsed_delta,
                "simulated": simulated,
                "needs_review": needs_review,
            },
        )
        return trend

    # ------------------------------------------------------------------
    # psutil readers
    # ------------------------------------------------------------------

    def _read_cpu(self) -> Tuple[float, str]:
        """Read CPU% since last call; returns (value, source)."""
        if not _HAS_PSUTIL:
            return 0.0, MetricSource.FALLBACK.value

        with self._lock:
            process = self._process
            if process is None:
                try:
                    process = psutil.Process()
                    self._process = process
                except Exception as e:
                    self._psutil_failures += 1
                    self._stats.increment("psutil_failures")
                    _GLOBAL_STATS.increment("psutil_failures")
                    logger.warning(
                        "self_monitor.psutil_failed",
                        extra={"op": "Process", "error": str(e)},
                    )
                    return 0.0, MetricSource.FALLBACK.value

        try:
            value = float(process.cpu_percent(interval=None))
            if not math.isfinite(value) or value < 0:
                return 0.0, MetricSource.FALLBACK.value
            return value, MetricSource.MEASURED.value
        except Exception as e:
            with self._lock:
                self._psutil_failures += 1
                self._stats.increment("psutil_failures")
                _GLOBAL_STATS.increment("psutil_failures")
                try:
                    self._process = psutil.Process()
                except Exception:
                    self._process = None
            logger.warning(
                "self_monitor.psutil_failed",
                extra={"op": "cpu_percent", "error": str(e)},
            )
            return 0.0, MetricSource.FALLBACK.value

    def _read_memory(self) -> Tuple[float, str]:
        """Read RSS in MB; returns (value, source)."""
        if not _HAS_PSUTIL:
            return 0.0, MetricSource.FALLBACK.value

        with self._lock:
            process = self._process
            if process is None:
                try:
                    process = psutil.Process()
                    self._process = process
                except Exception as e:
                    self._psutil_failures += 1
                    self._stats.increment("psutil_failures")
                    _GLOBAL_STATS.increment("psutil_failures")
                    logger.warning(
                        "self_monitor.psutil_failed",
                        extra={"op": "Process", "error": str(e)},
                    )
                    return 0.0, MetricSource.FALLBACK.value

        try:
            info = process.memory_info()
            value = float(info.rss) / (1024 ** 2)
            if not math.isfinite(value) or value < 0:
                return 0.0, MetricSource.FALLBACK.value
            return value, MetricSource.MEASURED.value
        except Exception as e:
            with self._lock:
                self._psutil_failures += 1
                self._stats.increment("psutil_failures")
                _GLOBAL_STATS.increment("psutil_failures")
                try:
                    self._process = psutil.Process()
                except Exception:
                    self._process = None
            logger.warning(
                "self_monitor.psutil_failed",
                extra={"op": "memory_info", "error": str(e)},
            )
            return 0.0, MetricSource.FALLBACK.value


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
    monitor = SelfMonitor()
    s1 = monitor.snapshot(tool_calls=1, depth=1)
    time.sleep(0.05)
    s2 = monitor.snapshot(tool_calls=2, depth=1)
    print(f"  snapshot 1: cpu={s1['cpu_percent']}%, "
          f"mem={s1['memory_mb']:.1f} MB")
    print(f"  snapshot 2: cpu={s2['cpu_percent']}%, "
          f"mem={s2['memory_mb']:.1f} MB")
    print(f"  v1 keys in snapshot: "
          f"{all(k in s1 for k in ('elapsed', 'cpu_percent', 'memory_mb', 'tool_calls', 'conversation_depth'))}")

    # =================================================================
    # 2. Trend cache — repeated reads don't recompute
    # =================================================================
    print("\n=== 2. Trend cache ===")
    cache_mon = SelfMonitor()
    cache_mon.snapshot(tool_calls=0, depth=0)
    time.sleep(0.01)
    cache_mon.snapshot(tool_calls=1, depth=0)
    cache_mon.last_trend()
    before = cache_mon.get_statistics()["trends_computed"]

    for _ in range(50):
        cache_mon.last_trend()
        cache_mon.last_trend_detailed()
        cache_mon.get_last_trend()

    after = cache_mon.get_statistics()["trends_computed"]
    print(f"  trends_computed before: {before}")
    print(f"  trends_computed after 150 reads: {after}  "
          f"(expected unchanged)")
    print(f"  history size: {len(cache_mon._history)}  "
          f"(expected 1)")

    # =================================================================
    # 3. Malformed input truncation warning
    # =================================================================
    print("\n=== 3. Truncation warning ===")
    trunc = SelfMonitor()
    snap_trunc = trunc.snapshot_detailed(tool_calls=3.7, depth=2)
    print(f"  tool_calls=3.7 → stored: {snap_trunc.tool_calls}")
    print(f"  (v1 truncated silently; v2 logs at DEBUG)")

    # =================================================================
    # 4. Reset does not clear psutil_failures by default
    # =================================================================
    print("\n=== 4. reset(full=False) vs reset(full=True) ===")
    reset_mon = SelfMonitor()
    reset_mon._psutil_failures = 5
    reset_mon._invalid_inputs = 3
    reset_mon.snapshot()
    reset_mon.snapshot()
    reset_mon.reset()
    s_after = reset_mon.get_statistics()
    print(f"  after reset(): psutil_failures={s_after['psutil_failures']}, "
          f"invalid_inputs={s_after['invalid_inputs']}  "
          f"(v1 preserved)")
    reset_mon.reset(full=True)
    s_full = reset_mon.get_statistics()
    print(f"  after reset(full=True): psutil_failures="
          f"{s_full['psutil_failures']}, invalid_inputs="
          f"{s_full['invalid_inputs']}")

    # =================================================================
    # 5. HITL denial annotation
    # =================================================================
    print("\n=== 5. HITL denial ===")
    hitl_mon = SelfMonitor(hitl_cpu_spike=1.0)
    hitl_mon.snapshot()
    # Force a review via the low threshold
    time.sleep(0.02)
    hitl_mon.snapshot()
    hitl_mon.set_hitl_callback(lambda t: False)
    trend_hitl = hitl_mon.get_last_trend()
    if trend_hitl is None:
        # Second snapshot triggers computation
        hitl_mon.last_trend()
        trend_hitl = hitl_mon.get_last_trend()
    if trend_hitl is not None and trend_hitl.needs_review:
        print(f"  needs_review:   {trend_hitl.needs_review}")
        print(f"  hitl_approved:  {trend_hitl.hitl_approved}")
        print(f"  review_reason:  {trend_hitl.review_reason}")
        print(f"  denial noted:   "
              f"{'[HITL: denied]' in (trend_hitl.review_reason or '')}")
    else:
        print("  (no review triggered on this run; threshold too high)")

    # =================================================================
    # 6. Configurable thresholds
    # =================================================================
    print("\n=== 6. Configurable HITL thresholds ===")
    cfg_mon = SelfMonitor(
        hitl_cpu_spike=10.0, hitl_memory_spike=50.0,
    )
    stats_cfg = cfg_mon.get_statistics()
    print(f"  hitl_cpu_spike:    {stats_cfg['hitl_cpu_spike']}")
    print(f"  hitl_memory_spike: {stats_cfg['hitl_memory_spike']}")
    try:
        SelfMonitor(hitl_cpu_spike=-5.0)
        print("  negative threshold accepted (unexpected)")
    except ValueError as e:
        print(f"  negative threshold rejected: {e}")

    # =================================================================
    # 7. max_snapshots validation
    # =================================================================
    print("\n=== 7. max_snapshots validation ===")
    try:
        SelfMonitor(max_snapshots=0)
        print("  0 accepted (unexpected)")
    except ValueError as e:
        print(f"  max_snapshots=0 → ValueError: {str(e)[:50]}...")
    try:
        SelfMonitor(max_snapshots=-10)
        print("  negative accepted (unexpected)")
    except ValueError as e:
        print(f"  max_snapshots=-10 → ValueError: {str(e)[:50]}...")

    # =================================================================
    # 8. Provenance bridge (v1 resolved to unknown)
    # =================================================================
    print("\n=== 8. Provenance bridge ===")
    prov_mon = SelfMonitor()
    prov_mon.snapshot()
    enriched = prov_mon.attach_provenance()
    if "metric_provenance" in enriched and enriched["metric_provenance"]:
        print(f"  metric_provenance: {enriched['metric_provenance']}")
        d = enriched.get("metric_provenance_detailed", {}).get("memory", {})
        print(f"  memory source_kind: {d.get('source_kind')}  "
              f"(expected 'measured' or 'fallback', NOT 'unknown')")
    else:
        print(f"  provenance bridge unavailable: "
              f"{enriched.get('_provenance_error', 'no error')}")

    # =================================================================
    # 9. to_decision_record doesn't fire trend computation by default
    # =================================================================
    print("\n=== 9. to_decision_record (no side-effect) ===")
    dr_mon = SelfMonitor()
    # Only one snapshot — no trend possible
    dr_mon.snapshot()
    before = dr_mon.get_statistics()["trends_computed"]
    dr = dr_mon.to_decision_record()
    after = dr_mon.get_statistics()["trends_computed"]
    print(f"  to_decision_record() returned: {dr}")
    print(f"  trends_computed delta: {after - before}  "
          f"(expected 0 — v1 would have been 1)")

    # With emit_if_missing=True
    dr2 = dr_mon.to_decision_record(emit_if_missing=True)
    after2 = dr_mon.get_statistics()["trends_computed"]
    print(f"  emit_if_missing=True → returned: {dr2 is not None}, "
          f"trends_computed delta: {after2 - before}")

    # =================================================================
    # 10. JSON-safe export
    # =================================================================
    print("\n=== 10. JSON-safe export ===")
    export_mon = SelfMonitor()
    export_mon.snapshot()
    export_mon.snapshot()
    try:
        json.dumps(export_mon.export(safe=True))
        print("  json.dumps(export(safe=True)): OK")
    except (TypeError, ValueError) as e:
        print(f"  FAILED: {e}")

    # =================================================================
    # 11. Thread safety
    # =================================================================
    print("\n=== 11. Thread safety ===")
    reset_statistics()
    conc = SelfMonitor()
    barrier = threading.Barrier(4)

    def _worker():
        barrier.wait()
        for i in range(50):
            conc.snapshot(tool_calls=i, depth=1)

    threads = [threading.Thread(target=_worker) for _ in range(4)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    stats_conc = conc.get_statistics()
    print(f"  snapshots_taken: {stats_conc['snapshots_taken']}  "
          f"(expected 200)")

    # =================================================================
    # 12. Async DecisionRecord with strict mock
    # =================================================================
    print("\n=== 12. DecisionRecord with strict mock ===")

    class _StrictDR:
        def __init__(self, run_id, timestamp, task_id,
                     selected_action, explanation, provenance):
            self.run_id = run_id
            self.timestamp = timestamp
            self.task_id = task_id
            self.selected_action = selected_action
            self.explanation = explanation
            self.provenance = provenance

    fake_mod = types.ModuleType("analysis")
    fake_mod.DecisionRecord = _StrictDR
    sys.modules["analysis"] = fake_mod

    import importlib
    import src.analysis.telemetry.self_monitor as _sm_mod
    importlib.reload(_sm_mod)

    try:
        dr_strict = _sm_mod.SelfMonitor()
        dr_strict.snapshot()
        dr_strict.snapshot()
        dr = dr_strict.to_decision_record()
        print(f"  constructed: {dr is not None}")
        if dr:
            print(f"  selected_action: {dr.selected_action}")
            print(f"  provenance keys: "
                  f"{sorted(dr.provenance.keys())[:5]}...")
    finally:
        del sys.modules["analysis"]
        importlib.reload(_sm_mod)

    # =================================================================
    # 13. Full statistics
    # =================================================================
    print("\n=== 13. Full statistics ===")
    print(json.dumps(monitor.get_statistics(), indent=2, default=str))
