# src/analysis/telemetry/self_monitor.py

"""
Self-monitoring introspection for Green Agent (Enhanced)
==========================================================

Tracks CPU, memory, tool usage, and conversation depth over time.
Produces the `last_trend()` dict consumed by `AdaptiveController` to
choose between eco-modes (normal / low_memory / low_energy).

Original API preserved:
    monitor = SelfMonitor()
    monitor.snapshot(tool_calls=3, depth=2)   # returns snapshot dict
    trend = monitor.last_trend()              # {"cpu_delta": ..., "memory_delta": ...}

Enhanced API:
    monitor = SelfMonitor(run_id="...", task_id="...", agent_id="...",
                          max_snapshots=1000)
    snap  = monitor.snapshot(tool_calls=3, depth=2)  # same shape
    trend = monitor.last_trend()                     # superset
    trend.rate_cpu_per_s                              # rate, not just delta
    trend.classification                              # "stable"/"rising"/...
    trend.needs_review                                # extreme delta flag
    monitor.get_statistics()
    monitor.to_decision_record()
    monitor.attach_provenance(metrics)

Enhancements:
  1. Quantum-Distillation      — precision context on snapshots
  2. Causal RL                 — provenance supports causal attribution
  3. Federated Analytics       — deployment_id on every snapshot
  4. Multi-Agent Coordination  — agent_id on every snapshot
  5. Temporal Logic            — snapshot ordering + rate verification
  6. Explainable AI            — trend classification rationale
  7. Adaptive Precision        — precision field on snapshots
  8. Carbon Markets            — (reserved; fed by EnergyMeter bridge)
  9. Resilience & Chaos        — psutil failure handling
 10. Human-in-the-Loop         — extreme-delta review flag
 +   First-call CPU warm-up (fixes the psutil gotcha)
 +   Cached Process object
 +   Bounded snapshots
 +   Full trend (cpu, memory, tool_calls, depth, elapsed, rates)
 +   Trend classification
 +   Statistics, DecisionRecord, provenance bridge
"""

from __future__ import annotations

import logging
import math
import threading
import time
import uuid
from collections import Counter, deque
from dataclasses import dataclass, field, asdict
from datetime import datetime
from enum import Enum
from typing import (
    Any, Callable, Deque, Dict, List, Optional, Tuple,
)

try:
    import psutil  # type: ignore
    _HAS_PSUTIL = True
except Exception:  # pragma: no cover
    psutil = None  # type: ignore
    _HAS_PSUTIL = False
    logger = logging.getLogger(__name__)
    logger.warning("psutil unavailable; SelfMonitor will report fallback values")

logger = logging.getLogger(__name__)


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
# Statistics
# =============================================================================

_STATS: Counter = Counter()


def get_statistics() -> Dict[str, Any]:
    return {
        "monitors_created": _STATS["monitors"],
        "snapshots_taken": _STATS["snapshots"],
        "psutil_failures": _STATS["psutil_failures"],
        "trends_computed": _STATS["trends"],
        "invalid_inputs": _STATS["invalid"],
    }


def reset_statistics() -> None:
    _STATS.clear()


# =============================================================================
# TrendResult — structured trend output
# =============================================================================

@dataclass
class TrendResult:
    """
    Structured trend between two snapshots.

    `to_dict()` returns a dict containing the original two keys
    (`cpu_delta`, `memory_delta`) plus the extended fields. Callers that
    only read the original keys are unaffected.
    """
    # --- Original keys (preserved) ---
    cpu_delta: float = 0.0
    memory_delta: float = 0.0

    # --- Extended deltas ---
    tool_calls_delta: int = 0
    depth_delta: int = 0
    elapsed_delta_s: float = 0.0

    # --- Rates (per second) ---
    cpu_rate: float = 0.0            # %/s
    memory_rate: float = 0.0         # MB/s
    tool_calls_rate: float = 0.0     # calls/s
    depth_rate: float = 0.0          # depth/s

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

    # --- XAI ---
    explanation: Optional[Dict[str, Any]] = None

    # --- Metadata ---
    at: datetime = field(default_factory=datetime.now)

    def to_dict(self) -> Dict[str, Any]:
        """Original keys first, extended keys after."""
        out = {
            "cpu_delta": self.cpu_delta,
            "memory_delta": self.memory_delta,
        }
        out.update({
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
            "explanation": self.explanation,
            "at": self.at.isoformat(),
        })
        return out


# =============================================================================
# Snapshot record
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
    at: datetime = field(default_factory=datetime.now)

    def to_dict(self) -> Dict[str, Any]:
        """Original 5 keys first, plus extensions."""
        out = {
            "elapsed": self.elapsed,
            "cpu_percent": self.cpu_percent,
            "memory_mb": self.memory_mb,
            "tool_calls": self.tool_calls,
            "conversation_depth": self.conversation_depth,
        }
        out.update({
            "index": self.index,
            "cpu_source": self.cpu_source,
            "memory_source": self.memory_source,
            "run_id": self.run_id,
            "task_id": self.task_id,
            "agent_id": self.agent_id,
            "deployment_id": self.deployment_id,
            "precision": self.precision,
            "at": self.at.isoformat(),
        })
        return out


# =============================================================================
# XAI
# =============================================================================

class TrendExplainer:
    @staticmethod
    def explain(trend: TrendResult) -> Dict[str, Any]:
        reasons: List[str] = []
        reasons.append(
            f"Δcpu={trend.cpu_delta:+.2f}%, "
            f"Δmemory={trend.memory_delta:+.2f} MB, "
            f"Δt={trend.elapsed_delta_s:.3f}s."
        )
        if trend.elapsed_delta_s > 0:
            reasons.append(
                f"Rates: cpu={trend.cpu_rate:+.2f}%/s, "
                f"memory={trend.memory_rate:+.2f} MB/s."
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
    """
    Classify a trend into one of the TrendKind values.

    A "spike" is any delta larger than the configured thresholds.
    Otherwise:
      - both non-negative and at least one positive  → rising
      - both non-positive and at least one negative → falling
      - mixed signs                                  → stable
      - both zero                                    → stable
    """
    # --- Spike detection ---
    if cpu_delta >= cpu_spike_pct or memory_delta >= memory_spike_mb:
        return TrendKind.SPIKE.value
    # --- Recovery detection (large negative) ---
    if cpu_delta <= -cpu_spike_pct or memory_delta <= -memory_spike_mb:
        return TrendKind.RECOVERING.value
    # --- Direction ---
    if cpu_delta >= 0 and memory_delta >= 0 and (cpu_delta > 0 or memory_delta > 0):
        return TrendKind.RISING.value
    if cpu_delta <= 0 and memory_delta <= 0 and (cpu_delta < 0 or memory_delta < 0):
        return TrendKind.FALLING.value
    return TrendKind.STABLE.value


# =============================================================================
# Validation
# =============================================================================

def _validate_int(value: Any, name: str) -> Tuple[bool, int, Optional[str]]:
    """Return (ok, coerced_value, error_message)."""
    if isinstance(value, bool):
        return False, 0, f"{name} must be int, got bool"
    try:
        f = float(value)
    except (TypeError, ValueError):
        return False, 0, f"{name} must be numeric, got {type(value).__name__}"
    if not math.isfinite(f):
        return False, 0, f"{name} must be finite, got {value}"
    if f < 0:
        return False, 0, f"{name} must be non-negative, got {value}"
    return True, int(f), None


# =============================================================================
# The Enhanced SelfMonitor
# =============================================================================

class SelfMonitor:
    """
    Enhanced self-monitor with first-call warm-up, structured trend
    output, provenance, XAI, and full integration with the analysis
    layer's shared contracts.

    Backward-compatible: `SelfMonitor()`, `snapshot(tool_calls, depth)`,
    and `last_trend()` preserve their original signatures and return
    shapes.
    """

    DEFAULT_MAX_SNAPSHOTS = 1000
    HITL_CPU_SPIKE = 50.0        # percentage points between two snapshots
    HITL_MEMORY_SPIKE = 100.0    # MB between two snapshots

    def __init__(
        self,
        *,
        run_id: Optional[str] = None,
        task_id: Optional[str] = None,
        agent_id: Optional[str] = None,
        deployment_id: str = "local",
        precision: Optional[str] = None,
        max_snapshots: int = DEFAULT_MAX_SNAPSHOTS,
        features: Optional[Dict[str, bool]] = None,
    ):
        # --- Original state (preserved names) ---
        self.start_time = time.time()
        # Bounded deque — behaves like a list for typical use but caps memory
        self.snapshots: Deque[Snapshot] = deque(maxlen=max_snapshots)

        # --- Enhancement config ---
        self.run_id = run_id or f"sm-{uuid.uuid4().hex[:8]}"
        self.task_id = task_id
        self.agent_id = agent_id
        self.deployment_id = deployment_id
        self.precision = precision
        self._max_snapshots = max_snapshots

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

        # --- Cached Process object (psutil recommendation) ---
        self._process: Any = None
        if _HAS_PSUTIL and self.features.get("cache_process", True):
            try:
                self._process = psutil.Process()
            except Exception as e:
                logger.warning(f"Could not cache psutil.Process: {e}")
                self._process = None

        # --- Warm up cpu_percent so the first real snapshot is valid ---
        if (
            self._process is not None
            and self.features.get("warm_up_cpu", True)
        ):
            try:
                self._process.cpu_percent(interval=None)
            except Exception as e:
                logger.debug(f"cpu_percent warm-up failed: {e}")

        # --- Thread safety ---
        self._lock = threading.RLock()

        # --- Statistics ---
        self._snapshot_count: int = 0
        self._psutil_failures: int = 0
        self._invalid_inputs: int = 0
        self._trend_count: int = 0
        self._history: Deque[TrendResult] = deque(maxlen=256)

        # --- HITL ---
        self._hitl_callback: Optional[
            Callable[[TrendResult], bool]
        ] = None

        _STATS["monitors"] += 1

        logger.debug(
            f"SelfMonitor initialized (run_id={self.run_id}, "
            f"max_snapshots={max_snapshots}, psutil={_HAS_PSUTIL})"
        )

    # ------------------------------------------------------------------
    # ORIGINAL public API — preserved exactly
    # ------------------------------------------------------------------

    def snapshot(self, tool_calls: int = 0, depth: int = 0) -> Dict[str, Any]:
        """
        Record a snapshot of the current process state.

        Backward-compatible: same signature, same return shape
        (`elapsed`, `cpu_percent`, `memory_mb`, `tool_calls`,
        `conversation_depth`), returned as a plain dict.

        Enhanced: additional keys are added (index, sources, provenance,
        timestamp), and psutil failures fall back gracefully instead of
        crashing.
        """
        # --- Validation ---
        ok_tc, tc_int, err_tc = _validate_int(tool_calls, "tool_calls")
        ok_dp, dp_int, err_dp = _validate_int(depth, "depth")
        if self.features.get("validation", True):
            if not ok_tc:
                self._invalid_inputs += 1
                _STATS["invalid"] += 1
                logger.warning(f"snapshot: {err_tc}; using 0")
                tc_int = 0
            if not ok_dp:
                self._invalid_inputs += 1
                _STATS["invalid"] += 1
                logger.warning(f"snapshot: {err_dp}; using 0")
                dp_int = 0

        with self._lock:
            self._snapshot_count += 1
            index = self._snapshot_count

        # --- Read CPU ---
        cpu_percent, cpu_source = self._read_cpu()
        # --- Read memory ---
        memory_mb, memory_source = self._read_memory()

        elapsed = time.time() - self.start_time

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

        _STATS["snapshots"] += 1

        # --- Return the same five original keys first ---
        return snap.to_dict()

    def last_trend(self) -> Dict[str, Any]:
        """
        Compute the trend between the last two snapshots.

        Backward-compatible: returns a dict. When fewer than two
        snapshots exist, returns `{}` (original behavior).

        Enhanced: the returned dict contains `cpu_delta` and
        `memory_delta` first, plus extended fields (tool_calls_delta,
        depth_delta, elapsed_delta_s, rates, classification, XAI,
        provenance).
        """
        with self._lock:
            if len(self.snapshots) < 2:
                return {}
            prev = self.snapshots[-2]
            curr = self.snapshots[-1]

        self._trend_count += 1
        _STATS["trends"] += 1

        elapsed_delta = max(0.0, curr.elapsed - prev.elapsed)
        cpu_delta = curr.cpu_percent - prev.cpu_percent
        memory_delta = curr.memory_mb - prev.memory_mb
        tool_calls_delta = curr.tool_calls - prev.tool_calls
        depth_delta = curr.conversation_depth - prev.conversation_depth

        # --- Rates (per second); guard against zero elapsed ---
        if elapsed_delta > 0:
            cpu_rate = cpu_delta / elapsed_delta
            memory_rate = memory_delta / elapsed_delta
            tool_calls_rate = tool_calls_delta / elapsed_delta
            depth_rate = depth_delta / elapsed_delta
        else:
            cpu_rate = memory_rate = tool_calls_rate = depth_rate = 0.0

        # --- Classification ---
        if self.features.get("classify_trends", True):
            classification = _classify_trend(
                cpu_delta, memory_delta, elapsed_delta,
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

        # --- HITL review for extreme deltas ---
        needs_review = False
        review_reason: Optional[str] = None
        if self.features.get("hitl", True):
            if cpu_delta >= self.HITL_CPU_SPIKE:
                needs_review = True
                review_reason = (
                    f"cpu_delta {cpu_delta:.1f}% >= "
                    f"{self.HITL_CPU_SPIKE}%"
                )
            elif memory_delta >= self.HITL_MEMORY_SPIKE:
                needs_review = True
                review_reason = (
                    f"memory_delta {memory_delta:.1f} MB >= "
                    f"{self.HITL_MEMORY_SPIKE} MB"
                )
            elif simulated:
                needs_review = True
                review_reason = "one or both snapshots contain fallback values"

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

        # --- XAI ---
        if self.features.get("xai", True):
            trend.explanation = TrendExplainer.explain(trend)

        # --- HITL callback ---
        if needs_review and self._hitl_callback is not None:
            try:
                self._hitl_callback(trend)
            except Exception as e:
                logger.warning(f"HITL callback failed: {e}")

        # --- History ---
        with self._lock:
            self._history.append(trend)

        return trend.to_dict()

    # ------------------------------------------------------------------
    # ENHANCED public API
    # ------------------------------------------------------------------

    def last_trend_detailed(self) -> Optional[TrendResult]:
        """Return the last trend as a TrendResult (or None if <2 snapshots)."""
        if len(self.snapshots) < 2:
            return None
        self.last_trend()  # ensures the trend is computed and appended
        with self._lock:
            return self._history[-1] if self._history else None

    def get_statistics(self) -> Dict[str, Any]:
        """Return cumulative monitor statistics."""
        stats = get_statistics()
        stats.update({
            "run_id": self.run_id,
            "deployment_id": self.deployment_id,
            "agent_id": self.agent_id,
            "task_id": self.task_id,
            "snapshots_stored": len(self.snapshots),
            "snapshots_taken": self._snapshot_count,
            "max_snapshots": self._max_snapshots,
            "trends_computed": self._trend_count,
            "psutil_failures": self._psutil_failures,
            "invalid_inputs": self._invalid_inputs,
            "features": dict(self.features),
            "psutil_available": _HAS_PSUTIL,
        })
        # --- Aggregate snapshot statistics ---
        if self.snapshots:
            cpus = [s.cpu_percent for s in self.snapshots]
            mems = [s.memory_mb for s in self.snapshots]
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
        # --- Trend classification distribution ---
        if self._history:
            counts: Counter = Counter()
            for t in self._history:
                counts[t.classification] += 1
            stats["trend_distribution"] = dict(counts)
        return stats

    def export(self) -> Dict[str, Any]:
        """Full serialisable export."""
        with self._lock:
            return {
                "statistics": self.get_statistics(),
                "snapshots": [s.to_dict() for s in self.snapshots],
                "trends": [t.to_dict() for t in self._history],
            }

    def reset(self) -> None:
        """Clear all snapshots and trends (keeps config and cached process)."""
        with self._lock:
            self.snapshots.clear()
            self._history.clear()
            self.start_time = time.time()
            self._snapshot_count = 0
            self._trend_count = 0
        # Re-warm the CPU baseline after reset
        if (
            self._process is not None
            and self.features.get("warm_up_cpu", True)
        ):
            try:
                self._process.cpu_percent(interval=None)
            except Exception:
                pass
        logger.debug(f"SelfMonitor reset (run_id={self.run_id})")

    def set_hitl_callback(
        self, callback: Callable[[TrendResult], bool],
    ) -> None:
        """Register a HITL callback for extreme trends."""
        self._hitl_callback = callback

    def to_decision_record(self) -> Optional[Any]:
        """Emit a DecisionRecord for the most recent trend."""
        trend = self.last_trend_detailed()
        if trend is None:
            return None
        try:
            from src.analysis import DecisionRecord  # type: ignore
        except Exception:
            try:
                from analysis import DecisionRecord  # type: ignore
            except Exception:
                return None

        return DecisionRecord(
            run_id=trend.run_id or self.run_id,
            timestamp=trend.at,
            task_id=trend.task_id or "",
            selected_action=f"self_monitor_trend({trend.classification})",
            model_or_agent=trend.agent_id or "",
            latency_ms=trend.elapsed_delta_s * 1000.0,
            explanation=trend.explanation or {},
            provenance={
                "source": "self_monitor",
                "cpu_delta": trend.cpu_delta,
                "memory_delta": trend.memory_delta,
                "simulated": trend.simulated,
            },
        )

    def attach_provenance(
        self, metrics: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        Bridge to `metric_provenance.attach_provenance`.

        Uses the latest snapshot's dict if `metrics` is None.
        """
        if metrics is None:
            with self._lock:
                if not self.snapshots:
                    return {}
                metrics = self.snapshots[-1].to_dict()
        try:
            from src.analysis.telemetry.metric_provenance import (
                attach_provenance as _attach,
            )
            return _attach(metrics)
        except Exception:
            try:
                from analysis.metric_provenance import (  # type: ignore
                    attach_provenance as _attach,
                )
                return _attach(metrics)
            except Exception as e:
                logger.debug(f"Could not bridge to metric_provenance: {e}")
                return metrics

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _read_cpu(self) -> Tuple[float, str]:
        """
        Read CPU% since last call.

        Returns (value, source). On psutil failure, returns (0.0,
        "fallback"). Guards against non-finite values.
        """
        if not _HAS_PSUTIL:
            return 0.0, MetricSource.FALLBACK.value

        process = self._process
        if process is None:
            # Fall back to creating one on the fly if caching was disabled
            try:
                process = psutil.Process()
                self._process = process
            except Exception as e:
                self._psutil_failures += 1
                _STATS["psutil_failures"] += 1
                logger.debug(f"psutil.Process() failed: {e}")
                return 0.0, MetricSource.FALLBACK.value

        try:
            value = float(process.cpu_percent(interval=None))
            if not math.isfinite(value) or value < 0:
                return 0.0, MetricSource.FALLBACK.value
            return value, MetricSource.MEASURED.value
        except Exception as e:
            self._psutil_failures += 1
            _STATS["psutil_failures"] += 1
            logger.debug(f"cpu_percent failed: {e}")
            # Recreate the process object in case it was invalidated
            try:
                self._process = psutil.Process()
            except Exception:
                self._process = None
            return 0.0, MetricSource.FALLBACK.value

    def _read_memory(self) -> Tuple[float, str]:
        """Read RSS in MB. Returns (value, source)."""
        if not _HAS_PSUTIL:
            return 0.0, MetricSource.FALLBACK.value

        process = self._process
        if process is None:
            try:
                process = psutil.Process()
                self._process = process
            except Exception as e:
                self._psutil_failures += 1
                _STATS["psutil_failures"] += 1
                logger.debug(f"psutil.Process() failed: {e}")
                return 0.0, MetricSource.FALLBACK.value

        try:
            info = process.memory_info()
            value = float(info.rss) / (1024 ** 2)
            if not math.isfinite(value) or value < 0:
                return 0.0, MetricSource.FALLBACK.value
            return value, MetricSource.MEASURED.value
        except Exception as e:
            self._psutil_failures += 1
            _STATS["psutil_failures"] += 1
            logger.debug(f"memory_info failed: {e}")
            try:
                self._process = psutil.Process()
            except Exception:
                self._process = None
            return 0.0, MetricSource.FALLBACK.value


# =============================================================================
# Demo
# =============================================================================

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    # --- Original behavior (backward compatible) ---
    print("=== Original behavior ===")
    monitor = SelfMonitor()
    s1 = monitor.snapshot(tool_calls=1, depth=1)
    print(f"  snapshot 1: cpu={s1['cpu_percent']}%, "
          f"mem={s1['memory_mb']:.1f} MB, elapsed={s1['elapsed']:.4f}s")
    time.sleep(0.05)
    s2 = monitor.snapshot(tool_calls=2, depth=1)
    print(f"  snapshot 2: cpu={s2['cpu_percent']}%, "
          f"mem={s2['memory_mb']:.1f} MB")

    trend = monitor.last_trend()
    print(f"\n  last_trend() (backward-compatible keys):")
    print(f"    cpu_delta:    {trend['cpu_delta']:+.2f}")
    print(f"    memory_delta: {trend['memory_delta']:+.2f}")

    # --- Enhanced: full trend with classification and rates ---
    print("\n=== Enhanced trend ===")
    print(f"  elapsed_delta_s:  {trend['elapsed_delta_s']:.4f}")
    print(f"  cpu_rate:         {trend['cpu_rate']:+.2f} %/s")
    print(f"  memory_rate:      {trend['memory_rate']:+.2f} MB/s")
    print(f"  tool_calls_delta: {trend['tool_calls_delta']:+d}")
    print(f"  depth_delta:      {trend['depth_delta']:+d}")
    print(f"  classification:   {trend['classification']}")
    print(f"  simulated:        {trend['simulated']}")
    print(f"  needs_review:     {trend['needs_review']}")

    if trend.get("explanation"):
        exp = trend["explanation"]
        print(f"\n  XAI: {exp['headline']}")
        for r in exp["rationale"]:
            print(f"    • {r}")

    # --- Spike detection ---
    print("\n=== Spike detection ===")
    monitor2 = SelfMonitor()
    monitor2.snapshot(tool_calls=0, depth=0)
    # Simulate a big memory jump by taking another snapshot
    # (in a real test we would mock psutil)
    time.sleep(0.02)
    monitor2.snapshot(tool_calls=0, depth=0)
    t2 = monitor2.last_trend()
    print(f"  classification: {t2['classification']}")

    # --- Malformed inputs ---
    print("\n=== Malformed inputs (original accepted silently) ===")
    monitor3 = SelfMonitor()
    monitor3.snapshot(tool_calls="five", depth=-3)  # both invalid, coerced to 0
    monitor3.snapshot(tool_calls=float("nan"), depth=2)  # NaN coerced to 0
    print(f"  stored: {len(monitor3.snapshots)}")

    # --- Bounded snapshots ---
    print("\n=== Bounded snapshots ===")
    monitor4 = SelfMonitor(max_snapshots=5)
    for i in range(20):
        monitor4.snapshot(tool_calls=i, depth=i)
    print(f"  took 20 snapshots, stored: {len(monitor4.snapshots)} "
          f"(maxlen=5)")

    # --- Reset ---
    print("\n=== Reset ===")
    monitor.reset()
    print(f"  after reset: {len(monitor.snapshots)} snapshots, "
          f"trend: {monitor.last_trend()}")

    # --- Statistics ---
    import json
    print("\n=== Statistics ===")
    print(json.dumps(monitor2.get_statistics(), indent=2, default=str))

    # --- AdaptiveController integration ---
    print("\n=== AdaptiveController integration ===")
    # The controller reads trend["memory_delta"] and trend["cpu_delta"]
    monitor5 = SelfMonitor()
    monitor5.snapshot()
    time.sleep(0.05)
    monitor5.snapshot()
    trend5 = monitor5.last_trend()
    print(f"  cpu_delta:    {trend5['cpu_delta']:.2f} "
          f"(controller threshold: 15)")
    print(f"  memory_delta: {trend5['memory_delta']:.2f} "
          f"(controller threshold: 25)")
    print("  (These are the exact keys AdaptiveController.evaluate() reads.)")
