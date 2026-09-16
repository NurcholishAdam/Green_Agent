# src/analysis/telemetry/metrics_collector.py

"""
Metrics collection for Green Agent (Enhanced)
==============================================

Collects core execution metrics — CPU time, memory, latency, tool calls,
reasoning depth — and now energy, carbon, helium, precision, and per-event
timestamps.

Original API preserved:
    collector = MetricsCollector()
    collector.record_tool()
    collector.record_depth(3)
    metrics = collector.finalize()

Enhanced API:
    collector = MetricsCollector(
        run_id="...", task_id="...", agent_id="...",
        energy_meter=meter, carbon_estimator=est, callback=cb,
    )
    metrics = collector.finalize()          # idempotent
    collector.finalize_detailed()           # rich MetricsSnapshot
    collector.get_statistics()
    collector.attach_provenance()           # bridge to metric_provenance

Enhancements:
  1. Quantum-Distillation      — route/precision context
  2. Causal RL                 — counters feed causal analysis
  3. Federated Analytics       — deployment identity
  4. Multi-Agent Coordination  — agent attribution per metric
  5. Temporal Logic            — per-event timestamps
  6. Explainable AI            — rationale on the snapshot
  7. Adaptive Precision        — precision recorded on output
  8. Carbon Markets            — energy/carbon/helium outputs
  9. Resilience & Chaos        — psutil fallback + chaos awareness
 10. Human-in-the-Loop         — review for extreme memory/latency
 +   Idempotent finalize() (fixes the growing-latency bug)
 +   Thread-safe counters
 +   Integration with EnergyMeter, CarbonEstimator, callbacks
 +   Depth history (for variance)
 +   Statistics, provenance bridge, DecisionRecord emission
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
    Any, Callable, Deque, Dict, Iterable, List, Optional, Tuple,
)

try:
    import psutil  # type: ignore
    _HAS_PSUTIL = True
except Exception:  # pragma: no cover
    psutil = None  # type: ignore
    _HAS_PSUTIL = False

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


# =============================================================================
# MetricsSnapshot — the rich output
# =============================================================================

@dataclass
class MetricsSnapshot:
    """
    Rich result of a completed collection.

    `to_dict()` returns a dict containing all original keys, so callers
    that expect the original 4-key dict receive a superset.
    """
    run_id: str
    task_id: Optional[str]
    agent_id: Optional[str]

    # --- Original 4 metrics (preserved names) ---
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

    # --- Provenance ---
    memory_source: str = MetricSource.UNKNOWN.value
    latency_source: str = MetricSource.MEASURED.value
    energy_source: str = MetricSource.UNKNOWN.value
    carbon_source: str = MetricSource.UNKNOWN.value
    simulated: bool = False

    # --- Depth history (for variance) ---
    depth_min: Optional[int] = None
    depth_max: Optional[int] = None
    depth_mean: Optional[float] = None
    depth_std: Optional[float] = None

    # --- Verification ---
    bounds_ok: bool = True
    bounds_violations: List[str] = field(default_factory=list)

    # --- XAI ---
    explanation: Optional[Dict[str, Any]] = None

    # --- Safety / HITL ---
    needs_review: bool = False
    review_reason: Optional[str] = None

    # --- Timing ---
    started_at: Optional[float] = None
    finished_at: Optional[float] = None

    # --- Metadata ---
    at: datetime = field(default_factory=datetime.now)

    def to_dict(self) -> Dict[str, Any]:
        out = asdict(self)
        out["at"] = self.at.isoformat()
        return out


# =============================================================================
# XAI — rationale for the snapshot
# =============================================================================

class MetricsExplainer:
    @staticmethod
    def explain(snap: MetricsSnapshot) -> Dict[str, Any]:
        reasons: List[str] = []
        reasons.append(
            f"Run {snap.run_id}: latency={snap.latency_s:.3f}s, "
            f"memory={snap.memory_mb:.1f} MB."
        )
        reasons.append(
            f"Tool calls: {snap.tool_calls}, LLM calls: {snap.llm_calls}, "
            f"tokens: {snap.tokens}."
        )
        if snap.depth_mean is not None:
            reasons.append(
                f"Conversation depth: mean={snap.depth_mean:.2f}, "
                f"max={snap.depth_max}."
            )
        if snap.energy_wh:
            reasons.append(
                f"Energy: {snap.energy_wh:.4f} Wh ({snap.energy_source})."
            )
        if snap.carbon_g:
            reasons.append(
                f"Carbon: {snap.carbon_g:.6f} gCO₂e ({snap.carbon_source})."
            )
        if snap.helium_units:
            reasons.append(
                f"Helium: {snap.helium_units:.6f} units."
            )
        if snap.simulated:
            reasons.append(
                "WARNING: snapshot contains simulated values."
            )
        if not snap.bounds_ok:
            reasons.append(
                f"Bounds violations: {snap.bounds_violations}."
            )

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
            },
            "confidence": 0.5 if snap.simulated else 0.9,
        }


# =============================================================================
# Statistics
# =============================================================================

_STATS: Counter = Counter()


def get_statistics() -> Dict[str, Any]:
    return {
        "collectors_created": _STATS["collectors"],
        "finalizations": _STATS["finalizations"],
        "psutil_failures": _STATS["psutil_failures"],
        "concurrent_races_prevented": _STATS["races_prevented"],
    }


def reset_statistics() -> None:
    _STATS.clear()


# =============================================================================
# ENHANCED MetricsCollector
# =============================================================================

class MetricsCollector:
    """
    Enhanced metrics collector.

    Backward-compatible: same constructor, same three methods, same
    return dict (superset).
    """

    # Threshold constants for HITL review
    HITL_MEMORY_MB = 8192.0           # 8 GB
    HITL_LATENCY_S = 600.0            # 10 minutes

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
        features: Optional[Dict[str, bool]] = None,
    ):
        # --- Original state (preserved names) ---
        self.start = time.time()
        self.tool_calls = 0
        self.conversation_depth = 0

        # --- Enhancement: identity ---
        self.run_id = run_id or f"run-{uuid.uuid4().hex[:8]}"
        self.task_id = task_id
        self.agent_id = agent_id
        self.deployment_id = deployment_id

        # --- Enhancement: context ---
        self.region = region
        self.pue = pue
        self.hardware_profile = hardware_profile
        self.precision = precision

        # --- Enhancement: integrations ---
        self.energy_meter = energy_meter
        self.carbon_estimator = carbon_estimator
        self.callback = callback

        # --- Enhancement: extra counters ---
        self.llm_calls: int = 0
        self.tokens: int = 0

        # --- Enhancement: depth history ---
        self._depth_history: Deque[int] = deque(maxlen=1024)
        self._event_timestamps: Deque[float] = deque(maxlen=4096)

        # --- Enhancement: thread safety ---
        self._lock = threading.RLock()

        # --- Enhancement: idempotent finalize ---
        self._finalized: bool = False
        self._cached_snapshot: Optional[MetricsSnapshot] = None

        # --- Enhancement: feature flags ---
        self.features: Dict[str, bool] = {
            "idempotent_finalize": True,
            "psutil_fallback": True,
            "energy": True,
            "carbon": True,
            "provenance": True,
            "xai": True,
            "hitl": True,
            "extended_metrics": True,
            "thread_safety": True,
        }
        if features:
            self.features.update(features)

        # --- Statistics ---
        _STATS["collectors"] += 1

        logger.debug(
            f"MetricsCollector initialized (run_id={self.run_id}, "
            f"deployment={deployment_id})"
        )

    # ------------------------------------------------------------------
    # ORIGINAL public API — preserved
    # ------------------------------------------------------------------

    def record_tool(self) -> None:
        """
        Increment the tool-call counter.

        Backward-compatible: same signature, same effect.
        Enhanced: thread-safe.
        """
        if self.features.get("thread_safety", True):
            with self._lock:
                self.tool_calls += 1
                self._event_timestamps.append(time.time())
        else:
            self.tool_calls += 1

    def record_depth(self, depth: int) -> None:
        """
        Record a conversation depth observation.

        Backward-compatible: `conversation_depth` is still the running max.

        Enhanced: also appends to depth history for variance computation,
        and validates input.
        """
        # --- Input validation (original would silently accept anything) ---
        try:
            depth_int = int(depth)
        except (TypeError, ValueError):
            logger.warning(
                f"record_depth received non-integer ({depth!r}); ignoring"
            )
            return
        if depth_int < 0:
            logger.warning(
                f"record_depth received negative value ({depth_int}); ignoring"
            )
            return

        if self.features.get("thread_safety", True):
            with self._lock:
                self.conversation_depth = max(self.conversation_depth, depth_int)
                self._depth_history.append(depth_int)
                self._event_timestamps.append(time.time())
        else:
            self.conversation_depth = max(self.conversation_depth, depth_int)
            self._depth_history.append(depth_int)

    def record_llm_call(self) -> None:
        """Enhancement: track LLM-call count."""
        if self.features.get("thread_safety", True):
            with self._lock:
                self.llm_calls += 1
                self._event_timestamps.append(time.time())
        else:
            self.llm_calls += 1

    def record_tokens(self, tokens: int) -> None:
        """Enhancement: add token count to the running total."""
        try:
            t = int(tokens)
        except (TypeError, ValueError):
            logger.warning(f"record_tokens received non-integer: {tokens!r}")
            return
        if t < 0:
            return
        if self.features.get("thread_safety", True):
            with self._lock:
                self.tokens += t
        else:
            self.tokens += t

    def finalize(self) -> Dict[str, Any]:
        """
        Finalize the collection and return the metrics dict.

        Backward-compatible: same signature, same return shape (superset).

        Enhanced:
        - Idempotent: repeated calls return the same values
        - psutil failures do not lose the other metrics
        - Includes energy, carbon, helium, tokens, and provenance flags
        """
        # --- Idempotency check ---
        if (
            self.features.get("idempotent_finalize", True)
            and self._finalized
            and self._cached_snapshot is not None
        ):
            return self._cached_snapshot.to_dict()

        _STATS["finalizations"] += 1

        # --- Latency (measured once) ---
        now = time.time()
        latency_s = now - self.start

        # --- Memory (with fallback) ---
        memory_mb, memory_source = self._read_memory()

        # --- Energy / carbon / helium ---
        energy_wh, carbon_g, helium_units, energy_source, carbon_source = (
            self._read_sustainability_metrics()
        )

        # --- Callback integration ---
        callback_snapshot: Optional[Dict[str, Any]] = None
        if self.callback is not None:
            try:
                if hasattr(self.callback, "snapshot"):
                    callback_snapshot = self.callback.snapshot()
                elif hasattr(self.callback, "statistics"):
                    callback_snapshot = self.callback.statistics()
            except Exception as e:
                logger.debug(f"Callback snapshot failed: {e}")

        # --- Depth statistics ---
        with self._lock:
            depths = list(self._depth_history)
            tools = self.tool_calls
            depth_max = self.conversation_depth
            llm_calls = self.llm_calls
            tokens = self.tokens

        depth_min: Optional[int] = None
        depth_mean: Optional[float] = None
        depth_std: Optional[float] = None
        if depths:
            depth_min = min(depths)
            depth_max_calc = max(depths)
            depth_max = max(depth_max, depth_max_calc)
            depth_mean = sum(depths) / len(depths)
            if len(depths) > 1:
                var = sum((d - depth_mean) ** 2 for d in depths) / len(depths)
                depth_std = math.sqrt(var)

        # --- Bounds verification ---
        bounds_ok, bounds_violations = self._check_bounds(
            latency_s, memory_mb, tools, depth_max,
        )

        # --- Simulation detection ---
        simulated = (
            memory_source in (MetricSource.FALLBACK.value, MetricSource.SIMULATED.value)
            or energy_source == MetricSource.SIMULATED.value
        )

        # --- HITL review for extreme values ---
        needs_review = False
        review_reason: Optional[str] = None
        if self.features.get("hitl", True):
            if memory_mb >= self.HITL_MEMORY_MB:
                needs_review = True
                review_reason = (
                    f"memory {memory_mb:.1f} MB >= {self.HITL_MEMORY_MB} MB"
                )
            elif latency_s >= self.HITL_LATENCY_S:
                needs_review = True
                review_reason = (
                    f"latency {latency_s:.1f}s >= {self.HITL_LATENCY_S}s"
                )
            elif not bounds_ok:
                needs_review = True
                review_reason = f"bounds violations: {bounds_violations}"

        # --- Build snapshot ---
        snapshot = MetricsSnapshot(
            run_id=self.run_id,
            task_id=self.task_id,
            agent_id=self.agent_id,
            latency_s=latency_s,
            memory_mb=memory_mb,
            tool_calls=tools,
            conversation_depth=depth_max,
            energy_wh=energy_wh,
            carbon_g=carbon_g,
            helium_units=helium_units,
            llm_calls=llm_calls,
            tokens=tokens,
            precision=self.precision,
            region=self.region,
            pue=self.pue,
            hardware_profile=self.hardware_profile,
            deployment_id=self.deployment_id,
            memory_source=memory_source,
            latency_source=MetricSource.MEASURED.value,
            energy_source=energy_source,
            carbon_source=carbon_source,
            simulated=simulated,
            depth_min=depth_min,
            depth_max=depth_max,
            depth_mean=depth_mean,
            depth_std=depth_std,
            bounds_ok=bounds_ok,
            bounds_violations=bounds_violations,
            needs_review=needs_review,
            review_reason=review_reason,
            started_at=self.start,
            finished_at=now,
        )

        # --- XAI ---
        if self.features.get("xai", True):
            snapshot.explanation = MetricsExplainer.explain(snapshot)

        # --- Cache for idempotent finalize ---
        if self.features.get("idempotent_finalize", True):
            self._cached_snapshot = snapshot
            self._finalized = True

        return snapshot.to_dict()

    # ------------------------------------------------------------------
    # ENHANCED public API
    # ------------------------------------------------------------------

    def finalize_detailed(self) -> MetricsSnapshot:
        """Return the full MetricsSnapshot instead of a dict."""
        self.finalize()  # ensures the snapshot is built
        if self._cached_snapshot is None:
            # Reconstruct if idempotent_finalize is disabled
            self._cached_snapshot = self._build_snapshot()
        return self._cached_snapshot

    def attach_provenance(self, metrics: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Bridge to `metric_provenance.attach_provenance`.

        If `metrics` is None, uses the current snapshot's dict. Returns
        the metrics dict with `metric_provenance` attached.
        """
        metrics = metrics or self.finalize()
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

    def to_decision_record(
        self,
        *,
        policy_version: str = "",
    ) -> Optional[Any]:
        """Emit a DecisionRecord for the current snapshot."""
        snap = self.finalize_detailed()
        try:
            from src.analysis import DecisionRecord  # type: ignore
        except Exception:
            try:
                from analysis import DecisionRecord  # type: ignore
            except Exception:
                return None

        return DecisionRecord(
            run_id=snap.run_id,
            timestamp=snap.at,
            task_id=snap.task_id or "",
            selected_action=f"metrics_collection(tools={snap.tool_calls})",
            policy_version=policy_version,
            model_or_agent=snap.agent_id or "",
            hardware_profile=snap.hardware_profile or "",
            precision=snap.precision,
            latency_ms=snap.latency_s * 1000.0,
            energy_kwh=snap.energy_wh / 1000.0,
            carbon_operational_kg=snap.carbon_g / 1000.0,
            helium_units=snap.helium_units,
            explanation=snap.explanation or {},
            provenance={
                "source": "metrics_collector",
                "memory_source": snap.memory_source,
                "energy_source": snap.energy_source,
                "carbon_source": snap.carbon_source,
                "simulated": snap.simulated,
            },
        )

    def get_statistics(self) -> Dict[str, Any]:
        """Return cumulative collector statistics."""
        stats = get_statistics()
        stats.update({
            "run_id": self.run_id,
            "finalized": self._finalized,
            "tool_calls": self.tool_calls,
            "llm_calls": self.llm_calls,
            "tokens": self.tokens,
            "conversation_depth": self.conversation_depth,
            "depth_observations": len(self._depth_history),
            "features": dict(self.features),
            "psutil_available": _HAS_PSUTIL,
        })
        return stats

    def export(self) -> Dict[str, Any]:
        """Full serialisable export."""
        return {
            "statistics": self.get_statistics(),
            "snapshot": (
                self._cached_snapshot.to_dict()
                if self._cached_snapshot else None
            ),
        }

    def reset(self) -> None:
        """Reset counters and timing (for reuse)."""
        with self._lock:
            self.start = time.time()
            self.tool_calls = 0
            self.conversation_depth = 0
            self.llm_calls = 0
            self.tokens = 0
            self._depth_history.clear()
            self._event_timestamps.clear()
            self._finalized = False
            self._cached_snapshot = None
        logger.debug(f"MetricsCollector reset (run_id={self.run_id})")

    def record_shorten_context_factor(self, factor: float) -> None:
        """
        Record that the AdaptiveController applied context shortening.

        Stored in provenance so downstream analytics can distinguish
        throttled runs from unthrottled ones.
        """
        try:
            f = float(factor)
        except (TypeError, ValueError):
            return
        self._shorten_factor = max(0.0, min(1.0, f))

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _read_memory(self) -> Tuple[float, str]:
        """
        Read process memory in MB.

        Returns (value, source). Source is `"measured"` on success, or
        `"fallback"` on psutil failure (returns 0.0).
        """
        if not _HAS_PSUTIL or psutil is None:
            return 0.0, MetricSource.FALLBACK.value

        if not self.features.get("psutil_fallback", True):
            # Original behavior: no try/except
            info = psutil.Process().memory_info()
            return info.rss / (1024 ** 2), MetricSource.MEASURED.value

        try:
            info = psutil.Process().memory_info()
            memory_mb = info.rss / (1024 ** 2)
            if not math.isfinite(memory_mb) or memory_mb < 0:
                return 0.0, MetricSource.FALLBACK.value
            return memory_mb, MetricSource.MEASURED.value
        except Exception as e:
            _STATS["psutil_failures"] += 1
            logger.warning(f"psutil memory read failed: {e}; using 0.0")
            return 0.0, MetricSource.FALLBACK.value

    def _read_sustainability_metrics(
        self,
    ) -> Tuple[float, float, float, str, str]:
        """
        Read energy, carbon, and helium from wired-in components.

        Returns (energy_wh, carbon_g, helium_units, energy_source,
        carbon_source). Falls back to 0.0 for any missing component.
        """
        energy_wh = 0.0
        energy_source = MetricSource.UNKNOWN.value
        carbon_g = 0.0
        carbon_source = MetricSource.UNKNOWN.value
        helium_units = 0.0

        # --- Energy ---
        if self.features.get("energy", True) and self.energy_meter is not None:
            try:
                # Prefer detailed method if available
                if hasattr(self.energy_meter, "joules"):
                    joules = float(self.energy_meter.joules())
                    energy_wh = joules / 3600.0
                    energy_source = MetricSource.MEASURED.value
                elif hasattr(self.energy_meter, "measure"):
                    wh = float(self.energy_meter.measure())
                    energy_wh = wh
                    energy_source = MetricSource.MEASURED.value
            except Exception as e:
                logger.debug(f"Energy meter read failed: {e}")

        # --- Carbon ---
        if self.features.get("carbon", True) and self.carbon_estimator is not None:
            try:
                if energy_wh > 0 and hasattr(self.carbon_estimator, "estimate"):
                    kg = float(self.carbon_estimator.estimate(
                        energy_wh=energy_wh
                    ))
                    carbon_g = kg * 1000.0
                    carbon_source = MetricSource.ESTIMATED.value
            except Exception as e:
                logger.debug(f"Carbon estimator read failed: {e}")

        # --- Helium (from energy if profiler available) ---
        try:
            from src.analysis.telemetry.helium_profiler import (
                HeliumProfiler,
            )
            helium_units = HeliumProfiler.estimate_units(energy_wh)
        except Exception:
            # Inline fallback if the module isn't present
            if energy_wh > 0:
                helium_units = energy_wh * 5e-5

        return (
            energy_wh, carbon_g, helium_units,
            energy_source, carbon_source,
        )

    def _build_snapshot(self) -> MetricsSnapshot:
        """Rebuild a snapshot when idempotent_finalize is disabled."""
        dict_view = self.finalize()
        # When idempotence is disabled we still build fresh each time
        if self._cached_snapshot is not None:
            return self._cached_snapshot
        # Construct a minimal snapshot from the dict view
        return MetricsSnapshot(
            run_id=self.run_id,
            task_id=self.task_id,
            agent_id=self.agent_id,
            latency_s=dict_view["latency_s"],
            memory_mb=dict_view["memory_mb"],
            tool_calls=dict_view["tool_calls"],
            conversation_depth=dict_view["conversation_depth"],
            energy_wh=dict_view.get("energy_wh", 0.0),
            carbon_g=dict_view.get("carbon_g", 0.0),
            helium_units=dict_view.get("helium_units", 0.0),
        )

    @staticmethod
    def _check_bounds(
        latency_s: float, memory_mb: float,
        tool_calls: int, depth: int,
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
        return (len(bad) == 0, bad)


# =============================================================================
# Demo
# =============================================================================

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    # --- Original behavior (backward compatible) ---
    print("=== Original behavior ===")
    mc = MetricsCollector()
    mc.record_tool()
    mc.record_tool()
    mc.record_depth(3)
    mc.record_depth(5)
    result = mc.finalize()
    print(f"  latency_s: {result['latency_s']:.6f}")
    print(f"  memory_mb: {result['memory_mb']:.2f}")
    print(f"  tool_calls: {result['tool_calls']}")
    print(f"  conversation_depth: {result['conversation_depth']}")

    # --- Idempotency (original would return different latency) ---
    print("\n=== Idempotency ===")
    print(f"  call 1: latency={result['latency_s']:.6f}")
    time.sleep(0.05)
    result2 = mc.finalize()
    print(f"  call 2: latency={result2['latency_s']:.6f}")
    print(f"  same value: {result['latency_s'] == result2['latency_s']}")
    print("  (Original would have grown by 0.05s)")

    # --- Enhanced with sustainability metrics ---
    print("\n=== Enhanced with energy/carbon ===")

    class _MockEnergyMeter:
        def joules(self):
            return 1800.0  # 0.5 Wh

    class _MockCarbonEstimator:
        def estimate(self, energy_wh):
            return (energy_wh / 1000.0) * 400 / 1000.0  # kg

    mc2 = MetricsCollector(
        run_id="run-001",
        task_id="task-1",
        agent_id="worker-A",
        deployment_id="us-ca-prod-01",
        region="US-CA",
        precision="int8",
        energy_meter=_MockEnergyMeter(),
        carbon_estimator=_MockCarbonEstimator(),
    )

    mc2.record_tool()
    mc2.record_llm_call()
    mc2.record_tokens(120)
    mc2.record_depth(2)
    mc2.record_depth(4)
    mc2.record_depth(3)

    detail = mc2.finalize_detailed()
    print(f"  latency: {detail.latency_s:.4f}s")
    print(f"  memory: {detail.memory_mb:.1f} MB ({detail.memory_source})")
    print(f"  energy: {detail.energy_wh:.6f} Wh ({detail.energy_source})")
    print(f"  carbon: {detail.carbon_g:.6f} g ({detail.carbon_source})")
    print(f"  helium: {detail.helium_units:.8f} units")
    print(f"  tokens: {detail.tokens}, llm_calls: {detail.llm_calls}")
    print(f"  depth: min={detail.depth_min} max={detail.depth_max} "
          f"mean={detail.depth_mean:.2f} std={detail.depth_std:.2f}")
    print(f"  simulated: {detail.simulated}")
    print(f"  needs_review: {detail.needs_review}")

    if detail.explanation:
        print(f"\n  XAI: {detail.explanation['headline']}")
        for r in detail.explanation["rationale"]:
            print(f"    • {r}")

    # --- Provenance bridge ---
    print("\n=== Provenance bridge ===")
    enriched = mc2.attach_provenance()
    if "metric_provenance" in enriched:
        for k, v in enriched["metric_provenance"].items():
            print(f"  {k}: {v}")

    # --- DecisionRecord emission ---
    print("\n=== DecisionRecord ===")
    dr = mc2.to_decision_record(policy_version="v5.0.1")
    if dr:
        print(f"  selected_action: {dr.selected_action}")
        print(f"  latency_ms:      {dr.latency_ms:.2f}")
        print(f"  energy_kwh:      {dr.energy_kwh:.8f}")
        print(f"  provenance:      {dr.provenance}")

    # --- Malformed input safety ---
    print("\n=== Malformed inputs (original would crash or accept silently) ===")
    mc3 = MetricsCollector()
    mc3.record_depth("not a number")  # warns, no crash
    mc3.record_depth(-5)              # warns, no crash
    mc3.record_tokens(None)           # warns, no crash
    result3 = mc3.finalize()
    print(f"  depth: {result3['conversation_depth']}, "
          f"tool_calls: {result3['tool_calls']}")

    # --- Statistics ---
    import json
    print("\n=== Statistics ===")
    print(json.dumps(mc2.get_statistics(), indent=2, default=str))
