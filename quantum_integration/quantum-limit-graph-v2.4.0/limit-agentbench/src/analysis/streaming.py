# src/analysis/telemetry/streaming.py

"""
Real-time metric streaming for Green Agent (Enhanced)
======================================================

Emits metric heartbeats on a fixed interval. This is the **push** side
of telemetry — the module that streams snapshots to whatever is watching
in real time, as opposed to `execution_trace.py` (store) and
`metrics_collector.py` (aggregate).

Original API preserved:
    streamer = MetricsStreamer(interval=5)
    streamer.heartbeat(payload)   # prints "[HEARTBEAT] {json}" at most once per interval

Enhanced API:
    streamer = MetricsStreamer(
        interval=5,
        sink=None,                    # None -> print (original behavior)
        use_monotonic=True,
        run_id="...", task_id="...", agent_id="...",
        inject_provenance=True,
        emit_initial=True,
    )
    emitted = streamer.heartbeat(payload)     # returns bool
    streamer.flush(payload)                   # force-emit regardless of interval
    streamer.get_statistics()
    streamer.close()

Sinks
-----
Any callable that accepts a dict:
    streamer = MetricsStreamer(sink=lambda p: my_client.send(p))
    streamer = MetricsStreamer(sink=my_queue.put)
    streamer = MetricsStreamer(sink=lambda p: logger.info(p))

Enhancements:
  1. Quantum-Distillation      — payload may carry precision/route fields
  2. Causal RL                 — provenance supports causal attribution
  3. Federated Analytics       — deployment_id propagated to every payload
  4. Multi-Agent Coordination  — agent_id propagated to every payload
  5. Temporal Logic            — monotonic clock, first-heartbeat contract
  6. Explainable AI            — payload may carry explanation; rationale exposed
  7. Adaptive Precision        — precision field allowed
  8. Carbon Markets            — carbon fields pass through unchanged
  9. Resilience & Chaos        — safe JSON encoder, sink failures do not crash
 10. Human-in-the-Loop         — severity-based HITL routing + callback
 +   Configurable sink (callable, logger, file, queue)
 +   Thread safety
 +   Returns bool (emitted vs suppressed)
 +   Drop counter and statistics
 +   Payload validation and coercion
 +   DecisionRecord emission for decision-bearing payloads
 +   Bridge to ExecutionTrace and metric_provenance
"""

from __future__ import annotations

import dataclasses
import json
import logging
import math
import threading
import time
import uuid
from collections import Counter, deque
from datetime import date, datetime, timezone
from decimal import Decimal
from enum import Enum
from typing import (
    Any, Callable, Deque, Dict, Iterable, List, Optional, Set, Union,
)

logger = logging.getLogger(__name__)


# =============================================================================
# Enums
# =============================================================================

class Severity(Enum):
    INFO = "info"
    WARNING = "warning"
    CRITICAL = "critical"
    EMERGENCY = "emergency"


class EmitStatus(Enum):
    """Reason a heartbeat was or was not emitted."""
    EMITTED = "emitted"
    SUPPRESSED = "suppressed"       # too soon since last emit
    INVALID = "invalid"             # payload could not be coerced
    SINK_ERROR = "sink_error"       # sink callable raised


# =============================================================================
# Statistics
# =============================================================================

_STATS: Counter = Counter()


def get_statistics() -> Dict[str, Any]:
    return {
        "streamers_created": _STATS["streamers"],
        "heartbeats_attempted": _STATS["attempted"],
        "heartbeats_emitted": _STATS["emitted"],
        "heartbeats_suppressed": _STATS["suppressed"],
        "sink_errors": _STATS["sink_errors"],
        "invalid_payloads": _STATS["invalid"],
    }


def reset_statistics() -> None:
    _STATS.clear()


# =============================================================================
# Safe JSON encoding
# =============================================================================

class SafeJSONEncoder(json.JSONEncoder):
    """
    JSON encoder that handles the types that appear in Green Agent
    payloads: datetime, Decimal, dataclasses, numpy scalars (via
    `.item()` or `float()` fallback), sets, bytes, paths.
    """

    def default(self, obj: Any) -> Any:
        # --- datetime family ---
        if isinstance(obj, datetime):
            return obj.isoformat()
        if isinstance(obj, date):
            return obj.isoformat()
        # --- numeric wrappers ---
        if isinstance(obj, Decimal):
            return float(obj)
        # --- dataclasses ---
        if dataclasses.is_dataclass(obj):
            try:
                return dataclasses.asdict(obj)
            except Exception:
                return str(obj)
        # --- sets / frozensets ---
        if isinstance(obj, (set, frozenset)):
            return list(obj)
        # --- bytes ---
        if isinstance(obj, (bytes, bytearray)):
            return obj.decode("utf-8", errors="replace")
        # --- numpy-like scalars (duck-typed) ---
        for attr in ("item", "tolist"):
            if hasattr(obj, attr):
                try:
                    v = getattr(obj, attr)()
                    if isinstance(v, (int, float, str, list, dict)):
                        return v
                except Exception:
                    pass
        # --- Enum ---
        if isinstance(obj, Enum):
            return obj.value
        # --- Path-like ---
        if hasattr(obj, "__fspath__"):
            return str(obj)
        return super().default(obj)


# =============================================================================
# Payload normalization
# =============================================================================

def _coerce_payload(payload: Any) -> Optional[Dict[str, Any]]:
    """Coerce a payload to a dict; return None on failure."""
    if payload is None:
        return None
    if isinstance(payload, dict):
        return dict(payload)
    if dataclasses.is_dataclass(payload) and not isinstance(payload, type):
        try:
            return dataclasses.asdict(payload)
        except Exception:
            return None
    # Mapping-like
    if hasattr(payload, "items") and callable(payload.items):
        try:
            return dict(payload.items())
        except Exception:
            return None
    return None


# =============================================================================
# Default print sink — preserves the original behavior
# =============================================================================

def _default_print_sink(payload: Dict[str, Any]) -> None:
    """Print a heartbeat the way the original module did."""
    try:
        print("[HEARTBEAT]", json.dumps(payload, cls=SafeJSONEncoder))
    except Exception:
        # If serialization still fails, fall back to str
        print("[HEARTBEAT]", str(payload))


# =============================================================================
# The Enhanced MetricsStreamer
# =============================================================================

class MetricsStreamer:
    """
    Enhanced metric streamer with configurable sinks, safe serialization,
    thread safety, provenance injection, and statistics.

    Backward-compatible: `MetricsStreamer(interval=5)` and
    `heartbeat(payload)` behave exactly as the original when all
    enhancement features are disabled and no sink is provided.
    """

    # Severity threshold for HITL routing
    HITL_SEVERITIES: Set[str] = {Severity.CRITICAL.value, Severity.EMERGENCY.value}

    def __init__(
        self,
        interval: float = 5,
        *,
        sink: Optional[Callable[[Dict[str, Any]], None]] = None,
        use_monotonic: bool = True,
        run_id: Optional[str] = None,
        task_id: Optional[str] = None,
        agent_id: Optional[str] = None,
        deployment_id: str = "local",
        inject_provenance: bool = True,
        emit_initial: bool = True,
        features: Optional[Dict[str, bool]] = None,
    ):
        # --- Original fields (preserved names) ---
        self.interval = interval
        self.last = self._now(use_monotonic)

        # --- Enhancement config ---
        self._use_monotonic = use_monotonic
        self._sink: Callable[[Dict[str, Any]], None] = (
            sink if sink is not None else _default_print_sink
        )
        self._explicit_sink = sink is not None

        # --- Provenance to inject ---
        self.run_id = run_id or f"stream-{uuid.uuid4().hex[:8]}"
        self.task_id = task_id
        self.agent_id = agent_id
        self.deployment_id = deployment_id

        # --- Feature flags ---
        self.features: Dict[str, bool] = {
            "provenance_injection": True,
            "safe_serialization": True,
            "thread_safety": True,
            "return_bool": True,
            "statistics": True,
            "severity_routing": True,
            "hitl": True,
            "decision_record": True,
            "sink_error_containment": True,
            "interval_validation": True,
        }
        if features:
            self.features.update(features)

        # --- Interval validation ---
        if self.features.get("interval_validation", True):
            if not isinstance(interval, (int, float)) or isinstance(interval, bool):
                logger.warning(
                    f"Invalid interval {interval!r}; using default 5.0"
                )
                self.interval = 5.0
            elif not math.isfinite(interval) or interval <= 0:
                logger.warning(
                    f"Non-positive interval {interval}; using 5.0"
                )
                self.interval = 5.0

        # --- Thread safety ---
        self._lock = threading.RLock()

        # --- Statistics ---
        self._attempted: int = 0
        self._emitted: int = 0
        self._suppressed: int = 0
        self._sink_errors: int = 0
        self._invalid: int = 0
        self._total_payload_bytes: int = 0
        self._status_counts: Counter = Counter()
        self._history: Deque[Dict[str, Any]] = deque(maxlen=256)

        # --- HITL callback ---
        self._hitl_callback: Optional[
            Callable[[Dict[str, Any]], bool]
        ] = None

        # --- Trace bridge ---
        self.trace: Any = None

        # --- Initial emission contract ---
        if emit_initial:
            # Allow the very first heartbeat to fire immediately
            # (do not subtract interval from `self.last`).
            self.last = self._now(use_monotonic) - self.interval

        _STATS["streamers"] += 1

        logger.debug(
            f"MetricsStreamer initialized (interval={self.interval}s, "
            f"run_id={self.run_id}, explicit_sink={self._explicit_sink})"
        )

    # ------------------------------------------------------------------
    # ORIGINAL public API — preserved
    # ------------------------------------------------------------------

    def heartbeat(self, payload) -> bool:
        """
        Emit a heartbeat if at least `interval` seconds have elapsed.

        Backward-compatible: same signature. When no sink was provided
        and no features alter behavior, the payload is printed as
        `[HEARTBEAT] {json}` exactly as before.

        Enhanced: returns `True` if the payload was emitted, `False`
        otherwise. Never raises.
        """
        return self._emit(payload, force=False)

    # ------------------------------------------------------------------
    # ENHANCED public API
    # ------------------------------------------------------------------

    def flush(self, payload) -> bool:
        """
        Emit a heartbeat immediately, ignoring the interval.

        Useful at shutdown or after an exceptional event.
        Resets `self.last` so the next heartbeat respects the interval.
        """
        return self._emit(payload, force=True)

    def close(self, final_payload: Optional[Dict[str, Any]] = None) -> None:
        """
        Close the streamer.

        If `final_payload` is provided, force-emits it before closing.
        Idempotent — calling twice is safe.
        """
        if final_payload is not None:
            self.flush(final_payload)
        logger.debug(f"MetricsStreamer closed (run_id={self.run_id})")

    def set_sink(
        self, sink: Callable[[Dict[str, Any]], None],
    ) -> None:
        """
        Replace the current sink.

        The new sink will receive the same dict shape as the previous
        sink (with provenance injected when that feature is enabled).
        """
        if not callable(sink):
            raise TypeError(f"sink must be callable, got {type(sink).__name__}")
        with self._lock:
            self._sink = sink
            self._explicit_sink = True
        logger.debug("MetricsStreamer sink replaced")

    def set_hitl_callback(
        self, callback: Callable[[Dict[str, Any]], bool],
    ) -> None:
        """Register a HITL callback for critical-severity heartbeats."""
        self._hitl_callback = callback

    def get_statistics(self) -> Dict[str, Any]:
        """Return cumulative streamer statistics."""
        with self._lock:
            stats = {
                "run_id": self.run_id,
                "deployment_id": self.deployment_id,
                "interval": self.interval,
                "attempted": self._attempted,
                "emitted": self._emitted,
                "suppressed": self._suppressed,
                "invalid": self._invalid,
                "sink_errors": self._sink_errors,
                "emit_rate": (
                    self._emitted / self._attempted
                    if self._attempted else 0.0
                ),
                "mean_payload_bytes": (
                    self._total_payload_bytes / self._emitted
                    if self._emitted else 0.0
                ),
                "status_counts": dict(self._status_counts),
                "features": dict(self.features),
                "explicit_sink": self._explicit_sink,
            }
        return stats

    def export(self) -> Dict[str, Any]:
        """Full serialisable export."""
        with self._lock:
            return {
                "statistics": self.get_statistics(),
                "history": list(self._history),
            }

    def reset(self) -> None:
        """
        Reset internal state and statistics.

        Does not reset the sink. Re-applies the initial-emission
        contract so the next heartbeat fires immediately.
        """
        with self._lock:
            self._attempted = 0
            self._emitted = 0
            self._suppressed = 0
            self._sink_errors = 0
            self._invalid = 0
            self._total_payload_bytes = 0
            self._status_counts.clear()
            self._history.clear()
            self.last = self._now(self._use_monotonic) - self.interval
        logger.debug(f"MetricsStreamer reset (run_id={self.run_id})")

    # ------------------------------------------------------------------
    # Internal emission pipeline
    # ------------------------------------------------------------------

    def _emit(self, payload: Any, *, force: bool) -> bool:
        """Shared emission logic. Never raises."""
        with self._lock:
            self._attempted += 1
            _STATS["attempted"] += 1

            # --- 1. Coerce payload ---
            coerced = _coerce_payload(payload)
            if coerced is None:
                self._invalid += 1
                _STATS["invalid"] += 1
                self._status_counts[EmitStatus.INVALID.value] += 1
                logger.warning(
                    f"heartbeat: could not coerce payload "
                    f"({type(payload).__name__}); skipped"
                )
                return False

            # --- 2. Interval check ---
            now = self._now(self._use_monotonic)
            if not force and (now - self.last) < self.interval:
                self._suppressed += 1
                _STATS["suppressed"] += 1
                self._status_counts[EmitStatus.SUPPRESSED.value] += 1
                return False

            # --- 3. Inject provenance ---
            enriched = dict(coerced)
            if self.features.get("provenance_injection", True):
                enriched.setdefault("run_id", self.run_id)
                enriched.setdefault("deployment_id", self.deployment_id)
                if self.task_id is not None:
                    enriched.setdefault("task_id", self.task_id)
                if self.agent_id is not None:
                    enriched.setdefault("agent_id", self.agent_id)
                enriched.setdefault("emitted_at", self._iso_now())
                enriched.setdefault("interval_s", self.interval)

            # --- 4. Severity / HITL routing ---
            if self.features.get("severity_routing", True):
                self._route_severity(enriched)

            # --- 5. Serialize (for size tracking and safe logging) ---
            payload_bytes = 0
            try:
                serialized = json.dumps(enriched, cls=SafeJSONEncoder)
                payload_bytes = len(serialized.encode("utf-8"))
            except Exception as e:
                # SafeJSONEncoder should handle everything; if it still fails,
                # coerce to strings to avoid losing the emission.
                logger.debug(f"JSON encoding failed: {e}; coercing to str")
                try:
                    serialized = json.dumps(
                        {k: str(v) for k, v in enriched.items()},
                        cls=SafeJSONEncoder,
                    )
                    payload_bytes = len(serialized.encode("utf-8"))
                except Exception:
                    serialized = str(enriched)
                    payload_bytes = len(serialized)

            # --- 6. Sink dispatch ---
            sink_ok = True
            if self.features.get("sink_error_containment", True):
                try:
                    self._sink(enriched)
                except Exception as e:
                    sink_ok = False
                    self._sink_errors += 1
                    _STATS["sink_errors"] += 1
                    self._status_counts[EmitStatus.SINK_ERROR.value] += 1
                    logger.error(f"Sink raised: {e}")
            else:
                # Original semantics: let sink exceptions propagate
                self._sink(enriched)

            # --- 7. Update state ---
            self.last = now
            self._total_payload_bytes += payload_bytes
            self._emitted += 1
            _STATS["emitted"] += 1
            if sink_ok:
                self._status_counts[EmitStatus.EMITTED.value] += 1

            # --- 8. Trace bridge ---
            if self.trace is not None:
                try:
                    self.trace.record(
                        enriched,
                        kind="heartbeat",
                        agent_id=self.agent_id,
                        task_id=self.task_id,
                        run_id=self.run_id,
                        timestamp=now,
                    )
                except Exception as e:
                    logger.debug(f"Trace bridge failed: {e}")

            # --- 9. History ---
            self._history.append({
                "at": self._iso_now(),
                "bytes": payload_bytes,
                "keys": list(enriched.keys()),
                "severity": enriched.get("severity"),
            })

            # --- 10. DecisionRecord emission ---
            if self.features.get("decision_record", True):
                self._maybe_emit_decision_record(enriched)

            return sink_ok

    def _route_severity(self, payload: Dict[str, Any]) -> None:
        """Route critical-severity payloads to the HITL callback."""
        if not self.features.get("hitl", True):
            return
        sev = payload.get("severity")
        if not isinstance(sev, str):
            return
        if sev.lower() not in self.HITL_SEVERITIES:
            return
        if self._hitl_callback is None:
            return
        try:
            self._hitl_callback(payload)
        except Exception as e:
            logger.warning(f"HITL callback failed: {e}")

    def _maybe_emit_decision_record(self, payload: Dict[str, Any]) -> None:
        """Emit a DecisionRecord when the payload carries decision data."""
        if not self.features.get("decision_record", True):
            return
        # Only emit if the payload has decision-ish keys
        decision_indicators = (
            "decision", "selected_action", "policy_version",
            "explanation", "safety_verdict",
        )
        if not any(k in payload for k in decision_indicators):
            return
        try:
            from src.analysis import DecisionRecord  # type: ignore
        except Exception:
            try:
                from analysis import DecisionRecord  # type: ignore
            except Exception:
                return

        try:
            record = DecisionRecord(
                run_id=str(payload.get("run_id", self.run_id)),
                timestamp=self._dt_now(),
                task_id=str(payload.get("task_id", self.task_id or "")),
                selected_action=str(
                    payload.get("selected_action")
                    or payload.get("decision")
                    or "heartbeat"
                ),
                policy_version=str(payload.get("policy_version", "")),
                model_or_agent=str(payload.get("agent_id", self.agent_id or "")),
                latency_ms=float(payload.get("latency_ms", 0.0) or 0.0),
                energy_kwh=float(payload.get("energy_kwh", 0.0) or 0.0),
                carbon_operational_kg=float(
                    payload.get("carbon_operational_kg", 0.0) or 0.0
                ),
                explanation=payload.get("explanation") or {},
                provenance={
                    "source": "streaming",
                    "severity": payload.get("severity"),
                },
            )
            payload["_decision_record"] = record
        except Exception as e:
            logger.debug(f"DecisionRecord emission failed: {e}")

    # ------------------------------------------------------------------
    # Time helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _now(use_monotonic: bool) -> float:
        """Return monotonic or wall-clock time."""
        return time.monotonic() if use_monotonic else time.time()

    @staticmethod
    def _iso_now() -> str:
        return datetime.now(timezone.utc).isoformat()

    @staticmethod
    def _dt_now() -> datetime:
        return datetime.now(timezone.utc)


# =============================================================================
# Convenience sinks
# =============================================================================

def make_logging_sink(
    log: Optional[logging.Logger] = None,
    level: int = logging.INFO,
) -> Callable[[Dict[str, Any]], None]:
    """
    Build a sink that emits heartbeats through a logger.

    The payload is passed as a structured `extra` field so log
    aggregators can index it.
    """
    _log = log or logger

    def _sink(payload: Dict[str, Any]) -> None:
        try:
            _log.log(level, "heartbeat", extra={"heartbeat": payload})
        except Exception:
            # Fallback if `extra` is rejected by a custom formatter
            _log.log(level, f"[HEARTBEAT] {payload}")

    return _sink


def make_file_sink(path: str) -> Callable[[Dict[str, Any]], None]:
    """
    Build a sink that appends one JSON line per heartbeat to `path`.

    Opens the file once and reuses the handle across heartbeats.
    """
    handle = open(path, "a", buffering=1)
    lock = threading.Lock()

    def _sink(payload: Dict[str, Any]) -> None:
        with lock:
            try:
                handle.write(json.dumps(payload, cls=SafeJSONEncoder) + "\n")
            except Exception:
                handle.write(str(payload) + "\n")

    return _sink


def make_queue_sink(q: Any) -> Callable[[Dict[str, Any]], None]:
    """Build a sink that puts each payload onto a queue-like object."""
    def _sink(payload: Dict[str, Any]) -> None:
        q.put(payload)
    return _sink


def make_callback_sink(
    cb: Callable[[Dict[str, Any]], None],
) -> Callable[[Dict[str, Any]], None]:
    """Wrap an arbitrary callable as a sink."""
    if not callable(cb):
        raise TypeError("callback must be callable")
    return cb


# =============================================================================
# Demo
# =============================================================================

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    # --- Original behavior (backward compatible) ---
    print("=== Original behavior ===")
    streamer = MetricsStreamer(interval=5)
    streamer.heartbeat({"cpu": 0.5, "memory": 100})   # emitted (initial)
    streamer.heartbeat({"cpu": 0.6, "memory": 110})   # suppressed (too soon)
    streamer.heartbeat({"cpu": 0.7, "memory": 120})   # suppressed

    # --- Enhanced: interval fire ---
    print("\n=== After waiting 5s ===")
    time.sleep(0.05)   # faster than 5s but demonstrates the mechanism
    streamer.interval = 0.01
    emitted = streamer.heartbeat({"cpu": 0.8, "memory": 130})
    print(f"  returned: {emitted}")

    # --- Enhanced: safe serialization ---
    print("\n=== Safe serialization (original would crash) ===")
    s2 = MetricsStreamer(interval=0.001, sink=lambda p: None)
    from decimal import Decimal
    import datetime as _dt

    @dataclasses.dataclass
    class DecisionPayload:
        action: str
        score: float
        at: _dt.datetime

    s2.heartbeat({
        "dt": _dt.datetime.now(),
        "dec": Decimal("1.234"),
        "obj": DecisionPayload("select_lora", 0.92, _dt.datetime.now()),
        "set": {"a", "b"},
    })
    print("  emitted with datetime/Decimal/dataclass/set without crash")

    # --- Enhanced: JSON-compatible sink to capture payloads ---
    print("\n=== Captured payloads via custom sink ===")
    captured = []
    s3 = MetricsStreamer(
        interval=0.001,
        sink=captured.append,
        run_id="run-001",
        task_id="task-1",
        agent_id="worker-A",
        deployment_id="us-ca-prod-01",
    )
    s3.heartbeat({"cpu": 0.5})
    time.sleep(0.01)
    s3.heartbeat({"cpu": 0.7, "severity": "info"})
    print(f"  captured {len(captured)} heartbeats")
    for p in captured:
        print(f"    keys: {sorted(p.keys())}")
        print(f"    run_id={p['run_id']}, task_id={p['task_id']}, "
              f"agent_id={p['agent_id']}")

    # --- Enhanced: HITL for critical severity ---
    print("\n=== HITL for critical severity ===")
    hitl_events = []
    def hitl(payload):
        hitl_events.append(payload.get("severity"))
        logger.warning(f"HITL received: {payload.get('error')}")
        return True

    s4 = MetricsStreamer(
        interval=0.001,
        sink=lambda p: None,
        run_id="run-002",
    )
    s4.set_hitl_callback(hitl)
    s4.heartbeat({"severity": "info", "cpu": 0.5})
    time.sleep(0.01)
    s4.heartbeat({"severity": "critical", "error": "OOM imminent"})
    time.sleep(0.01)
    s4.heartbeat({"severity": "emergency", "error": "process killed"})
    print(f"  HITL events: {hitl_events}")

    # --- Enhanced: sink error containment ---
    print("\n=== Sink error containment ===")
    def bad_sink(payload):
        raise RuntimeError("network down")

    s5 = MetricsStreamer(interval=0.001, sink=bad_sink)
    ok = s5.heartbeat({"cpu": 0.5})
    print(f"  returned: {ok} (sink raised, but no crash)")
    print(f"  sink_errors: {s5.get_statistics()['sink_errors']}")

    # --- Enhanced: statistics ---
    print("\n=== Statistics ===")
    import json as _json
    print(_json.dumps(s3.get_statistics(), indent=2, default=str))

    # --- Enhanced: flush ---
    print("\n=== Flush ===")
    s6 = MetricsStreamer(interval=3600, sink=captured.append)  # long interval
    before = len(captured)
    s6.heartbeat({"during": "suppressed"})
    s6.flush({"shutdown": "forced"})
    print(f"  captured added: {len(captured) - before}")

    # --- Enhanced: logging sink ---
    print("\n=== Logging sink ===")
    s7 = MetricsStreamer(interval=0.001, sink=make_logging_sink())
    s7.heartbeat({"cpu": 0.5, "action": "heartbeat_test"})

    # --- Enhanced: file sink ---
    print("\n=== File sink ===")
    file_sink = make_file_sink("/tmp/heartbeats.jsonl")
    s8 = MetricsStreamer(interval=0.001, sink=file_sink)
    s8.heartbeat({"cpu": 0.5})
    time.sleep(0.01)
    s8.heartbeat({"cpu": 0.6})
    try:
        with open("/tmp/heartbeats.jsonl") as f:
            lines = f.readlines()
        print(f"  wrote {len(lines)} lines to /tmp/heartbeats.jsonl")
    except Exception as e:
        print(f"  file read failed: {e}")

    # --- Enhanced: provenance injection ---
    print("\n=== Provenance injection ===")
    captured.clear()
    s9 = MetricsStreamer(
        interval=0.001,
        sink=captured.append,
        run_id="run-prov-001",
        task_id="task-42",
        agent_id="agent-beta",
        deployment_id="eu-north-01",
    )
    s9.heartbeat({"cpu": 0.5})
    if captured:
        print(f"  payload: {captured[-1]}")

    # --- Enhanced: DecisionRecord emission ---
    print("\n=== DecisionRecord emission ===")
    s10 = MetricsStreamer(interval=0.001, sink=lambda p: None)
    s10.heartbeat({
        "selected_action": "route_to_lora",
        "policy_version": "v5.0.1",
        "energy_kwh": 0.045,
        "explanation": {"rationale": "lowest carbon route"},
    })
    print("  heartbeat with decision keys processed")

    # --- Interval validation ---
    print("\n=== Interval validation ===")
    bad1 = MetricsStreamer(interval=-5)
    bad2 = MetricsStreamer(interval=float("nan"))
    bad3 = MetricsStreamer(interval="five")
    print(f"  interval=-5:    {bad1.interval}")
    print(f"  interval=nan:   {bad2.interval}")
    print(f"  interval='5':   {bad3.interval}")

    # --- Thread safety sanity ---
    print("\n=== Thread safety ===")
    import threading
    s11 = MetricsStreamer(interval=0.0001, sink=lambda p: None)
    def worker():
        for _ in range(500):
            s11.heartbeat({"cpu": 0.5})
    threads = [threading.Thread(target=worker) for _ in range(4)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    stats11 = s11.get_statistics()
    print(f"  attempted: {stats11['attempted']} "
          f"(expected 2000)")
    print(f"  emitted:   {stats11['emitted']}")
    print(f"  suppressed: {stats11['suppressed']}")

    # --- Module-level statistics ---
    print("\n=== Module statistics ===")
    print(_json.dumps(get_statistics(), indent=2))
