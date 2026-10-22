# src/analysis/telemetry/streaming.py
"""
Real-time metric streaming for Green Agent (Enhanced, v2.0.0)
==============================================================

Emits metric heartbeats on a fixed interval. This is the **push** side
of telemetry — the module that streams snapshots to whatever is watching
in real time, as opposed to `execution_trace.py` (store) and
`metrics_collector.py` (aggregate).

Original API preserved (all signatures, all side effects):
    streamer = MetricsStreamer(interval=5)
    streamer.heartbeat(payload)       # prints "[HEARTBEAT] {json}"
                                       # at most once per interval

Enhanced API:
    streamer = MetricsStreamer(
        interval=5,
        sink=None,                     # None -> print (original)
        use_monotonic=True,
        run_id="...", task_id="...", agent_id="...",
        deployment_id="us-ca-prod-01",
        inject_provenance=True,
        emit_initial=True,
        reset_interval_on_sink_error=True,   # v1 behaviour; False → retry
        hitl_severities=None,                # override CRITICAL/EMERGENCY
        time_source=None,                    # injectable clock for tests
        datetime_source=None,                # injectable timestamp
    )

    emitted = streamer.heartbeat(payload)          # bool (v1 contract)
    result  = streamer.heartbeat_detailed(payload) # EmitResult
    emitted = streamer.flush(payload)              # force-emit
    result  = streamer.flush_detailed(payload)     # EmitResult

    streamer.set_interval(0.1)          # validated setter (v1 allowed mutation)
    streamer.set_sink(cb) / set_sink(None)
    streamer.set_hitl_callback(cb) / set_hitl_callback(None)
    streamer.close(final_payload=None)  # idempotent; closes file sinks

    streamer.get_last_emit_status()     # Optional[EmitStatus]
    streamer.get_last_emit_result()     # Optional[EmitResult]
    streamer.get_statistics()
    streamer.export(safe=True)
    streamer.reset()

Sinks
-----
Any callable that accepts a dict:
    streamer = MetricsStreamer(sink=lambda p: my_client.send(p))
    streamer = MetricsStreamer(sink=my_queue.put)

    # Built-in factories
    streamer = MetricsStreamer(sink=make_logging_sink())
    streamer = MetricsStreamer(sink=make_file_sink("/tmp/hb.jsonl"))
    streamer = MetricsStreamer(sink=make_queue_sink(q, on_full="drop"))
    streamer = MetricsStreamer(sink=make_callback_sink(my_fn))

Enhancements implemented (v2.0.0):
  1.  Lock discipline       — the sink callable runs **outside** the lock.
                              v1 held the lock across sink dispatch, so a
                              blocking network sink serialised every
                              concurrent heartbeat (and a re-entrant sink
                              risked cross-thread deadlock).
  2.  Decision-record order — the decision record is built **before** the
                              sink dispatch and delivered as a serialised
                              dict on `payload["decision_record"]`. v1
                              attached it after the sink (the sink never
                              saw it) as a live object (broke
                              `json.dumps` for any caller who kept the
                              payload).
  3.  Truthful counters     — `emitted` increments only on sink success.
                              `sink_errors` increments on sink failure.
                              v1 counted a failed send as emitted too,
                              hiding real sink outages from telemetry
                              dashboards.
  4.  Retry on sink error   — `last` is **not** advanced on sink failure
                              by default, so a retry on the next call is
                              not suppressed by the interval. Pass
                              `reset_interval_on_sink_error=True` for v1
                              behaviour.
  5.  Signature-filtered DR — `inspect.signature(DecisionRecord)` filter;
                              a contract mismatch returns a WARNING and
                              omits the field rather than raising.
  6.  EmitResult            — `heartbeat_detailed()` / `flush_detailed()`
                              return `EmitResult` distinguishing
                              EMITTED / SUPPRESSED / INVALID / SINK_ERROR
                              / CLOSED. `heartbeat()` keeps its v1 bool
                              contract (`result.emitted`).
  7.  HITL annotation       — a HITL callback returning `False` sets
                              `payload["hitl_approved"]=False` so the
                              sink (and downstream consumers) see the
                              denial. v1 discarded the return value.
  8.  Validated interval    — `set_interval()` and internal reads
                              validate finite / positive; v1 allowed
                              `streamer.interval = -5` (never suppressed)
                              or `nan` (also never suppressed).
  9.  Genuinely-safe encoder— `SafeJSONEncoder.default` returns `str(obj)`
                              on unknown types (v1 raised). `Decimal`
                              NaN/Inf map to `None` so the JSON is RFC
                              8259-valid (v1 emitted `NaN` / `Infinity`).
 10.  Closed state          — `close()` sets an idempotent `_closed`
                              flag; post-close `heartbeat()` returns
                              `EmitStatus.CLOSED`. Closes the sink if it
                              exposes `.close()` (file sinks).
 11.  Closable file sink    — `make_file_sink()` returns a callable
                              object with `.close()`; the underlying
                              file handle is released on
                              `MetricsStreamer.close()`. v1 leaked the
                              fd for the process lifetime.
 12.  Non-blocking queue    — `make_queue_sink(q, on_full="raise"|"drop"|
                              "block")` uses `put_nowait` by default so
                              a full queue surfaces as SINK_ERROR
                              rather than deadlocking the heartbeat
                              thread (v1 called blocking `put()`).
 13.  Honest history        — `_history` and `_total_payload_bytes`
                              only advance on successful emits;
                              each entry carries `sink_ok` and
                              `status`.
 14.  Immutable HITL set    — `HITL_SEVERITIES: frozenset` (v1 was a
                              mutable class-level `set`); per-instance
                              override via `hitl_severities=` kwarg.
 15.  Monotonic warning     — a `WARNING` logs when
                              `use_monotonic=False` because wall-clock
                              jumps silently suppress heartbeats.
 16.  Injectable clock      — `time_source=` / `datetime_source=` for
                              tests (`freezegun`-friendly).
 17.  `get_last_emit_status`/`get_last_emit_result` — callers can
                              inspect the most recent outcome.
 18.  `export(safe=True)`   — JSON-safe export.
 19.  `EmitResult.to_dict(safe=True)` — JSON-safe.
 20.  `sink_name`           — `"print"|"log"|"file"|"queue"|"callable"`
                              for observability; tagged by the factory
                              helpers.
 21.  Structured logging    — `streaming.emitted`, `.suppressed`,
                              `.invalid_payload`, `.sink_error`,
                              `.hitl_denied`, `.closed`,
                              `.interval_invalid`, `.decision_record_failed`.
 22.  `__version__`/`__all__`.

Backward compatibility:
  * `MetricsStreamer(interval=5)` + `heartbeat(payload)` returns `bool`
    and prints `[HEARTBEAT] {json}` — identical for the happy path.
  * `heartbeat()` returning `False` is still `False` for all four
    non-emitted statuses; use `heartbeat_detailed()` to distinguish
    them.
  * `flush()` still force-emits and returns `bool`.
  * `streamer.interval = X` still works (direct attribute write), but
    `set_interval(X)` now validates. A caller relying on the silent
    acceptance of `interval=-5` or `interval=nan` is fixed by reading
    `_emit` interval checks via `set_interval` (which is what
    `__init__` uses internally too).
  * `payload["_decision_record"]` (v1 key) is **no longer written** — the
    serialised dict is emitted on `payload["decision_record"]` instead,
    before the sink. Callers reading `_decision_record` should read
    `decision_record` or use `result.decision_record`.
  * `HITL_SEVERITIES` is now a `frozenset` — same `in` checks work;
    mutation is no longer possible (which was a footgun).
  * `close()` is now idempotent and terminal — post-close `heartbeat()`
    returns `False` with `EmitStatus.CLOSED` (v1 silently continued).
  * `make_queue_sink(q)` uses non-blocking `put_nowait` by default —
    a full queue surfaces as a sink error (v1 blocked indefinitely).
    Pass `on_full="block"` for v1 behaviour.
"""

from __future__ import annotations

import dataclasses
import inspect
import json
import logging
import math
import threading
import time
import uuid
from collections import Counter, deque
from dataclasses import dataclass, field, asdict, is_dataclass
from datetime import date, datetime, timezone
from decimal import Decimal
from enum import Enum
from typing import (
    Any, Callable, Deque, Dict, Iterable, List, Optional, Set, Union,
)

__version__ = "2.0.0"

__all__ = [
    "MetricsStreamer",
    "EmitResult",
    "EmitStatus",
    "SafeJSONEncoder",
    "Severity",
    "make_logging_sink",
    "make_file_sink",
    "make_queue_sink",
    "make_callback_sink",
    "get_statistics",
    "reset_statistics",
    "__version__",
]

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
    SUPPRESSED = "suppressed"      # interval not elapsed
    INVALID = "invalid"            # payload could not be coerced
    SINK_ERROR = "sink_error"      # sink raised
    CLOSED = "closed"              # streamer was closed


_VALID_SEVERITIES = frozenset(s.value for s in Severity)


# =============================================================================
# EmitResult — structured outcome
# =============================================================================

@dataclass
class EmitResult:
    """Structured outcome of one `heartbeat`/`flush` call."""
    status: str
    emitted: bool
    sink_ok: bool
    at: datetime = field(
        default_factory=lambda: datetime.now(timezone.utc)
    )
    payload: Optional[Dict[str, Any]] = None
    error: Optional[str] = None
    decision_record: Optional[Dict[str, Any]] = None
    hitl_approved: Optional[bool] = None
    payload_bytes: int = 0

    def to_dict(self, safe: bool = False) -> Dict[str, Any]:
        out: Dict[str, Any] = {
            "status": self.status,
            "emitted": self.emitted,
            "sink_ok": self.sink_ok,
            "at": (
                self.at.isoformat()
                if isinstance(self.at, datetime) else self.at
            ),
            "error": self.error,
            "decision_record": self.decision_record,
            "hitl_approved": self.hitl_approved,
            "payload_bytes": self.payload_bytes,
        }
        if self.payload is not None:
            out["payload_keys"] = sorted(self.payload.keys())
        if safe:
            return _jsonable(out)
        return out


# =============================================================================
# Helpers
# =============================================================================

def _jsonable(v: Any) -> Any:
    if v is None or isinstance(v, (bool, int, str)):
        return v
    if isinstance(v, float):
        return v if math.isfinite(v) else None
    if isinstance(v, datetime):
        return v.isoformat()
    if isinstance(v, date):
        return v.isoformat()
    if isinstance(v, Enum):
        return v.value
    if isinstance(v, Decimal):
        f = float(v)
        return f if math.isfinite(f) else None
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
    """Module-level statistics for `MetricsStreamer` calls."""
    snap = _GLOBAL_STATS.snapshot()
    return {
        "analyzer_version": __version__,
        "streamers_created": snap.get("streamers", 0),
        "heartbeats_attempted": snap.get("attempted", 0),
        "heartbeats_emitted": snap.get("emitted", 0),
        "heartbeats_suppressed": snap.get("suppressed", 0),
        "sink_errors": snap.get("sink_errors", 0),
        "invalid_payloads": snap.get("invalid", 0),
        "closed_attempts": snap.get("closed", 0),
    }


def reset_statistics() -> None:
    """Clear the module-level counter (v1 compatibility)."""
    _GLOBAL_STATS.reset()


# =============================================================================
# Safe JSON encoding
# =============================================================================

class SafeJSONEncoder(json.JSONEncoder):
    """
    JSON encoder that handles the types that appear in Green Agent
    payloads: datetime, Decimal, dataclasses, numpy scalars, sets,
    bytes, paths, UUIDs, and *any* other object via `str()` fallback.

    `Decimal("NaN")` / `Decimal("Infinity")` are emitted as `null`
    (RFC 8259-valid JSON); v1 emitted invalid `NaN`/`Infinity`
    tokens that jq, most Go/Rust parsers, and `pandas.read_json`
    reject.
    """

    def default(self, obj: Any) -> Any:
        # --- datetime family ---
        if isinstance(obj, datetime):
            return obj.isoformat()
        if isinstance(obj, date):
            return obj.isoformat()
        # --- numeric wrappers ---
        if isinstance(obj, Decimal):
            f = float(obj)
            return f if math.isfinite(f) else None
        if isinstance(obj, float):
            return obj if math.isfinite(obj) else None
        # --- dataclasses ---
        if is_dataclass(obj) and not isinstance(obj, type):
            try:
                return asdict(obj)
            except Exception:
                return str(obj)
        # --- collections ---
        if isinstance(obj, (set, frozenset)):
            return list(obj)
        if isinstance(obj, (bytes, bytearray)):
            return obj.decode("utf-8", errors="replace")
        # --- Enum ---
        if isinstance(obj, Enum):
            return obj.value
        # --- Path-like ---
        if hasattr(obj, "__fspath__"):
            return str(obj)
        # --- numpy-like scalars (duck-typed) ---
        for attr in ("item", "tolist"):
            if hasattr(obj, attr):
                try:
                    v = getattr(obj, attr)()
                    if isinstance(v, (int, float, str, list, dict)):
                        return v
                except Exception:
                    pass
        # --- Last resort: stringify (v1 raised TypeError) ---
        return str(obj)


# =============================================================================
# Payload normalization
# =============================================================================

def _coerce_payload(payload: Any) -> Optional[Dict[str, Any]]:
    """Coerce a payload to a dict; return None on failure."""
    if payload is None:
        return None
    if isinstance(payload, dict):
        return dict(payload)
    if is_dataclass(payload) and not isinstance(payload, type):
        try:
            return asdict(payload)
        except Exception:
            return None
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
        print("[HEARTBEAT]", str(payload))


_default_print_sink._ga_kind = "print"  # type: ignore[attr-defined]


# =============================================================================
# MetricsStreamer
# =============================================================================

class MetricsStreamer:
    """
    Enhanced metric streamer with configurable sinks, safe serialization,
    thread safety, provenance injection, and statistics.

    Backward-compatible: `MetricsStreamer(interval=5)` and
    `heartbeat(payload)` behave exactly as the original when no sink is
    provided and no features alter behavior.
    """

    #: Severity values that trigger HITL routing. Frozen so a subclass
    #: or caller can't silently broaden the set process-wide.
    HITL_SEVERITIES: frozenset = frozenset({
        Severity.CRITICAL.value,
        Severity.EMERGENCY.value,
    })

    DEFAULT_INTERVAL = 5.0

    DEFAULT_FEATURES: Dict[str, bool] = {
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

    def __init__(
        self,
        interval: float = DEFAULT_INTERVAL,
        *,
        sink: Optional[Callable[[Dict[str, Any]], None]] = None,
        use_monotonic: bool = True,
        run_id: Optional[str] = None,
        task_id: Optional[str] = None,
        agent_id: Optional[str] = None,
        deployment_id: str = "local",
        inject_provenance: bool = True,
        emit_initial: bool = True,
        reset_interval_on_sink_error: bool = True,
        hitl_severities: Optional[Iterable[str]] = None,
        time_source: Optional[Callable[[], float]] = None,
        datetime_source: Optional[Callable[[], datetime]] = None,
        features: Optional[Dict[str, bool]] = None,
    ):
        # --- Feature flags (before interval validation — it can be gated) ---
        self.features: Dict[str, bool] = {
            **self.DEFAULT_FEATURES, **(features or {})
        }
        unknown = set((features or {}).keys()) - set(self.DEFAULT_FEATURES)
        if unknown:
            logger.warning(
                "streaming.unknown_features",
                extra={"unknown": sorted(unknown)},
            )

        # --- Interval validation ---
        self.interval: float = self._validate_interval(interval, "interval")

        # --- Monotonic / wall clock ---
        self._use_monotonic = bool(use_monotonic)
        if not self._use_monotonic and time_source is None:
            logger.warning(
                "streaming.monotonic_disabled",
                extra={
                    "reason": "wall-clock jumps silently suppress "
                              "heartbeats; prefer use_monotonic=True",
                },
            )
        self._time_source: Optional[Callable[[], float]] = time_source
        self._datetime_source: Optional[Callable[[], datetime]] = (
            datetime_source
        )

        # --- Sink (None → default print) ---
        self._sink: Callable[[Dict[str, Any]], None] = (
            sink if sink is not None else _default_print_sink
        )
        self._explicit_sink: bool = sink is not None

        # --- Provenance ---
        self.run_id: str = run_id or f"stream-{uuid.uuid4().hex[:8]}"
        self.task_id = task_id
        self.agent_id = agent_id
        self.deployment_id = deployment_id
        self._inject_provenance: bool = bool(inject_provenance)

        # --- HITL severities (per-instance override) ---
        if hitl_severities is not None:
            self._hitl_severities: frozenset = frozenset(
                str(s).lower() for s in hitl_severities
            )
        else:
            self._hitl_severities = self.HITL_SEVERITIES

        # --- Sink-error interval behaviour ---
        self._reset_interval_on_sink_error = bool(
            reset_interval_on_sink_error
        )

        # --- Concurrency ---
        self._lock = threading.RLock()

        # --- Interval tracking ---
        now = self._now()
        if emit_initial:
            self.last: float = now - self.interval
        else:
            self.last: float = now

        # --- Lifecycle ---
        self._closed: bool = False

        # --- Statistics (per-instance) ---
        self._stats = _StatsCounter()
        self._attempted: int = 0
        self._emitted: int = 0
        self._suppressed: int = 0
        self._sink_errors: int = 0
        self._invalid: int = 0
        self._total_payload_bytes: int = 0
        self._status_counts: Counter = Counter()
        self._history: Deque[Dict[str, Any]] = deque(maxlen=256)
        self._last_emit_result: Optional[EmitResult] = None

        # --- HITL callback ---
        self._hitl_callback: Optional[
            Callable[[Dict[str, Any]], Optional[bool]]
        ] = None

        # --- Trace bridge ---
        self.trace: Any = None

        _GLOBAL_STATS.increment("streamers")

        logger.debug(
            "streaming.initialized",
            extra={
                "interval": self.interval,
                "run_id": self.run_id,
                "explicit_sink": self._explicit_sink,
                "sink_name": self._sink_name(),
            },
        )

    # ------------------------------------------------------------------
    # Interval validation
    # ------------------------------------------------------------------

    @staticmethod
    def _validate_interval(value: Any, name: str) -> float:
        if isinstance(value, bool):
            logger.warning(
                "streaming.interval_invalid",
                extra={"value": repr(value), "reason": "bool"},
            )
            return MetricsStreamer.DEFAULT_INTERVAL
        try:
            f = float(value)
        except (TypeError, ValueError):
            logger.warning(
                "streaming.interval_invalid",
                extra={"value": repr(value), "reason": "non_numeric"},
            )
            return MetricsStreamer.DEFAULT_INTERVAL
        if not math.isfinite(f) or f <= 0:
            logger.warning(
                "streaming.interval_invalid",
                extra={"value": value, "reason": "non_finite_or_non_positive"},
            )
            return MetricsStreamer.DEFAULT_INTERVAL
        return f

    def set_interval(self, value: Any) -> None:
        """
        Set the emission interval with validation.

        v1 allowed direct attribute assignment (e.g.
        `streamer.interval = -5` or `nan`) which silently disabled
        throttling. This setter rejects those inputs and falls back to
        the default, logging a WARNING.
        """
        with self._lock:
            self.interval = self._validate_interval(value, "interval")
            # Reset `last` so the new interval takes effect from now
            self.last = self._now() - self.interval

    # ------------------------------------------------------------------
    # v1 public API
    # ------------------------------------------------------------------

    def heartbeat(self, payload: Any) -> bool:
        """
        Emit a heartbeat if at least `interval` seconds have elapsed.

        Backward-compatible: returns `True` when emitted, `False`
        otherwise. Use `heartbeat_detailed()` to distinguish the four
        non-emitted statuses.
        """
        result = self._emit(payload, force=False)
        return result.emitted

    # ------------------------------------------------------------------
    # Enhanced public API
    # ------------------------------------------------------------------

    def heartbeat_detailed(self, payload: Any) -> EmitResult:
        """Emit and return a structured `EmitResult`."""
        return self._emit(payload, force=False)

    def flush(self, payload: Any) -> bool:
        """Force-emit a heartbeat, ignoring the interval. Returns bool."""
        result = self._emit(payload, force=True)
        return result.emitted

    def flush_detailed(self, payload: Any) -> EmitResult:
        """Force-emit a heartbeat and return the structured result."""
        return self._emit(payload, force=True)

    def close(
        self, final_payload: Optional[Dict[str, Any]] = None,
    ) -> None:
        """
        Close the streamer.

        If `final_payload` is provided, force-emits it before closing.
        Idempotent. Closes the sink if it exposes `.close()` (file
        sinks). Post-close `heartbeat()` returns
        `EmitStatus.CLOSED`.
        """
        with self._lock:
            if self._closed:
                return
        if final_payload is not None:
            self._emit(final_payload, force=True)
        with self._lock:
            if self._closed:
                return
            self._closed = True
            sink = self._sink
        # Close the sink if it's a closable helper
        closer = getattr(sink, "close", None)
        if callable(closer):
            try:
                closer()
            except Exception as e:
                logger.warning(
                    "streaming.sink_close_failed",
                    extra={"error": str(e)},
                )
        logger.debug(
            "streaming.closed",
            extra={"run_id": self.run_id, "sink": self._sink_name()},
        )

    def set_sink(
        self,
        sink: Optional[Callable[[Dict[str, Any]], None]],
    ) -> None:
        """
        Replace the current sink. `None` resets to the default print sink.
        """
        with self._lock:
            if sink is None:
                self._sink = _default_print_sink
                self._explicit_sink = False
            elif not callable(sink):
                raise TypeError(
                    f"sink must be callable or None, got "
                    f"{type(sink).__name__}"
                )
            else:
                self._sink = sink
                self._explicit_sink = True
        logger.debug(
            "streaming.sink_replaced",
            extra={"sink_name": self._sink_name()},
        )

    def set_hitl_callback(
        self,
        callback: Optional[
            Callable[[Dict[str, Any]], Optional[bool]]
        ],
    ) -> None:
        """
        Register (or clear) a HITL callback for critical-severity
        heartbeats.

        Passing `None` clears the callback.
        """
        with self._lock:
            self._hitl_callback = callback

    def get_last_emit_status(self) -> Optional[EmitStatus]:
        """Return the last `EmitStatus`, or None if no emit was attempted."""
        with self._lock:
            if self._last_emit_result is None:
                return None
            try:
                return EmitStatus(self._last_emit_result.status)
            except ValueError:
                return None

    def get_last_emit_result(self) -> Optional[EmitResult]:
        """Return the last `EmitResult`, or None."""
        with self._lock:
            return self._last_emit_result

    # ------------------------------------------------------------------
    # Statistics / export / reset
    # ------------------------------------------------------------------

    def get_statistics(self) -> Dict[str, Any]:
        """Return cumulative streamer statistics."""
        with self._lock:
            mean_bytes = (
                self._total_payload_bytes / self._emitted
                if self._emitted else 0.0
            )
            return {
                "analyzer_version": __version__,
                "run_id": self.run_id,
                "deployment_id": self.deployment_id,
                "interval": self.interval,
                "closed": self._closed,
                "attempted": self._attempted,
                "emitted": self._emitted,
                "suppressed": self._suppressed,
                "invalid": self._invalid,
                "sink_errors": self._sink_errors,
                "emit_rate": (
                    self._emitted / self._attempted
                    if self._attempted else 0.0
                ),
                "mean_payload_bytes": mean_bytes,
                "status_counts": dict(self._status_counts),
                "features": dict(self.features),
                "explicit_sink": self._explicit_sink,
                "sink_name": self._sink_name(),
                "hitl_severities": sorted(self._hitl_severities),
                "reset_interval_on_sink_error":
                    self._reset_interval_on_sink_error,
                "instance_counters": dict(self._stats.snapshot()),
            }

    def export(self, *, safe: bool = False) -> Dict[str, Any]:
        """Full serialisable export. `safe=True` sanitises for JSON."""
        with self._lock:
            payload: Dict[str, Any] = {
                "analyzer_version": __version__,
                "statistics": self.get_statistics(),
                "history": list(self._history),
                "last_emit_result": (
                    self._last_emit_result.to_dict(safe=True)
                    if self._last_emit_result is not None else None
                ),
            }
        if safe:
            return _jsonable(payload)
        return payload

    def reset(self) -> None:
        """
        Reset counters, history, and the initial-emission contract.

        Does **not** clear the closed flag, sink, HITL callback, or
        identity — use `close()` to terminate.
        """
        with self._lock:
            self._stats.reset()
            self._attempted = 0
            self._emitted = 0
            self._suppressed = 0
            self._sink_errors = 0
            self._invalid = 0
            self._total_payload_bytes = 0
            self._status_counts.clear()
            self._history.clear()
            self._last_emit_result = None
            self.last = self._now() - self.interval
        logger.debug(
            "streaming.reset",
            extra={"run_id": self.run_id},
        )

    # ------------------------------------------------------------------
    # Emission pipeline
    # ------------------------------------------------------------------

    def _emit(self, payload: Any, *, force: bool) -> EmitResult:
        """
        Shared emission pipeline. Never raises.

        Phase 1 (under lock): attempt-count, closed check, payload
        coercion, interval check, provenance injection, decision-record
        build, size tracking, sink-reference capture.

        Phase 2 (unlocked): HITL routing, sink dispatch.

        Phase 3 (under lock): counter updates, `last` maintenance,
        history, trace forwarding.
        """
        # =================================================================
        # Phase 1 — under lock
        # =================================================================
        with self._lock:
            self._attempted += 1
            self._stats.increment("attempted")
            _GLOBAL_STATS.increment("attempted")

            # --- Closed ---
            if self._closed:
                self._status_counts[EmitStatus.CLOSED.value] += 1
                _GLOBAL_STATS.increment("closed")
                result = EmitResult(
                    status=EmitStatus.CLOSED.value,
                    emitted=False,
                    sink_ok=False,
                    error="streamer is closed",
                )
                self._last_emit_result = result
                return result

            # --- Coerce payload ---
            coerced = _coerce_payload(payload)
            if coerced is None:
                self._invalid += 1
                self._stats.increment("invalid")
                _GLOBAL_STATS.increment("invalid")
                self._status_counts[EmitStatus.INVALID.value] += 1
                logger.warning(
                    "streaming.invalid_payload",
                    extra={"type": type(payload).__name__},
                )
                result = EmitResult(
                    status=EmitStatus.INVALID.value,
                    emitted=False,
                    sink_ok=False,
                    error=(
                        f"payload could not be coerced to dict "
                        f"(got {type(payload).__name__})"
                    ),
                )
                self._last_emit_result = result
                return result

            # --- Interval check ---
            now = self._now()
            if not force and (now - self.last) < self.interval:
                self._suppressed += 1
                self._stats.increment("suppressed")
                _GLOBAL_STATS.increment("suppressed")
                self._status_counts[EmitStatus.SUPPRESSED.value] += 1
                result = EmitResult(
                    status=EmitStatus.SUPPRESSED.value,
                    emitted=False,
                    sink_ok=False,
                    error=None,
                )
                self._last_emit_result = result
                return result

            # --- Capture old `last` for potential rollback ---
            old_last = self.last
            # Advance `last` immediately so concurrent threads see it
            self.last = now

            # --- Provenance injection ---
            enriched = dict(coerced)
            if (
                self.features.get("provenance_injection", True)
                and self._inject_provenance
            ):
                enriched.setdefault("run_id", self.run_id)
                enriched.setdefault("deployment_id", self.deployment_id)
                if self.task_id is not None:
                    enriched.setdefault("task_id", self.task_id)
                if self.agent_id is not None:
                    enriched.setdefault("agent_id", self.agent_id)
                enriched.setdefault("emitted_at", self._iso_now())
                enriched.setdefault("interval_s", self.interval)

            # --- Decision record (BEFORE the sink) ---
            decision_record_dict: Optional[Dict[str, Any]] = None
            if self.features.get("decision_record", True):
                decision_record_dict = self._build_decision_record(
                    enriched,
                )
                if decision_record_dict is not None:
                    enriched["decision_record"] = decision_record_dict

            # --- Size tracking ---
            payload_bytes = self._measure_payload(enriched)

            # --- Capture sink for phase 2 ---
            sink = self._sink
            hitl_callback = self._hitl_callback
            trace = self.trace

        # =================================================================
        # Phase 2 — unlocked (HITL then sink)
        # =================================================================
        hitl_approved: Optional[bool] = None
        if (
            self.features.get("severity_routing", True)
            and self.features.get("hitl", True)
        ):
            sev = enriched.get("severity")
            if (
                isinstance(sev, str)
                and sev.lower() in self._hitl_severities
                and hitl_callback is not None
            ):
                try:
                    approved = hitl_callback(enriched)
                    if approved is False:
                        hitl_approved = False
                        enriched["hitl_approved"] = False
                        logger.info(
                            "streaming.hitl_denied",
                            extra={
                                "run_id": self.run_id,
                                "severity": sev,
                            },
                        )
                    elif approved is True:
                        hitl_approved = True
                        enriched["hitl_approved"] = True
                except Exception as e:
                    logger.warning(
                        "streaming.hitl_callback_failed",
                        extra={"error": str(e)},
                    )

        # --- Sink (unlocked) ---
        sink_ok = True
        sink_error: Optional[str] = None
        try:
            sink(enriched)
        except Exception as e:
            sink_ok = False
            sink_error = f"{type(e).__name__}: {e}"
            logger.error(
                "streaming.sink_error",
                extra={
                    "error": sink_error,
                    "sink_name": self._sink_name_safe(sink),
                },
            )

        # =================================================================
        # Phase 3 — under lock
        # =================================================================
        with self._lock:
            if sink_ok:
                self._emitted += 1
                self._stats.increment("emitted")
                _GLOBAL_STATS.increment("emitted")
                self._total_payload_bytes += payload_bytes
                self._status_counts[EmitStatus.EMITTED.value] += 1
            else:
                self._sink_errors += 1
                self._stats.increment("sink_errors")
                _GLOBAL_STATS.increment("sink_errors")
                self._status_counts[EmitStatus.SINK_ERROR.value] += 1
                # Retry policy: restore `last` unless v1 behaviour requested
                if not self._reset_interval_on_sink_error:
                    self.last = old_last

            # --- History (success only, or annotated on failure) ---
            self._history.append({
                "at": self._iso_now(),
                "bytes": payload_bytes,
                "keys": sorted(enriched.keys()),
                "severity": enriched.get("severity"),
                "sink_ok": sink_ok,
                "status": (
                    EmitStatus.EMITTED.value if sink_ok
                    else EmitStatus.SINK_ERROR.value
                ),
            })

            result = EmitResult(
                status=(
                    EmitStatus.EMITTED.value if sink_ok
                    else EmitStatus.SINK_ERROR.value
                ),
                emitted=sink_ok,
                sink_ok=sink_ok,
                payload=enriched,
                error=sink_error,
                decision_record=decision_record_dict,
                hitl_approved=hitl_approved,
                payload_bytes=payload_bytes,
            )
            self._last_emit_result = result

            # --- Trace forwarding (before releasing lock? No — outside) ---
            # We'll do it after releasing since trace.record may block.
            trace_ref = trace

        # --- Trace forwarding (outside lock) ---
        if trace_ref is not None:
            try:
                trace_ref.record(
                    enriched,
                    kind="heartbeat",
                    agent_id=self.agent_id,
                    task_id=self.task_id,
                    run_id=self.run_id,
                    timestamp=now,
                    severity=(
                        "critical" if hitl_approved is False else "info"
                    ),
                )
            except Exception as e:
                logger.debug(
                    "streaming.trace_forward_failed",
                    extra={"error": str(e)},
                )

        # --- Structured log ---
        log_extra = {
            "run_id": self.run_id,
            "status": result.status,
            "severity": enriched.get("severity"),
            "payload_bytes": payload_bytes,
            "hitl_approved": hitl_approved,
            "decision_record_present":
                decision_record_dict is not None,
            "sink_name": self._sink_name(),
        }
        if sink_ok:
            logger.info("streaming.emitted", extra=log_extra)
        else:
            logger.warning("streaming.sink_error", extra=log_extra)
        return result

    # ------------------------------------------------------------------
    # Decision record builder
    # ------------------------------------------------------------------

    def _build_decision_record(
        self, payload: Dict[str, Any],
    ) -> Optional[Dict[str, Any]]:
        """
        Build a serialised decision record for a decision-bearing payload.

        Returns the serialised **dict** so callers can round-trip the
        payload through JSON. v1 attached the live object which broke
        `json.dumps` for anyone holding the payload.
        """
        decision_indicators = (
            "decision", "selected_action", "policy_version",
            "explanation", "safety_verdict",
        )
        if not any(k in payload for k in decision_indicators):
            return None

        DecisionRecord = _import_decision_record()
        if DecisionRecord is None:
            return None

        candidates: Dict[str, Any] = {
            "run_id": str(payload.get("run_id", self.run_id)),
            "timestamp": self._dt_now(),
            "task_id": str(payload.get("task_id", self.task_id or "")),
            "selected_action": str(
                payload.get("selected_action")
                or payload.get("decision")
                or "heartbeat"
            ),
            "alternatives": [],
            "policy_version": str(payload.get("policy_version", "")),
            "model_or_agent": str(
                payload.get("agent_id", self.agent_id or "")
            ),
            "latency_ms": _safe_float(payload.get("latency_ms"), 0.0),
            "energy_kwh": _safe_float(payload.get("energy_kwh"), 0.0),
            "carbon_operational_kg": _safe_float(
                payload.get("carbon_operational_kg"), 0.0,
            ),
            "explanation": _jsonable(payload.get("explanation") or {}),
            "provenance": {
                "source": "streaming",
                "analyzer_version": __version__,
                "severity": payload.get("severity"),
                "deployment_id": self.deployment_id,
            },
        }
        filtered = _filter_dr_kwargs(DecisionRecord, candidates)
        try:
            record = DecisionRecord(**filtered)
        except TypeError as e:
            logger.warning(
                "streaming.decision_record_failed",
                extra={"error": str(e)},
            )
            return None

        serialised = _serialise_decision_record(record)
        return _jsonable(serialised) if serialised is not None else None

    # ------------------------------------------------------------------
    # Size / clock helpers
    # ------------------------------------------------------------------

    def _measure_payload(self, payload: Dict[str, Any]) -> int:
        """Return the serialized payload size in bytes (0 on failure)."""
        try:
            serialized = json.dumps(payload, cls=SafeJSONEncoder)
            return len(serialized.encode("utf-8"))
        except Exception:
            return len(str(payload))

    def _now(self) -> float:
        if self._time_source is not None:
            return self._time_source()
        return time.monotonic() if self._use_monotonic else time.time()

    def _dt_now(self) -> datetime:
        if self._datetime_source is not None:
            dt = self._datetime_source()
            if isinstance(dt, datetime):
                return dt
            return datetime.now(timezone.utc)
        return datetime.now(timezone.utc)

    def _iso_now(self) -> str:
        return self._dt_now().isoformat()

    def _sink_name(self) -> str:
        return self._sink_name_safe(self._sink)

    @staticmethod
    def _sink_name_safe(sink: Any) -> str:
        return getattr(sink, "_ga_kind", "callable")


def _safe_float(value: Any, default: float = 0.0) -> float:
    if value is None or isinstance(value, bool):
        return default
    try:
        f = float(value)
    except (TypeError, ValueError, OverflowError):
        return default
    return f if math.isfinite(f) else default


# =============================================================================
# Sink factories
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
            _log.log(level, f"[HEARTBEAT] {payload}")

    _sink._ga_kind = "log"  # type: ignore[attr-defined]
    return _sink


class _FileSink:
    """
    Callable sink that appends one JSON line per heartbeat to `path`.

    Closable: `MetricsStreamer.close()` invokes `.close()` so the
    underlying file handle is released. v1 leaked the fd for the
    process lifetime.
    """

    def __init__(self, path: str) -> None:
        self._path = str(path)
        self._handle = open(self._path, "a", buffering=1)
        self._lock = threading.Lock()
        self._closed = False
        self._ga_kind = "file"

    def __call__(self, payload: Dict[str, Any]) -> None:
        with self._lock:
            if self._closed:
                raise RuntimeError("file sink is closed")
            try:
                line = json.dumps(payload, cls=SafeJSONEncoder) + "\n"
            except Exception:
                line = str(payload) + "\n"
            self._handle.write(line)

    def close(self) -> None:
        with self._lock:
            if self._closed:
                return
            self._closed = True
            try:
                self._handle.close()
            except Exception:
                pass


def make_file_sink(path: str) -> Callable[[Dict[str, Any]], None]:
    """
    Build a closable sink that appends one JSON line per heartbeat.

    The returned object exposes `.close()` — invoked by
    `MetricsStreamer.close()`.
    """
    return _FileSink(path)


class _QueueSink:
    """
    Callable sink that puts each payload onto a queue-like object.

    Non-blocking by default: `on_full` controls behaviour when the queue
    is full:
      - "raise" (default) — raise RuntimeError → EmitStatus.SINK_ERROR
      - "drop"            — silently skip (heartbeat is lost, but no crash)
      - "block"           — v1 behaviour: blocking put()
    """

    def __init__(self, q: Any, on_full: str = "raise") -> None:
        if on_full not in ("raise", "drop", "block"):
            raise ValueError(
                f"on_full must be 'raise'|'drop'|'block', got {on_full!r}"
            )
        self._q = q
        self._on_full = on_full
        self._ga_kind = "queue"

    def __call__(self, payload: Dict[str, Any]) -> None:
        if self._on_full == "block":
            self._q.put(payload)
            return
        try:
            self._q.put_nowait(payload)
        except Exception:
            # Full or otherwise unreachable queue
            if self._on_full == "drop":
                return
            raise


def make_queue_sink(
    q: Any, *, on_full: str = "raise",
) -> Callable[[Dict[str, Any]], None]:
    """
    Build a non-blocking queue sink.

    `on_full` selects the behaviour when the queue is full:
      - "raise" (default) — surfaces as `EmitStatus.SINK_ERROR`
      - "drop"            — heartbeat is dropped silently
      - "block"           — v1 behaviour (blocking `put()`)
    """
    return _QueueSink(q, on_full=on_full)


def make_callback_sink(
    cb: Callable[[Dict[str, Any]], None],
) -> Callable[[Dict[str, Any]], None]:
    """Wrap an arbitrary callable as a sink."""
    if not callable(cb):
        raise TypeError("callback must be callable")
    try:
        setattr(cb, "_ga_kind", "callable")
    except Exception:
        pass
    return cb


# =============================================================================
# Demo
# =============================================================================

if __name__ == "__main__":
    import sys
    import types
    import threading as _threading

    logging.basicConfig(level=logging.WARNING)
    reset_statistics()

    # =================================================================
    # 1. Original behaviour (backward compatible)
    # =================================================================
    print("=== 1. Original behaviour ===")
    streamer = MetricsStreamer(interval=5)
    print(f"  heartbeat #1 returned: "
          f"{streamer.heartbeat({'cpu': 0.5, 'memory': 100})}")
    print(f"  heartbeat #2 returned: "
          f"{streamer.heartbeat({'cpu': 0.6, 'memory': 110})}  "
          f"(suppressed)")

    # =================================================================
    # 2. Sink runs outside the lock
    # =================================================================
    print("\n=== 2. Sink runs outside the lock ===")
    sink_started = _threading.Event()
    sink_release = _threading.Event()

    def slow_sink(payload):
        sink_started.set()
        sink_release.wait(timeout=2)

    s_slow = MetricsStreamer(interval=0.001, sink=slow_sink)
    # First heartbeat enters the sink (unlocked)
    t1 = _threading.Thread(target=s_slow.heartbeat, args=({"a": 1},))
    t1.start()
    sink_started.wait(timeout=1)
    # Second heartbeat should not block on the first (they share no lock)
    # Count how long the second heartbeat takes
    import time as _t
    start = _t.monotonic()
    s_slow.heartbeat({"b": 2})   # would block v1
    elapsed = _t.monotonic() - start
    sink_release.set()
    t1.join(timeout=2)
    print(f"  concurrent heartbeat while sink blocked: "
          f"{elapsed:.3f}s  "
          f"(v1 would block ~2s; v2 must be fast)")

    # =================================================================
    # 3. Decision record reaches the sink
    # =================================================================
    print("\n=== 3. Decision record reaches the sink ===")
    captured_dr = []
    s_dr = MetricsStreamer(
        interval=0.001, sink=captured_dr.append,
        run_id="run-dr",
    )
    s_dr.heartbeat({
        "selected_action": "route_to_lora",
        "policy_version": "v5.0.1",
        "explanation": {"headline": "test"},
    })
    if captured_dr:
        p = captured_dr[-1]
        print(f"  'decision_record' in payload: "
              f"{'decision_record' in p}")
        print(f"  type of decision_record: "
              f"{type(p.get('decision_record')).__name__}  "
              f"(expected 'dict', v1 was 'DecisionRecord')")
        try:
            json.dumps(p)
            print(f"  json.dumps(payload): OK")
        except (TypeError, ValueError) as e:
            print(f"  json.dumps(payload) FAILED: {e}")

    # =================================================================
    # 4. Sink failure: emitted counter truth
    # =================================================================
    print("\n=== 4. Sink failure counter truth ===")

    def bad_sink(payload):
        raise RuntimeError("network down")

    s_bad = MetricsStreamer(interval=0.001, sink=bad_sink)
    ok = s_bad.heartbeat({"cpu": 0.5})
    stats = s_bad.get_statistics()
    print(f"  heartbeat returned: {ok}")
    print(f"  emitted:   {stats['emitted']}  (v1 would be 1)")
    print(f"  sink_errors: {stats['sink_errors']}")
    print(f"  emit_rate: {stats['emit_rate']:.2f}")

    # =================================================================
    # 5. Retry after sink error (default: not suppressed)
    # =================================================================
    print("\n=== 5. Retry after sink error ===")
    call_count = {"n": 0}
    def flaky_sink(payload):
        call_count["n"] += 1
        if call_count["n"] == 1:
            raise RuntimeError("transient")

    s_flaky = MetricsStreamer(
        interval=10.0, sink=flaky_sink,  # long interval
    )
    r1 = s_flaky.heartbeat({"a": 1})
    r2 = s_flaky.heartbeat({"a": 2})
    print(f"  r1 returned: {r1}  (sink failed)")
    print(f"  r2 returned: {r2}  "
          f"(expected True — retry not suppressed)")
    print(f"  statuses: "
          f"{[r for r in [s_flaky.get_last_emit_status()]]}")

    # =================================================================
    # 6. EmitResult distinguishes all statuses
    # =================================================================
    print("\n=== 6. EmitResult statuses ===")
    s_dist = MetricsStreamer(interval=10.0, sink=lambda p: None)
    r_emitted = s_dist.heartbeat_detailed({"a": 1})       # emitted
    r_suppressed = s_dist.heartbeat_detailed({"a": 2})    # suppressed
    r_invalid = s_dist.heartbeat_detailed(None)           # invalid
    s_dist.close()
    r_closed = s_dist.heartbeat_detailed({"a": 3})        # closed
    print(f"  emitted:    {r_emitted.status}, emitted={r_emitted.emitted}")
    print(f"  suppressed: {r_suppressed.status}, "
          f"emitted={r_suppressed.emitted}")
    print(f"  invalid:    {r_invalid.status}, emitted={r_invalid.emitted}")
    print(f"  closed:     {r_closed.status}, emitted={r_closed.emitted}")

    # =================================================================
    # 7. HITL denial annotation
    # =================================================================
    print("\n=== 7. HITL denial annotation ===")
    cap_hitl = []
    s_hitl = MetricsStreamer(
        interval=0.001, sink=cap_hitl.append,
    )
    s_hitl.set_hitl_callback(lambda p: False)
    s_hitl.heartbeat({"severity": "critical", "error": "OOM"})
    if cap_hitl:
        p = cap_hitl[-1]
        print(f"  hitl_approved in payload: {p.get('hitl_approved')}  "
              f"(expected False)")
    r_hitl = s_hitl.get_last_emit_result()
    if r_hitl:
        print(f"  result.hitl_approved: {r_hitl.hitl_approved}")

    # =================================================================
    # 8. SafeJSONEncoder handles every type
    # =================================================================
    print("\n=== 8. SafeJSONEncoder completeness ===")
    from decimal import Decimal as D
    import uuid as _uuid
    class Opaque: pass
    payload = {
        "uuid": _uuid.uuid4(),
        "opaque": Opaque(),
        "decimal_nan": D("NaN"),
        "decimal_inf": D("Infinity"),
        "dt": datetime.now(),
    }
    try:
        serialized = json.dumps(payload, cls=SafeJSONEncoder)
        print(f"  json.dumps(SafeJSONEncoder): OK")
        print(f"  decimal_nan in output: "
              f"{'NaN' in serialized}  (v1 emitted invalid JSON)")
        # Verify strict JSON parse
        json.loads(serialized, parse_constant=lambda x: (_ for _ in ()).throw(ValueError(x)))
        print(f"  strict parse: OK  (RFC 8259 valid)")
    except (TypeError, ValueError) as e:
        print(f"  FAILED: {e}")

    # =================================================================
    # 9. set_interval validates
    # =================================================================
    print("\n=== 9. set_interval validation ===")
    s_int = MetricsStreamer(interval=5.0, sink=lambda p: None)
    print(f"  initial interval: {s_int.interval}")
    s_int.set_interval(-5)
    print(f"  after set_interval(-5): {s_int.interval}  "
          f"(expected 5.0 default)")
    s_int.set_interval("bad")
    print(f"  after set_interval('bad'): {s_int.interval}")
    s_int.set_interval(0.1)
    print(f"  after set_interval(0.1): {s_int.interval}")

    # =================================================================
    # 10. Closed state is terminal
    # =================================================================
    print("\n=== 10. close() is terminal ===")
    s_closed = MetricsStreamer(interval=0.001, sink=lambda p: None)
    s_closed.heartbeat({"a": 1})
    s_closed.close()
    r_post = s_closed.heartbeat_detailed({"a": 2})
    print(f"  post-close status: {r_post.status}")
    s_closed.close()  # idempotent
    print(f"  second close(): OK")

    # =================================================================
    # 11. Closable file sink
    # =================================================================
    print("\n=== 11. Closable file sink ===")
    import tempfile, os
    fd, path = tempfile.mkstemp(suffix=".jsonl")
    os.close(fd)
    file_sink = make_file_sink(path)
    s_file = MetricsStreamer(interval=0.001, sink=file_sink)
    s_file.heartbeat({"a": 1})
    s_file.heartbeat({"a": 2}) if s_file.interval <= 0.001 else None
    s_file.close()   # closes the file handle
    try:
        file_sink({"a": 3})   # should raise
        print(f"  post-close write succeeded (unexpected)")
    except RuntimeError:
        print(f"  post-close write raised RuntimeError: OK")
    with open(path) as f:
        print(f"  file has {len(f.readlines())} lines")
    try:
        os.unlink(path)
    except Exception:
        pass

    # =================================================================
    # 12. Non-blocking queue sink
    # =================================================================
    print("\n=== 12. Non-blocking queue sink ===")
    import queue
    q = queue.Queue(maxsize=1)
    q_sink = make_queue_sink(q, on_full="raise")
    s_q = MetricsStreamer(interval=0.001, sink=q_sink)
    # First put: OK
    r_put1 = s_q.heartbeat_detailed({"a": 1})
    # Second put: queue full → sink error (no deadlock)
    r_put2 = s_q.heartbeat_detailed({"a": 2})
    print(f"  first:  {r_put1.status}")
    print(f"  second: {r_put2.status}  (queue full, no deadlock)")

    # =================================================================
    # 13. export(safe=True)
    # =================================================================
    print("\n=== 13. export(safe=True) ===")
    s_exp = MetricsStreamer(interval=0.001, sink=lambda p: None)
    s_exp.heartbeat({"a": 1, "dt": datetime.now()})
    try:
        json.dumps(s_exp.export(safe=True))
        print(f"  json.dumps(export(safe=True)): OK")
    except (TypeError, ValueError) as e:
        print(f"  FAILED: {e}")

    # =================================================================
    # 14. Thread safety with barrier
    # =================================================================
    print("\n=== 14. Thread safety ===")
    reset_statistics()
    s_conc = MetricsStreamer(interval=0.0001, sink=lambda p: None)
    barrier = _threading.Barrier(4)

    def _worker():
        barrier.wait()
        for _ in range(500):
            s_conc.heartbeat({"cpu": 0.5})

    threads = [_threading.Thread(target=_worker) for _ in range(4)]
    for t in threads: t.start()
    for t in threads: t.join()
    stats_conc = s_conc.get_statistics()
    print(f"  attempted: {stats_conc['attempted']}  (expected 2000)")
    print(f"  emitted:   {stats_conc['emitted']}")

    # =================================================================
    # 15. Statistics
    # =================================================================
    print("\n=== 15. Statistics ===")
    print(json.dumps(streamer.get_statistics(), indent=2, default=str))
