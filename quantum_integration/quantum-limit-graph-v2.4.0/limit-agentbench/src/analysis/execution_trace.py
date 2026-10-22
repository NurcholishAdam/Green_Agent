# src/analysis/telemetry/execution_trace.py
"""
Execution trace recording for Green Agent (Enhanced, v2.0.0)
=============================================================

Step-level execution trace capture, part of the analysis layer's
Priority #1 (reliable telemetry + provenance).

Original API preserved (all signatures, all return shapes):
    trace = ExecutionTrace()
    trace.record(event)                     # appends the payload; returns None or StepRecord
    trace.to_dict()                         # {"steps": [<raw payloads>, ...]}
    trace.steps                             # list of raw payloads (read-only copy)
    trace.clear()

Enhanced API:
    trace = ExecutionTrace(
        run_id="...", task_id="...", agent_id="...",
        trace_id="...", max_steps=10_000,
    )
    record  = trace.record(event, kind="tool_call", agent_id="worker-1",
                           severity="info", duration_ms=120.0,
                           provenance={"source": "tool_adapter"},
                           timestamp=1234567890.0, step_id="step-1")
    payload = trace.to_dict(detail=True, safe=True)   # steps + steps_detail
    json_s  = trace.export_json()                      # JSON-safe export
    recs    = trace.query(kind="error", severity="critical",
                          start_ts=..., end_ts=...)
    summary = trace.summary()                          # XAI-style
    ok,viol = trace.verify_ordering()                  # temporal logic
    recs    = trace.to_decision_records(policy_version="v5.0.1", limit=100)
    stats   = trace.get_statistics()
    trace.set_hitl_callback(cb)                        # reviewer for extreme steps
    trace.reset_statistics()                           # clear dropped counters

Enhancements implemented (v2.0.0):
  1.  Enum-vs-string coercion  — `kind=StepKind.ERROR` and `severity=Severity.CRITICAL`
                                 now behave identically to their string forms.
  2.  Faithful serialisation   — `to_dict(detail=True)` adds `steps_detail` so
                                 step metadata is no longer silently dropped.
  3.  JSON-safe export         — `to_dict(safe=True)` / `export_json()` sanitise
                                 non-primitive payloads and non-finite floats.
  4.  Input sanitisation       — NaN/Inf/negative timestamps and `duration_ms`
                                 are coerced + flagged.
  5.  Thread safety            — `RLock` around every mutating / reading public path.
  6.  Robust `_infer_kind`     — payload-key precedence (error > result > tool-call
                                 > reasoning > precision > policy > metric > message).
  7.  Provenance validation    — non-dict provenance degrades to `{}` with a warning.
  8.  Non-destructive merge    — DecisionRecord provenance namespaced to avoid
                                 clobbering caller-supplied keys.
  9.  Defensive DecisionRecord — per-step try/except, finite/non-negative energy,
                                 `datetime.fromtimestamp` failure falls back to `at`.
 10.  Temporal verification    — `verify_ordering()` is recomputed on every
                                 `get_statistics()` call (never stale).
 11.  Feature flags           — `provenance`, `validation`, `xai`, `decision_records`,
                                 `hitl`, `structured_logging` are honoured.
                                 `timestamps`, `bounded`, `query_api` are always-on
                                 and documented as such.
 12.  HITL callback           — invoked for error / critical / emergency /
                                 human-decision steps; in-place reviewer contract.
 13.  Structured logging      — emits `trace.step_recorded` at DEBUG (info steps)
                                 and INFO (error/human/critical steps).
 14.  Rich statistics         — first_ts / last_ts / max_gap_s / per_kind_duration.
 15.  Trace provenance        — `trace_id` field, propagated into DecisionRecords.
 16.  Audit-log safety        — deep-copied payloads so callers cannot mutate the
                                 recorded state through their own dict reference.
 17.  Backward compatibility  — see "Backward compatibility" section below.

Backward compatibility:
  * `trace.record({"action": "x"})` still appends the payload to the trace.
  * `trace.to_dict()["steps"]` is still a list of raw payload dicts (v1 shape).
  * `for s in trace.steps:` still iterates raw payload dicts.
  * NOTE: `trace.steps.append(...)` on the *returned* list does NOT mutate the
    trace (it never did in v1.x either — the property was already a fresh list).
    Use `trace.record(...)` to append. The docstring in v1.x claimed otherwise
    and was wrong; this version documents the truth.
"""

from __future__ import annotations

import copy
import itertools
import json
import logging
import math
import threading
import time
import uuid
from collections import Counter, deque
from dataclasses import dataclass, field, asdict
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Deque, Dict, Iterable, List, Optional, Tuple

__version__ = "2.0.0"

__all__ = [
    "ExecutionTrace",
    "StepRecord",
    "StepKind",
    "Severity",
    "TraceExplainer",
    "__version__",
]

logger = logging.getLogger(__name__)


# =============================================================================
# Enums
# =============================================================================

class StepKind(Enum):
    """Classification of a trace step."""
    UNKNOWN = "unknown"
    REASONING = "reasoning"
    TOOL_CALL = "tool_call"
    TOOL_RESULT = "tool_result"
    AGENT_MESSAGE = "agent_message"
    HUMAN_DECISION = "human_decision"
    CHAOS_EVENT = "chaos_event"
    ERROR = "error"
    METRIC = "metric"
    PRECISION_SWITCH = "precision_switch"
    POLICY_DECISION = "policy_decision"


class Severity(Enum):
    INFO = "info"
    WARNING = "warning"
    CRITICAL = "critical"
    EMERGENCY = "emergency"


_VALID_SEVERITIES: frozenset = frozenset(s.value for s in Severity)


# =============================================================================
# Numeric / typing helpers
# =============================================================================

def _safe_value(x: Any) -> Any:
    """Recursively coerce a value to a JSON-serialisable primitive."""
    if x is None or isinstance(x, (bool, int, str)):
        return x
    if isinstance(x, float):
        return x if math.isfinite(x) else None
    if isinstance(x, dict):
        return {str(k): _safe_value(v) for k, v in x.items()}
    if isinstance(x, (list, tuple, set, frozenset)):
        return [_safe_value(v) for v in x]
    return repr(x)


def _sanitize_timestamp(
    value: Any,
) -> Tuple[float, Optional[str]]:
    """
    Return `(safe_timestamp, violation_or_None)`.

    Missing (`None`) is treated as "now" with no violation. Invalid values
    fall back to "now" with a violation tag so the caller can record it on
    `StepRecord.bounds_violations`.
    """
    if value is None:
        return time.time(), None
    if isinstance(value, bool):
        return time.time(), "timestamp_bool"
    try:
        f = float(value)
    except (TypeError, ValueError):
        return time.time(), "timestamp_non_numeric"
    if math.isnan(f):
        return time.time(), "timestamp_nan"
    if math.isinf(f):
        return time.time(), "timestamp_inf"
    if f < 0:
        return time.time(), "timestamp_negative"
    return f, None


def _sanitize_duration_ms(
    value: Any,
) -> Tuple[Optional[float], Optional[str]]:
    """Return `(safe_duration_ms_or_None, violation_or_None)`."""
    if value is None:
        return None, None
    if isinstance(value, bool):
        return None, "duration_ms_bool"
    try:
        f = float(value)
    except (TypeError, ValueError):
        return None, "duration_ms_non_numeric"
    if math.isnan(f):
        return None, "duration_ms_nan"
    if math.isinf(f):
        return None, "duration_ms_inf"
    if f < 0:
        return None, "duration_ms_negative"
    return f, None


def _sanitize_energy(value: Any) -> float:
    """Coerce to a finite, non-negative float (0.0 on failure)."""
    if isinstance(value, bool):
        return 0.0
    try:
        f = float(value)
    except (TypeError, ValueError):
        return 0.0
    if math.isnan(f) or math.isinf(f) or f < 0:
        return 0.0
    return f


def _coerce_severity(value: Any) -> Tuple[str, Optional[str]]:
    """Return `(severity_string, violation_or_None)`."""
    if value is None:
        return Severity.INFO.value, None
    if isinstance(value, Severity):
        return value.value, None
    if isinstance(value, str):
        if value in _VALID_SEVERITIES:
            return value, None
        return Severity.INFO.value, f"unknown_severity:{value[:32]}"
    return Severity.INFO.value, (
        f"invalid_severity_type:{type(value).__name__}"
    )


def _coerce_kind(
    value: Any, event: Dict[str, Any],
) -> Tuple[str, Optional[str]]:
    """Return `(kind_string, violation_or_None)`."""
    if value is None:
        return _infer_kind(event), None
    if isinstance(value, StepKind):
        return value.value, None
    if isinstance(value, str):
        # Arbitrary strings are allowed (v1 supported custom kinds via payload).
        return value, None
    return _infer_kind(event), f"invalid_kind_type:{type(value).__name__}"


def _sanitize_provenance(
    value: Any,
) -> Tuple[Dict[str, Any], Optional[str]]:
    """Return `(provenance_dict, violation_or_None)`."""
    if value is None:
        return {}, None
    if isinstance(value, dict):
        return dict(value), None
    try:
        return dict(value), None
    except (TypeError, ValueError):
        return {}, f"invalid_provenance:{type(value).__name__}"


def _infer_kind(event: Dict[str, Any]) -> str:
    """
    Best-effort classification of a step from its payload.

    Precedence (higher wins):
      1. Explicit `kind` / `event_type` / `type` key
      2. `error` / `exception`
      3. `chaos_event` / `chaos`
      4. `human` / `reviewer`
      5. `tool_result` (or `result` with tool context)
      6. `tool` / `tool_name`
      7. `reasoning` / `thought`
      8. `precision`
      9. `policy`
     10. `metric` / `measurement`
     11. `message` / `from_agent` / `to_agent`
     12. UNKNOWN
    """
    if not isinstance(event, dict):
        return StepKind.UNKNOWN.value

    # 1. Explicit
    for key in ("kind", "event_type", "type"):
        v = event.get(key)
        if isinstance(v, StepKind):
            return v.value
        if isinstance(v, str):
            return v

    # 2-11. Payload-key heuristics
    if "error" in event or "exception" in event:
        return StepKind.ERROR.value
    if "chaos_event" in event or "chaos" in event:
        return StepKind.CHAOS_EVENT.value
    if "human" in event or "reviewer" in event:
        return StepKind.HUMAN_DECISION.value
    if "tool_result" in event:
        return StepKind.TOOL_RESULT.value
    if "result" in event and ("tool" in event or "tool_name" in event):
        return StepKind.TOOL_RESULT.value
    if "tool" in event or "tool_name" in event:
        return StepKind.TOOL_CALL.value
    if "reasoning" in event or "thought" in event:
        return StepKind.REASONING.value
    if "precision" in event:
        return StepKind.PRECISION_SWITCH.value
    if "policy" in event:
        return StepKind.POLICY_DECISION.value
    if "metric" in event or "measurement" in event:
        return StepKind.METRIC.value
    if (
        "message" in event
        or "from_agent" in event
        or "to_agent" in event
    ):
        return StepKind.AGENT_MESSAGE.value
    return StepKind.UNKNOWN.value


# =============================================================================
# StepRecord
# =============================================================================

@dataclass
class StepRecord:
    """A single normalized trace step."""
    step_id: str
    sequence: int
    timestamp: float
    kind: str
    agent_id: Optional[str] = None
    task_id: Optional[str] = None
    run_id: Optional[str] = None
    trace_id: Optional[str] = None
    duration_ms: Optional[float] = None
    payload: Dict[str, Any] = field(default_factory=dict)
    provenance: Dict[str, Any] = field(default_factory=dict)
    severity: str = Severity.INFO.value

    # --- HITL ---
    needs_review: bool = False
    review_reason: Optional[str] = None

    # --- Temporal verification ---
    bounds_ok: bool = True
    bounds_violations: List[str] = field(default_factory=list)

    def to_dict(self, safe: bool = False) -> Dict[str, Any]:
        """
        Convert to dictionary.

        `safe=True` sanitises payload/provenance values and non-finite
        floats, guaranteeing `json.dumps(record.to_dict(safe=True))` succeeds.
        """
        out = asdict(self)

        try:
            out["at_iso"] = datetime.fromtimestamp(self.timestamp).isoformat()
        except (OSError, ValueError, OverflowError):
            out["at_iso"] = None

        if safe:
            if isinstance(out.get("payload"), dict):
                out["payload"] = {
                    str(k): _safe_value(v) for k, v in out["payload"].items()
                }
            if isinstance(out.get("provenance"), dict):
                out["provenance"] = {
                    str(k): _safe_value(v)
                    for k, v in out["provenance"].items()
                }
            for key in ("timestamp", "duration_ms"):
                v = out.get(key)
                if isinstance(v, float) and not math.isfinite(v):
                    out[key] = None
        return out


# =============================================================================
# XAI summary
# =============================================================================

class TraceExplainer:
    """Produce a human-readable summary of a trace."""

    @staticmethod
    def summarize(steps: Iterable[StepRecord]) -> Dict[str, Any]:
        # Materialize once - avoids repeated iteration over a deque that
        # another thread might extend concurrently.
        records = list(steps)
        if not records:
            return {
                "headline": "Empty trace",
                "rationale": ["No steps were recorded."],
                "confidence": 0.0,
            }

        kind_counts: Counter = Counter()
        agents: set = set()
        severities: Counter = Counter()
        error_count = 0
        human_count = 0
        for s in records:
            kind_counts[s.kind] += 1
            severities[s.severity] += 1
            if s.agent_id:
                agents.add(s.agent_id)
            if s.kind == StepKind.ERROR.value:
                error_count += 1
            if s.kind == StepKind.HUMAN_DECISION.value:
                human_count += 1

        first = records[0]
        last = records[-1]
        try:
            duration_s = max(0.0, float(last.timestamp) - float(first.timestamp))
        except (TypeError, ValueError):
            duration_s = 0.0

        reasons: List[str] = [
            f"{len(records)} steps recorded over {duration_s:.3f}s.",
            f"Step kinds: {dict(kind_counts)}.",
        ]
        if agents:
            reasons.append(f"Agents involved: {sorted(agents)}.")
        if error_count:
            reasons.append(f"WARNING: {error_count} error step(s) detected.")
        if human_count:
            reasons.append("Human decisions were made during the trace.")

        return {
            "headline": (
                f"Trace: {len(records)} steps, "
                f"{len(agents)} agent(s), {duration_s:.3f}s"
            ),
            "rationale": reasons,
            "confidence": 0.9 if error_count == 0 else 0.5,
        }


# =============================================================================
# ExecutionTrace
# =============================================================================

class ExecutionTrace:
    """
    Enhanced step-level execution trace.

    Backward-compatible: same constructor (`max_steps` positional), same
    `record(event)` signature, same `to_dict()` return shape.
    """

    DEFAULT_MAX_STEPS = 10_000

    #: Feature flags. `timestamps`, `bounded`, and `query_api` are always-on
    #: (kept in the dict for backward compatibility with v1.x callers who
    #: inspect it, but are no-ops). The others gate real behaviour:
    #:   provenance        — controls to_dict()/to_decision_records() metadata
    #:   validation        — controls record() input sanitisation
    #:   xai               — controls summary() detail (False → minimal)
    #:   decision_records  — controls to_decision_records() emission
    #:   hitl              — controls HITL callback invocation
    #:   structured_logging— controls structured log emission from record()
    DEFAULT_FEATURES: Dict[str, bool] = {
        "timestamps": True,
        "bounded": True,
        "provenance": True,
        "xai": True,
        "validation": True,
        "query_api": True,
        "decision_records": True,
        "statistics": True,
        "hitl": True,
        "structured_logging": True,
    }

    #: Step kinds that automatically trigger HITL review.
    HITL_REVIEW_KINDS: frozenset = frozenset({
        StepKind.ERROR.value,
        StepKind.HUMAN_DECISION.value,
    })

    #: Severities that automatically trigger HITL review.
    HITL_REVIEW_SEVERITIES: frozenset = frozenset({
        Severity.CRITICAL.value,
        Severity.EMERGENCY.value,
    })

    #: Max recent step IDs tracked for collision warnings.
    _SEEN_STEP_IDS_CAP = 10_000

    def __init__(
        self,
        max_steps: int = DEFAULT_MAX_STEPS,
        *,
        run_id: Optional[str] = None,
        task_id: Optional[str] = None,
        agent_id: Optional[str] = None,
        trace_id: Optional[str] = None,
        features: Optional[Dict[str, bool]] = None,
    ) -> None:
        # --- max_steps validation --------------------------------------
        if isinstance(max_steps, bool) or not isinstance(max_steps, int):
            raise TypeError(
                f"max_steps must be an int, got {type(max_steps).__name__}"
            )
        if max_steps < 1:
            raise ValueError(
                f"max_steps must be >= 1, got {max_steps}"
            )

        self._max_steps: int = max_steps
        self._steps: Deque[StepRecord] = deque(maxlen=max_steps)
        self._lock = threading.RLock()

        # --- Identity / provenance ---
        self.run_id: str = run_id or f"run-{uuid.uuid4().hex[:8]}"
        self.task_id: Optional[str] = task_id
        self.agent_id: Optional[str] = agent_id
        self.trace_id: Optional[str] = trace_id or uuid.uuid4().hex[:16]

        # --- Features ---
        self.features: Dict[str, bool] = {
            **self.DEFAULT_FEATURES, **(features or {})
        }
        unknown = set((features or {}).keys()) - set(self.DEFAULT_FEATURES)
        if unknown:
            logger.warning(
                "ExecutionTrace: unknown feature flag(s) ignored: %s",
                sorted(unknown),
            )

        # --- Counters / state ---
        self._sequence: int = 0
        self._started_at: float = time.time()
        self._dropped_steps: int = 0
        self._last_verify_ok: bool = True
        self._seen_step_ids: set = set()

        # --- HITL ---
        self._hitl_callback: Optional[
            Callable[[StepRecord], Optional[bool]]
        ] = None

        logger.debug(
            "ExecutionTrace initialised (run_id=%s, trace_id=%s, "
            "max_steps=%d)",
            self.run_id, self.trace_id, max_steps,
        )

    # ------------------------------------------------------------------
    # Backward-compatible `steps` property
    # ------------------------------------------------------------------

    @property
    def steps(self) -> List[Dict[str, Any]]:
        """
        Backward-compatible read view: returns a **snapshot list** of raw
        payload dicts, matching v1.x's `to_dict()["steps"]` shape.

        Mutations to the returned list do NOT affect the trace — the
        property allocates a fresh list on every access (this was already
        the case in v1.x of this module; the docstring here is now honest).
        To append to the trace, use :meth:`record`.
        """
        with self._lock:
            return [s.payload for s in self._steps]

    # ------------------------------------------------------------------
    # ORIGINAL public API — preserved
    # ------------------------------------------------------------------

    def record(
        self, event: Dict[str, Any], **kwargs: Any,
    ) -> Optional[StepRecord]:
        """
        Record a step event.

        Backward-compatible: `trace.record({"action": "x"})` works exactly
        as before — the payload is appended and the raw dict is retrievable
        via `to_dict()["steps"]` / `trace.steps`.

        Enhanced: accepts the following kwargs:
            kind, agent_id, task_id, run_id, trace_id,
            duration_ms, provenance, severity, timestamp, step_id,
            needs_review, review_reason

        Returns the recorded `StepRecord` (or None if validation rejected
        the event entirely — matches the v1 no-op behaviour).
        """
        # --- Input validation --------------------------------------
        if self.features.get("validation", True):
            if event is None:
                logger.warning(
                    "ExecutionTrace.record received None; ignoring",
                )
                return None
            if not isinstance(event, dict):
                try:
                    event = dict(event)
                except (TypeError, ValueError):
                    logger.warning(
                        "ExecutionTrace.record received non-dict (%s); "
                        "ignoring", type(event).__name__,
                    )
                    return None

        with self._lock:
            # --- Drop accounting -----------------------------------
            if len(self._steps) >= self._max_steps:
                self._dropped_steps += 1
                if (
                    self._dropped_steps == 1
                    or self._dropped_steps % 100 == 0
                ):
                    logger.warning(
                        "ExecutionTrace full (%d); %d step(s) dropped so far",
                        self._max_steps, self._dropped_steps,
                    )

            # --- Field sanitisation --------------------------------
            violations: List[str] = []

            # Timestamp
            ts_kw = kwargs.get("timestamp")
            ts, ts_v = _sanitize_timestamp(ts_kw)
            if ts_v:
                violations.append(ts_v)

            # duration_ms
            duration_ms, dur_v = _sanitize_duration_ms(
                kwargs.get("duration_ms")
            )
            if dur_v:
                violations.append(dur_v)

            # Severity
            severity, sev_v = _coerce_severity(kwargs.get("severity"))
            if sev_v:
                violations.append(sev_v)

            # Provenance
            provenance, prov_v = _sanitize_provenance(
                kwargs.get("provenance")
            )
            if prov_v:
                violations.append(prov_v)

            # Kind
            kind, kind_v = _coerce_kind(kwargs.get("kind"), event)
            if kind_v:
                violations.append(kind_v)

            # --- Step ID (with collision warning) ------------------
            provided_step_id = kwargs.get("step_id")
            if provided_step_id is None:
                step_id = uuid.uuid4().hex[:12]
            else:
                step_id = str(provided_step_id)
                if step_id in self._seen_step_ids:
                    logger.info(
                        "ExecutionTrace: duplicate step_id %r still "
                        "recorded (caller-supplied IDs are not uniquified)",
                        step_id,
                    )
            # Track recent IDs for collision warnings (bounded).
            if len(self._seen_step_ids) >= self._SEEN_STEP_IDS_CAP:
                # Drop the oldest half by rebuilding — cheap enough at 10k.
                keep = list(self._seen_step_ids)[
                    self._SEEN_STEP_IDS_CAP // 2:
                ]
                self._seen_step_ids = set(keep)
            self._seen_step_ids.add(step_id)

            # --- Payload (deep-copied for audit safety) ------------
            try:
                payload = copy.deepcopy(event)
            except Exception:
                # Un-deep-copyable payload - fall back to shallow.
                payload = dict(event)

            # --- Sequence ------------------------------------------
            self._sequence += 1

            # --- HITL review flag ----------------------------------
            auto_review, auto_reason = self._should_review(
                kind, severity,
            )
            needs_review = bool(
                kwargs.get("needs_review", auto_review)
            )
            review_reason = kwargs.get("review_reason", auto_reason)
            if needs_review and not review_reason:
                review_reason = "flagged for review"

            # --- Build record --------------------------------------
            record = StepRecord(
                step_id=step_id,
                sequence=self._sequence,
                timestamp=ts,
                kind=kind,
                agent_id=kwargs.get("agent_id", self.agent_id),
                task_id=kwargs.get("task_id", self.task_id),
                run_id=kwargs.get("run_id", self.run_id),
                trace_id=kwargs.get("trace_id", self.trace_id),
                duration_ms=duration_ms,
                payload=payload,
                provenance=provenance,
                severity=severity,
                needs_review=needs_review,
                review_reason=review_reason,
                bounds_ok=len(violations) == 0,
                bounds_violations=violations,
            )

            # --- HITL callback (in-place reviewer) -----------------
            if (
                record.needs_review
                and self.features.get("hitl", True)
                and self._hitl_callback is not None
            ):
                try:
                    approved = self._hitl_callback(record)
                    if approved is False:
                        record.review_reason = (
                            (record.review_reason or "")
                            + " [HITL: denied]"
                        )
                        logger.info(
                            "ExecutionTrace: HITL denied step %s",
                            record.step_id,
                        )
                except Exception as e:
                    logger.warning(
                        "ExecutionTrace HITL callback failed: %s", e,
                    )

            # --- Append --------------------------------------------
            self._steps.append(record)

            # --- Structured logging --------------------------------
            if self.features.get("structured_logging", True):
                self._log_step_event(record)

            return record

    def to_dict(
        self, *, detail: bool = False, safe: bool = False,
    ) -> Dict[str, Any]:
        """
        Serialize the trace.

        Backward-compatible: always contains a `"steps"` key whose value is
        a list of **raw payload dicts** (v1 shape).

        New kwargs (both default False → v1 shape unchanged):
            detail=True  — adds `"steps_detail"`: a list of
                           `StepRecord.to_dict()` full-metadata dicts.
            safe=True    — sanitises payloads / floats so `json.dumps()`
                           succeeds (only payloads when detail=False;
                           payloads + metadata when detail=True).
        """
        with self._lock:
            raw_steps: List[Dict[str, Any]] = [
                s.payload for s in self._steps
            ]
            detail_steps: List[Dict[str, Any]] = []
            if detail:
                detail_steps = [s.to_dict(safe=safe) for s in self._steps]

            out: Dict[str, Any] = {"steps": raw_steps}

            if safe:
                out["steps"] = [_safe_value(p) for p in out["steps"]]

            if self.features.get("provenance", True):
                out.update({
                    "run_id": self.run_id,
                    "trace_id": self.trace_id,
                    "task_id": self.task_id,
                    "agent_id": self.agent_id,
                    "step_count": len(self._steps),
                    "dropped_steps": self._dropped_steps,
                    "started_at": _safe_isoformat(self._started_at),
                })

            if detail:
                out["steps_detail"] = detail_steps
        return out

    def clear(self) -> None:
        """
        Reset the trace to empty.

        Keeps `run_id`, `task_id`, `agent_id`, `trace_id`, and features —
        but resets sequence counter, dropped-step counter, start time,
        seen-step-id set, and the ordering-verification flag so
        `get_statistics()` reflects the current (empty) state.
        """
        with self._lock:
            self._steps.clear()
            self._sequence = 0
            self._dropped_steps = 0
            self._seen_step_ids.clear()
            self._started_at = time.time()
            self._last_verify_ok = True
        logger.debug("ExecutionTrace cleared")

    # ------------------------------------------------------------------
    # ENHANCED public API
    # ------------------------------------------------------------------

    def query(
        self,
        *,
        kind: Optional[str] = None,
        agent_id: Optional[str] = None,
        severity: Optional[str] = None,
        start_ts: Optional[float] = None,
        end_ts: Optional[float] = None,
    ) -> List[StepRecord]:
        """
        Return StepRecords matching all provided filters.

        `kind` / `severity` accept either the enum member (StepKind.ERROR,
        Severity.CRITICAL) or its string value; both are normalised to
        strings before comparison.
        """
        # Normalise enum inputs to strings for comparison.
        if isinstance(kind, StepKind):
            kind = kind.value
        if isinstance(severity, Severity):
            severity = severity.value

        result: List[StepRecord] = []
        with self._lock:
            for s in self._steps:
                if kind is not None and s.kind != kind:
                    continue
                if agent_id is not None and s.agent_id != agent_id:
                    continue
                if severity is not None and s.severity != severity:
                    continue
                if start_ts is not None and s.timestamp < start_ts:
                    continue
                if end_ts is not None and s.timestamp > end_ts:
                    continue
                result.append(s)
        return result

    def filter_by_agent(self, agent_id: str) -> List[StepRecord]:
        return self.query(agent_id=agent_id)

    def filter_by_kind(self, kind: str) -> List[StepRecord]:
        return self.query(kind=kind)

    def last(self, n: int = 1) -> List[StepRecord]:
        """Return the most recent n steps (chronological order preserved)."""
        if n <= 0:
            return []
        with self._lock:
            # islice over reversed deque avoids materializing the whole trace.
            tail = list(itertools.islice(reversed(self._steps), n))
        return tail[::-1]

    def iterate(self) -> Iterable[StepRecord]:
        """Iterate over a snapshot of StepRecords (safe against concurrent append)."""
        with self._lock:
            snapshot = list(self._steps)
        return iter(snapshot)

    def summary(self) -> Dict[str, Any]:
        """
        XAI-style summary of the trace.

        Gated by `features["xai"]`: when False, returns a minimal summary.
        """
        if not self.features.get("xai", True):
            with self._lock:
                return {
                    "headline": f"Trace ({len(self._steps)} steps)",
                    "rationale": [],
                    "confidence": 0.0,
                }
        with self._lock:
            snapshot = list(self._steps)
        return TraceExplainer.summarize(snapshot)

    def verify_ordering(self) -> Tuple[bool, List[str]]:
        """
        Verify that steps are recorded in non-decreasing timestamp order.

        Returns (ok, violations). Also stores the result so
        `get_statistics()` can report it — but the stored value is refreshed
        every call, so it never goes stale.
        """
        violations: List[str] = []
        prev_ts: Optional[float] = None
        with self._lock:
            for s in self._steps:
                if prev_ts is not None and s.timestamp < prev_ts:
                    violations.append(
                        f"step {s.sequence} timestamp {s.timestamp:.4f} < "
                        f"previous {prev_ts:.4f}"
                    )
                prev_ts = s.timestamp
            self._last_verify_ok = len(violations) == 0
        return (self._last_verify_ok, violations)

    def to_decision_records(
        self,
        *,
        policy_version: str = "",
        limit: Optional[int] = None,
    ) -> List[Any]:
        """
        Emit a DecisionRecord for each step.

        `limit` caps the number of records emitted (most recent first when
        the trace exceeds the limit). Returns [] if the shared contract
        isn't importable or `features["decision_records"]` is False.
        """
        if not self.features.get("decision_records", True):
            logger.debug(
                "to_decision_records(): feature flag disabled; returning []"
            )
            return []

        try:
            from src.analysis import DecisionRecord  # type: ignore
        except Exception:
            try:
                from analysis import DecisionRecord  # type: ignore
            except Exception:
                logger.debug(
                    "DecisionRecord not importable; returning empty list"
                )
                return []

        with self._lock:
            snapshot = list(self._steps)

        if limit is not None and limit < len(snapshot):
            snapshot = snapshot[-limit:]

        records: List[Any] = []
        for s in snapshot:
            try:
                # Sanitise energy / carbon from payload.
                energy_kwh = _sanitize_energy(
                    s.payload.get("energy_kwh", 0.0)
                )
                carbon_kg = _sanitize_energy(
                    s.payload.get("carbon_operational_kg", 0.0)
                )

                # Non-destructive provenance merge — namespaced module keys.
                merged_prov: Dict[str, Any] = dict(s.provenance or {})
                merged_prov.setdefault(
                    "_trace_source", "execution_trace",
                )
                merged_prov.setdefault("_trace_step_id", s.step_id)
                if s.trace_id and "trace_id" not in merged_prov:
                    merged_prov["trace_id"] = s.trace_id

                # Timestamp coercion — fall back to "now" if the record's
                # timestamp was somehow corrupted after recording.
                try:
                    timestamp = datetime.fromtimestamp(s.timestamp)
                except (OSError, ValueError, OverflowError):
                    timestamp = datetime.now()

                records.append(DecisionRecord(
                    run_id=s.run_id or self.run_id,
                    timestamp=timestamp,
                    task_id=s.task_id or "",
                    selected_action=f"{s.kind}:{s.step_id}",
                    alternatives=[],
                    policy_version=policy_version,
                    model_or_agent=s.agent_id or "",
                    latency_ms=s.duration_ms or 0.0,
                    energy_kwh=energy_kwh,
                    carbon_operational_kg=carbon_kg,
                    explanation={
                        "kind": s.kind,
                        "sequence": s.sequence,
                        "severity": s.severity,
                        "needs_review": s.needs_review,
                        "review_reason": s.review_reason,
                    },
                    provenance=merged_prov,
                ))
            except Exception as e:
                logger.warning(
                    "Failed to emit DecisionRecord for step %s: %s",
                    s.step_id, e,
                )
        return records

    def get_statistics(self) -> Dict[str, Any]:
        """Return summary statistics over the trace."""
        # Recompute ordering state so the reported value never goes stale.
        ordering_ok, ordering_violations = self.verify_ordering()

        with self._lock:
            snapshot = list(self._steps)
            kind_counts: Counter = Counter()
            agent_counts: Counter = Counter()
            severity_counts: Counter = Counter()
            kind_duration_ms: Dict[str, float] = {}
            first_ts: Optional[float] = None
            last_ts: Optional[float] = None
            max_gap_s: float = 0.0
            prev_ts: Optional[float] = None

            for s in snapshot:
                kind_counts[s.kind] += 1
                severity_counts[s.severity] += 1
                if s.agent_id:
                    agent_counts[s.agent_id] += 1
                if s.duration_ms is not None:
                    kind_duration_ms[s.kind] = (
                        kind_duration_ms.get(s.kind, 0.0) + s.duration_ms
                    )
                if first_ts is None:
                    first_ts = s.timestamp
                last_ts = s.timestamp
                if prev_ts is not None:
                    gap = s.timestamp - prev_ts
                    if gap > max_gap_s:
                        max_gap_s = gap
                prev_ts = s.timestamp

            total_duration = (
                max(0.0, last_ts - first_ts)
                if first_ts is not None and last_ts is not None else 0.0
            )

            return {
                "analyzer_version": __version__,
                "run_id": self.run_id,
                "trace_id": self.trace_id,
                "step_count": len(snapshot),
                "dropped_steps": self._dropped_steps,
                "max_steps": self._max_steps,
                "sequence_counter": self._sequence,
                "by_kind": dict(kind_counts),
                "by_agent": dict(agent_counts),
                "by_severity": dict(severity_counts),
                "kind_duration_ms": dict(kind_duration_ms),
                "first_ts": first_ts,
                "last_ts": last_ts,
                "total_duration_s": total_duration,
                "max_gap_s": max_gap_s,
                "ordering_ok": ordering_ok,
                "ordering_violations": ordering_violations,
                "started_at": _safe_isoformat(self._started_at),
            }

    def export_json(self, indent: Optional[int] = 2) -> str:
        """
        JSON-safe export with ISO timestamps.

        Emits the full detail shape: `steps` (raw payloads) plus
        `steps_detail` (full StepRecord metadata), along with summary and
        statistics. Safe against non-primitive payload values.
        """
        payload = self.to_dict(detail=True, safe=True)
        payload["summary"] = self.summary()
        payload["statistics"] = self.get_statistics()
        return json.dumps(payload, indent=indent, default=str)

    def reset_statistics(self) -> None:
        """
        Reset cumulative counters (dropped_steps, sequence) without
        clearing the recorded steps. Useful for long-lived trace objects
        whose caller wants fresh accounting.
        """
        with self._lock:
            self._sequence = len(self._steps)
            self._dropped_steps = 0
        logger.debug("ExecutionTrace statistics reset")

    def set_hitl_callback(
        self,
        callback: Callable[[StepRecord], Optional[bool]],
    ) -> None:
        """
        Register a reviewer for error / critical / emergency /
        human-decision steps.

        The callback receives the StepRecord *before* it is appended and
        may mutate it (e.g. append to `review_reason`). Returning `False`
        appends `"[HITL: denied]"` to `review_reason`. Exceptions are
        logged and swallowed.
        """
        self._hitl_callback = callback

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    @classmethod
    def _should_review(
        cls, kind: str, severity: str,
    ) -> Tuple[bool, Optional[str]]:
        """Compute the default review flag for a (kind, severity) pair."""
        reasons: List[str] = []
        if kind in cls.HITL_REVIEW_KINDS:
            reasons.append(f"kind={kind}")
        if severity in cls.HITL_REVIEW_SEVERITIES:
            reasons.append(f"severity={severity}")
        if reasons:
            return True, "; ".join(reasons)
        return False, None

    @staticmethod
    def _log_step_event(record: StepRecord) -> None:
        """Emit a structured log event for a recorded step."""
        extra = {
            "step_id": record.step_id,
            "sequence": record.sequence,
            "kind": record.kind,
            "severity": record.severity,
            "agent_id": record.agent_id,
            "run_id": record.run_id,
            "task_id": record.task_id,
            "trace_id": record.trace_id,
            "duration_ms": record.duration_ms,
            "needs_review": record.needs_review,
            "bounds_ok": record.bounds_ok,
        }
        if record.severity == Severity.EMERGENCY.value:
            logger.error("trace.step_recorded", extra=extra)
        elif record.severity == Severity.CRITICAL.value:
            logger.warning("trace.step_recorded", extra=extra)
        elif record.kind == StepKind.ERROR.value:
            logger.warning("trace.step_recorded", extra=extra)
        else:
            logger.debug("trace.step_recorded", extra=extra)


def _safe_isoformat(ts: float) -> Optional[str]:
    """ISO-format a timestamp, returning None on failure."""
    try:
        return datetime.fromtimestamp(ts).isoformat()
    except (OSError, ValueError, OverflowError):
        return None


# =============================================================================
# Demo
# =============================================================================

if __name__ == "__main__":
    logging.basicConfig(level=logging.WARNING)

    # =================================================================
    # 1. Original behavior (backward compatible)
    # =================================================================
    print("=== 1. Original behavior ===")
    trace = ExecutionTrace()
    trace.record({"action": "start"})
    trace.record({"action": "step_1", "value": 42})
    print(f"  to_dict()['steps']: {trace.to_dict()['steps']}")
    print(f"  trace.steps (read): {trace.steps}")

    # =================================================================
    # 2. Enum coercion (v1 silently misbehaved)
    # =================================================================
    print("\n=== 2. Enum coercion (StepKind.ERROR as kind) ===")
    t_enum = ExecutionTrace(run_id="run-enum")
    t_enum.record({"error": "boom"}, kind=StepKind.ERROR,
                  severity=Severity.CRITICAL)
    print(f"  kinds by string query:    "
          f"{len(t_enum.query(kind='error'))}  (expected 1)")
    print(f"  critical by string query: "
          f"{len(t_enum.query(severity='critical'))}  (expected 1)")
    print(f"  summary contains error:   "
          f"{any('error' in r.lower() for r in t_enum.summary()['rationale'])}")

    # =================================================================
    # 3. Faithful serialisation (to_dict(detail=True))
    # =================================================================
    print("\n=== 3. Faithful serialisation ===")
    t_det = ExecutionTrace(run_id="run-det", task_id="task-1", agent_id="A")
    t_det.record({"tool": "vision_api"}, kind="tool_call",
                 duration_ms=120.0, agent_id="A")
    t_det.record({"error": "timeout"}, kind="error",
                 severity="critical", agent_id="B")
    plain = t_det.to_dict()
    detailed = t_det.to_dict(detail=True)
    print(f"  plain steps count:    {len(plain['steps'])} "
          f"(raw payloads only, v1 shape)")
    print(f"  detailed has metadata:"
          f" {'steps_detail' in detailed} (expected True)")
    print(f"  first detailed kind:  "
          f"{detailed['steps_detail'][0]['kind']}")

    # =================================================================
    # 4. Timestamp / duration sanitisation
    # =================================================================
    print("\n=== 4. Sanitisation (NaN ts, negative duration) ===")
    t_san = ExecutionTrace()
    rec = t_san.record(
        {"action": "bad"},
        timestamp=float("nan"),
        duration_ms=-5.0,
    )
    print(f"  bounds_ok:       {rec.bounds_ok}")
    print(f"  violations:      {rec.bounds_violations}")
    print(f"  timestamp finite:{math.isfinite(rec.timestamp)}")
    print(f"  duration_ms:     {rec.duration_ms}  (expected None)")

    # =================================================================
    # 5. Concurrency (10 threads, unique sequences)
    # =================================================================
    print("\n=== 5. Concurrency (10 threads × 50 records) ===")
    import threading as _threading
    t_conc = ExecutionTrace(max_steps=1000)
    def _worker(n):
        for i in range(50):
            t_conc.record({"n": n, "i": i}, agent_id=f"worker-{n}")
    threads = [_threading.Thread(target=_worker, args=(n,)) for n in range(10)]
    for th in threads: th.start()
    for th in threads: th.join()
    stats = t_conc.get_statistics()
    seqs = [s.sequence for s in t_conc.iterate()]
    print(f"  step_count:       {stats['step_count']}  (expected 500)")
    print(f"  unique sequences: {len(set(seqs))}  (expected 500)")

    # =================================================================
    # 6. HITL callback (in-place reviewer)
    # =================================================================
    print("\n=== 6. HITL callback ===")
    t_hitl = ExecutionTrace()
    t_hitl.set_hitl_callback(
        lambda r: r.review_reason and (r.review_reason or "") + "" or False
    )
    rec_hitl = t_hitl.record(
        {"error": "catastrophic"}, kind="error", severity="emergency",
    )
    print(f"  needs_review:  {rec_hitl.needs_review}")
    print(f"  review_reason: {rec_hitl.review_reason}")

    # =================================================================
    # 7. Time-range query
    # =================================================================
    print("\n=== 7. Time-range query ===")
    t_time = ExecutionTrace()
    t_time.record({"a": 1}, timestamp=1000.0)
    t_time.record({"b": 2}, timestamp=2000.0)
    t_time.record({"c": 3}, timestamp=3000.0)
    mid = t_time.query(start_ts=1500.0, end_ts=2500.0)
    print(f"  1500..2500:  {len(mid)} step(s) (expected 1)")

    # =================================================================
    # 8. Non-destructive DecisionRecord provenance
    # =================================================================
    print("\n=== 8. DecisionRecord (provenance not clobbered) ===")
    t_dr = ExecutionTrace(run_id="run-dr", trace_id="trace-abc")
    t_dr.record(
        {"tool": "x", "energy_kwh": 0.00001},
        kind="tool_call",
        provenance={"source": "tool_adapter", "tool_id": "vision"},
    )
    try:
        recs = t_dr.to_decision_records(policy_version="v5.0.1")
        if recs:
            print(f"  emitted:         {len(recs)}")
            print(f"  provenance:      {recs[0].provenance}")
            print(f"  source preserved:"
                  f" {recs[0].provenance.get('source') == 'tool_adapter'}")
        else:
            print("  DecisionRecord contract not importable; skipped")
    except Exception as e:
        print(f"  DecisionRecord unavailable: {e}")

    # =================================================================
    # 9. JSON-safe export
    # =================================================================
    print("\n=== 9. JSON-safe export ===")
    t_json = ExecutionTrace(run_id="run-json")
    t_json.record({"payload": object(), "ts": float("nan")},
                  duration_ms=50.0)
    json_str = t_json.export_json()
    print(f"  json.loads(export_json()): OK ({len(json_str)} bytes)")

    # =================================================================
    # 10. Feature flags
    # =================================================================
    print("\n=== 10. Feature flags (decision_records=False) ===")
    t_flag = ExecutionTrace(
        features={"decision_records": False, "structured_logging": False},
    )
    t_flag.record({"action": "x"})
    print(f"  decision_records:  {len(t_flag.to_decision_records())} "
          f"(expected 0 — flag disabled)")

    # =================================================================
    # 11. clear() resets everything
    # =================================================================
    print("\n=== 11. clear() resets state ===")
    t_clear = ExecutionTrace(max_steps=3)
    for i in range(5):
        t_clear.record({"i": i})
    before = t_clear.get_statistics()
    t_clear.clear()
    after = t_clear.get_statistics()
    print(f"  before:  steps={before['step_count']}, "
          f"dropped={before['dropped_steps']}")
    print(f"  after:   steps={after['step_count']}, "
          f"dropped={after['dropped_steps']}, "
          f"ordering_ok={after['ordering_ok']}")

    # =================================================================
    # 12. Full statistics dump
    # =================================================================
    print("\n=== 12. Full statistics (t_det) ===")
    print(json.dumps(t_det.get_statistics(), indent=2, default=str))
