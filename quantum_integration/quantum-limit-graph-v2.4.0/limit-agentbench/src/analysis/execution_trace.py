# src/analysis/telemetry/execution_trace.py

"""
Execution trace recording for Green Agent (Enhanced)
====================================================

Step-level execution trace capture, part of the analysis layer's
Priority #1 (reliable telemetry + provenance).

Original API preserved:
    trace = ExecutionTrace()
    trace.record(event)
    trace.to_dict()   # {"steps": [...]}

Enhanced API:
    trace = ExecutionTrace(run_id="...", task_id="...", agent_id="...")
    trace.record(event, kind="tool_call", agent_id="worker-1")
    trace.query(kind="tool_call")
    trace.filter_by_agent("worker-1")
    trace.summary()                    # XAI-style headline
    trace.to_decision_records()        # shared contract
    trace.get_statistics()
    trace.export_json()                # JSON-safe
    trace.verify_ordering()            # temporal logic
"""

from __future__ import annotations

import json
import logging
import time
import uuid
from collections import Counter, deque
from dataclasses import dataclass, field, asdict
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Deque, Dict, Iterable, List, Optional, Tuple

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


# =============================================================================
# StepRecord — the normalized step shape
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
    duration_ms: Optional[float] = None
    payload: Dict[str, Any] = field(default_factory=dict)
    provenance: Dict[str, Any] = field(default_factory=dict)
    severity: str = Severity.INFO.value

    def to_dict(self) -> Dict[str, Any]:
        out = asdict(self)
        out["at_iso"] = datetime.fromtimestamp(self.timestamp).isoformat()
        return out


# =============================================================================
# XAI summary
# =============================================================================

class TraceExplainer:
    """Produce a human-readable summary of a trace."""

    @staticmethod
    def summarize(steps: List[StepRecord]) -> Dict[str, Any]:
        if not steps:
            return {
                "headline": "Empty trace",
                "rationale": ["No steps were recorded."],
                "confidence": 0.0,
            }

        kind_counts: Counter = Counter(s.kind for s in steps)
        agents = {s.agent_id for s in steps if s.agent_id}
        first = steps[0]
        last = steps[-1]
        duration_s = max(0.0, last.timestamp - first.timestamp)

        reasons: List[str] = [
            f"{len(steps)} steps recorded over {duration_s:.3f}s.",
            f"Step kinds: {dict(kind_counts)}.",
        ]
        if agents:
            reasons.append(f"Agents involved: {sorted(agents)}.")
        if any(s.kind == StepKind.ERROR.value for s in steps):
            errors = sum(1 for s in steps if s.kind == StepKind.ERROR.value)
            reasons.append(f"WARNING: {errors} error step(s) detected.")
        if any(s.kind == StepKind.HUMAN_DECISION.value for s in steps):
            reasons.append("Human decisions were made during the trace.")

        return {
            "headline": (
                f"Trace: {len(steps)} steps, "
                f"{len(agents)} agent(s), {duration_s:.3f}s"
            ),
            "rationale": reasons,
            "confidence": (
                0.9 if not any(s.kind == StepKind.ERROR.value for s in steps)
                else 0.5
            ),
        }


# =============================================================================
# ENHANCED ExecutionTrace
# =============================================================================

class ExecutionTrace:
    """
    Enhanced step-level execution trace.

    Backward-compatible: same constructor, same `record(event)` signature,
    same `to_dict()` return shape.
    """

    DEFAULT_MAX_STEPS = 10_000
    DEFAULT_FEATURES: Dict[str, bool] = {
        "timestamps": True,
        "bounded": True,
        "provenance": True,
        "xai": True,
        "validation": True,
        "query_api": True,
        "decision_records": True,
        "statistics": True,
    }

    def __init__(
        self,
        max_steps: int = DEFAULT_MAX_STEPS,
        *,
        run_id: Optional[str] = None,
        task_id: Optional[str] = None,
        agent_id: Optional[str] = None,
        features: Optional[Dict[str, bool]] = None,
    ):
        # --- Original field (preserved name; now a deque for bounded memory) ---
        # The public attribute `steps` remains a list-like sequence.
        self._steps: Deque[StepRecord] = deque(maxlen=max_steps)
        self._max_steps = max_steps

        # --- Enhancement state ---
        self.run_id = run_id or f"run-{uuid.uuid4().hex[:8]}"
        self.task_id = task_id
        self.agent_id = agent_id
        self.features: Dict[str, bool] = {
            **self.DEFAULT_FEATURES, **(features or {})
        }

        self._sequence: int = 0
        self._started_at: float = time.time()
        self._dropped_steps: int = 0
        self._last_verify_ok: bool = True

        logger.debug(
            f"ExecutionTrace initialised (run_id={self.run_id}, "
            f"max_steps={max_steps})"
        )

    # ------------------------------------------------------------------
    # Backward-compatible `steps` property
    # ------------------------------------------------------------------

    @property
    def steps(self) -> List[Dict[str, Any]]:
        """
        Backward-compatible view of raw step payloads.

        Original code that did `trace.steps.append(...)` or
        `for s in trace.steps:` still works.
        """
        return [s.payload for s in self._steps]

    # ------------------------------------------------------------------
    # ORIGINAL public API — preserved
    # ------------------------------------------------------------------

    def record(self, event: Dict[str, Any], **kwargs: Any) -> None:
        """
        Record a step event.

        Backward-compatible: `trace.record({"action": "x"})` works exactly
        as before — the payload is appended.

        Enhanced: accepts `kind`, `agent_id`, `duration_ms`, `provenance`,
        `severity`, and `timestamp` as keyword arguments.
        """
        # --- Input validation ---
        if self.features.get("validation", True):
            if event is None:
                logger.warning("ExecutionTrace.record received None; ignoring")
                return
            if not isinstance(event, dict):
                try:
                    event = dict(event)
                except (TypeError, ValueError):
                    logger.warning(
                        f"ExecutionTrace.record received non-dict "
                        f"({type(event).__name__}); ignoring"
                    )
                    return

        # --- Bounded memory handling ---
        if len(self._steps) == self._max_steps:
            self._dropped_steps += 1
            if self._dropped_steps == 1 or self._dropped_steps % 100 == 0:
                logger.warning(
                    f"ExecutionTrace full ({self._max_steps}); "
                    f"{self._dropped_steps} step(s) dropped so far"
                )

        # --- Build the normalized StepRecord ---
        self._sequence += 1
        step_id = kwargs.get("step_id") or uuid.uuid4().hex[:12]
        ts = (
            float(kwargs["timestamp"])
            if kwargs.get("timestamp") is not None else time.time()
        )
        kind = kwargs.get("kind") or self._infer_kind(event)

        record = StepRecord(
            step_id=step_id,
            sequence=self._sequence,
            timestamp=ts,
            kind=kind,
            agent_id=kwargs.get("agent_id", self.agent_id),
            task_id=kwargs.get("task_id", self.task_id),
            run_id=kwargs.get("run_id", self.run_id),
            duration_ms=kwargs.get("duration_ms"),
            payload=dict(event),
            provenance=dict(kwargs.get("provenance") or {}),
            severity=kwargs.get("severity", Severity.INFO.value),
        )
        self._steps.append(record)

    def to_dict(self) -> Dict[str, Any]:
        """
        Serialize the trace.

        Backward-compatible: always contains a `"steps"` key whose value is
        the list of payloads — matching the original return shape exactly.

        When provenance/timestamps features are enabled, additional metadata
        keys are added (purely additive).
        """
        out: Dict[str, Any] = {"steps": self.steps}

        if self.features.get("provenance", True):
            out.update({
                "run_id": self.run_id,
                "task_id": self.task_id,
                "agent_id": self.agent_id,
                "step_count": len(self._steps),
                "dropped_steps": self._dropped_steps,
                "started_at": datetime.fromtimestamp(
                    self._started_at
                ).isoformat(),
            })
        return out

    # ------------------------------------------------------------------
    # ENHANCED public API
    # ------------------------------------------------------------------

    def query(
        self,
        *,
        kind: Optional[str] = None,
        agent_id: Optional[str] = None,
        severity: Optional[str] = None,
    ) -> List[StepRecord]:
        """Return StepRecords matching all provided filters."""
        result: List[StepRecord] = []
        for s in self._steps:
            if kind is not None and s.kind != kind:
                continue
            if agent_id is not None and s.agent_id != agent_id:
                continue
            if severity is not None and s.severity != severity:
                continue
            result.append(s)
        return result

    def filter_by_agent(self, agent_id: str) -> List[StepRecord]:
        return self.query(agent_id=agent_id)

    def filter_by_kind(self, kind: str) -> List[StepRecord]:
        return self.query(kind=kind)

    def last(self, n: int = 1) -> List[StepRecord]:
        """Return the most recent n steps."""
        return list(self._steps)[-n:]

    def summary(self) -> Dict[str, Any]:
        """XAI-style summary of the trace."""
        return TraceExplainer.summarize(list(self._steps))

    def verify_ordering(self) -> Tuple[bool, List[str]]:
        """
        Verify that steps are recorded in non-decreasing timestamp order.

        Returns (ok, violations). Violations is a list of human-readable
        strings describing any ordering issues.
        """
        violations: List[str] = []
        prev_ts: Optional[float] = None
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
    ) -> List[Any]:
        """
        Emit a DecisionRecord for each step.

        Returns an empty list if the shared contract isn't importable.
        """
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

        records: List[Any] = []
        for s in self._steps:
            records.append(DecisionRecord(
                run_id=s.run_id or self.run_id,
                timestamp=datetime.fromtimestamp(s.timestamp),
                task_id=s.task_id or "",
                selected_action=f"{s.kind}:{s.step_id}",
                alternatives=[],
                policy_version=policy_version,
                model_or_agent=s.agent_id or "",
                latency_ms=s.duration_ms or 0.0,
                energy_kwh=float(s.payload.get("energy_kwh", 0.0) or 0.0),
                carbon_operational_kg=float(
                    s.payload.get("carbon_operational_kg", 0.0) or 0.0
                ),
                explanation={
                    "kind": s.kind,
                    "sequence": s.sequence,
                },
                provenance={
                    **s.provenance,
                    "source": "execution_trace",
                    "step_id": s.step_id,
                },
            ))
        return records

    def get_statistics(self) -> Dict[str, Any]:
        """Return summary statistics over the trace."""
        kinds: Counter = Counter(s.kind for s in self._steps)
        agents: Counter = Counter(
            s.agent_id for s in self._steps if s.agent_id
        )
        severities: Counter = Counter(s.severity for s in self._steps)

        total_duration = 0.0
        if self._steps:
            total_duration = max(
                0.0,
                self._steps[-1].timestamp - self._steps[0].timestamp,
            )

        return {
            "run_id": self.run_id,
            "step_count": len(self._steps),
            "dropped_steps": self._dropped_steps,
            "max_steps": self._max_steps,
            "sequence_counter": self._sequence,
            "by_kind": dict(kinds),
            "by_agent": dict(agents),
            "by_severity": dict(severities),
            "total_duration_s": total_duration,
            "ordering_ok": self._last_verify_ok,
        }

    def export_json(self, indent: Optional[int] = 2) -> str:
        """JSON-safe export with ISO timestamps."""
        return json.dumps(
            {
                "run_id": self.run_id,
                "task_id": self.task_id,
                "agent_id": self.agent_id,
                "steps": [s.to_dict() for s in self._steps],
                "summary": self.summary(),
                "statistics": self.get_statistics(),
            },
            indent=indent,
            default=str,
        )

    def clear(self) -> None:
        """Reset the trace to empty (keeps run_id, task_id, agent_id)."""
        self._steps.clear()
        self._sequence = 0
        self._dropped_steps = 0
        logger.debug("ExecutionTrace cleared")

    def iterate(self) -> Iterable[StepRecord]:
        """Iterate over StepRecords (in place of `for s in trace.steps`)."""
        return iter(self._steps)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _infer_kind(event: Dict[str, Any]) -> str:
        """Best-effort classification of a step from its payload."""
        if not isinstance(event, dict):
            return StepKind.UNKNOWN.value
        for key in ("kind", "event_type", "type"):
            v = event.get(key)
            if isinstance(v, str):
                return v
        if "tool" in event or "tool_name" in event:
            return StepKind.TOOL_CALL.value
        if "tool_result" in event or "result" in event and "tool" in event:
            return StepKind.TOOL_RESULT.value
        if "reasoning" in event or "thought" in event:
            return StepKind.REASONING.value
        if "human" in event or "reviewer" in event:
            return StepKind.HUMAN_DECISION.value
        if "chaos_event" in event:
            return StepKind.CHAOS_EVENT.value
        if "error" in event or "exception" in event:
            return StepKind.ERROR.value
        if "precision" in event:
            return StepKind.PRECISION_SWITCH.value
        if "policy" in event:
            return StepKind.POLICY_DECISION.value
        return StepKind.UNKNOWN.value


# =============================================================================
# Demo
# =============================================================================

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    # --- Original behavior (backward compatible) ---
    print("=== Original behavior ===")
    trace = ExecutionTrace()
    trace.record({"action": "start"})
    trace.record({"action": "step_1", "value": 42})
    print(f"  to_dict(): {trace.to_dict()}")

    # --- Enhanced behavior ---
    print("\n=== Enhanced behavior ===")
    trace2 = ExecutionTrace(
        run_id="run-abc123",
        task_id="task-1",
        agent_id="worker-A",
        max_steps=5,
    )

    trace2.record({"thought": "plan the task"}, kind="reasoning")
    trace2.record(
        {"tool": "vision_api", "params": {"image": "x.jpg"}},
        kind="tool_call", agent_id="worker-A",
    )
    trace2.record(
        {"result": "cat detected"}, kind="tool_result",
        agent_id="vision_api", duration_ms=120.0,
    )
    trace2.record(
        {"error": "timeout"}, kind="error",
        severity="critical",
    )
    trace2.record({"human": "approved"}, kind="human_decision")
    trace2.record({"extra": "would be dropped"})  # exceeds max_steps=5

    # Query API
    print(f"  tool calls: {len(trace2.filter_by_kind('tool_call'))}")
    print(f"  worker-A steps: {len(trace2.filter_by_agent('worker-A'))}")
    print(f"  critical severity: {len(trace2.query(severity='critical'))}")

    # XAI summary
    print("\n=== Summary ===")
    s = trace2.summary()
    print(f"  headline: {s['headline']}")
    for r in s["rationale"]:
        print(f"    • {r}")

    # Ordering check
    ok, violations = trace2.verify_ordering()
    print(f"\n  ordering_ok: {ok}, violations: {violations}")

    # DecisionRecords
    print("\n=== DecisionRecords ===")
    try:
        records = trace2.to_decision_records(policy_version="v5.0.1")
        print(f"  emitted: {len(records)} records")
        for r in records[:2]:
            print(f"    {r.selected_action}")
    except Exception as e:
        print(f"  (DecisionRecord unavailable: {e})")

    # Statistics
    import json
    print("\n=== Statistics ===")
    print(json.dumps(trace2.get_statistics(), indent=2, default=str))

    # JSON export
    print("\n=== JSON export (first 400 chars) ===")
    print(trace2.export_json()[:400] + "...")
