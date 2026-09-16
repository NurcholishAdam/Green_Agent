# src/analysis/adapters/langchain_callbacks.py

"""
LangChain runtime callback adapter for Green Agent (Enhanced)
=============================================================

Collects real LangChain runtime metrics and emits them as the analysis
layer's shared contracts (`StepRecord`, `AgentObservation`).

Original API preserved:
    cb = GreenLangChainCallback()
    cb.snapshot()   # {"tool_calls": int, "llm_calls": int}

Enhanced API:
    cb = GreenLangChainCallback(run_id="...", agent_id="...",
                                trace=trace, emit_observations=True)
    cb.steps()             # list of step dicts
    cb.observations()      # list of AgentObservation
    cb.errors()            # list of error records
    cb.statistics()        # rich statistics
    cb.export()            # full serialisable snapshot
"""

from __future__ import annotations

import logging
import threading
import time
import uuid
from collections import Counter, defaultdict, deque
from dataclasses import dataclass, field, asdict
from datetime import datetime
from typing import Any, Deque, Dict, List, Optional

logger = logging.getLogger(__name__)


# =============================================================================
# Optional contract imports (soft dependency on analysis package)
# =============================================================================

def _try_import_observation():
    try:
        from src.analysis import AgentObservation  # type: ignore
        return AgentObservation
    except Exception:
        try:
            from analysis import AgentObservation  # type: ignore
            return AgentObservation
        except Exception:
            return None


_AgentObservation = _try_import_observation()


# =============================================================================
# StepRecord — normalized callback event
# =============================================================================

@dataclass
class CallbackStep:
    """A single LangChain lifecycle event with full provenance."""
    step_id: str
    sequence: int
    kind: str               # "tool_start" | "tool_end" | "llm_start" | ...
    timestamp: float
    run_id: Optional[str] = None
    parent_run_id: Optional[str] = None
    agent_id: Optional[str] = None
    task_id: Optional[str] = None
    name: Optional[str] = None            # tool name / chain name / model name
    duration_ms: Optional[float] = None
    success: Optional[bool] = None
    error: Optional[str] = None
    tokens: Optional[int] = None
    prompt_tokens: Optional[int] = None
    completion_tokens: Optional[int] = None
    payload: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        out = asdict(self)
        out["at_iso"] = datetime.fromtimestamp(self.timestamp).isoformat()
        return out


# =============================================================================
# ORIGINAL + ENHANCED callback
# =============================================================================

class GreenLangChainCallback:
    """
    LangChain callback adapter.

    Backward-compatible: `snapshot()` always returns a dict containing
    `tool_calls` and `llm_calls` as ints.

    Enhanced: also emits `CallbackStep` records, `AgentObservation`s
    (when the analysis contract is importable), and error records.
    """

    MAX_STEPS = 10_000

    def __init__(
        self,
        *,
        run_id: Optional[str] = None,
        task_id: Optional[str] = None,
        agent_id: Optional[str] = None,
        trace: Any = None,                 # optional ExecutionTrace sink
        emit_observations: bool = True,
        emit_trace: bool = True,
        capture_tokens: bool = True,
        capture_errors: bool = True,
    ):
        # --- Original state ---
        self.tool_calls: int = 0
        self.llm_calls: int = 0

        # --- Enhancement state ---
        self.run_id = run_id or f"lc-{uuid.uuid4().hex[:8]}"
        self.task_id = task_id
        self.agent_id = agent_id
        self.trace = trace
        self.emit_observations = emit_observations
        self.emit_trace = emit_trace
        self.capture_tokens = capture_tokens
        self.capture_errors = capture_errors

        # --- Thread safety ---
        self._lock = threading.RLock()

        # --- Sequential ID & step history ---
        self._sequence = 0
        self._steps: Deque[CallbackStep] = deque(maxlen=self.MAX_STEPS)

        # --- Open-run tracking: run_id -> {start_time, kind, name} ---
        self._open_runs: Dict[str, Dict[str, Any]] = {}

        # --- Tool call counts by name ---
        self._tool_calls_by_name: Counter = Counter()
        self._llm_calls_by_model: Counter = Counter()

        # --- Token aggregation ---
        self._prompt_tokens_total = 0
        self._completion_tokens_total = 0

        # --- Error tracking ---
        self._errors: Deque[Dict[str, Any]] = deque(maxlen=256)

        # --- Observation accumulation (agent -> counters) ---
        self._agent_calls: Counter = Counter()
        self._agent_latency_ms: Dict[str, float] = defaultdict(float)
        self._agent_errors: Counter = Counter()

        # --- Statistics ---
        self._started_at = time.time()
        self._by_kind: Counter = Counter()

    # ------------------------------------------------------------------
    # ORIGINAL callbacks (preserved verbatim)
    # ------------------------------------------------------------------

    def on_tool_start(
        self,
        serialized: Dict[str, Any],
        input_str: str,
        *,
        run_id: Optional[str] = None,
        parent_run_id: Optional[str] = None,
        **kwargs: Any,
    ) -> None:
        """Original behavior: `self.tool_calls += 1`."""
        self.tool_calls += 1

        # --- Enhancement: record step ---
        with self._lock:
            self._sequence += 1
            name = self._extract_tool_name(serialized)
            self._tool_calls_by_name[name] += 1
            if run_id is not None:
                self._open_runs[run_id] = {
                    "kind": "tool", "name": name,
                    "start_time": time.time(),
                }
            step = CallbackStep(
                step_id=uuid.uuid4().hex[:12],
                sequence=self._sequence,
                kind="tool_start",
                timestamp=time.time(),
                run_id=run_id,
                parent_run_id=parent_run_id,
                agent_id=self.agent_id,
                task_id=self.task_id,
                name=name,
                payload={"input_len": len(input_str or "")},
            )
            self._record_step(step)

    def on_llm_start(
        self,
        serialized: Dict[str, Any],
        prompts: List[str],
        *,
        run_id: Optional[str] = None,
        parent_run_id: Optional[str] = None,
        **kwargs: Any,
    ) -> None:
        """Original behavior: `self.llm_calls += 1`."""
        self.llm_calls += 1

        # --- Enhancement: record step ---
        with self._lock:
            self._sequence += 1
            model = self._extract_llm_name(serialized)
            self._llm_calls_by_model[model] += 1
            if run_id is not None:
                self._open_runs[run_id] = {
                    "kind": "llm", "name": model,
                    "start_time": time.time(),
                }
            step = CallbackStep(
                step_id=uuid.uuid4().hex[:12],
                sequence=self._sequence,
                kind="llm_start",
                timestamp=time.time(),
                run_id=run_id,
                parent_run_id=parent_run_id,
                agent_id=self.agent_id,
                task_id=self.task_id,
                name=model,
                payload={"n_prompts": len(prompts or [])},
            )
            self._record_step(step)

    def snapshot(self) -> Dict[str, int]:
        """
        Backward-compatible snapshot.

        Original keys: `tool_calls`, `llm_calls`. Additional keys are
        additive.
        """
        with self._lock:
            return {
                "tool_calls": self.tool_calls,
                "llm_calls": self.llm_calls,
                # Enhancement additions
                "prompt_tokens": self._prompt_tokens_total,
                "completion_tokens": self._completion_tokens_total,
                "total_tokens": (
                    self._prompt_tokens_total + self._completion_tokens_total
                ),
                "errors": len(self._errors),
                "steps_recorded": len(self._steps),
            }

    # ------------------------------------------------------------------
    # ENHANCED callbacks — end events
    # ------------------------------------------------------------------

    def on_tool_end(
        self,
        output: Any,
        *,
        run_id: Optional[str] = None,
        parent_run_id: Optional[str] = None,
        **kwargs: Any,
    ) -> None:
        """Record tool-call completion with duration."""
        with self._lock:
            self._sequence += 1
            duration_ms = self._close_run(run_id, "tool")
            step = CallbackStep(
                step_id=uuid.uuid4().hex[:12],
                sequence=self._sequence,
                kind="tool_end",
                timestamp=time.time(),
                run_id=run_id,
                parent_run_id=parent_run_id,
                agent_id=self.agent_id,
                task_id=self.task_id,
                duration_ms=duration_ms,
                success=True,
                payload={"output_len": len(str(output or ""))},
            )
            self._record_step(step)
            if self.agent_id:
                self._agent_calls[self.agent_id] += 1
                if duration_ms:
                    self._agent_latency_ms[self.agent_id] += duration_ms

    def on_tool_error(
        self,
        error: BaseException,
        *,
        run_id: Optional[str] = None,
        parent_run_id: Optional[str] = None,
        **kwargs: Any,
    ) -> None:
        """Record tool-call error."""
        if not self.capture_errors:
            return
        with self._lock:
            self._sequence += 1
            duration_ms = self._close_run(run_id, "tool")
            error_text = self._error_to_text(error)
            self._errors.append({
                "kind": "tool_error",
                "at": datetime.now().isoformat(),
                "run_id": run_id,
                "error": error_text,
            })
            step = CallbackStep(
                step_id=uuid.uuid4().hex[:12],
                sequence=self._sequence,
                kind="tool_error",
                timestamp=time.time(),
                run_id=run_id,
                parent_run_id=parent_run_id,
                agent_id=self.agent_id,
                task_id=self.task_id,
                duration_ms=duration_ms,
                success=False,
                error=error_text,
            )
            self._record_step(step)
            if self.agent_id:
                self._agent_errors[self.agent_id] += 1

    def on_llm_end(
        self,
        response: Any,
        *,
        run_id: Optional[str] = None,
        parent_run_id: Optional[str] = None,
        **kwargs: Any,
    ) -> None:
        """
        Record LLM completion with duration AND token usage.

        This is where the original docstring's "token usage" claim is
        finally honored — tokens are read from `response.llm_output`.
        """
        with self._lock:
            self._sequence += 1
            duration_ms = self._close_run(run_id, "llm")
            tokens = self._extract_tokens(response) if self.capture_tokens else {}
            self._prompt_tokens_total += tokens.get("prompt_tokens", 0)
            self._completion_tokens_total += tokens.get("completion_tokens", 0)
            step = CallbackStep(
                step_id=uuid.uuid4().hex[:12],
                sequence=self._sequence,
                kind="llm_end",
                timestamp=time.time(),
                run_id=run_id,
                parent_run_id=parent_run_id,
                agent_id=self.agent_id,
                task_id=self.task_id,
                duration_ms=duration_ms,
                success=True,
                tokens=tokens.get("total_tokens"),
                prompt_tokens=tokens.get("prompt_tokens"),
                completion_tokens=tokens.get("completion_tokens"),
            )
            self._record_step(step)
            if self.agent_id:
                self._agent_calls[self.agent_id] += 1
                if duration_ms:
                    self._agent_latency_ms[self.agent_id] += duration_ms

    def on_llm_error(
        self,
        error: BaseException,
        *,
        run_id: Optional[str] = None,
        parent_run_id: Optional[str] = None,
        **kwargs: Any,
    ) -> None:
        """Record LLM error."""
        if not self.capture_errors:
            return
        with self._lock:
            self._sequence += 1
            duration_ms = self._close_run(run_id, "llm")
            error_text = self._error_to_text(error)
            self._errors.append({
                "kind": "llm_error",
                "at": datetime.now().isoformat(),
                "run_id": run_id,
                "error": error_text,
            })
            step = CallbackStep(
                step_id=uuid.uuid4().hex[:12],
                sequence=self._sequence,
                kind="llm_error",
                timestamp=time.time(),
                run_id=run_id,
                parent_run_id=parent_run_id,
                agent_id=self.agent_id,
                task_id=self.task_id,
                duration_ms=duration_ms,
                success=False,
                error=error_text,
            )
            self._record_step(step)
            if self.agent_id:
                self._agent_errors[self.agent_id] += 1

    # ------------------------------------------------------------------
    # ENHANCED public API
    # ------------------------------------------------------------------

    def steps(self) -> List[Dict[str, Any]]:
        """Return all recorded steps as dicts."""
        with self._lock:
            return [s.to_dict() for s in self._steps]

    def errors(self) -> List[Dict[str, Any]]:
        """Return all error records."""
        with self._lock:
            return list(self._errors)

    def observations(self) -> List[Any]:
        """
        Emit `AgentObservation`s for `MultiAgentRoleAnalyzer`.

        One observation per (agent_id, task_id) pair with accumulated
        coordination calls and latency. Returns [] if the contract is not
        importable.
        """
        if _AgentObservation is None or not self.emit_observations:
            return []
        obs: List[Any] = []
        for agent_id, calls in self._agent_calls.items():
            obs.append(_AgentObservation(
                agent_id=agent_id,
                task_id=self.task_id or "",
                role="generalist",
                delegated_to=[],
                coordination_calls=calls,
                latency_ms=self._agent_latency_ms.get(agent_id, 0.0),
                energy_kwh=0.0,  # filled by EnergyMeter if wired
            ))
        return obs

    def statistics(self) -> Dict[str, Any]:
        """Rich statistics for this callback session."""
        with self._lock:
            return {
                "run_id": self.run_id,
                "agent_id": self.agent_id,
                "task_id": self.task_id,
                "tool_calls": self.tool_calls,
                "llm_calls": self.llm_calls,
                "tool_calls_by_name": dict(self._tool_calls_by_name),
                "llm_calls_by_model": dict(self._llm_calls_by_model),
                "prompt_tokens": self._prompt_tokens_total,
                "completion_tokens": self._completion_tokens_total,
                "total_tokens": (
                    self._prompt_tokens_total + self._completion_tokens_total
                ),
                "errors_count": len(self._errors),
                "steps_recorded": len(self._steps),
                "elapsed_s": time.time() - self._started_at,
            }

    def export(self) -> Dict[str, Any]:
        """Fully serialisable snapshot for JSON export."""
        return {
            "statistics": self.statistics(),
            "steps": self.steps(),
            "errors": self.errors(),
        }

    def reset(self) -> None:
        """Reset all counters and history (keeps run_id, agent_id)."""
        with self._lock:
            self.tool_calls = 0
            self.llm_calls = 0
            self._steps.clear()
            self._errors.clear()
            self._open_runs.clear()
            self._tool_calls_by_name.clear()
            self._llm_calls_by_model.clear()
            self._prompt_tokens_total = 0
            self._completion_tokens_total = 0
            self._agent_calls.clear()
            self._agent_latency_ms.clear()
            self._agent_errors.clear()
            self._started_at = time.time()

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _record_step(self, step: CallbackStep) -> None:
        self._steps.append(step)
        self._by_kind[step.kind] += 1
        # Forward to ExecutionTrace if wired
        if self.emit_trace and self.trace is not None:
            try:
                self.trace.record(
                    step.payload,
                    kind=step.kind,
                    agent_id=step.agent_id,
                    task_id=step.task_id,
                    run_id=step.run_id,
                    duration_ms=step.duration_ms,
                    timestamp=step.timestamp,
                    severity="error" if step.success is False else "info",
                )
            except Exception as e:
                logger.debug(f"Trace forwarding failed: {e}")

    def _close_run(
        self, run_id: Optional[str], expected_kind: str,
    ) -> Optional[float]:
        """Return duration in ms if the run was open; else None."""
        if run_id is None or run_id not in self._open_runs:
            return None
        info = self._open_runs.pop(run_id)
        if info.get("kind") != expected_kind:
            # Unexpected close; still compute duration
            pass
        start = info.get("start_time")
        if start is None:
            return None
        return max(0.0, (time.time() - start) * 1000.0)

    @staticmethod
    def _extract_tool_name(serialized: Dict[str, Any]) -> str:
        if not isinstance(serialized, dict):
            return "unknown"
        # LangChain variants: {"name": ...}, {"id": [...]}
        name = serialized.get("name")
        if isinstance(name, str) and name:
            return name
        sid = serialized.get("id")
        if isinstance(sid, list) and sid:
            return str(sid[-1])
        return "unknown"

    @staticmethod
    def _extract_llm_name(serialized: Dict[str, Any]) -> str:
        if not isinstance(serialized, dict):
            return "unknown"
        name = serialized.get("name")
        if isinstance(name, str) and name:
            return name
        sid = serialized.get("id")
        if isinstance(sid, list) and sid:
            return str(sid[-1])
        return "unknown"

    @staticmethod
    def _extract_tokens(response: Any) -> Dict[str, int]:
        """
        Best-effort token extraction from an LLMResult-like object.

        Looks for `response.llm_output["token_usage"]` or
        `response.llm_output["usage"]` which is the LangChain convention.
        """
        out: Dict[str, int] = {}
        try:
            llm_output = getattr(response, "llm_output", None) or {}
            if not isinstance(llm_output, dict):
                return out
            usage = (
                llm_output.get("token_usage")
                or llm_output.get("usage")
                or {}
            )
            if not isinstance(usage, dict):
                return out
            for src, dst in (
                ("prompt_tokens", "prompt_tokens"),
                ("completion_tokens", "completion_tokens"),
                ("total_tokens", "total_tokens"),
            ):
                if src in usage:
                    try:
                        out[dst] = int(usage[src])
                    except (TypeError, ValueError):
                        pass
            # Fallback: compute total if only prompt+completion present
            if "total_tokens" not in out and "prompt_tokens" in out and "completion_tokens" in out:
                out["total_tokens"] = out["prompt_tokens"] + out["completion_tokens"]
        except Exception:
            pass
        return out

    @staticmethod
    def _error_to_text(error: Any) -> str:
        try:
            return f"{type(error).__name__}: {error}"
        except Exception:
            return "unknown_error"


# =============================================================================
# Demo
# =============================================================================

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    # --- Original behavior (backward compatible) ---
    print("=== Original behavior ===")
    cb = GreenLangChainCallback()
    cb.on_tool_start({"name": "web_search"}, "query")
    cb.on_llm_start({"name": "gpt-4"}, ["prompt"])
    print(f"  snapshot(): {cb.snapshot()}")

    # --- Enhanced behavior ---
    print("\n=== Enhanced behavior ===")
    cb2 = GreenLangChainCallback(
        run_id="run-001",
        task_id="task-1",
        agent_id="agent-A",
    )

    # Tool call lifecycle
    cb2.on_tool_start(
        {"name": "web_search"}, "query",
        run_id="tool-1",
    )
    time.sleep(0.01)
    cb2.on_tool_end("result", run_id="tool-1")

    # LLM call lifecycle with tokens
    cb2.on_llm_start(
        {"name": "gpt-4"}, ["prompt"],
        run_id="llm-1",
    )
    time.sleep(0.02)

    class _FakeLLMResult:
        llm_output = {
            "token_usage": {
                "prompt_tokens": 120,
                "completion_tokens": 45,
                "total_tokens": 165,
            }
        }

    cb2.on_llm_end(_FakeLLMResult(), run_id="llm-1")

    # Error case
    cb2.on_llm_start({"name": "gpt-4"}, ["prompt"], run_id="llm-2")
    cb2.on_llm_error(
        RuntimeError("rate limit"),
        run_id="llm-2",
    )

    # Snapshot
    print(f"\n  snapshot(): {cb2.snapshot()}")

    # Statistics
    import json
    print("\n=== Statistics ===")
    print(json.dumps(cb2.statistics(), indent=2))

    # Steps
    print("\n=== Steps ===")
    for s in cb2.steps():
        print(f"  [{s['kind']:12s}] name={s.get('name')} "
              f"duration_ms={s.get('duration_ms')} "
              f"tokens={s.get('tokens')}")

    # Observations
    print("\n=== Observations ===")
    for o in cb2.observations():
        print(f"  {o}")

    # Errors
    print("\n=== Errors ===")
    for e in cb2.errors():
        print(f"  {e}")

    # --- Thread safety sanity check ---
    print("\n=== Thread safety ===")
    import threading

    cb3 = GreenLangChainCallback()
    def worker():
        for _ in range(100):
            cb3.on_tool_start({"name": "t"}, "i")
            cb3.on_llm_start({"name": "m"}, ["p"])

    threads = [threading.Thread(target=worker) for _ in range(4)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    print(f"  tool_calls={cb3.tool_calls} (expected 400)")
    print(f"  llm_calls={cb3.llm_calls} (expected 400)")
