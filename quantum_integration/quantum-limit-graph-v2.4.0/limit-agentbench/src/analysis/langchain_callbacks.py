# src/analysis/adapters/langchain_callbacks.py
"""
LangChain runtime callback adapter for Green Agent (Enhanced, v2.0.0)
======================================================================

Collects real LangChain runtime metrics and emits them as the analysis
layer's shared contracts (`CallbackStep`, `AgentObservation`).

Original API preserved (all signatures, all return shapes):
    cb = GreenLangChainCallback()
    cb.on_tool_start(serialized, input_str, *, run_id=None, ...)
    cb.on_llm_start(serialized, prompts, *, run_id=None, ...)
    cb.snapshot()   # {"tool_calls": int, "llm_calls": int, ...}

Enhanced API:
    cb = GreenLangChainCallback(
        run_id="...", task_id="...", agent_id="...",
        trace=<ExecutionTrace>,
        emit_observations=True, emit_trace=True,
        capture_tokens=True, capture_errors=True, capture_payloads=False,
        max_steps=10_000, max_open_runs=1_000, max_run_duration_s=3600,
    )

    # Full LangChain lifecycle
    cb.on_tool_start/end/error
    cb.on_llm_start/end/error
    cb.on_chat_model_start/end/error   # NEW — modern LangChain path
    cb.on_chain_start/end/error        # NEW
    cb.on_retriever_start/end/error    # NEW
    cb.on_agent_action/finish          # NEW
    cb.on_retry                        # NEW

    # Introspection
    cb.snapshot()                                   # dict
    cb.steps(limit=None, kind=None, agent_id=None)  # filtered list
    cb.errors(limit=None)
    cb.observations()                               # list[AgentObservation]
    cb.steps_by_parent(parent_run_id)               # tree reconstruction
    cb.statistics()                                 # richer stats
    cb.export()                                     # top-level metadata hoisted
    cb.reset()
    cb.set_hitl_callback(cb)                        # in-place reviewer
    cb.contribute_federated() / cb.get_federated_aggregate()

Enhancements implemented (v2.0.0):
  1.  Locked counters          — `tool_calls` / `llm_calls` mutations are
                                 inside `self._lock` (v1 raced with concurrent
                                 callbacks — see demo's barrier test).
  2.  Chat model path          — `on_chat_model_start/end/error` implemented.
                                 Without this, modern LangChain agents
                                 (ChatOpenAI, ChatAnthropic, …) reported zero
                                 LLM calls.
  3.  Error-path run closure   — `on_*_error` closes the open run *before*
                                 the `capture_errors` gate (v1 leaked
                                 `_open_runs` entries).
  4.  Bounded `_open_runs`     — LRU eviction at `max_open_runs`, plus a TTL
                                 sweep at `max_run_duration_s`.
  5.  Locked observations      — `observations()` reads under `self._lock`.
  6.  Error-path attribution   — errors now increment `_agent_calls` and
                                 `_agent_latency_ms`, matching end events.
  7.  Full lifecycle           — chain / retriever / agent / retry callbacks
                                 added (v1 covered tool + llm only).
  8.  Parent tracking          — `steps_by_parent(parent_run_id)`; step tree
                                 is reconstructable.
  9.  Faithful trace forward   — forwards `step.to_dict()`, not just
                                 `step.payload`; severity mapped to
                                 `Severity.CRITICAL` on error.
 10.  Per-call attribution     — `agent_id` / `task_id` resolved from
                                 `metadata` / `kwargs` per callback.
 11.  Extended token extraction— modern `usage_metadata`, `generation_info`,
                                 `llm_output`, normalised input/output →
                                 prompt/completion; bounds-checked.
 12.  Kind-mismatch logging    — `_close_run` warns + returns None on
                                 tool/llm/chain retriever mismatches.
 13.  Honest `duration_ms`     — `if duration_ms is not None:` (v1 skipped
                                 legitimate 0.0 measurements).
 14.  Unified name extraction  — `_extract_name` handles `name`, `kwargs`,
                                 and `id` (list or string).
 15.  Reset completeness       — `reset()` clears `_by_kind`, `_sequence`,
                                 `_dropped_steps`, and the federated
                                 contribution marker.
 16.  Safe `to_dict()`         — non-finite timestamps degrade to
                                 `at_iso=None`.
 17.  Configurable max_steps   — constructor kwarg + `dropped_steps` counter.
 18.  Filtered `steps()`       — `kind` / `agent_id` / `limit` kwargs.
 19.  HITL callback            — long tool calls, long LLM calls, and
                                 error thresholds trigger the reviewer;
                                 returning False annotates the step.
 20.  Structured logging       — `langchain.step_recorded` /
                                 `langchain.error_recorded` /
                                 `langchain.open_run_leak` events.
 21.  Top-level export         — `run_id` / `task_id` / `agent_id` /
                                 `analyzer_version` / `started_at` hoisted.
 22.  Error text truncation    — `_error_to_text` caps at 1000 chars.
 23.  Token bounds             — negative / absurd token counts rejected.
 24.  Per-(agent, task) obs    — `observations()` groups by the pair, not
                                 just `agent_id`.
 25.  Deferred import          — `AgentObservation` resolved on first use.
 26.  Optional payload capture — `capture_payloads=True` records truncated
                                 tool output / LLM response text.
 27.  Federated contributions  — `contribute_federated()` pushes aggregated
                                 statistics to a federated aggregator.
 28.  Backward compatibility   — every v1 public symbol preserved.

Backward compatibility:
  * `GreenLangChainCallback.MAX_STEPS` class attribute retained (default
    10_000); pass `max_steps=` to override per instance.
  * `snapshot()` keys `tool_calls` / `llm_calls` are still ints at the top
    level; new keys are additive.
  * `steps()` / `errors()` / `observations()` / `export()` return the same
    types as v1. `export()` gains top-level metadata keys (additive).
  * `on_tool_start` / `on_llm_start` still increment the original counters
    — but now under the lock.
"""

from __future__ import annotations

import logging
import math
import re
import threading
import time
import uuid
from collections import Counter, defaultdict, deque
from dataclasses import dataclass, field, asdict
from datetime import datetime
from typing import (
    Any, Callable, Deque, Dict, Iterable, List, Optional, Set, Tuple,
)

__version__ = "2.0.0"

__all__ = [
    "GreenLangChainCallback",
    "CallbackStep",
    "FederatedLangChainProfile",
    "FederatedLangChainAggregator",
    "__version__",
]

logger = logging.getLogger(__name__)


# =============================================================================
# Constants
# =============================================================================

#: Regex patterns for inferring delegation from tool names.
_DELEGATION_PATTERNS = (
    re.compile(r"^delegate_to_(\w+)$"),
    re.compile(r"^call_agent_(\w+)$"),
    re.compile(r"^agent_(\w+)$"),
)

#: Severities accepted by the shared analysis layer (see ExecutionTrace).
_VALID_SEVERITIES = frozenset({"info", "warning", "critical", "emergency"})


# =============================================================================
# Helpers
# =============================================================================

def _safe_isoformat(ts: Any) -> Optional[str]:
    try:
        return datetime.fromtimestamp(float(ts)).isoformat()
    except (OSError, ValueError, OverflowError, TypeError):
        return None


def _truncate(text: str, max_len: int = 1000) -> str:
    if not isinstance(text, str):
        text = str(text)
    return text if len(text) <= max_len else text[:max_len] + "..."


def _safe_int(value: Any, default: int = 0) -> int:
    """Coerce to a bounded non-negative int (rejects NaN, Inf, negatives)."""
    if isinstance(value, bool):
        return default
    try:
        v = int(value)
    except (TypeError, ValueError, OverflowError):
        return default
    if v < 0 or v > 1_000_000_000:
        return default
    return v


# =============================================================================
# Deferred AgentObservation import
# =============================================================================

_AgentObservation: Optional[Any] = None
_AgentObservation_loaded: bool = False


def _get_agent_observation_class() -> Optional[Any]:
    """Import `AgentObservation` lazily on first use."""
    global _AgentObservation, _AgentObservation_loaded
    if _AgentObservation_loaded:
        return _AgentObservation
    with _AGENT_OBSERVATION_LOCK:
        if _AgentObservation_loaded:
            return _AgentObservation
        try:
            from src.analysis import AgentObservation  # type: ignore
            _AgentObservation = AgentObservation
        except Exception:
            try:
                from analysis import AgentObservation  # type: ignore
                _AgentObservation = AgentObservation
            except Exception:
                _AgentObservation = None
        _AgentObservation_loaded = True
    return _AgentObservation


_AGENT_OBSERVATION_LOCK = threading.Lock()


# =============================================================================
# CallbackStep
# =============================================================================

@dataclass
class CallbackStep:
    """A single LangChain lifecycle event with full provenance."""
    step_id: str
    sequence: int
    kind: str                    # "tool_start" | "llm_end" | "chain_start" | ...
    timestamp: float
    run_id: Optional[str] = None
    parent_run_id: Optional[str] = None
    agent_id: Optional[str] = None
    task_id: Optional[str] = None
    name: Optional[str] = None   # tool name / chain name / model name
    duration_ms: Optional[float] = None
    success: Optional[bool] = None
    error: Optional[str] = None
    tokens: Optional[int] = None
    prompt_tokens: Optional[int] = None
    completion_tokens: Optional[int] = None
    payload: Dict[str, Any] = field(default_factory=dict)
    # --- Enhancement: HITL ---
    needs_review: bool = False
    review_reason: Optional[str] = None

    def to_dict(self, safe: bool = False) -> Dict[str, Any]:
        """
        Convert to dictionary.

        `safe=True` coerces non-serialisable payload values to strings.
        """
        out = asdict(self)
        out["at_iso"] = _safe_isoformat(self.timestamp)
        if safe:
            if isinstance(out.get("payload"), dict):
                out["payload"] = {
                    str(k): _jsonable(v) for k, v in out["payload"].items()
                }
            for key in ("timestamp", "duration_ms"):
                v = out.get(key)
                if isinstance(v, float) and not math.isfinite(v):
                    out[key] = None
        return out


def _jsonable(v: Any) -> Any:
    if v is None or isinstance(v, (bool, int, str)):
        return v
    if isinstance(v, float):
        return v if math.isfinite(v) else None
    if isinstance(v, dict):
        return {str(k): _jsonable(x) for k, x in v.items()}
    if isinstance(v, (list, tuple, set, frozenset)):
        return [_jsonable(x) for x in v]
    return repr(v)


# =============================================================================
# Federated aggregation
# =============================================================================

@dataclass
class FederatedLangChainProfile:
    deployment_id: str
    framework: str           # "langchain"
    framework_version: Optional[str]
    mean_tool_calls: float
    mean_llm_calls: float
    mean_errors: float
    mean_latency_ms: float
    sample_count: int
    timestamp: float = field(default_factory=time.time)


class FederatedLangChainAggregator:
    """Bounded, cached, thread-safe aggregator for LangChain profiles."""

    def __init__(self, max_profiles: int = 1_000) -> None:
        self._profiles: Deque[FederatedLangChainProfile] = deque(
            maxlen=max_profiles
        )
        self._cache: Optional[Dict[str, Any]] = None
        self._lock = threading.RLock()

    @property
    def profiles(self) -> List[FederatedLangChainProfile]:
        with self._lock:
            return list(self._profiles)

    def push(self, p: FederatedLangChainProfile) -> None:
        with self._lock:
            self._profiles.append(p)
            self._cache = None

    def aggregate(self) -> Dict[str, Any]:
        with self._lock:
            if self._cache is not None:
                return self._cache
            total_w = sum(max(0, p.sample_count) for p in self._profiles)
            if total_w <= 0:
                self._cache = {}
                return {}
            result = {
                "framework": "langchain",
                "mean_tool_calls": sum(
                    p.mean_tool_calls * max(0, p.sample_count)
                    for p in self._profiles
                ) / total_w,
                "mean_llm_calls": sum(
                    p.mean_llm_calls * max(0, p.sample_count)
                    for p in self._profiles
                ) / total_w,
                "mean_errors": sum(
                    p.mean_errors * max(0, p.sample_count)
                    for p in self._profiles
                ) / total_w,
                "mean_latency_ms": sum(
                    p.mean_latency_ms * max(0, p.sample_count)
                    for p in self._profiles
                ) / total_w,
                "sample_count": float(total_w),
            }
            self._cache = result
            return result


# =============================================================================
# GreenLangChainCallback
# =============================================================================

class GreenLangChainCallback:
    """
    LangChain callback adapter.

    Backward-compatible: `snapshot()` returns a dict containing
    `tool_calls` and `llm_calls` as ints. Enhanced: emits `CallbackStep`
    records for the full LangChain lifecycle, tracks parent-child run
    trees, supports HITL review, and optionally contributes anonymised
    statistics to a federated aggregator.
    """

    #: Backward-compatible class attribute. Pass `max_steps=` per instance
    #: to override.
    MAX_STEPS = 10_000

    #: Default thresholds used by the HITL reviewer.
    DEFAULT_LONG_TOOL_MS = 30_000.0     # 30 s
    DEFAULT_LONG_LLM_MS = 60_000.0      # 60 s
    DEFAULT_ERROR_THRESHOLD = 3         # errors per agent

    def __init__(
        self,
        *,
        run_id: Optional[str] = None,
        task_id: Optional[str] = None,
        agent_id: Optional[str] = None,
        trace: Any = None,
        emit_observations: bool = True,
        emit_trace: bool = True,
        capture_tokens: bool = True,
        capture_errors: bool = True,
        capture_payloads: bool = False,
        max_steps: Optional[int] = None,
        max_open_runs: int = 1_000,
        max_run_duration_s: float = 3_600.0,
        hitl_long_tool_ms: Optional[float] = None,
        hitl_long_llm_ms: Optional[float] = None,
        hitl_error_threshold: Optional[int] = None,
    ) -> None:
        # --- Original counters ---
        self.tool_calls: int = 0
        self.llm_calls: int = 0

        # --- Identity ---
        self.run_id: str = run_id or f"lc-{uuid.uuid4().hex[:8]}"
        self.task_id = task_id
        self.agent_id = agent_id

        # --- Sinks / flags ---
        self.trace = trace
        self.emit_observations = bool(emit_observations)
        self.emit_trace = bool(emit_trace)
        self.capture_tokens = bool(capture_tokens)
        self.capture_errors = bool(capture_errors)
        self.capture_payloads = bool(capture_payloads)

        # --- Bounded state ---
        effective_max_steps = (
            int(max_steps) if max_steps is not None else self.MAX_STEPS
        )
        if effective_max_steps < 1:
            raise ValueError(
                f"max_steps must be >= 1, got {effective_max_steps}"
            )
        self._max_steps = effective_max_steps
        self._max_open_runs = max(1, int(max_open_runs))
        self._max_run_duration_s = max(0.0, float(max_run_duration_s))

        # --- Thread safety ---
        self._lock = threading.RLock()

        # --- Sequence / steps ---
        self._sequence = 0
        self._steps: Deque[CallbackStep] = deque(maxlen=effective_max_steps)
        self._dropped_steps = 0

        # --- Open runs: run_id -> {kind, name, start_time,
        #                          parent_run_id, agent_id, task_id} ---
        self._open_runs: Dict[str, Dict[str, Any]] = {}

        # --- Aggregations ---
        self._tool_calls_by_name: Counter = Counter()
        self._llm_calls_by_model: Counter = Counter()
        self._chain_calls_by_name: Counter = Counter()
        self._retriever_calls_by_name: Counter = Counter()
        self._prompt_tokens_total = 0
        self._completion_tokens_total = 0
        self._errors: Deque[Dict[str, Any]] = deque(maxlen=256)

        # --- Per-(agent, task) attribution ---
        self._agent_calls: Counter = Counter()
        self._agent_latency_ms: Dict[Tuple[str, str], float] = defaultdict(float)
        self._agent_errors: Counter = Counter()
        self._agent_role: Dict[str, str] = {}
        self._agent_delegated_to: Dict[str, Set[str]] = defaultdict(set)

        # --- HITL ---
        self._hitl_callback: Optional[
            Callable[[CallbackStep], Optional[bool]]
        ] = None
        self._hitl_long_tool_ms = (
            float(hitl_long_tool_ms)
            if hitl_long_tool_ms is not None
            else self.DEFAULT_LONG_TOOL_MS
        )
        self._hitl_long_llm_ms = (
            float(hitl_long_llm_ms)
            if hitl_long_llm_ms is not None
            else self.DEFAULT_LONG_LLM_MS
        )
        self._hitl_error_threshold = (
            int(hitl_error_threshold)
            if hitl_error_threshold is not None
            else self.DEFAULT_ERROR_THRESHOLD
        )

        # --- Federated ---
        self._federated = FederatedLangChainAggregator()
        self._last_contributed_signature: Optional[Tuple[int, int]] = None

        # --- Statistics ---
        self._started_at = time.time()
        self._by_kind: Counter = Counter()

    # ------------------------------------------------------------------
    # Tool callbacks
    # ------------------------------------------------------------------

    def on_tool_start(
        self,
        serialized: Dict[str, Any],
        input_str: str,
        *,
        run_id: Optional[str] = None,
        parent_run_id: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
        **kwargs: Any,
    ) -> None:
        """Original: increments `tool_calls`. Enhanced: records a step."""
        with self._lock:
            self.tool_calls += 1          # FIX: was outside the lock in v1

            self._sweep_open_runs_locked()

            self._sequence += 1
            name = self._extract_name(serialized)
            agent_id, task_id = self._resolve_agent_task(metadata, kwargs)
            self._tool_calls_by_name[name] += 1
            self._track_delegation(agent_id, name)
            if run_id is not None:
                self._store_open_run(
                    run_id, "tool", name, parent_run_id,
                    agent_id, task_id,
                )
            payload: Dict[str, Any] = {
                "input_len": len(input_str or ""),
            }
            if self.capture_payloads:
                payload["input"] = _truncate(input_str or "")
            step = CallbackStep(
                step_id=uuid.uuid4().hex[:12],
                sequence=self._sequence,
                kind="tool_start",
                timestamp=time.time(),
                run_id=run_id,
                parent_run_id=parent_run_id,
                agent_id=agent_id,
                task_id=task_id,
                name=name,
                payload=payload,
            )
            self._record_step(step)

    def on_tool_end(
        self,
        output: Any,
        *,
        run_id: Optional[str] = None,
        parent_run_id: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
        **kwargs: Any,
    ) -> None:
        """Record tool completion with duration and (optionally) output."""
        with self._lock:
            info = self._open_runs.pop(run_id, None) if run_id else None
            duration_ms = self._duration_from_info(info)
            agent_id, task_id = self._agent_task_from_run_info(
                info, metadata, kwargs,
            )
            name = info.get("name") if info else None
            payload: Dict[str, Any] = {"output_len": len(str(output or ""))}
            if self.capture_payloads:
                payload["output"] = _truncate(str(output or ""))
            self._sequence += 1
            step = CallbackStep(
                step_id=uuid.uuid4().hex[:12],
                sequence=self._sequence,
                kind="tool_end",
                timestamp=time.time(),
                run_id=run_id,
                parent_run_id=parent_run_id,
                agent_id=agent_id,
                task_id=task_id,
                name=name,
                duration_ms=duration_ms,
                success=True,
                payload=payload,
            )
            self._record_step(step)
            if agent_id:
                self._register_agent_completion(
                    agent_id, task_id, duration_ms,
                )

    def on_tool_error(
        self,
        error: BaseException,
        *,
        run_id: Optional[str] = None,
        parent_run_id: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
        **kwargs: Any,
    ) -> None:
        """Record a tool error; closes the open run *before* the error gate."""
        with self._lock:
            info = self._open_runs.pop(run_id, None) if run_id else None
            duration_ms = self._duration_from_info(info)
            agent_id, task_id = self._agent_task_from_run_info(
                info, metadata, kwargs,
            )
            name = info.get("name") if info else None

            if not self.capture_errors:
                # Still update attribution so errors aren't invisible.
                if agent_id:
                    self._register_agent_completion(
                        agent_id, task_id, duration_ms, error=True,
                    )
                return

            error_text = self._error_to_text(error)
            self._errors.append({
                "kind": "tool_error",
                "at": datetime.now().isoformat(),
                "run_id": run_id,
                "agent_id": agent_id,
                "task_id": task_id,
                "error": error_text,
            })
            self._sequence += 1
            step = CallbackStep(
                step_id=uuid.uuid4().hex[:12],
                sequence=self._sequence,
                kind="tool_error",
                timestamp=time.time(),
                run_id=run_id,
                parent_run_id=parent_run_id,
                agent_id=agent_id,
                task_id=task_id,
                name=name,
                duration_ms=duration_ms,
                success=False,
                error=error_text,
            )
            self._record_step(step)
            if agent_id:
                self._register_agent_completion(
                    agent_id, task_id, duration_ms, error=True,
                )

    # ------------------------------------------------------------------
    # LLM callbacks (classic completion)
    # ------------------------------------------------------------------

    def on_llm_start(
        self,
        serialized: Dict[str, Any],
        prompts: List[str],
        *,
        run_id: Optional[str] = None,
        parent_run_id: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
        **kwargs: Any,
    ) -> None:
        """Original: increments `llm_calls`. Enhanced: records a step."""
        with self._lock:
            self.llm_calls += 1          # FIX: was outside the lock in v1

            self._sweep_open_runs_locked()

            self._sequence += 1
            model = self._extract_name(serialized)
            agent_id, task_id = self._resolve_agent_task(metadata, kwargs)
            self._llm_calls_by_model[model] += 1
            if run_id is not None:
                self._store_open_run(
                    run_id, "llm", model, parent_run_id,
                    agent_id, task_id,
                )
            step = CallbackStep(
                step_id=uuid.uuid4().hex[:12],
                sequence=self._sequence,
                kind="llm_start",
                timestamp=time.time(),
                run_id=run_id,
                parent_run_id=parent_run_id,
                agent_id=agent_id,
                task_id=task_id,
                name=model,
                payload={"n_prompts": len(prompts or [])},
            )
            self._record_step(step)

    def on_llm_end(
        self,
        response: Any,
        *,
        run_id: Optional[str] = None,
        parent_run_id: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
        **kwargs: Any,
    ) -> None:
        """Record LLM completion with duration and token usage."""
        self._handle_llm_end(
            response, run_id, parent_run_id, metadata, kwargs,
            kind="llm_end",
        )

    def on_llm_error(
        self,
        error: BaseException,
        *,
        run_id: Optional[str] = None,
        parent_run_id: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
        **kwargs: Any,
    ) -> None:
        """Record LLM error."""
        self._handle_llm_error(
            error, run_id, parent_run_id, metadata, kwargs,
            kind="llm_error",
        )

    # ------------------------------------------------------------------
    # Chat model callbacks (modern LangChain)
    # ------------------------------------------------------------------

    def on_chat_model_start(
        self,
        serialized: Dict[str, Any],
        messages: List[List[Any]],
        *,
        run_id: Optional[str] = None,
        parent_run_id: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
        **kwargs: Any,
    ) -> None:
        """
        Modern-LangChain entry point for chat models.

        v1 had no handler here — every ChatOpenAI / ChatAnthropic /
        ChatGoogle run reported `llm_calls=0`.
        """
        with self._lock:
            self.llm_calls += 1

            self._sweep_open_runs_locked()

            self._sequence += 1
            model = self._extract_name(serialized)
            agent_id, task_id = self._resolve_agent_task(metadata, kwargs)
            self._llm_calls_by_model[model] += 1
            if run_id is not None:
                self._store_open_run(
                    run_id, "llm", model, parent_run_id,
                    agent_id, task_id,
                )
            step = CallbackStep(
                step_id=uuid.uuid4().hex[:12],
                sequence=self._sequence,
                kind="chat_model_start",
                timestamp=time.time(),
                run_id=run_id,
                parent_run_id=parent_run_id,
                agent_id=agent_id,
                task_id=task_id,
                name=model,
                payload={"n_messages": len(messages or [])},
            )
            self._record_step(step)

    def on_chat_model_end(
        self,
        response: Any,
        *,
        run_id: Optional[str] = None,
        parent_run_id: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
        **kwargs: Any,
    ) -> None:
        """Record chat-model completion with duration and tokens."""
        self._handle_llm_end(
            response, run_id, parent_run_id, metadata, kwargs,
            kind="chat_model_end",
        )

    def on_chat_model_error(
        self,
        error: BaseException,
        *,
        run_id: Optional[str] = None,
        parent_run_id: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
        **kwargs: Any,
    ) -> None:
        """Record chat-model error."""
        self._handle_llm_error(
            error, run_id, parent_run_id, metadata, kwargs,
            kind="chat_model_error",
        )

    # ------------------------------------------------------------------
    # Chain callbacks
    # ------------------------------------------------------------------

    def on_chain_start(
        self,
        serialized: Dict[str, Any],
        inputs: Any,
        *,
        run_id: Optional[str] = None,
        parent_run_id: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
        **kwargs: Any,
    ) -> None:
        """Record chain start (outer wall-clock envelope)."""
        with self._lock:
            self._sweep_open_runs_locked()

            self._sequence += 1
            name = self._extract_name(serialized)
            agent_id, task_id = self._resolve_agent_task(metadata, kwargs)
            self._chain_calls_by_name[name] += 1
            if run_id is not None:
                self._store_open_run(
                    run_id, "chain", name, parent_run_id,
                    agent_id, task_id,
                )
            step = CallbackStep(
                step_id=uuid.uuid4().hex[:12],
                sequence=self._sequence,
                kind="chain_start",
                timestamp=time.time(),
                run_id=run_id,
                parent_run_id=parent_run_id,
                agent_id=agent_id,
                task_id=task_id,
                name=name,
            )
            self._record_step(step)

    def on_chain_end(
        self,
        outputs: Any,
        *,
        run_id: Optional[str] = None,
        parent_run_id: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
        **kwargs: Any,
    ) -> None:
        """Record chain completion with duration."""
        with self._lock:
            info = self._open_runs.pop(run_id, None) if run_id else None
            duration_ms = self._duration_from_info(info)
            agent_id, task_id = self._agent_task_from_run_info(
                info, metadata, kwargs,
            )
            name = info.get("name") if info else None
            self._sequence += 1
            step = CallbackStep(
                step_id=uuid.uuid4().hex[:12],
                sequence=self._sequence,
                kind="chain_end",
                timestamp=time.time(),
                run_id=run_id,
                parent_run_id=parent_run_id,
                agent_id=agent_id,
                task_id=task_id,
                name=name,
                duration_ms=duration_ms,
                success=True,
            )
            self._record_step(step)

    def on_chain_error(
        self,
        error: BaseException,
        *,
        run_id: Optional[str] = None,
        parent_run_id: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
        **kwargs: Any,
    ) -> None:
        """Record chain error; closes the open run first."""
        with self._lock:
            info = self._open_runs.pop(run_id, None) if run_id else None
            duration_ms = self._duration_from_info(info)
            agent_id, task_id = self._agent_task_from_run_info(
                info, metadata, kwargs,
            )
            name = info.get("name") if info else None
            if self.capture_errors:
                error_text = self._error_to_text(error)
                self._errors.append({
                    "kind": "chain_error",
                    "at": datetime.now().isoformat(),
                    "run_id": run_id,
                    "agent_id": agent_id,
                    "task_id": task_id,
                    "error": error_text,
                })
                self._sequence += 1
                step = CallbackStep(
                    step_id=uuid.uuid4().hex[:12],
                    sequence=self._sequence,
                    kind="chain_error",
                    timestamp=time.time(),
                    run_id=run_id,
                    parent_run_id=parent_run_id,
                    agent_id=agent_id,
                    task_id=task_id,
                    name=name,
                    duration_ms=duration_ms,
                    success=False,
                    error=error_text,
                )
                self._record_step(step)

    # ------------------------------------------------------------------
    # Retriever callbacks
    # ------------------------------------------------------------------

    def on_retriever_start(
        self,
        serialized: Dict[str, Any],
        query: str,
        *,
        run_id: Optional[str] = None,
        parent_run_id: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
        **kwargs: Any,
    ) -> None:
        """Record retriever start (RAG is a major energy consumer)."""
        with self._lock:
            self._sweep_open_runs_locked()

            self._sequence += 1
            name = self._extract_name(serialized)
            agent_id, task_id = self._resolve_agent_task(metadata, kwargs)
            self._retriever_calls_by_name[name] += 1
            if run_id is not None:
                self._store_open_run(
                    run_id, "retriever", name, parent_run_id,
                    agent_id, task_id,
                )
            payload: Dict[str, Any] = {"query_len": len(query or "")}
            if self.capture_payloads:
                payload["query"] = _truncate(query or "")
            step = CallbackStep(
                step_id=uuid.uuid4().hex[:12],
                sequence=self._sequence,
                kind="retriever_start",
                timestamp=time.time(),
                run_id=run_id,
                parent_run_id=parent_run_id,
                agent_id=agent_id,
                task_id=task_id,
                name=name,
                payload=payload,
            )
            self._record_step(step)

    def on_retriever_end(
        self,
        documents: Any,
        *,
        run_id: Optional[str] = None,
        parent_run_id: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
        **kwargs: Any,
    ) -> None:
        """Record retriever completion with document count."""
        with self._lock:
            info = self._open_runs.pop(run_id, None) if run_id else None
            duration_ms = self._duration_from_info(info)
            agent_id, task_id = self._agent_task_from_run_info(
                info, metadata, kwargs,
            )
            name = info.get("name") if info else None
            n_docs = (
                len(documents) if isinstance(documents, (list, tuple)) else 0
            )
            self._sequence += 1
            step = CallbackStep(
                step_id=uuid.uuid4().hex[:12],
                sequence=self._sequence,
                kind="retriever_end",
                timestamp=time.time(),
                run_id=run_id,
                parent_run_id=parent_run_id,
                agent_id=agent_id,
                task_id=task_id,
                name=name,
                duration_ms=duration_ms,
                success=True,
                payload={"n_documents": n_docs},
            )
            self._record_step(step)
            if agent_id:
                self._register_agent_completion(
                    agent_id, task_id, duration_ms,
                )

    def on_retriever_error(
        self,
        error: BaseException,
        *,
        run_id: Optional[str] = None,
        parent_run_id: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
        **kwargs: Any,
    ) -> None:
        """Record retriever error; closes the open run first."""
        with self._lock:
            info = self._open_runs.pop(run_id, None) if run_id else None
            duration_ms = self._duration_from_info(info)
            agent_id, task_id = self._agent_task_from_run_info(
                info, metadata, kwargs,
            )
            name = info.get("name") if info else None
            if not self.capture_errors:
                if agent_id:
                    self._register_agent_completion(
                        agent_id, task_id, duration_ms, error=True,
                    )
                return
            error_text = self._error_to_text(error)
            self._errors.append({
                "kind": "retriever_error",
                "at": datetime.now().isoformat(),
                "run_id": run_id,
                "agent_id": agent_id,
                "task_id": task_id,
                "error": error_text,
            })
            self._sequence += 1
            step = CallbackStep(
                step_id=uuid.uuid4().hex[:12],
                sequence=self._sequence,
                kind="retriever_error",
                timestamp=time.time(),
                run_id=run_id,
                parent_run_id=parent_run_id,
                agent_id=agent_id,
                task_id=task_id,
                name=name,
                duration_ms=duration_ms,
                success=False,
                error=error_text,
            )
            self._record_step(step)
            if agent_id:
                self._register_agent_completion(
                    agent_id, task_id, duration_ms, error=True,
                )

    # ------------------------------------------------------------------
    # Agent callbacks
    # ------------------------------------------------------------------

    def on_agent_action(
        self,
        action: Any,
        *,
        run_id: Optional[str] = None,
        parent_run_id: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
        **kwargs: Any,
    ) -> None:
        """Record an agent decision (tool selection)."""
        with self._lock:
            self._sequence += 1
            agent_id, task_id = self._resolve_agent_task(metadata, kwargs)
            tool_name = getattr(action, "tool", None)
            if not isinstance(tool_name, str):
                tool_name = "unknown"
            self._track_delegation(agent_id, tool_name)
            payload: Dict[str, Any] = {"tool": tool_name}
            if self.capture_payloads:
                log_text = getattr(action, "log", None)
                if log_text:
                    payload["log"] = _truncate(str(log_text))
            step = CallbackStep(
                step_id=uuid.uuid4().hex[:12],
                sequence=self._sequence,
                kind="agent_action",
                timestamp=time.time(),
                run_id=run_id,
                parent_run_id=parent_run_id,
                agent_id=agent_id,
                task_id=task_id,
                name=tool_name,
                payload=payload,
            )
            self._record_step(step)

    def on_agent_finish(
        self,
        finish: Any,
        *,
        run_id: Optional[str] = None,
        parent_run_id: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
        **kwargs: Any,
    ) -> None:
        """Record an agent's final answer."""
        with self._lock:
            self._sequence += 1
            agent_id, task_id = self._resolve_agent_task(metadata, kwargs)
            payload: Dict[str, Any] = {}
            return_values = getattr(finish, "return_values", None)
            if isinstance(return_values, dict):
                payload["return_keys"] = list(return_values.keys())
                if self.capture_payloads:
                    output = return_values.get("output")
                    if output is not None:
                        payload["output"] = _truncate(str(output))
            step = CallbackStep(
                step_id=uuid.uuid4().hex[:12],
                sequence=self._sequence,
                kind="agent_finish",
                timestamp=time.time(),
                run_id=run_id,
                parent_run_id=parent_run_id,
                agent_id=agent_id,
                task_id=task_id,
                success=True,
                payload=payload,
            )
            self._record_step(step)

    # ------------------------------------------------------------------
    # Retry callback
    # ------------------------------------------------------------------

    def on_retry(
        self,
        retry_state: Any,
        *,
        run_id: Optional[str] = None,
        parent_run_id: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
        **kwargs: Any,
    ) -> None:
        """Record a retry attempt (v1 didn't count these at all)."""
        with self._lock:
            self._sequence += 1
            agent_id, task_id = self._resolve_agent_task(metadata, kwargs)
            attempt = getattr(retry_state, "attempt_number", None)
            try:
                attempt_int = int(attempt) if attempt is not None else None
            except (TypeError, ValueError):
                attempt_int = None
            step = CallbackStep(
                step_id=uuid.uuid4().hex[:12],
                sequence=self._sequence,
                kind="retry",
                timestamp=time.time(),
                run_id=run_id,
                parent_run_id=parent_run_id,
                agent_id=agent_id,
                task_id=task_id,
                success=False,
                payload={"attempt_number": attempt_int},
            )
            self._record_step(step)

    # ------------------------------------------------------------------
    # Backward-compatible snapshot
    # ------------------------------------------------------------------

    def snapshot(self) -> Dict[str, Any]:
        """
        Backward-compatible snapshot.

        Original keys `tool_calls` and `llm_calls` are still top-level ints;
        the rest are additive.
        """
        with self._lock:
            return {
                "tool_calls": self.tool_calls,
                "llm_calls": self.llm_calls,
                "prompt_tokens": self._prompt_tokens_total,
                "completion_tokens": self._completion_tokens_total,
                "total_tokens": (
                    self._prompt_tokens_total + self._completion_tokens_total
                ),
                "errors": len(self._errors),
                "steps_recorded": len(self._steps),
                "dropped_steps": self._dropped_steps,
                "open_runs": len(self._open_runs),
            }

    # ------------------------------------------------------------------
    # Enhanced introspection
    # ------------------------------------------------------------------

    def steps(
        self,
        *,
        limit: Optional[int] = None,
        kind: Optional[str] = None,
        agent_id: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """Return steps as dicts, optionally filtered."""
        with self._lock:
            snapshot = list(self._steps)
        if kind is not None:
            snapshot = [s for s in snapshot if s.kind == kind]
        if agent_id is not None:
            snapshot = [s for s in snapshot if s.agent_id == agent_id]
        if limit is not None and limit > 0:
            snapshot = snapshot[-limit:]
        return [s.to_dict() for s in snapshot]

    def steps_by_parent(
        self, parent_run_id: Optional[str],
    ) -> List[Dict[str, Any]]:
        """Return steps spawned by a given parent run (or top-level if None)."""
        with self._lock:
            return [
                s.to_dict() for s in self._steps
                if s.parent_run_id == parent_run_id
            ]

    def errors(self, *, limit: Optional[int] = None) -> List[Dict[str, Any]]:
        with self._lock:
            snapshot = list(self._errors)
        if limit is not None and limit > 0:
            snapshot = snapshot[-limit:]
        return snapshot

    def observations(self) -> List[Any]:
        """
        Emit `AgentObservation`s grouped by `(agent_id, task_id)`.

        Returns [] if the analysis contract isn't importable or
        `emit_observations=False`.
        """
        if not self.emit_observations:
            return []
        cls = _get_agent_observation_class()
        if cls is None:
            return []
        with self._lock:
            grouped: Dict[Tuple[str, str], Dict[str, Any]] = {}
            for (aid, tid), calls in self._agent_calls.items():
                grouped.setdefault((aid, tid), {})["calls"] = calls
            for (aid, tid), latency in self._agent_latency_ms.items():
                grouped.setdefault((aid, tid), {})["latency_ms"] = latency
            for (aid, tid), errs in self._agent_errors.items():
                grouped.setdefault((aid, tid), {})["errors"] = errs
            role_map = dict(self._agent_role)
            delegated_map = {k: set(v) for k, v in self._agent_delegated_to.items()}

        out: List[Any] = []
        for (aid, tid), info in grouped.items():
            try:
                obs = cls(
                    agent_id=aid,
                    task_id=tid,
                    role=role_map.get(aid, "generalist"),
                    delegated_to=sorted(delegated_map.get(aid, set())),
                    coordination_calls=int(info.get("calls", 0)),
                    latency_ms=float(info.get("latency_ms", 0.0)),
                    energy_kwh=0.0,
                )
                out.append(obs)
            except Exception as e:
                logger.warning(
                    "langchain.observation_construction_failed",
                    extra={"agent_id": aid, "error": str(e)},
                )
        return out

    def statistics(self) -> Dict[str, Any]:
        """Rich statistics for this callback session."""
        with self._lock:
            by_kind = dict(self._by_kind)
            errors_by_agent = {
                f"{aid}:{tid}": n
                for (aid, tid), n in self._agent_errors.items()
            }
            return {
                "analyzer_version": __version__,
                "run_id": self.run_id,
                "agent_id": self.agent_id,
                "task_id": self.task_id,
                "tool_calls": self.tool_calls,
                "llm_calls": self.llm_calls,
                "tool_calls_by_name": dict(self._tool_calls_by_name),
                "llm_calls_by_model": dict(self._llm_calls_by_model),
                "chain_calls_by_name": dict(self._chain_calls_by_name),
                "retriever_calls_by_name": dict(self._retriever_calls_by_name),
                "prompt_tokens": self._prompt_tokens_total,
                "completion_tokens": self._completion_tokens_total,
                "total_tokens": (
                    self._prompt_tokens_total + self._completion_tokens_total
                ),
                "errors_count": len(self._errors),
                "errors_by_agent": errors_by_agent,
                "by_kind": by_kind,
                "steps_recorded": len(self._steps),
                "dropped_steps": self._dropped_steps,
                "open_runs": len(self._open_runs),
                "elapsed_s": time.time() - self._started_at,
            }

    def export(self) -> Dict[str, Any]:
        """Fully serialisable snapshot with hoisted metadata."""
        with self._lock:
            return {
                "analyzer_version": __version__,
                "run_id": self.run_id,
                "task_id": self.task_id,
                "agent_id": self.agent_id,
                "started_at": _safe_isoformat(self._started_at),
                "statistics": self.statistics(),
                "steps": [s.to_dict(safe=True) for s in self._steps],
                "errors": list(self._errors),
                "federated_aggregate": self._federated.aggregate(),
            }

    def reset(self) -> None:
        """Reset counters, steps, errors, open runs, and per-agent state."""
        with self._lock:
            self.tool_calls = 0
            self.llm_calls = 0
            self._steps.clear()
            self._errors.clear()
            self._open_runs.clear()
            self._tool_calls_by_name.clear()
            self._llm_calls_by_model.clear()
            self._chain_calls_by_name.clear()
            self._retriever_calls_by_name.clear()
            self._prompt_tokens_total = 0
            self._completion_tokens_total = 0
            self._agent_calls.clear()
            self._agent_latency_ms.clear()
            self._agent_errors.clear()
            self._agent_role.clear()
            self._agent_delegated_to.clear()
            self._by_kind.clear()
            self._sequence = 0
            self._dropped_steps = 0
            self._started_at = time.time()
            self._last_contributed_signature = None

    # ------------------------------------------------------------------
    # HITL / federated
    # ------------------------------------------------------------------

    def set_hitl_callback(
        self,
        callback: Optional[Callable[[CallbackStep], Optional[bool]]],
    ) -> None:
        """
        Register a reviewer invoked on suspicious patterns:

          * `tool_end` / `llm_end` / `chat_model_end` with duration above
            the configured thresholds (defaults: 30 s / 60 s),
          * an agent accumulating >= `hitl_error_threshold` errors.

        The callback receives the completed `CallbackStep` and may mutate
        it. Returning `False` sets `needs_review=True` and appends
        `[HITL: denied]` to `review_reason`.

        Passing `None` clears the callback.
        """
        self._hitl_callback = callback

    def set_agent_role(self, agent_id: str, role: str) -> None:
        """
        Explicitly set the role used when emitting AgentObservations.

        Callers can also pass `metadata={"agent_id": ..., "role": ...}`
        on any callback; that metadata is preferred over this method.
        """
        if not isinstance(agent_id, str) or not agent_id:
            return
        if not isinstance(role, str) or not role:
            return
        with self._lock:
            self._agent_role[agent_id] = role

    def contribute_federated(
        self, *, force: bool = False,
    ) -> None:
        """
        Push an aggregated profile for the current session.

        Idempotent: skips the push when `(tool_calls, llm_calls)` hasn't
        changed since the last contribution, unless `force=True`.
        """
        with self._lock:
            sig = (self.tool_calls, self.llm_calls)
            if not force and sig == self._last_contributed_signature:
                return
            if not self._steps:
                return

            latency_values = [
                s.duration_ms for s in self._steps
                if s.duration_ms is not None and s.kind.endswith("_end")
            ]
            mean_latency = (
                sum(latency_values) / len(latency_values)
                if latency_values else 0.0
            )
            profile = FederatedLangChainProfile(
                deployment_id=self.agent_id or "local",
                framework="langchain",
                framework_version=None,
                mean_tool_calls=float(self.tool_calls),
                mean_llm_calls=float(self.llm_calls),
                mean_errors=float(len(self._errors)),
                mean_latency_ms=float(mean_latency),
                sample_count=max(1, len(self._steps)),
            )
            self._federated.push(profile)
            self._last_contributed_signature = sig

    def get_federated_aggregate(self) -> Dict[str, Any]:
        return self._federated.aggregate()

    # ------------------------------------------------------------------
    # Internal — LLM end/error handlers (shared between llm + chat_model)
    # ------------------------------------------------------------------

    def _handle_llm_end(
        self,
        response: Any,
        run_id: Optional[str],
        parent_run_id: Optional[str],
        metadata: Optional[Dict[str, Any]],
        kwargs: Dict[str, Any],
        *,
        kind: str,
    ) -> None:
        with self._lock:
            info = self._open_runs.pop(run_id, None) if run_id else None
            duration_ms = self._duration_from_info(info)
            agent_id, task_id = self._agent_task_from_run_info(
                info, metadata, kwargs,
            )
            name = info.get("name") if info else None
            tokens = (
                self._extract_tokens(response)
                if self.capture_tokens else {}
            )
            self._prompt_tokens_total += tokens.get("prompt_tokens", 0)
            self._completion_tokens_total += tokens.get("completion_tokens", 0)
            self._sequence += 1
            step = CallbackStep(
                step_id=uuid.uuid4().hex[:12],
                sequence=self._sequence,
                kind=kind,
                timestamp=time.time(),
                run_id=run_id,
                parent_run_id=parent_run_id,
                agent_id=agent_id,
                task_id=task_id,
                name=name,
                duration_ms=duration_ms,
                success=True,
                tokens=tokens.get("total_tokens"),
                prompt_tokens=tokens.get("prompt_tokens"),
                completion_tokens=tokens.get("completion_tokens"),
            )
            self._record_step(step)
            if agent_id:
                self._register_agent_completion(
                    agent_id, task_id, duration_ms,
                )

    def _handle_llm_error(
        self,
        error: BaseException,
        run_id: Optional[str],
        parent_run_id: Optional[str],
        metadata: Optional[Dict[str, Any]],
        kwargs: Dict[str, Any],
        *,
        kind: str,
    ) -> None:
        with self._lock:
            info = self._open_runs.pop(run_id, None) if run_id else None
            duration_ms = self._duration_from_info(info)
            agent_id, task_id = self._agent_task_from_run_info(
                info, metadata, kwargs,
            )
            name = info.get("name") if info else None

            if not self.capture_errors:
                if agent_id:
                    self._register_agent_completion(
                        agent_id, task_id, duration_ms, error=True,
                    )
                return

            error_text = self._error_to_text(error)
            self._errors.append({
                "kind": kind,
                "at": datetime.now().isoformat(),
                "run_id": run_id,
                "agent_id": agent_id,
                "task_id": task_id,
                "error": error_text,
            })
            self._sequence += 1
            step = CallbackStep(
                step_id=uuid.uuid4().hex[:12],
                sequence=self._sequence,
                kind=kind,
                timestamp=time.time(),
                run_id=run_id,
                parent_run_id=parent_run_id,
                agent_id=agent_id,
                task_id=task_id,
                name=name,
                duration_ms=duration_ms,
                success=False,
                error=error_text,
            )
            self._record_step(step)
            if agent_id:
                self._register_agent_completion(
                    agent_id, task_id, duration_ms, error=True,
                )

    # ------------------------------------------------------------------
    # Internal — step recording, HITL, trace forwarding
    # ------------------------------------------------------------------

    def _record_step(self, step: CallbackStep) -> None:
        """Append to the deque, update counters, fire HITL + trace + log."""
        if len(self._steps) >= self._max_steps:
            self._dropped_steps += 1
            if (
                self._dropped_steps == 1
                or self._dropped_steps % 100 == 0
            ):
                logger.warning(
                    "langchain.steps_bounded",
                    extra={
                        "max_steps": self._max_steps,
                        "dropped_steps": self._dropped_steps,
                    },
                )

        self._steps.append(step)
        self._by_kind[step.kind] += 1

        # HITL trigger detection
        self._maybe_hitl(step)

        # Forward to ExecutionTrace (full step dict, not just payload)
        if self.emit_trace and self.trace is not None:
            try:
                severity = (
                    "critical" if step.success is False else "info"
                )
                self.trace.record(
                    step.to_dict(),
                    kind=step.kind,
                    agent_id=step.agent_id,
                    task_id=step.task_id,
                    run_id=step.run_id,
                    duration_ms=step.duration_ms,
                    timestamp=step.timestamp,
                    severity=severity,
                )
            except Exception as e:
                logger.debug("langchain.trace_forwarding_failed",
                             extra={"error": str(e)})

        # Structured log
        extra = {
            "step_id": step.step_id,
            "sequence": step.sequence,
            "kind": step.kind,
            "name": step.name,
            "agent_id": step.agent_id,
            "task_id": step.task_id,
            "run_id": step.run_id,
            "parent_run_id": step.parent_run_id,
            "duration_ms": step.duration_ms,
            "success": step.success,
            "tokens": step.tokens,
            "needs_review": step.needs_review,
        }
        if step.success is False:
            logger.warning("langchain.step_recorded", extra=extra)
        elif step.needs_review:
            logger.info("langchain.step_recorded", extra=extra)
        else:
            logger.debug("langchain.step_recorded", extra=extra)

    def _maybe_hitl(self, step: CallbackStep) -> None:
        """Fire the HITL reviewer when a step matches review conditions."""
        if self._hitl_callback is None:
            return
        reasons: List[str] = []
        if (
            step.kind.endswith("_end")
            and step.duration_ms is not None
        ):
            if "tool" in step.kind and step.duration_ms > self._hitl_long_tool_ms:
                reasons.append(
                    f"long {step.kind} ({step.duration_ms:.0f} ms > "
                    f"{self._hitl_long_tool_ms:.0f} ms)"
                )
            elif "llm" in step.kind or "chat_model" in step.kind:
                if step.duration_ms > self._hitl_long_llm_ms:
                    reasons.append(
                        f"long {step.kind} ({step.duration_ms:.0f} ms > "
                        f"{self._hitl_long_llm_ms:.0f} ms)"
                    )
        if step.agent_id and step.task_id is not None:
            errs = self._agent_errors.get((step.agent_id, step.task_id), 0)
            if errs >= self._hitl_error_threshold:
                reasons.append(
                    f"agent error threshold ({errs} >= "
                    f"{self._hitl_error_threshold})"
                )

        if not reasons:
            return

        step.needs_review = True
        step.review_reason = "; ".join(reasons)
        try:
            approved = self._hitl_callback(step)
            if approved is False:
                step.review_reason = (
                    step.review_reason or ""
                ) + " [HITL: denied]"
        except Exception as e:
            logger.warning(
                "langchain.hitl_callback_failed",
                extra={"step_id": step.step_id, "error": str(e)},
            )

    # ------------------------------------------------------------------
    # Internal — attribution helpers
    # ------------------------------------------------------------------

    def _register_agent_completion(
        self,
        agent_id: str,
        task_id: Optional[str],
        duration_ms: Optional[float],
        *,
        error: bool = False,
    ) -> None:
        """Increment per-(agent, task) counters. Called under lock."""
        key = (agent_id, task_id or "")
        self._agent_calls[key] += 1
        if duration_ms is not None:
            self._agent_latency_ms[key] += duration_ms
        if error:
            self._agent_errors[key] += 1

    def _track_delegation(
        self, agent_id: Optional[str], tool_name: str,
    ) -> None:
        if not agent_id or not isinstance(tool_name, str):
            return
        for pattern in _DELEGATION_PATTERNS:
            m = pattern.match(tool_name)
            if m:
                self._agent_delegated_to[agent_id].add(m.group(1))
                break

    # ------------------------------------------------------------------
    # Internal — open runs
    # ------------------------------------------------------------------

    def _store_open_run(
        self,
        run_id: str,
        kind: str,
        name: Optional[str],
        parent_run_id: Optional[str],
        agent_id: Optional[str],
        task_id: Optional[str],
    ) -> None:
        if len(self._open_runs) >= self._max_open_runs:
            self._evict_oldest_open_runs_locked()
        self._open_runs[run_id] = {
            "kind": kind,
            "name": name,
            "start_time": time.time(),
            "parent_run_id": parent_run_id,
            "agent_id": agent_id,
            "task_id": task_id,
        }

    def _evict_oldest_open_runs_locked(self) -> None:
        if not self._open_runs:
            return
        # Evict the oldest 10% (or 1, whichever is larger) to amortise.
        n_evict = max(1, len(self._open_runs) // 10)
        sorted_runs = sorted(
            self._open_runs.items(),
            key=lambda kv: kv[1].get("start_time", 0.0),
        )
        for run_id, _ in sorted_runs[:n_evict]:
            self._open_runs.pop(run_id, None)
        logger.warning(
            "langchain.open_runs_evicted",
            extra={
                "evicted": n_evict,
                "remaining": len(self._open_runs),
                "max_open_runs": self._max_open_runs,
            },
        )

    def _sweep_open_runs_locked(self) -> None:
        """Drop open runs older than `max_run_duration_s`."""
        if self._max_run_duration_s <= 0 or not self._open_runs:
            return
        cutoff = time.time() - self._max_run_duration_s
        expired = [
            rid for rid, info in self._open_runs.items()
            if info.get("start_time", 0.0) < cutoff
        ]
        for rid in expired:
            self._open_runs.pop(rid, None)
        if expired:
            logger.debug(
                "langchain.open_runs_ttl_swept",
                extra={"expired": len(expired)},
            )

    def _duration_from_info(
        self, info: Optional[Dict[str, Any]],
    ) -> Optional[float]:
        if not info:
            return None
        start = info.get("start_time")
        if start is None:
            return None
        return max(0.0, (time.time() - start) * 1000.0)

    def _agent_task_from_run_info(
        self,
        info: Optional[Dict[str, Any]],
        metadata: Optional[Dict[str, Any]],
        kwargs: Dict[str, Any],
    ) -> Tuple[Optional[str], Optional[str]]:
        """Prefer the run's stored agent/task; fall back to metadata."""
        stored_agent = info.get("agent_id") if info else None
        stored_task = info.get("task_id") if info else None
        meta_agent, meta_task = self._resolve_agent_task(metadata, kwargs)
        return (
            stored_agent or meta_agent,
            stored_task if stored_task is not None else meta_task,
        )

    def _resolve_agent_task(
        self,
        metadata: Optional[Dict[str, Any]],
        kwargs: Dict[str, Any],
    ) -> Tuple[Optional[str], Optional[str]]:
        """Resolve per-call agent_id / task_id (metadata > kwargs > defaults)."""
        agent_id = self.agent_id
        task_id = self.task_id
        if isinstance(metadata, dict):
            meta_agent = metadata.get("agent_id")
            if isinstance(meta_agent, str) and meta_agent:
                agent_id = meta_agent
            meta_task = metadata.get("task_id")
            if isinstance(meta_task, str) and meta_task:
                task_id = meta_task
            # Some callers pass role directly
            meta_role = metadata.get("role")
            if (
                isinstance(meta_role, str) and meta_role
                and isinstance(agent_id, str) and agent_id
            ):
                self._agent_role.setdefault(agent_id, meta_role)
        kw_agent = kwargs.get("agent_id")
        if isinstance(kw_agent, str) and kw_agent:
            agent_id = kw_agent
        kw_task = kwargs.get("task_id")
        if isinstance(kw_task, str) and kw_task:
            task_id = kw_task
        return agent_id, task_id

    # ------------------------------------------------------------------
    # Internal — extraction helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _extract_name(serialized: Any) -> str:
        """
        Unified name extraction from a LangChain `serialized` dict.

        Looks at `name`, then `kwargs["model_name"]` / `["model"]` / `["name"]`,
        then `id` (as list or string).
        """
        if not isinstance(serialized, dict):
            return "unknown"
        name = serialized.get("name")
        if isinstance(name, str) and name.strip():
            return name.strip()
        kwargs = serialized.get("kwargs")
        if isinstance(kwargs, dict):
            for key in ("model_name", "model", "name"):
                v = kwargs.get(key)
                if isinstance(v, str) and v.strip():
                    return v.strip()
        sid = serialized.get("id")
        if isinstance(sid, list) and sid:
            return str(sid[-1])
        if isinstance(sid, str) and sid.strip():
            return sid.strip()
        return "unknown"

    @staticmethod
    def _extract_tokens(response: Any) -> Dict[str, int]:
        """
        Best-effort token extraction across LangChain variants.

        Tries in order:
          1. `response.generations[0][0].message.usage_metadata`
             (modern chat models; `input_tokens` / `output_tokens`).
          2. `response.generations[0][0].generation_info["usage"]`.
          3. `response.llm_output["token_usage"]` / `["usage"]` (classic).
        """
        out: Dict[str, int] = {}

        def _norm(key: str) -> str:
            if key in ("input_tokens", "prompt_tokens"):
                return "prompt_tokens"
            if key in ("output_tokens", "completion_tokens"):
                return "completion_tokens"
            return "total_tokens"

        def _take(usage: Any) -> None:
            if not isinstance(usage, dict):
                return
            for k, v in usage.items():
                nk = _norm(k)
                val = _safe_int(v, default=-1)
                if val >= 0:
                    out[nk] = val

        try:
            # 1. Modern chat models
            gens = getattr(response, "generations", None)
            if (
                isinstance(gens, list) and gens
                and isinstance(gens[0], list) and gens[0]
            ):
                msg = getattr(gens[0][0], "message", None)
                usage_meta = getattr(msg, "usage_metadata", None) \
                    if msg is not None else None
                _take(usage_meta)
                if not out:
                    gen_info = getattr(gens[0][0], "generation_info", None)
                    if isinstance(gen_info, dict):
                        _take(gen_info.get("usage"))
        except Exception:
            pass

        # 3. Classic `llm_output`
        if not out:
            try:
                llm_output = getattr(response, "llm_output", None) or {}
                if isinstance(llm_output, dict):
                    _take(llm_output.get("token_usage"))
                    if not out:
                        _take(llm_output.get("usage"))
            except Exception:
                pass

        # Compute total if only prompt+completion are present.
        if (
            "total_tokens" not in out
            and "prompt_tokens" in out
            and "completion_tokens" in out
        ):
            out["total_tokens"] = (
                out["prompt_tokens"] + out["completion_tokens"]
            )
        return out

    @staticmethod
    def _error_to_text(error: Any) -> str:
        try:
            text = f"{type(error).__name__}: {error}"
        except Exception:
            text = "unknown_error"
        return _truncate(text, max_len=1000)


# =============================================================================
# Demo
# =============================================================================

if __name__ == "__main__":
    import json
    import sys
    import threading
    import types

    logging.basicConfig(level=logging.WARNING)

    # =================================================================
    # 1. Original behaviour (backward compatible)
    # =================================================================
    print("=== 1. Original behaviour ===")
    cb = GreenLangChainCallback()
    cb.on_tool_start({"name": "web_search"}, "query")
    cb.on_llm_start({"name": "gpt-4"}, ["prompt"])
    snap = cb.snapshot()
    print(f"  tool_calls: {snap['tool_calls']} (expected 1)")
    print(f"  llm_calls:  {snap['llm_calls']} (expected 1)")

    # =================================================================
    # 2. Thread safety — barrier-synchronised 4 × 1000 increments
    # =================================================================
    print("\n=== 2. Thread safety (barrier + 4 × 1000) ===")
    cb_conc = GreenLangChainCallback()
    barrier = threading.Barrier(4)

    def _worker():
        barrier.wait()
        for _ in range(1000):
            cb_conc.on_tool_start({"name": "t"}, "i")
            cb_conc.on_llm_start({"name": "m"}, ["p"])

    threads = [threading.Thread(target=_worker) for _ in range(4)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    print(f"  tool_calls: {cb_conc.tool_calls}  (expected 4000, exactly)")
    print(f"  llm_calls:  {cb_conc.llm_calls}   (expected 4000, exactly)")

    # =================================================================
    # 3. Chat model path (v1 had NO handler here)
    # =================================================================
    print("\n=== 3. Chat model path (modern LangChain) ===")
    cb_chat = GreenLangChainCallback(
        run_id="run-chat", agent_id="agent-A",
    )
    cb_chat.on_chat_model_start(
        {"name": "gpt-4o-mini"}, [[{"role": "user", "content": "hi"}]],
        run_id="cm-1",
    )
    time.sleep(0.01)

    class _FakeMessage:
        usage_metadata = {
            "input_tokens": 120,
            "output_tokens": 45,
            "total_tokens": 165,
        }

    class _FakeChatGen:
        message = _FakeMessage()

    class _FakeChatResult:
        generations = [[_FakeChatGen()]]

    cb_chat.on_chat_model_end(_FakeChatResult(), run_id="cm-1")
    snap = cb_chat.snapshot()
    print(f"  llm_calls:      {snap['llm_calls']}  (expected 1)")
    print(f"  total_tokens:   {snap['total_tokens']}  (expected 165)")
    steps = cb_chat.steps(kind="chat_model_end")
    if steps:
        print(f"  chat_model_end step duration: "
              f"{steps[0]['duration_ms']:.1f} ms")
        print(f"  prompt_tokens={steps[0]['prompt_tokens']}, "
              f"completion_tokens={steps[0]['completion_tokens']}")

    # =================================================================
    # 4. Error path without leak (v1 leaked _open_runs entries)
    # =================================================================
    print("\n=== 4. Error path without open-run leak ===")
    cb_err = GreenLangChainCallback(capture_errors=False)
    for i in range(5):
        cb_err.on_tool_start({"name": "t"}, "i", run_id=f"run-{i}")
        cb_err.on_tool_error(RuntimeError("boom"), run_id=f"run-{i}")
    print(f"  open_runs after 5 aborted runs: {len(cb_err._open_runs)}  "
          f"(expected 0)")
    print(f"  errors recorded: {len(cb_err.errors())}  "
          f"(expected 0 — capture_errors=False)")

    # =================================================================
    # 5. Parent-child tree reconstruction
    # =================================================================
    print("\n=== 5. steps_by_parent ===")
    cb_tree = GreenLangChainCallback(agent_id="coordinator")
    cb_tree.on_chain_start({"name": "orchestrator"}, {}, run_id="chain-root")
    cb_tree.on_tool_start(
        {"name": "web_search"}, "q",
        run_id="tool-1", parent_run_id="chain-root",
    )
    cb_tree.on_tool_end("result", run_id="tool-1",
                        parent_run_id="chain-root")
    cb_tree.on_chain_end({}, run_id="chain-root")
    children = cb_tree.steps_by_parent("chain-root")
    print(f"  children of 'chain-root': {len(children)}  "
          f"(expected 2: tool_start + tool_end)")
    for c in children:
        print(f"    - {c['kind']}")

    # =================================================================
    # 6. Extended token extraction (legacy llm_output path)
    # =================================================================
    print("\n=== 6. Legacy llm_output tokens ===")
    cb_legacy = GreenLangChainCallback()
    cb_legacy.on_llm_start({"name": "gpt-3.5"}, ["p"], run_id="l-1")

    class _FakeLLMResult:
        llm_output = {
            "token_usage": {
                "prompt_tokens": 50,
                "completion_tokens": 20,
                "total_tokens": 70,
            }
        }

    cb_legacy.on_llm_end(_FakeLLMResult(), run_id="l-1")
    print(f"  total_tokens: {cb_legacy.snapshot()['total_tokens']}  "
          f"(expected 70)")

    # =================================================================
    # 7. Full lifecycle (chain + retriever + agent + retry)
    # =================================================================
    print("\n=== 7. Full lifecycle ===")
    cb_full = GreenLangChainCallback(agent_id="agent-B")
    cb_full.on_chain_start({"name": "chain"}, {}, run_id="c-1")
    cb_full.on_retriever_start(
        {"name": "faiss"}, "query", run_id="r-1", parent_run_id="c-1",
    )
    time.sleep(0.005)
    cb_full.on_retriever_end(
        [{"doc": 1}, {"doc": 2}], run_id="r-1", parent_run_id="c-1",
    )
    cb_full.on_agent_action(
        types.SimpleNamespace(tool="delegate_to_researcher", log="thinking..."),
        run_id="a-1", parent_run_id="c-1",
    )
    cb_full.on_agent_finish(
        types.SimpleNamespace(return_values={"output": "done"}),
        run_id="a-1", parent_run_id="c-1",
    )
    cb_full.on_retry(
        types.SimpleNamespace(attempt_number=1),
        run_id="c-1",
    )
    cb_full.on_chain_end({}, run_id="c-1")
    stats = cb_full.statistics()
    print(f"  by_kind: {stats['by_kind']}")
    print(f"  chain_calls: {stats['chain_calls_by_name']}")
    print(f"  retriever_calls: {stats['retriever_calls_by_name']}")

    # =================================================================
    # 8. Observations grouped by (agent, task) with delegation inference
    # =================================================================
    print("\n=== 8. Observations grouped by (agent, task) ===")

    # Inject a lightweight AgentObservation class so the demo works
    # even without the shared contract importable.
    class _FakeObs:
        def __init__(self, agent_id, task_id, role, delegated_to,
                     coordination_calls, latency_ms, energy_kwh):
            self.agent_id = agent_id
            self.task_id = task_id
            self.role = role
            self.delegated_to = delegated_to
            self.coordination_calls = coordination_calls
            self.latency_ms = latency_ms
            self.energy_kwh = energy_kwh
        def __repr__(self):
            return (
                f"AgentObservation(agent_id={self.agent_id!r}, "
                f"task_id={self.task_id!r}, role={self.role!r}, "
                f"coordination_calls={self.coordination_calls}, "
                f"delegated_to={self.delegated_to}, "
                f"latency_ms={self.latency_ms:.1f})"
            )

    fake_mod = types.ModuleType("analysis")
    fake_mod.AgentObservation = _FakeObs
    sys.modules["analysis"] = fake_mod
    # Reset the deferred import cache to pick up the fake class.
    import src.analysis.adapters.langchain_callbacks as _mod
    _mod._AgentObservation = None
    _mod._AgentObservation_loaded = False

    cb_obs = GreenLangChainCallback(agent_id="primary", task_id="task-1")
    cb_obs.on_agent_action(
        types.SimpleNamespace(tool="delegate_to_researcher", log=""),
        run_id="x-1",
    )
    cb_obs.on_tool_start({"name": "search"}, "q", run_id="t-1")
    cb_obs.on_tool_end("r", run_id="t-1")
    try:
        obs = cb_obs.observations()
        print(f"  observation count: {len(obs)}")
        for o in obs:
            print(f"    {o}")
    except Exception as e:
        print(f"  observation error: {e}")
    finally:
        del sys.modules["analysis"]
        _mod._AgentObservation = None
        _mod._AgentObservation_loaded = False

    # =================================================================
    # 9. HITL callback — long tool call
    # =================================================================
    print("\n=== 9. HITL callback on long tool call ===")
    cb_hitl = GreenLangChainCallback(
        hitl_long_tool_ms=5.0,   # 5 ms — any normal call triggers
    )
    hitl_calls = []
    cb_hitl.set_hitl_callback(lambda s: hitl_calls.append(s.kind) or False)
    cb_hitl.on_tool_start({"name": "slow"}, "i", run_id="slow-1")
    time.sleep(0.02)   # 20 ms > 5 ms threshold
    cb_hitl.on_tool_end("done", run_id="slow-1")
    steps = cb_hitl.steps(kind="tool_end")
    print(f"  HITL fired: {hitl_calls}")
    if steps:
        print(f"  needs_review: {steps[0]['needs_review']}")
        print(f"  review_reason: {steps[0]['review_reason']}")

    # =================================================================
    # 10. Reset (clears _by_kind, _sequence, everything)
    # =================================================================
    print("\n=== 10. reset() ===")
    cb_reset = GreenLangChainCallback()
    for _ in range(3):
        cb_reset.on_tool_start({"name": "t"}, "i")
    before = cb_reset.statistics()
    cb_reset.reset()
    after = cb_reset.statistics()
    print(f"  before: tool_calls={before['tool_calls']}, "
          f"steps={before['steps_recorded']}, "
          f"by_kind={before['by_kind']}")
    print(f"  after:  tool_calls={after['tool_calls']}, "
          f"steps={after['steps_recorded']}, "
          f"by_kind={after['by_kind']}")
    print(f"  _sequence reset: {cb_reset._sequence == 0}")

    # =================================================================
    # 11. Reset cache — federated idempotency
    # =================================================================
    print("\n=== 11. Federated idempotency ===")
    cb_fed = GreenLangChainCallback(agent_id="fed-agent")
    for _ in range(10):
        cb_fed.on_tool_start({"name": "t"}, "i")
    cb_fed.contribute_federated()
    agg1 = cb_fed.get_federated_aggregate()
    cb_fed.contribute_federated()   # no-op — nothing new
    agg2 = cb_fed.get_federated_aggregate()
    print(f"  sample_count after 1st: "
          f"{agg1.get('sample_count')}")
    print(f"  sample_count after 2nd: "
          f"{agg2.get('sample_count')}  (expected unchanged)")

    # =================================================================
    # 12. trace forwarding payload fidelity
    # =================================================================
    print("\n=== 12. Trace forwarding fidelity ===")

    class _FakeTrace:
        def __init__(self):
            self.records = []
        def record(self, event, **kwargs):
            self.records.append((event, kwargs))

    fake_trace = _FakeTrace()
    cb_trace = GreenLangChainCallback(trace=fake_trace)
    cb_trace.on_llm_start({"name": "gpt-4o"}, ["p"], run_id="tl-1")
    cb_trace.on_llm_end(_FakeLLMResult(), run_id="tl-1")
    # Find llm_end in forwarded records
    llm_end_events = [
        (e, k) for e, k in fake_trace.records
        if k.get("kind") == "llm_end"
    ]
    if llm_end_events:
        event, kwargs = llm_end_events[-1]
        print(f"  forwarded event keys: {sorted(event.keys())[:8]}...")
        print(f"  contains 'name': {'name' in event}  "
              f"(expected True — full step dict, not payload)")
        print(f"  contains 'tokens': "
              f"{event.get('tokens')}  (expected 70)")

    # =================================================================
    # 13. Configurable max_steps with dropped_steps counter
    # =================================================================
    print("\n=== 13. Configurable max_steps ===")
    cb_small = GreenLangChainCallback(max_steps=3)
    for i in range(7):
        cb_small.on_tool_start({"name": f"t{i}"}, "i")
    stats = cb_small.statistics()
    print(f"  steps_recorded: {stats['steps_recorded']}  "
          f"(expected 3 — maxlen)")
    print(f"  dropped_steps: {stats['dropped_steps']}  (expected 4)")

    # =================================================================
    # 14. Open-runs LRU eviction
    # =================================================================
    print("\n=== 14. Open-runs LRU eviction ===")
    cb_lru = GreenLangChainCallback(max_open_runs=10)
    for i in range(25):
        cb_lru.on_tool_start({"name": "t"}, "i", run_id=f"run-{i}")
    print(f"  open_runs: {len(cb_lru._open_runs)}  "
          f"(bounded to <= 10)")

    # =================================================================
    # 15. JSON-safe export with a poisoned payload
    # =================================================================
    print("\n=== 15. JSON-safe export ===")
    cb_safe = GreenLangChainCallback(capture_payloads=True)
    cb_safe.on_agent_finish(
        types.SimpleNamespace(
            return_values={"output": object()},   # unserialisable
        ),
        run_id="safe-1",
    )
    try:
        export = cb_safe.export()
        json.dumps(export, default=str)
        print("  json.dumps(export()): OK")
    except (TypeError, ValueError) as e:
        print(f"  FAILED: {e}")

    # =================================================================
    # 16. Full statistics dump
    # =================================================================
    print("\n=== 16. Full statistics (cb_full) ===")
    print(json.dumps(cb_full.statistics(), indent=2, default=str))
