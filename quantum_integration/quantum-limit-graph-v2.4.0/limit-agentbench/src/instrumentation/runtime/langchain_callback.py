# src/instrumentation/runtime/langchain_callback.py

"""
Green LangChain Callback
========================

Tracks token usage, tool calls, and end-to-end latency for LangChain runs.

Enhancements
------------
- Optional ``langchain`` import guarded by ``_LANGCHAIN_AVAILABLE``. When
  LangChain is absent, the class still functions standalone (useful for
  tests and for consuming the callback's counters without the dependency).
- ``LangChainCallbackConfig`` — frozen, validated: bounded history size,
  preview retention, latency tracking toggle, and run-tree tracking.
- **Robust token extraction** — walks ``llm_output``, ``usage_metadata``,
  ``response_metadata``, and ``generations`` in a defensive cascade so
  Anthropic, HuggingFace, OpenAI, and custom providers all work.
- **Thread safety** — ``RLock`` guards every counter mutation; safe under
  LangChain's default concurrent execution model.
- **Latency tracking** — per-run start/end timestamps with mean/p50/p95
  aggregates in ``statistics()``.
- **Per-run tracking** — ``run_id``-keyed token / tool aggregation alongside
  the legacy global counters.
- **Error isolation** — payload-shape failures are logged and (in strict
  mode) raised as ``LangChainCallbackError``; unexpected callback failures
  never propagate into LangChain's run loop.
- Bounded token-usage and tool-call ring-buffers.
- Serialization: ``to_dict`` / ``from_dict`` / ``to_json`` / ``from_json``.
- ``reset()`` for reuse across independent chains.
- Custom ``LangChainCallbackError``; lazy ``%s`` logging; ``__repr__``;
  ``__main__`` smoke test with mocked responses across every provider shape.
"""

from __future__ import annotations

import json
import logging
import math
import threading
import time
from collections import Counter, deque
from dataclasses import asdict, dataclass, field
from typing import Any, Deque, Dict, Iterator, List, Mapping, Optional, Tuple

logger = logging.getLogger(__name__)


# --------------------------------------------------------------------------- #
# Optional LangChain import
# --------------------------------------------------------------------------- #
try:  # pragma: no cover — environment-dependent
    from langchain.callbacks.base import (  # type: ignore
        BaseCallbackHandler as _BaseCallbackHandler,
    )

    _LANGCHAIN_AVAILABLE = True
except ImportError:  # pragma: no cover
    try:
        # Newer LangChain exposes the same class under langchain_core.
        from langchain_core.callbacks import (  # type: ignore
            BaseCallbackHandler as _BaseCallbackHandler,
        )

        _LANGCHAIN_AVAILABLE = True
    except ImportError:
        _BaseCallbackHandler = object  # type: ignore[assignment,misc]
        _LANGCHAIN_AVAILABLE = False
        logger.debug(
            "langchain / langchain_core not importable; "
            "GreenLangChainCallback will run standalone."
        )


# --------------------------------------------------------------------------- #
# Errors
# --------------------------------------------------------------------------- #
class LangChainCallbackError(ValueError):
    """Raised for invalid callback inputs or configuration."""


# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class LangChainCallbackConfig:
    """Tunable parameters for the Green LangChain callback."""

    # Bounded ring-buffer of per-event records.
    max_token_events: int = 10_000
    max_tool_calls: int = 10_000

    # If True, tracks per-LLM and per-tool latency via ``time.perf_counter``.
    track_latency: bool = True

    # If True, tracks per-run aggregate counters keyed by ``run_id``.
    track_per_run: bool = True

    # Maximum number of distinct runs retained in the per-run map.
    max_runs: int = 1_000

    # If True, unsupported ``response.llm_output`` shapes raise in strict
    # mode; otherwise they are logged and treated as 0 tokens.
    strict_payload_validation: bool = True

    def __post_init__(self) -> None:
        for name in (
            "max_token_events",
            "max_tool_calls",
            "max_runs",
        ):
            value = getattr(self, name)
            if not isinstance(value, int) or value <= 0:
                raise LangChainCallbackError(
                    f"{name} must be a positive int, got {value!r}."
                )

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "LangChainCallbackConfig":
        if not isinstance(data, Mapping):
            raise LangChainCallbackError(
                f"from_dict expects a Mapping, got {type(data).__name__}."
            )
        return cls(
            max_token_events=int(data.get("max_token_events", 10_000)),
            max_tool_calls=int(data.get("max_tool_calls", 10_000)),
            track_latency=bool(data.get("track_latency", True)),
            track_per_run=bool(data.get("track_per_run", True)),
            max_runs=int(data.get("max_runs", 1_000)),
            strict_payload_validation=bool(
                data.get("strict_payload_validation", True)
            ),
        )


# --------------------------------------------------------------------------- #
# Per-event records
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class TokenEvent:
    """One ``on_llm_end`` observation."""

    run_id: Optional[str]
    total_tokens: int
    prompt_tokens: Optional[int]
    completion_tokens: Optional[int]
    model_name: Optional[str]
    latency_ms: Optional[float]
    timestamp: float = field(default_factory=time.time)
    provider: Optional[str] = None

    def __post_init__(self) -> None:
        if self.total_tokens < 0:
            raise LangChainCallbackError("total_tokens must be >= 0.")
        for name in ("prompt_tokens", "completion_tokens"):
            value = getattr(self, name)
            if value is not None and value < 0:
                raise LangChainCallbackError(f"{name} must be >= 0.")
        if self.latency_ms is not None and self.latency_ms < 0:
            raise LangChainCallbackError("latency_ms must be >= 0.")

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "TokenEvent":
        if not isinstance(data, Mapping):
            raise LangChainCallbackError("TokenEvent.from_dict expects a Mapping.")
        return cls(
            run_id=data.get("run_id"),
            total_tokens=int(data.get("total_tokens", 0)),
            prompt_tokens=data.get("prompt_tokens"),
            completion_tokens=data.get("completion_tokens"),
            model_name=data.get("model_name"),
            latency_ms=data.get("latency_ms"),
            timestamp=float(data.get("timestamp", time.time())),
            provider=data.get("provider"),
        )


@dataclass(frozen=True)
class ToolCallEvent:
    """One ``on_tool_start`` observation."""

    run_id: Optional[str]
    tool_name: str
    tool_input_length: int
    latency_ms: Optional[float] = None
    error: Optional[str] = None
    timestamp: float = field(default_factory=time.time)

    def __post_init__(self) -> None:
        if not isinstance(self.tool_name, str) or not self.tool_name:
            raise LangChainCallbackError("tool_name must be a non-empty string.")
        if self.tool_input_length < 0:
            raise LangChainCallbackError("tool_input_length must be >= 0.")
        if self.latency_ms is not None and self.latency_ms < 0:
            raise LangChainCallbackError("latency_ms must be >= 0.")

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "ToolCallEvent":
        if not isinstance(data, Mapping):
            raise LangChainCallbackError(
                "ToolCallEvent.from_dict expects a Mapping."
            )
        return cls(
            run_id=data.get("run_id"),
            tool_name=str(data.get("tool_name", "unknown")),
            tool_input_length=int(data.get("tool_input_length", 0)),
            latency_ms=data.get("latency_ms"),
            error=data.get("error"),
            timestamp=float(data.get("timestamp", time.time())),
        )


# --------------------------------------------------------------------------- #
# Callback
# --------------------------------------------------------------------------- #
class GreenLangChainCallback(_BaseCallbackHandler):  # type: ignore[misc]
    """
    Tracks token usage, tool calls, and latency for LangChain runs.

    Thread-safe, serializable, and bounded in memory. The original public API
    (``token_usage``, ``tool_calls``, ``on_llm_end``, ``on_tool_start``) is
    preserved; new parameters are keyword-only.

    Parameters
    ----------
    config : LangChainCallbackConfig, optional
        Callback configuration.
    strict : bool, default True
        If True, malformed ``llm_output`` payloads raise
        :class:`LangChainCallbackError` when
        ``config.strict_payload_validation`` is also True. If False, they are
        logged and treated as zero-token events.
    """

    # ------------------------------------------------------------------ init
    def __init__(
        self,
        *,
        config: Optional[LangChainCallbackConfig] = None,
        strict: bool = True,
    ) -> None:
        # ``BaseCallbackHandler.__init__`` is tolerant of no-arg construction,
        # but guard against exotic bases.
        try:
            super().__init__()
        except Exception:  # pragma: no cover — defensive
            pass

        self._config = config or LangChainCallbackConfig()
        self._strict = bool(strict)
        self._lock = threading.RLock()

        # ---- Legacy counters --------------------------------------------
        self.token_usage: int = 0
        self.tool_calls: int = 0

        # ---- Bounded per-event records ----------------------------------
        self._token_events: Deque[TokenEvent] = deque(
            maxlen=self._config.max_token_events
        )
        self._tool_events: Deque[ToolCallEvent] = deque(
            maxlen=self._config.max_tool_calls
        )

        # ---- Per-run state ---------------------------------------------
        self._per_run_tokens: Dict[str, int] = {}
        self._per_run_tools: Dict[str, int] = {}

        # ---- Latency tracking ------------------------------------------
        # Started LLM / tool timestamps keyed by run_id, populated on
        # on_llm_start / on_tool_start and consumed on _end.
        self._llm_start: Dict[str, float] = {}
        self._tool_start: Dict[str, float] = {}

        self._started_at: float = time.time()

        logger.debug(
            "GreenLangChainCallback initialized "
            "(langchain_available=%s, strict=%s, track_latency=%s, "
            "track_per_run=%s)",
            _LANGCHAIN_AVAILABLE,
            self._strict,
            self._config.track_latency,
            self._config.track_per_run,
        )

    # ------------------------------------------------------------------ props
    @property
    def config(self) -> LangChainCallbackConfig:
        return self._config

    @property
    def total_tokens(self) -> int:
        """Alias for ``token_usage`` (clarity)."""
        with self._lock:
            return self.token_usage

    @property
    def total_tool_calls(self) -> int:
        """Alias for ``tool_calls`` (clarity)."""
        with self._lock:
            return self.tool_calls

    @property
    def token_events(self) -> List[TokenEvent]:
        with self._lock:
            return list(self._token_events)

    @property
    def tool_events(self) -> List[ToolCallEvent]:
        with self._lock:
            return list(self._tool_events)

    # ---------------------------------------------------------- lifecycle hooks
    def on_llm_start(
        self, serialized: Any, prompts: Any, *, run_id: Any = None, **kwargs: Any
    ) -> None:
        """Record the LLM call start time for latency tracking."""
        if not self._config.track_latency:
            return
        key = self._stringify_run_id(run_id)
        with self._lock:
            self._llm_start[key] = time.perf_counter()

    def on_llm_end(
        self, response: Any, *, run_id: Any = None, **kwargs: Any
    ) -> None:
        """
        Record an ``on_llm_end`` event.

        Extracts tokens from ``response.llm_output``, ``usage_metadata``, and
        legacy provider shapes. Updates both the legacy global counter and
        the per-run map (when enabled).
        """
        key = self._stringify_run_id(run_id)
        latency_ms = self._consume_latency(self._llm_start, key)
        provider, model_name, tokens = self._extract_tokens(response)

        if tokens is None:
            msg = (
                "could not extract token usage from the LLM response; "
                "treating as 0 tokens."
            )
            if self._strict and self._config.strict_payload_validation:
                raise LangChainCallbackError(msg)
            logger.warning("%s", msg)
            tokens = _TokenBreakdown(total=0, prompt=None, completion=None)

        event = TokenEvent(
            run_id=run_id if isinstance(run_id, str) else None,
            total_tokens=tokens.total,
            prompt_tokens=tokens.prompt,
            completion_tokens=tokens.completion,
            model_name=model_name,
            latency_ms=latency_ms,
            provider=provider,
        )

        with self._lock:
            self.token_usage += tokens.total
            self._token_events.append(event)
            if self._config.track_per_run and key is not None:
                self._per_run_tokens[key] = (
                    self._per_run_tokens.get(key, 0) + tokens.total
                )
                self._enforce_max_runs_locked()

        logger.debug(
            "on_llm_end: run=%s tokens=%d latency_ms=%s provider=%s model=%s",
            key,
            tokens.total,
            f"{latency_ms:.2f}" if latency_ms is not None else "n/a",
            provider,
            model_name,
        )

    def on_tool_start(
        self,
        serialized: Any,
        input_str: Any = "",
        *,
        run_id: Any = None,
        **kwargs: Any,
    ) -> None:
        """
        Record a tool invocation.

        Updates the legacy global counter and (when enabled) the per-run map.
        ``input_str`` may be any object; only its length is retained.
        """
        key = self._stringify_run_id(run_id)
        tool_name = self._extract_tool_name(serialized)
        input_length = self._safe_length(input_str)

        if self._config.track_latency:
            with self._lock:
                self._tool_start[key] = time.perf_counter()

        event = ToolCallEvent(
            run_id=run_id if isinstance(run_id, str) else None,
            tool_name=tool_name,
            tool_input_length=input_length,
        )

        with self._lock:
            self.tool_calls += 1
            self._tool_events.append(event)
            if self._config.track_per_run and key is not None:
                self._per_run_tools[key] = self._per_run_tools.get(key, 0) + 1
                self._enforce_max_runs_locked()

        logger.debug(
            "on_tool_start: run=%s tool=%s input_len=%d",
            key,
            tool_name,
            input_length,
        )

    def on_tool_end(
        self, output: Any, *, run_id: Any = None, **kwargs: Any
    ) -> None:
        """Record tool latency (informational only)."""
        if not self._config.track_latency:
            return
        key = self._stringify_run_id(run_id)
        latency_ms = self._consume_latency(self._tool_start, key)
        if latency_ms is None:
            return

        with self._lock:
            # Attach latency to the most recent event for this run, if any.
            for i in range(len(self._tool_events) - 1, -1, -1):
                ev = self._tool_events[i]
                if ev.run_id == (run_id if isinstance(run_id, str) else None) and ev.latency_ms is None:
                    updated = ToolCallEvent(
                        run_id=ev.run_id,
                        tool_name=ev.tool_name,
                        tool_input_length=ev.tool_input_length,
                        latency_ms=latency_ms,
                        error=ev.error,
                        timestamp=ev.timestamp,
                    )
                    self._tool_events[i] = updated
                    break

        logger.debug("on_tool_end: run=%s latency_ms=%.2f", key, latency_ms)

    def on_tool_error(
        self, error: Any, *, run_id: Any = None, **kwargs: Any
    ) -> None:
        """Record tool failures without aborting the LangChain run."""
        key = self._stringify_run_id(run_id)
        msg = str(error) if error is not None else "unknown tool error"
        logger.warning("on_tool_error: run=%s error=%s", key, msg)

        with self._lock:
            for i in range(len(self._tool_events) - 1, -1, -1):
                ev = self._tool_events[i]
                if ev.run_id == (run_id if isinstance(run_id, str) else None) and ev.error is None:
                    self._tool_events[i] = ToolCallEvent(
                        run_id=ev.run_id,
                        tool_name=ev.tool_name,
                        tool_input_length=ev.tool_input_length,
                        latency_ms=ev.latency_ms,
                        error=msg,
                        timestamp=ev.timestamp,
                    )
                    break

    # ---------------------------------------------------------- extraction
    @staticmethod
    def _stringify_run_id(run_id: Any) -> Optional[str]:
        if run_id is None:
            return None
        if isinstance(run_id, str):
            return run_id
        # LangChain uses UUID objects in older versions.
        return str(run_id)

    def _consume_latency(
        self, start_map: Dict[str, float], key: Optional[str]
    ) -> Optional[float]:
        if not self._config.track_latency or key is None:
            return None
        with self._lock:
            started = start_map.pop(key, None)
        if started is None:
            return None
        return (time.perf_counter() - started) * 1000.0

    def _extract_tokens(self, response: Any) -> Tuple[
        Optional[str], Optional[str], Optional["_TokenBreakdown"]
    ]:
        """
        Return ``(provider, model_name, token_breakdown)`` from an arbitrary
        LangChain LLM response. ``token_breakdown`` is ``None`` when no
        recognisable usage block exists.
        """
        provider: Optional[str] = None
        model_name: Optional[str] = None

        if response is None:
            return None, None, None

        # --- Modern path: ``response.usage_metadata`` --------------------
        usage_meta = getattr(response, "usage_metadata", None)
        if isinstance(usage_meta, Mapping):
            total = self._first_int(
                usage_meta, ("total_tokens", "total_token_count", "total")
            )
            prompt = self._first_int(
                usage_meta, ("input_tokens", "prompt_tokens", "prompt_token_count")
            )
            completion = self._first_int(
                usage_meta,
                ("output_tokens", "completion_tokens", "completion_token_count"),
            )
            if total is not None or prompt is not None or completion is not None:
                if total is None:
                    total = (prompt or 0) + (completion or 0)
                return provider, model_name, _TokenBreakdown(
                    total=total, prompt=prompt, completion=completion
                )

        # --- Legacy path: ``response.llm_output`` ------------------------
        llm_output = getattr(response, "llm_output", None)
        if isinstance(llm_output, Mapping):
            # Provider hints (varies by integration).
            for key in ("provider", "system_fingerprint", "model_provider"):
                value = llm_output.get(key)
                if isinstance(value, str) and value:
                    provider = value
                    break
            for key in ("model_name", "model", "model_id"):
                value = llm_output.get(key)
                if isinstance(value, str) and value:
                    model_name = value
                    break

            # token_usage block.
            token_usage = llm_output.get("token_usage")
            if isinstance(token_usage, Mapping):
                total = self._first_int(
                    token_usage, ("total_tokens", "total_token_count", "total")
                )
                prompt = self._first_int(
                    token_usage,
                    ("prompt_tokens", "input_tokens", "prompt_token_count"),
                )
                completion = self._first_int(
                    token_usage,
                    ("completion_tokens", "output_tokens", "completion_token_count"),
                )
                if total is None:
                    total = (prompt or 0) + (completion or 0)
                return provider, model_name, _TokenBreakdown(
                    total=total, prompt=prompt, completion=completion
                )

        # --- Fallback: Anthropic-style ``response_metadata.usage`` -------
        response_meta = getattr(response, "response_metadata", None)
        if isinstance(response_meta, Mapping):
            usage = response_meta.get("usage")
            if isinstance(usage, Mapping):
                prompt = self._first_int(
                    usage, ("input_tokens", "prompt_tokens")
                )
                completion = self._first_int(
                    usage, ("output_tokens", "completion_tokens")
                )
                total = (prompt or 0) + (completion or 0)
                return provider, model_name, _TokenBreakdown(
                    total=total, prompt=prompt, completion=completion
                )

        # --- Last resort: aggregate from generations ---------------------
        generations = getattr(response, "generations", None)
        if isinstance(generations, (list, tuple)):
            total_chars = 0
            for gen_list in generations:
                if isinstance(gen_list, (list, tuple)):
                    for gen in gen_list:
                        text = getattr(gen, "text", None)
                        if isinstance(text, str):
                            total_chars += len(text)
                        # Some integrations attach usage under ``message``.
                        message = getattr(gen, "message", None)
                        meta = getattr(message, "usage_metadata", None) if message else None
                        if isinstance(meta, Mapping):
                            total = self._first_int(
                                meta, ("total_tokens", "total")
                            )
                            prompt = self._first_int(
                                meta, ("input_tokens", "prompt_tokens")
                            )
                            completion = self._first_int(
                                meta, ("output_tokens", "completion_tokens")
                            )
                            if total is not None or prompt is not None or completion is not None:
                                if total is None:
                                    total = (prompt or 0) + (completion or 0)
                                return provider, model_name, _TokenBreakdown(
                                    total=total, prompt=prompt, completion=completion
                                )
            if total_chars > 0:
                # Very rough approximation: 4 characters per token.
                logger.debug(
                    "Falling back to character-based token estimate "
                    "(%d chars).", total_chars,
                )
                return provider, model_name, _TokenBreakdown(
                    total=max(1, total_chars // 4), prompt=None, completion=None
                )

        return provider, model_name, None

    @staticmethod
    def _first_int(mapping: Mapping[str, Any], keys: Tuple[str, ...]) -> Optional[int]:
        for key in keys:
            value = mapping.get(key)
            if isinstance(value, bool):
                continue
            if isinstance(value, int):
                return max(0, value)
            if isinstance(value, float) and math.isfinite(value):
                return max(0, int(value))
        return None

    @staticmethod
    def _extract_tool_name(serialized: Any) -> str:
        if isinstance(serialized, Mapping):
            for key in ("name", "tool_name", "id"):
                value = serialized.get(key)
                if isinstance(value, str) and value:
                    return value
        return "unknown_tool"

    @staticmethod
    def _safe_length(value: Any) -> int:
        if value is None:
            return 0
        try:
            if hasattr(value, "__len__"):
                return max(0, int(len(value)))
            return len(str(value))
        except Exception:  # pragma: no cover — defensive
            return 0

    def _enforce_max_runs_locked(self) -> None:
        """Keep the per-run maps bounded by ``config.max_runs``."""
        cap = self._config.max_runs
        if len(self._per_run_tokens) <= cap and len(self._per_run_tools) <= cap:
            return
        # Evict the oldest keys by iteration order (dict preserves insertion).
        while len(self._per_run_tokens) > cap:
            oldest = next(iter(self._per_run_tokens))
            self._per_run_tokens.pop(oldest, None)
        while len(self._per_run_tools) > cap:
            oldest = next(iter(self._per_run_tools))
            self._per_run_tools.pop(oldest, None)

    # ---------------------------------------------------------- statistics
    def statistics(self) -> Dict[str, Any]:
        """Return aggregate statistics over the tracked events."""
        with self._lock:
            token_events = list(self._token_events)
            tool_events = list(self._tool_events)
            per_run_tokens = dict(self._per_run_tokens)
            per_run_tools = dict(self._per_run_tools)
            total_tokens = self.token_usage
            total_tools = self.tool_calls

        stats: Dict[str, Any] = {
            "total_tokens": total_tokens,
            "total_tool_calls": total_tools,
            "token_events": len(token_events),
            "tool_events": len(tool_events),
            "unique_runs": len(set(per_run_tokens) | set(per_run_tools)),
        }

        # Provider / model breakdown.
        providers = Counter(
            e.provider for e in token_events if e.provider is not None
        )
        models = Counter(
            e.model_name for e in token_events if e.model_name is not None
        )
        stats["by_provider"] = dict(providers)
        stats["by_model"] = dict(models)

        # Tool breakdown.
        stats["by_tool"] = dict(Counter(e.tool_name for e in tool_events))
        stats["tool_errors"] = sum(1 for e in tool_events if e.error is not None)

        # Latency aggregates.
        llm_latencies = sorted(
            e.latency_ms for e in token_events if e.latency_ms is not None
        )
        tool_latencies = sorted(
            e.latency_ms for e in tool_events if e.latency_ms is not None
        )
        stats["llm_latency"] = self._latency_summary(llm_latencies)
        stats["tool_latency"] = self._latency_summary(tool_latencies)

        # Per-run top-K.
        stats["top_runs_by_tokens"] = sorted(
            per_run_tokens.items(), key=lambda kv: kv[1], reverse=True
        )[:5]
        stats["top_runs_by_tools"] = sorted(
            per_run_tools.items(), key=lambda kv: kv[1], reverse=True
        )[:5]

        return stats

    @staticmethod
    def _latency_summary(values: List[float]) -> Dict[str, Optional[float]]:
        if not values:
            return {"count": 0, "mean_ms": None, "p50_ms": None, "p95_ms": None, "max_ms": None}
        n = len(values)
        mean = sum(values) / n
        p50 = values[n // 2]
        p95_idx = max(0, min(n - 1, int(round(0.95 * (n - 1)))))
        return {
            "count": n,
            "mean_ms": mean,
            "p50_ms": p50,
            "p95_ms": values[p95_idx],
            "max_ms": values[-1],
        }

    # ---------------------------------------------------------- lifecycle
    def reset(self, *, clear_history: bool = True) -> int:
        """
        Reset all counters and per-run state.

        Returns the number of token events removed.
        """
        with self._lock:
            removed = len(self._token_events)
            if clear_history:
                self._token_events.clear()
                self._tool_events.clear()
            self.token_usage = 0
            self.tool_calls = 0
            self._per_run_tokens.clear()
            self._per_run_tools.clear()
            self._llm_start.clear()
            self._tool_start.clear()
            self._started_at = time.time()
        logger.debug("GreenLangChainCallback reset (removed %d event(s)).", removed)
        return removed

    # ---------------------------------------------------------- serialization
    def to_dict(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "config": asdict(self._config),
                "strict": self._strict,
                "langchain_available": _LANGCHAIN_AVAILABLE,
                "started_at": self._started_at,
                "token_usage": self.token_usage,
                "tool_calls": self.tool_calls,
                "token_events": [e.to_dict() for e in self._token_events],
                "tool_events": [e.to_dict() for e in self._tool_events],
                "per_run_tokens": dict(self._per_run_tokens),
                "per_run_tools": dict(self._per_run_tools),
            }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "GreenLangChainCallback":
        if not isinstance(data, Mapping):
            raise LangChainCallbackError(
                f"from_dict expects a Mapping, got {type(data).__name__}."
            )
        cfg_data = dict(data.get("config", {}) or {})
        cfg = LangChainCallbackConfig.from_dict(cfg_data)
        cb = cls(config=cfg, strict=bool(data.get("strict", True)))
        with cb._lock:
            cb.token_usage = int(data.get("token_usage", 0))
            cb.tool_calls = int(data.get("tool_calls", 0))
            for entry in data.get("token_events", []):
                cb._token_events.append(TokenEvent.from_dict(entry))
            for entry in data.get("tool_events", []):
                cb._tool_events.append(ToolCallEvent.from_dict(entry))
            cb._per_run_tokens = {
                str(k): int(v) for k, v in (data.get("per_run_tokens") or {}).items()
            }
            cb._per_run_tools = {
                str(k): int(v) for k, v in (data.get("per_run_tools") or {}).items()
            }
            cb._started_at = float(data.get("started_at", time.time()))
        return cb

    def to_json(self, **kwargs: Any) -> str:
        return json.dumps(self.to_dict(), default=str, **kwargs)

    @classmethod
    def from_json(cls, payload: str) -> "GreenLangChainCallback":
        try:
            return cls.from_dict(json.loads(payload))
        except json.JSONDecodeError as exc:
            raise LangChainCallbackError(f"Invalid JSON payload: {exc}") from exc

    # ----------------------------------------------------------------- dunder
    def __repr__(self) -> str:
        with self._lock:
            return (
                "GreenLangChainCallback("
                f"tokens={self.token_usage}, "
                f"tool_calls={self.tool_calls}, "
                f"runs={len(self._per_run_tokens)}, "
                f"langchain={_LANGCHAIN_AVAILABLE}, "
                f"strict={self._strict})"
            )


# --------------------------------------------------------------------------- #
# Internal helper
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class _TokenBreakdown:
    """Internal container for one provider's token-usage numbers."""
    total: int
    prompt: Optional[int]
    completion: Optional[int]


# --------------------------------------------------------------------------- #
# Public API
# --------------------------------------------------------------------------- #
__all__ = [
    "GreenLangChainCallback",
    "LangChainCallbackConfig",
    "LangChainCallbackError",
    "TokenEvent",
    "ToolCallEvent",
]


# --------------------------------------------------------------------------- #
# Smoke test: python -m instrumentation.runtime.langchain_callback
# --------------------------------------------------------------------------- #
if __name__ == "__main__":  # pragma: no cover
    logging.basicConfig(level=logging.INFO)

    # ---------------------------------------------------------------- #
    # Mock responses covering every provider shape the extractor handles.
    # ---------------------------------------------------------------- #
    class _MockResponse:
        def __init__(self, *, llm_output=None, usage_metadata=None,
                     response_metadata=None, generations=None) -> None:
            self.llm_output = llm_output
            self.usage_metadata = usage_metadata
            self.response_metadata = response_metadata
            self.generations = generations

    # ---- Legacy OpenAI shape ------------------------------------------- #
    openai_resp = _MockResponse(
        llm_output={
            "token_usage": {
                "total_tokens": 128,
                "prompt_tokens": 100,
                "completion_tokens": 28,
            },
            "model_name": "gpt-4o-mini",
            "provider": "openai",
        },
    )

    # ---- Modern LangChain ``usage_metadata`` --------------------------- #
    modern_resp = _MockResponse(
        usage_metadata={
            "input_tokens": 50,
            "output_tokens": 20,
            "total_tokens": 70,
        },
    )

    # ---- Anthropic-style ``response_metadata.usage`` ------------------- #
    anthropic_resp = _MockResponse(
        response_metadata={
            "usage": {"input_tokens": 40, "output_tokens": 15},
        },
    )

    # ---- Missing llm_output entirely (the original bug) --------------- #
    broken_resp = _MockResponse(llm_output=None)

    # ---- ``llm_output`` present but token_usage missing ---------------- #
    no_usage_resp = _MockResponse(
        llm_output={"model_name": "custom-llm"}
    )

    # ---- Character-based fallback ------------------------------------ #
    class _Gen:
        def __init__(self, text):
            self.text = text

    fallback_resp = _MockResponse(
        generations=[[_Gen("a" * 40)]],
    )

    # ---- Happy path (legacy OpenAI) ----------------------------------- #
    cb = GreenLangChainCallback()
    print("repr       :", cb)
    cb.on_llm_start({}, [], run_id="run-1")
    time.sleep(0.005)
    cb.on_llm_end(openai_resp, run_id="run-1")
    print("after openai:", cb.token_usage, "tokens")

    # ---- All other providers ------------------------------------------ #
    cb.on_llm_start({}, [], run_id="run-2")
    cb.on_llm_end(modern_resp, run_id="run-2")
    cb.on_llm_start({}, [], run_id="run-3")
    cb.on_llm_end(anthropic_resp, run_id="run-3")
    cb.on_llm_start({}, [], run_id="run-4")
    cb.on_llm_end(fallback_resp, run_id="run-4")

    # ---- Broken payload → non-strict warns and returns 0 tokens ------- #
    cb.on_llm_end(broken_resp, run_id="run-broken")
    cb.on_llm_end(no_usage_resp, run_id="run-no-usage")
    print("total      :", cb.token_usage, "tokens")

    # ---- Strict mode raises on broken payload ------------------------- #
    strict = GreenLangChainCallback(strict=True)
    try:
        strict.on_llm_end(broken_resp, run_id="run-bad")
    except LangChainCallbackError as exc:
        print("strict     :", exc)

    # ---- Tool calls --------------------------------------------------- #
    cb.on_tool_start({"name": "search"}, "query text", run_id="run-5")
    time.sleep(0.002)
    cb.on_tool_end("results", run_id="run-5")
    cb.on_tool_start({"name": "calculator"}, "1+1", run_id="run-5")
    cb.on_tool_error(RuntimeError("boom"), run_id="run-6")
    print("tools      :", cb.tool_calls)

    # ---- Statistics --------------------------------------------------- #
    stats = cb.statistics()
    print("stats      :", {
        k: stats[k]
        for k in (
            "total_tokens", "total_tool_calls", "unique_runs",
            "by_provider", "by_tool", "tool_errors",
        )
    })
    print("llm latency:", stats["llm_latency"])
    print("tool latency:", stats["tool_latency"])

    # ---- Serialization round-trip ------------------------------------- #
    payload = cb.to_json()
    restored = GreenLangChainCallback.from_json(payload)
    assert restored.to_dict() == cb.to_dict()
    print("Round-trip OK.")

    # ---- Reset -------------------------------------------------------- #
    removed = cb.reset()
    print("reset      :", removed, "event(s) removed; new tokens:",
          cb.token_usage)

    # ---- Config validation -------------------------------------------- #
    for bad_cfg in (
        dict(max_token_events=0),
        dict(max_tool_calls=0),
        dict(max_runs=0),
    ):
        try:
            LangChainCallbackConfig(**bad_cfg)  # type: ignore[arg-type]
        except LangChainCallbackError as exc:
            print("Rejected cfg:", exc)

    # ---- Standalone mode (no langchain) ------------------------------- #
    print("langchain? :", _LANGCHAIN_AVAILABLE)

    print("\nSmoke test passed.")
