# src/instrumentation/runtime/langchain_callback.py

"""
Green LangChain Callback
========================

Tracks token usage, tool calls, and end-to-end latency for LangChain runs.

Enhancements
------------
- **True error isolation** — every callback is wrapped by an isolation
  decorator. Unexpected exceptions are counted in ``callback_errors`` and
  (in ``strict=False``) never propagate into LangChain's run loop. The
  docstring's original promise is now actually honoured.
- **``on_llm_error`` implemented** — cleans up the pending ``_llm_start``
  entry, eliminating the leak on failed LLM calls. ``on_tool_error``
  also pops the ``_tool_start`` entry.
- **Bounded start-time dicts** — ``_llm_start`` / ``_tool_start`` evict
  the oldest entries when the map grows beyond ``max_runs * 2``. Combined
  with the error handlers, no leak remains.
- **Strict-boolean parsing** — ``from_dict`` rejects non-bool config
  values (``"track_latency": "false"`` no longer silently enables
  tracking). Non-bool ``strict`` raises at construction.
- **Strict type validation** in ``TokenEvent.from_dict`` and
  ``ToolCallEvent.from_dict`` — ``tool_name=null`` raises instead of
  becoming ``"None"``; ``prompt_tokens="abc"`` raises instead of
  propagating.
- **Consistent ``run_id``** — every event stores the **stringified**
  run_id (``_stringify_run_id(run_id)``). UUID run_ids no longer become
  ``None`` in the event.
- **``_TokenBreakdown`` moved before its users** — no forward references.
- **Standard nearest-rank percentiles** — p50/p95 match statistical
  tooling.
- **Read-only counters** — ``token_usage`` and ``tool_calls`` are
  properties; writes raise ``AttributeError``. Private ``_token_usage`` /
  ``_tool_calls`` remain.
- **New observability properties** — ``last_token_event``,
  ``last_tool_event``, ``pending_llm_runs``, ``pending_tool_runs``,
  ``callback_errors``.
- **``char_tokens_ratio`` config** — tune the character-based fallback
  for providers that don't report usage.
- **Guarded ``from_dict``** — ``int()`` / ``float()`` failures raise
  ``LangChainCallbackError``; ``token_events`` / ``tool_events`` must be
  lists; ``started_at`` must be finite and non-negative.
- ``__main__`` smoke test extended with strict-bool, `on_llm_error`,
  read-only-counter, and Bounded-start-map regression cases.

Note on ``strict`` vs ``strict_payload_validation``
---------------------------------------------------
These are independent:
* ``strict`` governs whether **unexpected exceptions** from the callback
  body raise. ``strict=False`` guarantees the callback never crashes the
  host.
* ``strict_payload_validation`` governs whether **bad payload shapes**
  (unknown token format, missing usage block) raise. It only applies
  when ``strict`` is also ``True`` (both flags must be on for a payload
  failure to raise).
"""

from __future__ import annotations

import functools
import json
import logging
import math
import numbers
import threading
import time
from collections import Counter, deque
from dataclasses import asdict, dataclass, field
from typing import (
    Any,
    Deque,
    Dict,
    Iterator,
    List,
    Mapping,
    Optional,
    Tuple,
)

logger = logging.getLogger(__name__)


# --------------------------------------------------------------------------- #
# Optional LangChain import
# --------------------------------------------------------------------------- #
try:  # pragma: no cover
    from langchain.callbacks.base import (  # type: ignore
        BaseCallbackHandler as _BaseCallbackHandler,
    )
    _LANGCHAIN_AVAILABLE = True
except ImportError:  # pragma: no cover
    try:
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


__all__ = [
    "GreenLangChainCallback",
    "LangChainCallbackConfig",
    "LangChainCallbackError",
    "TokenEvent",
    "ToolCallEvent",
]


# --------------------------------------------------------------------------- #
# Errors
# --------------------------------------------------------------------------- #
class LangChainCallbackError(ValueError):
    """Raised for invalid callback inputs or configuration."""


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #
def _require_positive_int(value: Any, label: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise LangChainCallbackError(
            f"{label} must be an int, not bool"
        )
    if value <= 0:
        raise LangChainCallbackError(f"{label} must be > 0")
    return value


def _require_non_negative_int(value: Any, label: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise LangChainCallbackError(
            f"{label} must be an int, not bool"
        )
    if value < 0:
        raise LangChainCallbackError(f"{label} must be >= 0")
    return value


def _require_bool(value: Any, label: str) -> bool:
    if not isinstance(value, bool):
        raise LangChainCallbackError(
            f"{label} must be a bool, got {type(value).__name__}"
        )
    return value


def _require_non_negative_finite_float(
    value: Any, label: str,
) -> float:
    if isinstance(value, bool) or not isinstance(value, numbers.Real):
        raise LangChainCallbackError(
            f"{label} must be a number, not bool"
        )
    fv = float(value)
    if not math.isfinite(fv):
        raise LangChainCallbackError(f"{label} must be finite")
    if fv < 0:
        raise LangChainCallbackError(f"{label} must be >= 0")
    return fv


def _strict_bool_from_dict(
    value: Any, label: str, default: bool,
) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    raise LangChainCallbackError(
        f"{label} must be a bool (True/False) or omitted, got "
        f"{type(value).__name__}: {value!r}"
    )


def _strict_int_from_dict(
    value: Any, label: str, default: int,
) -> int:
    if value is None:
        return default
    if isinstance(value, bool):
        raise LangChainCallbackError(
            f"{label} must be an int, not bool"
        )
    try:
        return int(value)
    except (TypeError, ValueError) as exc:
        raise LangChainCallbackError(
            f"{label} invalid: {exc}"
        ) from exc


def _strict_float_from_dict(
    value: Any, label: str, default: float,
) -> float:
    if value is None:
        return default
    if isinstance(value, bool) or not isinstance(value, numbers.Real):
        raise LangChainCallbackError(
            f"{label} must be a number, not bool"
        )
    fv = float(value)
    if not math.isfinite(fv):
        raise LangChainCallbackError(f"{label} must be finite")
    return fv


# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class LangChainCallbackConfig:
    """Tunable parameters for the Green LangChain callback."""

    max_token_events: int = 10_000
    max_tool_calls: int = 10_000
    track_latency: bool = True
    track_per_run: bool = True
    max_runs: int = 1_000
    strict_payload_validation: bool = True
    char_tokens_ratio: float = 4.0

    def __post_init__(self) -> None:
        object.__setattr__(
            self, "max_token_events",
            _require_positive_int(
                self.max_token_events, "max_token_events",
            ),
        )
        object.__setattr__(
            self, "max_tool_calls",
            _require_positive_int(
                self.max_tool_calls, "max_tool_calls",
            ),
        )
        object.__setattr__(
            self, "max_runs",
            _require_positive_int(self.max_runs, "max_runs"),
        )
        object.__setattr__(
            self, "track_latency",
            _require_bool(self.track_latency, "track_latency"),
        )
        object.__setattr__(
            self, "track_per_run",
            _require_bool(self.track_per_run, "track_per_run"),
        )
        object.__setattr__(
            self, "strict_payload_validation",
            _require_bool(
                self.strict_payload_validation,
                "strict_payload_validation",
            ),
        )
        ratio = _require_non_negative_finite_float(
            self.char_tokens_ratio, "char_tokens_ratio",
        )
        if ratio <= 0:
            raise LangChainCallbackError(
                "char_tokens_ratio must be > 0"
            )
        object.__setattr__(self, "char_tokens_ratio", ratio)

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any],
    ) -> "LangChainCallbackConfig":
        if not isinstance(data, Mapping):
            raise LangChainCallbackError(
                f"from_dict expects a Mapping, got "
                f"{type(data).__name__}"
            )
        return cls(
            max_token_events=_strict_int_from_dict(
                data.get("max_token_events"),
                "max_token_events", 10_000,
            ),
            max_tool_calls=_strict_int_from_dict(
                data.get("max_tool_calls"),
                "max_tool_calls", 10_000,
            ),
            track_latency=_strict_bool_from_dict(
                data.get("track_latency"),
                "track_latency", True,
            ),
            track_per_run=_strict_bool_from_dict(
                data.get("track_per_run"),
                "track_per_run", True,
            ),
            max_runs=_strict_int_from_dict(
                data.get("max_runs"), "max_runs", 1_000,
            ),
            strict_payload_validation=_strict_bool_from_dict(
                data.get("strict_payload_validation"),
                "strict_payload_validation", True,
            ),
            char_tokens_ratio=_strict_float_from_dict(
                data.get("char_tokens_ratio"),
                "char_tokens_ratio", 4.0,
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
        object.__setattr__(
            self, "total_tokens",
            _require_non_negative_int(
                self.total_tokens, "total_tokens",
            ),
        )
        for name in ("prompt_tokens", "completion_tokens"):
            value = getattr(self, name)
            if value is not None:
                object.__setattr__(
                    self, name,
                    _require_non_negative_int(value, name),
                )
        if self.latency_ms is not None:
            object.__setattr__(
                self, "latency_ms",
                _require_non_negative_finite_float(
                    self.latency_ms, "latency_ms",
                ),
            )
        object.__setattr__(
            self, "timestamp",
            _require_non_negative_finite_float(
                self.timestamp, "timestamp",
            ),
        )
        for name in ("run_id", "model_name", "provider"):
            value = getattr(self, name)
            if value is not None:
                if not isinstance(value, str):
                    raise LangChainCallbackError(
                        f"{name} must be a string or None, got "
                        f"{type(value).__name__}"
                    )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "run_id": self.run_id,
            "total_tokens": self.total_tokens,
            "prompt_tokens": self.prompt_tokens,
            "completion_tokens": self.completion_tokens,
            "model_name": self.model_name,
            "latency_ms": self.latency_ms,
            "timestamp": self.timestamp,
            "provider": self.provider,
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "TokenEvent":
        if not isinstance(data, Mapping):
            raise LangChainCallbackError(
                "TokenEvent.from_dict expects a Mapping"
            )

        def _opt_int(key: str) -> Optional[int]:
            raw = data.get(key)
            if raw is None:
                return None
            if isinstance(raw, bool):
                raise LangChainCallbackError(
                    f"{key} must be an int, not bool"
                )
            try:
                return int(raw)
            except (TypeError, ValueError) as exc:
                raise LangChainCallbackError(
                    f"{key} invalid: {exc}"
                ) from exc

        def _opt_str(key: str) -> Optional[str]:
            raw = data.get(key)
            if raw is None:
                return None
            if not isinstance(raw, str):
                raise LangChainCallbackError(
                    f"{key} must be a string or None"
                )
            return raw

        raw_ts = data.get("timestamp")
        timestamp = (
            time.time() if raw_ts is None else raw_ts
        )

        raw_latency = data.get("latency_ms")
        latency = (
            None if raw_latency is None
            else _strict_float_from_dict(
                raw_latency, "latency_ms", 0.0,
            )
        )

        return cls(
            run_id=_opt_str("run_id"),
            total_tokens=_strict_int_from_dict(
                data.get("total_tokens"), "total_tokens", 0,
            ),
            prompt_tokens=_opt_int("prompt_tokens"),
            completion_tokens=_opt_int("completion_tokens"),
            model_name=_opt_str("model_name"),
            latency_ms=latency,
            timestamp=_strict_float_from_dict(
                timestamp, "timestamp", time.time(),
            ),
            provider=_opt_str("provider"),
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
            raise LangChainCallbackError(
                "tool_name must be a non-empty string"
            )
        object.__setattr__(
            self, "tool_input_length",
            _require_non_negative_int(
                self.tool_input_length, "tool_input_length",
            ),
        )
        if self.latency_ms is not None:
            object.__setattr__(
                self, "latency_ms",
                _require_non_negative_finite_float(
                    self.latency_ms, "latency_ms",
                ),
            )
        object.__setattr__(
            self, "timestamp",
            _require_non_negative_finite_float(
                self.timestamp, "timestamp",
            ),
        )
        if self.run_id is not None and not isinstance(self.run_id, str):
            raise LangChainCallbackError(
                "run_id must be a string or None"
            )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "run_id": self.run_id,
            "tool_name": self.tool_name,
            "tool_input_length": self.tool_input_length,
            "latency_ms": self.latency_ms,
            "error": self.error,
            "timestamp": self.timestamp,
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "ToolCallEvent":
        if not isinstance(data, Mapping):
            raise LangChainCallbackError(
                "ToolCallEvent.from_dict expects a Mapping"
            )

        raw_name = data.get("tool_name")
        if not isinstance(raw_name, str) or not raw_name:
            raise LangChainCallbackError(
                f"tool_name must be a non-empty string, got "
                f"{type(raw_name).__name__}: {raw_name!r}"
            )

        raw_run = data.get("run_id")
        if raw_run is not None and not isinstance(raw_run, str):
            raise LangChainCallbackError(
                "run_id must be a string or None"
            )

        raw_error = data.get("error")
        if raw_error is not None and not isinstance(raw_error, str):
            raise LangChainCallbackError(
                "error must be a string or None"
            )

        raw_latency = data.get("latency_ms")
        latency = (
            None if raw_latency is None
            else _strict_float_from_dict(
                raw_latency, "latency_ms", 0.0,
            )
        )

        raw_ts = data.get("timestamp")
        timestamp = time.time() if raw_ts is None else raw_ts

        return cls(
            run_id=raw_run,
            tool_name=raw_name,
            tool_input_length=_strict_int_from_dict(
                data.get("tool_input_length"),
                "tool_input_length", 0,
            ),
            latency_ms=latency,
            error=raw_error,
            timestamp=_strict_float_from_dict(
                timestamp, "timestamp", time.time(),
            ),
        )


# --------------------------------------------------------------------------- #
# Internal helper — must be defined before GreenLangChainCallback
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class _TokenBreakdown:
    """Internal container for one provider's token-usage numbers."""
    total: int
    prompt: Optional[int]
    completion: Optional[int]


# --------------------------------------------------------------------------- #
# Isolation decorator
# --------------------------------------------------------------------------- #
def _isolated(fn):
    """Ensure callback bodies never raise in non-strict mode."""
    @functools.wraps(fn)
    def wrapper(self, *args, **kwargs):
        try:
            return fn(self, *args, **kwargs)
        except LangChainCallbackError:
            raise
        except Exception as exc:
            self._callback_errors += 1
            if self._strict:
                raise LangChainCallbackError(
                    f"{fn.__name__} failed: {exc}"
                ) from exc
            logger.exception(
                "%s failed; continuing (strict=False)", fn.__name__,
            )
    return wrapper


# --------------------------------------------------------------------------- #
# Callback
# --------------------------------------------------------------------------- #
class GreenLangChainCallback(_BaseCallbackHandler):  # type: ignore[misc]
    """
    Tracks token usage, tool calls, and latency for LangChain runs.

    See the module docstring for the error-isolation contract, the
    ``strict`` vs ``strict_payload_validation`` split, and the migration
    notes about read-only counters and stringified run_ids.
    """

    def __init__(
        self,
        *,
        config: Optional[LangChainCallbackConfig] = None,
        strict: bool = True,
    ) -> None:
        try:
            super().__init__()
        except Exception:  # pragma: no cover — defensive
            pass

        if config is not None and not isinstance(
            config, LangChainCallbackConfig
        ):
            raise LangChainCallbackError(
                "config must be a LangChainCallbackConfig instance"
            )
        self._config = config or LangChainCallbackConfig()
        self._strict = _require_bool(strict, "strict")
        self._lock = threading.RLock()

        # Private counters — exposed as read-only properties.
        self._token_usage: int = 0
        self._tool_calls: int = 0

        self._token_events: Deque[TokenEvent] = deque(
            maxlen=self._config.max_token_events
        )
        self._tool_events: Deque[ToolCallEvent] = deque(
            maxlen=self._config.max_tool_calls
        )

        self._per_run_tokens: Dict[str, int] = {}
        self._per_run_tools: Dict[str, int] = {}

        # Pending start times, bounded at max_runs * 2.
        self._llm_start: Dict[str, float] = {}
        self._tool_start: Dict[str, float] = {}

        self._callback_errors: int = 0
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

    # ---------------------------------------------------------- props
    @property
    def config(self) -> LangChainCallbackConfig:
        return self._config

    @property
    def token_usage(self) -> int:
        with self._lock:
            return self._token_usage

    @property
    def tool_calls(self) -> int:
        with self._lock:
            return self._tool_calls

    @property
    def total_tokens(self) -> int:
        return self.token_usage

    @property
    def total_tool_calls(self) -> int:
        return self.tool_calls

    @property
    def token_events(self) -> List[TokenEvent]:
        with self._lock:
            return list(self._token_events)

    @property
    def tool_events(self) -> List[ToolCallEvent]:
        with self._lock:
            return list(self._tool_events)

    @property
    def last_token_event(self) -> Optional[TokenEvent]:
        with self._lock:
            return self._token_events[-1] if self._token_events else None

    @property
    def last_tool_event(self) -> Optional[ToolCallEvent]:
        with self._lock:
            return self._tool_events[-1] if self._tool_events else None

    @property
    def pending_llm_runs(self) -> int:
        with self._lock:
            return len(self._llm_start)

    @property
    def pending_tool_runs(self) -> int:
        with self._lock:
            return len(self._tool_start)

    @property
    def callback_errors(self) -> int:
        return self._callback_errors

    # ---------------------------------------------------------- internal helpers
    @staticmethod
    def _stringify_run_id(run_id: Any) -> Optional[str]:
        if run_id is None:
            return None
        if isinstance(run_id, str):
            return run_id
        return str(run_id)

    def _bounded_put(
        self, d: Dict[str, float], key: str, value: float,
    ) -> None:
        """Insert into ``d``, evicting oldest-by-insertion over the cap."""
        d[key] = value
        cap = self._config.max_runs * 2
        while len(d) > cap:
            oldest = next(iter(d))
            d.pop(oldest, None)

    def _consume_latency(
        self, start_map: Dict[str, float], key: Optional[str],
    ) -> Optional[float]:
        if not self._config.track_latency or key is None:
            return None
        with self._lock:
            started = start_map.pop(key, None)
        if started is None:
            return None
        return (time.perf_counter() - started) * 1000.0

    # ---------------------------------------------------------- callbacks
    @_isolated
    def on_llm_start(
        self, serialized: Any, prompts: Any, *,
        run_id: Any = None, **kwargs: Any,
    ) -> None:
        if not self._config.track_latency:
            return
        key = self._stringify_run_id(run_id)
        if key is None:
            return
        with self._lock:
            self._bounded_put(
                self._llm_start, key, time.perf_counter(),
            )

    @_isolated
    def on_llm_error(
        self, error: Any, *, run_id: Any = None, **kwargs: Any,
    ) -> None:
        """Clean up pending start time and log the failure."""
        key = self._stringify_run_id(run_id)
        if key is None:
            return
        with self._lock:
            self._llm_start.pop(key, None)
        logger.warning(
            "on_llm_error: run=%s error=%s", key, error,
        )

    @_isolated
    def on_llm_end(
        self, response: Any, *, run_id: Any = None, **kwargs: Any,
    ) -> None:
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
            tokens = _TokenBreakdown(
                total=0, prompt=None, completion=None,
            )

        event = TokenEvent(
            run_id=key,
            total_tokens=tokens.total,
            prompt_tokens=tokens.prompt,
            completion_tokens=tokens.completion,
            model_name=model_name,
            latency_ms=latency_ms,
            provider=provider,
        )

        with self._lock:
            self._token_usage += tokens.total
            self._token_events.append(event)
            if self._config.track_per_run and key is not None:
                self._per_run_tokens[key] = (
                    self._per_run_tokens.get(key, 0) + tokens.total
                )
                self._enforce_max_runs_locked()

        logger.debug(
            "on_llm_end: run=%s tokens=%d latency_ms=%s provider=%s "
            "model=%s",
            key, tokens.total,
            f"{latency_ms:.2f}" if latency_ms is not None else "n/a",
            provider, model_name,
        )

    @_isolated
    def on_tool_start(
        self, serialized: Any, input_str: Any = "", *,
        run_id: Any = None, **kwargs: Any,
    ) -> None:
        key = self._stringify_run_id(run_id)
        tool_name = self._extract_tool_name(serialized)
        input_length = self._safe_length(input_str)

        if self._config.track_latency and key is not None:
            with self._lock:
                self._bounded_put(
                    self._tool_start, key, time.perf_counter(),
                )

        event = ToolCallEvent(
            run_id=key,
            tool_name=tool_name,
            tool_input_length=input_length,
        )

        with self._lock:
            self._tool_calls += 1
            self._tool_events.append(event)
            if self._config.track_per_run and key is not None:
                self._per_run_tools[key] = (
                    self._per_run_tools.get(key, 0) + 1
                )
                self._enforce_max_runs_locked()

        logger.debug(
            "on_tool_start: run=%s tool=%s input_len=%d",
            key, tool_name, input_length,
        )

    @_isolated
    def on_tool_end(
        self, output: Any, *, run_id: Any = None, **kwargs: Any,
    ) -> None:
        if not self._config.track_latency:
            return
        key = self._stringify_run_id(run_id)
        latency_ms = self._consume_latency(self._tool_start, key)
        if latency_ms is None or key is None:
            return

        with self._lock:
            for i in range(len(self._tool_events) - 1, -1, -1):
                ev = self._tool_events[i]
                if ev.run_id == key and ev.latency_ms is None:
                    self._tool_events[i] = ToolCallEvent(
                        run_id=ev.run_id,
                        tool_name=ev.tool_name,
                        tool_input_length=ev.tool_input_length,
                        latency_ms=latency_ms,
                        error=ev.error,
                        timestamp=ev.timestamp,
                    )
                    break

        logger.debug(
            "on_tool_end: run=%s latency_ms=%.2f", key, latency_ms,
        )

    @_isolated
    def on_tool_error(
        self, error: Any, *, run_id: Any = None, **kwargs: Any,
    ) -> None:
        key = self._stringify_run_id(run_id)
        msg = (
            str(error) if error is not None else "unknown tool error"
        )
        logger.warning("on_tool_error: run=%s error=%s", key, msg)

        with self._lock:
            if key is not None:
                self._tool_start.pop(key, None)
            if key is None:
                return
            for i in range(len(self._tool_events) - 1, -1, -1):
                ev = self._tool_events[i]
                if ev.run_id == key and ev.error is None:
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
    def _extract_tokens(self, response: Any) -> Tuple[
        Optional[str], Optional[str], Optional[_TokenBreakdown],
    ]:
        provider: Optional[str] = None
        model_name: Optional[str] = None

        if response is None:
            return None, None, None

        # --- Modern path: response.usage_metadata --------------------
        usage_meta = getattr(response, "usage_metadata", None)
        if isinstance(usage_meta, Mapping):
            total = self._first_int(
                usage_meta,
                ("total_tokens", "total_token_count", "total"),
            )
            prompt = self._first_int(
                usage_meta,
                ("input_tokens", "prompt_tokens", "prompt_token_count"),
            )
            completion = self._first_int(
                usage_meta,
                ("output_tokens", "completion_tokens",
                 "completion_token_count"),
            )
            if (
                total is not None
                or prompt is not None
                or completion is not None
            ):
                if total is None:
                    total = (prompt or 0) + (completion or 0)
                return provider, model_name, _TokenBreakdown(
                    total=total, prompt=prompt, completion=completion,
                )

        # --- Legacy path: response.llm_output ------------------------
        llm_output = getattr(response, "llm_output", None)
        if isinstance(llm_output, Mapping):
            for key in (
                "provider", "system_fingerprint", "model_provider",
            ):
                value = llm_output.get(key)
                if isinstance(value, str) and value:
                    provider = value
                    break
            for key in ("model_name", "model", "model_id"):
                value = llm_output.get(key)
                if isinstance(value, str) and value:
                    model_name = value
                    break

            token_usage = llm_output.get("token_usage")
            if isinstance(token_usage, Mapping):
                total = self._first_int(
                    token_usage,
                    ("total_tokens", "total_token_count", "total"),
                )
                prompt = self._first_int(
                    token_usage,
                    ("prompt_tokens", "input_tokens",
                     "prompt_token_count"),
                )
                completion = self._first_int(
                    token_usage,
                    ("completion_tokens", "output_tokens",
                     "completion_token_count"),
                )
                if total is None:
                    total = (prompt or 0) + (completion or 0)
                return provider, model_name, _TokenBreakdown(
                    total=total, prompt=prompt, completion=completion,
                )

        # --- Fallback: response_metadata.usage -----------------------
        response_meta = getattr(response, "response_metadata", None)
        if isinstance(response_meta, Mapping):
            usage = response_meta.get("usage")
            if isinstance(usage, Mapping):
                prompt = self._first_int(
                    usage, ("input_tokens", "prompt_tokens"),
                )
                completion = self._first_int(
                    usage, ("output_tokens", "completion_tokens"),
                )
                total = (prompt or 0) + (completion or 0)
                return provider, model_name, _TokenBreakdown(
                    total=total, prompt=prompt, completion=completion,
                )

        # --- Last resort: char-based estimate from generations -------
        generations = getattr(response, "generations", None)
        if isinstance(generations, (list, tuple)):
            total_chars = 0
            for gen_list in generations:
                if not isinstance(gen_list, (list, tuple)):
                    continue
                for gen in gen_list:
                    text = getattr(gen, "text", None)
                    if isinstance(text, str):
                        total_chars += len(text)
                    message = getattr(gen, "message", None)
                    meta = (
                        getattr(message, "usage_metadata", None)
                        if message else None
                    )
                    if isinstance(meta, Mapping):
                        total = self._first_int(
                            meta, ("total_tokens", "total"),
                        )
                        prompt = self._first_int(
                            meta, ("input_tokens", "prompt_tokens"),
                        )
                        completion = self._first_int(
                            meta,
                            ("output_tokens", "completion_tokens"),
                        )
                        if (
                            total is not None
                            or prompt is not None
                            or completion is not None
                        ):
                            if total is None:
                                total = (
                                    (prompt or 0) + (completion or 0)
                                )
                            return provider, model_name, _TokenBreakdown(
                                total=total,
                                prompt=prompt,
                                completion=completion,
                            )
            if total_chars > 0:
                ratio = self._config.char_tokens_ratio
                logger.debug(
                    "Falling back to character-based token estimate "
                    "(%d chars / %.1f = %d tokens)",
                    total_chars, ratio,
                    max(1, int(total_chars // ratio)),
                )
                return provider, model_name, _TokenBreakdown(
                    total=max(1, int(total_chars // ratio)),
                    prompt=None,
                    completion=None,
                )

        return provider, model_name, None

    @staticmethod
    def _first_int(
        mapping: Mapping[str, Any], keys: Tuple[str, ...],
    ) -> Optional[int]:
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
        cap = self._config.max_runs
        while len(self._per_run_tokens) > cap:
            oldest = next(iter(self._per_run_tokens))
            self._per_run_tokens.pop(oldest, None)
        while len(self._per_run_tools) > cap:
            oldest = next(iter(self._per_run_tools))
            self._per_run_tools.pop(oldest, None)

    # ---------------------------------------------------------- statistics
    def statistics(self) -> Dict[str, Any]:
        with self._lock:
            token_events = list(self._token_events)
            tool_events = list(self._tool_events)
            per_run_tokens = dict(self._per_run_tokens)
            per_run_tools = dict(self._per_run_tools)
            total_tokens = self._token_usage
            total_tools = self._tool_calls
            callback_errors = self._callback_errors

        stats: Dict[str, Any] = {
            "total_tokens": total_tokens,
            "total_tool_calls": total_tools,
            "token_events": len(token_events),
            "tool_events": len(tool_events),
            "unique_runs": len(
                set(per_run_tokens) | set(per_run_tools)
            ),
            "callback_errors": callback_errors,
            "pending_llm_runs": len(self._llm_start),
            "pending_tool_runs": len(self._tool_start),
        }

        providers = Counter(
            e.provider for e in token_events if e.provider is not None
        )
        models = Counter(
            e.model_name
            for e in token_events if e.model_name is not None
        )
        stats["by_provider"] = dict(providers)
        stats["by_model"] = dict(models)
        stats["by_tool"] = dict(
            Counter(e.tool_name for e in tool_events)
        )
        stats["tool_errors"] = sum(
            1 for e in tool_events if e.error is not None
        )

        llm_latencies = sorted(
            e.latency_ms
            for e in token_events if e.latency_ms is not None
        )
        tool_latencies = sorted(
            e.latency_ms
            for e in tool_events if e.latency_ms is not None
        )
        stats["llm_latency"] = self._latency_summary(llm_latencies)
        stats["tool_latency"] = self._latency_summary(tool_latencies)

        stats["top_runs_by_tokens"] = sorted(
            per_run_tokens.items(), key=lambda kv: kv[1], reverse=True,
        )[:5]
        stats["top_runs_by_tools"] = sorted(
            per_run_tools.items(), key=lambda kv: kv[1], reverse=True,
        )[:5]

        return stats

    @classmethod
    def _latency_summary(
        cls, values: List[float],
    ) -> Dict[str, Optional[float]]:
        if not values:
            return {
                "count": 0, "mean_ms": None,
                "p50_ms": None, "p95_ms": None, "max_ms": None,
            }
        n = len(values)
        return {
            "count": n,
            "mean_ms": sum(values) / n,
            "p50_ms": cls._percentile(values, 0.50),
            "p95_ms": cls._percentile(values, 0.95),
            "max_ms": values[-1],
        }

    @staticmethod
    def _percentile(
        sorted_values: List[float], p: float,
    ) -> Optional[float]:
        """Nearest-rank percentile on an already-sorted list."""
        n = len(sorted_values)
        if n == 0:
            return None
        rank = max(1, min(n, math.ceil(p * n)))
        return sorted_values[rank - 1]

    # ---------------------------------------------------------- lifecycle
    def reset(self, *, clear_history: bool = True) -> int:
        with self._lock:
            removed = len(self._token_events)
            if clear_history:
                self._token_events.clear()
                self._tool_events.clear()
            self._token_usage = 0
            self._tool_calls = 0
            self._per_run_tokens.clear()
            self._per_run_tools.clear()
            self._llm_start.clear()
            self._tool_start.clear()
            self._callback_errors = 0
            self._started_at = time.time()
        logger.debug(
            "GreenLangChainCallback reset (removed %d event(s)).",
            removed,
        )
        return removed

    # ---------------------------------------------------------- serialization
    def to_dict(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "config": self._config.to_dict(),
                "strict": self._strict,
                "langchain_available": _LANGCHAIN_AVAILABLE,
                "started_at": self._started_at,
                "token_usage": self._token_usage,
                "tool_calls": self._tool_calls,
                "callback_errors": self._callback_errors,
                "token_events": [
                    e.to_dict() for e in self._token_events
                ],
                "tool_events": [
                    e.to_dict() for e in self._tool_events
                ],
                "per_run_tokens": dict(self._per_run_tokens),
                "per_run_tools": dict(self._per_run_tools),
            }

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any],
    ) -> "GreenLangChainCallback":
        if not isinstance(data, Mapping):
            raise LangChainCallbackError(
                f"from_dict expects a Mapping, got "
                f"{type(data).__name__}"
            )

        cfg_data = data.get("config") or {}
        if not isinstance(cfg_data, Mapping):
            raise LangChainCallbackError(
                f"config must be a Mapping, got "
                f"{type(cfg_data).__name__}"
            )
        cfg = LangChainCallbackConfig.from_dict(cfg_data)

        strict = _strict_bool_from_dict(
            data.get("strict"), "strict", True,
        )
        cb = cls(config=cfg, strict=strict)

        token_events_data = data.get("token_events", [])
        if not isinstance(token_events_data, list):
            raise LangChainCallbackError(
                "token_events must be a list"
            )
        tool_events_data = data.get("tool_events", [])
        if not isinstance(tool_events_data, list):
            raise LangChainCallbackError(
                "tool_events must be a list"
            )
        per_run_tokens_raw = data.get("per_run_tokens") or {}
        per_run_tools_raw = data.get("per_run_tools") or {}
        if not isinstance(per_run_tokens_raw, Mapping):
            raise LangChainCallbackError(
                "per_run_tokens must be a Mapping"
            )
        if not isinstance(per_run_tools_raw, Mapping):
            raise LangChainCallbackError(
                "per_run_tools must be a Mapping"
            )

        with cb._lock:
            cb._token_usage = _strict_int_from_dict(
                data.get("token_usage"), "token_usage", 0,
            )
            cb._tool_calls = _strict_int_from_dict(
                data.get("tool_calls"), "tool_calls", 0,
            )
            cb._callback_errors = _strict_int_from_dict(
                data.get("callback_errors"),
                "callback_errors", 0,
            )
            for entry in token_events_data:
                cb._token_events.append(TokenEvent.from_dict(entry))
            for entry in tool_events_data:
                cb._tool_events.append(ToolCallEvent.from_dict(entry))
            cb._per_run_tokens = {
                str(k): _strict_int_from_dict(v, "per_run_tokens", 0)
                for k, v in per_run_tokens_raw.items()
            }
            cb._per_run_tools = {
                str(k): _strict_int_from_dict(v, "per_run_tools", 0)
                for k, v in per_run_tools_raw.items()
            }
            raw_started = data.get("started_at", time.time())
            cb._started_at = _strict_float_from_dict(
                raw_started, "started_at", time.time(),
            )
        return cb

    def to_json(self, **kwargs: Any) -> str:
        return json.dumps(self.to_dict(), default=str, **kwargs)

    @classmethod
    def from_json(
        cls, payload: str,
    ) -> "GreenLangChainCallback":
        try:
            return cls.from_dict(json.loads(payload))
        except json.JSONDecodeError as exc:
            raise LangChainCallbackError(
                f"Invalid JSON payload: {exc}"
            ) from exc

    def __repr__(self) -> str:
        with self._lock:
            return (
                "GreenLangChainCallback("
                f"tokens={self._token_usage}, "
                f"tool_calls={self._tool_calls}, "
                f"runs={len(self._per_run_tokens)}, "
                f"errors={self._callback_errors}, "
                f"langchain={_LANGCHAIN_AVAILABLE}, "
                f"strict={self._strict})"
            )


# --------------------------------------------------------------------------- #
# Smoke test
# --------------------------------------------------------------------------- #
if __name__ == "__main__":  # pragma: no cover
    logging.basicConfig(level=logging.INFO)

    class _MockResponse:
        def __init__(
            self, *, llm_output=None, usage_metadata=None,
            response_metadata=None, generations=None,
        ) -> None:
            self.llm_output = llm_output
            self.usage_metadata = usage_metadata
            self.response_metadata = response_metadata
            self.generations = generations

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
    modern_resp = _MockResponse(
        usage_metadata={
            "input_tokens": 50,
            "output_tokens": 20,
            "total_tokens": 70,
        },
    )
    anthropic_resp = _MockResponse(
        response_metadata={
            "usage": {"input_tokens": 40, "output_tokens": 15},
        },
    )
    broken_resp = _MockResponse(llm_output=None)

    class _Gen:
        def __init__(self, text):
            self.text = text

    fallback_resp = _MockResponse(generations=[[_Gen("a" * 40)]])

    # Happy path
    cb = GreenLangChainCallback()
    print("repr        :", cb)
    cb.on_llm_start({}, [], run_id="run-1")
    time.sleep(0.005)
    cb.on_llm_end(openai_resp, run_id="run-1")
    assert cb.token_usage == 128
    print("after openai:", cb.token_usage, "tokens")

    # Provider variations
    for i, resp in enumerate(
        [modern_resp, anthropic_resp, fallback_resp], start=2,
    ):
        cb.on_llm_start({}, [], run_id=f"run-{i}")
        cb.on_llm_end(resp, run_id=f"run-{i}")

    # Broken payload — non-strict logs and returns 0
    cb.on_llm_end(broken_resp, run_id="run-broken")

    # Strict raises
    strict = GreenLangChainCallback(strict=True)
    try:
        strict.on_llm_end(broken_resp, run_id="run-bad")
    except LangChainCallbackError as exc:
        print("strict      :", exc)

    # Error isolation: an exotic payload doesn't crash in lenient mode
    class _Crashing:
        @property
        def llm_output(self):
            raise RuntimeError("exotic response")

    lenient = GreenLangChainCallback(strict=False)
    lenient.on_llm_end(_Crashing(), run_id="run-exotic")
    assert lenient.callback_errors == 1
    print("isolation   : callback_errors =", lenient.callback_errors)

    # Tool calls + on_llm_error cleanup
    cb.on_tool_start({"name": "search"}, "q", run_id="tool-1")
    time.sleep(0.002)
    cb.on_tool_end("results", run_id="tool-1")
    cb.on_tool_start({"name": "calc"}, "1+1", run_id="tool-2")
    cb.on_tool_error(RuntimeError("boom"), run_id="tool-2")
    cb.on_llm_start({}, [], run_id="failed-llm")
    assert cb.pending_llm_runs == 1
    cb.on_llm_error(RuntimeError("llm crashed"), run_id="failed-llm")
    assert cb.pending_llm_runs == 0
    print("pending LLM :", cb.pending_llm_runs, "(cleaned by on_llm_error)")

    # Statistics
    stats = cb.statistics()
    print("stats       :", {
        k: stats[k] for k in (
            "total_tokens", "total_tool_calls", "unique_runs",
            "by_provider", "by_tool", "tool_errors",
            "callback_errors", "pending_llm_runs",
        )
    })
    print("llm latency :", stats["llm_latency"])
    print("tool latency:", stats["tool_latency"])

    # Read-only counters
    try:
        cb.token_usage = 999  # type: ignore[misc]
    except AttributeError as exc:
        print("read-only   :", exc)

    # Serialization round-trip
    payload = cb.to_json()
    restored = GreenLangChainCallback.from_json(payload)
    assert restored.to_dict() == cb.to_dict()
    print("Round-trip OK.")

    # Reset
    removed = cb.reset()
    print(
        "reset       :", removed, "event(s); tokens:",
        cb.token_usage,
    )

    # Strict-bool config regression
    try:
        LangChainCallbackConfig.from_dict(
            {"track_latency": "false"}
        )
    except LangChainCallbackError as exc:
        print("string-bool :", exc)
    else:
        raise AssertionError(
            '"track_latency": "false" must raise'
        )

    # tool_name=null rejection
    try:
        ToolCallEvent.from_dict({
            "run_id": "r1",
            "tool_name": None,
            "tool_input_length": 10,
        })
    except LangChainCallbackError as exc:
        print("null tool   :", exc)
    else:
        raise AssertionError("null tool_name must raise")

    # Bool rejection
    try:
        TokenEvent(
            run_id=None, total_tokens=True,  # type: ignore[arg-type]
            prompt_tokens=None, completion_tokens=None,
            model_name=None, latency_ms=None,
        )
    except LangChainCallbackError as exc:
        print("bool tokens :", exc)

    # Config validation
    for bad in (
        dict(max_token_events=0),
        dict(max_tool_calls=0),
        dict(max_runs=0),
        dict(track_latency="yes"),  # type: ignore[arg-type]
        dict(track_per_run=1),  # type: ignore[arg-type]
        dict(char_tokens_ratio=0),
    ):
        try:
            LangChainCallbackConfig(**bad)  # type: ignore[arg-type]
        except LangChainCallbackError as exc:
            print("Rejected    :", exc)
        else:
            raise AssertionError(
                f"config should have rejected {bad}"
            )

    print("\nSmoke test passed.")
