# src/runtime/langchain_runtime.py

"""
LangChain Runtime Adapter
=========================

Framework-specific runtime adapter that simulates LangChain-style chain
execution to measure accuracy, tool-call counts, and conversation depth.

.. note::
    This is a **simulation mock**, not a real LangChain integration. By
    default every ``run()`` returns the configured ``base_accuracy``
    verbatim — it does **not** depend on the query. To customize behaviour
    per query, override :meth:`LangChainRuntime._compute_accuracy` and/or
    :meth:`LangChainRuntime._compute_per_run_tools` in a subclass.

Semantics
---------
- ``run_result["tool_calls"]`` is the number of tools used **in this run**
  (matching ``AutoGenRuntime``).
- ``run_result["cumulative_tool_calls"]`` is the running total across all
  runs since the last :meth:`LangChainRuntime.reset`.
- ``shorten_context()`` **halves** the accumulated ``depth`` (different
  from ``AutoGenRuntime``, which decrements the *increment*). Once
  called, ``run_result["context_shortened"]`` is ``True`` for the rest
  of the session until :meth:`init` / :meth:`reset`.

Enhancements
------------
- ``LangChainRuntimeConfig`` — frozen, ``slots``, concise ``__repr__``;
  every field type-checked (rejects ``bool`` for int fields, no more
  ``TypeError`` leaks).
- ``LangChainRuntimeConfig.from_dict(..., strict=True)`` — rejects
  unknown keys.
- ``LangChainRunResult`` — frozen, ``slots``, parse-error wrapping.
- ``run()`` is **rollback-safe** — the result is constructed *before*
  any runtime state is mutated.
- ``run()`` returns a ``RunMetrics`` TypedDict; ``tool_calls`` is
  per-run, ``cumulative_tool_calls`` is a new additive field.
- ``statistics()`` is **atomic** — every field is read under the lock.
- Lifecycle counters: ``total_attempts``, ``failed_runs``,
  ``dropped_runs``. Cumulative for the object's lifetime (not reset by
  :meth:`init` / :meth:`reset`).
- ``metrics()`` — aggregate accuracy / depth / per-run tool averages.
- ``init()`` accepts ``reset_throttle`` and ``reset_clock``; no silent
  state loss on metadata refresh.
- ``_finalized`` guard applied to ``reduce_tool_calls()``,
  ``shorten_context()``, and ``finalize()``.
- ``shorten_context()`` and ``reduce_tool_calls()`` now documented in
  terms of their exact effect.
- ``self.config`` (caller metadata) is preserved through
  ``to_dict`` / ``from_dict``.
- ``to_json`` no longer uses ``default=str``.
- ``__exit__`` re-raises ``finalize()`` failures instead of swallowing
  them.
- ``__eq__`` / ``__hash__`` on the runtime keyed on ``(config, strict)``.
- ``__slots__`` on both dataclasses; ``__repr__`` on the config.
- Typed public returns: ``RunMetrics``, ``RuntimeMetrics``.

.. note::
    The validation helpers in this module duplicate those in
    ``autogen_runtime.py``. When the ``src/runtime`` tree gains a shared
    ``_base.py``, these should be lifted into it to prevent drift.
"""

from __future__ import annotations

import json
import logging
import math
import threading
import time
from collections import deque
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Any, Deque, Dict, List, Mapping, Optional, TypedDict

logger = logging.getLogger(__name__)


# --------------------------------------------------------------------------- #
# Errors
# --------------------------------------------------------------------------- #
class LangChainRuntimeError(ValueError):
    """Raised for invalid LangChain runtime inputs or configuration."""


# --------------------------------------------------------------------------- #
# Typed return shapes
# --------------------------------------------------------------------------- #
class RunMetrics(TypedDict):
    """Result of a single :meth:`LangChainRuntime.run` call."""

    accuracy: float
    tool_calls: int                 # per-run (NEW semantics)
    cumulative_tool_calls: int      # running total (NEW)
    max_tools_available: int        # current throttle level (NEW)
    conversation_depth: int
    context_shortened: bool


class RuntimeMetrics(TypedDict):
    """Result of :meth:`LangChainRuntime.metrics`."""

    runs: int
    total_attempts: int
    failed_runs: int
    dropped_runs: int
    average_accuracy: float
    average_depth: float
    average_tool_calls_per_run: float
    total_cumulative_tool_calls: int


# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #
@dataclass(frozen=True, slots=True)
class LangChainRuntimeConfig:
    """Tunable parameters for :class:`LangChainRuntime`."""

    # Metric simulation.
    base_accuracy: float = 0.82
    base_max_tools: int = 5
    tools_per_run: int = 2
    depth_increment: int = 1

    # Throttling limits.
    min_max_tools: int = 1
    max_conversation_depth: int = 1_000

    # Bounded run history.
    max_history: int = 1_000

    def __post_init__(self) -> None:
        # --- Numeric (float | int), non-bool, finite ----------------- #
        if isinstance(self.base_accuracy, bool) or not isinstance(
            self.base_accuracy, (int, float)
        ):
            raise LangChainRuntimeError("base_accuracy must be numeric.")
        if not math.isfinite(float(self.base_accuracy)):
            raise LangChainRuntimeError("base_accuracy must be finite.")
        if not 0.0 <= float(self.base_accuracy) <= 1.0:
            raise LangChainRuntimeError("base_accuracy must be in [0, 1].")

        # --- Strictly int, non-bool ---------------------------------- #
        for name in (
            "base_max_tools", "tools_per_run", "depth_increment",
            "min_max_tools", "max_conversation_depth", "max_history",
        ):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, int):
                raise LangChainRuntimeError(f"{name} must be an int.")

        if self.base_max_tools < 0:
            raise LangChainRuntimeError("base_max_tools must be >= 0.")
        if self.tools_per_run < 0:
            raise LangChainRuntimeError("tools_per_run must be >= 0.")
        if self.depth_increment < 0:
            raise LangChainRuntimeError("depth_increment must be >= 0.")
        if self.tools_per_run > self.base_max_tools:
            raise LangChainRuntimeError(
                "tools_per_run must be <= base_max_tools."
            )
        if self.min_max_tools < 1:
            raise LangChainRuntimeError(
                "min_max_tools must be a positive int."
            )
        if self.min_max_tools > self.base_max_tools:
            raise LangChainRuntimeError(
                "min_max_tools must be <= base_max_tools."
            )
        if self.max_conversation_depth <= 0:
            raise LangChainRuntimeError(
                "max_conversation_depth must be > 0."
            )
        if self.max_history <= 0:
            raise LangChainRuntimeError("max_history must be > 0.")

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any], *, strict: bool = True
    ) -> "LangChainRuntimeConfig":
        if not isinstance(data, Mapping):
            raise LangChainRuntimeError(
                "LangChainRuntimeConfig.from_dict expects a Mapping."
            )
        valid = set(cls.__dataclass_fields__.keys())
        kwargs: Dict[str, Any] = {}
        unknown = [k for k in data if k not in valid]
        if unknown:
            msg = f"unknown LangChainRuntimeConfig keys: {unknown}"
            if strict:
                raise LangChainRuntimeError(msg)
            logger.warning(msg)
        for k in data:
            if k in valid:
                kwargs[k] = data[k]
        try:
            return cls(**kwargs)
        except TypeError as exc:
            raise LangChainRuntimeError(
                f"invalid LangChainRuntimeConfig payload: {exc}"
            ) from exc

    def __repr__(self) -> str:
        return (
            "LangChainRuntimeConfig("
            f"accuracy={self.base_accuracy:.3g}, "
            f"max_tools={self.base_max_tools}, "
            f"tools/run={self.tools_per_run}, "
            f"Δdepth={self.depth_increment}, "
            f"max_depth={self.max_conversation_depth}, "
            f"max_history={self.max_history})"
        )


# --------------------------------------------------------------------------- #
# Run result
# --------------------------------------------------------------------------- #
@dataclass(frozen=True, slots=True)
class LangChainRunResult:
    """
    Immutable metrics produced by one ``run`` invocation.

    .. note::
        ``tool_calls`` is the number of tools used **in this run**,
        not the cumulative total. ``cumulative_tool_calls`` is provided
        separately.
    """

    query: str
    accuracy: float
    tool_calls: int
    conversation_depth: int
    cumulative_tool_calls: int = 0
    timestamp: datetime = field(
        default_factory=lambda: datetime.now(timezone.utc)
    )

    def __post_init__(self) -> None:
        if not isinstance(self.query, str):
            raise LangChainRuntimeError("query must be a string.")
        if isinstance(self.accuracy, bool) or not isinstance(
            self.accuracy, (int, float)
        ):
            raise LangChainRuntimeError("accuracy must be numeric.")
        fa = float(self.accuracy)
        if not math.isfinite(fa) or not 0.0 <= fa <= 1.0:
            raise LangChainRuntimeError(
                f"accuracy must be finite and in [0, 1], got "
                f"{self.accuracy!r}."
            )
        for name in ("tool_calls", "conversation_depth", "cumulative_tool_calls"):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, int):
                raise LangChainRuntimeError(f"{name} must be an int.")
            if value < 0:
                raise LangChainRuntimeError(f"{name} must be >= 0.")

    def to_dict(self) -> Dict[str, Any]:
        return {
            "query": self.query,
            "accuracy": float(self.accuracy),
            "tool_calls": int(self.tool_calls),
            "conversation_depth": int(self.conversation_depth),
            "cumulative_tool_calls": int(self.cumulative_tool_calls),
            "timestamp": self.timestamp.isoformat(),
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "LangChainRunResult":
        if not isinstance(data, Mapping):
            raise LangChainRuntimeError(
                "LangChainRunResult.from_dict expects a Mapping."
            )
        try:
            ts = data.get("timestamp")
            if ts is None:
                timestamp = datetime.now(timezone.utc)
            elif isinstance(ts, str):
                timestamp = datetime.fromisoformat(ts)
                if timestamp.tzinfo is None:
                    timestamp = timestamp.replace(tzinfo=timezone.utc)
            elif isinstance(ts, datetime):
                timestamp = ts
            else:
                raise LangChainRuntimeError(
                    f"timestamp must be str|datetime|None, "
                    f"got {type(ts).__name__}."
                )
            return cls(
                query=str(data.get("query", "")),
                accuracy=float(data.get("accuracy", 0.0)),
                tool_calls=int(data.get("tool_calls", 0)),
                conversation_depth=int(data.get("conversation_depth", 0)),
                cumulative_tool_calls=int(
                    data.get("cumulative_tool_calls", 0)
                ),
                timestamp=timestamp,
            )
        except LangChainRuntimeError:
            raise
        except (TypeError, ValueError) as exc:
            raise LangChainRuntimeError(
                f"invalid LangChainRunResult payload: {exc}"
            ) from exc

    def __repr__(self) -> str:
        return (
            "LangChainRunResult("
            f"query={self.query!r}, "
            f"accuracy={self.accuracy:.3f}, "
            f"tools={self.tool_calls}, "
            f"cum_tools={self.cumulative_tool_calls}, "
            f"depth={self.conversation_depth})"
        )


# --------------------------------------------------------------------------- #
# Runtime
# --------------------------------------------------------------------------- #
class LangChainRuntime:
    """
    Runtime adapter that simulates LangChain-style chain execution.

    Thread-safe for all public methods. State mutations via direct
    attribute assignment (``runtime.tool_calls``, ``runtime.depth``,
    ``runtime.max_tools``, ``runtime.config``) are **not** thread-safe —
    use the public API.

    The original public API (``init``, ``run``, ``reduce_tool_calls``,
    ``shorten_context``, ``finalize``) is preserved; new parameters are
    keyword-only.

    Lifecycle counters (``total_attempts``, ``failed_runs``,
    ``dropped_runs``) are cumulative for the object's lifetime and are
    **not** reset by :meth:`init` or :meth:`reset`.
    """

    def __init__(
        self,
        *,
        config: Optional[LangChainRuntimeConfig] = None,
        strict: bool = True,
    ) -> None:
        if config is not None and not isinstance(config, LangChainRuntimeConfig):
            raise LangChainRuntimeError(
                f"config must be a LangChainRuntimeConfig, got "
                f"{type(config).__name__}."
            )
        self._config = config or LangChainRuntimeConfig()
        self._strict = bool(strict)

        # Legacy attributes preserved (see class docstring re: thread-safety).
        self.tool_calls: int = 0            # cumulative tool count
        self.depth: int = 0                 # accumulated depth
        self.max_tools: int = self._config.base_max_tools
        self.config: Dict[str, Any] = {}    # caller metadata

        self._lock = threading.RLock()
        self._history: Deque[LangChainRunResult] = deque(
            maxlen=self._config.max_history
        )
        self._context_shortened: bool = False
        self._finalized: bool = False
        self._started_at: float = time.time()

        # Lifecycle counters (cumulative, not reset by init/reset).
        self._total_attempts: int = 0
        self._failed_runs: int = 0
        self._dropped_count: int = 0

        logger.debug(
            "LangChainRuntime initialized "
            "(accuracy=%.3f, max_tools=%d, tools_per_run=%d, strict=%s)",
            self._config.base_accuracy,
            self._config.base_max_tools,
            self._config.tools_per_run,
            self._strict,
        )

    # ------------------------------------------------------------------ props
    @property
    def config_typed(self) -> LangChainRuntimeConfig:
        """Typed, immutable configuration (see also ``self.config`` dict)."""
        return self._config

    @property
    def typed_config(self) -> LangChainRuntimeConfig:
        """Alias for :attr:`config_typed` (preferred going forward)."""
        return self._config

    @property
    def raw_config(self) -> Mapping[str, Any]:
        """Read-only view of the caller-supplied metadata dict."""
        with self._lock:
            return dict(self.config)

    @property
    def history(self) -> List[LangChainRunResult]:
        with self._lock:
            return list(self._history)

    @property
    def finalized(self) -> bool:
        with self._lock:
            return self._finalized

    @property
    def strict(self) -> bool:
        return self._strict

    # ---------------------------------------------------------- override hooks
    def _compute_accuracy(self, query: str, depth: int) -> float:
        """
        Compute accuracy for a single run.

        Default: returns ``config.base_accuracy`` (query-independent mock).
        Override in subclasses for per-query behaviour.
        """
        return self._config.base_accuracy

    def _compute_per_run_tools(self, max_tools: int) -> int:
        """
        Compute how many tools to use in a single run.

        Default: returns ``min(config.tools_per_run, max_tools)``.
        Override in subclasses for query-dependent tool selection.
        """
        return min(self._config.tools_per_run, max_tools)

    # ---------------------------------------------------------- public API
    def init(
        self,
        config: Any,
        *,
        reset_throttle: bool = True,
        reset_clock: bool = True,
    ) -> None:
        """
        Configure the runtime with caller-supplied metadata.

        Parameters
        ----------
        config : Mapping or any
            Caller metadata; stored verbatim in ``self.config``. Not
            interpreted by the runtime, but preserved across serialization.
        reset_throttle : bool, default True
            If True, ``max_tools`` and ``_context_shortened`` are restored
            to their post-construction values (undoing any prior
            :meth:`reduce_tool_calls` / :meth:`shorten_context`).
            Set to False to preserve throttling state across metadata
            refreshes.
        reset_clock : bool, default True
            If True, ``_started_at`` is reset so
            ``statistics()["uptime_seconds"]`` restarts from zero.

        .. note::
            ``tool_calls`` and ``depth`` are **not** reset by ``init()``;
            only :meth:`reset` clears them.
        """
        if config is None:
            if self._strict:
                raise LangChainRuntimeError("config must not be None.")
            logger.warning("config is None; ignoring.")
            return
        if isinstance(config, Mapping):
            new_config = dict(config)
        else:
            new_config = {"value": config}

        with self._lock:
            self.config = new_config
            self._finalized = False
            if reset_throttle:
                self._context_shortened = False
                self.max_tools = self._config.base_max_tools
            if reset_clock:
                self._started_at = time.time()

        logger.debug("LangChainRuntime init: config=%s", self.config)

    def run(self, query: Any) -> RunMetrics:
        """
        Simulate one execution of ``query``.

        Returns
        -------
        RunMetrics
            A dict with keys ``accuracy``, ``tool_calls`` (per-run),
            ``cumulative_tool_calls``, ``max_tools_available``,
            ``conversation_depth``, and ``context_shortened``.

        Raises
        ------
        LangChainRuntimeError
            If ``strict=True`` and ``query`` is not a string, or if the
            runtime has been finalized. In either case the failure is
            recorded in ``failed_runs`` and **no** runtime state is
            mutated.
        """
        with self._lock:
            self._total_attempts += 1
            try:
                if not isinstance(query, str):
                    msg = (
                        f"query must be a string, "
                        f"got {type(query).__name__}."
                    )
                    if self._strict:
                        raise LangChainRuntimeError(msg)
                    logger.warning("%s Coercing via str().", msg)
                    query = str(query)

                if self._finalized:
                    raise LangChainRuntimeError(
                        "runtime has been finalized; call init() to reset."
                    )

                # Compute prospective state — do not commit yet.
                per_run_tools = self._compute_per_run_tools(self.max_tools)
                next_cumulative_tools = self.tool_calls + per_run_tools
                next_depth = min(
                    self.depth + self._config.depth_increment,
                    self._config.max_conversation_depth,
                )

                # Construct the result *before* any state is committed,
                # so a validation failure leaves the runtime consistent.
                result = LangChainRunResult(
                    query=query,
                    accuracy=self._compute_accuracy(query, next_depth),
                    tool_calls=per_run_tools,
                    conversation_depth=next_depth,
                    cumulative_tool_calls=next_cumulative_tools,
                )

                # Commit atomically.
                self.tool_calls = next_cumulative_tools
                self.depth = next_depth
                self._append_history_locked(result)
            except LangChainRuntimeError:
                self._failed_runs += 1
                raise
            except Exception:
                self._failed_runs += 1
                raise

            context_shortened = self._context_shortened
            max_tools_available = self.max_tools

        logger.debug(
            "LangChain run: query=%r, tools=%d (cum=%d), depth=%d, "
            "max_tools=%d, shortened=%s",
            query, per_run_tools, next_cumulative_tools, next_depth,
            max_tools_available, context_shortened,
        )
        return {
            "accuracy": result.accuracy,
            "tool_calls": result.tool_calls,
            "cumulative_tool_calls": result.cumulative_tool_calls,
            "max_tools_available": max_tools_available,
            "conversation_depth": result.conversation_depth,
            "context_shortened": context_shortened,
        }

    def _append_history_locked(self, result: LangChainRunResult) -> None:
        """Append under lock, tracking evictions. Caller must hold ``_lock``."""
        if len(self._history) == self._config.max_history:
            self._dropped_count += 1
        self._history.append(result)

    def reduce_tool_calls(self) -> int:
        """
        Reduce the maximum number of tools available for subsequent runs.

        Returns the new ``max_tools`` value. Idempotent once the floor
        (``config.min_max_tools``) is reached.

        Raises
        ------
        LangChainRuntimeError
            If the runtime has been finalized.
        """
        with self._lock:
            if self._finalized:
                raise LangChainRuntimeError(
                    "runtime has been finalized; call init() to reset."
                )
            if self.max_tools <= self._config.min_max_tools:
                return self.max_tools
            self.max_tools -= 1
            new_value = self.max_tools
        logger.debug("max_tools reduced to %d.", new_value)
        return new_value

    def shorten_context(self) -> int:
        """
        Trim the accumulated ``depth`` for subsequent runs.

        **Model:** halves the current ``depth`` (``depth // 2``). This is
        different from ``AutoGenRuntime``, which decrements the *depth
        increment* instead. After ``shorten_context()``, all subsequent
        run results report ``context_shortened=True`` until
        :meth:`init` / :meth:`reset`.

        Returns the new depth.

        Raises
        ------
        LangChainRuntimeError
            If the runtime has been finalized.
        """
        with self._lock:
            if self._finalized:
                raise LangChainRuntimeError(
                    "runtime has been finalized; call init() to reset."
                )
            self.depth = max(0, self.depth // 2)
            self._context_shortened = True
            new_depth = self.depth
        logger.debug("Context shortened; depth=%d.", new_depth)
        return new_depth

    def finalize(self) -> Dict[str, Any]:
        """
        Finalize the runtime and return a summary.

        Idempotent. After finalization, :meth:`run`,
        :meth:`reduce_tool_calls`, and :meth:`shorten_context` raise until
        :meth:`init` or :meth:`reset` is called.
        """
        with self._lock:
            self._finalized = True
            summary = {
                "finalized": True,
                "runs": len(self._history),
                "total_tool_calls": self.tool_calls,
                "final_depth": self.depth,
                "final_max_tools": self.max_tools,
                "total_attempts": self._total_attempts,
                "failed_runs": self._failed_runs,
                "dropped_runs": self._dropped_count,
            }
        logger.debug("LangChainRuntime finalized: %s", summary)
        return summary

    # ---------------------------------------------------------- lifecycle
    def reset(self, *, clear_history: bool = True) -> int:
        """
        Reset runtime state. Returns the number of history entries removed.

        Counters (``total_attempts``, ``failed_runs``, ``dropped_runs``)
        are **not** reset — they are cumulative for the object's lifetime.
        """
        with self._lock:
            removed = len(self._history)
            if clear_history:
                self._history.clear()
            self.tool_calls = 0
            self.depth = 0
            self.max_tools = self._config.base_max_tools
            self._context_shortened = False
            self._finalized = False
            self._started_at = time.time()
        logger.debug("LangChainRuntime reset (removed %d).", removed)
        return removed

    def statistics(self) -> Dict[str, Any]:
        """
        Return a JSON-safe, **atomic** snapshot of runtime state.

        Every field is read under the lock, so the snapshot is internally
        consistent even under concurrent :meth:`run` calls.
        """
        with self._lock:
            return {
                "runs": len(self._history),
                "tool_calls": self.tool_calls,
                "depth": self.depth,
                "max_tools": self.max_tools,
                "context_shortened": self._context_shortened,
                "finalized": self._finalized,
                "config": self._config.to_dict(),
                "uptime_seconds": time.time() - self._started_at,
                "total_attempts": self._total_attempts,
                "failed_runs": self._failed_runs,
                "dropped_runs": self._dropped_count,
            }

    def metrics(self) -> RuntimeMetrics:
        """
        Aggregate per-run accuracy, depth, and tool-call averages over
        the current history.
        """
        with self._lock:
            runs = list(self._history)
            total_attempts = self._total_attempts
            failed = self._failed_runs
            dropped = self._dropped_count
            cumulative = self.tool_calls

        n = len(runs)
        if n == 0:
            return {
                "runs": 0,
                "total_attempts": total_attempts,
                "failed_runs": failed,
                "dropped_runs": dropped,
                "average_accuracy": 0.0,
                "average_depth": 0.0,
                "average_tool_calls_per_run": 0.0,
                "total_cumulative_tool_calls": cumulative,
            }
        return {
            "runs": n,
            "total_attempts": total_attempts,
            "failed_runs": failed,
            "dropped_runs": dropped,
            "average_accuracy": sum(r.accuracy for r in runs) / n,
            "average_depth": sum(r.conversation_depth for r in runs) / n,
            "average_tool_calls_per_run": sum(r.tool_calls for r in runs) / n,
            "total_cumulative_tool_calls": cumulative,
        }

    # ---------------------------------------------------------- serialization
    def to_dict(self, *, include_history: bool = False) -> Dict[str, Any]:
        with self._lock:
            payload: Dict[str, Any] = {
                "config": self._config.to_dict(),
                "strict": self._strict,
                "tool_calls": self.tool_calls,
                "depth": self.depth,
                "max_tools": self.max_tools,
                "context_shortened": self._context_shortened,
                "finalized": self._finalized,
                "started_at": self._started_at,
                "total_attempts": self._total_attempts,
                "failed_runs": self._failed_runs,
                "dropped_count": self._dropped_count,
                "config_metadata": dict(self.config),
            }
            if include_history:
                payload["history"] = [r.to_dict() for r in self._history]
        return payload

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "LangChainRuntime":
        if not isinstance(data, Mapping):
            raise LangChainRuntimeError(
                f"from_dict expects a Mapping, got {type(data).__name__}."
            )
        try:
            cfg = LangChainRuntimeConfig.from_dict(
                dict(data.get("config") or {})
            )
            runtime = cls(config=cfg, strict=bool(data.get("strict", True)))

            with runtime._lock:
                runtime.tool_calls = int(data.get("tool_calls", 0))
                runtime.depth = int(data.get("depth", 0))
                runtime.max_tools = int(
                    data.get("max_tools", cfg.base_max_tools)
                )
                runtime._context_shortened = bool(
                    data.get("context_shortened", False)
                )
                runtime._finalized = bool(data.get("finalized", False))
                runtime._started_at = float(
                    data.get("started_at", time.time())
                )
                runtime._total_attempts = int(
                    data.get("total_attempts", 0)
                )
                runtime._failed_runs = int(data.get("failed_runs", 0))
                runtime._dropped_count = int(data.get("dropped_count", 0))

                metadata = data.get("config_metadata")
                if isinstance(metadata, Mapping):
                    runtime.config = dict(metadata)

                for entry in data.get("history", []) or []:
                    runtime._append_history_locked(
                        LangChainRunResult.from_dict(entry)
                    )
            return runtime
        except LangChainRuntimeError:
            raise
        except (TypeError, ValueError) as exc:
            raise LangChainRuntimeError(
                f"invalid LangChainRuntime payload: {exc}"
            ) from exc

    def to_json(self, *, include_history: bool = False, **kwargs: Any) -> str:
        # Payload is primitive-only; no default= needed.
        return json.dumps(
            self.to_dict(include_history=include_history), **kwargs
        )

    @classmethod
    def from_json(cls, payload: str) -> "LangChainRuntime":
        try:
            data = json.loads(payload)
        except json.JSONDecodeError as exc:
            raise LangChainRuntimeError(f"Invalid JSON: {exc}") from exc
        return cls.from_dict(data)

    # ---------------------------------------------------------- context mgr
    def __enter__(self) -> "LangChainRuntime":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        if exc_type is None:
            try:
                self.finalize()
            except Exception:
                logger.exception("finalize() failed on context exit.")
                raise
        else:
            logger.warning(
                "LangChainRuntime scope exited with %s.", exc_type.__name__,
            )
        return None

    # ----------------------------------------------------------------- dunder
    def __repr__(self) -> str:
        with self._lock:
            return (
                "LangChainRuntime("
                f"runs={len(self._history)}, "
                f"tools={self.tool_calls}, "
                f"depth={self.depth}, "
                f"max_tools={self.max_tools}, "
                f"attempts={self._total_attempts}, "
                f"failed={self._failed_runs}, "
                f"finalized={self._finalized})"
            )

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, LangChainRuntime):
            return NotImplemented
        return self._config == other._config and self._strict == other._strict

    def __hash__(self) -> int:
        return hash((self._config, self._strict))


# --------------------------------------------------------------------------- #
# Public API
# --------------------------------------------------------------------------- #
__all__ = [
    "LangChainRunResult",
    "LangChainRuntime",
    "LangChainRuntimeConfig",
    "LangChainRuntimeError",
    "RunMetrics",
    "RuntimeMetrics",
]


# --------------------------------------------------------------------------- #
# Smoke test: python -m runtime.langchain_runtime
# --------------------------------------------------------------------------- #
if __name__ == "__main__":  # pragma: no cover
    logging.basicConfig(
        level=logging.INFO, format="%(levelname)s %(name)s: %(message)s"
    )

    # ---- Happy path -------------------------------------------------- #
    runtime = LangChainRuntime()
    print("repr       :", runtime)

    runtime.init({"framework": "langchain", "version": "0.1"})
    for i in range(3):
        metrics = runtime.run(f"query {i}")
        print(f"run {i}     :", metrics)

    # ---- Verify per-run vs cumulative semantics ---------------------- #
    results = runtime.history
    assert all(r.tool_calls == 2 for r in results), \
        "per-run tool_calls should be 2 for every run"
    assert [r.cumulative_tool_calls for r in results] == [2, 4, 6], \
        "cumulative_tool_calls should accumulate"
    print("semantics  : per-run=2, cumulative=2,4,6  [OK]")

    # ---- Throttling -------------------------------------------------- #
    new_max = runtime.reduce_tool_calls()
    print(f"max tools  : {runtime._config.base_max_tools} -> {new_max}")

    shortened_depth = runtime.shorten_context()
    print(f"shortened  : depth={shortened_depth}")

    # Post-throttle run reflects reduced tools + shortened flag.
    post = runtime.run("post-throttle")
    print("post-thrott :", post)
    assert post["tool_calls"] == 2  # min(tools_per_run=2, max_tools=4)
    assert post["context_shortened"] is True

    # ---- Finalize ---------------------------------------------------- #
    summary = runtime.finalize()
    print("finalize   :", summary)

    # ---- Bug fix: run after finalize --------------------------------- #
    try:
        runtime.run("post-finalize")
    except LangChainRuntimeError as exc:
        print("Rejected   :", exc)

    # ---- Bug fix: throttle after finalize ---------------------------- #
    try:
        runtime.reduce_tool_calls()
    except LangChainRuntimeError as exc:
        print("Rejected   :", exc)

    try:
        runtime.shorten_context()
    except LangChainRuntimeError as exc:
        print("Rejected   :", exc)

    # ---- Metrics ----------------------------------------------------- #
    print("metrics    :", runtime.metrics())

    # ---- Config metadata round-trip ---------------------------------- #
    rt2 = LangChainRuntime()
    rt2.init({"framework": "langchain", "version": "0.2"})
    rt2.run("a")
    payload = rt2.to_json(include_history=True)
    restored = LangChainRuntime.from_json(payload)
    assert restored.config == {"framework": "langchain", "version": "0.2"}, \
        "config metadata should survive round-trip"
    assert restored.tool_calls == rt2.tool_calls
    assert restored == rt2
    print("metadata   : round-trip OK")

    # ---- init(reset_throttle=False) preserves max_tools -------------- #
    rt3 = LangChainRuntime()
    rt3.init({})
    rt3.reduce_tool_calls()  # 5 -> 4
    before = rt3.statistics()["max_tools"]
    rt3.init({"meta": 1}, reset_throttle=False)
    after = rt3.statistics()["max_tools"]
    print(f"init(thr)  : before={before} after={after} (preserved)")
    assert before == after

    # ---- Reset ------------------------------------------------------- #
    removed = runtime.reset()
    print(f"reset      : {removed} run(s) removed")
    runtime.init({"reset": True})
    print("post-reset :", runtime.run("fresh"))

    # ---- Statistics -------------------------------------------------- #
    print("statistics :", {
        k: v for k, v in runtime.statistics().items()
        if k != "uptime_seconds"
    })

    # ---- Serialization ----------------------------------------------- #
    payload = runtime.to_json(include_history=True)
    restored = LangChainRuntime.from_json(payload)
    assert restored.to_dict() == runtime.to_dict()
    print("Round-trip OK.")

    # ---- Validation failures ----------------------------------------- #
    for bad_cfg in (
        dict(base_accuracy=1.5),
        dict(base_accuracy=-0.1),
        dict(base_accuracy="0.5"),            # was TypeError
        dict(base_accuracy=True),             # bool-as-float
        dict(base_max_tools=0),
        dict(base_max_tools=True),            # bool-as-int
        dict(tools_per_run=10, base_max_tools=5),
        dict(depth_increment=-1),
        dict(min_max_tools=0),
        dict(min_max_tools=10, base_max_tools=5),
        dict(max_conversation_depth=0),
        dict(max_conversation_depth="100"),   # was TypeError
        dict(max_history=0),
        dict(max_history="100"),              # was TypeError
    ):
        try:
            LangChainRuntimeConfig(**bad_cfg)  # type: ignore[arg-type]
        except LangChainRuntimeError as exc:
            print("Rejected cfg:", exc)

    strict = LangChainRuntime(strict=True)
    try:
        strict.init(None)
    except LangChainRuntimeError as exc:
        print("Rejected   :", exc)

    try:
        strict.run(123)  # type: ignore[arg-type]
    except LangChainRuntimeError as exc:
        print("Rejected   :", exc)

    # ---- Unknown config keys rejected by from_dict(strict=True) ------ #
    try:
        LangChainRuntimeConfig.from_dict(
            {"base_acuracy": 0.9}, strict=True
        )
    except LangChainRuntimeError as exc:
        print("Rejected   :", exc)

    # ---- from_dict leaks no ValueError ------------------------------- #
    try:
        LangChainRuntime.from_dict({"started_at": "not-a-number"})
    except LangChainRuntimeError as exc:
        print("Rejected   :", exc)

    try:
        LangChainRuntime.from_dict({"history": [{"accuracy": "bad"}]})
    except LangChainRuntimeError as exc:
        print("Rejected   :", exc)

    # ---- Non-strict coerces ------------------------------------------ #
    lenient = LangChainRuntime(strict=False)
    print("lenient    :", lenient.run(123))  # type: ignore[arg-type]

    # ---- Context manager --------------------------------------------- #
    with LangChainRuntime() as scoped:
        scoped.init({})
        scoped.run("ctx")
    print("Context    : OK")

    # ---- Subclass hooks ---------------------------------------------- #
    class _QueryAwareRuntime(LangChainRuntime):
        def _compute_accuracy(self, query: str, depth: int) -> float:
            return 0.5 if "hard" in query else 0.9

    aware = _QueryAwareRuntime()
    print("aware easy :", aware.run("easy question"))
    print("aware hard :", aware.run("hard question"))

    print("\nSmoke test passed.")
