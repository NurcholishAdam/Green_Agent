# src/runtime/autogen_runtime.py

"""
AutoGen Runtime Adapter
=======================

Framework-specific runtime adapter that simulates AutoGen-style multi-agent
conversation execution to measure accuracy, tool-call counts, and
conversation depth.

.. note::
    This is a **simulation mock**, not a real AutoGen integration. By default
    every ``run()`` returns the configured ``base_accuracy`` verbatim — it
    does **not** depend on the query. This is by design (unit-test fixture /
    benchmark harness). To customize behaviour per query, override the
    protected hooks :meth:`AutoGenRuntime._compute_accuracy` and
    :meth:`AutoGenRuntime._compute_tool_calls` in a subclass.

Enhancements
------------
- ``AutoGenRuntimeConfig`` — frozen, validated, ``slots``, concise
  ``__repr__``; every field type-checked (no more ``TypeError`` leaks from
  ``__post_init__``).
- ``AutoGenRunResult`` — frozen, ``slots``, parse-error wrapping.
- **Every** failure path raises :class:`AutoGenRuntimeError` — no
  ``TypeError`` / ``ValueError`` leaking from ``from_dict``.
- **Atomic ``run()``** — the result is constructed before depth / history
  are committed, so validation failures cannot corrupt state.
- **Atomic ``statistics()``** — every field read under the lock.
- **Counters**: ``total_attempts``, ``failed_runs``, ``dropped_runs``
  (history evicted from the bounded deque).
- ``metrics()`` — aggregate accuracy / depth / tool-call averages.
- ``init()`` accepts ``reset_throttle`` and ``reset_clock``; no silent
  state loss.
- ``__exit__`` re-raises ``finalize()`` failures instead of swallowing them.
- ``from_dict(..., strict=True)`` rejects unknown keys.
- ``to_json`` no longer uses ``default=str``.
- ``__eq__`` / ``__hash__`` on the runtime keyed on ``(config, strict)``.
- Pluggable accuracy / tool-call computation via protected hooks.
- Typed public returns: ``RunMetrics``, ``RuntimeMetrics``.
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
from typing import Any, Deque, Dict, Mapping, Optional, TypedDict

logger = logging.getLogger(__name__)


# --------------------------------------------------------------------------- #
# Errors
# --------------------------------------------------------------------------- #
class AutoGenRuntimeError(ValueError):
    """Raised for invalid AutoGen runtime inputs or configuration."""


# --------------------------------------------------------------------------- #
# Typed return shapes
# --------------------------------------------------------------------------- #
class RunMetrics(TypedDict):
    """Result of a single :meth:`AutoGenRuntime.run` call."""

    accuracy: float
    tool_calls: int
    conversation_depth: int
    context_shortened: bool


class RuntimeMetrics(TypedDict):
    """Result of :meth:`AutoGenRuntime.metrics`."""

    runs: int
    total_attempts: int
    failed_runs: int
    dropped_runs: int
    average_accuracy: float
    average_depth: float
    average_tool_calls: float


# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #
@dataclass(frozen=True, slots=True)
class AutoGenRuntimeConfig:
    """Tunable parameters for :class:`AutoGenRuntime`."""

    # Metric simulation.
    base_accuracy: float = 0.85
    depth_increment: int = 2
    base_tool_calls: int = 1

    # Throttling limits.
    min_depth_increment: int = 1
    min_tool_calls: int = 0
    max_conversation_depth: int = 1_000

    # Bounded run history.
    max_history: int = 1_000

    def __post_init__(self) -> None:
        # --- Numeric (float or int), non-bool, finite ------------------ #
        if isinstance(self.base_accuracy, bool) or not isinstance(
            self.base_accuracy, (int, float)
        ):
            raise AutoGenRuntimeError("base_accuracy must be numeric.")
        if not math.isfinite(float(self.base_accuracy)):
            raise AutoGenRuntimeError("base_accuracy must be finite.")
        if not 0.0 <= float(self.base_accuracy) <= 1.0:
            raise AutoGenRuntimeError("base_accuracy must be in [0, 1].")

        # --- Strictly int, non-bool ------------------------------------ #
        for name in (
            "depth_increment", "base_tool_calls",
            "min_depth_increment", "min_tool_calls",
            "max_conversation_depth", "max_history",
        ):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, int):
                raise AutoGenRuntimeError(f"{name} must be an int.")

        if self.depth_increment < 0:
            raise AutoGenRuntimeError("depth_increment must be >= 0.")
        if self.base_tool_calls < 0:
            raise AutoGenRuntimeError("base_tool_calls must be >= 0.")
        if self.min_depth_increment < 0:
            raise AutoGenRuntimeError("min_depth_increment must be >= 0.")
        if self.min_depth_increment > self.depth_increment:
            raise AutoGenRuntimeError(
                "min_depth_increment must be <= depth_increment."
            )
        if self.min_tool_calls < 0:
            raise AutoGenRuntimeError("min_tool_calls must be >= 0.")
        if self.min_tool_calls > self.base_tool_calls:
            raise AutoGenRuntimeError(
                "min_tool_calls must be <= base_tool_calls."
            )
        if self.max_conversation_depth <= 0:
            raise AutoGenRuntimeError("max_conversation_depth must be > 0.")
        if self.max_history <= 0:
            raise AutoGenRuntimeError("max_history must be > 0.")

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any], *, strict: bool = True
    ) -> "AutoGenRuntimeConfig":
        if not isinstance(data, Mapping):
            raise AutoGenRuntimeError(
                "AutoGenRuntimeConfig.from_dict expects a Mapping."
            )
        valid = set(cls.__dataclass_fields__.keys())
        kwargs: Dict[str, Any] = {}
        unknown = [k for k in data if k not in valid]
        if unknown:
            msg = f"unknown AutoGenRuntimeConfig keys: {unknown}"
            if strict:
                raise AutoGenRuntimeError(msg)
            logger.warning(msg)
        for k in data:
            if k in valid:
                kwargs[k] = data[k]
        try:
            return cls(**kwargs)
        except TypeError as exc:
            raise AutoGenRuntimeError(
                f"invalid AutoGenRuntimeConfig payload: {exc}"
            ) from exc

    def __repr__(self) -> str:
        return (
            "AutoGenRuntimeConfig("
            f"accuracy={self.base_accuracy:.3g}, "
            f"Δdepth={self.depth_increment}, "
            f"tools={self.base_tool_calls}, "
            f"max_depth={self.max_conversation_depth}, "
            f"max_history={self.max_history})"
        )


# --------------------------------------------------------------------------- #
# Run result
# --------------------------------------------------------------------------- #
@dataclass(frozen=True, slots=True)
class AutoGenRunResult:
    """Immutable metrics produced by one ``run`` invocation."""

    query: str
    accuracy: float
    tool_calls: int
    conversation_depth: int
    timestamp: datetime = field(
        default_factory=lambda: datetime.now(timezone.utc)
    )

    def __post_init__(self) -> None:
        if not isinstance(self.query, str):
            raise AutoGenRuntimeError("query must be a string.")
        if isinstance(self.accuracy, bool) or not isinstance(
            self.accuracy, (int, float)
        ):
            raise AutoGenRuntimeError("accuracy must be numeric.")
        if not math.isfinite(float(self.accuracy)) or not (
            0.0 <= float(self.accuracy) <= 1.0
        ):
            raise AutoGenRuntimeError(
                f"accuracy must be finite and in [0, 1], got {self.accuracy!r}."
            )
        if isinstance(self.tool_calls, bool) or not isinstance(
            self.tool_calls, int
        ) or self.tool_calls < 0:
            raise AutoGenRuntimeError(
                "tool_calls must be a non-negative int."
            )
        if isinstance(self.conversation_depth, bool) or not isinstance(
            self.conversation_depth, int
        ) or self.conversation_depth < 0:
            raise AutoGenRuntimeError(
                "conversation_depth must be a non-negative int."
            )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "query": self.query,
            "accuracy": float(self.accuracy),
            "tool_calls": int(self.tool_calls),
            "conversation_depth": int(self.conversation_depth),
            "timestamp": self.timestamp.isoformat(),
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "AutoGenRunResult":
        if not isinstance(data, Mapping):
            raise AutoGenRuntimeError(
                "AutoGenRunResult.from_dict expects a Mapping."
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
                raise AutoGenRuntimeError(
                    f"timestamp must be str|datetime|None, "
                    f"got {type(ts).__name__}."
                )
            return cls(
                query=str(data.get("query", "")),
                accuracy=float(data.get("accuracy", 0.0)),
                tool_calls=int(data.get("tool_calls", 0)),
                conversation_depth=int(data.get("conversation_depth", 0)),
                timestamp=timestamp,
            )
        except AutoGenRuntimeError:
            raise
        except (TypeError, ValueError) as exc:
            raise AutoGenRuntimeError(
                f"invalid AutoGenRunResult payload: {exc}"
            ) from exc

    def __repr__(self) -> str:
        return (
            "AutoGenRunResult("
            f"query={self.query!r}, "
            f"accuracy={self.accuracy:.3f}, "
            f"tool_calls={self.tool_calls}, "
            f"depth={self.conversation_depth})"
        )


# --------------------------------------------------------------------------- #
# Runtime
# --------------------------------------------------------------------------- #
class AutoGenRuntime:
    """
    Runtime adapter that simulates AutoGen-style agent execution.

    Thread-safe for all public methods. State mutations via direct attribute
    assignment (``runtime.graph_depth``, ``runtime.config``) are **not**
    thread-safe — use the public API. The original public API
    (``init``, ``run``, ``reduce_tool_calls``, ``shorten_context``,
    ``finalize``) is preserved; new parameters are keyword-only.

    Counters (``total_attempts``, ``failed_runs``, ``dropped_runs``) are
    cumulative for the lifetime of the instance — they are **not** reset by
    :meth:`init` or :meth:`reset`.
    """

    def __init__(
        self,
        *,
        config: Optional[AutoGenRuntimeConfig] = None,
        strict: bool = True,
    ) -> None:
        if config is not None and not isinstance(config, AutoGenRuntimeConfig):
            raise AutoGenRuntimeError(
                f"config must be an AutoGenRuntimeConfig, got "
                f"{type(config).__name__}."
            )
        self._config = config or AutoGenRuntimeConfig()
        self._strict = bool(strict)

        # Legacy attributes preserved (see class docstring re: thread-safety).
        self.graph_depth: int = 0
        self.config: Dict[str, Any] = {}

        self._lock = threading.RLock()
        self._history: Deque[AutoGenRunResult] = deque(
            maxlen=self._config.max_history
        )
        self._current_depth_increment: int = self._config.depth_increment
        self._current_tool_calls: int = self._config.base_tool_calls
        self._context_shortened: bool = False
        self._finalized: bool = False
        self._started_at: float = time.time()

        # Counters (cumulative, not reset by init/reset).
        self._total_attempts: int = 0
        self._failed_runs: int = 0
        self._dropped_count: int = 0

        logger.debug(
            "AutoGenRuntime initialized "
            "(accuracy=%.3f, depth_increment=%d, tool_calls=%d, strict=%s)",
            self._config.base_accuracy,
            self._config.depth_increment,
            self._config.base_tool_calls,
            self._strict,
        )

    # ------------------------------------------------------------------ props
    @property
    def config_typed(self) -> AutoGenRuntimeConfig:
        """Typed, immutable configuration (see also ``self.config`` dict)."""
        return self._config

    @property
    def history(self) -> list[AutoGenRunResult]:
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
        Compute the accuracy for a single run.

        Default: returns ``config.base_accuracy`` (query-independent mock).
        Override in subclasses to introduce per-query variation.
        """
        return self._config.base_accuracy

    def _compute_tool_calls(self, query: str) -> int:
        """
        Compute the tool-call count for a single run.

        Default: returns the currently throttled tool-call level.
        Override in subclasses for query-dependent behaviour.
        """
        return self._current_tool_calls

    # ---------------------------------------------------------- public API
    def init(
        self,
        config: Any,
        *,
        reset_throttle: bool = True,
        reset_clock: bool = True,
    ) -> None:
        """
        Configure the runtime with a caller-supplied object or mapping.

        Parameters
        ----------
        config : Mapping or any
            Caller metadata; stored verbatim in ``self.config``.
        reset_throttle : bool, default True
            If True, current depth-increment and tool-call count are restored
            from ``self._config``. Set to False to preserve throttling state
            earned via :meth:`reduce_tool_calls` / :meth:`shorten_context`.
        reset_clock : bool, default True
            If True, ``_started_at`` is reset so ``statistics()["uptime_seconds"]``
            restarts from zero.
        """
        if config is None:
            if self._strict:
                raise AutoGenRuntimeError("config must not be None.")
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
                self._current_depth_increment = self._config.depth_increment
                self._current_tool_calls = self._config.base_tool_calls
            if reset_clock:
                self._started_at = time.time()

        logger.debug("AutoGenRuntime init: config=%s", self.config)

    def run(self, query: Any) -> RunMetrics:
        """
        Simulate one execution of ``query``.

        Returns
        -------
        RunMetrics
            A dict with ``accuracy``, ``tool_calls``, ``conversation_depth``
            (legacy keys) plus ``context_shortened`` (additive).

        Raises
        ------
        AutoGenRuntimeError
            If ``strict=True`` and ``query`` is not a string, or if the
            runtime has been finalized. In either case the failure is
            recorded in ``failed_runs`` and **no** state is mutated.
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
                        raise AutoGenRuntimeError(msg)
                    logger.warning("%s Coercing via str().", msg)
                    query = str(query)

                if self._finalized:
                    raise AutoGenRuntimeError(
                        "runtime has been finalized; call init() to reset."
                    )

                next_depth = min(
                    self.graph_depth + self._current_depth_increment,
                    self._config.max_conversation_depth,
                )

                # Construct the result *before* committing any state, so
                # a validation failure leaves the runtime consistent.
                result = AutoGenRunResult(
                    query=query,
                    accuracy=self._compute_accuracy(query, next_depth),
                    tool_calls=self._compute_tool_calls(query),
                    conversation_depth=next_depth,
                )

                self.graph_depth = next_depth
                self._append_history_locked(result)
            except AutoGenRuntimeError:
                self._failed_runs += 1
                raise
            except Exception:
                self._failed_runs += 1
                raise

            context_shortened = self._context_shortened

        logger.debug(
            "AutoGen run: query=%r, depth=%d, tools=%d, shortened=%s",
            query, result.conversation_depth, result.tool_calls,
            context_shortened,
        )
        return {
            "accuracy": result.accuracy,
            "tool_calls": result.tool_calls,
            "conversation_depth": result.conversation_depth,
            "context_shortened": context_shortened,
        }

    def _append_history_locked(self, result: AutoGenRunResult) -> None:
        """Append under lock, tracking evictions. Caller must hold ``_lock``."""
        if len(self._history) == self._config.max_history:
            self._dropped_count += 1
        self._history.append(result)

    def reduce_tool_calls(self) -> int:
        """
        Reduce the tool-call intensity for subsequent runs.

        Returns the new tool-call count. Idempotent once the floor is reached.
        """
        with self._lock:
            if self._current_tool_calls <= self._config.min_tool_calls:
                return self._current_tool_calls
            self._current_tool_calls -= 1
            new_value = self._current_tool_calls
        logger.debug("Tool-call count reduced to %d.", new_value)
        return new_value

    def shorten_context(self) -> int:
        """
        Reduce the conversation-depth increment for subsequent runs.

        Returns the new depth increment. Sets ``_context_shortened`` to
        ``True`` so subsequent :meth:`run` results report the shortened
        context.
        """
        with self._lock:
            self._context_shortened = True
            if self._current_depth_increment <= self._config.min_depth_increment:
                return self._current_depth_increment
            self._current_depth_increment -= 1
            new_value = self._current_depth_increment
        logger.debug("Depth increment reduced to %d.", new_value)
        return new_value

    def finalize(self) -> Dict[str, Any]:
        """
        Finalize the runtime and return a summary.

        After finalization, :meth:`run` raises until :meth:`init` or
        :meth:`reset` is called.
        """
        with self._lock:
            self._finalized = True
            summary = {
                "finalized": True,
                "runs": len(self._history),
                "final_depth": self.graph_depth,
                "config_used": bool(self.config),
                "total_attempts": self._total_attempts,
                "failed_runs": self._failed_runs,
                "dropped_runs": self._dropped_count,
            }
        logger.debug("AutoGenRuntime finalized: %s", summary)
        return summary

    # ---------------------------------------------------------- lifecycle
    def reset(self, *, clear_history: bool = True) -> int:
        """
        Reset runtime state. Returns the number of history entries removed.

        Counters (``total_attempts``, ``failed_runs``, ``dropped_runs``) are
        **not** reset — they are cumulative for the object's lifetime.
        """
        with self._lock:
            removed = len(self._history)
            if clear_history:
                self._history.clear()
            self.graph_depth = 0
            self._current_depth_increment = self._config.depth_increment
            self._current_tool_calls = self._config.base_tool_calls
            self._context_shortened = False
            self._finalized = False
            self._started_at = time.time()
        logger.debug("AutoGenRuntime reset (removed %d).", removed)
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
                "graph_depth": self.graph_depth,
                "current_depth_increment": self._current_depth_increment,
                "current_tool_calls": self._current_tool_calls,
                "context_shortened": self._context_shortened,
                "finalized": self._finalized,
                "config": self._config.to_dict(),
                "uptime_seconds": time.time() - self._started_at,
                "total_attempts": self._total_attempts,
                "failed_runs": self._failed_runs,
                "dropped_runs": self._dropped_count,
            }

    def metrics(self) -> RuntimeMetrics:
        """Aggregate accuracy / depth / tool-call averages over the history."""
        with self._lock:
            runs = list(self._history)
            total_attempts = self._total_attempts
            failed = self._failed_runs
            dropped = self._dropped_count

        n = len(runs)
        if n == 0:
            return {
                "runs": 0,
                "total_attempts": total_attempts,
                "failed_runs": failed,
                "dropped_runs": dropped,
                "average_accuracy": 0.0,
                "average_depth": 0.0,
                "average_tool_calls": 0.0,
            }
        return {
            "runs": n,
            "total_attempts": total_attempts,
            "failed_runs": failed,
            "dropped_runs": dropped,
            "average_accuracy": sum(r.accuracy for r in runs) / n,
            "average_depth": sum(r.conversation_depth for r in runs) / n,
            "average_tool_calls": sum(r.tool_calls for r in runs) / n,
        }

    # ---------------------------------------------------------- serialization
    def to_dict(self, *, include_history: bool = False) -> Dict[str, Any]:
        with self._lock:
            payload: Dict[str, Any] = {
                "config": self._config.to_dict(),
                "strict": self._strict,
                "graph_depth": self.graph_depth,
                "current_depth_increment": self._current_depth_increment,
                "current_tool_calls": self._current_tool_calls,
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
    def from_dict(cls, data: Mapping[str, Any]) -> "AutoGenRuntime":
        if not isinstance(data, Mapping):
            raise AutoGenRuntimeError(
                f"from_dict expects a Mapping, got {type(data).__name__}."
            )
        try:
            cfg = AutoGenRuntimeConfig.from_dict(
                dict(data.get("config") or {})
            )
            runtime = cls(config=cfg, strict=bool(data.get("strict", True)))

            with runtime._lock:
                runtime.graph_depth = int(data.get("graph_depth", 0))
                runtime._current_depth_increment = int(
                    data.get("current_depth_increment", cfg.depth_increment)
                )
                runtime._current_tool_calls = int(
                    data.get("current_tool_calls", cfg.base_tool_calls)
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
                        AutoGenRunResult.from_dict(entry)
                    )
            return runtime
        except AutoGenRuntimeError:
            raise
        except (TypeError, ValueError) as exc:
            raise AutoGenRuntimeError(
                f"invalid AutoGenRuntime payload: {exc}"
            ) from exc

    def to_json(self, *, include_history: bool = False, **kwargs: Any) -> str:
        # Payload is primitive-only; no default= needed.
        return json.dumps(
            self.to_dict(include_history=include_history), **kwargs
        )

    @classmethod
    def from_json(cls, payload: str) -> "AutoGenRuntime":
        try:
            data = json.loads(payload)
        except json.JSONDecodeError as exc:
            raise AutoGenRuntimeError(f"Invalid JSON: {exc}") from exc
        return cls.from_dict(data)

    # ---------------------------------------------------------- context mgr
    def __enter__(self) -> "AutoGenRuntime":
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
                "AutoGenRuntime scope exited with %s.", exc_type.__name__,
            )
        return None

    # ----------------------------------------------------------------- dunder
    def __repr__(self) -> str:
        with self._lock:
            return (
                "AutoGenRuntime("
                f"depth={self.graph_depth}, "
                f"runs={len(self._history)}, "
                f"attempts={self._total_attempts}, "
                f"failed={self._failed_runs}, "
                f"accuracy={self._config.base_accuracy:.3f}, "
                f"finalized={self._finalized})"
            )

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, AutoGenRuntime):
            return NotImplemented
        return self._config == other._config and self._strict == other._strict

    def __hash__(self) -> int:
        return hash((self._config, self._strict))


# --------------------------------------------------------------------------- #
# Public API
# --------------------------------------------------------------------------- #
__all__ = [
    "AutoGenRunResult",
    "AutoGenRuntime",
    "AutoGenRuntimeConfig",
    "AutoGenRuntimeError",
    "RunMetrics",
    "RuntimeMetrics",
]


# --------------------------------------------------------------------------- #
# Smoke test: python -m runtime.autogen_runtime
# --------------------------------------------------------------------------- #
if __name__ == "__main__":  # pragma: no cover
    logging.basicConfig(
        level=logging.INFO, format="%(levelname)s %(name)s: %(message)s"
    )

    # ---- Happy path -------------------------------------------------- #
    runtime = AutoGenRuntime()
    print("repr       :", runtime)

    runtime.init({"framework": "autogen", "version": "0.2"})
    for i in range(3):
        metrics = runtime.run(f"query {i}")
        print(f"run {i}     :", metrics)

    # ---- Throttling -------------------------------------------------- #
    before = runtime.statistics()["current_tool_calls"]
    new_tools = runtime.reduce_tool_calls()
    print(f"tools      : {before} -> {new_tools}")

    before_depth = runtime.statistics()["current_depth_increment"]
    new_depth = runtime.shorten_context()
    print(f"depth inc  : {before_depth} -> {new_depth}")

    # ---- Finalize ---------------------------------------------------- #
    summary = runtime.finalize()
    print("finalize   :", summary)

    # ---- Bug fix: run after finalize --------------------------------- #
    try:
        runtime.run("post-finalize")
    except AutoGenRuntimeError as exc:
        print("Rejected   :", exc)

    # ---- Metrics ----------------------------------------------------- #
    print("metrics    :", runtime.metrics())

    # ---- init() with reset_throttle=False preserves tuning ----------- #
    runtime2 = AutoGenRuntime()
    runtime2.init({})
    runtime2.reduce_tool_calls()
    runtime2.reduce_tool_calls()   # base_tool_calls=1, min_tool_calls=0, so 1 -> 0
    tuned = runtime2.statistics()["current_tool_calls"]
    runtime2.init({"meta": 1}, reset_throttle=False)
    kept = runtime2.statistics()["current_tool_calls"]
    print(f"init       : tuned={tuned} kept={kept} (preserved)")

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
    restored = AutoGenRuntime.from_json(payload)
    assert restored.to_dict() == runtime.to_dict()
    assert restored == runtime
    print("Round-trip OK.")

    # ---- Validation failures ----------------------------------------- #
    for bad_cfg in (
        dict(base_accuracy=1.5),
        dict(base_accuracy=-0.1),
        dict(base_accuracy="0.5"),            # was TypeError
        dict(depth_increment=-1),
        dict(base_tool_calls=-1),
        dict(min_depth_increment=5, depth_increment=2),
        dict(min_tool_calls=3, base_tool_calls=1),
        dict(max_conversation_depth=0),
        dict(max_conversation_depth="100"),   # was TypeError
        dict(max_history=0),
        dict(max_history="100"),              # was TypeError
    ):
        try:
            AutoGenRuntimeConfig(**bad_cfg)  # type: ignore[arg-type]
        except AutoGenRuntimeError as exc:
            print("Rejected cfg:", exc)

    strict = AutoGenRuntime(strict=True)
    try:
        strict.init(None)
    except AutoGenRuntimeError as exc:
        print("Rejected   :", exc)

    try:
        strict.run(123)  # type: ignore[arg-type]
    except AutoGenRuntimeError as exc:
        print("Rejected   :", exc)

    # ---- Non-strict coerces ------------------------------------------ #
    lenient = AutoGenRuntime(strict=False)
    print("lenient    :", lenient.run(123))  # type: ignore[arg-type]

    # ---- from_dict leaks no ValueError ------------------------------- #
    try:
        AutoGenRuntime.from_dict({"started_at": "not-a-number"})
    except AutoGenRuntimeError as exc:
        print("Rejected   :", exc)

    try:
        AutoGenRuntime.from_dict({"history": [{"accuracy": "bad"}]})
    except AutoGenRuntimeError as exc:
        print("Rejected   :", exc)

    # ---- Context manager --------------------------------------------- #
    with AutoGenRuntime() as scoped:
        scoped.init({})
        scoped.run("ctx")
    print("Context    : OK")

    # ---- Subclass hook ----------------------------------------------- #
    class _QueryAwareRuntime(AutoGenRuntime):
        def _compute_accuracy(self, query: str, depth: int) -> float:
            return 0.5 if "hard" in query else 0.9

    aware = _QueryAwareRuntime()
    print("aware easy :", aware.run("easy question"))
    print("aware hard :", aware.run("hard question"))

    print("\nSmoke test passed.")
