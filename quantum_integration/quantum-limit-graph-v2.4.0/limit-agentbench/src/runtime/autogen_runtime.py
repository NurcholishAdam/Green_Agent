# src/runtime/autogen_runtime.py

"""
AutoGen Runtime Adapter
=======================

Framework-specific runtime adapter that simulates AutoGen-style multi-agent
conversation execution to measure accuracy, tool-call counts, and
conversation depth.

Enhancements
------------
- ``AutoGenRuntimeConfig`` — frozen, validated: base accuracy, depth
  increment, tool-call behaviour, bounded history.
- ``AutoGenRunResult`` — frozen dataclass with validation and serialization.
- **Full validation** of every argument; strict / non-strict modes.
- **Thread safety** — ``RLock`` guards mutable state.
- **Bounded run history** — ``deque(maxlen=config.max_history)``.
- **Implemented stubs** — ``reduce_tool_calls()`` and ``shorten_context()``
  now return structured results instead of doing nothing.
- ``reset()`` — clears depth and history.
- Serialization: ``to_dict`` / ``from_dict`` / ``to_json`` on the runtime
  and the result dataclass.
- ``__repr__``, custom ``AutoGenRuntimeError(ValueError)``, lazy ``%s``
  logging, and a ``__main__`` smoke test.
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
from typing import Any, Deque, Dict, List, Mapping, Optional

logger = logging.getLogger(__name__)


# --------------------------------------------------------------------------- #
# Errors
# --------------------------------------------------------------------------- #
class AutoGenRuntimeError(ValueError):
    """Raised for invalid AutoGen runtime inputs or configuration."""


# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
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
        if not 0.0 <= self.base_accuracy <= 1.0:
            raise AutoGenRuntimeError(
                "base_accuracy must be in [0, 1]."
            )
        if not isinstance(self.depth_increment, int) or self.depth_increment < 0:
            raise AutoGenRuntimeError(
                "depth_increment must be a non-negative int."
            )
        if not isinstance(self.base_tool_calls, int) or self.base_tool_calls < 0:
            raise AutoGenRuntimeError(
                "base_tool_calls must be a non-negative int."
            )
        if not isinstance(self.min_depth_increment, int) or self.min_depth_increment < 0:
            raise AutoGenRuntimeError(
                "min_depth_increment must be a non-negative int."
            )
        if self.min_depth_increment > self.depth_increment:
            raise AutoGenRuntimeError(
                "min_depth_increment must be <= depth_increment."
            )
        if not isinstance(self.min_tool_calls, int) or self.min_tool_calls < 0:
            raise AutoGenRuntimeError(
                "min_tool_calls must be a non-negative int."
            )
        if self.min_tool_calls > self.base_tool_calls:
            raise AutoGenRuntimeError(
                "min_tool_calls must be <= base_tool_calls."
            )
        if self.max_conversation_depth <= 0:
            raise AutoGenRuntimeError(
                "max_conversation_depth must be > 0."
            )
        if self.max_history <= 0:
            raise AutoGenRuntimeError("max_history must be > 0.")

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "AutoGenRuntimeConfig":
        if not isinstance(data, Mapping):
            raise AutoGenRuntimeError(
                "AutoGenRuntimeConfig.from_dict expects a Mapping."
            )
        valid = set(cls.__dataclass_fields__.keys())
        kwargs = {k: v for k, v in data.items() if k in valid}
        return cls(**kwargs)


# --------------------------------------------------------------------------- #
# Run result
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
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
        for name in ("accuracy",):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, (int, float)):
                raise AutoGenRuntimeError(f"{name} must be numeric.")
            fv = float(value)
            if math.isnan(fv) or math.isinf(fv) or not 0.0 <= fv <= 1.0:
                raise AutoGenRuntimeError(
                    f"{name} must be finite and in [0, 1], got {value!r}."
                )
        if not isinstance(self.tool_calls, int) or self.tool_calls < 0:
            raise AutoGenRuntimeError(
                "tool_calls must be a non-negative int."
            )
        if not isinstance(self.conversation_depth, int) or self.conversation_depth < 0:
            raise AutoGenRuntimeError(
                "conversation_depth must be a non-negative int."
            )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "query": self.query,
            "accuracy": self.accuracy,
            "tool_calls": self.tool_calls,
            "conversation_depth": self.conversation_depth,
            "timestamp": self.timestamp.isoformat(),
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "AutoGenRunResult":
        if not isinstance(data, Mapping):
            raise AutoGenRuntimeError(
                "AutoGenRunResult.from_dict expects a Mapping."
            )
        ts = data.get("timestamp")
        if isinstance(ts, str):
            timestamp = datetime.fromisoformat(ts)
            if not timestamp.tzinfo:
                timestamp = timestamp.replace(tzinfo=timezone.utc)
        elif isinstance(ts, datetime):
            timestamp = ts
        else:
            timestamp = datetime.now(timezone.utc)
        return cls(
            query=str(data.get("query", "")),
            accuracy=float(data.get("accuracy", 0.0)),
            tool_calls=int(data.get("tool_calls", 0)),
            conversation_depth=int(data.get("conversation_depth", 0)),
            timestamp=timestamp,
        )

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

    Thread-safe, serializable, and bounded in memory. The original public API
    (``init``, ``run``, ``reduce_tool_calls``, ``shorten_context``,
    ``finalize``) is preserved; new parameters are keyword-only.
    """

    def __init__(
        self,
        *,
        config: Optional[AutoGenRuntimeConfig] = None,
        strict: bool = True,
    ) -> None:
        self._config = config or AutoGenRuntimeConfig()
        self._strict = bool(strict)

        # Legacy attributes preserved.
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
        return self._config

    @property
    def history(self) -> List[AutoGenRunResult]:
        with self._lock:
            return list(self._history)

    @property
    def finalized(self) -> bool:
        with self._lock:
            return self._finalized

    # ---------------------------------------------------------- public API
    def init(self, config: Any) -> None:
        """Configure the runtime with a caller-supplied object or mapping."""
        if config is None:
            if self._strict:
                raise AutoGenRuntimeError("config must not be None.")
            logger.warning("config is None; ignoring.")
            return
        if isinstance(config, Mapping):
            self.config = dict(config)
        else:
            self.config = {"value": config}

        with self._lock:
            self._finalized = False
            self._context_shortened = False
            self._current_depth_increment = self._config.depth_increment
            self._current_tool_calls = self._config.base_tool_calls

        logger.debug("AutoGenRuntime initialized with config=%s", self.config)

    def run(self, query: Any) -> Dict[str, Any]:
        """
        Simulate one execution of ``query``.

        Returns a dict with keys ``accuracy``, ``tool_calls``, and
        ``conversation_depth`` — identical to the original return shape.
        """
        if not isinstance(query, str):
            msg = f"query must be a string, got {type(query).__name__}."
            if self._strict:
                raise AutoGenRuntimeError(msg)
            logger.warning("%s Coercing via str().", msg)
            query = str(query)

        with self._lock:
            if self._finalized:
                raise AutoGenRuntimeError(
                    "runtime has been finalized; call init() to reset."
                )
            self.graph_depth = min(
                self.graph_depth + self._current_depth_increment,
                self._config.max_conversation_depth,
            )
            depth = self.graph_depth
            tool_calls = self._current_tool_calls

            result = AutoGenRunResult(
                query=query,
                accuracy=self._config.base_accuracy,
                tool_calls=tool_calls,
                conversation_depth=depth,
            )
            self._history.append(result)

        logger.debug(
            "AutoGen run: query=%r, depth=%d, tools=%d",
            query, depth, tool_calls,
        )
        return {
            "accuracy": result.accuracy,
            "tool_calls": result.tool_calls,
            "conversation_depth": result.conversation_depth,
        }

    def reduce_tool_calls(self) -> int:
        """
        Reduce the tool-call intensity for subsequent runs.

        Returns the new tool-call count. The original was a ``pass`` stub.
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

        Returns the new depth increment. The original was a ``pass`` stub.
        """
        with self._lock:
            if self._current_depth_increment <= self._config.min_depth_increment:
                self._context_shortened = True
                return self._current_depth_increment
            self._current_depth_increment -= 1
            self._context_shortened = True
            new_value = self._current_depth_increment
        logger.debug("Depth increment reduced to %d.", new_value)
        return new_value

    def finalize(self) -> Dict[str, Any]:
        """
        Finalize the runtime and return a summary.

        The original was a ``pass`` stub. Now returns ``{"finalized": True,
        "runs": N, "final_depth": D}``.
        """
        with self._lock:
            self._finalized = True
            summary = {
                "finalized": True,
                "runs": len(self._history),
                "final_depth": self.graph_depth,
                "config_used": bool(self.config),
            }
        logger.debug("AutoGenRuntime finalized: %s", summary)
        return summary

    # ---------------------------------------------------------- lifecycle
    def reset(self, *, clear_history: bool = True) -> int:
        """Reset the runtime state. Returns the number of history entries removed."""
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
        """Return a JSON-safe snapshot of the runtime state."""
        with self._lock:
            history = list(self._history)
        return {
            "runs": len(history),
            "graph_depth": self.graph_depth,
            "current_depth_increment": self._current_depth_increment,
            "current_tool_calls": self._current_tool_calls,
            "context_shortened": self._context_shortened,
            "finalized": self._finalized,
            "config": self._config.to_dict(),
            "uptime_seconds": time.time() - self._started_at,
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
        cfg = AutoGenRuntimeConfig.from_dict(
            dict(data.get("config", {}) or {})
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
            runtime._started_at = float(data.get("started_at", time.time()))
            for entry in data.get("history", []):
                runtime._history.append(AutoGenRunResult.from_dict(entry))
        return runtime

    def to_json(self, **kwargs: Any) -> str:
        return json.dumps(self.to_dict(), default=str, **kwargs)

    @classmethod
    def from_json(cls, payload: str) -> "AutoGenRuntime":
        try:
            return cls.from_dict(json.loads(payload))
        except json.JSONDecodeError as exc:
            raise AutoGenRuntimeError(f"Invalid JSON: {exc}") from exc

    # ---------------------------------------------------------- context mgr
    def __enter__(self) -> "AutoGenRuntime":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        if exc_type is None:
            try:
                self.finalize()
            except Exception:
                logger.exception("finalize() failed on context exit.")
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
                f"accuracy={self._config.base_accuracy:.3f}, "
                f"finalized={self._finalized})"
            )


# --------------------------------------------------------------------------- #
# Public API
# --------------------------------------------------------------------------- #
__all__ = [
    "AutoGenRunResult",
    "AutoGenRuntime",
    "AutoGenRuntimeConfig",
    "AutoGenRuntimeError",
]


# --------------------------------------------------------------------------- #
# Smoke test: python -m runtime.autogen_runtime
# --------------------------------------------------------------------------- #
if __name__ == "__main__":  # pragma: no cover
    logging.basicConfig(
        level=logging.INFO, format="%(levelname)s %(name)s: %(message)s"
    )

    # ---- Happy path ------------------------------------------------ #
    runtime = AutoGenRuntime()
    print("repr       :", runtime)

    runtime.init({"framework": "autogen", "version": "0.2"})
    for i in range(3):
        metrics = runtime.run(f"query {i}")
        print(f"run {i}     :", metrics)

    # ---- Throttling ------------------------------------------------- #
    before = runtime.statistics()["current_tool_calls"]
    new_tools = runtime.reduce_tool_calls()
    print(f"tools      : {before} -> {new_tools}")

    before_depth = runtime.statistics()["current_depth_increment"]
    new_depth = runtime.shorten_context()
    print(f"depth inc  : {before_depth} -> {new_depth}")

    # ---- Finalize --------------------------------------------------- #
    summary = runtime.finalize()
    print("finalize   :", summary)

    # ---- Bug fix: run after finalize -------------------------------- #
    try:
        runtime.run("post-finalize")
    except AutoGenRuntimeError as exc:
        print("Rejected   :", exc)

    # ---- Reset ------------------------------------------------------ #
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
    payload = runtime.to_json()
    restored = AutoGenRuntime.from_json(payload)
    assert restored.to_dict() == runtime.to_dict()
    print("Round-trip OK.")

    # ---- Validation failures ---------------------------------------- #
    for bad_cfg in (
        dict(base_accuracy=1.5),
        dict(base_accuracy=-0.1),
        dict(depth_increment=-1),
        dict(base_tool_calls=-1),
        dict(min_depth_increment=5, depth_increment=2),
        dict(min_tool_calls=3, base_tool_calls=1),
        dict(max_conversation_depth=0),
        dict(max_history=0),
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

    # ---- Non-strict coerces ----------------------------------------- #
    lenient = AutoGenRuntime(strict=False)
    print("lenient    :", lenient.run(123))  # type: ignore[arg-type]

    # ---- Context manager -------------------------------------------- #
    with AutoGenRuntime() as scoped:
        scoped.init({})
        scoped.run("ctx")
    print("Context    : OK")

    print("\nSmoke test passed.")
