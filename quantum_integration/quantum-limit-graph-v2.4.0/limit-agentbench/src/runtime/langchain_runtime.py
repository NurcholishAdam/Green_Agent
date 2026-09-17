# src/runtime/langchain_runtime.py

"""
LangChain Runtime Adapter
=========================

Framework-specific runtime adapter that simulates LangChain-style chain
execution to measure accuracy, tool-call counts, and conversation depth.

Enhancements
------------
- ``LangChainRuntimeConfig`` — frozen, validated: base accuracy, tool-call
  behaviour, depth limits, bounded history.
- ``LangChainRunResult`` — frozen dataclass with validation and serialization.
- **Full validation** of every argument; strict / non-strict modes.
- **Thread safety** — ``RLock`` guards mutable state.
- **Bounded run history** — ``deque(maxlen=config.max_history)``.
- **Implemented stubs** — ``shorten_context()`` now returns a structured
  result; ``finalize()`` returns a summary.
- ``reset()`` — clears counters and history.
- Serialization: ``to_dict`` / ``from_dict`` / ``to_json`` on the runtime
  and the result dataclass.
- ``__repr__``, custom ``LangChainRuntimeError(ValueError)``, lazy ``%s``
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
class LangChainRuntimeError(ValueError):
    """Raised for invalid LangChain runtime inputs or configuration."""


# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
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
        if not 0.0 <= self.base_accuracy <= 1.0:
            raise LangChainRuntimeError(
                "base_accuracy must be in [0, 1]."
            )
        for name in ("base_max_tools", "tools_per_run", "depth_increment"):
            value = getattr(self, name)
            if not isinstance(value, int) or value < 0:
                raise LangChainRuntimeError(
                    f"{name} must be a non-negative int."
                )
        if self.tools_per_run > self.base_max_tools:
            raise LangChainRuntimeError(
                "tools_per_run must be <= base_max_tools."
            )
        if not isinstance(self.min_max_tools, int) or self.min_max_tools < 1:
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
    def from_dict(cls, data: Mapping[str, Any]) -> "LangChainRuntimeConfig":
        if not isinstance(data, Mapping):
            raise LangChainRuntimeError(
                "LangChainRuntimeConfig.from_dict expects a Mapping."
            )
        valid = set(cls.__dataclass_fields__.keys())
        kwargs = {k: v for k, v in data.items() if k in valid}
        return cls(**kwargs)


# --------------------------------------------------------------------------- #
# Run result
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class LangChainRunResult:
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
            raise LangChainRuntimeError("query must be a string.")
        if isinstance(self.accuracy, bool) or not isinstance(
            self.accuracy, (int, float)
        ):
            raise LangChainRuntimeError("accuracy must be numeric.")
        fa = float(self.accuracy)
        if math.isnan(fa) or math.isinf(fa) or not 0.0 <= fa <= 1.0:
            raise LangChainRuntimeError(
                f"accuracy must be finite and in [0, 1], got "
                f"{self.accuracy!r}."
            )
        if not isinstance(self.tool_calls, int) or self.tool_calls < 0:
            raise LangChainRuntimeError(
                "tool_calls must be a non-negative int."
            )
        if not isinstance(self.conversation_depth, int) or self.conversation_depth < 0:
            raise LangChainRuntimeError(
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
    def from_dict(cls, data: Mapping[str, Any]) -> "LangChainRunResult":
        if not isinstance(data, Mapping):
            raise LangChainRuntimeError(
                "LangChainRunResult.from_dict expects a Mapping."
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
            "LangChainRunResult("
            f"query={self.query!r}, "
            f"accuracy={self.accuracy:.3f}, "
            f"tool_calls={self.tool_calls}, "
            f"depth={self.conversation_depth})"
        )


# --------------------------------------------------------------------------- #
# Runtime
# --------------------------------------------------------------------------- #
class LangChainRuntime:
    """
    Runtime adapter that simulates LangChain-style chain execution.

    Thread-safe, serializable, and bounded in memory. The original public API
    (``init``, ``run``, ``reduce_tool_calls``, ``shorten_context``,
    ``finalize``) is preserved; new parameters are keyword-only.
    """

    def __init__(
        self,
        *,
        config: Optional[LangChainRuntimeConfig] = None,
        strict: bool = True,
    ) -> None:
        self._config = config or LangChainRuntimeConfig()
        self._strict = bool(strict)

        # Legacy attributes preserved.
        self.tool_calls: int = 0
        self.depth: int = 0
        self.max_tools: int = self._config.base_max_tools
        self.config: Dict[str, Any] = {}

        self._lock = threading.RLock()
        self._history: Deque[LangChainRunResult] = deque(
            maxlen=self._config.max_history
        )
        self._context_shortened: bool = False
        self._finalized: bool = False
        self._started_at: float = time.time()

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
        return self._config

    @property
    def history(self) -> List[LangChainRunResult]:
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
                raise LangChainRuntimeError("config must not be None.")
            logger.warning("config is None; ignoring.")
            return
        if isinstance(config, Mapping):
            self.config = dict(config)
        else:
            self.config = {"value": config}

        with self._lock:
            self._finalized = False
            self._context_shortened = False
            self.max_tools = self._config.base_max_tools

        logger.debug("LangChainRuntime initialized with config=%s", self.config)

    def run(self, query: Any) -> Dict[str, Any]:
        """
        Simulate one execution of ``query``.

        Returns a dict with keys ``accuracy``, ``tool_calls``, and
        ``conversation_depth`` — identical to the original return shape.
        """
        if not isinstance(query, str):
            msg = f"query must be a string, got {type(query).__name__}."
            if self._strict:
                raise LangChainRuntimeError(msg)
            logger.warning("%s Coercing via str().", msg)
            query = str(query)

        with self._lock:
            if self._finalized:
                raise LangChainRuntimeError(
                    "runtime has been finalized; call init() to reset."
                )
            used = min(self._config.tools_per_run, self.max_tools)
            self.tool_calls += used
            self.depth = min(
                self.depth + self._config.depth_increment,
                self._config.max_conversation_depth,
            )

            result = LangChainRunResult(
                query=query,
                accuracy=self._config.base_accuracy,
                tool_calls=self.tool_calls,
                conversation_depth=self.depth,
            )
            self._history.append(result)

        logger.debug(
            "LangChain run: query=%r, tools=%d, depth=%d",
            query, self.tool_calls, self.depth,
        )
        return {
            "accuracy": result.accuracy,
            "tool_calls": result.tool_calls,
            "conversation_depth": result.conversation_depth,
        }

    def reduce_tool_calls(self) -> int:
        """
        Reduce the maximum number of tools available for subsequent runs.

        Returns the new ``max_tools`` value.
        """
        with self._lock:
            if self.max_tools <= self._config.min_max_tools:
                return self.max_tools
            self.max_tools -= 1
            new_value = self.max_tools
        logger.debug("max_tools reduced to %d.", new_value)
        return new_value

    def shorten_context(self) -> int:
        """
        Trim the conversation depth for subsequent runs.

        Returns the new depth. The original was a ``pass`` stub.
        """
        with self._lock:
            self.depth = max(0, self.depth // 2)
            self._context_shortened = True
            new_depth = self.depth
        logger.debug("Context shortened; depth=%d.", new_depth)
        return new_depth

    def finalize(self) -> Dict[str, Any]:
        """
        Finalize the runtime and return a summary.

        The original was a ``pass`` stub. Now returns ``{"finalized": True,
        "runs": N, "total_tool_calls": M}``.
        """
        with self._lock:
            self._finalized = True
            summary = {
                "finalized": True,
                "runs": len(self._history),
                "total_tool_calls": self.tool_calls,
                "final_depth": self.depth,
                "final_max_tools": self.max_tools,
            }
        logger.debug("LangChainRuntime finalized: %s", summary)
        return summary

    # ---------------------------------------------------------- lifecycle
    def reset(self, *, clear_history: bool = True) -> int:
        """Reset the runtime state. Returns the number of history entries removed."""
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
        """Return a JSON-safe snapshot of the runtime state."""
        with self._lock:
            history = list(self._history)
        return {
            "runs": len(history),
            "tool_calls": self.tool_calls,
            "depth": self.depth,
            "max_tools": self.max_tools,
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
                "tool_calls": self.tool_calls,
                "depth": self.depth,
                "max_tools": self.max_tools,
                "context_shortened": self._context_shortened,
                "finalized": self._finalized,
                "started_at": self._started_at,
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
        cfg = LangChainRuntimeConfig.from_dict(
            dict(data.get("config", {}) or {})
        )
        runtime = cls(config=cfg, strict=bool(data.get("strict", True)))
        with runtime._lock:
            runtime.tool_calls = int(data.get("tool_calls", 0))
            runtime.depth = int(data.get("depth", 0))
            runtime.max_tools = int(data.get("max_tools", cfg.base_max_tools))
            runtime._context_shortened = bool(
                data.get("context_shortened", False)
            )
            runtime._finalized = bool(data.get("finalized", False))
            runtime._started_at = float(data.get("started_at", time.time()))
            for entry in data.get("history", []):
                runtime._history.append(LangChainRunResult.from_dict(entry))
        return runtime

    def to_json(self, **kwargs: Any) -> str:
        return json.dumps(self.to_dict(), default=str, **kwargs)

    @classmethod
    def from_json(cls, payload: str) -> "LangChainRuntime":
        try:
            return cls.from_dict(json.loads(payload))
        except json.JSONDecodeError as exc:
            raise LangChainRuntimeError(f"Invalid JSON: {exc}") from exc

    # ---------------------------------------------------------- context mgr
    def __enter__(self) -> "LangChainRuntime":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        if exc_type is None:
            try:
                self.finalize()
            except Exception:
                logger.exception("finalize() failed on context exit.")
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
                f"finalized={self._finalized})"
            )


# --------------------------------------------------------------------------- #
# Public API
# --------------------------------------------------------------------------- #
__all__ = [
    "LangChainRunResult",
    "LangChainRuntime",
    "LangChainRuntimeConfig",
    "LangChainRuntimeError",
]


# --------------------------------------------------------------------------- #
# Smoke test: python -m runtime.langchain_runtime
# --------------------------------------------------------------------------- #
if __name__ == "__main__":  # pragma: no cover
    logging.basicConfig(
        level=logging.INFO, format="%(levelname)s %(name)s: %(message)s"
    )

    # ---- Happy path ------------------------------------------------ #
    runtime = LangChainRuntime()
    print("repr       :", runtime)

    runtime.init({"framework": "langchain", "version": "0.1"})
    for i in range(3):
        metrics = runtime.run(f"query {i}")
        print(f"run {i}     :", metrics)

    # ---- Throttling ------------------------------------------------- #
    new_max = runtime.reduce_tool_calls()
    print(f"max tools  : {runtime._config.base_max_tools} -> {new_max}")

    shortened_depth = runtime.shorten_context()
    print(f"shortened  : depth={shortened_depth}")

    # ---- Finalize --------------------------------------------------- #
    summary = runtime.finalize()
    print("finalize   :", summary)

    # ---- Bug fix: run after finalize -------------------------------- #
    try:
        runtime.run("post-finalize")
    except LangChainRuntimeError as exc:
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
    restored = LangChainRuntime.from_json(payload)
    assert restored.to_dict() == runtime.to_dict()
    print("Round-trip OK.")

    # ---- Validation failures ---------------------------------------- #
    for bad_cfg in (
        dict(base_accuracy=1.5),
        dict(base_accuracy=-0.1),
        dict(base_max_tools=0),
        dict(tools_per_run=10, base_max_tools=5),
        dict(depth_increment=-1),
        dict(min_max_tools=0),
        dict(min_max_tools=10, base_max_tools=5),
        dict(max_conversation_depth=0),
        dict(max_history=0),
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

    # ---- Non-strict coerces ----------------------------------------- #
    lenient = LangChainRuntime(strict=False)
    print("lenient    :", lenient.run(123))  # type: ignore[arg-type]

    # ---- Context manager -------------------------------------------- #
    with LangChainRuntime() as scoped:
        scoped.init({})
        scoped.run("ctx")
    print("Context    : OK")

    print("\nSmoke test passed.")
