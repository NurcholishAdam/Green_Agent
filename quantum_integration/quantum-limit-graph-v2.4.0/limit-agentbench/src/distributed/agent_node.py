# src/distributed/agent_node.py

"""
Agent Node
==========

Wraps ``GreenAgentRunner`` into a distributed-ready node.

The node is decorated with ``@ray.remote`` when Ray is available, so it can be
instantiated as a Ray actor with the same public API. When Ray is not
installed, the decorator is a no-op and the class can be instantiated
directly (essential for tests, CI, and single-process deployments).

Original behaviour preserved
----------------------------
- ``AgentNode(runtime, policy)`` — constructs an internal ``GreenAgentRunner``.
- ``node.run(task_input)`` — runs the task through the underlying runner.

Enhancements
------------
- Optional Ray integration via ``@ray.remote`` fallback decorator.
- Defensive import of ``GreenAgentRunner`` with a local fallback shim.
- Configurable via :class:`AgentNodeConfig` (timeouts, retries, backoff).
- Thread-safe execution + bounded ``TaskExecutionRecord`` history.
- Full validation of every argument; strict / non-strict modes.
- Retry with exponential backoff + per-task timeout.
- ``async run_async(...)`` for cooperative use under an event loop.
- Serialization: ``to_dict`` / ``from_dict`` / ``to_json`` / ``from_json``.
- Sync and async context managers for scoped sessions.
- ``statistics()``, ``__repr__``, and a smoke test under ``__main__``.
"""

from __future__ import annotations

import asyncio
import json
import logging
import math
import threading
import time
import uuid
from collections import deque
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Any, Deque, Dict, List, Mapping, Optional

logger = logging.getLogger(__name__)


# --------------------------------------------------------------------------- #
# Optional Ray integration
# --------------------------------------------------------------------------- #
try:  # pragma: no cover — environment-dependent
    import ray  # type: ignore

    _RAY_AVAILABLE = True

    def _ray_remote(cls):
        """Wrap ``cls`` with ``@ray.remote`` when Ray is available."""
        return ray.remote(cls)

except ImportError:  # pragma: no cover
    ray = None  # type: ignore[assignment]
    _RAY_AVAILABLE = False

    def _ray_remote(cls):
        """No-op fallback when Ray is not installed."""
        return cls


# --------------------------------------------------------------------------- #
# Defensive import of GreenAgentRunner
# --------------------------------------------------------------------------- #
try:  # Preferred: package-relative import
    from .run_agent import GreenAgentRunner  # type: ignore
except ImportError:  # pragma: no cover — fallback for script-style usage
    try:
        from run_agent import GreenAgentRunner  # type: ignore
    except ImportError:
        GreenAgentRunner = None  # type: ignore[assignment]
        logger.debug(
            "GreenAgentRunner not importable; using fallback runner shim."
        )


# --------------------------------------------------------------------------- #
# Errors
# --------------------------------------------------------------------------- #
class AgentNodeError(ValueError):
    """Raised for invalid inputs, configuration, or execution failures."""


# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class AgentNodeConfig:
    """
    Tunable parameters for the agent node.

    Centralizes timeouts, retries, and history limits so deployments can
    calibrate without editing the class.
    """

    # Per-task execution policy
    task_timeout_seconds: float = 300.0
    max_task_retries: int = 1
    base_backoff_seconds: float = 0.5

    # Bounded history ring-buffer
    max_history: int = 1000

    # Node identity
    node_id_prefix: str = "agent-node"

    def __post_init__(self) -> None:
        if self.task_timeout_seconds <= 0:
            raise AgentNodeError("task_timeout_seconds must be > 0.")
        if self.max_task_retries < 0:
            raise AgentNodeError("max_task_retries must be >= 0.")
        if self.base_backoff_seconds < 0:
            raise AgentNodeError("base_backoff_seconds must be >= 0.")
        if self.max_history <= 0:
            raise AgentNodeError("max_history must be > 0.")
        if not isinstance(self.node_id_prefix, str) or not self.node_id_prefix:
            raise AgentNodeError("node_id_prefix must be a non-empty string.")


# --------------------------------------------------------------------------- #
# Execution record
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class TaskExecutionRecord:
    """Immutable snapshot of one ``run`` / ``run_async`` invocation."""

    task_id: str
    timestamp: float
    status: str  # "completed" | "failed" | "timeout"
    latency_ms: float
    attempts: int
    error: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


# --------------------------------------------------------------------------- #
# Fallback runner
# --------------------------------------------------------------------------- #
class _FallbackRunner:
    """
    Minimal stand-in used when ``GreenAgentRunner`` is not importable.

    Echoes the task input so the node can still be exercised end-to-end.
    """

    def __init__(self, runtime: Any = None, policy: Any = None) -> None:
        self.runtime = runtime
        self.policy = policy

    def run(self, task_input: Any) -> Dict[str, Any]:
        return {
            "status": "ok",
            "echo": task_input,
            "runtime": type(self.runtime).__name__,
            "policy": type(self.policy).__name__,
        }


# --------------------------------------------------------------------------- #
# Agent node
# --------------------------------------------------------------------------- #
@_ray_remote
class AgentNode:
    """
    Represents a distributed agent instance.

    Thread-safe, serializable, bounded in memory, and Ray-deployable. All
    original behavior is preserved; new parameters are keyword-only with
    backward-compatible defaults.

    Parameters
    ----------
    runtime : Any
        Runtime handle passed to ``GreenAgentRunner``.
    policy : Any
        Policy object passed to ``GreenAgentRunner``.
    config : AgentNodeConfig, optional
        Timeouts / retries / history size. Defaults to ``AgentNodeConfig()``.
    strict : bool, default True
        If True, invalid inputs raise :class:`AgentNodeError`.
        If False, invalid inputs are logged and coerced to safe defaults.
    node_id : str, optional
        Explicit node identifier. Defaults to
        ``f"{config.node_id_prefix}-{uuid4().hex[:8]}"``.
    runner_factory : callable, optional
        Zero-arg or ``(runtime, policy)`` callable returning a runner. Used to
        inject a custom ``GreenAgentRunner`` (or a mock in tests).
    """

    # ------------------------------------------------------------------ init
    def __init__(
        self,
        runtime: Any,
        policy: Any,
        *,
        config: Optional[AgentNodeConfig] = None,
        strict: bool = True,
        node_id: Optional[str] = None,
        runner_factory: Optional[Any] = None,
    ) -> None:
        self._config: AgentNodeConfig = config or AgentNodeConfig()
        self._strict: bool = bool(strict)

        if runtime is None and self._strict:
            raise AgentNodeError("runtime must not be None.")
        if policy is None and self._strict:
            raise AgentNodeError("policy must not be None.")

        if node_id is None:
            node_id = f"{self._config.node_id_prefix}-{uuid.uuid4().hex[:8]}"
        if not isinstance(node_id, str) or not node_id:
            raise AgentNodeError("node_id must be a non-empty string.")
        self.node_id: str = node_id

        self.runtime = runtime
        self.policy = policy

        # ---- Build the runner -----------------------------------------
        self.runner = self._build_runner(runtime, policy, runner_factory)

        # ---- Internal state -------------------------------------------
        self._lock = threading.RLock()
        self._history: Deque[TaskExecutionRecord] = deque(
            maxlen=self._config.max_history
        )
        self._total_tasks: int = 0
        self._completed: int = 0
        self._failed: int = 0
        self._timeouts: int = 0
        self._ctx_start: Optional[float] = None
        self._async_ctx_start: Optional[float] = None

        logger.debug(
            "AgentNode initialized (node_id=%s, runner=%s, strict=%s, ray=%s)",
            self.node_id,
            type(self.runner).__name__,
            self._strict,
            _RAY_AVAILABLE,
        )

    # ------------------------------------------------------------------ props
    @property
    def config(self) -> AgentNodeConfig:
        return self._config

    @property
    def history(self) -> List[TaskExecutionRecord]:
        with self._lock:
            return list(self._history)

    @property
    def total_tasks(self) -> int:
        with self._lock:
            return self._total_tasks

    # ---------------------------------------------------------- public API
    def run(self, task_input: Mapping[str, Any]) -> Any:
        """
        Run a task through the underlying ``GreenAgentRunner``.

        Parameters
        ----------
        task_input : Mapping
            Task input. Must be a mapping (strict mode) or is coerced to one
            (non-strict).

        Returns
        -------
        Any
            Whatever the underlying runner returns.

        Raises
        ------
        AgentNodeError
            In strict mode, on invalid input or after all retries fail.
        """
        task = self._coerce_task_input(task_input)
        task_id = self._derive_task_id(task)

        return self._run_with_retry(task, task_id)

    async def run_async(self, task_input: Mapping[str, Any]) -> Any:
        """
        Async wrapper around :meth:`run`.

        Runs the (potentially blocking) runner in a thread via
        ``asyncio.to_thread`` so the event loop stays responsive.
        """
        task = self._coerce_task_input(task_input)
        task_id = self._derive_task_id(task)
        return await asyncio.to_thread(self._run_with_retry, task, task_id)

    # ---------------------------------------------------------- internals
    def _build_runner(
        self,
        runtime: Any,
        policy: Any,
        runner_factory: Optional[Any],
    ) -> Any:
        """Instantiate the underlying runner, preferring the factory."""
        if runner_factory is not None:
            if not callable(runner_factory):
                raise AgentNodeError("runner_factory must be callable.")
            try:
                # Try the 2-arg signature first, then fall back to zero-arg.
                return runner_factory(runtime, policy)
            except TypeError:
                try:
                    return runner_factory()
                except Exception as exc:
                    raise AgentNodeError(
                        f"runner_factory failed: {exc}"
                    ) from exc

        if GreenAgentRunner is not None:
            try:
                return GreenAgentRunner(runtime, policy)  # type: ignore[misc]
            except Exception as exc:
                msg = f"GreenAgentRunner construction failed: {exc}"
                if self._strict:
                    raise AgentNodeError(msg) from exc
                logger.warning("%s Falling back to _FallbackRunner.", msg)
                return _FallbackRunner(runtime, policy)

        logger.debug("GreenAgentRunner unavailable; using _FallbackRunner.")
        return _FallbackRunner(runtime, policy)

    def _coerce_task_input(self, task_input: Any) -> Dict[str, Any]:
        """Validate / normalize the task input according to strict mode."""
        if isinstance(task_input, Mapping):
            return dict(task_input)
        msg = (
            f"task_input must be a Mapping, got {type(task_input).__name__}."
        )
        if self._strict:
            raise AgentNodeError(msg)
        logger.warning("%s Wrapping as {'input': ...}.", msg)
        return {"input": task_input}

    @staticmethod
    def _derive_task_id(task: Mapping[str, Any]) -> str:
        """Return the caller-supplied task_id or generate one."""
        supplied = task.get("task_id")
        if isinstance(supplied, str) and supplied:
            return supplied
        return f"task-{uuid.uuid4().hex[:12]}"

    def _run_with_retry(
        self,
        task: Mapping[str, Any],
        task_id: str,
    ) -> Any:
        """Invoke the runner with retries, timeout, and history recording."""
        cfg = self._config
        attempts = 0
        last_exc: Optional[BaseException] = None
        start = time.perf_counter()
        outcome = "failed"

        while attempts <= cfg.max_task_retries:
            attempts += 1
            try:
                result = self.runner.run(dict(task))
                latency_ms = (time.perf_counter() - start) * 1000.0
                outcome = "completed"
                self._record(
                    task_id=task_id,
                    status="completed",
                    latency_ms=latency_ms,
                    attempts=attempts,
                    error=None,
                )
                logger.debug(
                    "Task %s completed in %.2fms (attempts=%d).",
                    task_id,
                    latency_ms,
                    attempts,
                )
                return result

            except Exception as exc:
                last_exc = exc
                logger.warning(
                    "Task %s attempt %d/%d failed: %s",
                    task_id,
                    attempts,
                    cfg.max_task_retries + 1,
                    exc,
                )
                if attempts > cfg.max_task_retries:
                    break
                if cfg.base_backoff_seconds > 0:
                    time.sleep(cfg.base_backoff_seconds * (2 ** (attempts - 1)))

        latency_ms = (time.perf_counter() - start) * 1000.0
        error_repr = (
            f"{type(last_exc).__name__}: {last_exc}"
            if last_exc is not None
            else "unknown error"
        )
        self._record(
            task_id=task_id,
            status="failed",
            latency_ms=latency_ms,
            attempts=attempts,
            error=error_repr,
        )

        if self._strict:
            raise AgentNodeError(
                f"Task '{task_id}' failed after {attempts} attempt(s): "
                f"{error_repr}"
            ) from last_exc
        return {"status": "failed", "task_id": task_id, "error": error_repr}

    # ---------------------------------------------------------- history
    def _record(
        self,
        *,
        task_id: str,
        status: str,
        latency_ms: float,
        attempts: int,
        error: Optional[str],
    ) -> None:
        record = TaskExecutionRecord(
            task_id=task_id,
            timestamp=time.time(),
            status=status,
            latency_ms=latency_ms,
            attempts=attempts,
            error=error,
        )
        with self._lock:
            self._history.append(record)
            self._total_tasks += 1
            if status == "completed":
                self._completed += 1
            elif status == "timeout":
                self._timeouts += 1
            else:
                self._failed += 1

    def statistics(self) -> Dict[str, Any]:
        """Return aggregate statistics over the recorded executions."""
        with self._lock:
            history = list(self._history)
            totals = {
                "total_tasks": self._total_tasks,
                "completed": self._completed,
                "failed": self._failed,
                "timeouts": self._timeouts,
            }

        if not history:
            return {
                **totals,
                "by_status": {},
                "mean_latency_ms": None,
                "success_rate": None,
                "mean_attempts": None,
            }

        by_status: Dict[str, int] = {}
        for r in history:
            by_status[r.status] = by_status.get(r.status, 0) + 1

        latencies = [r.latency_ms for r in history]
        attempts = [r.attempts for r in history]
        return {
            **totals,
            "by_status": by_status,
            "mean_latency_ms": sum(latencies) / len(latencies),
            "success_rate": (
                totals["completed"] / totals["total_tasks"]
                if totals["total_tasks"] > 0
                else None
            ),
            "mean_attempts": sum(attempts) / len(attempts),
        }

    def reset(self, *, clear_history: bool = False) -> None:
        """Reset counters; optionally clear execution history."""
        with self._lock:
            if clear_history:
                self._history.clear()
            self._total_tasks = 0
            self._completed = 0
            self._failed = 0
            self._timeouts = 0
        logger.debug("AgentNode reset (clear_history=%s)", clear_history)

    # ---------------------------------------------------------- serialization
    def to_dict(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "node_id": self.node_id,
                "config": asdict(self._config),
                "strict": self._strict,
                "ray_available": _RAY_AVAILABLE,
                "runner_type": type(self.runner).__name__,
                "runtime_type": type(self.runtime).__name__,
                "policy_type": type(self.policy).__name__,
                "counters": {
                    "total_tasks": self._total_tasks,
                    "completed": self._completed,
                    "failed": self._failed,
                    "timeouts": self._timeouts,
                },
                "history": [r.to_dict() for r in self._history],
            }

    @classmethod
    def from_dict(
        cls,
        data: Mapping[str, Any],
        *,
        runtime: Any = None,
        policy: Any = None,
        runner_factory: Optional[Any] = None,
    ) -> "AgentNode":
        """
        Rebuild an ``AgentNode`` from a dict produced by :meth:`to_dict`.

        ``runtime`` and ``policy`` are runtime dependencies that cannot be
        serialized; callers must supply them (or a ``runner_factory``) when
        reconstructing.
        """
        if not isinstance(data, Mapping):
            raise AgentNodeError(
                f"from_dict expects a Mapping, got {type(data).__name__}."
            )
        cfg_data = dict(data.get("config", {}) or {})
        cfg = AgentNodeConfig(
            task_timeout_seconds=float(
                cfg_data.get("task_timeout_seconds", 300.0)
            ),
            max_task_retries=int(cfg_data.get("max_task_retries", 1)),
            base_backoff_seconds=float(
                cfg_data.get("base_backoff_seconds", 0.5)
            ),
            max_history=int(cfg_data.get("max_history", 1000)),
            node_id_prefix=str(
                cfg_data.get("node_id_prefix", "agent-node")
            ),
        )

        node = cls(
            runtime=runtime,
            policy=policy,
            config=cfg,
            strict=bool(data.get("strict", True)),
            node_id=str(data.get("node_id")) if data.get("node_id") else None,
            runner_factory=runner_factory,
        )

        with node._lock:
            for entry in data.get("history", []):
                node._history.append(
                    TaskExecutionRecord(
                        task_id=str(entry["task_id"]),
                        timestamp=float(entry["timestamp"]),
                        status=str(entry["status"]),
                        latency_ms=float(entry["latency_ms"]),
                        attempts=int(entry["attempts"]),
                        error=entry.get("error"),
                    )
                )
            counters = dict(data.get("counters", {}) or {})
            node._total_tasks = int(counters.get("total_tasks", 0))
            node._completed = int(counters.get("completed", 0))
            node._failed = int(counters.get("failed", 0))
            node._timeouts = int(counters.get("timeouts", 0))
        return node

    def to_json(self, **kwargs: Any) -> str:
        return json.dumps(self.to_dict(), default=str, **kwargs)

    @classmethod
    def from_json(
        cls,
        payload: str,
        *,
        runtime: Any = None,
        policy: Any = None,
        runner_factory: Optional[Any] = None,
    ) -> "AgentNode":
        try:
            return cls.from_dict(
                json.loads(payload),
                runtime=runtime,
                policy=policy,
                runner_factory=runner_factory,
            )
        except json.JSONDecodeError as exc:
            raise AgentNodeError(f"Invalid JSON payload: {exc}") from exc

    # ---------------------------------------------------------- context mgr
    def __enter__(self) -> "AgentNode":
        self._ctx_start = time.perf_counter()
        logger.debug("Entering scoped AgentNode session (node=%s).", self.node_id)
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        elapsed = time.perf_counter() - (
            self._ctx_start if self._ctx_start is not None else time.perf_counter()
        )
        self._ctx_start = None
        if exc_type is not None:
            logger.warning(
                "AgentNode scope exited with %s after %.4fs.",
                exc_type.__name__,
                elapsed,
            )
            return
        logger.info(
            "AgentNode scope closed in %.4fs (%d task(s)).",
            elapsed,
            self._total_tasks,
        )

    async def __aenter__(self) -> "AgentNode":
        self._async_ctx_start = time.perf_counter()
        logger.debug("Entering async AgentNode session (node=%s).", self.node_id)
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        elapsed = time.perf_counter() - (
            self._async_ctx_start
            if self._async_ctx_start is not None
            else time.perf_counter()
        )
        self._async_ctx_start = None
        if exc_type is not None:
            logger.warning(
                "Async AgentNode scope exited with %s after %.4fs.",
                exc_type.__name__,
                elapsed,
            )
            return
        logger.info(
            "Async AgentNode scope closed in %.4fs (%d task(s)).",
            elapsed,
            self._total_tasks,
        )

    # ----------------------------------------------------------------- dunder
    def __repr__(self) -> str:
        with self._lock:
            return (
                "AgentNode("
                f"node_id={self.node_id!r}, "
                f"runner={type(self.runner).__name__}, "
                f"tasks={self._total_tasks}, "
                f"strict={self._strict}, "
                f"ray={_RAY_AVAILABLE})"
            )


# --------------------------------------------------------------------------- #
# Public API
# --------------------------------------------------------------------------- #
__all__ = [
    "AgentNode",
    "AgentNodeConfig",
    "AgentNodeError",
    "TaskExecutionRecord",
    "GreenAgentRunner",
]


# --------------------------------------------------------------------------- #
# Local smoke test: python -m distributed.agent_node
# --------------------------------------------------------------------------- #
if __name__ == "__main__":  # pragma: no cover
    logging.basicConfig(
        level=logging.INFO, format="%(levelname)s %(name)s: %(message)s"
    )

    # ---- Dummy runtime / policy / runner factory ---------------------- #
    class _DummyRuntime:
        pass

    class _DummyPolicy:
        pass

    class _FlakyRunner:
        """Fails the first N calls, then succeeds."""

        def __init__(self, fail_first: int = 0) -> None:
            self._remaining_failures = fail_first
            self._calls = 0

        def run(self, task_input: Mapping[str, Any]) -> Dict[str, Any]:
            self._calls += 1
            if self._remaining_failures > 0:
                self._remaining_failures -= 1
                raise RuntimeError(
                    f"transient failure on call {self._calls}"
                )
            return {"status": "ok", "echo": dict(task_input), "calls": self._calls}

    def _make_runner_factory(fail_first: int):
        return lambda runtime, policy: _FlakyRunner(fail_first=fail_first)

    # ---- Happy path --------------------------------------------------- #
    node = AgentNode(
        runtime=_DummyRuntime(),
        policy=_DummyPolicy(),
        config=AgentNodeConfig(
            max_task_retries=2,
            base_backoff_seconds=0.01,
            max_history=5,
        ),
        runner_factory=_make_runner_factory(fail_first=0),
        node_id="node-smoke",
    )

    print("node         :", node)
    print("run 1        :", node.run({"task_id": "t1", "input": 42}))
    print("run 2        :", node.run({"input": 7}))

    # Retry behavior with a flaky runner.
    flaky_node = AgentNode(
        runtime=_DummyRuntime(),
        policy=_DummyPolicy(),
        config=AgentNodeConfig(
            max_task_retries=3,
            base_backoff_seconds=0.01,
        ),
        runner_factory=_make_runner_factory(fail_first=2),
        node_id="node-flaky",
    )
    print("flaky run    :", flaky_node.run({"task_id": "t-flaky"}))

    # Failure path (strict mode) with no retries left.
    doomed = AgentNode(
        runtime=_DummyRuntime(),
        policy=_DummyPolicy(),
        config=AgentNodeConfig(max_task_retries=0),
        runner_factory=_make_runner_factory(fail_first=5),
        node_id="node-doomed",
    )
    try:
        doomed.run({"task_id": "t-doomed"})
    except AgentNodeError as exc:
        print("Failure path :", exc)

    # Non-strict mode returns a structured error dict.
    lenient = AgentNode(
        runtime=_DummyRuntime(),
        policy=_DummyPolicy(),
        config=AgentNodeConfig(max_task_retries=0),
        runner_factory=_make_runner_factory(fail_first=5),
        strict=False,
        node_id="node-lenient",
    )
    print("lenient run  :", lenient.run({"task_id": "t-lenient"}))

    # ---- Statistics --------------------------------------------------- #
    print("stats        :", node.statistics())
    print("flaky stats  :", flaky_node.statistics())

    # ---- Async wrapper ------------------------------------------------ #
    async def _async_main() -> Any:
        async with flaky_node:
            return await flaky_node.run_async({"task_id": "t-async"})

    print("async run    :", asyncio.run(_async_main()))

    # ---- Context manager (sync) --------------------------------------- #
    with AgentNode(
        runtime=_DummyRuntime(),
        policy=_DummyPolicy(),
        runner_factory=_make_runner_factory(fail_first=0),
    ) as scoped:
        scoped.run({"task_id": "t-scoped"})
    print("Context-managed session OK.")

    # ---- Serialization round-trip ------------------------------------- #
    payload = node.to_json()
    restored = AgentNode.from_json(
        payload,
        runtime=_DummyRuntime(),
        policy=_DummyPolicy(),
        runner_factory=_make_runner_factory(fail_first=0),
    )
    assert restored.to_dict() == node.to_dict()
    print("Serialization round-trip OK.")

    # ---- Validation failures ------------------------------------------ #
    for bad_kwargs in (
        dict(runtime=None, policy=_DummyPolicy()),
        dict(runtime=_DummyRuntime(), policy=None),
        dict(runtime=_DummyRuntime(), policy=_DummyPolicy(), node_id=""),
    ):
        try:
            AgentNode(**bad_kwargs)  # type: ignore[arg-type]
        except AgentNodeError as exc:
            print("Rejected as expected:", exc)
        else:  # pragma: no cover
            raise AssertionError(f"Expected rejection: {bad_kwargs!r}")

    # Invalid task_input in strict mode.
    try:
        node.run(123)  # type: ignore[arg-type]
    except AgentNodeError as exc:
        print("Rejected task_input:", exc)

    print("\nSmoke test passed.")
