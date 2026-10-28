# src/distributed/agent_node.py

"""
Agent Node — v2.0.0
===================

Wraps ``GreenAgentRunner`` into a distributed-ready node.

The node is deployable as a Ray actor via ``RemoteAgentNode`` when Ray is
installed; ``AgentNode`` itself is always a plain class so it can be
instantiated directly in tests, CI, and single-process deployments.

Original behaviour preserved
----------------------------
- ``AgentNode(runtime, policy)`` — constructs an internal ``GreenAgentRunner``.
- ``node.run(task_input)`` — runs the task through the underlying runner.

v2.0.0 changes vs. v1.0.0
-------------------------
**P0 bug fixes**
  * 2.1 ``task_timeout_seconds`` is now *enforced* (previously validated but
        never read). Each attempt is bounded and the whole task respects a
        hard deadline.
  * 2.2 ``"timeout"`` status and the ``_timeouts`` counter are now reachable
        via the ``ExecutionStatus`` enum, which is what actually gets written.
  * 2.3 ``run_async`` enforces the timeout and cooperates with cancellation
        via ``asyncio.wait_for``.
  * 2.4 ``AgentNode`` is never wrapped in ``@ray.remote``; use
        ``RemoteAgentNode`` in Ray environments. The file's own smoke test
        works in both Ray-installed and Ray-absent environments.
  * 2.6 ``from_dict`` delegates to ``AgentNodeConfig.from_legacy_dict`` so
        new config fields default correctly instead of being silently lost.

**P1 robustness / observability**
  * 2.5 Warn if ``runner_factory`` captures free variables (Ray pickling).
  * 2.7 ``statistics(window=N)`` for a consistent windowed view; the all-time
        view still uses the counters so both ``success_rate`` and
        ``mean_latency_ms`` refer to the same population.
  * 2.8 The async path uses ``asyncio.sleep`` for backoff (no thread burns
        during retry waits).
  * 3.1 ``runner_factory`` arity is detected via ``inspect.signature``, not
        ``TypeError`` catching.
  * 3.2 The runner is validated to expose a callable ``.run`` at construction.
  * 3.3 ``ExecutionStatus`` enum for a stable machine contract.
  * 3.4 ``metrics_hook(event, fields)`` for Prometheus / OTel.
  * 3.5 Backoff jitter + ``max_backoff_seconds`` ceiling.
  * 3.7 ``__getstate__`` / ``__setstate__`` drop the ``RLock`` so the node is
        picklable (Ray actor handles are picklable).

**P2 enhancements**
  * 3.6 Opt-in idempotent dedup cache for caller-supplied ``task_id``.
  * 3.8 ``trace_id`` on ``TaskExecutionRecord`` for distributed tracing.
  * 3.10 ``close()`` hook to release runner resources.
  * 3.11 Log a runner-type mismatch after restore.
  * 3.12 48-bit ``node_id`` suffix (was 32-bit).
  * 3.13 Validate ``node_id_prefix`` against a safe character set.
  * 2.9 ``__enter__``/``__exit__`` are locked; ``reset()`` clears context state.

Thread-safe, serializable, bounded in memory, and picklable.
"""

from __future__ import annotations

import asyncio
import concurrent.futures
import inspect
import json
import logging
import math
import random
import re
import threading
import time
import uuid
from collections import OrderedDict, deque
from dataclasses import asdict, dataclass, field, fields
from datetime import datetime, timezone
from enum import Enum
from typing import (
    Any, Callable, Deque, Dict, List, Mapping, Optional, Tuple,
)

logger = logging.getLogger(__name__)

__version__ = "2.0.0"


# --------------------------------------------------------------------------- #
# Optional Ray integration (P0 2.4: two names instead of decorator)
# --------------------------------------------------------------------------- #
try:  # pragma: no cover — environment-dependent
    import ray  # type: ignore

    _RAY_AVAILABLE = True
except ImportError:  # pragma: no cover
    ray = None  # type: ignore[assignment]
    _RAY_AVAILABLE = False


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
# Status enum (P1 3.3)
# --------------------------------------------------------------------------- #
class ExecutionStatus(str, Enum):
    """Terminal status of one ``run`` / ``run_async`` invocation."""

    COMPLETED = "completed"
    FAILED = "failed"
    TIMEOUT = "timeout"


# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #
_NODE_ID_PREFIX_RE = re.compile(r"^[a-zA-Z0-9][a-zA-Z0-9._-]*$")


@dataclass(frozen=True)
class AgentNodeConfig:
    """
    Tunable parameters for the agent node.

    Centralizes timeouts, retries, backoff strategy, and history limits so
    deployments can calibrate without editing the class.
    """

    # Per-task execution policy
    task_timeout_seconds: float = 300.0
    max_task_retries: int = 1
    base_backoff_seconds: float = 0.5
    max_backoff_seconds: float = 30.0
    backoff_jitter: bool = True

    # Bounded history ring-buffer
    max_history: int = 1000

    # Node identity
    node_id_prefix: str = "agent-node"

    # Idempotency (P2 3.6)
    dedup_completed: bool = False
    dedup_cache_size: int = 256

    def __post_init__(self) -> None:
        if self.task_timeout_seconds <= 0:
            raise AgentNodeError("task_timeout_seconds must be > 0.")
        if self.max_task_retries < 0:
            raise AgentNodeError("max_task_retries must be >= 0.")
        if self.base_backoff_seconds < 0:
            raise AgentNodeError("base_backoff_seconds must be >= 0.")
        if self.max_backoff_seconds < 0:
            raise AgentNodeError("max_backoff_seconds must be >= 0.")
        if self.max_history <= 0:
            raise AgentNodeError("max_history must be > 0.")
        if self.dedup_cache_size <= 0:
            raise AgentNodeError("dedup_cache_size must be > 0.")
        if not isinstance(self.node_id_prefix, str) or not self.node_id_prefix:
            raise AgentNodeError("node_id_prefix must be a non-empty string.")
        if not _NODE_ID_PREFIX_RE.match(self.node_id_prefix):
            # P2 3.13: warn, don't reject — many orchestrators are tolerant
            logger.warning(
                "node_id_prefix %r contains characters outside "
                "[a-zA-Z0-9._-]; some orchestrators may reject node ids.",
                self.node_id_prefix,
            )

    # ---------------------------------------------------- convenience
    @classmethod
    def from_legacy_dict(
        cls, cfg: Optional[Mapping[str, Any]]
    ) -> "AgentNodeConfig":
        """
        Build an ``AgentNodeConfig`` from a flat config mapping (P0 2.6).

        Only keys matching a dataclass field are consumed, so new fields added
        to ``AgentNodeConfig`` automatically use their declared defaults
        instead of the caller silently getting the old default.
        """
        cfg = dict(cfg or {})
        valid = {f.name for f in fields(cls)}
        return cls(**{k: v for k, v in cfg.items() if k in valid})


# --------------------------------------------------------------------------- #
# Execution record
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class TaskExecutionRecord:
    """Immutable snapshot of one ``run`` / ``run_async`` invocation."""

    task_id: str
    timestamp: float
    status: str  # ExecutionStatus.value
    latency_ms: float
    attempts: int
    error: Optional[str] = None
    trace_id: Optional[str] = None  # P2 3.8

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
class AgentNode:
    """
    Represents a distributed agent instance.

    Thread-safe, serializable, picklable, and bounded in memory. All original
    behavior is preserved; new parameters are keyword-only with backward-
    compatible defaults.
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
        runner_factory: Optional[Callable[..., Any]] = None,
        metrics_hook: Optional[Callable[[str, Mapping[str, Any]], None]] = None,
    ) -> None:
        self._config: AgentNodeConfig = config or AgentNodeConfig()
        self._strict: bool = bool(strict)

        if metrics_hook is not None and not callable(metrics_hook):
            raise AgentNodeError("metrics_hook must be callable.")
        self._metrics_hook = metrics_hook

        if runtime is None and self._strict:
            raise AgentNodeError("runtime must not be None.")
        if policy is None and self._strict:
            raise AgentNodeError("policy must not be None.")

        if node_id is None:
            # P2 3.12: 48-bit suffix (was 32-bit)
            node_id = (
                f"{self._config.node_id_prefix}-"
                f"{uuid.uuid4().hex[:12]}"
            )
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
        self._completed_cache: "OrderedDict[str, Any]" = OrderedDict()
        self._total_tasks: int = 0
        self._completed: int = 0
        self._failed: int = 0
        self._timeouts: int = 0
        self._ctx_start: Optional[float] = None
        self._async_ctx_start: Optional[float] = None

        logger.debug(
            "AgentNode v%s initialized "
            "(node_id=%s, runner=%s, strict=%s, ray=%s)",
            __version__,
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
    def run(
        self,
        task_input: Mapping[str, Any],
        *,
        trace_id: Optional[str] = None,
    ) -> Any:
        """
        Run a task through the underlying ``GreenAgentRunner``.

        Parameters
        ----------
        task_input : Mapping
            Task input. Must be a mapping (strict mode) or is coerced to one
            (non-strict).
        trace_id : str, optional
            Correlation ID propagated into the ``TaskExecutionRecord``.

        Returns
        -------
        Any
            The runner's result on success. On failure, in non-strict mode,
            returns ``{"status": "failed"|"timeout", "task_id": ..., "error": ...}``.

        Raises
        ------
        AgentNodeError
            In strict mode, on invalid input or after all retries fail.
        """
        task = self._coerce_task_input(task_input)
        task_id = self._derive_task_id(task)

        # P2 3.6: idempotent cache
        if self._config.dedup_completed:
            hit, cached = self._lookup_cache(task_id)
            if hit:
                logger.debug("Cache hit for task %s", task_id)
                return cached

        return self._run_sync(task, task_id, trace_id)

    async def run_async(
        self,
        task_input: Mapping[str, Any],
        *,
        trace_id: Optional[str] = None,
    ) -> Any:
        """
        Async wrapper around :meth:`run` (P0 2.3).

        The runner is executed in a worker thread via ``asyncio.to_thread``,
        wrapped in ``asyncio.wait_for`` so the caller's event loop stays
        responsive and cancellation propagates. The worker thread itself
        cannot be forcibly cancelled — if the runner ignores its deadline it
        will finish in the background.
        """
        task = self._coerce_task_input(task_input)
        task_id = self._derive_task_id(task)

        if self._config.dedup_completed:
            hit, cached = self._lookup_cache(task_id)
            if hit:
                logger.debug("Cache hit for task %s", task_id)
                return cached

        return await self._run_async_impl(task, task_id, trace_id)

    # ---------------------------------------------------------- internals
    def _build_runner(
        self,
        runtime: Any,
        policy: Any,
        runner_factory: Optional[Callable[..., Any]],
    ) -> Any:
        """Instantiate the underlying runner, preferring the factory."""
        if runner_factory is not None:
            if not callable(runner_factory):
                raise AgentNodeError("runner_factory must be callable.")
            # P1 3.1: arity detection via inspect, not TypeError catching.
            try:
                n_params = len(inspect.signature(runner_factory).parameters)
            except (TypeError, ValueError):
                n_params = 2  # assume (runtime, policy)

            # P1 2.5: warn about Ray pickling risk
            if inspect.isfunction(runner_factory) and \
                    runner_factory.__code__.co_freevars:
                logger.warning(
                    "runner_factory captures free variables "
                    "(%s); this factory may not be picklable under Ray.",
                    runner_factory.__code__.co_freevars,
                )

            try:
                if n_params == 0:
                    runner = runner_factory()
                else:
                    runner = runner_factory(runtime, policy)
            except Exception as exc:  # noqa: BLE001
                raise AgentNodeError(
                    f"runner_factory failed: {exc}") from exc

            # P1 3.2: validate the runner contract
            if not callable(getattr(runner, "run", None)):
                raise AgentNodeError(
                    f"runner must expose a callable .run; "
                    f"got {type(runner).__name__}")
            return runner

        if GreenAgentRunner is not None:
            try:
                runner = GreenAgentRunner(runtime, policy)  # type: ignore[misc]
            except Exception as exc:  # noqa: BLE001
                msg = f"GreenAgentRunner construction failed: {exc}"
                if self._strict:
                    raise AgentNodeError(msg) from exc
                logger.warning("%s Falling back to _FallbackRunner.", msg)
                return _FallbackRunner(runtime, policy)
            if not callable(getattr(runner, "run", None)):
                raise AgentNodeError(
                    f"GreenAgentRunner must expose a callable .run; "
                    f"got {type(runner).__name__}")
            return runner

        logger.debug("GreenAgentRunner unavailable; using _FallbackRunner.")
        return _FallbackRunner(runtime, policy)

    def _coerce_task_input(self, task_input: Any) -> Dict[str, Any]:
        """Validate / normalize the task input according to strict mode."""
        if isinstance(task_input, Mapping):
            return dict(task_input)
        msg = (
            f"task_input must be a Mapping, "
            f"got {type(task_input).__name__}."
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

    def _compute_backoff(self, attempts: int) -> float:
        """Exponential backoff with optional jitter and a hard ceiling."""
        delay = self._config.base_backoff_seconds * (2 ** max(0, attempts - 1))
        delay = min(delay, self._config.max_backoff_seconds)
        if self._config.backoff_jitter and delay > 0:
            delay = random.uniform(0, delay)
        return delay

    @staticmethod
    def _format_error(exc: Optional[BaseException]) -> str:
        if exc is None:
            return "unknown error"
        return f"{type(exc).__name__}: {exc}"

    # ---------------------------------------------------------- sync retry
    def _run_sync(
        self,
        task: Mapping[str, Any],
        task_id: str,
        trace_id: Optional[str],
    ) -> Any:
        cfg = self._config
        attempts = 0
        last_exc: Optional[BaseException] = None
        start = time.perf_counter()
        deadline = time.monotonic() + cfg.task_timeout_seconds
        status = ExecutionStatus.FAILED.value

        while True:
            # Hard deadline check (P0 2.1)
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                status = ExecutionStatus.TIMEOUT.value
                if last_exc is None:
                    last_exc = TimeoutError(
                        f"task budget {cfg.task_timeout_seconds}s exhausted")
                break

            if attempts > cfg.max_task_retries:
                break  # status stays FAILED

            attempts += 1
            try:
                result = self._execute_once_sync(task, remaining)
                latency_ms = (time.perf_counter() - start) * 1000.0
                self._record(
                    task_id=task_id,
                    status=ExecutionStatus.COMPLETED.value,
                    latency_ms=latency_ms,
                    attempts=attempts,
                    error=None,
                    trace_id=trace_id,
                )
                self._maybe_cache(task_id, result)
                logger.debug(
                    "Task %s completed in %.2fms (attempts=%d).",
                    task_id, latency_ms, attempts)
                return result
            except concurrent.futures.TimeoutError as exc:
                last_exc = exc
                status = ExecutionStatus.TIMEOUT.value
                logger.warning(
                    "Task %s attempt %d/%d exceeded %.2fs budget.",
                    task_id, attempts, cfg.max_task_retries + 1, remaining)
                break
            except Exception as exc:  # noqa: BLE001
                last_exc = exc
                logger.warning(
                    "Task %s attempt %d/%d failed: %s",
                    task_id, attempts, cfg.max_task_retries + 1, exc)
                if attempts > cfg.max_task_retries:
                    break
                delay = self._compute_backoff(attempts)
                if delay > 0:
                    time.sleep(delay)

        latency_ms = (time.perf_counter() - start) * 1000.0
        error_repr = self._format_error(last_exc)
        self._record(
            task_id=task_id,
            status=status,
            latency_ms=latency_ms,
            attempts=attempts,
            error=error_repr,
            trace_id=trace_id,
        )

        if self._strict:
            raise AgentNodeError(
                f"Task '{task_id}' {status} after {attempts} attempt(s): "
                f"{error_repr}"
            ) from last_exc
        return {"status": status, "task_id": task_id, "error": error_repr}

    def _execute_once_sync(
        self, task: Mapping[str, Any], timeout_seconds: float,
    ) -> Any:
        """
        Execute one attempt with a hard timeout (P0 2.1).

        A daemon worker thread runs the runner; we block on an event for at
        most ``timeout_seconds``. If the runner hangs, the thread is orphaned
        but is daemonic, so it will not delay interpreter shutdown.
        """
        result_box: List[Any] = [None]
        exception_box: List[Optional[BaseException]] = [None]
        done = threading.Event()

        def _target() -> None:
            try:
                result_box[0] = self.runner.run(dict(task))
            except BaseException as exc:  # noqa: BLE001
                exception_box[0] = exc
            finally:
                done.set()

        thread = threading.Thread(
            target=_target, daemon=True,
            name=f"agent-node-{self.node_id}",
        )
        thread.start()

        if not done.wait(timeout=timeout_seconds):
            raise concurrent.futures.TimeoutError(
                f"runner exceeded {timeout_seconds:.3f}s budget")
        if exception_box[0] is not None:
            raise exception_box[0]
        return result_box[0]

    # --------------------------------------------------------- async retry
    async def _run_async_impl(
        self,
        task: Mapping[str, Any],
        task_id: str,
        trace_id: Optional[str],
    ) -> Any:
        cfg = self._config
        attempts = 0
        last_exc: Optional[BaseException] = None
        start = time.perf_counter()
        deadline = time.monotonic() + cfg.task_timeout_seconds
        status = ExecutionStatus.FAILED.value

        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                status = ExecutionStatus.TIMEOUT.value
                if last_exc is None:
                    last_exc = TimeoutError(
                        f"task budget {cfg.task_timeout_seconds}s exhausted")
                break

            if attempts > cfg.max_task_retries:
                break

            attempts += 1
            try:
                # P0 2.3: hard timeout + cancellation propagation
                result = await asyncio.wait_for(
                    asyncio.to_thread(self.runner.run, dict(task)),
                    timeout=remaining,
                )
                latency_ms = (time.perf_counter() - start) * 1000.0
                self._record(
                    task_id=task_id,
                    status=ExecutionStatus.COMPLETED.value,
                    latency_ms=latency_ms,
                    attempts=attempts,
                    error=None,
                    trace_id=trace_id,
                )
                self._maybe_cache(task_id, result)
                logger.debug(
                    "Task %s completed in %.2fms (attempts=%d) [async].",
                    task_id, latency_ms, attempts)
                return result
            except asyncio.TimeoutError as exc:
                last_exc = exc
                status = ExecutionStatus.TIMEOUT.value
                logger.warning(
                    "Task %s attempt %d/%d exceeded %.2fs budget [async].",
                    task_id, attempts, cfg.max_task_retries + 1, remaining)
                break
            except Exception as exc:  # noqa: BLE001
                last_exc = exc
                logger.warning(
                    "Task %s attempt %d/%d failed [async]: %s",
                    task_id, attempts, cfg.max_task_retries + 1, exc)
                if attempts > cfg.max_task_retries:
                    break
                delay = self._compute_backoff(attempts)
                if delay > 0:
                    # P1 2.8: async backoff — does not block a worker thread
                    await asyncio.sleep(delay)

        latency_ms = (time.perf_counter() - start) * 1000.0
        error_repr = self._format_error(last_exc)
        self._record(
            task_id=task_id,
            status=status,
            latency_ms=latency_ms,
            attempts=attempts,
            error=error_repr,
            trace_id=trace_id,
        )

        if self._strict:
            raise AgentNodeError(
                f"Task '{task_id}' {status} after {attempts} attempt(s): "
                f"{error_repr}"
            ) from last_exc
        return {"status": status, "task_id": task_id, "error": error_repr}

    # ---------------------------------------------------------- dedup cache
    def _lookup_cache(self, task_id: str) -> Tuple[bool, Any]:
        """Return ``(hit, value)``; moves the entry to the LRU tail on hit."""
        with self._lock:
            if task_id in self._completed_cache:
                value = self._completed_cache.pop(task_id)
                self._completed_cache[task_id] = value
                return True, value
        return False, None

    def _maybe_cache(self, task_id: str, result: Any) -> None:
        if not self._config.dedup_completed:
            return
        with self._lock:
            self._completed_cache[task_id] = result
            while len(self._completed_cache) > self._config.dedup_cache_size:
                self._completed_cache.popitem(last=False)

    # ---------------------------------------------------------- history
    def _record(
        self,
        *,
        task_id: str,
        status: str,
        latency_ms: float,
        attempts: int,
        error: Optional[str],
        trace_id: Optional[str] = None,
    ) -> None:
        record = TaskExecutionRecord(
            task_id=task_id,
            timestamp=time.time(),
            status=status,
            latency_ms=latency_ms,
            attempts=attempts,
            error=error,
            trace_id=trace_id,
        )
        with self._lock:
            self._history.append(record)
            self._total_tasks += 1
            if status == ExecutionStatus.COMPLETED.value:
                self._completed += 1
            elif status == ExecutionStatus.TIMEOUT.value:
                self._timeouts += 1
            else:
                self._failed += 1

        # P1 3.4: metrics hook (outside lock, best-effort)
        self._emit_metric("task_execution", {
            "status": status,
            "attempts": attempts,
            "node_id": self.node_id,
            "runner": type(self.runner).__name__,
            "trace_id": trace_id or "",
        })

    def _emit_metric(
        self, event: str, fields: Mapping[str, Any],
    ) -> None:
        if self._metrics_hook is None:
            return
        try:
            self._metrics_hook(event, dict(fields))
        except Exception:  # noqa: BLE001
            logger.debug("metrics_hook raised; ignoring", exc_info=True)

    def statistics(self, window: Optional[int] = None) -> Dict[str, Any]:
        """
        Return aggregate statistics over the recorded executions.

        ``window`` (P1 2.7): when provided, restrict *both* the counters and
        the derived metrics (mean latency, mean attempts) to the last
        ``window`` decisions, so the numbers are internally consistent.
        When ``None``, counters are all-time and means are over the bounded
        history — the two windows are the same only when the history has
        never rolled over.
        """
        with self._lock:
            if window is None:
                history = list(self._history)
                totals = {
                    "total_tasks": self._total_tasks,
                    "completed": self._completed,
                    "failed": self._failed,
                    "timeouts": self._timeouts,
                }
            else:
                history = list(self._history)[-window:]
                totals = {
                    "total_tasks": len(history),
                    "completed": sum(
                        1 for r in history
                        if r.status == ExecutionStatus.COMPLETED.value),
                    "failed": sum(
                        1 for r in history
                        if r.status == ExecutionStatus.FAILED.value),
                    "timeouts": sum(
                        1 for r in history
                        if r.status == ExecutionStatus.TIMEOUT.value),
                }

        if not history:
            return {
                **totals,
                "by_status": {},
                "mean_latency_ms": None,
                "success_rate": (
                    totals["completed"] / totals["total_tasks"]
                    if totals["total_tasks"] > 0 else None
                ),
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
                if totals["total_tasks"] > 0 else None
            ),
            "mean_attempts": sum(attempts) / len(attempts),
        }

    def reset(
        self,
        *,
        clear_history: bool = False,
        clear_cache: bool = False,
    ) -> None:
        """Reset counters; optionally clear history and/or the dedup cache."""
        with self._lock:
            if clear_history:
                self._history.clear()
            if clear_cache:
                self._completed_cache.clear()
            self._total_tasks = 0
            self._completed = 0
            self._failed = 0
            self._timeouts = 0
            self._ctx_start = None
            self._async_ctx_start = None
        logger.debug(
            "AgentNode reset (clear_history=%s, clear_cache=%s)",
            clear_history, clear_cache,
        )

    def close(self) -> None:
        """Release resources held by the underlying runner (P2 3.10)."""
        close_fn = getattr(self.runner, "close", None)
        if callable(close_fn):
            try:
                close_fn()
            except Exception:  # noqa: BLE001
                logger.warning("runner.close() failed", exc_info=True)

    # ---------------------------------------------------------- serialization
    def to_dict(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "version": __version__,
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
        runner_factory: Optional[Callable[..., Any]] = None,
        metrics_hook: Optional[Callable[[str, Mapping[str, Any]], None]] = None,
    ) -> "AgentNode":
        """
        Rebuild an ``AgentNode`` from a dict produced by :meth:`to_dict`.

        ``runtime`` and ``policy`` are runtime dependencies that cannot be
        serialized; callers must supply them (or a ``runner_factory``).
        """
        if not isinstance(data, Mapping):
            raise AgentNodeError(
                f"from_dict expects a Mapping, got {type(data).__name__}."
            )

        # P0 2.6: delegate to from_legacy_dict so new fields default correctly
        cfg = AgentNodeConfig.from_legacy_dict(data.get("config") or {})

        node = cls(
            runtime=runtime,
            policy=policy,
            config=cfg,
            strict=bool(data.get("strict", True)),
            node_id=(
                str(data.get("node_id"))
                if data.get("node_id") else None
            ),
            runner_factory=runner_factory,
            metrics_hook=metrics_hook,
        )

        with node._lock:
            for entry in data.get("history", []):
                node._history.append(TaskExecutionRecord(
                    task_id=str(entry["task_id"]),
                    timestamp=float(entry["timestamp"]),
                    status=str(entry["status"]),
                    latency_ms=float(entry["latency_ms"]),
                    attempts=int(entry["attempts"]),
                    error=entry.get("error"),
                    trace_id=entry.get("trace_id"),
                ))
            counters = dict(data.get("counters", {}) or {})
            node._total_tasks = int(counters.get("total_tasks", 0))
            node._completed = int(counters.get("completed", 0))
            node._failed = int(counters.get("failed", 0))
            node._timeouts = int(counters.get("timeouts", 0))

        # P2 3.11: log runner-type mismatch
        serialized_runner = data.get("runner_type")
        actual_runner = type(node.runner).__name__
        if serialized_runner and serialized_runner != actual_runner:
            logger.info(
                "Runner type differs after restore: %r -> %r",
                serialized_runner, actual_runner,
            )
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
        runner_factory: Optional[Callable[..., Any]] = None,
        metrics_hook: Optional[Callable[[str, Mapping[str, Any]], None]] = None,
    ) -> "AgentNode":
        try:
            return cls.from_dict(
                json.loads(payload),
                runtime=runtime,
                policy=policy,
                runner_factory=runner_factory,
                metrics_hook=metrics_hook,
            )
        except json.JSONDecodeError as exc:
            raise AgentNodeError(
                f"Invalid JSON payload: {exc}") from exc

    # ---------------------------------------------------------- pickling
    def __getstate__(self) -> Dict[str, Any]:
        # P1 3.7: RLock is not picklable; drop it on serialize.
        state = self.__dict__.copy()
        state.pop("_lock", None)
        return state

    def __setstate__(self, state: Dict[str, Any]) -> None:
        self.__dict__.update(state)
        self._lock = threading.RLock()

    # ---------------------------------------------------------- context mgr
    def __enter__(self) -> "AgentNode":
        with self._lock:
            self._ctx_start = time.perf_counter()
        logger.debug(
            "Entering scoped AgentNode session (node=%s).", self.node_id)
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        with self._lock:
            start = (
                self._ctx_start
                if self._ctx_start is not None
                else time.perf_counter()
            )
            elapsed = time.perf_counter() - start
            self._ctx_start = None
            total = self._total_tasks
        if exc_type is not None:
            logger.warning(
                "AgentNode scope exited with %s after %.4fs.",
                exc_type.__name__, elapsed,
            )
            return
        logger.info(
            "AgentNode scope closed in %.4fs (%d task(s)).",
            elapsed, total,
        )

    async def __aenter__(self) -> "AgentNode":
        with self._lock:
            self._async_ctx_start = time.perf_counter()
        logger.debug(
            "Entering async AgentNode session (node=%s).", self.node_id)
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        with self._lock:
            start = (
                self._async_ctx_start
                if self._async_ctx_start is not None
                else time.perf_counter()
            )
            elapsed = time.perf_counter() - start
            self._async_ctx_start = None
            total = self._total_tasks
        if exc_type is not None:
            logger.warning(
                "Async AgentNode scope exited with %s after %.4fs.",
                exc_type.__name__, elapsed,
            )
            return
        logger.info(
            "Async AgentNode scope closed in %.4fs (%d task(s)).",
            elapsed, total,
        )

    # ----------------------------------------------------------------- dunder
    def __repr__(self) -> str:
        with self._lock:
            return (
                "AgentNode("
                f"version={__version__!r}, "
                f"node_id={self.node_id!r}, "
                f"runner={type(self.runner).__name__}, "
                f"tasks={self._total_tasks}, "
                f"strict={self._strict}, "
                f"ray={_RAY_AVAILABLE})"
            )


# --------------------------------------------------------------------------- #
# Ray integration — two names (P0 2.4)
# --------------------------------------------------------------------------- #
if _RAY_AVAILABLE:  # pragma: no cover — environment-dependent
    RemoteAgentNode = ray.remote(AgentNode)  # type: ignore[assignment]
else:
    RemoteAgentNode = AgentNode


# --------------------------------------------------------------------------- #
# Public API
# --------------------------------------------------------------------------- #
__all__ = [
    "AgentNode",
    "RemoteAgentNode",
    "AgentNodeConfig",
    "AgentNodeError",
    "ExecutionStatus",
    "TaskExecutionRecord",
    "GreenAgentRunner",
]


# --------------------------------------------------------------------------- #
# Local smoke test: python -m distributed.agent_node
# --------------------------------------------------------------------------- #
if __name__ == "__main__":  # pragma: no cover
    logging.basicConfig(
        level=logging.INFO,
        format="%(levelname)s %(name)s: %(message)s",
    )

    # ---- Dummy runtime / policy / runners ----------------------------- #
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
            return {
                "status": "ok",
                "echo": dict(task_input),
                "calls": self._calls,
            }

    class _SlowRunner:
        """Sleeps for a fixed duration to exercise timeouts."""

        def __init__(self, sleep_seconds: float) -> None:
            self._sleep = sleep_seconds

        def run(self, task_input: Mapping[str, Any]) -> Dict[str, Any]:
            time.sleep(self._sleep)
            return {"status": "ok", "slept": self._sleep}

    def _flaky_factory(fail_first: int):
        return lambda runtime, policy: _FlakyRunner(fail_first=fail_first)

    def _slow_factory(secs: float):
        return lambda runtime, policy: _SlowRunner(secs)

    # ------------------------------------------------------------ #
    # 1. Happy path
    # ------------------------------------------------------------ #
    node = AgentNode(
        runtime=_DummyRuntime(),
        policy=_DummyPolicy(),
        config=AgentNodeConfig(
            max_task_retries=2,
            base_backoff_seconds=0.01,
            max_history=5,
        ),
        runner_factory=_flaky_factory(fail_first=0),
        node_id="node-smoke",
    )

    print("node        :", node)
    print("run 1       :", node.run({"task_id": "t1", "input": 42}))
    print("run 2       :", node.run({"input": 7}))

    # ------------------------------------------------------------ #
    # 2. Retry behaviour
    # ------------------------------------------------------------ #
    flaky = AgentNode(
        runtime=_DummyRuntime(),
        policy=_DummyPolicy(),
        config=AgentNodeConfig(
            max_task_retries=3,
            base_backoff_seconds=0.01,
            backoff_jitter=False,
        ),
        runner_factory=_flaky_factory(fail_first=2),
        node_id="node-flaky",
    )
    print("flaky run   :", flaky.run({"task_id": "t-flaky"}))

    # ------------------------------------------------------------ #
    # 3. P0 2.1 + 2.2 — real sync timeout
    # ------------------------------------------------------------ #
    print("\n=== P0 2.1 / 2.2: sync timeout is enforced ===")
    slow = AgentNode(
        runtime=_DummyRuntime(),
        policy=_DummyPolicy(),
        config=AgentNodeConfig(
            task_timeout_seconds=0.1,
            max_task_retries=0,
        ),
        runner_factory=_slow_factory(2.0),
        strict=False,
        node_id="node-slow-sync",
    )
    t0 = time.perf_counter()
    result = slow.run({"task_id": "t-slow-sync"})
    elapsed = time.perf_counter() - t0
    assert result["status"] == "timeout", result
    assert elapsed < 1.0, f"timeout not enforced: {elapsed:.2f}s"
    assert slow.statistics()["timeouts"] == 1
    print(f"  ✓ returned in {elapsed:.3f}s with status={result['status']}")
    print(f"  ✓ counters: {slow.statistics()}")

    # ------------------------------------------------------------ #
    # 4. P0 2.3 — async timeout + cancellation
    # ------------------------------------------------------------ #
    print("\n=== P0 2.3: async timeout is enforced ===")

    async def _async_timeout() -> None:
        slow_async = AgentNode(
            runtime=_DummyRuntime(),
            policy=_DummyPolicy(),
            config=AgentNodeConfig(
                task_timeout_seconds=0.1,
                max_task_retries=0,
            ),
            runner_factory=_slow_factory(0.3),
            strict=False,
            node_id="node-slow-async",
        )
        r = await slow_async.run_async({"task_id": "t-slow-async"})
        assert r["status"] == "timeout", r
        assert slow_async.statistics()["timeouts"] == 1
        print(f"  ✓ async timeout status={r['status']}")

    asyncio.run(_async_timeout())

    # ------------------------------------------------------------ #
    # 5. P1 2.8 — async backoff (asyncio.sleep, not time.sleep)
    # ------------------------------------------------------------ #
    print("\n=== P1 2.8: async retry uses asyncio.sleep ===")

    async def _async_backoff() -> None:
        n = AgentNode(
            runtime=_DummyRuntime(),
            policy=_DummyPolicy(),
            config=AgentNodeConfig(
                max_task_retries=2,
                base_backoff_seconds=0.05,
                backoff_jitter=False,
            ),
            runner_factory=_flaky_factory(fail_first=2),
        )
        result = await n.run_async({"task_id": "t-async-flaky"})
        assert result["status"] == "ok", result
        print(f"  ✓ succeeded after async backoff: {result}")

    asyncio.run(_async_backoff())

    # ------------------------------------------------------------ #
    # 6. P1 2.7 — statistics(window=N)
    # ------------------------------------------------------------ #
    print("\n=== P1 2.7: statistics(window=) is consistent ===")
    all_stats = node.statistics()
    win_stats = node.statistics(window=2)
    print(f"  all    : {all_stats['total_tasks']} tasks, "
          f"by_status={all_stats['by_status']}")
    print(f"  window2: {win_stats['total_tasks']} tasks, "
          f"by_status={win_stats['by_status']}")
    assert win_stats["total_tasks"] <= 2

    # ------------------------------------------------------------ #
    # 7. P2 3.6 — dedup cache
    # ------------------------------------------------------------ #
    print("\n=== P2 3.6: dedup cache ===")
    dedup = AgentNode(
        runtime=_DummyRuntime(),
        policy=_DummyPolicy(),
        config=AgentNodeConfig(dedup_completed=True),
        runner_factory=_flaky_factory(fail_first=0),
        node_id="node-dedup",
    )
    r1 = dedup.run({"task_id": "t-dedup", "input": 1})
    r2 = dedup.run({"task_id": "t-dedup", "input": 1})
    assert r1 == r2
    assert r1["calls"] == 1, f"expected 1 runner call, got {r1['calls']}"
    print(f"  ✓ second run served from cache (runner calls={r1['calls']})")

    # ------------------------------------------------------------ #
    # 8. P2 3.8 — trace_id
    # ------------------------------------------------------------ #
    print("\n=== P2 3.8: trace_id propagation ===")
    node.run({"task_id": "t-trace"}, trace_id="trace-abc-123")
    last = node.history[-1]
    assert last.trace_id == "trace-abc-123"
    print(f"  ✓ trace_id captured in record: {last.trace_id}")

    # ------------------------------------------------------------ #
    # 9. P1 3.4 — metrics hook
    # ------------------------------------------------------------ #
    print("\n=== P1 3.4: metrics_hook ===")
    events: List[Tuple[str, Mapping[str, Any]]] = []
    mn = AgentNode(
        runtime=_DummyRuntime(),
        policy=_DummyPolicy(),
        runner_factory=_flaky_factory(fail_first=0),
        metrics_hook=lambda ev, f: events.append((ev, f)),
        node_id="node-metric",
    )
    mn.run({"task_id": "t-metric"})
    assert len(events) == 1
    assert events[0][1]["status"] == "completed"
    print(f"  ✓ captured {len(events)} metric event(s): "
          f"status={events[0][1]['status']}")

    # ------------------------------------------------------------ #
    # 10. P2 3.12 — node_id entropy
    # ------------------------------------------------------------ #
    print("\n=== P2 3.12: node_id has 48-bit entropy ===")
    suffix = node.node_id.split("-", 1)[1]
    assert len(suffix) == 12, suffix
    print(f"  ✓ node_id={node.node_id} (suffix length={len(suffix)})")

    # ------------------------------------------------------------ #
    # 11. Backward-compat sanity
    # ------------------------------------------------------------ #
    print("\n=== Backward compatibility ===")
    assert node.run({"task_id": "t-legacy"})["status"] == "ok"
    assert isinstance(node.statistics(), dict)
    assert len(node.history) >= 1
    print("  ✓ legacy API preserved")

    # ------------------------------------------------------------ #
    # 12. P0 2.6 — from_dict defaults
    # ------------------------------------------------------------ #
    print("\n=== P0 2.6: from_dict handles new fields ===")
    legacy_payload = {
        "config": {
            "task_timeout_seconds": 42.0,
            "max_task_retries": 0,
            # note: all new fields omitted
        },
        "strict": True,
        "history": [],
    }
    restored = AgentNode.from_dict(
        legacy_payload,
        runtime=_DummyRuntime(),
        policy=_DummyPolicy(),
        runner_factory=_flaky_factory(fail_first=0),
    )
    assert restored.config.task_timeout_seconds == 42.0
    assert restored.config.max_backoff_seconds == 30.0  # default
    assert restored.config.dedup_completed is False     # default
    print("  ✓ new config fields default correctly")

    # ------------------------------------------------------------ #
    # 13. Serialization + pickling round-trip
    # ------------------------------------------------------------ #
    print("\n=== Serialization + pickling ===")
    payload = node.to_json()
    restored = AgentNode.from_json(
        payload,
        runtime=_DummyRuntime(),
        policy=_DummyPolicy(),
        runner_factory=_flaky_factory(fail_first=0),
    )
    assert restored.to_dict()["counters"] == node.to_dict()["counters"]
    print("  ✓ JSON round-trip OK")

    import pickle
    pickled = pickle.loads(pickle.dumps(node))
    assert pickled.node_id == node.node_id
    assert pickled.statistics() == node.statistics()
    # Lock must be reconstructed
    pickled.run({"task_id": "t-after-pickle"})
    print("  ✓ Pickle round-trip OK")

    # ------------------------------------------------------------ #
    # 14. Validation
    # ------------------------------------------------------------ #
    print("\n=== Validation ===")
    for bad_kwargs in (
        dict(runtime=None, policy=_DummyPolicy()),
        dict(runtime=_DummyRuntime(), policy=None),
        dict(runtime=_DummyRuntime(), policy=_DummyPolicy(), node_id=""),
    ):
        try:
            AgentNode(**bad_kwargs)  # type: ignore[arg-type]
        except AgentNodeError as exc:
            print(f"  ✓ rejected: {exc}")
        else:
            raise AssertionError(f"Expected rejection: {bad_kwargs!r}")

    try:
        node.run(123)  # type: ignore[arg-type]
    except AgentNodeError as exc:
        print(f"  ✓ rejected task_input: {exc}")
    else:
        raise AssertionError("Expected rejection for non-Mapping task")

    # Bad config: zero timeout
    try:
        AgentNodeConfig(task_timeout_seconds=0)
    except AgentNodeError as exc:
        print(f"  ✓ rejected config: {exc}")
    else:
        raise AssertionError("Expected config rejection")

    print("\n✓ All smoke + regression tests passed.")
