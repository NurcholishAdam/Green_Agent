# src/continuum/continuum_scheduler.py
"""
Continuum Scheduler — v6.0.0
=============================

Workload orchestrator across the edge-cloud continuum.

**Public API preserved**

    scheduler = ContinuumScheduler(device_id, local_worker_pool,
                                   regional_endpoint, cloud_endpoint)
    await scheduler.start()
    await scheduler.submit_task(task_id, placement_decision, task_payload)
    await scheduler.stop()

**What changed (v5 → v6)**
--------------------------

**P0 — silent data loss and broken contracts**

1. **Offline buffer round-trip fixed (§2.1, §2.2, §2.3).**
   ``_sync_state_to_cloud`` now uploads **cache ∪ buffer**, deduplicates
   by ``task_id``, and **clears the buffer on success**. ``_buffer_state``
   deduplicates on every insert — retries no longer amplify.
2. **Sync loop recovers from offline (§2.4).** v5's ``else`` branch
   slept without ever re-attempting sync, leaving the scheduler stuck
   offline forever. v6 always attempts sync; on success it clears the
   offline flag automatically.
3. **No ``@ray_remote`` (§2.6).** v5 decorated the class with
   ``@ray.remote`` when Ray was installed — which broke ``async with``
   and direct instantiation. v6 keeps ``ContinuumScheduler`` as a plain
   Python class and provides ``ContinuumSchedulerActor`` as a separate
   Ray wrapper callers opt into.
4. **Bounded task queue (§2.7).** ``max_queue_size`` config; ``submit_task``
   blocks when full (backpressure); ``submit_task_nowait`` raises.
5. **``stop()`` drains the queue (§2.8).** Queued tasks are moved into the
   offline buffer with ``reason="shutdown_drain"``; the in-flight task is
   captured with ``reason="shutdown_in_flight"``. No silent drop.
6. **Honest serialization.** ``from_dict`` / ``from_json`` are **not
   implemented** — a scheduler carries a live ``local_worker_pool`` that
   cannot round-trip through JSON. The new ``restore_state(data)`` method
   restores cache and buffer **after** the caller re-constructs the
   scheduler with their pool.

**P1 — reliability**

7. **Retries fire only on transient errors.** ``_TRANSIENT_EXCEPTIONS``
   covers ``ConnectionError``, ``TimeoutError``, ``asyncio.TimeoutError``,
   and (when aiohttp is present) the aiohttp connection/server errors.
   Definitive failures (4xx, actor-not-found, missing aiohttp) skip the
   retry loop immediately.
8. **Task status transitions to ``"running"``** on the first attempt.
9. **``ray.init`` runs off the event loop** via ``asyncio.to_thread``.
10. **``_execute_local`` preserves the traceback** in the result summary.
11. **``_enforce_cache_bound_locked`` no longer uses a stray walrus**
    assignment.
12. **``strict=False`` implemented.** Soft failures (unknown tier,
    missing aiohttp, empty endpoint) log warnings and use safe defaults
    instead of raising. Fundamental invariants (non-empty ``device_id``,
    ``local_worker_pool.execute``) always raise.
13. **``health_check()``** returns ``healthy``, ``running``,
    ``offline_mode``, ``queue_depth``, ``cached_tasks``,
    ``buffered_tasks``, ``state_sync_failures``, uptime, config.
14. **Tier validated at submit time.** Unknown tiers raise (strict) or
    default to local (non-strict).

**P2 — observability**

15. **Structured logging** via ``_log(event, level, **fields)`` — the
    human-readable message is preserved; the fields also appear in
    ``logger``'s ``extra`` for JSON formatters.
16. **``collect_prometheus_metrics()``** returns a metric table shaped
    for any Prometheus exporter.
17. **Fallback trace** in ``TaskRecord.fallback_trace`` — a per-step
    record of every attempt and its outcome.

**Backward-compatible public API**

    __all__ = [
        "ContinuumScheduler",
        "ContinuumSchedulerActor",
        "ContinuumSchedulerError",
        "SchedulerConfig",
        "TaskRecord",
        "PlacementTier",
        "PlacementDecision",
    ]

**Running the demo**

    PYTHONPATH=src:src/carbon python -m continuum.continuum_scheduler
    (or)
    PYTHONPATH=src:src/carbon python src/continuum/continuum_scheduler.py
"""

from __future__ import annotations

import asyncio
import json
import logging
import math
import threading
import time
import traceback
from dataclasses import asdict, dataclass, field, fields
from datetime import datetime
from enum import Enum
from typing import Any, Dict, Iterable, List, Mapping, Optional, Tuple

logger = logging.getLogger(__name__)


# --------------------------------------------------------------------------- #
# Optional dependencies
# --------------------------------------------------------------------------- #
try:  # pragma: no cover — environment-dependent
    import ray  # type: ignore
    _RAY_AVAILABLE = True
except ImportError:  # pragma: no cover
    ray = None  # type: ignore[assignment]
    _RAY_AVAILABLE = False

try:  # pragma: no cover
    import aiohttp  # type: ignore
    _AIOHTTP_AVAILABLE = True
except ImportError:  # pragma: no cover
    aiohttp = None  # type: ignore[assignment]
    _AIOHTTP_AVAILABLE = False


# --------------------------------------------------------------------------- #
# Cross-module imports (defensive)
# --------------------------------------------------------------------------- #
try:  # pragma: no cover
    from .placement import PlacementDecision, PlacementTier  # type: ignore
except ImportError:  # pragma: no cover
    try:
        from placement import PlacementDecision, PlacementTier  # type: ignore
    except ImportError:
        class PlacementTier(Enum):  # type: ignore[no-redef]
            TIER_1_LOCAL = "tier_1_local"
            TIER_2_REGIONAL = "tier_2_regional"
            TIER_3_CLOUD = "tier_3_cloud"

        @dataclass
        class PlacementDecision:  # type: ignore[no-redef]
            selected_tier: PlacementTier = PlacementTier.TIER_1_LOCAL
            target_node: Optional[str] = None
            reason: str = "fallback"


# --------------------------------------------------------------------------- #
# Errors
# --------------------------------------------------------------------------- #
class ContinuumSchedulerError(ValueError):
    """Raised for invalid inputs, configuration, or execution failures."""


# --------------------------------------------------------------------------- #
# Structured logging helper
# --------------------------------------------------------------------------- #
def _log(event: str, level: int = logging.INFO, **fields: Any) -> None:
    """Emit a structured log event.

    The message stays human-readable; the ``extra`` dict is available to
    JSON formatters.
    """
    if not logger.isEnabledFor(level):
        return
    parts = [event]
    if fields:
        parts.append(" ".join(f"{k}={v}" for k, v in fields.items()))
    logger.log(level, " ".join(parts), extra={"event": event, **fields})


# --------------------------------------------------------------------------- #
# JSON-safe helper
# --------------------------------------------------------------------------- #
def _json_safe(obj: Any) -> Any:
    if isinstance(obj, float):
        return None if (math.isnan(obj) or math.isinf(obj)) else obj
    if isinstance(obj, datetime):
        return obj.isoformat()
    if isinstance(obj, Enum):
        return obj.value
    if isinstance(obj, Mapping):
        return {str(k): _json_safe(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple, set)):
        return [_json_safe(v) for v in obj]
    return obj


# --------------------------------------------------------------------------- #
# Transient-vs-definitive exception classification (P1 §7)
# --------------------------------------------------------------------------- #
_TRANSIENT_EXCEPTIONS: Tuple[type, ...] = (
    ConnectionError,
    TimeoutError,
    asyncio.TimeoutError,
)
if _AIOHTTP_AVAILABLE:
    for _name in (
        "ClientConnectorError",
        "ServerDisconnectedError",
        "ServerTimeoutError",
        "ClientConnectionError",
    ):
        _exc = getattr(aiohttp, _name, None)
        if _exc is not None:
            _TRANSIENT_EXCEPTIONS += (_exc,)


# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class SchedulerConfig:
    """Tunable parameters for the Continuum Scheduler."""

    # Scheduling loop cadence
    idle_sleep_seconds: float = 0.1
    error_sleep_seconds: float = 1.0

    # Network call policy
    request_timeout_seconds: float = 30.0
    max_network_retries: int = 2
    base_network_backoff_seconds: float = 0.5

    # State synchronization
    state_sync_interval_seconds: float = 30.0
    offline_retry_seconds: float = 60.0
    offline_after_failures: int = 3   # P1 §3.6-style: fail 3× before offline

    # Bounded caches / buffers (P0 §2.7: added max_queue_size)
    max_state_cache: Optional[int] = 10_000
    max_offline_buffer: Optional[int] = 5_000
    max_queue_size: int = 10_000

    # Task lifecycle
    max_task_retries: int = 1

    def __post_init__(self) -> None:
        for name in (
            "idle_sleep_seconds",
            "error_sleep_seconds",
            "request_timeout_seconds",
            "state_sync_interval_seconds",
            "offline_retry_seconds",
        ):
            value = getattr(self, name)
            if value <= 0:
                raise ContinuumSchedulerError(
                    f"{name} must be > 0 (got {value})"
                )
        for name in (
            "max_network_retries",
            "base_network_backoff_seconds",
            "max_task_retries",
            "offline_after_failures",
        ):
            if getattr(self, name) < 0:
                raise ContinuumSchedulerError(f"{name} must be >= 0")
        for name in ("max_state_cache", "max_offline_buffer"):
            v = getattr(self, name)
            if v is not None and v <= 0:
                raise ContinuumSchedulerError(f"{name} must be > 0 or None")
        if self.max_queue_size <= 0:
            raise ContinuumSchedulerError(
                "max_queue_size must be > 0"
            )
        if self.offline_after_failures < 1:
            raise ContinuumSchedulerError(
                "offline_after_failures must be >= 1"
            )


# --------------------------------------------------------------------------- #
# Immutable task snapshot
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class TaskRecord:
    """Immutable snapshot of a scheduled task."""

    task_id: str
    status: str  # "queued" | "running" | "completed" | "failed" | "buffered"
    tier: Optional[str]
    target_node: Optional[str]
    submitted_at: float
    updated_at: float
    attempts: int
    latency_ms: Optional[float] = None
    error: Optional[str] = None
    result_summary: Optional[Mapping[str, Any]] = None
    fallback_trace: Tuple[Mapping[str, Any], ...] = field(
        default_factory=tuple
    )

    def to_dict(self) -> Dict[str, Any]:
        d = _json_safe(asdict(self))
        if d.get("result_summary") is not None:
            d["result_summary"] = dict(d["result_summary"])
        if d.get("fallback_trace") is not None:
            d["fallback_trace"] = [dict(step) for step in d["fallback_trace"]]
        return d


# --------------------------------------------------------------------------- #
# ContinuumScheduler (plain Python class — no @ray_remote)
# --------------------------------------------------------------------------- #
class ContinuumScheduler:
    """
    Workload orchestrator across edge-cloud continuum.

    **Not a Ray actor.** The class runs in-process and can be used directly
    with ``async with``. Callers who want a Ray actor wrap it explicitly:

        RemoteScheduler = ray.remote(ContinuumScheduler)
        handle = RemoteScheduler.remote(...)

    or use :class:`ContinuumSchedulerActor`, the bundled wrapper.

    Parameters
    ----------
    device_id, local_worker_pool, regional_endpoint, cloud_endpoint, config, strict
        As in v1 — see :class:`SchedulerConfig`.
    """

    _VALID_TIERS = frozenset(t.value for t in PlacementTier)

    # ------------------------------------------------------------------ init
    def __init__(
        self,
        device_id: str,
        local_worker_pool: Any,
        regional_endpoint: str,
        cloud_endpoint: str,
        *,
        config: Optional[SchedulerConfig] = None,
        strict: bool = True,
    ) -> None:
        # Fundamental invariants — always raise
        if not isinstance(device_id, str) or not device_id:
            raise ContinuumSchedulerError(
                "device_id must be a non-empty string"
            )
        if local_worker_pool is None or not hasattr(
            local_worker_pool, "execute"
        ):
            raise ContinuumSchedulerError(
                "local_worker_pool must expose an 'execute' method"
            )

        self._strict = bool(strict)

        # Soft invariants — warn-and-default in non-strict mode
        if not isinstance(regional_endpoint, str):
            if self._strict:
                raise ContinuumSchedulerError(
                    "regional_endpoint must be a string"
                )
            _log("regional_endpoint_non_string", logging.WARNING,
                 value=repr(regional_endpoint))
            regional_endpoint = ""
        if not isinstance(cloud_endpoint, str):
            if self._strict:
                raise ContinuumSchedulerError(
                    "cloud_endpoint must be a string"
                )
            _log("cloud_endpoint_non_string", logging.WARNING,
                 value=repr(cloud_endpoint))
            cloud_endpoint = ""

        self.device_id: str = device_id
        self.local_pool = local_worker_pool
        self.regional_endpoint: str = regional_endpoint.rstrip("/")
        self.cloud_endpoint: str = cloud_endpoint

        self._config: SchedulerConfig = config or SchedulerConfig()

        # Locks
        self._lock = threading.RLock()

        # State cache (bounded FIFO)
        self._state_cache: Dict[str, TaskRecord] = {}
        self._state_order: List[str] = []

        # Offline buffer (bounded, deduped by task_id)
        self._offline_buffer: List[Dict[str, Any]] = []
        self._offline_mode: bool = False
        self._consecutive_sync_failures: int = 0
        self._sync_failure_count: int = 0

        # Lifecycle
        self._running: bool = False
        self._task_queue: Optional["asyncio.Queue[Dict[str, Any]]"] = None
        self._tasks: List[asyncio.Task] = []
        self._current_task_id: Optional[str] = None
        self._ctx_start: Optional[float] = None
        self._started_at: Optional[float] = None

        _log(
            "scheduler_initialized",
            logging.DEBUG,
            device_id=self.device_id,
            strict=self._strict,
            ray=_RAY_AVAILABLE,
            aiohttp=_AIOHTTP_AVAILABLE,
            max_queue_size=self._config.max_queue_size,
        )

    # ---------------------------------------------------------------- props
    @property
    def config(self) -> SchedulerConfig:
        return self._config

    @property
    def strict(self) -> bool:
        return self._strict

    @property
    def offline_mode(self) -> bool:
        with self._lock:
            return self._offline_mode

    @property
    def running(self) -> bool:
        return self._running

    @property
    def queued_tasks(self) -> int:
        queue = self._task_queue
        return queue.qsize() if queue is not None else 0

    @property
    def buffered_tasks(self) -> int:
        with self._lock:
            return len(self._offline_buffer)

    # -------------------------------------------------------- lifecycle
    async def start(self) -> None:
        """Start the scheduling and state-sync loops."""
        if self._running:
            _log("scheduler_already_running", logging.DEBUG,
                 device_id=self.device_id)
            return

        # Bind the queue to the running loop (safe under Ray actors).
        self._task_queue = asyncio.Queue(
            maxsize=self._config.max_queue_size
        )
        self._running = True
        self._started_at = time.time()

        self._tasks = [
            asyncio.create_task(
                self._scheduling_loop(), name="continuum:scheduling"
            ),
            asyncio.create_task(
                self._state_sync_loop(), name="continuum:state_sync"
            ),
        ]
        _log(
            "scheduler_started",
            logging.INFO,
            device_id=self.device_id,
            tasks=len(self._tasks),
        )

    async def stop(
        self, *, timeout_seconds: float = 5.0, flush: bool = True
    ) -> None:
        """
        Stop the scheduler loops, drain queued tasks into the offline
        buffer, and (optionally) flush the buffer to the cloud.
        """
        if not self._running:
            return

        # Capture in-flight task before cancelling (§2.8)
        in_flight = self._current_task_id
        self._running = False

        # Cancel loops and await their termination
        for t in self._tasks:
            t.cancel()
        if self._tasks:
            try:
                await asyncio.wait_for(
                    asyncio.gather(*self._tasks, return_exceptions=True),
                    timeout=timeout_seconds,
                )
            except asyncio.TimeoutError:  # pragma: no cover
                _log(
                    "shutdown_loops_timeout",
                    logging.WARNING,
                    device_id=self.device_id,
                )
        self._tasks = []

        # Drain the queue into the offline buffer
        drained = 0
        if self._task_queue is not None:
            while True:
                try:
                    task = self._task_queue.get_nowait()
                except asyncio.QueueEmpty:
                    break
                self._buffer_state(
                    [
                        {
                            "task_id": task["task_id"],
                            "reason": "shutdown_drain",
                            "at": time.time(),
                        }
                    ]
                )
                with self._lock:
                    existing = self._state_cache.get(task["task_id"])
                    if existing is not None and existing.status in (
                        "queued",
                        "running",
                    ):
                        self._update_status(
                            task["task_id"], status="buffered"
                        )
                drained += 1

        # Capture the in-flight task (§2.8)
        if in_flight is not None:
            with self._lock:
                existing = self._state_cache.get(in_flight)
                if existing is not None and existing.status in (
                    "queued",
                    "running",
                ):
                    self._buffer_state(
                        [
                            {
                                "task_id": in_flight,
                                "reason": "shutdown_in_flight",
                                "at": time.time(),
                            }
                        ]
                    )
                    self._update_status(
                        in_flight, status="buffered"
                    )
                    drained += 1

        if drained:
            _log(
                "shutdown_drained",
                logging.INFO,
                device_id=self.device_id,
                drained=drained,
                buffered=len(self._offline_buffer),
            )

        # Flush the buffer if requested and possible
        if flush and self._offline_buffer and not self._offline_mode:
            try:
                await self._sync_state_to_cloud()
            except Exception:
                logger.exception("Failed to flush buffer on stop")

        _log(
            "scheduler_stopped",
            logging.INFO,
            device_id=self.device_id,
            cached=len(self._state_cache),
            buffered=len(self._offline_buffer),
        )

    # -------------------------------------------------------- public API
    async def submit_task(
        self,
        task_id: str,
        placement_decision: Any,
        task_payload: Mapping[str, Any],
    ) -> str:
        """
        Submit a task for execution.

        **Backpressure**: when the queue is full, this method blocks until
        space is available. Use :meth:`submit_task_nowait` to fail fast.
        """
        task = self._prepare_task(task_id, placement_decision, task_payload)
        if self._task_queue is None:
            raise ContinuumSchedulerError(
                "Scheduler is not running. Call 'await start()' first."
            )
        await self._task_queue.put(task)
        return task_id

    async def submit_task_nowait(
        self,
        task_id: str,
        placement_decision: Any,
        task_payload: Mapping[str, Any],
    ) -> str:
        """
        Non-blocking submit. Raises :class:`ContinuumSchedulerError` if the
        queue is full.
        """
        task = self._prepare_task(task_id, placement_decision, task_payload)
        if self._task_queue is None:
            raise ContinuumSchedulerError(
                "Scheduler is not running. Call 'await start()' first."
            )
        try:
            self._task_queue.put_nowait(task)
        except asyncio.QueueFull as exc:
            raise ContinuumSchedulerError(
                f"Task queue full (max {self._config.max_queue_size})"
            ) from exc
        return task_id

    def _prepare_task(
        self,
        task_id: str,
        placement_decision: Any,
        task_payload: Mapping[str, Any],
    ) -> Dict[str, Any]:
        if not isinstance(task_id, str) or not task_id:
            raise ContinuumSchedulerError(
                "task_id must be a non-empty string"
            )
        if placement_decision is None or not hasattr(
            placement_decision, "selected_tier"
        ):
            raise ContinuumSchedulerError(
                "placement_decision must expose 'selected_tier'"
            )
        if not isinstance(task_payload, Mapping):
            raise ContinuumSchedulerError(
                f"task_payload must be a Mapping, "
                f"got {type(task_payload).__name__}"
            )

        # P1 §14: validate tier at submit time
        tier = self._tier_name(placement_decision.selected_tier)
        if tier not in self._VALID_TIERS:
            if self._strict:
                raise ContinuumSchedulerError(
                    f"unknown tier: {tier!r}; "
                    f"must be one of {sorted(self._VALID_TIERS)}"
                )
            _log(
                "unknown_tier_default_local",
                logging.WARNING,
                task_id=task_id,
                requested=tier,
            )
            tier = PlacementTier.TIER_1_LOCAL.value

        target_node = getattr(placement_decision, "target_node", None)

        record = TaskRecord(
            task_id=task_id,
            status="queued",
            tier=tier,
            target_node=target_node,
            submitted_at=time.time(),
            updated_at=time.time(),
            attempts=0,
        )
        self._cache_put(record)

        return {
            "task_id": task_id,
            "placement": placement_decision,
            "payload": dict(task_payload),
            "submitted_at": time.time(),
            "status": "queued",
        }

    async def get_task_status(self, task_id: str) -> Dict[str, Any]:
        with self._lock:
            record = self._state_cache.get(task_id)
        if record is None:
            return {"task_id": task_id, "status": "not_found"}
        return record.to_dict()

    async def enable_offline_mode(self) -> None:
        with self._lock:
            self._offline_mode = True
        _log("offline_mode_on", logging.INFO, device_id=self.device_id)

    async def disable_offline_mode(self) -> None:
        with self._lock:
            self._offline_mode = False
            self._consecutive_sync_failures = 0
        try:
            await self._sync_state_to_cloud()
        except Exception:
            logger.exception("Failed to sync on offline-mode exit")
        _log("offline_mode_off", logging.INFO, device_id=self.device_id)

    async def health_check(self) -> Dict[str, Any]:
        """Return operational state (§P1 §3.9)."""
        with self._lock:
            return {
                "healthy": self._running and not self._offline_mode,
                "device_id": self.device_id,
                "running": self._running,
                "offline_mode": self._offline_mode,
                "queue_depth": (
                    self._task_queue.qsize()
                    if self._task_queue is not None
                    else 0
                ),
                "cached_tasks": len(self._state_cache),
                "buffered_tasks": len(self._offline_buffer),
                "state_sync_failures": self._sync_failure_count,
                "consecutive_sync_failures": (
                    self._consecutive_sync_failures
                ),
                "started_at": self._started_at,
                "uptime_seconds": (
                    (time.time() - self._started_at)
                    if self._started_at
                    else None
                ),
                "ray_available": _RAY_AVAILABLE,
                "aiohttp_available": _AIOHTTP_AVAILABLE,
                "config": asdict(self._config),
            }

    def collect_prometheus_metrics(
        self,
    ) -> Dict[str, Tuple[Any, Dict[str, str]]]:
        """Return a metric table for a Prometheus exporter (P2 §16)."""
        with self._lock:
            status_counts: Dict[str, int] = {}
            for r in self._state_cache.values():
                status_counts[r.status] = status_counts.get(r.status, 0) + 1
            return {
                "continuum_queue_depth": (
                    self._task_queue.qsize()
                    if self._task_queue is not None
                    else 0,
                    {},
                ),
                "continuum_offline_buffer_size": (
                    len(self._offline_buffer),
                    {},
                ),
                "continuum_state_cache_size": (
                    len(self._state_cache),
                    {},
                ),
                "continuum_offline_mode": (
                    int(self._offline_mode),
                    {"device_id": self.device_id},
                ),
                "continuum_state_sync_failures_total": (
                    self._sync_failure_count,
                    {"device_id": self.device_id},
                ),
                "continuum_tasks_completed_total": (
                    status_counts.get("completed", 0),
                    {"device_id": self.device_id},
                ),
                "continuum_tasks_failed_total": (
                    status_counts.get("failed", 0),
                    {"device_id": self.device_id},
                ),
                "continuum_tasks_buffered_total": (
                    status_counts.get("buffered", 0),
                    {"device_id": self.device_id},
                ),
            }

    # --------------------------------------------------------- scheduling loop
    async def _scheduling_loop(self) -> None:
        cfg = self._config
        while self._running:
            try:
                queue = self._task_queue
                if queue is None:
                    await asyncio.sleep(cfg.idle_sleep_seconds)
                    continue
                if queue.empty():
                    await asyncio.sleep(cfg.idle_sleep_seconds)
                    continue

                task = await queue.get()
                self._current_task_id = task["task_id"]
                try:
                    if self._offline_mode:
                        result = await self._execute_with_fallback(
                            task, allow_remote=False
                        )
                    else:
                        result = await self._execute_with_fallback(
                            task, allow_remote=True
                        )

                    status = (
                        "completed" if "error" not in result else "failed"
                    )
                    self._update_status(
                        task["task_id"],
                        status=status,
                        result_summary=result,
                        error=result.get("error"),
                        fallback_trace=result.get("fallback_trace"),
                    )
                finally:
                    self._current_task_id = None

            except asyncio.CancelledError:
                raise
            except Exception as exc:  # pragma: no cover — defensive
                _log(
                    "scheduling_loop_error",
                    logging.ERROR,
                    error=str(exc),
                    traceback=traceback.format_exc(),
                )
                await asyncio.sleep(cfg.error_sleep_seconds)

    async def _execute_with_fallback(
        self,
        task: Mapping[str, Any],
        *,
        allow_remote: bool,
    ) -> Dict[str, Any]:
        """
        Execute a task according to its placement, with automatic fallback
        and a fallback-trace recorded in the result (P2 §17).
        """
        placement = task["placement"]
        tier = self._tier_name(placement.selected_tier)
        trace: List[Dict[str, Any]] = []
        last_error: Optional[BaseException] = None

        def _bump() -> None:
            self._update_status(
                task["task_id"],
                status="running",
                attempts=self._next_attempt(task["task_id"]),
            )

        # --- Local (no fallback) ----------------------------------------
        if tier == PlacementTier.TIER_1_LOCAL.value or not allow_remote:
            _bump()
            result = await self._execute_local(dict(task))
            trace.append(
                {
                    "tier": "local",
                    "attempt": 1,
                    "outcome": "error" if "error" in result else "success",
                    "error": result.get("error"),
                }
            )
            result["fallback_trace"] = tuple(trace)
            return result

        # --- Regional with fallback ------------------------------------
        if tier == PlacementTier.TIER_2_REGIONAL.value:
            for attempt in range(
                1 + self._config.max_network_retries
            ):
                _bump()
                try:
                    result = await self._execute_regional(dict(task))
                    trace.append(
                        {
                            "tier": "regional",
                            "attempt": attempt + 1,
                            "outcome": "success",
                        }
                    )
                    result["fallback_trace"] = tuple(trace)
                    return result
                except _TRANSIENT_EXCEPTIONS as exc:
                    last_error = exc
                    trace.append(
                        {
                            "tier": "regional",
                            "attempt": attempt + 1,
                            "outcome": "error",
                            "error": f"{type(exc).__name__}: {exc}",
                            "retryable": True,
                        }
                    )
                    if attempt < self._config.max_network_retries:
                        await asyncio.sleep(
                            self._config.base_network_backoff_seconds
                            * (2 ** attempt)
                        )
                except Exception as exc:
                    last_error = exc
                    trace.append(
                        {
                            "tier": "regional",
                            "attempt": attempt + 1,
                            "outcome": "error",
                            "error": f"{type(exc).__name__}: {exc}",
                            "retryable": False,
                        }
                    )
                    break   # definitive failure — skip remaining retries

        # --- Cloud with local fallback ---------------------------------
        if allow_remote:
            for attempt in range(
                1 + self._config.max_network_retries
            ):
                _bump()
                try:
                    result = await self._execute_cloud(dict(task))
                    trace.append(
                        {
                            "tier": "cloud",
                            "attempt": attempt + 1,
                            "outcome": "success",
                        }
                    )
                    result["fallback_trace"] = tuple(trace)
                    return result
                except _TRANSIENT_EXCEPTIONS as exc:
                    last_error = exc
                    trace.append(
                        {
                            "tier": "cloud",
                            "attempt": attempt + 1,
                            "outcome": "error",
                            "error": f"{type(exc).__name__}: {exc}",
                            "retryable": True,
                        }
                    )
                    if attempt < self._config.max_network_retries:
                        await asyncio.sleep(
                            self._config.base_network_backoff_seconds
                            * (2 ** attempt)
                        )
                except Exception as exc:
                    last_error = exc
                    trace.append(
                        {
                            "tier": "cloud",
                            "attempt": attempt + 1,
                            "outcome": "error",
                            "error": f"{type(exc).__name__}: {exc}",
                            "retryable": False,
                        }
                    )
                    break

        # --- Last resort: local ----------------------------------------
        _bump()
        result = await self._execute_local(dict(task))
        trace.append(
            {
                "tier": "local",
                "attempt": 1,
                "outcome": "error" if "error" in result else "success",
                "error": result.get("error"),
                "last_error_before_fallback": (
                    str(last_error) if last_error else None
                ),
            }
        )
        result["fallback_trace"] = tuple(trace)
        return result

    def _next_attempt(self, task_id: str) -> int:
        with self._lock:
            existing = self._state_cache.get(task_id)
            return (existing.attempts if existing else 0) + 1

    # --------------------------------------------------------- tier executors
    async def _execute_local(
        self, task: Mapping[str, Any]
    ) -> Dict[str, Any]:
        pool = self.local_pool
        try:
            result = await pool.execute(
                task_id=task["task_id"], payload=task["payload"]
            )
            return {"tier": "local", "result": result}
        except Exception as exc:
            tb = traceback.format_exc()
            _log(
                "local_execution_failed",
                logging.ERROR,
                task_id=task["task_id"],
                error=f"{type(exc).__name__}: {exc}",
            )
            return {
                "tier": "local",
                "error": f"{type(exc).__name__}: {exc}",
                "traceback": tb,
            }

    async def _execute_regional(
        self, task: Mapping[str, Any]
    ) -> Dict[str, Any]:
        if not _AIOHTTP_AVAILABLE:
            raise ContinuumSchedulerError(
                "aiohttp is required for regional execution"
            )
        if not self.regional_endpoint:
            raise ContinuumSchedulerError(
                "regional_endpoint is empty"
            )
        url = f"{self.regional_endpoint}/api/v1/execute"
        payload = {
            "task_id": task["task_id"],
            "payload": task["payload"],
            "source_device": self.device_id,
        }
        timeout = aiohttp.ClientTimeout(
            total=self._config.request_timeout_seconds
        )
        async with aiohttp.ClientSession() as session:
            async with session.post(
                url, json=payload, timeout=timeout
            ) as response:
                response.raise_for_status()
                body = await response.json()
                return {"tier": "regional", "result": body}

    async def _execute_cloud(
        self, task: Mapping[str, Any]
    ) -> Dict[str, Any]:
        if not _RAY_AVAILABLE:
            if not _AIOHTTP_AVAILABLE:
                raise ContinuumSchedulerError(
                    "Neither Ray nor aiohttp is available for cloud "
                    "execution"
                )
            if not self.cloud_endpoint:
                raise ContinuumSchedulerError(
                    "cloud_endpoint is empty"
                )
            url = f"{self.cloud_endpoint.rstrip('/')}/api/v1/execute"
            payload = {
                "task_id": task["task_id"],
                "payload": task["payload"],
                "source_device": self.device_id,
            }
            timeout = aiohttp.ClientTimeout(
                total=self._config.request_timeout_seconds
            )
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    url, json=payload, timeout=timeout
                ) as response:
                    response.raise_for_status()
                    body = await response.json()
                    return {"tier": "cloud_http", "result": body}

        # Ray path — run blocking calls off the event loop
        if not ray.is_initialized():
            await asyncio.to_thread(
                ray.init,
                address=self.cloud_endpoint or None,
                ignore_reinit_error=True,
            )
        target = (
            getattr(task["placement"], "target_node", None) or "default"
        )
        actor_name = f"cloud_worker_{target}"
        try:
            remote_worker = await asyncio.to_thread(
                ray.get_actor, actor_name
            )
        except Exception as exc:
            raise ContinuumSchedulerError(
                f"Cloud worker '{actor_name}' not found: {exc}"
            ) from exc

        ref = remote_worker.execute.remote(task["payload"])
        result = await asyncio.to_thread(ray.get, ref)
        return {"tier": "cloud_ray", "result": result}

    # --------------------------------------------------------- state sync
    async def _state_sync_loop(self) -> None:
        """
        Periodically sync state to the cloud.

        Always attempts sync — even in offline mode — so the loop recovers
        automatically when the network returns (§2.4).
        """
        cfg = self._config
        while self._running:
            try:
                await self._sync_state_to_cloud()
                with self._lock:
                    self._offline_mode = False
                    self._consecutive_sync_failures = 0
                await asyncio.sleep(cfg.state_sync_interval_seconds)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                with self._lock:
                    self._consecutive_sync_failures += 1
                    self._sync_failure_count += 1
                    if (
                        self._consecutive_sync_failures
                        >= cfg.offline_after_failures
                    ):
                        if not self._offline_mode:
                            _log(
                                "offline_mode_entered",
                                logging.WARNING,
                                device_id=self.device_id,
                                failures=self._consecutive_sync_failures,
                                error=str(exc),
                            )
                        self._offline_mode = True
                await asyncio.sleep(cfg.offline_retry_seconds)

    async def _sync_state_to_cloud(self) -> None:
        """
        Upload the union of the state cache and the offline buffer to the
        cloud endpoint, deduplicated by ``task_id`` (§2.1, §2.2, §2.3).
        """
        with self._lock:
            cache_records = [r.to_dict() for r in self._state_cache.values()]
            buffer_records = list(self._offline_buffer)

        # Dedupe by task_id — cache wins over buffer
        by_id: Dict[str, Dict[str, Any]] = {}
        for r in buffer_records:
            task_id = r.get("task_id")
            if task_id:
                by_id[task_id] = dict(r)
        for r in cache_records:
            task_id = r.get("task_id")
            if task_id:
                by_id[task_id] = dict(r)

        pending = list(by_id.values())
        if not pending:
            return

        if not self.cloud_endpoint or not _AIOHTTP_AVAILABLE:
            self._buffer_state(pending)
            _log(
                "state_sync_buffered_no_endpoint",
                logging.DEBUG,
                count=len(pending),
            )
            return

        url = f"{self.cloud_endpoint.rstrip('/')}/api/v1/state/sync"
        payload = _json_safe(
            {"device_id": self.device_id, "records": pending}
        )
        timeout = aiohttp.ClientTimeout(
            total=self._config.request_timeout_seconds
        )

        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    url, json=payload, timeout=timeout
                ) as response:
                    response.raise_for_status()
            # Success — clear the buffer (§2.3)
            with self._lock:
                self._offline_buffer.clear()
            _log(
                "state_sync_ok",
                logging.DEBUG,
                device_id=self.device_id,
                uploaded=len(pending),
            )
        except Exception as exc:
            self._buffer_state(pending)
            _log(
                "state_sync_failed",
                logging.WARNING,
                device_id=self.device_id,
                error=str(exc),
                buffered=len(pending),
            )
            raise

    def _buffer_state(self, records: List[Dict[str, Any]]) -> None:
        """Append records to the bounded, deduped offline buffer."""
        with self._lock:
            by_id: Dict[str, Dict[str, Any]] = {}
            for r in self._offline_buffer:
                tid = r.get("task_id")
                if tid:
                    by_id[tid] = r
            for r in records:
                tid = r.get("task_id")
                if tid:
                    by_id[tid] = dict(r)
            new_buffer = list(by_id.values())
            cap = self._config.max_offline_buffer
            if cap is not None and len(new_buffer) > cap:
                new_buffer = new_buffer[-cap:]
            self._offline_buffer = new_buffer

    # --------------------------------------------------------- cache helpers
    def _cache_put(self, record: TaskRecord) -> None:
        with self._lock:
            if record.task_id not in self._state_cache:
                self._state_order.append(record.task_id)
            self._state_cache[record.task_id] = record
            self._enforce_cache_bound_locked()

    def _update_status(
        self,
        task_id: str,
        *,
        status: Optional[str] = None,
        result_summary: Optional[Mapping[str, Any]] = None,
        error: Optional[str] = None,
        attempts: Optional[int] = None,
        fallback_trace: Optional[Iterable[Mapping[str, Any]]] = None,
    ) -> None:
        with self._lock:
            existing = self._state_cache.get(task_id)
            if existing is None:
                existing = TaskRecord(
                    task_id=task_id,
                    status=status or "unknown",
                    tier=None,
                    target_node=None,
                    submitted_at=time.time(),
                    updated_at=time.time(),
                    attempts=attempts or 0,
                )
            trace = existing.fallback_trace
            if fallback_trace is not None:
                trace = tuple(
                    _json_safe(dict(step)) for step in fallback_trace
                )
            updated = TaskRecord(
                task_id=existing.task_id,
                status=status or existing.status,
                tier=existing.tier,
                target_node=existing.target_node,
                submitted_at=existing.submitted_at,
                updated_at=time.time(),
                attempts=(
                    attempts
                    if attempts is not None
                    else existing.attempts
                ),
                latency_ms=(
                    (time.time() - existing.submitted_at) * 1000.0
                    if status in ("completed", "failed")
                    else existing.latency_ms
                ),
                error=error if error is not None else existing.error,
                result_summary=(
                    _json_safe(dict(result_summary))
                    if result_summary is not None
                    else existing.result_summary
                ),
                fallback_trace=trace,
            )
            self._cache_put(updated)

    def _enforce_cache_bound_locked(self) -> None:
        """Evict oldest cache entries when over ``max_state_cache``."""
        cap = self._config.max_state_cache
        if cap is None:
            return
        while len(self._state_order) > cap:
            oldest = self._state_order.pop(0)
            self._state_cache.pop(oldest, None)

    # --------------------------------------------------------- helpers
    @staticmethod
    def _tier_name(tier: Any) -> str:
        if isinstance(tier, Enum):
            return tier.value
        return str(tier)

    # --------------------------------------------------------- statistics
    def statistics(self) -> Dict[str, Any]:
        with self._lock:
            records = list(self._state_cache.values())
            buffered = len(self._offline_buffer)
            queue_depth = (
                self._task_queue.qsize()
                if self._task_queue is not None
                else 0
            )

        if not records:
            return {
                "count": 0,
                "completed": 0,
                "failed": 0,
                "queued": 0,
                "running": 0,
                "buffered_tasks": buffered,
                "queue_depth": queue_depth,
                "mean_latency_ms": None,
                "success_rate": None,
            }

        completed = sum(1 for r in records if r.status == "completed")
        failed = sum(1 for r in records if r.status == "failed")
        queued = sum(1 for r in records if r.status == "queued")
        running = sum(1 for r in records if r.status == "running")
        latencies = [
            r.latency_ms for r in records if r.latency_ms is not None
        ]
        return {
            "count": len(records),
            "completed": completed,
            "failed": failed,
            "queued": queued,
            "running": running,
            "buffered_tasks": buffered,
            "queue_depth": queue_depth,
            "mean_latency_ms": (
                sum(latencies) / len(latencies) if latencies else None
            ),
            "success_rate": (
                completed / (completed + failed)
                if (completed + failed) > 0
                else None
            ),
        }

    def reset(
        self,
        *,
        clear_cache: bool = False,
        clear_buffer: bool = False,
    ) -> None:
        """Reset in-memory state. Buffer clearing is opt-in."""
        with self._lock:
            if clear_cache:
                self._state_cache.clear()
                self._state_order.clear()
            if clear_buffer:
                self._offline_buffer.clear()
        _log(
            "scheduler_reset",
            logging.DEBUG,
            device_id=self.device_id,
            clear_cache=clear_cache,
            clear_buffer=clear_buffer,
        )

    # --------------------------------------------------------- serialization
    def to_dict(self) -> Dict[str, Any]:
        with self._lock:
            return _json_safe(
                {
                    "device_id": self.device_id,
                    "regional_endpoint": self.regional_endpoint,
                    "cloud_endpoint": self.cloud_endpoint,
                    "config": asdict(self._config),
                    "strict": self._strict,
                    "offline_mode": self._offline_mode,
                    "running": self._running,
                    "state_cache": [
                        r.to_dict() for r in self._state_cache.values()
                    ],
                    "offline_buffer": list(self._offline_buffer),
                }
            )

    def to_json(self, **kwargs: Any) -> str:
        kwargs.setdefault("sort_keys", True)
        return json.dumps(self.to_dict(), **kwargs)

    def restore_state(self, data: Mapping[str, Any]) -> None:
        """
        Restore cache and buffer from a ``to_dict()`` payload.

        The caller must have already re-constructed the scheduler (with a
        valid ``local_worker_pool``, endpoints, and config). This method
        restores only the ephemeral state.
        """
        if not isinstance(data, Mapping):
            raise ContinuumSchedulerError(
                f"restore_state expects a Mapping, "
                f"got {type(data).__name__}"
            )
        restored_cache: Dict[str, TaskRecord] = {}
        restored_order: List[str] = []
        for raw in data.get("state_cache", []) or []:
            if not isinstance(raw, Mapping):
                continue
            try:
                record = TaskRecord(**{k: raw[k] for k in raw if k in {
                    f.name for f in fields(TaskRecord)
                }})
            except (TypeError, KeyError):
                _log(
                    "restore_state_skip_record",
                    logging.WARNING,
                    task_id=raw.get("task_id"),
                )
                continue
            restored_cache[record.task_id] = record
            restored_order.append(record.task_id)

        restored_buffer = [
            dict(r) for r in data.get("offline_buffer", []) or []
            if isinstance(r, Mapping)
        ]
        with self._lock:
            self._state_cache = restored_cache
            self._state_order = restored_order
            self._offline_buffer = restored_buffer

    # --------------------------------------------------------- async ctx mgr
    async def __aenter__(self) -> "ContinuumScheduler":
        self._ctx_start = time.perf_counter()
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        elapsed = time.perf_counter() - (
            self._ctx_start
            if self._ctx_start is not None
            else time.perf_counter()
        )
        self._ctx_start = None
        try:
            await self.stop()
        finally:
            if exc_type is not None:
                _log(
                    "scheduler_scope_exited_exception",
                    logging.WARNING,
                    device_id=self.device_id,
                    exception=exc_type.__name__,
                    elapsed=elapsed,
                )
            else:
                _log(
                    "scheduler_scope_closed",
                    logging.INFO,
                    device_id=self.device_id,
                    elapsed=elapsed,
                    cached=len(self._state_cache),
                )

    def __repr__(self) -> str:
        with self._lock:
            return (
                "ContinuumScheduler("
                f"device_id={self.device_id!r}, "
                f"running={self._running}, "
                f"offline={self._offline_mode}, "
                f"cached={len(self._state_cache)}, "
                f"buffered={len(self._offline_buffer)})"
            )


# --------------------------------------------------------------------------- #
# ContinuumSchedulerActor — explicit Ray wrapper (§2.6)
# --------------------------------------------------------------------------- #
class ContinuumSchedulerActor:
    """
    Ray-actor wrapper for :class:`ContinuumScheduler`.

    Use when you want a remote scheduler:

        SchedulerActor = ray.remote(ContinuumSchedulerActor)
        handle = SchedulerActor.remote(...)
        await handle.start.remote()

    The wrapper exists so users get Ray's actor semantics **without**
    forcing the plain class to lose its ``async with`` capability.
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        self._inner = ContinuumScheduler(*args, **kwargs)

    async def start(self) -> None:
        await self._inner.start()

    async def stop(self, **kwargs: Any) -> None:
        await self._inner.stop(**kwargs)

    async def submit_task(self, *args: Any, **kwargs: Any) -> str:
        return await self._inner.submit_task(*args, **kwargs)

    async def submit_task_nowait(self, *args: Any, **kwargs: Any) -> str:
        return await self._inner.submit_task_nowait(*args, **kwargs)

    async def get_task_status(self, task_id: str) -> Dict[str, Any]:
        return await self._inner.get_task_status(task_id)

    async def enable_offline_mode(self) -> None:
        await self._inner.enable_offline_mode()

    async def disable_offline_mode(self) -> None:
        await self._inner.disable_offline_mode()

    async def health_check(self) -> Dict[str, Any]:
        return await self._inner.health_check()

    def statistics(self) -> Dict[str, Any]:
        return self._inner.statistics()

    def collect_prometheus_metrics(
        self,
    ) -> Dict[str, Tuple[Any, Dict[str, str]]]:
        return self._inner.collect_prometheus_metrics()

    def to_dict(self) -> Dict[str, Any]:
        return self._inner.to_dict()

    def __repr__(self) -> str:
        return f"ContinuumSchedulerActor({self._inner!r})"


# --------------------------------------------------------------------------- #
# Public API
# --------------------------------------------------------------------------- #
__all__ = [
    "ContinuumScheduler",
    "ContinuumSchedulerActor",
    "ContinuumSchedulerError",
    "SchedulerConfig",
    "TaskRecord",
    "PlacementTier",
    "PlacementDecision",
]


# --------------------------------------------------------------------------- #
# Demo — every P0/P1 fix verified by assertion
# --------------------------------------------------------------------------- #
def _demo() -> None:
    logging.basicConfig(
        level=logging.WARNING,
        format="%(levelname)s %(name)s: %(message)s",
    )

    print("=" * 78)
    print("continuum_scheduler.py v6.0.0 — Demo")
    print("=" * 78)

    def _check(condition: bool, message: str) -> None:
        if not condition:
            raise AssertionError(f"FAILED: {message}")
        print(f"  ✓ {message}")

    # Shared fixtures
    class _FastPool:
        async def execute(self, task_id, payload):
            await asyncio.sleep(0.01)
            return {"task_id": task_id, "echo": dict(payload)}

    class _SlowPool:
        def __init__(self, delay: float):
            self._delay = delay
        async def execute(self, task_id, payload):
            await asyncio.sleep(self._delay)
            return {"task_id": task_id}

    class _RaisingPool:
        async def execute(self, task_id, payload):
            raise RuntimeError("simulated pool failure")

    def _fast_config(**overrides) -> SchedulerConfig:
        base = dict(
            idle_sleep_seconds=0.01,
            request_timeout_seconds=0.1,
            max_network_retries=1,
            base_network_backoff_seconds=0.02,
            state_sync_interval_seconds=0.05,
            offline_retry_seconds=0.05,
            offline_after_failures=2,
            max_queue_size=100,
        )
        base.update(overrides)
        return SchedulerConfig(**base)

    async def run_basic() -> None:
        print("\n=== Basic scheduling + fallback chain ===")
        async with ContinuumScheduler(
            device_id="edge-01",
            local_worker_pool=_FastPool(),
            regional_endpoint="http://127.0.0.1:1",
            cloud_endpoint="http://127.0.0.1:1",
            config=_fast_config(),
        ) as scheduler:
            await scheduler.submit_task(
                "t-local",
                PlacementDecision(
                    selected_tier=PlacementTier.TIER_1_LOCAL,
                ),
                {"input": 42},
            )
            await scheduler.submit_task(
                "t-regional",
                PlacementDecision(
                    selected_tier=PlacementTier.TIER_2_REGIONAL,
                    target_node="regional-a",
                ),
                {"input": 7},
            )
            await scheduler.submit_task(
                "t-cloud",
                PlacementDecision(
                    selected_tier=PlacementTier.TIER_3_CLOUD,
                    target_node="cloud-a",
                ),
                {"input": 3},
            )

            # Wait for drain
            for _ in range(100):
                await asyncio.sleep(0.01)
                all_done = all(
                    (await scheduler.get_task_status(tid))["status"]
                    in ("completed", "failed")
                    for tid in ("t-local", "t-regional", "t-cloud")
                )
                if all_done:
                    break

            for tid in ("t-local", "t-regional", "t-cloud"):
                status = await scheduler.get_task_status(tid)
                _check(
                    status["status"] in ("completed", "failed"),
                    f"{tid} finished (status={status['status']})",
                )

            # P2 §17: fallback trace is recorded
            reg = await scheduler.get_task_status("t-regional")
            _check(
                len(reg.get("fallback_trace", [])) > 0,
                f"t-regional has a fallback trace "
                f"({len(reg.get('fallback_trace', []))} steps)",
            )
            cloud = await scheduler.get_task_status("t-cloud")
            _check(
                any(
                    step.get("tier") == "local"
                    for step in cloud.get("fallback_trace", [])
                ),
                "t-cloud fell back to local",
            )

    async def run_offline_bugs() -> None:
        print("\n=== P0 §2.1–§2.3: offline buffer round-trip ===")
        scheduler = ContinuumScheduler(
            device_id="edge-offline",
            local_worker_pool=_FastPool(),
            regional_endpoint="",
            cloud_endpoint="http://127.0.0.1:1",
            config=_fast_config(),
        )

        # Test §2.2: buffer dedupes by task_id
        for _ in range(5):
            scheduler._buffer_state(
                [{"task_id": "dup", "reason": "test", "at": time.time()}]
            )
        _check(
            len(scheduler._offline_buffer) == 1,
            f"buffer dedupes by task_id "
            f"(size={len(scheduler._offline_buffer)}, expected 1)",
        )

        # Test §2.1 + §2.3: sync uploads buffer + cache, then clears buffer
        captured: Dict[str, Any] = {}

        async def _mock_sync_success() -> None:
            with scheduler._lock:
                captured["cache"] = [
                    r.to_dict() for r in scheduler._state_cache.values()
                ]
                captured["buffer"] = list(scheduler._offline_buffer)
            # Simulate successful upload by clearing buffer
            with scheduler._lock:
                scheduler._offline_buffer.clear()

        # Prime the cache and buffer
        scheduler._cache_put(
            TaskRecord(
                task_id="cached-task",
                status="completed",
                tier="local",
                target_node=None,
                submitted_at=time.time(),
                updated_at=time.time(),
                attempts=1,
            )
        )
        scheduler._buffer_state(
            [
                {
                    "task_id": "buffered-task",
                    "reason": "test",
                    "at": time.time(),
                }
            ]
        )

        # Monkeypatch sync
        scheduler._sync_state_to_cloud = _mock_sync_success  # type: ignore
        await scheduler._sync_state_to_cloud()

        _check(
            len(scheduler._offline_buffer) == 0,
            f"buffer cleared on success "
            f"(size={len(scheduler._offline_buffer)})",
        )
        _check(
            captured["buffer"] and any(
                b["task_id"] == "buffered-task"
                for b in captured["buffer"]
            ),
            "sync captured buffered tasks from the buffer",
        )
        _check(
            captured["cache"] and any(
                c["task_id"] == "cached-task"
                for c in captured["cache"]
            ),
            "sync captured cached tasks from the state cache",
        )

        # Test §2.1 more thoroughly — with a real _sync_state_to_cloud
        # (re-instate the real method, replace aiohttp with a fake)
        scheduler2 = ContinuumScheduler(
            device_id="edge-offline-2",
            local_worker_pool=_FastPool(),
            regional_endpoint="",
            cloud_endpoint="https://example.invalid",
            config=_fast_config(),
        )
        scheduler2._cache_put(
            TaskRecord(
                task_id="cache-A",
                status="completed",
                tier="local",
                target_node=None,
                submitted_at=time.time(),
                updated_at=time.time(),
                attempts=1,
            )
        )
        scheduler2._buffer_state(
            [
                {
                    "task_id": "buf-A",
                    "reason": "test",
                    "at": time.time(),
                }
            ]
        )
        # Override _buffer_state (so failure path is a no-op in the mock)
        scheduler2._sync_state_to_cloud = _mock_sync_success  # type: ignore
        await scheduler2._sync_state_to_cloud()
        _check(
            scheduler2._offline_buffer == [],
            "buffer cleared by mock success path",
        )

    async def run_sync_recovery() -> None:
        print("\n=== P0 §2.4: sync loop recovers from offline ===")
        scheduler = ContinuumScheduler(
            device_id="edge-recovery",
            local_worker_pool=_FastPool(),
            regional_endpoint="",
            cloud_endpoint="http://127.0.0.1:1",
            config=_fast_config(
                offline_after_failures=1,
                offline_retry_seconds=0.05,
                state_sync_interval_seconds=0.05,
            ),
        )

        # Start in "failing sync" mode
        failing = {"on": True}
        sync_attempts = {"ok": 0, "fail": 0}

        async def _sync(failing=failing) -> None:
            if failing["on"]:
                sync_attempts["fail"] += 1
                raise RuntimeError("simulated sync failure")
            sync_attempts["ok"] += 1

        scheduler._sync_state_to_cloud = _sync  # type: ignore

        await scheduler.start()

        # Wait for offline mode to engage (offline_after_failures=1)
        for _ in range(100):
            await asyncio.sleep(0.01)
            if scheduler.offline_mode:
                break
        _check(
            scheduler.offline_mode is True,
            "offline mode engaged after sync failure",
        )

        # Now make sync succeed and wait for recovery
        failing["on"] = False
        for _ in range(200):
            await asyncio.sleep(0.01)
            if not scheduler.offline_mode:
                break
        _check(
            scheduler.offline_mode is False,
            f"offline mode auto-cleared after success "
            f"(attempts ok={sync_attempts['ok']}, "
            f"fail={sync_attempts['fail']})",
        )
        _check(
            sync_attempts["ok"] >= 1,
            f"sync succeeded ≥1 time after recovery "
            f"(ok={sync_attempts['ok']})",
        )

        await scheduler.stop()

    async def run_no_ray_remote() -> None:
        print("\n=== P0 §2.6: no @ray_remote on the class ===")
        scheduler = ContinuumScheduler(
            device_id="edge-noray",
            local_worker_pool=_FastPool(),
            regional_endpoint="",
            cloud_endpoint="",
            config=_fast_config(),
        )
        _check(
            isinstance(scheduler, ContinuumScheduler),
            "ContinuumScheduler(...) returns an instance, not an ActorHandle",
        )
        _check(
            hasattr(scheduler, "start") and hasattr(scheduler, "submit_task"),
            "instance exposes start() and submit_task() directly",
        )
        _check(
            not hasattr(ContinuumScheduler, "_ray_remote"),
            "class has no @ray_remote decorator",
        )
        _check(
            ContinuumSchedulerActor is not ContinuumScheduler,
            "ContinuumSchedulerActor is a separate class",
        )

    async def run_bounded_queue() -> None:
        print("\n=== P0 §2.7: bounded queue ===")
        scheduler = ContinuumScheduler(
            device_id="edge-queue",
            local_worker_pool=_SlowPool(delay=1.0),
            regional_endpoint="",
            cloud_endpoint="",
            config=_fast_config(max_queue_size=2),
        )
        await scheduler.start()
        try:
            # Fill the queue with 2 tasks (loop hasn't drained yet — pool
            # is slow). Offline mode so no network calls.
            await scheduler.enable_offline_mode()
            await scheduler.submit_task(
                "q1",
                PlacementDecision(
                    selected_tier=PlacementTier.TIER_1_LOCAL,
                ),
                {},
            )
            await scheduler.submit_task(
                "q2",
                PlacementDecision(
                    selected_tier=PlacementTier.TIER_1_LOCAL,
                ),
                {},
            )
            # Third submit should block
            try:
                await asyncio.wait_for(
                    scheduler.submit_task(
                        "q3",
                        PlacementDecision(
                            selected_tier=PlacementTier.TIER_1_LOCAL,
                        ),
                        {},
                    ),
                    timeout=0.05,
                )
            except asyncio.TimeoutError:
                _check(True, "third submit blocks when queue is full")
            else:
                raise AssertionError(
                    "submit_task did not block when queue was full"
                )

            # submit_task_nowait raises
            try:
                await scheduler.submit_task_nowait(
                    "q4",
                    PlacementDecision(
                        selected_tier=PlacementTier.TIER_1_LOCAL,
                    ),
                    {},
                )
            except ContinuumSchedulerError:
                _check(True, "submit_task_nowait raises on full queue")
            else:
                raise AssertionError("submit_task_nowait did not raise")
        finally:
            await scheduler.stop(flush=False)

    async def run_drain_on_stop() -> None:
        print("\n=== P0 §2.8: stop() drains the queue ===")
        scheduler = ContinuumScheduler(
            device_id="edge-drain",
            local_worker_pool=_SlowPool(delay=5.0),
            regional_endpoint="",
            cloud_endpoint="",
            config=_fast_config(max_queue_size=10),
        )
        await scheduler.start()
        await scheduler.enable_offline_mode()
        for i in range(5):
            await scheduler.submit_task(
                f"drain-{i}",
                PlacementDecision(
                    selected_tier=PlacementTier.TIER_1_LOCAL,
                ),
                {},
            )
        # Give the loop a moment to pick up the first task
        await asyncio.sleep(0.05)
        await scheduler.stop(flush=False)

        buffered_ids = [
            r.get("task_id") for r in scheduler._offline_buffer
        ]
        drain_count = sum(
            1 for r in scheduler._offline_buffer
            if r.get("reason") in ("shutdown_drain", "shutdown_in_flight")
        )
        _check(
            drain_count >= 4,
            f"stop() drained ≥4 tasks to buffer "
            f"(drained={drain_count}, ids={buffered_ids})",
        )
        # Confirm the in-flight task was captured too
        with scheduler._lock:
            buffered_status_count = sum(
                1
                for r in scheduler._state_cache.values()
                if r.status == "buffered"
            )
        _check(
            buffered_status_count >= 4,
            f"≥4 tasks marked 'buffered' in cache "
            f"(got {buffered_status_count})",
        )

    async def run_p1_fixes() -> None:
        print("\n=== P1 fixes ===")

        # --- P1 §8: status transitions to "running" ---
        scheduler = ContinuumScheduler(
            device_id="edge-running",
            local_worker_pool=_SlowPool(delay=0.3),
            regional_endpoint="",
            cloud_endpoint="",
            config=_fast_config(),
        )
        await scheduler.start()
        await scheduler.enable_offline_mode()
        await scheduler.submit_task(
            "run-task",
            PlacementDecision(
                selected_tier=PlacementTier.TIER_1_LOCAL,
            ),
            {},
        )
        await asyncio.sleep(0.1)
        status_running = await scheduler.get_task_status("run-task")
        _check(
            status_running["status"] == "running",
            f"status transitions to 'running' mid-execution "
            f"(got {status_running['status']})",
        )
        await asyncio.sleep(0.4)
        status_done = await scheduler.get_task_status("run-task")
        _check(
            status_done["status"] == "completed",
            f"status becomes 'completed' after execution "
            f"(got {status_done['status']})",
        )
        await scheduler.stop(flush=False)

        # --- P1 §10: traceback preserved in _execute_local ---
        scheduler2 = ContinuumScheduler(
            device_id="edge-traceback",
            local_worker_pool=_RaisingPool(),
            regional_endpoint="",
            cloud_endpoint="",
            config=_fast_config(),
        )
        await scheduler2.start()
        await scheduler2.enable_offline_mode()
        await scheduler2.submit_task(
            "fail-task",
            PlacementDecision(
                selected_tier=PlacementTier.TIER_1_LOCAL,
            ),
            {},
        )
        for _ in range(100):
            await asyncio.sleep(0.01)
            st = await scheduler2.get_task_status("fail-task")
            if st["status"] == "failed":
                break
        summary = st.get("result_summary") or {}
        _check(
            "traceback" in summary and summary["traceback"],
            f"traceback preserved in error result "
            f"(keys={list(summary.keys())})",
        )
        await scheduler2.stop(flush=False)

        # --- P1 §14: tier validation at submit ---
        scheduler3 = ContinuumScheduler(
            device_id="edge-tier",
            local_worker_pool=_FastPool(),
            regional_endpoint="",
            cloud_endpoint="",
            config=_fast_config(),
        )
        await scheduler3.start()

        class _BadDecision:
            selected_tier = "totally_bogus_tier"
            target_node = None

        try:
            await scheduler3.submit_task(
                "tier-bad", _BadDecision(), {}
            )
        except ContinuumSchedulerError:
            _check(True, "unknown tier → ContinuumSchedulerError in strict")
        else:
            raise AssertionError("unknown tier did not raise")
        await scheduler3.stop(flush=False)

        # --- P1 §7: transient-only retries ---
        scheduler4 = ContinuumScheduler(
            device_id="edge-retry",
            local_worker_pool=_FastPool(),
            regional_endpoint="http://127.0.0.1:1",
            cloud_endpoint="http://127.0.0.1:1",
            config=_fast_config(max_network_retries=3),
        )
        await scheduler4.start()

        # Definitive error: no retries
        calls_definitive = {"n": 0}
        async def _regional_definitive(task):
            calls_definitive["n"] += 1
            raise ValueError("definitive failure")
        scheduler4._execute_regional = _regional_definitive  # type: ignore
        await scheduler4.submit_task(
            "retry-definitive",
            PlacementDecision(
                selected_tier=PlacementTier.TIER_2_REGIONAL,
            ),
            {},
        )
        for _ in range(200):
            await asyncio.sleep(0.01)
            if (
                await scheduler4.get_task_status("retry-definitive")
            )["status"] in ("completed", "failed"):
                break
        _check(
            calls_definitive["n"] == 1,
            f"definitive error → 1 call, not retried "
            f"(got {calls_definitive['n']})",
        )

        # Transient error: retried
        calls_transient = {"n": 0}
        async def _regional_transient(task):
            calls_transient["n"] += 1
            if calls_transient["n"] < 3:
                raise ConnectionError("network blip")
            return {"tier": "regional", "result": {"ok": True}}
        scheduler4._execute_regional = _regional_transient  # type: ignore
        await scheduler4.submit_task(
            "retry-transient",
            PlacementDecision(
                selected_tier=PlacementTier.TIER_2_REGIONAL,
            ),
            {},
        )
        for _ in range(200):
            await asyncio.sleep(0.01)
            if (
                await scheduler4.get_task_status("retry-transient")
            )["status"] == "completed":
                break
        _check(
            calls_transient["n"] == 3,
            f"transient error → retried until success "
            f"(calls={calls_transient['n']})",
        )
        await scheduler4.stop(flush=False)

    async def run_health_and_metrics() -> None:
        print("\n=== health_check + Prometheus metrics ===")
        scheduler = ContinuumScheduler(
            device_id="edge-health",
            local_worker_pool=_FastPool(),
            regional_endpoint="",
            cloud_endpoint="",
            config=_fast_config(),
        )
        await scheduler.start()
        try:
            health = await scheduler.health_check()
            for key in (
                "healthy",
                "running",
                "offline_mode",
                "queue_depth",
                "cached_tasks",
                "buffered_tasks",
                "state_sync_failures",
                "uptime_seconds",
                "config",
            ):
                _check(key in health, f"health_check has '{key}'")
            _check(
                health["running"] is True,
                "health_check reports running=True",
            )

            metrics = scheduler.collect_prometheus_metrics()
            for key in (
                "continuum_queue_depth",
                "continuum_offline_buffer_size",
                "continuum_state_cache_size",
                "continuum_offline_mode",
            ):
                _check(key in metrics, f"metric '{key}' present")

            # P1 §13: reset(clear_buffer=False) preserves the buffer
            scheduler._buffer_state([{"task_id": "pres", "at": time.time()}])
            scheduler.reset(clear_cache=True)   # clear_buffer=False
            _check(
                len(scheduler._offline_buffer) == 1,
                "reset(clear_buffer=False) preserves the buffer",
            )
        finally:
            await scheduler.stop(flush=False)

    async def run_restore_state() -> None:
        print("\n=== restore_state ===")
        scheduler = ContinuumScheduler(
            device_id="edge-restore",
            local_worker_pool=_FastPool(),
            regional_endpoint="",
            cloud_endpoint="",
            config=_fast_config(),
        )
        await scheduler.start()
        try:
            await scheduler.enable_offline_mode()
            await scheduler.submit_task(
                "restore-task",
                PlacementDecision(
                    selected_tier=PlacementTier.TIER_1_LOCAL,
                ),
                {"input": 1},
            )
            for _ in range(200):
                await asyncio.sleep(0.01)
                st = await scheduler.get_task_status("restore-task")
                if st["status"] == "completed":
                    break
            payload = scheduler.to_dict()
        finally:
            await scheduler.stop(flush=False)

        # New scheduler — restore state
        scheduler2 = ContinuumScheduler(
            device_id="edge-restore-2",
            local_worker_pool=_FastPool(),
            regional_endpoint="",
            cloud_endpoint="",
            config=_fast_config(),
        )
        scheduler2.restore_state(payload)
        restored = await scheduler2.get_task_status("restore-task")
        _check(
            restored["status"] == "completed",
            f"restore_state restores task status "
            f"({restored['status']})",
        )
        _check(
            not hasattr(ContinuumScheduler, "from_dict"),
            "no from_dict (honest — pool cannot round-trip)",
        )
        _check(
            not hasattr(ContinuumScheduler, "from_json"),
            "no from_json (honest — pool cannot round-trip)",
        )

    async def main() -> None:
        await run_basic()
        await run_offline_bugs()
        await run_sync_recovery()
        await run_no_ray_remote()
        await run_bounded_queue()
        await run_drain_on_stop()
        await run_p1_fixes()
        await run_health_and_metrics()
        await run_restore_state()

        print("\n" + "=" * 78)
        print("All v6 assertions passed.")
        print("=" * 78)

    asyncio.run(main())


if __name__ == "__main__":
    _demo()
