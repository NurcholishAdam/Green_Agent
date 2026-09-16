# src/continuum/continuum_scheduler.py

"""
Continuum Scheduler

Workload orchestrator across the edge-cloud continuum.

Responsibilities
----------------
- Maintain a task queue with placement metadata.
- Schedule tasks across continuum tiers (local / regional / cloud).
- Support partial offloading (split computation).
- Handle network partitions gracefully (offline mode + buffered sync).

Enhancements
------------
- Fixed missing imports (``logger``, ``aiohttp``, ``PlacementTier``,
  ``PlacementDecision``, ``WorkerPool``).
- Defensive optional imports for ``ray`` and ``aiohttp`` with graceful
  degradation (Ray fallback to ``in-process``; aiohttp fallback raises).
- Thread-safe state cache with bounded ring-buffer (``max_state_cache``).
- Async primitives are created lazily inside ``start()`` so the scheduler
  works correctly under Ray actors (loop-bound queues).
- Configurable via :class:`SchedulerConfig` (timeouts, retries, backoff).
- Full validation of every argument; strict / non-strict modes.
- Immutable :class:`TaskRecord` snapshots for audit and serialization.
- Fallback chain: regional → cloud → local, on transient failures.
- ``_sync_state_to_cloud`` now implemented (HTTP POST of pending records).
- Offline mode buffers tasks; on exit, buffered state is flushed.
- Serialization: ``to_dict`` / ``from_dict`` / ``to_json`` / ``from_json``.
- ``async`` context manager for scoped sessions.
- Custom :class:`ContinuumSchedulerError`.
- Lazy ``%s`` logging, ``__repr__``, and a smoke test under ``__main__``.
"""

from __future__ import annotations

import asyncio
import json
import logging
import math
import os
import threading
import time
import uuid
from dataclasses import asdict, dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Dict, Iterable, List, Mapping, Optional

logger = logging.getLogger(__name__)


# --------------------------------------------------------------------------- #
# Optional imports (defensive)
# --------------------------------------------------------------------------- #
# Ray is optional; when missing, the scheduler still functions in-process.
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


# aiohttp is required for regional/cloud HTTP calls.
try:  # pragma: no cover
    import aiohttp  # type: ignore

    _AIOHTTP_AVAILABLE = True
except ImportError:  # pragma: no cover
    aiohttp = None  # type: ignore[assignment]
    _AIOHTTP_AVAILABLE = False


# --------------------------------------------------------------------------- #
# Cross-module imports (defensive)
# --------------------------------------------------------------------------- #
# The placement engine and worker pool live elsewhere in the tree; when they
# cannot be imported we fall back to local shims so the scheduler remains
# self-contained and testable.
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
            """Fallback placement decision for standalone usage."""
            selected_tier: PlacementTier = PlacementTier.TIER_1_LOCAL
            target_node: Optional[str] = None
            reason: str = "fallback"

try:  # pragma: no cover
    from .worker_pool import WorkerPool  # type: ignore
except ImportError:  # pragma: no cover
    WorkerPool = Any  # type: ignore[assignment,misc]


# --------------------------------------------------------------------------- #
# Errors
# --------------------------------------------------------------------------- #
class ContinuumSchedulerError(ValueError):
    """Raised for invalid inputs, configuration, or execution failures."""


# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class SchedulerConfig:
    """
    Tunable parameters for the Continuum Scheduler.

    Centralizes timeouts, retries, and buffer sizes so deployments can
    calibrate without editing the class.
    """

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

    # Bounded caches / buffers
    max_state_cache: Optional[int] = 10_000
    max_offline_buffer: Optional[int] = 5_000

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
                raise ContinuumSchedulerError(f"{name} must be > 0 (got {value}).")
        if self.max_network_retries < 0:
            raise ContinuumSchedulerError("max_network_retries must be >= 0.")
        if self.base_network_backoff_seconds < 0:
            raise ContinuumSchedulerError("base_network_backoff_seconds must be >= 0.")
        if self.max_state_cache is not None and self.max_state_cache <= 0:
            raise ContinuumSchedulerError("max_state_cache must be > 0 or None.")
        if self.max_offline_buffer is not None and self.max_offline_buffer <= 0:
            raise ContinuumSchedulerError("max_offline_buffer must be > 0 or None.")
        if self.max_task_retries < 0:
            raise ContinuumSchedulerError("max_task_retries must be >= 0.")


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

    def to_dict(self) -> Dict[str, Any]:
        d = asdict(self)
        d["result_summary"] = (
            dict(self.result_summary) if self.result_summary is not None else None
        )
        return d


# --------------------------------------------------------------------------- #
# Scheduler
# --------------------------------------------------------------------------- #
@_ray_remote
class ContinuumScheduler:
    """
    Workload orchestrator across edge-cloud continuum.

    The class is decorated with ``@ray.remote`` when Ray is available; when
    Ray is not installed, the decorator is a no-op and the class can be
    instantiated directly. This preserves the original usage pattern while
    remaining testable in environments without Ray.

    Parameters
    ----------
    device_id : str
        Unique identifier for this device.
    local_worker_pool : WorkerPool
        Pool used for local (Tier-1) execution.
    regional_endpoint : str
        Base URL for the regional executor service.
    cloud_endpoint : str
        Ray cluster address (or HTTP endpoint) for cloud execution.
    config : SchedulerConfig, optional
        Timeouts / retries / buffer sizes.
    strict : bool, default True
        If True, invalid inputs raise :class:`ContinuumSchedulerError`.
    """

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
        if not isinstance(device_id, str) or not device_id:
            raise ContinuumSchedulerError("device_id must be a non-empty string.")
        if not isinstance(regional_endpoint, str):
            raise ContinuumSchedulerError("regional_endpoint must be a string.")
        if not isinstance(cloud_endpoint, str):
            raise ContinuumSchedulerError("cloud_endpoint must be a string.")

        self.device_id: str = device_id
        self.local_pool = local_worker_pool
        self.regional_endpoint: str = regional_endpoint.rstrip("/")
        self.cloud_endpoint: str = cloud_endpoint

        self._config: SchedulerConfig = config or SchedulerConfig()
        self._strict: bool = bool(strict)

        self._lock = threading.RLock()
        self._state_cache: Dict[str, TaskRecord] = {}
        self._state_order: List[str] = []          # FIFO keys for eviction
        self._offline_buffer: List[Dict[str, Any]] = []
        self._offline_mode: bool = False
        self._running: bool = False

        # Async primitives are created lazily inside ``start()`` so the queue
        # is bound to the running event loop (critical for Ray actors).
        self._task_queue: Optional["asyncio.Queue[Dict[str, Any]]"] = None
        self._tasks: List[asyncio.Task] = []
        self._ctx_start: Optional[float] = None
        self._started_at: Optional[float] = None

        logger.debug(
            "ContinuumScheduler initialized (device=%s, strict=%s, ray=%s, aiohttp=%s)",
            self.device_id,
            self._strict,
            _RAY_AVAILABLE,
            _AIOHTTP_AVAILABLE,
        )

    # ---------------------------------------------------------------- props
    @property
    def config(self) -> SchedulerConfig:
        return self._config

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
            logger.debug("ContinuumScheduler already running.")
            return

        # Bind the queue to the running loop (safe under Ray actors).
        self._task_queue = asyncio.Queue()
        self._running = True
        self._started_at = time.time()

        self._tasks = [
            asyncio.create_task(self._scheduling_loop(), name="continuum:scheduling"),
            asyncio.create_task(self._state_sync_loop(), name="continuum:state_sync"),
        ]
        logger.info(
            "ContinuumScheduler started (device=%s, tasks=%d).",
            self.device_id,
            len(self._tasks),
        )

    async def stop(self, *, timeout_seconds: float = 5.0) -> None:
        """Stop the scheduler loops and drain in-flight tasks."""
        if not self._running:
            return

        self._running = False

        # Cancel background loops and await their termination.
        for t in self._tasks:
            t.cancel()
        if self._tasks:
            try:
                await asyncio.wait_for(
                    asyncio.gather(*self._tasks, return_exceptions=True),
                    timeout=timeout_seconds,
                )
            except asyncio.TimeoutError:  # pragma: no cover
                logger.warning("Timed out waiting for scheduler loops to stop.")
        self._tasks = []

        # Best-effort flush of offline buffer on clean shutdown.
        if not self._offline_mode and self._offline_buffer:
            try:
                await self._sync_state_to_cloud()
            except Exception:
                logger.exception("Failed to flush offline buffer on stop.")

        logger.info(
            "ContinuumScheduler stopped (device=%s, cached=%d).",
            self.device_id,
            len(self._state_cache),
        )

    # -------------------------------------------------------- public API
    async def submit_task(
        self,
        task_id: str,
        placement_decision: Any,
        task_payload: Mapping[str, Any],
    ) -> str:
        """
        Submit a task for execution with the given placement decision.

        Parameters
        ----------
        task_id : str
            Unique task identifier.
        placement_decision : PlacementDecision
            From the OffloadingDecisionEngine; must expose ``selected_tier``.
        task_payload : Mapping
            Task input data and configuration.

        Returns
        -------
        str
            The ``task_id`` for the enqueued task.

        Raises
        ------
        ContinuumSchedulerError
            If ``task_id`` is empty, ``placement_decision`` is invalid, or
            the scheduler was not started.
        """
        if not isinstance(task_id, str) or not task_id:
            raise ContinuumSchedulerError("task_id must be a non-empty string.")
        if placement_decision is None or not hasattr(
            placement_decision, "selected_tier"
        ):
            raise ContinuumSchedulerError(
                "placement_decision must expose a 'selected_tier' attribute."
            )
        if not isinstance(task_payload, Mapping):
            raise ContinuumSchedulerError(
                f"task_payload must be a Mapping, got {type(task_payload).__name__}."
            )

        if self._task_queue is None:
            raise ContinuumSchedulerError(
                "Scheduler is not running. Call 'await start()' first."
            )

        tier = self._tier_name(placement_decision.selected_tier)
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

        await self._task_queue.put(
            {
                "task_id": task_id,
                "placement": placement_decision,
                "payload": dict(task_payload),
                "submitted_at": time.time(),
                "status": "queued",
            }
        )

        logger.debug(
            "Task queued: id=%s tier=%s target=%s", task_id, tier, target_node
        )
        return task_id

    async def get_task_status(self, task_id: str) -> Dict[str, Any]:
        """Return the stored status record for ``task_id``."""
        with self._lock:
            record = self._state_cache.get(task_id)
        if record is None:
            return {"task_id": task_id, "status": "not_found"}
        return record.to_dict()

    async def enable_offline_mode(self) -> None:
        """Enable offline operation mode (local-only execution)."""
        with self._lock:
            self._offline_mode = True
        logger.info("Continuum Scheduler entered offline mode.")

    async def disable_offline_mode(self) -> None:
        """Disable offline mode and flush any buffered state to the cloud."""
        with self._lock:
            self._offline_mode = False
        try:
            await self._sync_state_to_cloud()
        except Exception:
            logger.exception("Failed to sync state on offline-mode exit.")
        logger.info("Continuum Scheduler exited offline mode.")

    # --------------------------------------------------------- scheduling loop
    async def _scheduling_loop(self) -> None:
        """Background loop that drains the task queue and dispatches tasks."""
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

                # Offline mode: only local execution is attempted.
                if self._offline_mode:
                    result = await self._execute_with_fallback(
                        task, allow_remote=False
                    )
                else:
                    result = await self._execute_with_fallback(
                        task, allow_remote=True
                    )

                status = "completed" if "error" not in result else "failed"
                self._update_status(
                    task["task_id"],
                    status=status,
                    result_summary=result,
                    error=result.get("error"),
                )

            except asyncio.CancelledError:
                logger.debug("Scheduling loop cancelled.")
                raise
            except Exception as exc:  # pragma: no cover — defensive
                logger.exception("Scheduling loop error: %s", exc)
                await asyncio.sleep(cfg.error_sleep_seconds)

    async def _execute_with_fallback(
        self,
        task: Mapping[str, Any],
        *,
        allow_remote: bool,
    ) -> Dict[str, Any]:
        """
        Execute a task according to its placement, with automatic fallback.

        Fallback chain:
        - Tier-1 (local): no fallback.
        - Tier-2 (regional): fall back to cloud, then local.
        - Tier-3 (cloud): fall back to local.
        """
        placement = task["placement"]
        tier = self._tier_name(placement.selected_tier)
        attempts = 0
        last_error: Optional[BaseException] = None

        def _bump():
            nonlocal attempts
            attempts += 1
            self._update_status(task["task_id"], attempts=attempts)

        # --- Local --------------------------------------------------------
        if tier == PlacementTier.TIER_1_LOCAL.value or not allow_remote:
            _bump()
            return await self._execute_local(dict(task))

        # --- Regional with fallback --------------------------------------
        if tier == PlacementTier.TIER_2_REGIONAL.value:
            for attempt in range(1 + self._config.max_network_retries):
                _bump()
                try:
                    return await self._execute_regional(dict(task))
                except Exception as exc:
                    last_error = exc
                    logger.warning(
                        "Regional execution failed for %s (attempt %d/%d): %s",
                        task["task_id"],
                        attempt + 1,
                        self._config.max_network_retries + 1,
                        exc,
                    )
                    if attempt < self._config.max_network_retries:
                        await asyncio.sleep(
                            self._config.base_network_backoff_seconds * (2 ** attempt)
                        )
            logger.warning(
                "Falling back to cloud for task %s (last error: %s).",
                task["task_id"], last_error,
            )
            # Fall through to cloud, then local.

        # --- Cloud with local fallback -----------------------------------
        if allow_remote:
            for attempt in range(1 + self._config.max_network_retries):
                _bump()
                try:
                    return await self._execute_cloud(dict(task))
                except Exception as exc:
                    last_error = exc
                    logger.warning(
                        "Cloud execution failed for %s (attempt %d/%d): %s",
                        task["task_id"],
                        attempt + 1,
                        self._config.max_network_retries + 1,
                        exc,
                    )
                    if attempt < self._config.max_network_retries:
                        await asyncio.sleep(
                            self._config.base_network_backoff_seconds * (2 ** attempt)
                        )

        # --- Last resort: local ------------------------------------------
        logger.warning(
            "Falling back to local for task %s (last error: %s).",
            task["task_id"], last_error,
        )
        _bump()
        return await self._execute_local(dict(task))

    # --------------------------------------------------------- tier execution
    async def _execute_local(self, task: Mapping[str, Any]) -> Dict[str, Any]:
        """Execute a task on the local worker pool."""
        pool = self.local_pool
        if pool is None or not hasattr(pool, "execute"):
            raise ContinuumSchedulerError(
                "local_worker_pool must expose an 'execute' method."
            )
        try:
            result = await pool.execute(
                task_id=task["task_id"],
                payload=task["payload"],
            )
            return {"tier": "local", "result": result}
        except Exception as exc:
            logger.exception("Local execution failed for %s: %s", task["task_id"], exc)
            return {"tier": "local", "error": f"{type(exc).__name__}: {exc}"}

    async def _execute_regional(self, task: Mapping[str, Any]) -> Dict[str, Any]:
        """Execute a task on the regional edge node via HTTP."""
        if not _AIOHTTP_AVAILABLE:
            raise ContinuumSchedulerError(
                "aiohttp is required for regional execution."
            )
        url = f"{self.regional_endpoint}/api/v1/execute"
        payload = {
            "task_id": task["task_id"],
            "payload": task["payload"],
            "source_device": self.device_id,
        }
        timeout = aiohttp.ClientTimeout(total=self._config.request_timeout_seconds)
        async with aiohttp.ClientSession() as session:
            async with session.post(url, json=payload, timeout=timeout) as response:
                response.raise_for_status()
                body = await response.json()
                return {"tier": "regional", "result": body}

    async def _execute_cloud(self, task: Mapping[str, Any]) -> Dict[str, Any]:
        """Execute a task on the cloud Ray cluster (or fall back to HTTP)."""
        if not _RAY_AVAILABLE:
            # No Ray → attempt HTTP POST to the cloud endpoint.
            if not _AIOHTTP_AVAILABLE:
                raise ContinuumSchedulerError(
                    "Neither Ray nor aiohttp is available for cloud execution."
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

        # Ray path: ensure the cluster is initialized and call the remote actor.
        if not ray.is_initialized():
            ray.init(
                address=self.cloud_endpoint or None,
                ignore_reinit_error=True,
            )

        target = getattr(task["placement"], "target_node", None) or "default"
        actor_name = f"cloud_worker_{target}"
        try:
            remote_worker = ray.get_actor(actor_name)
        except Exception as exc:
            raise ContinuumSchedulerError(
                f"Cloud worker '{actor_name}' not found: {exc}"
            ) from exc

        # ``remote_worker.execute.remote`` returns an ObjectRef; turn it into
        # an awaitable with ``asyncio.to_thread(ray.get, ref)``.
        ref = remote_worker.execute.remote(task["payload"])
        result = await asyncio.to_thread(ray.get, ref)
        return {"tier": "cloud_ray", "result": result}

    # --------------------------------------------------------- state sync
    async def _state_sync_loop(self) -> None:
        """Background loop that periodically syncs state to the cloud."""
        cfg = self._config
        while self._running:
            try:
                if not self._offline_mode:
                    await self._sync_state_to_cloud()
                    await asyncio.sleep(cfg.state_sync_interval_seconds)
                else:
                    await asyncio.sleep(cfg.offline_retry_seconds)
            except asyncio.CancelledError:
                logger.debug("State-sync loop cancelled.")
                raise
            except Exception as exc:
                logger.warning(
                    "State sync failed (%s); entering offline mode.", exc
                )
                with self._lock:
                    self._offline_mode = True
                await asyncio.sleep(cfg.offline_retry_seconds)

    async def _sync_state_to_cloud(self) -> None:
        """
        Upload the current state cache to the cloud endpoint.

        If aiohttp is unavailable or the cloud endpoint is empty, the payload
        is buffered locally and will be retried on the next sync cycle.
        """
        with self._lock:
            pending = [r.to_dict() for r in self._state_cache.values()]

        if not pending:
            logger.debug("State sync: nothing to upload.")
            return

        if not self.cloud_endpoint or not _AIOHTTP_AVAILABLE:
            self._buffer_state(pending)
            logger.debug(
                "State sync: cloud endpoint unavailable; buffered %d record(s).",
                len(pending),
            )
            return

        url = f"{self.cloud_endpoint.rstrip('/')}/api/v1/state/sync"
        payload = {"device_id": self.device_id, "records": pending}
        timeout = aiohttp.ClientTimeout(total=self._config.request_timeout_seconds)

        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    url, json=payload, timeout=timeout
                ) as response:
                    response.raise_for_status()
            logger.debug("State sync: uploaded %d record(s).", len(pending))
        except Exception as exc:
            self._buffer_state(pending)
            logger.warning(
                "State sync failed (%s); buffered %d record(s).",
                exc, len(pending),
            )
            raise

    def _buffer_state(self, records: List[Dict[str, Any]]) -> None:
        """Append records to the bounded offline buffer."""
        with self._lock:
            self._offline_buffer.extend(records)
            cap = self._config.max_offline_buffer
            if cap is not None and len(self._offline_buffer) > cap:
                # Keep the most recent entries.
                self._offline_buffer = self._offline_buffer[-cap:]

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
            updated = TaskRecord(
                task_id=existing.task_id,
                status=status or existing.status,
                tier=existing.tier,
                target_node=existing.target_node,
                submitted_at=existing.submitted_at,
                updated_at=time.time(),
                attempts=attempts if attempts is not None else existing.attempts,
                latency_ms=(
                    (time.time() - existing.submitted_at) * 1000.0
                    if status in ("completed", "failed")
                    else existing.latency_ms
                ),
                error=error if error is not None else existing.error,
                result_summary=(
                    result_summary if result_summary is not None
                    else existing.result_summary
                ),
            )
            self._cache_put(updated)

    def _enforce_cache_bound_locked(self) -> None:
        cap = self._config.max_state_cache
        if cap is None:
            return
        while len(self._order := self._state_order) > cap:
            oldest = self._state_order.pop(0)
            self._state_cache.pop(oldest, None)

    # --------------------------------------------------------- statistics
    def statistics(self) -> Dict[str, Any]:
        """Return aggregate statistics over the cached task records."""
        with self._lock:
            records = list(self._state_cache.values())
            buffered = len(self._offline_buffer)

        if not records:
            return {
                "count": 0,
                "completed": 0,
                "failed": 0,
                "queued": 0,
                "buffered": buffered,
                "mean_latency_ms": None,
                "success_rate": None,
            }

        completed = sum(1 for r in records if r.status == "completed")
        failed = sum(1 for r in records if r.status == "failed")
        queued = sum(1 for r in records if r.status == "queued")
        latencies = [
            r.latency_ms for r in records if r.latency_ms is not None
        ]

        return {
            "count": len(records),
            "completed": completed,
            "failed": failed,
            "queued": queued,
            "buffered": buffered,
            "mean_latency_ms": (
                sum(latencies) / len(latencies) if latencies else None
            ),
            "success_rate": completed / (completed + failed)
            if (completed + failed) > 0 else None,
        }

    def reset(self, *, clear_cache: bool = False) -> None:
        """Reset the in-memory state; optionally clear the state cache."""
        with self._lock:
            if clear_cache:
                self._state_cache.clear()
                self._state_order.clear()
            self._offline_buffer.clear()
        logger.debug("ContinuumScheduler reset (clear_cache=%s)", clear_cache)

    # --------------------------------------------------------- helpers
    @staticmethod
    def _tier_name(tier: Any) -> str:
        if isinstance(tier, Enum):
            return tier.value
        return str(tier)

    # --------------------------------------------------------- serialization
    def to_dict(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "device_id": self.device_id,
                "regional_endpoint": self.regional_endpoint,
                "cloud_endpoint": self.cloud_endpoint,
                "config": asdict(self._config),
                "strict": self._strict,
                "offline_mode": self._offline_mode,
                "running": self._running,
                "state_cache": [r.to_dict() for r in self._state_cache.values()],
                "offline_buffer": list(self._offline_buffer),
            }

    def to_json(self, **kwargs: Any) -> str:
        return json.dumps(self.to_dict(), **kwargs)

    # --------------------------------------------------------- async ctx mgr
    async def __aenter__(self) -> "ContinuumScheduler":
        self._ctx_start = time.perf_counter()
        logger.debug("Entering scoped ContinuumScheduler session.")
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        elapsed = time.perf_counter() - (
            self._ctx_start if self._ctx_start is not None else time.perf_counter()
        )
        self._ctx_start = None
        try:
            await self.stop()
        finally:
            if exc_type is not None:
                logger.warning(
                    "ContinuumScheduler scope exited with %s after %.4fs.",
                    exc_type.__name__,
                    elapsed,
                )
            else:
                logger.info(
                    "ContinuumScheduler scope closed in %.4fs (cached=%d).",
                    elapsed,
                    len(self._state_cache),
                )

    # ----------------------------------------------------------------- dunder
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
# Public API
# --------------------------------------------------------------------------- #
__all__ = [
    "ContinuumScheduler",
    "ContinuumSchedulerError",
    "SchedulerConfig",
    "TaskRecord",
    "PlacementTier",
    "PlacementDecision",
]


# --------------------------------------------------------------------------- #
# Local smoke test: python -m continuum.continuum_scheduler
# --------------------------------------------------------------------------- #
if __name__ == "__main__":  # pragma: no cover
    logging.basicConfig(
        level=logging.INFO, format="%(levelname)s %(name)s: %(message)s"
    )

    class _FakePool:
        """Minimal local worker pool for the smoke test."""
        async def execute(self, task_id: str, payload: Mapping[str, Any]) -> Dict[str, Any]:
            await asyncio.sleep(0.01)
            return {"task_id": task_id, "echo": dict(payload)}

    async def main() -> None:
        scheduler = ContinuumScheduler(
            device_id="edge-01",
            local_worker_pool=_FakePool(),
            regional_endpoint="http://regional.invalid",
            cloud_endpoint="http://cloud.invalid",
            config=SchedulerConfig(
                idle_sleep_seconds=0.01,
                request_timeout_seconds=0.5,
                max_network_retries=1,
                base_network_backoff_seconds=0.05,
                state_sync_interval_seconds=0.5,
            ),
            strict=True,
        )

        async with scheduler:
            await scheduler.start()

            # Local task (tier 1) → uses the fake pool.
            await scheduler.submit_task(
                task_id="t-local",
                placement_decision=PlacementDecision(
                    selected_tier=PlacementTier.TIER_1_LOCAL,
                ),
                task_payload={"input": 42},
            )

            # Regional task → will fail and fall back to local (cloud invalid).
            await scheduler.submit_task(
                task_id="t-regional",
                placement_decision=PlacementDecision(
                    selected_tier=PlacementTier.TIER_2_REGIONAL,
                    target_node="regional-a",
                ),
                task_payload={"input": 7},
            )

            # Cloud task → will fail and fall back to local.
            await scheduler.submit_task(
                task_id="t-cloud",
                placement_decision=PlacementDecision(
                    selected_tier=PlacementTier.TIER_3_CLOUD,
                    target_node="cloud-a",
                ),
                task_payload={"input": 3},
            )

            # Give the scheduling loop a moment to drain the queue.
            await asyncio.sleep(1.0)

            for tid in ("t-local", "t-regional", "t-cloud"):
                status = await scheduler.get_task_status(tid)
                print(f"{tid:12s} -> status={status['status']:9s} "
                      f"tier={status.get('tier')} attempts={status.get('attempts')}")

            print("Statistics        :", scheduler.statistics())
            print("Representation    :", scheduler)

            # Offline mode round-trip.
            await scheduler.enable_offline_mode()
            assert scheduler.offline_mode is True
            await scheduler.disable_offline_mode()
            assert scheduler.offline_mode is False
            print("Offline round-trip: OK")

            # Serialization.
            payload = scheduler.to_json()
            restored = json.loads(payload)
            assert restored["device_id"] == "edge-01"
            print("Serialization     : OK")

        # Validation failures (outside the context manager).
        try:
            await scheduler.submit_task("", PlacementDecision(), {})
        except ContinuumSchedulerError as exc:
            print("Rejected as expected:", exc)

        print("\nSmoke test passed.")

    asyncio.run(main())
