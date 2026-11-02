"""
State Buffer (Enhanced v2)
==========================

Quantum-classical state buffer.

Responsibilities:
  - Buffer ThoughtSignature during quantum state collapse
  - Translate quantum measurement outcomes to classical tokens
  - Preserve meta-cognitive context across the boundary
  - Manage buffer queue with priority scheduling and coherence expiry

Original API preserved:
    buffer = StateBuffer(
        max_buffer_size=1000,
        coherence_timeout_ms=100.0,
        priority_queue=True,
    )
    buffer_id = await buffer.buffer_state(sig, amps, ctx)
    b = await buffer.get_buffered_state(buffer_id)
    await buffer.complete_state(buffer_id, outcomes, tokens)
    await buffer.mark_decohered(buffer_id)
    b = await buffer.get_next_for_execution()
    stats = await buffer.get_buffer_stats()

v2 fixes (see analysis):
  * BufferedState declares the measurement/completion fields.
  * Real priority queue (heapq) honoring priority_queue=True.
  * Amplitude validation: 1-D, power-of-2, finite, non-zero norm.
  * Stale queue entries skipped; no more head-of-line stalls.
  * Executing states never evicted; BufferFullError when all in-flight.
  * Lifetime decohered / failed / evicted / expired / reclaimed counters.
  * O(1) _completed eviction (OrderedDict).
  * secrets.token_hex IDs — no more timestamp collisions.
  * Coherence expiry enforced in get_next_for_execution.
  * Thread-safe (RLock) across all state mutations.
  * Loop-evict to cap; optional reject policy on overflow.
  * Eviction history with reasons.
  * Frozen dataclasses; MappingProxyType for metadata.
  * Idempotent complete_state; DuplicateBufferError.
  * reclaim_stalled() for zombie executing states.
  * StateBufferConfig; MetricsRecorder; health(); UTC datetimes.
  * Structured logging; BufferStatus enum; enumeration APIs.
"""

from __future__ import annotations

import asyncio
import hashlib
import heapq
import logging
import math
import secrets
import threading
import time
from collections import OrderedDict, deque
from dataclasses import dataclass, field, replace, asdict
from datetime import datetime, timedelta, timezone
from enum import Enum
from types import MappingProxyType
from typing import (
    Any, Deque, Dict, List, Mapping, Optional, Sequence, Tuple,
)

import numpy as np

logger = logging.getLogger(__name__)

__version__ = "2.0.0"


# =============================================================================
# Helpers
# =============================================================================

def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _ensure_utc(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt


def _freeze_mapping(m: Any) -> Mapping[str, Any]:
    if m is None:
        return MappingProxyType({})
    if isinstance(m, MappingProxyType):
        return m
    if not isinstance(m, Mapping):
        raise TypeError(
            f"expected Mapping, got {type(m).__name__}"
        )
    return MappingProxyType(dict(m))


# =============================================================================
# Errors
# =============================================================================

class StateBufferError(Exception):
    """Base class for StateBuffer errors."""


class BufferNotFoundError(StateBufferError):
    pass


class DuplicateBufferError(StateBufferError):
    pass


class BufferFullError(StateBufferError):
    """Buffer is full and no state can be evicted (all executing)."""


class StateBufferValidationError(StateBufferError, ValueError):
    pass


class InvalidTransitionError(StateBufferError):
    """Attempted state transition is not legal for the current status."""


# =============================================================================
# Status enum
# =============================================================================

class BufferStatus(str, Enum):
    QUEUED = "queued"
    EXECUTING = "executing"
    COMPLETED = "completed"
    FAILED = "failed"
    DECOHERED = "decohered"
    EVICTED = "evicted"
    EXPIRED = "expired"


# =============================================================================
# Config
# =============================================================================

@dataclass(frozen=True)
class StateBufferConfig:
    max_buffer_size: int = 1000
    default_coherence_timeout_ms: float = 100.0
    max_completed_cache: int = 10_000
    max_evicted_history: int = 1000
    enable_priority_queue: bool = True
    # "evict" — drop the oldest queued state when full.
    # "reject" — raise BufferFullError instead of evicting.
    overflow_policy: str = "evict"
    # If an executing state has been executing for longer than this and
    # reclaim_stalled() is called, it is re-queued.
    executing_timeout_s: float = 120.0
    # How long to keep an evicted/expired state ID in the history deque.
    eviction_history_ttl_s: float = 3600.0
    enable_background_sweeper: bool = False
    background_sweep_interval_s: float = 5.0

    def __post_init__(self) -> None:
        if self.max_buffer_size <= 0:
            raise ValueError("max_buffer_size must be > 0")
        if self.default_coherence_timeout_ms <= 0:
            raise ValueError("default_coherence_timeout_ms must be > 0")
        if self.overflow_policy not in ("evict", "reject"):
            raise ValueError(
                "overflow_policy must be 'evict' or 'reject'"
            )
        if self.executing_timeout_s <= 0:
            raise ValueError("executing_timeout_s must be > 0")

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


# =============================================================================
# Metrics
# =============================================================================

class MetricsRecorder:
    def __init__(self) -> None:
        self.counters: Dict[str, float] = {}
        self.histograms: Dict[str, List[float]] = {}
        self._lock = threading.RLock()

    @staticmethod
    def _key(name: str, labels: Mapping[str, Any]) -> str:
        if not labels:
            return name
        parts = ",".join(f'{k}="{v}"' for k, v in sorted(labels.items()))
        return f"{name}{{{parts}}}"

    def inc(self, name: str, value: float = 1.0, **labels: Any) -> None:
        with self._lock:
            k = self._key(name, labels)
            self.counters[k] = self.counters.get(k, 0.0) + value

    def observe(self, name: str, value: float, **labels: Any) -> None:
        with self._lock:
            k = self._key(name, labels)
            self.histograms.setdefault(k, []).append(float(value))

    def snapshot(self) -> Dict[str, Any]:
        with self._lock:
            hist: Dict[str, Dict[str, float]] = {}
            for k, vals in self.histograms.items():
                if not vals:
                    continue
                s = sorted(vals)
                hist[k] = {
                    "count": len(vals),
                    "mean": sum(vals) / len(vals),
                    "p50": s[len(s) // 2],
                    "p99": s[min(len(s) - 1, int(len(s) * 0.99))],
                    "max": s[-1],
                }
            return {
                "counters": dict(self.counters),
                "histograms": hist,
            }


# =============================================================================
# Data classes (frozen)
# =============================================================================

@dataclass(frozen=True)
class QuantumState:
    state_id: str
    amplitudes: np.ndarray
    qubit_count: int
    created_at: datetime
    coherence_time_ms: float
    thought_signature: str
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not isinstance(self.amplitudes, np.ndarray):
            raise StateBufferValidationError(
                "amplitudes must be np.ndarray"
            )
        if self.amplitudes.ndim != 1:
            raise StateBufferValidationError(
                f"amplitudes must be 1-D, got shape {self.amplitudes.shape}"
            )
        if self.qubit_count < 0:
            raise StateBufferValidationError("qubit_count must be >= 0")
        object.__setattr__(self, "created_at", _ensure_utc(self.created_at))
        if not isinstance(self.metadata, MappingProxyType):
            object.__setattr__(
                self, "metadata", _freeze_mapping(self.metadata),
            )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "state_id": self.state_id,
            "amplitudes_shape": list(self.amplitudes.shape),
            "amplitudes_dtype": str(self.amplitudes.dtype),
            "qubit_count": self.qubit_count,
            "created_at": self.created_at.isoformat(),
            "coherence_time_ms": self.coherence_time_ms,
            "thought_signature": self.thought_signature,
            "metadata": dict(self.metadata),
        }


@dataclass(frozen=True)
class BufferedState:
    buffer_id: str
    quantum_state: QuantumState
    classical_context: Mapping[str, Any]
    queued_at: datetime
    expected_execution_time: Optional[datetime] = None
    status: str = BufferStatus.QUEUED.value
    priority: int = 0

    # Completion / failure fields (§1.1).
    measurement_outcomes: Tuple[int, ...] = ()
    classical_tokens: Tuple[str, ...] = ()
    completed_at: Optional[datetime] = None
    failure_reason: Optional[str] = None

    # Lifecycle timestamps for reclaim / expiry sweeps (§2.12).
    executing_started_at: Optional[datetime] = None
    evicted_at: Optional[datetime] = None
    eviction_reason: Optional[str] = None

    def __post_init__(self) -> None:
        object.__setattr__(
            self, "queued_at", _ensure_utc(self.queued_at),
        )
        if self.expected_execution_time is not None:
            object.__setattr__(
                self, "expected_execution_time",
                _ensure_utc(self.expected_execution_time),
            )
        if self.completed_at is not None:
            object.__setattr__(
                self, "completed_at", _ensure_utc(self.completed_at),
            )
        if self.executing_started_at is not None:
            object.__setattr__(
                self, "executing_started_at",
                _ensure_utc(self.executing_started_at),
            )
        if not isinstance(self.classical_context, MappingProxyType):
            object.__setattr__(
                self, "classical_context",
                _freeze_mapping(self.classical_context),
            )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "buffer_id": self.buffer_id,
            "quantum_state": self.quantum_state.to_dict(),
            "classical_context": dict(self.classical_context),
            "queued_at": self.queued_at.isoformat(),
            "expected_execution_time": (
                self.expected_execution_time.isoformat()
                if self.expected_execution_time else None
            ),
            "status": self.status,
            "priority": self.priority,
            "measurement_outcomes": list(self.measurement_outcomes),
            "classical_tokens": list(self.classical_tokens),
            "completed_at": (
                self.completed_at.isoformat()
                if self.completed_at else None
            ),
            "failure_reason": self.failure_reason,
            "executing_started_at": (
                self.executing_started_at.isoformat()
                if self.executing_started_at else None
            ),
            "evicted_at": (
                self.evicted_at.isoformat() if self.evicted_at else None
            ),
            "eviction_reason": self.eviction_reason,
        }

    def deadline(self) -> datetime:
        """Coherence expiry deadline, derived from queued_at."""
        return self.queued_at + timedelta(
            milliseconds=float(self.quantum_state.coherence_time_ms),
        )

    def is_expired(self, at: Optional[datetime] = None) -> bool:
        return (at or _utcnow()) > self.deadline()


# =============================================================================
# Amplitude validation
# =============================================================================

def _validate_and_normalise(amplitudes: Any) -> Tuple[np.ndarray, int]:
    """
    Validate and canonicalise the amplitude vector.

    Returns (amplitudes_complex128, qubit_count).
    Raises StateBufferValidationError on any structural problem.
    """
    if amplitudes is None:
        raise StateBufferValidationError("amplitudes must not be None")
    arr = np.asarray(amplitudes, dtype=np.complex128)
    if arr.ndim != 1:
        raise StateBufferValidationError(
            f"amplitudes must be 1-D, got shape {arr.shape}"
        )
    n = int(arr.shape[0])
    if n == 0:
        raise StateBufferValidationError("amplitudes must not be empty")
    if n & (n - 1):
        raise StateBufferValidationError(
            f"amplitudes length {n} is not a power of 2"
        )
    probs = np.abs(arr) ** 2
    total = float(probs.sum())
    if total <= 0 or not math.isfinite(total):
        raise StateBufferValidationError(
            "amplitudes have zero or non-finite total probability"
        )
    # Make the stored array read-only to defend against caller aliasing.
    canonical = np.ascontiguousarray(arr, dtype=np.complex128).copy()
    canonical.setflags(write=False)
    return canonical, int(n.bit_length() - 1)


# =============================================================================
# The Enhanced StateBuffer
# =============================================================================

class StateBuffer:
    """
    Quantum-classical state buffer.

    Thread-safe: all public methods take an internal ``RLock``. Async is
    for interface compatibility with ``QuantumExecutionMonitor`` — none of
    the current methods yield, so operations are effectively synchronous.
    """

    def __init__(
        self,
        max_buffer_size: int = 1000,
        coherence_timeout_ms: float = 100.0,
        priority_queue: bool = True,
        *,
        config: Optional[StateBufferConfig] = None,
        metrics: Optional[MetricsRecorder] = None,
    ) -> None:
        # Backward-compatible positional args override config defaults.
        cfg = config or StateBufferConfig()
        if (max_buffer_size != cfg.max_buffer_size
                or coherence_timeout_ms
                    != cfg.default_coherence_timeout_ms
                or priority_queue != cfg.enable_priority_queue):
            cfg = replace(
                cfg,
                max_buffer_size=int(max_buffer_size),
                default_coherence_timeout_ms=float(coherence_timeout_ms),
                enable_priority_queue=bool(priority_queue),
            )
            cfg.__post_init__()
        self.config = cfg
        self.max_buffer_size = cfg.max_buffer_size
        self.coherence_timeout = cfg.default_coherence_timeout_ms
        self.priority_queue = cfg.enable_priority_queue

        self._lock = threading.RLock()
        self._buffer: Dict[str, BufferedState] = {}
        # Priority queue entries: (-priority, queued_mono, seq, buffer_id)
        self._pq: List[Tuple[int, float, int, str]] = []
        self._pq_seq: int = 0
        # FIFO queue when priority_queue is disabled.
        self._fifo: Deque[str] = deque()
        # Set of buffer IDs cancelled (complete/decohere) whose queue
        # entries remain — cheap O(1) skip on dequeue.
        self._cancelled_ids: set[str] = set()

        # Completed states.
        self._completed: "OrderedDict[str, BufferedState]" = OrderedDict()

        # Lifetime counters (§1.6).
        self._total_buffered: int = 0
        self._decohered_count: int = 0
        self._failed_count: int = 0
        self._evicted_count: int = 0
        self._expired_count: int = 0
        self._reclaimed_count: int = 0
        self._completed_count: int = 0

        # Eviction history (§2.2).
        self._recently_evicted: Deque[
            Tuple[str, datetime, str]
        ] = deque(maxlen=cfg.max_evicted_history)

        # Metrics.
        self._metrics = metrics or MetricsRecorder()

        # Lifecycle.
        self._closed = False
        self._started_mono = time.monotonic()
        self._sweeper_task: Optional[asyncio.Task] = None
        self._running = False

    # ------------------------------------------------------------------ lifecycle
    async def __aenter__(self) -> "StateBuffer":
        if self.config.enable_background_sweeper:
            await self.start()
        return self

    async def __aexit__(self, *exc_info: Any) -> None:
        await self.close()

    async def start(self) -> None:
        if self._running:
            return
        self._running = True
        if self.config.enable_background_sweeper:
            self._sweeper_task = asyncio.create_task(
                self._sweeper_loop(), name="state-buffer-sweeper",
            )
        logger.info(
            "StateBuffer started",
            extra={"max_buffer_size": self.max_buffer_size},
        )

    async def close(self) -> None:
        self._closed = True
        self._running = False
        task = self._sweeper_task
        self._sweeper_task = None
        if task is not None:
            task.cancel()
            try:
                await task
            except (asyncio.CancelledError, Exception):
                pass
        logger.info("StateBuffer closed")

    async def _sweeper_loop(self) -> None:
        interval = self.config.background_sweep_interval_s
        while self._running:
            try:
                await asyncio.sleep(interval)
                await self.expire_overdue()
                await self.reclaim_stalled(
                    self.config.executing_timeout_s,
                )
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("state-buffer sweeper error")
                await asyncio.sleep(1.0)

    # ------------------------------------------------------------------ buffer
    async def buffer_state(
        self,
        thought_signature: str,
        quantum_state: np.ndarray,
        classical_context: Mapping[str, Any],
        *,
        priority: Optional[int] = None,
        coherence_timeout_ms: Optional[float] = None,
    ) -> str:
        """
        Buffer a quantum state.

        ``priority`` (0-10, higher wins) is taken from the argument first,
        then ``classical_context["priority"]``, then 0.
        """
        if self._closed:
            raise StateBufferError("buffer is closed")
        if not isinstance(thought_signature, str):
            raise StateBufferValidationError(
                "thought_signature must be a string"
            )

        amplitudes, qubit_count = _validate_and_normalise(quantum_state)
        ctx = _freeze_mapping(classical_context)

        # Priority resolution.
        if priority is None:
            raw = ctx.get("priority", 0) if isinstance(ctx, Mapping) else 0
            try:
                priority = int(raw)
            except (TypeError, ValueError):
                priority = 0
        priority = max(0, min(10, int(priority)))

        # Coherence timeout override.
        coh_ms = (
            float(coherence_timeout_ms)
            if coherence_timeout_ms is not None
            else float(self.coherence_timeout)
        )
        if coh_ms <= 0:
            raise StateBufferValidationError(
                "coherence_timeout_ms must be > 0"
            )

        buffer_id = self._generate_buffer_id()
        state_id = self._generate_state_id()
        now = _utcnow()

        # Expected execution time comes from an explicit hint if supplied,
        # otherwise it is None (§3.3 — was a duplicate of queued_at).
        expected = None
        try:
            wait_ms = float(ctx.get("estimated_wait_ms", 0) or 0)
        except (TypeError, ValueError):
            wait_ms = 0.0
        if wait_ms > 0:
            expected = now + timedelta(milliseconds=wait_ms)

        q_state = QuantumState(
            state_id=state_id,
            amplitudes=amplitudes,
            qubit_count=qubit_count,
            created_at=now,
            coherence_time_ms=coh_ms,
            thought_signature=thought_signature,
            metadata=ctx,
        )
        buffered = BufferedState(
            buffer_id=buffer_id,
            quantum_state=q_state,
            classical_context=ctx,
            queued_at=now,
            expected_execution_time=expected,
            status=BufferStatus.QUEUED.value,
            priority=priority,
        )

        with self._lock:
            if buffer_id in self._buffer:
                # Astronomically unlikely with secrets.token_hex, but
                # check anyway — silent overwrite is worse than raising.
                raise DuplicateBufferError(
                    f"buffer_id collision: {buffer_id}"
                )
            self._buffer[buffer_id] = buffered
            self._total_buffered += 1
            self._enqueue_locked(buffered)

            # Enforce capacity (§2.1).
            overflowed = False
            while len(self._buffer) > self.max_buffer_size:
                try:
                    self._evict_oldest_locked()
                except BufferFullError:
                    if self.config.overflow_policy == "reject":
                        # Roll back this insert before raising.
                        self._buffer.pop(buffer_id, None)
                        self._total_buffered -= 1
                        self._cancelled_ids.add(buffer_id)
                        raise
                    # Can't evict — allow the buffer to overflow this cycle.
                    overflowed = True
                    break

        self._metrics.inc("buffer_state_total")
        self._metrics.observe(
            "buffer_state_priority", float(priority),
        )
        if overflowed:
            self._metrics.inc("buffer_overflows")
            logger.warning(
                "buffer overflowed (all in-flight)",
                extra={
                    "buffer_id": buffer_id,
                    "size": len(self._buffer),
                    "max": self.max_buffer_size,
                },
            )
        logger.debug(
            "state buffered",
            extra={
                "buffer_id": buffer_id,
                "priority": priority,
                "qubit_count": qubit_count,
            },
        )
        return buffer_id

    # ------------------------------------------------------------------ retrieve
    async def get_buffered_state(
        self, buffer_id: str,
    ) -> Optional[BufferedState]:
        with self._lock:
            return self._buffer.get(buffer_id)

    async def get_completed_state(
        self, buffer_id: str,
    ) -> Optional[BufferedState]:
        with self._lock:
            return self._completed.get(buffer_id)

    async def list_queued(self) -> List[BufferedState]:
        with self._lock:
            return [
                b for b in self._buffer.values()
                if b.status == BufferStatus.QUEUED.value
            ]

    async def list_by_status(self, status: str) -> List[BufferedState]:
        with self._lock:
            return [
                b for b in self._buffer.values() if b.status == status
            ]

    def get_eviction_reason(
        self, buffer_id: str,
    ) -> Optional[str]:
        with self._lock:
            for bid, _, reason in self._recently_evicted:
                if bid == buffer_id:
                    return reason
        return None

    # ------------------------------------------------------------------ dequeue
    async def get_next_for_execution(
        self,
    ) -> Optional[BufferedState]:
        """
        Dequeue the next eligible state for execution.

        Skips cancelled / stale / expired entries (§1.4, §1.9).
        """
        with self._lock:
            now = _utcnow()
            while True:
                buffer_id = self._pop_next_id_locked()
                if buffer_id is None:
                    return None

                # Cancelled (complete/decohere happened first)?
                if buffer_id in self._cancelled_ids:
                    self._cancelled_ids.discard(buffer_id)
                    continue

                buffered = self._buffer.get(buffer_id)
                if buffered is None:
                    continue
                if buffered.status != BufferStatus.QUEUED.value:
                    continue

                # Coherence expiry (§1.9).
                if buffered.is_expired(now):
                    self._expire_locked(buffer_id, buffered)
                    continue

                executing = replace(
                    buffered,
                    status=BufferStatus.EXECUTING.value,
                    executing_started_at=now,
                )
                self._buffer[buffer_id] = executing
                self._metrics.observe(
                    "queue_lag_ms",
                    (now - buffered.queued_at).total_seconds() * 1000.0,
                )
                return executing

    # ------------------------------------------------------------------ complete
    async def complete_state(
        self,
        buffer_id: str,
        measurement_outcomes: Sequence[int],
        classical_tokens: Sequence[str],
    ) -> Optional[BufferedState]:
        """
        Mark a state completed. Idempotent (§2.11).
        """
        with self._lock:
            # Idempotent: if already completed, return the existing record.
            existing = self._completed.get(buffer_id)
            if existing is not None:
                self._metrics.inc("complete_idempotent")
                return existing

            buffered = self._buffer.get(buffer_id)
            if buffered is None:
                raise BufferNotFoundError(
                    f"buffer ID {buffer_id!r} not found"
                )
            if buffered.status not in (
                BufferStatus.QUEUED.value,
                BufferStatus.EXECUTING.value,
            ):
                raise InvalidTransitionError(
                    f"cannot complete buffer in status {buffered.status!r}"
                )

            outcomes = tuple(int(x) for x in measurement_outcomes)
            tokens = tuple(str(x) for x in classical_tokens)

            completed = replace(
                buffered,
                status=BufferStatus.COMPLETED.value,
                measurement_outcomes=outcomes,
                classical_tokens=tokens,
                completed_at=_utcnow(),
            )
            self._completed[completed.buffer_id] = completed
            self._completed.move_to_end(completed.buffer_id)
            while len(self._completed) > self.config.max_completed_cache:
                self._completed.popitem(last=False)

            del self._buffer[completed.buffer_id]
            self._cancelled_ids.add(completed.buffer_id)
            self._completed_count += 1

        self._metrics.inc("complete_state_total")
        self._metrics.observe("completed_outcomes", len(outcomes))
        logger.debug(
            "state completed",
            extra={
                "buffer_id": buffer_id,
                "n_outcomes": len(outcomes),
                "n_tokens": len(tokens),
            },
        )
        return completed

    async def mark_decohered(
        self, buffer_id: str, reason: str = "decoherence",
    ) -> bool:
        """
        Mark a state decohered. Returns True if applied (§2.10).
        """
        with self._lock:
            buffered = self._buffer.get(buffer_id)
            if buffered is None:
                self._metrics.inc("mark_decohered_miss")
                logger.warning(
                    "mark_decohered: buffer not found",
                    extra={"buffer_id": buffer_id},
                )
                return False
            self._decohered_count += 1
            del self._buffer[buffer_id]
            self._cancelled_ids.add(buffer_id)
            self._recently_evicted.append(
                (buffer_id, _utcnow(), f"decohered:{reason}"),
            )
        self._metrics.inc("decohered_total", reason=reason)
        logger.warning(
            "state decohered",
            extra={"buffer_id": buffer_id, "reason": reason},
        )
        return True

    async def mark_failed(
        self, buffer_id: str, reason: str,
    ) -> bool:
        with self._lock:
            buffered = self._buffer.get(buffer_id)
            if buffered is None:
                return False
            self._failed_count += 1
            del self._buffer[buffer_id]
            self._cancelled_ids.add(buffer_id)
            self._recently_evicted.append(
                (buffer_id, _utcnow(), f"failed:{reason}"),
            )
        self._metrics.inc("failed_total")
        logger.warning(
            "state failed",
            extra={"buffer_id": buffer_id, "reason": reason},
        )
        return True

    # ------------------------------------------------------------------ sweeps
    async def expire_overdue(self) -> int:
        """Expire states past their coherence deadline. Returns count."""
        expired = 0
        with self._lock:
            now = _utcnow()
            expired_ids = [
                bid for bid, b in self._buffer.items()
                if b.status == BufferStatus.QUEUED.value
                and b.is_expired(now)
            ]
            for bid in expired_ids:
                b = self._buffer.get(bid)
                if b is None:
                    continue
                self._expire_locked(bid, b)
                expired += 1
        if expired:
            self._metrics.inc("expired_total", count=expired)
            logger.info(
                "expired overdue states",
                extra={"count": expired},
            )
        return expired

    async def reclaim_stalled(
        self, timeout_s: float,
    ) -> int:
        """
        Reset EXECUTING states older than ``timeout_s`` back to QUEUED.

        Guards against zombie executions where the consumer never
        completed the state. Returns the number reclaimed.
        """
        reclaimed = 0
        cutoff = _utcnow() - timedelta(seconds=timeout_s)
        with self._lock:
            for bid, b in list(self._buffer.items()):
                if b.status != BufferStatus.EXECUTING.value:
                    continue
                started = b.executing_started_at
                if started is None:
                    continue
                if started > cutoff:
                    continue
                requeued = replace(
                    b,
                    status=BufferStatus.QUEUED.value,
                    executing_started_at=None,
                )
                self._buffer[bid] = requeued
                self._enqueue_locked(requeued)
                self._reclaimed_count += 1
                reclaimed += 1
        if reclaimed:
            self._metrics.inc("reclaimed_total", count=reclaimed)
            logger.warning(
                "reclaimed stalled executions",
                extra={"count": reclaimed},
            )
        return reclaimed

    # ------------------------------------------------------------------ stats
    async def get_buffer_stats(self) -> Dict[str, Any]:
        with self._lock:
            by_status: Dict[str, int] = {}
            coherence = []
            for b in self._buffer.values():
                by_status[b.status] = by_status.get(b.status, 0) + 1
                coherence.append(b.quantum_state.coherence_time_ms)
            oldest_age_ms = 0.0
            if self._buffer:
                now = _utcnow()
                oldest = min(
                    self._buffer.values(),
                    key=lambda b: b.queued_at,
                )
                oldest_age_ms = (
                    now - oldest.queued_at
                ).total_seconds() * 1000.0

            return {
                "total_buffered": len(self._buffer),
                "total_completed_cached": len(self._completed),
                "queue_length": len(self._pq) + len(self._fifo),
                "cancelled_pending": len(self._cancelled_ids),
                "by_status": by_status,
                "avg_coherence_time_ms": (
                    float(np.mean(coherence)) if coherence else 0.0
                ),
                "oldest_queued_age_ms": oldest_age_ms,
                "occupancy_pct": (
                    len(self._buffer) / self.max_buffer_size * 100.0
                    if self.max_buffer_size else 0.0
                ),
                # Lifetime counters (§1.6).
                "lifetime": {
                    "total_buffered": self._total_buffered,
                    "completed": self._completed_count,
                    "decohered": self._decohered_count,
                    "failed": self._failed_count,
                    "evicted": self._evicted_count,
                    "expired": self._expired_count,
                    "reclaimed": self._reclaimed_count,
                },
            }

    def health(self) -> Dict[str, Any]:
        with self._lock:
            occupancy = (
                len(self._buffer) / self.max_buffer_size
                if self.max_buffer_size else 0.0
            )
            queue_len = len(self._pq) + len(self._fifo)
        return {
            "ready": not self._closed,
            "closed": self._closed,
            "buffer_size": len(self._buffer),
            "max_buffer_size": self.max_buffer_size,
            "occupancy_pct": occupancy * 100.0,
            "queue_length": queue_len,
            "priority_queue_enabled": self.priority_queue,
            "overflow_policy": self.config.overflow_policy,
            "uptime_seconds": time.monotonic() - self._started_mono,
            "metrics": self._metrics.snapshot(),
        }

    def get_statistics(self) -> Dict[str, Any]:
        h = self.health()
        h["config"] = self.config.to_dict()
        return h

    # ------------------------------------------------------------------ internals
    def _enqueue_locked(self, buffered: BufferedState) -> None:
        """Enqueue under the lock. Priority path uses heapq."""
        if self.priority_queue:
            self._pq_seq += 1
            heapq.heappush(
                self._pq,
                (
                    -int(buffered.priority),
                    time.monotonic(),
                    self._pq_seq,
                    buffered.buffer_id,
                ),
            )
        else:
            self._fifo.append(buffered.buffer_id)

    def _pop_next_id_locked(self) -> Optional[str]:
        if self.priority_queue:
            if not self._pq:
                return None
            _, _, _, buffer_id = heapq.heappop(self._pq)
            return buffer_id
        if not self._fifo:
            return None
        return self._fifo.popleft()

    def _evict_oldest_locked(self) -> None:
        """
        Evict the oldest QUEUED state. Never evicts EXECUTING (§1.5).
        Raises BufferFullError if no evictable state exists.
        """
        candidates = [
            (bid, b) for bid, b in self._buffer.items()
            if b.status == BufferStatus.QUEUED.value
        ]
        if not candidates:
            raise BufferFullError(
                f"buffer full ({len(self._buffer)}) and no evictable "
                f"(all executing or in-flight)"
            )
        evicted_id, _ = min(candidates, key=lambda kb: kb[1].queued_at)
        del self._buffer[evicted_id]
        self._cancelled_ids.add(evicted_id)
        self._evicted_count += 1
        self._recently_evicted.append(
            (evicted_id, _utcnow(), "capacity"),
        )
        self._metrics.inc("evicted_total")
        logger.info(
            "state evicted (capacity)",
            extra={"buffer_id": evicted_id},
        )

    def _expire_locked(
        self, buffer_id: str, buffered: BufferedState,
    ) -> None:
        del self._buffer[buffer_id]
        self._cancelled_ids.add(buffer_id)
        self._expired_count += 1
        self._recently_evicted.append(
            (buffer_id, _utcnow(), "expired"),
        )
        logger.info(
            "state expired",
            extra={
                "buffer_id": buffer_id,
                "coherence_ms": buffered.quantum_state.coherence_time_ms,
            },
        )

    # ------------------------------------------------------------------ ids
    @staticmethod
    def _generate_buffer_id() -> str:
        return "buf-" + secrets.token_hex(16)

    @staticmethod
    def _generate_state_id() -> str:
        return "state-" + secrets.token_hex(16)


__all__ = [
    "BufferFullError",
    "BufferNotFoundError",
    "BufferStatus",
    "BufferedState",
    "DuplicateBufferError",
    "InvalidTransitionError",
    "MetricsRecorder",
    "QuantumState",
    "StateBuffer",
    "StateBufferConfig",
    "StateBufferError",
    "StateBufferValidationError",
]


# =============================================================================
# Demo: python -m qcs.state_buffer
# =============================================================================

if __name__ == "__main__":  # pragma: no cover
    logging.basicConfig(level=logging.INFO)

    def _bell_state() -> np.ndarray:
        return np.array(
            [1 / math.sqrt(2), 0, 0, 1 / math.sqrt(2)],
            dtype=np.complex128,
        )

    async def demo() -> None:
        buffer = StateBuffer(
            max_buffer_size=5,
            coherence_timeout_ms=200.0,
            priority_queue=True,
        )

        async with buffer:
            print("\n=== Priority queue ===")
            ids = []
            for prio in (1, 5, 2, 9, 3):
                bid = await buffer.buffer_state(
                    thought_signature=f"sig-{prio}",
                    quantum_state=_bell_state(),
                    classical_context={"priority": prio},
                )
                ids.append(bid)

            order = []
            while True:
                b = await buffer.get_next_for_execution()
                if b is None:
                    break
                order.append(b.priority)
                await buffer.complete_state(
                    b.buffer_id,
                    measurement_outcomes=(0, 3),
                    classical_tokens=("token_00", "token_11"),
                )
            print(f"  dequeue order by priority: {order}")
            assert order == [9, 5, 3, 2, 1], order

            # --- Completed retrieval ---
            print("\n=== Completed retrieval ===")
            bid = await buffer.buffer_state(
                "sig-x", _bell_state(), {"k": "v"},
            )
            b = await buffer.get_next_for_execution()
            await buffer.complete_state(
                b.buffer_id, [0, 1], ["token_00", "token_01"],
            )
            got = await buffer.get_completed_state(b.buffer_id)
            print(f"  outcomes: {got.measurement_outcomes}")
            print(f"  tokens:   {got.classical_tokens}")
            assert got.measurement_outcomes == (0, 1)
            assert got.classical_tokens == ("token_00", "token_01")

            # Idempotency.
            got2 = await buffer.complete_state(
                b.buffer_id, [0, 1], ["token_00", "token_01"],
            )
            print(f"  idempotent second call: {got2 is not None}")

            # --- Stale queue skip ---
            print("\n=== Stale queue skip ===")
            bid_a = await buffer.buffer_state("sig-a", _bell_state(), {})
            bid_b = await buffer.buffer_state("sig-b", _bell_state(), {})
            await buffer.mark_decohered(bid_a, reason="test")
            b = await buffer.get_next_for_execution()
            print(f"  got buffer_b (not None): {b is not None}, "
                  f"id={b.buffer_id if b else None}")
            assert b is not None
            assert b.buffer_id == bid_b
            await buffer.complete_state(
                b.buffer_id, [0], ["token_0"],
            )

            # --- Amplitude validation ---
            print("\n=== Amplitude validation ===")
            for bad, label in [
                (np.ones(3), "not-power-of-2"),
                (np.array([]), "empty"),
                (np.zeros(4, dtype=np.complex128), "zero-norm"),
            ]:
                try:
                    await buffer.buffer_state("sig-bad", bad, {})
                except StateBufferValidationError as e:
                    print(f"  {label}: rejected — {type(e).__name__}")

            # --- Coherence expiry ---
            print("\n=== Coherence expiry ===")
            bid_short = await buffer.buffer_state(
                "sig-short", _bell_state(), {},
                coherence_timeout_ms=1.0,
            )
            await asyncio.sleep(0.01)
            b = await buffer.get_next_for_execution()
            print(f"  expired state skipped: {b is None}")
            assert b is None
            print(f"  eviction reason: "
                  f"{buffer.get_eviction_reason(bid_short)}")

            # --- Zombie reclaim ---
            print("\n=== Zombie reclaim ===")
            _ = await buffer.buffer_state("sig-zombie", _bell_state(), {})
            z = await buffer.get_next_for_execution()
            assert z is not None
            n = await buffer.reclaim_stalled(timeout_s=-1)  # immediate
            print(f"  reclaimed: {n}")
            assert n == 1
            z2 = await buffer.get_next_for_execution()
            assert z2 is not None and z2.buffer_id == z.buffer_id
            await buffer.complete_state(z2.buffer_id, [0], ["token_0"])

            # --- Capacity eviction ---
            print("\n=== Capacity eviction ===")
            for i in range(20):
                await buffer.buffer_state(
                    f"sig-cap-{i}", _bell_state(),
                    {"priority": i % 3},
                )
            stats = await buffer.get_buffer_stats()
            print(f"  buffered: {stats['total_buffered']} (max 5)")
            assert stats["total_buffered"] <= 5
            print(f"  lifetime evicted: {stats['lifetime']['evicted']}")

            # --- Stats ---
            print("\n=== Stats ===")
            print(await buffer.get_buffer_stats())

        # --- Health ---
        print("\n=== Health (after close) ===")
        h = buffer.health()
        print(f"  ready: {h['ready']}, closed: {h['closed']}")
        assert h["ready"] is False and h["closed"] is True

        print("\nSmoke test passed.")

    asyncio.run(demo())
