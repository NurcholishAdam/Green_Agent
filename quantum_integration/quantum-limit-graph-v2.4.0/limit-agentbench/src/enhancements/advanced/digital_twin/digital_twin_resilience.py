# src/quantum_integration/digital_twin/digital_twin_resilience.py

"""Circuit breaker and retry helpers with lazy loop binding."""

from __future__ import annotations

import asyncio
import random
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Callable, Optional, Tuple

from .digital_twin_errors import DigitalTwinCircuitOpenError
from .digital_twin_helpers import _LazyLock

SCHEMA_VERSION: int = 1


class CircuitBreakerState(str, Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"

    def __str__(self) -> str:
        return self.value


@dataclass
class CircuitBreakerStats:
    successes: int = 0
    failures: int = 0
    open_count: int = 0
    half_open_count: int = 0
    last_failure_iso: Optional[str] = None

    def to_dict(self) -> dict:
        return {
            "successes": self.successes,
            "failures": self.failures,
            "open_count": self.open_count,
            "half_open_count": self.half_open_count,
            "last_failure_iso": self.last_failure_iso,
        }


class CircuitBreaker:
    """Async circuit breaker.

    Fixes the original's loop-binding bug (``_LazyLock``) and its
    HALF_OPEN admits-many-callers bug (via ``_in_flight``).
    """

    def __init__(
        self,
        failure_threshold: int,
        recovery_timeout: float,
        *,
        on_state_change: Optional[
            Callable[[CircuitBreakerState, CircuitBreakerState], None]
        ] = None,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        if failure_threshold < 1:
            raise DigitalTwinCircuitOpenError(
                "failure_threshold must be >= 1."
            )
        if recovery_timeout < 0:
            raise DigitalTwinCircuitOpenError(
                "recovery_timeout must be >= 0."
            )
        self.failure_threshold = int(failure_threshold)
        self.recovery_timeout = float(recovery_timeout)
        self._state = CircuitBreakerState.CLOSED
        self._failure_count = 0
        self._last_failure_mono: Optional[float] = None
        self._in_flight_half_open = False
        self._on_state_change = on_state_change
        self._clock = clock
        self._lock = _LazyLock()
        self.stats = CircuitBreakerStats()

    @property
    def state(self) -> CircuitBreakerState:
        return self._state

    @property
    def is_open(self) -> bool:
        return self._state is CircuitBreakerState.OPEN

    @property
    def is_closed(self) -> bool:
        return self._state is CircuitBreakerState.CLOSED

    def _transition(self, new_state: CircuitBreakerState) -> None:
        if new_state is self._state:
            return
        old = self._state
        self._state = new_state
        if new_state is CircuitBreakerState.OPEN:
            self.stats.open_count += 1
        elif new_state is CircuitBreakerState.HALF_OPEN:
            self.stats.half_open_count += 1
        if self._on_state_change:
            try:
                self._on_state_change(old, new_state)
            except Exception:  # pragma: no cover - user callback
                pass

    async def _maybe_half_open(self) -> None:
        """Transition OPEN → HALF_OPEN after recovery_timeout."""
        if self._state is not CircuitBreakerState.OPEN:
            return
        if self._last_failure_mono is None:
            return
        elapsed = self._clock() - self._last_failure_mono
        if elapsed >= self.recovery_timeout:
            self._transition(CircuitBreakerState.HALF_OPEN)
            self._failure_count = 0
            self._in_flight_half_open = False

    async def call(self, func: Callable, *args: Any, **kwargs: Any) -> Any:
        async with self._lock:
            await self._maybe_half_open()
            if self._state is CircuitBreakerState.OPEN:
                raise DigitalTwinCircuitOpenError("Circuit breaker OPEN")
            if (
                self._state is CircuitBreakerState.HALF_OPEN
                and self._in_flight_half_open
            ):
                # Only one caller admitted in HALF_OPEN.
                raise DigitalTwinCircuitOpenError(
                    "Circuit breaker HALF_OPEN with in-flight probe"
                )
            if self._state is CircuitBreakerState.HALF_OPEN:
                self._in_flight_half_open = True

        try:
            result = await func(*args, **kwargs)
        except BaseException as exc:
            async with self._lock:
                self.stats.failures += 1
                self.stats.last_failure_iso = datetime.now(timezone.utc).isoformat()
                self._failure_count += 1
                self._last_failure_mono = self._clock()
                if self._state is CircuitBreakerState.HALF_OPEN:
                    self._transition(CircuitBreakerState.OPEN)
                elif (
                    self._state is CircuitBreakerState.CLOSED
                    and self._failure_count >= self.failure_threshold
                ):
                    self._transition(CircuitBreakerState.OPEN)
                self._in_flight_half_open = False
            raise exc

        async with self._lock:
            self.stats.successes += 1
            if self._state is CircuitBreakerState.HALF_OPEN:
                self._transition(CircuitBreakerState.CLOSED)
            self._failure_count = 0
            self._in_flight_half_open = False
        return result

    def statistics(self) -> dict:
        return {
            "state": self._state.value,
            "failure_count": self._failure_count,
            "failure_threshold": self.failure_threshold,
            "recovery_timeout": self.recovery_timeout,
            **self.stats.to_dict(),
        }


async def retry_async(
    func: Callable,
    *args: Any,
    max_retries: int,
    base_delay_ms: float,
    max_delay_ms: float,
    retryable: Optional[Callable[[BaseException], bool]] = None,
    rng: Optional[random.Random] = None,
    **kwargs: Any,
) -> Any:
    """Retry an async function with exponential backoff and jitter.

    Parameters
    ----------
    retryable : Callable[[BaseException], bool], optional
        Predicate deciding whether an exception should be retried.
        Defaults to retrying only on ``OSError``, ``TimeoutError``, and
        ``asyncio.TimeoutError``.
    rng : random.Random, optional
        Source of jitter. Provides reproducibility when seeded.
    """
    if max_retries < 0:
        raise ValueError("max_retries must be >= 0.")
    if base_delay_ms <= 0 or max_delay_ms < base_delay_ms:
        raise ValueError(
            "base_delay_ms > 0 and max_delay_ms >= base_delay_ms required."
        )

    if retryable is None:
        retryable = lambda e: isinstance(
            e, (OSError, TimeoutError, asyncio.TimeoutError)
        )
    rng = rng or random.Random()

    last: Optional[BaseException] = None
    for attempt in range(max_retries + 1):
        try:
            return await func(*args, **kwargs)
        except BaseException as exc:  # noqa: BLE001
            last = exc
            if not retryable(exc) or attempt == max_retries:
                raise
            delay_ms = min(base_delay_ms * (2 ** attempt), max_delay_ms)
            jitter = rng.uniform(0.0, delay_ms * 0.1)
            await asyncio.sleep((delay_ms + jitter) / 1000.0)
    assert last is not None  # pragma: no cover
    raise RuntimeError("Max retries exceeded") from last


__all__ = [
    "SCHEMA_VERSION",
    "CircuitBreaker",
    "CircuitBreakerState",
    "CircuitBreakerStats",
    "retry_async",
]
