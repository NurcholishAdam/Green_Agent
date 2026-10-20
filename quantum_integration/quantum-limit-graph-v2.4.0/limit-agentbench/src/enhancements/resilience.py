"""
resilience.py — Circuit breaker used by consumers to guard external calls.

Enhancements over the initial version
--------------------------------------
Bugs fixed
    B1. Reset ``failure_count`` / ``success_count`` on every transition to
        CLOSED.  Previously, a breaker that opened once would re-open on the
        *very next* failure after a HALF_OPEN → CLOSED recovery, because the
        stale failure count was still ≥ threshold.
    B2. All metric updates are performed under the lock.  ``total_calls`` is
        incremented exactly once per attempted call, even when the call is
        rejected.
    B3. ``GlobalCircuitBreaker.get_or_create`` uses ``dict.setdefault`` so
        concurrent callers get the *same* breaker instance — previously two
        callers could each build one and the second would silently overwrite
        the first, so the two callers guarded the same resource with two
        different breakers.

Design improvements
    D1. Lock is never held across the guarded call; single pre-call gate
        plus a post-call slot release.
    D2. Constructor validates argument ranges.
    D3. Optional per-call timeout (``timeout=``) or default via ``call_timeout``.
    D4. Last underlying exception is chained via ``raise ... from`` when the
        breaker is OPEN, so callers can see the reason.
    D5. Latency tracking, state-transition history, ``avg_latency``,
        ``seconds_since_last_failure``.
    D6. ``reset()`` / ``force_open()`` on ``CircuitBreaker``; ``get()`` /
        ``list()`` / ``all_metrics()`` / ``reset()`` / ``reset_all()`` /
        ``clear()`` on the registry.
    D7. Optional per-call ``fallback=`` invoked when the breaker rejects.
    D8. Type hints corrected to ``Callable[..., Awaitable[T]]``.

The clock is ``time.monotonic`` by default (immune to wall-clock jumps); it can
be overridden via ``clock=`` for deterministic tests.
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections import deque
from enum import Enum
from typing import Any, Awaitable, Callable, Deque, Dict, List, Optional, TypeVar

logger = logging.getLogger(__name__)


T = TypeVar("T")


class CircuitBreakerState(str, Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


class CircuitBreakerError(Exception):
    """Raised when the breaker rejects a call (OPEN or HALF_OPEN exhausted)."""


class CircuitBreakerTimeoutError(CircuitBreakerError):
    """Raised when a guarded call exceeds its configured timeout."""


# --------------------------------------------------------------------------- #
# Primary breaker
# --------------------------------------------------------------------------- #
class CircuitBreaker:
    def __init__(
        self,
        name: str,
        failure_threshold: int = 5,
        recovery_timeout: float = 60.0,
        half_open_success_threshold: int = 2,
        half_open_max_concurrent: int = 1,
        call_timeout: Optional[float] = None,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        # D2: argument validation
        if not name:
            raise ValueError("name must be non-empty")
        if failure_threshold < 1:
            raise ValueError("failure_threshold must be >= 1")
        if recovery_timeout < 0:
            raise ValueError("recovery_timeout must be >= 0")
        if half_open_success_threshold < 1:
            raise ValueError("half_open_success_threshold must be >= 1")
        if half_open_max_concurrent < 1:
            raise ValueError("half_open_max_concurrent must be >= 1")
        if call_timeout is not None and call_timeout <= 0:
            raise ValueError("call_timeout must be > 0 when provided")

        self.name = name
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.half_open_success_threshold = half_open_success_threshold
        self.half_open_max_concurrent = half_open_max_concurrent
        self.call_timeout = call_timeout
        self._clock = clock

        # State
        self._state = CircuitBreakerState.CLOSED
        self._failure_count = 0
        self._success_count = 0
        self._half_open_in_flight = 0
        self._last_failure_time = 0.0
        self._last_state_change_time = self._clock()
        self._last_exception: Optional[BaseException] = None

        # Concurrency
        self._lock = asyncio.Lock()

        # Metrics
        self._metrics: Dict[str, float] = {
            "total_calls": 0,
            "successful_calls": 0,
            "failed_calls": 0,
            "rejected_calls": 0,
            "timeout_calls": 0,
            "fallback_calls": 0,
            "latency_sum": 0.0,
            "latency_max": 0.0,
        }
        self._state_transitions: Deque[Dict[str, Any]] = deque(maxlen=100)

    # ------------------------------------------------------------------ #
    # Public API
    # ------------------------------------------------------------------ #
    async def call(
        self,
        func: Callable[..., Awaitable[T]],
        *args: Any,
        timeout: Optional[float] = None,
        fallback: Optional[Callable[[], Any]] = None,
        **kwargs: Any,
    ) -> T:
        # D3 + D7: optional per-call timeout and fallback.
        try:
            acquired_half_open_slot = await self._admit()
        except CircuitBreakerError as rejected:
            return await self._use_fallback(fallback, rejected)

        started = self._clock()
        try:
            result = await self._invoke(func, *args, timeout=timeout, **kwargs)
            await self._on_success(self._clock() - started)
            return result
        except CircuitBreakerTimeoutError as exc:
            await self._on_failure(
                self._clock() - started, exc=exc, is_timeout=True
            )
            raise
        except Exception as exc:
            await self._on_failure(self._clock() - started, exc=exc)
            raise
        finally:
            if acquired_half_open_slot:
                await self._release_half_open_slot()

    def get_metrics(self) -> Dict[str, Any]:
        successes = self._metrics["successful_calls"] or 0
        avg_latency = (
            self._metrics["latency_sum"] / successes if successes else 0.0
        )
        return {
            **self._metrics,
            "state": self._state.value,
            "failure_count": self._failure_count,
            "success_count": self._success_count,
            "half_open_in_flight": self._half_open_in_flight,
            "avg_latency": avg_latency,
            "seconds_since_last_failure": (
                self._clock() - self._last_failure_time
                if self._last_failure_time > 0 else None
            ),
            "last_state_change_time": self._last_state_change_time,
            "last_exception": (
                repr(self._last_exception) if self._last_exception else None
            ),
            "recent_transitions": list(self._state_transitions),
        }

    async def reset(self) -> None:
        """Force the breaker back to CLOSED with clean counters."""
        async with self._lock:
            self._transition_locked(
                CircuitBreakerState.CLOSED, reason="manual reset"
            )
            for key in self._metrics:
                self._metrics[key] = 0
            self._last_exception = None

    async def force_open(self, reason: str = "manual") -> None:
        """Force the breaker OPEN (useful in tests and failover drills)."""
        async with self._lock:
            self._last_failure_time = self._clock()
            self._transition_locked(CircuitBreakerState.OPEN, reason=reason)

    # ------------------------------------------------------------------ #
    # Admit / release
    # ------------------------------------------------------------------ #
    async def _admit(self) -> bool:
        """
        Decide whether the current call may proceed.

        Returns True iff a HALF_OPEN slot was acquired (must be released
        via ``_release_half_open_slot``).
        """
        async with self._lock:
            self._metrics["total_calls"] += 1
            now = self._clock()

            # OPEN -> maybe HALF_OPEN (recovery timeout elapsed).
            if self._state == CircuitBreakerState.OPEN:
                if now - self._last_failure_time >= self.recovery_timeout:
                    self._transition_locked(
                        CircuitBreakerState.HALF_OPEN,
                        reason="recovery timeout elapsed",
                    )
                else:
                    self._metrics["rejected_calls"] += 1
                    raise self._rejected_error()

            # HALF_OPEN -> CLOSED or reject.
            if self._state == CircuitBreakerState.HALF_OPEN:
                if self._success_count >= self.half_open_success_threshold:
                    self._transition_locked(
                        CircuitBreakerState.CLOSED,
                        reason="half-open success threshold reached",
                    )
                    return False
                if self._half_open_in_flight >= self.half_open_max_concurrent:
                    self._metrics["rejected_calls"] += 1
                    raise CircuitBreakerError(
                        f"Circuit breaker {self.name} HALF_OPEN is at "
                        f"max concurrent probes"
                    )
                self._half_open_in_flight += 1
                return True

            return False

    async def _release_half_open_slot(self) -> None:
        async with self._lock:
            if self._half_open_in_flight > 0:
                self._half_open_in_flight -= 1

    # ------------------------------------------------------------------ #
    # Invocation
    # ------------------------------------------------------------------ #
    async def _invoke(
        self,
        func: Callable[..., Awaitable[T]],
        *args: Any,
        timeout: Optional[float] = None,
        **kwargs: Any,
    ) -> T:
        effective_timeout = (
            timeout if timeout is not None else self.call_timeout
        )
        coro = func(*args, **kwargs)
        if effective_timeout is None:
            return await coro
        try:
            return await asyncio.wait_for(coro, timeout=effective_timeout)
        except asyncio.TimeoutError:
            raise CircuitBreakerTimeoutError(
                f"Circuit breaker {self.name} call timed out after "
                f"{effective_timeout}s"
            )

    async def _use_fallback(
        self,
        fallback: Optional[Callable[[], Any]],
        rejected: CircuitBreakerError,
    ) -> T:
        if fallback is None:
            raise rejected
        async with self._lock:
            self._metrics["fallback_calls"] += 1
        result = fallback()
        if asyncio.iscoroutine(result):
            return await result
        return result

    # ------------------------------------------------------------------ #
    # Success / failure recording
    # ------------------------------------------------------------------ #
    async def _on_success(self, latency: float) -> None:
        async with self._lock:
            self._metrics["successful_calls"] += 1
            self._metrics["latency_sum"] += latency
            self._metrics["latency_max"] = max(
                self._metrics["latency_max"], latency
            )
            # B1: any success clears the failure streak.
            self._failure_count = 0
            self._success_count += 1
            if (
                self._state == CircuitBreakerState.HALF_OPEN
                and self._success_count >= self.half_open_success_threshold
            ):
                self._transition_locked(
                    CircuitBreakerState.CLOSED,
                    reason="half-open success threshold reached",
                )

    async def _on_failure(
        self,
        latency: float,
        exc: Optional[BaseException] = None,
        is_timeout: bool = False,
    ) -> None:
        async with self._lock:
            self._metrics["failed_calls"] += 1
            if is_timeout:
                self._metrics["timeout_calls"] += 1
            self._metrics["latency_sum"] += latency
            self._metrics["latency_max"] = max(
                self._metrics["latency_max"], latency
            )
            self._failure_count += 1
            self._success_count = 0
            self._last_failure_time = self._clock()
            if exc is not None:
                self._last_exception = exc

            if self._state == CircuitBreakerState.CLOSED:
                if self._failure_count >= self.failure_threshold:
                    self._transition_locked(
                        CircuitBreakerState.OPEN,
                        reason="failure threshold reached",
                    )
            elif self._state == CircuitBreakerState.HALF_OPEN:
                self._transition_locked(
                    CircuitBreakerState.OPEN,
                    reason="failed during half-open probe",
                )

    # ------------------------------------------------------------------ #
    # State transition
    # ------------------------------------------------------------------ #
    def _transition_locked(
        self, new_state: CircuitBreakerState, reason: str
    ) -> None:
        """Must be called with the lock held."""
        old = self._state
        if old == new_state:
            return
        self._state = new_state
        self._last_state_change_time = self._clock()
        # B1: full reset when returning to CLOSED.
        if new_state == CircuitBreakerState.CLOSED:
            self._failure_count = 0
            self._success_count = 0
            self._half_open_in_flight = 0
        elif new_state == CircuitBreakerState.HALF_OPEN:
            self._success_count = 0
            self._half_open_in_flight = 0
        # OPEN leaves counters intact so callers can see why we opened.

        self._state_transitions.append({
            "from": old.value,
            "to": new_state.value,
            "reason": reason,
            "timestamp_monotonic": self._last_state_change_time,
        })
        logger.info(
            "Circuit breaker %s: %s -> %s (%s)",
            self.name, old.value, new_state.value, reason,
        )

    def _rejected_error(self) -> CircuitBreakerError:
        # D4: chain the last underlying exception if we have one.
        exc = CircuitBreakerError(
            f"Circuit breaker {self.name} is OPEN"
        )
        if self._last_exception is not None:
            try:
                exc.__cause__ = self._last_exception
            except Exception:
                pass
        return exc


# --------------------------------------------------------------------------- #
# Global registry
# --------------------------------------------------------------------------- #
class GlobalCircuitBreaker:
    _instance: Optional["GlobalCircuitBreaker"] = None
    _breakers: Dict[str, CircuitBreaker] = {}

    def __new__(cls) -> "GlobalCircuitBreaker":
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def get_or_create(self, name: str, **kwargs: Any) -> CircuitBreaker:
        # B3: setdefault keeps the first winner under CPython's GIL.
        existing = self._breakers.get(name)
        if existing is not None:
            return existing
        return self._breakers.setdefault(name, CircuitBreaker(name, **kwargs))

    def get(self, name: str) -> Optional[CircuitBreaker]:
        return self._breakers.get(name)

    def list(self) -> List[str]:
        return list(self._breakers.keys())

    def all_metrics(self) -> Dict[str, Dict[str, Any]]:
        return {name: b.get_metrics() for name, b in self._breakers.items()}

    async def reset(self, name: str) -> bool:
        breaker = self._breakers.get(name)
        if breaker is None:
            return False
        await breaker.reset()
        return True

    async def reset_all(self) -> None:
        for breaker in list(self._breakers.values()):
            await breaker.reset()

    def clear(self) -> None:
        """Remove all registered breakers (mainly useful in tests)."""
        self._breakers.clear()


# --------------------------------------------------------------------------- #
# Self-test (run with ``python resilience.py``)
# --------------------------------------------------------------------------- #
async def _self_test() -> int:
    import sys

    failures: List[str] = []

    def check(name: str, cond: bool, detail: str = "") -> None:
        print(f"  [{'PASS' if cond else 'FAIL'}] {name}"
              + (f" — {detail}" if detail and not cond else ""))
        if not cond:
            failures.append(name)

    print("=" * 70)
    print("resilience.py self-test")
    print("=" * 70)

    # --- B1: recovery works ------------------------------------------------
    print("\n[B1] Recovery clears counters")
    breaker = CircuitBreaker("b1", failure_threshold=3,
                              recovery_timeout=0.05,
                              half_open_success_threshold=1)

    async def boom() -> None:
        raise RuntimeError("fail")

    # Open the breaker.
    for _ in range(3):
        try:
            await breaker.call(boom)
        except RuntimeError:
            pass
    check("breaker opened", breaker.get_metrics()["state"] == "open")

    await asyncio.sleep(0.1)  # let recovery timeout elapse

    async def ok() -> str:
        return "ok"

    # One successful HALF_OPEN probe closes it.
    result = await breaker.call(ok)
    m = breaker.get_metrics()
    check("closed after half-open success", m["state"] == "closed")
    check("failure_count reset to 0", m["failure_count"] == 0)
    check("successful call returned", result == "ok")

    # A single failure after recovery should NOT reopen immediately.
    try:
        await breaker.call(boom)
    except RuntimeError:
        pass
    m = breaker.get_metrics()
    check("single failure does not reopen (B1 fixed)",
          m["state"] == "closed", f"state={m['state']}")

    # --- B2: total_calls counts every attempt ------------------------------
    print("\n[B2] total_calls under lock")
    breaker2 = CircuitBreaker("b2", failure_threshold=2,
                               recovery_timeout=10.0)
    for _ in range(2):
        try:
            await breaker2.call(boom)
        except RuntimeError:
            pass
    # Now the breaker is OPEN; further calls are rejected.
    try:
        await breaker2.call(ok)
    except CircuitBreakerError:
        pass
    m = breaker2.get_metrics()
    check("total_calls = successes + failures + rejected",
          m["total_calls"] == m["successful_calls"] + m["failed_calls"] + m["rejected_calls"],
          f"total={m['total_calls']} suc={m['successful_calls']} "
          f"fail={m['failed_calls']} rej={m['rejected_calls']}")

    # --- B3: registry returns the same instance ---------------------------
    print("\n[B3] Registry returns same instance")
    reg = GlobalCircuitBreaker()
    reg.clear()
    a = reg.get_or_create("shared")
    b = reg.get_or_create("shared")
    check("same instance", a is b)
    reg.clear()

    # --- D3: timeout -------------------------------------------------------
    print("\n[D3] Per-call timeout")

    async def slow() -> None:
        await asyncio.sleep(1.0)

    breaker3 = CircuitBreaker("b3", failure_threshold=5)
    try:
        await breaker3.call(slow, timeout=0.02)
        check("timeout raised", False)
    except CircuitBreakerTimeoutError as exc:
        check("timeout raised", True)
        check("timeout metric recorded",
              breaker3.get_metrics()["timeout_calls"] == 1)
        check("timeout is a CircuitBreakerError",
              isinstance(exc, CircuitBreakerError))

    # --- D4: exception chaining -------------------------------------------
    print("\n[D4] Exception chaining")
    breaker4 = CircuitBreaker("b4", failure_threshold=1,
                               recovery_timeout=10.0)
    try:
        await breaker4.call(boom)
    except RuntimeError:
        pass
    try:
        await breaker4.call(ok)
    except CircuitBreakerError as exc:
        check("rejected error chains last exception",
              isinstance(exc.__cause__, RuntimeError),
              f"__cause__={exc.__cause__!r}")

    # --- D7: fallback ------------------------------------------------------
    print("\n[D7] Fallback on rejection")

    async def cached() -> str:
        return "cached"

    breaker5 = CircuitBreaker("b5", failure_threshold=1,
                               recovery_timeout=10.0)
    try:
        await breaker5.call(boom)
    except RuntimeError:
        pass
    result = await breaker5.call(ok, fallback=cached)
    check("fallback used", result == "cached")
    check("fallback metric",
          breaker5.get_metrics()["fallback_calls"] == 1)

    # --- D6: reset / force_open -------------------------------------------
    print("\n[D6] reset / force_open")
    breaker6 = CircuitBreaker("b6")
    await breaker6.force_open("testing")
    check("force_open works", breaker6.get_metrics()["state"] == "open")
    await breaker6.reset()
    m = breaker6.get_metrics()
    check("reset works", m["state"] == "closed" and m["total_calls"] == 0)

    print("\n" + "=" * 70)
    if failures:
        print(f"FAILED: {len(failures)} test(s)")
        for f in failures:
            print(f"  - {f}")
        return 1
    print("ALL TESTS PASSED")
    return 0


if __name__ == "__main__":
    import sys
    sys.exit(asyncio.run(_self_test()))
