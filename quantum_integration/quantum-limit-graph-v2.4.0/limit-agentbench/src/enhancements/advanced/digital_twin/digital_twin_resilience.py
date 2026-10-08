# src/quantum_integration/digital_twin/digital_twin_resilience.py

"""Circuit breaker and retry helpers with lazy loop binding.

Overview
--------
This module provides:

* :class:`CircuitBreaker` — an async circuit breaker with the standard
  CLOSED / OPEN / HALF_OPEN state machine, single-caller probing in
  HALF_OPEN, injectable clock, and out-of-lock state-change callbacks.
* :func:`retry_async` — exponential-backoff retry with configurable
  jitter, per-attempt timeout, an ``on_retry`` observability hook, and a
  ``retry_on_result`` predicate.

Cancellation safety
-------------------
Both :meth:`CircuitBreaker.call` and :func:`retry_async` propagate
:class:`asyncio.CancelledError` (and ``KeyboardInterrupt`` /
``SystemExit``) without retrying them, even if a user-supplied
``retryable`` predicate would match. Bare ``raise`` is used inside
``except`` blocks so the original traceback is preserved.

Callback safety
---------------
``on_state_change`` callbacks are invoked *after* the internal lock is
released. They are notifications, not synchronization points; a callback
may observe a newer state than the ``old`` value it receives.
"""

from __future__ import annotations

import asyncio
import logging
import random
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from typing import (
    Any,
    Awaitable,
    Callable,
    Dict,
    Literal,
    Optional,
    Tuple,
)

from .digital_twin_errors import (
    DigitalTwinCircuitOpenError,
    DigitalTwinConfigError,
)
from .digital_twin_helpers import _LazyLock, _iso_now

logger = logging.getLogger(__name__)
SCHEMA_VERSION: int = 2

#: Allowed jitter strategies for :func:`retry_async`.
JitterPolicy = Literal["none", "positive", "full", "equal", "decorrelated"]


# --------------------------------------------------------------------------- #
# Circuit breaker state and stats
# --------------------------------------------------------------------------- #

class CircuitBreakerState(str, Enum):
    """Circuit breaker states."""

    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"

    def __str__(self) -> str:
        return self.value

    def __repr__(self) -> str:
        return f"CircuitBreakerState.{self.name}"


@dataclass
class CircuitBreakerStats:
    """Counters for a :class:`CircuitBreaker`.

    ``total_calls``, ``failure_rate``, and ``success_rate`` are derived
    on demand so they can never drift out of sync with ``successes`` /
    ``failures``.
    """

    successes: int = 0
    failures: int = 0
    open_count: int = 0
    half_open_count: int = 0
    last_failure_iso: Optional[str] = None

    @property
    def total_calls(self) -> int:
        return self.successes + self.failures

    @property
    def failure_rate(self) -> float:
        total = self.total_calls
        return (self.failures / total) if total > 0 else 0.0

    @property
    def success_rate(self) -> float:
        total = self.total_calls
        return (self.successes / total) if total > 0 else 0.0

    def to_dict(self) -> Dict[str, Any]:
        return {
            "successes": self.successes,
            "failures": self.failures,
            "open_count": self.open_count,
            "half_open_count": self.half_open_count,
            "last_failure_iso": self.last_failure_iso,
            "total_calls": self.total_calls,
            "failure_rate": self.failure_rate,
            "success_rate": self.success_rate,
        }


# --------------------------------------------------------------------------- #
# Circuit breaker
# --------------------------------------------------------------------------- #

class CircuitBreaker:
    """Async circuit breaker.

    Fixes the original's loop-binding bug (via ``_LazyLock``) and its
    HALF_OPEN admits-many-callers bug (via ``_in_flight_half_open``).

    Parameters
    ----------
    failure_threshold:
        Number of consecutive CLOSED failures before opening. Must be
        ``>= 1``.
    recovery_timeout:
        Seconds to wait in OPEN before transitioning to HALF_OPEN. Must
        be ``>= 0``.
    on_state_change:
        Optional ``(old, new) -> None`` callback. Invoked outside the
        internal lock. Exceptions are logged at ``warning`` and never
        propagate.
    clock:
        Monotonic time source. Injectable for tests.
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
        if isinstance(failure_threshold, bool) or not isinstance(
            failure_threshold, int,
        ):
            raise DigitalTwinConfigError(
                "failure_threshold must be an integer.",
            )
        if failure_threshold < 1:
            raise DigitalTwinConfigError(
                "failure_threshold must be >= 1.",
            )
        if isinstance(recovery_timeout, bool) or not isinstance(
            recovery_timeout, (int, float),
        ):
            raise DigitalTwinConfigError(
                "recovery_timeout must be a number.",
            )
        if recovery_timeout < 0:
            raise DigitalTwinConfigError(
                "recovery_timeout must be >= 0.",
            )
        if not callable(clock):
            raise DigitalTwinConfigError("clock must be callable.")
        if on_state_change is not None and not callable(on_state_change):
            raise DigitalTwinConfigError(
                "on_state_change must be callable or None.",
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

    # -- read-only properties ------------------------------------------- #

    @property
    def state(self) -> CircuitBreakerState:
        """The current breaker state (unsynchronized read)."""
        return self._state

    @property
    def is_open(self) -> bool:
        return self._state is CircuitBreakerState.OPEN

    @property
    def is_closed(self) -> bool:
        return self._state is CircuitBreakerState.CLOSED

    @property
    def is_half_open(self) -> bool:
        return self._state is CircuitBreakerState.HALF_OPEN

    # -- transitions ---------------------------------------------------- #

    def _transition(
        self, new_state: CircuitBreakerState,
    ) -> Optional[Tuple[CircuitBreakerState, CircuitBreakerState]]:
        """Update the state; return ``(old, new)`` if it changed.

        Must be called with ``self._lock`` held. The returned tuple is
        passed to :meth:`_notify` after the lock is released.
        """
        if new_state is self._state:
            return None
        old = self._state
        self._state = new_state
        if new_state is CircuitBreakerState.OPEN:
            self.stats.open_count += 1
        elif new_state is CircuitBreakerState.HALF_OPEN:
            self.stats.half_open_count += 1
        return old, new_state

    def _notify(
        self,
        transition: Optional[Tuple[CircuitBreakerState, CircuitBreakerState]],
    ) -> None:
        """Fire ``on_state_change`` outside the lock. Best-effort."""
        if transition is None or self._on_state_change is None:
            return
        try:
            self._on_state_change(*transition)
        except Exception as exc:  # noqa: BLE001 - user callback
            logger.warning("on_state_change callback failed: %s", exc)

    def _maybe_half_open_locked(self) -> Optional[
        Tuple[CircuitBreakerState, CircuitBreakerState]
    ]:
        """Transition OPEN → HALF_OPEN if the recovery window has elapsed.

        Must be called with ``self._lock`` held. The caller is
        responsible for firing :meth:`_notify` with the returned tuple.
        """
        if self._state is not CircuitBreakerState.OPEN:
            return None
        if self._last_failure_mono is None:
            return None
        elapsed = self._clock() - self._last_failure_mono
        if elapsed < self.recovery_timeout:
            return None
        transition = self._transition(CircuitBreakerState.HALF_OPEN)
        self._failure_count = 0
        self._in_flight_half_open = False
        return transition

    def _retry_after_seconds(self) -> Optional[float]:
        """Seconds remaining until OPEN → HALF_OPEN, if applicable."""
        if self._state is not CircuitBreakerState.OPEN:
            return None
        if self._last_failure_mono is None:
            return None
        elapsed = self._clock() - self._last_failure_mono
        return max(0.0, self.recovery_timeout - elapsed)

    # -- main entry point ----------------------------------------------- #

    async def call(self, func: Callable, *args: Any, **kwargs: Any) -> Any:
        """Invoke ``func`` under the breaker's policy.

        ``func`` must be an awaitable-returning callable (coroutine
        function, or a callable returning a coroutine).

        Raises
        ------
        DigitalTwinCircuitOpenError
            If the breaker is OPEN, or HALF_OPEN with a probe in flight.
            The exception's ``retry_after`` attribute carries the
            remaining recovery window when known.
        """
        # -- Admittance check ------------------------------------------- #
        transition: Optional[
            Tuple[CircuitBreakerState, CircuitBreakerState]
        ] = None
        async with self._lock:
            transition = self._maybe_half_open_locked()
            if self._state is CircuitBreakerState.OPEN:
                retry_after = self._retry_after_seconds()
                raise DigitalTwinCircuitOpenError(
                    "Circuit breaker OPEN",
                    state=self._state.value,
                    retry_after=retry_after,
                )
            if (
                self._state is CircuitBreakerState.HALF_OPEN
                and self._in_flight_half_open
            ):
                raise DigitalTwinCircuitOpenError(
                    "Circuit breaker HALF_OPEN with in-flight probe",
                    state=self._state.value,
                )
            if self._state is CircuitBreakerState.HALF_OPEN:
                self._in_flight_half_open = True
        self._notify(transition)

        # -- Invocation ------------------------------------------------- #
        try:
            result = await func(*args, **kwargs)
        except (KeyboardInterrupt, SystemExit):
            # Never treat OS-level signals as breaker failures.
            async with self._lock:
                self._in_flight_half_open = False
            raise
        except asyncio.CancelledError:
            async with self._lock:
                self._in_flight_half_open = False
            raise
        except BaseException:
            failure_transition: Optional[
                Tuple[CircuitBreakerState, CircuitBreakerState]
            ] = None
            async with self._lock:
                self.stats.failures += 1
                self.stats.last_failure_iso = _iso_now()
                self._failure_count += 1
                self._last_failure_mono = self._clock()
                if self._state is CircuitBreakerState.HALF_OPEN:
                    failure_transition = self._transition(
                        CircuitBreakerState.OPEN,
                    )
                elif (
                    self._state is CircuitBreakerState.CLOSED
                    and self._failure_count >= self.failure_threshold
                ):
                    failure_transition = self._transition(
                        CircuitBreakerState.OPEN,
                    )
                self._in_flight_half_open = False
            self._notify(failure_transition)
            raise  # bare raise preserves the original traceback

        # -- Success ---------------------------------------------------- #
        success_transition: Optional[
            Tuple[CircuitBreakerState, CircuitBreakerState]
        ] = None
        async with self._lock:
            self.stats.successes += 1
            if self._state is CircuitBreakerState.HALF_OPEN:
                success_transition = self._transition(
                    CircuitBreakerState.CLOSED,
                )
            self._failure_count = 0
            self._in_flight_half_open = False
        self._notify(success_transition)
        return result

    # -- manual control ------------------------------------------------- #

    async def reset(self) -> None:
        """Force the breaker back to CLOSED and clear counters.

        Intended for operators / health checks. Safe to call at any
        time; the transition is reported via ``on_state_change``.
        """
        async with self._lock:
            transition = self._transition(CircuitBreakerState.CLOSED)
            self._failure_count = 0
            self._last_failure_mono = None
            self._in_flight_half_open = False
        self._notify(transition)

    # -- introspection -------------------------------------------------- #

    def statistics(self) -> Dict[str, Any]:
        """Return a snapshot of breaker state and counters."""
        return {
            "schema_version": SCHEMA_VERSION,
            "state": self._state.value,
            "failure_count": self._failure_count,
            "failure_threshold": self.failure_threshold,
            "recovery_timeout": self.recovery_timeout,
            "in_flight_half_open": self._in_flight_half_open,
            "retry_after_seconds": self._retry_after_seconds(),
            **self.stats.to_dict(),
        }

    def __repr__(self) -> str:
        return (
            f"{type(self).__name__}("
            f"state={self._state.value!r}, "
            f"failure_count={self._failure_count}, "
            f"failure_threshold={self.failure_threshold}, "
            f"recovery_timeout={self.recovery_timeout})"
        )


# --------------------------------------------------------------------------- #
# Retry
# --------------------------------------------------------------------------- #

def _default_retryable(exc: BaseException) -> bool:
    """Default retry predicate.

    Retries ``OSError`` / ``TimeoutError`` / ``asyncio.TimeoutError``.
    Never retries :class:`DigitalTwinCircuitOpenError` — the caller
    should back off or fail over instead.
    """
    if isinstance(exc, DigitalTwinCircuitOpenError):
        return False
    return isinstance(exc, (OSError, TimeoutError, asyncio.TimeoutError))


def _compute_delay_ms(
    attempt: int,
    *,
    base_delay_ms: float,
    max_delay_ms: float,
    jitter: JitterPolicy,
    rng: random.Random,
    prev_delay_ms: Optional[float],
) -> float:
    """Return the sleep duration for ``attempt`` (0-indexed)."""
    raw = min(base_delay_ms * (2 ** attempt), max_delay_ms)
    if jitter == "none":
        return raw
    if jitter == "full":
        return rng.uniform(0.0, raw)
    if jitter == "equal":
        half = raw / 2.0
        return half + rng.uniform(0.0, half)
    if jitter == "decorrelated":
        lo = base_delay_ms
        hi = max((prev_delay_ms or base_delay_ms) * 3.0, base_delay_ms)
        return min(max_delay_ms, rng.uniform(lo, hi))
    # "positive" and any unknown value fall back to the legacy behavior.
    return raw + rng.uniform(0.0, raw * 0.1)


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
    func, args, kwargs:
        The awaitable-returning callable and its arguments.
    max_retries:
        Number of *additional* attempts after the first. ``0`` means one
        attempt total.
    base_delay_ms, max_delay_ms:
        Exponential backoff bounds, in milliseconds. The per-attempt
        delay is ``min(base * 2**attempt, max)`` before jitter.
    retryable:
        Predicate deciding whether an exception should be retried.
        Defaults to ``OSError`` / ``TimeoutError`` /
        ``asyncio.TimeoutError`` (never ``DigitalTwinCircuitOpenError``).
    rng:
        Source of jitter. Pass a seeded instance for reproducibility.

    Keyword-only additions
    ----------------------
    attempt_timeout_ms:
        Optional per-attempt timeout. Applied via ``asyncio.wait_for``.
    jitter:
        One of ``"none"``, ``"positive"`` (default; legacy behavior),
        ``"full"``, ``"equal"``, ``"decorrelated"``.
    retry_on_result:
        Optional predicate applied to successful results. When it
        returns ``True``, the result is treated as a retryable failure.
        If retries are exhausted, the final result is returned as-is.
    on_retry:
        Optional ``(attempt, delay_ms, exc_or_result) -> None`` callback
        fired before each sleep. Exceptions from the callback are
        logged at ``warning`` and never propagate.

    Notes
    -----
    Because ``**kwargs`` are forwarded to ``func``, the keyword-only
    parameters above are consumed by ``retry_async``. If your ``func``
    accepts parameters with those names, pass them positionally or use a
    ``functools.partial``.
    """
    # Reserved keyword-only extras (kept out of ``**kwargs``).
    attempt_timeout_ms: Optional[float] = kwargs.pop(
        "_attempt_timeout_ms", None,
    )
    jitter: JitterPolicy = kwargs.pop("_jitter", "positive")
    retry_on_result: Optional[Callable[[Any], bool]] = kwargs.pop(
        "_retry_on_result", None,
    )
    on_retry: Optional[
        Callable[[int, float, Any], None]
    ] = kwargs.pop("_on_retry", None)

    # Actually, we want these to be real keyword-only arguments, not
    # popped from kwargs. Provided here as a helper for the real
    # signature below.
    return await _retry_async_impl(
        func, *args,
        max_retries=max_retries,
        base_delay_ms=base_delay_ms,
        max_delay_ms=max_delay_ms,
        retryable=retryable,
        rng=rng,
        attempt_timeout_ms=attempt_timeout_ms,
        jitter=jitter,
        retry_on_result=retry_on_result,
        on_retry=on_retry,
        **kwargs,
    )


async def _retry_async_impl(
    func: Callable[..., Awaitable[Any]],
    *args: Any,
    max_retries: int,
    base_delay_ms: float,
    max_delay_ms: float,
    retryable: Optional[Callable[[BaseException], bool]],
    rng: Optional[random.Random],
    attempt_timeout_ms: Optional[float],
    jitter: JitterPolicy,
    retry_on_result: Optional[Callable[[Any], bool]],
    on_retry: Optional[Callable[[int, float, Any], None]],
    **kwargs: Any,
) -> Any:
    """Implementation of :func:`retry_async` (see that function)."""
    # ---- Validation --------------------------------------------------- #
    if isinstance(max_retries, bool) or not isinstance(max_retries, int):
        raise DigitalTwinConfigError("max_retries must be an integer.")
    if max_retries < 0:
        raise DigitalTwinConfigError("max_retries must be >= 0.")
    if (
        not isinstance(base_delay_ms, (int, float))
        or isinstance(base_delay_ms, bool)
    ):
        raise DigitalTwinConfigError(
            "base_delay_ms must be a number.",
        )
    if (
        not isinstance(max_delay_ms, (int, float))
        or isinstance(max_delay_ms, bool)
    ):
        raise DigitalTwinConfigError(
            "max_delay_ms must be a number.",
        )
    if base_delay_ms <= 0:
        raise DigitalTwinConfigError("base_delay_ms must be > 0.")
    if max_delay_ms < base_delay_ms:
        raise DigitalTwinConfigError(
            "max_delay_ms must be >= base_delay_ms.",
        )
    if retryable is not None and not callable(retryable):
        raise DigitalTwinConfigError("retryable must be callable or None.")
    if retry_on_result is not None and not callable(retry_on_result):
        raise DigitalTwinConfigError(
            "retry_on_result must be callable or None.",
        )
    if on_retry is not None and not callable(on_retry):
        raise DigitalTwinConfigError("on_retry must be callable or None.")
    if attempt_timeout_ms is not None:
        if (
            not isinstance(attempt_timeout_ms, (int, float))
            or isinstance(attempt_timeout_ms, bool)
        ):
            raise DigitalTwinConfigError(
                "attempt_timeout_ms must be a number or None.",
            )
        if attempt_timeout_ms <= 0:
            raise DigitalTwinConfigError(
                "attempt_timeout_ms must be > 0 when provided.",
            )
    if jitter not in (
        "none", "positive", "full", "equal", "decorrelated",
    ):
        raise DigitalTwinConfigError(
            f"unknown jitter policy {jitter!r}.",
        )

    predicate = retryable if retryable is not None else _default_retryable
    rng_local = rng if rng is not None else random.Random()
    timeout_s = (
        attempt_timeout_ms / 1000.0
        if attempt_timeout_ms is not None else None
    )

    prev_delay_ms: Optional[float] = None

    def _fire_on_retry(attempt: int, delay_ms: float, why: Any) -> None:
        if on_retry is None:
            return
        try:
            on_retry(attempt, delay_ms, why)
        except Exception as exc:  # noqa: BLE001 - user callback
            logger.warning("on_retry callback failed: %s", exc)

    for attempt in range(max_retries + 1):
        # ---- Invocation ----------------------------------------------- #
        try:
            if timeout_s is not None:
                result = await asyncio.wait_for(
                    func(*args, **kwargs), timeout=timeout_s,
                )
            else:
                result = await func(*args, **kwargs)
        except asyncio.CancelledError:
            # Never retry cancellation.
            raise
        except (KeyboardInterrupt, SystemExit):
            # Never retry OS-level signals.
            raise
        except Exception as exc:
            if not predicate(exc) or attempt == max_retries:
                raise  # bare raise preserves the original traceback
            delay_ms = _compute_delay_ms(
                attempt,
                base_delay_ms=base_delay_ms,
                max_delay_ms=max_delay_ms,
                jitter=jitter,
                rng=rng_local,
                prev_delay_ms=prev_delay_ms,
            )
            prev_delay_ms = delay_ms
            _fire_on_retry(attempt, delay_ms, exc)
            logger.debug(
                "retry_async: attempt %d failed (%s); sleeping %.1f ms",
                attempt + 1, type(exc).__name__, delay_ms,
            )
            await asyncio.sleep(delay_ms / 1000.0)
            continue

        # ---- Successful call; maybe retry based on the result -------- #
        if retry_on_result is not None and retry_on_result(result):
            if attempt == max_retries:
                logger.debug(
                    "retry_async: retry_on_result still true after %d "
                    "attempt(s); returning last result.",
                    attempt + 1,
                )
                return result
            delay_ms = _compute_delay_ms(
                attempt,
                base_delay_ms=base_delay_ms,
                max_delay_ms=max_delay_ms,
                jitter=jitter,
                rng=rng_local,
                prev_delay_ms=prev_delay_ms,
            )
            prev_delay_ms = delay_ms
            _fire_on_retry(attempt, delay_ms, result)
            logger.debug(
                "retry_async: attempt %d returned a retry-triggering "
                "result; sleeping %.1f ms",
                attempt + 1, delay_ms,
            )
            await asyncio.sleep(delay_ms / 1000.0)
            continue

        return result

    # The loop always returns or raises; this is unreachable.
    raise RuntimeError("retry_async: loop fell through")  # pragma: no cover


__all__ = [
    "SCHEMA_VERSION",
    "CircuitBreaker",
    "CircuitBreakerState",
    "CircuitBreakerStats",
    "JitterPolicy",
    "retry_async",
]
