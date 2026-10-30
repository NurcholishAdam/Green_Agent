# src/qcs/coherence_manager.py

"""
Quantum-coherence window tracking (v2).

Responsibilities
----------------
* Track qubit coherence windows on a specific QPU.
* Project fidelity decay over the window (exponential, calibrated to a
  configurable half-life).
* Emit alerts on fidelity loss, imminent expiry, and decoherence.
* Recover windows to "active" when conditions clear (never latch).
* Provide a bounded alert history for auditing and dashboards.

v2 fixes over v1
----------------

* **Logger was undefined.** Every error path raised ``NameError``.
  Now ``logger = logging.getLogger(__name__)`` at module scope.

* **Monitoring task was discarded.** ``stop()`` could not cancel the
  loop; ``start()`` twice created two loops. Now ``self._task`` is
  tracked, ``stop()`` awaits cancellation, and ``start()`` is
  idempotent.

* **Alert storm.** Once a window crossed ``fidelity_threshold``, every
  monitoring tick re-fired the same alert. Now alerts fire only on
  **escalation** (type change or severity increase), with an optional
  critical-refire cooldown.

* **``"decohered"`` was unreachable.** The old ``if/elif`` chain had
  overlapping conditions such that a truly decohered window was
  reported as ``"expiring"`` with a warning. Now the priority order is
  correct (decohered → low_fidelity → expiring_soon → active) and the
  severities match: decohered/low_fidelity are critical,
  expiring_soon is warning.

* **Windows could not recover.** The old status was a one-way latch.
  Now a window transitions back to ``"active"`` when the underlying
  conditions clear.

* **Fidelity model was inconsistent.** The constructor set
  ``current_fidelity = initial_fidelity`` but the projection at
  ``t=0`` yielded ``1.0``. Now both use the same exponential model
  ``f(t) = initial_fidelity · 2^(-t/half_life_ms)``.

* **``measured_fidelity`` did not recalibrate the model.**
  Now the half-life is recomputed from the measured point.

* **Time source was split.** Callers could not trust ``elapsed_ms``
  because ``update_coherence`` used the caller's value but
  ``get_coherence_status`` used wall clock. Now the caller's value is
  authoritative; wall clock is exposed separately as
  ``real_elapsed_ms``.

* **No input validation.** ``expected_duration_ms=0`` raised
  ``ZeroDivisionError``; negative fidelities and non-positive
  half-lives produced nonsense. All validated.

* **No thread safety.** ``_active_windows`` was mutated concurrently
  by the monitoring loop and by caller code. Now every mutation is
  guarded by a ``threading.RLock``.

* **Callbacks were sync-only and unbounded.** A slow callback stalled
  the monitoring loop. Now async callbacks are awaited and sync
  callbacks are wrapped in ``asyncio.to_thread`` with a timeout.

* **Monitoring loop re-raised on every error.** Now it distinguishes
  cancellation from real errors, caps consecutive failures, and gives
  up loudly after N failures instead of spinning silently.

* **IDs could collide.** Two windows created in the same microsecond
  produced the same ID. Now a monotonic counter + ``time.monotonic_ns``
  + ``hashlib`` guarantees uniqueness.

New public API
--------------

* ``list_windows()`` — snapshot of every active window, sorted by
  urgency.
* ``get_recent_alerts(n)`` — bounded alert history for dashboards.
* ``health()`` — readiness, task state, counters.
* ``metrics()`` — bounded counters and per-type alert tally.
* ``unregister_alert_callback``, ``clear_alert_callbacks``.
* ``async with CoherenceManager(...) as mgr:`` — lifecycle-managed.
* ``WindowStatus``, ``AlertType``, ``AlertSeverity`` enums (str mixin
  keeps backward compatibility with raw strings).
"""

from __future__ import annotations

import asyncio
import hashlib
import inspect
import logging
import math
import threading
import time
from collections import defaultdict, deque
from dataclasses import dataclass, field, replace
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Callable, Deque, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)

__version__ = "2.0.0"


# =============================================================================
# Helpers
# =============================================================================

def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


# =============================================================================
# Enums
# =============================================================================

class WindowStatus(str, Enum):
    """Lifecycle states a coherence window can be in."""
    ACTIVE = "active"
    EXPIRING = "expiring"
    DECOHERED = "decohered"
    COMPLETED = "completed"
    FAILED = "failed"


class AlertType(str, Enum):
    """Category of coherence alert."""
    LOW_FIDELITY = "low_fidelity"
    EXPIRING_SOON = "expiring_soon"
    DECOHERED = "decohered"


class AlertSeverity(str, Enum):
    WARNING = "warning"
    CRITICAL = "critical"


# Priority ordering (higher rank = more severe).
_SEVERITY_RANK: Dict[str, int] = {
    AlertSeverity.WARNING.value: 1,
    AlertSeverity.CRITICAL.value: 2,
}


# =============================================================================
# Data classes
# =============================================================================

@dataclass
class CoherenceWindow:
    """Tracked quantum-coherence window.

    Fidelity decays exponentially from ``initial_fidelity`` with
    half-life ``half_life_ms``:

        fidelity(t) = initial_fidelity * 2 ** (-t / half_life_ms)

    The stored ``decoherence_rate`` is the exponential decay constant
    ``lambda = ln(2) / half_life_ms`` (per millisecond).
    """

    window_id: str
    qpu_id: str
    start_time: datetime
    expected_duration_ms: float
    remaining_duration_ms: float
    current_fidelity: float
    initial_fidelity: float
    decoherence_rate: float
    half_life_ms: float
    status: str = WindowStatus.ACTIVE.value
    start_monotonic: float = field(default_factory=time.monotonic)
    last_update_at: Optional[datetime] = None
    last_alert_type: Optional[str] = None
    last_alert_at: Optional[datetime] = None
    last_alert_severity: Optional[str] = None
    ended_at: Optional[datetime] = None

    def fidelity_at(self, elapsed_ms: float) -> float:
        """Projected fidelity at ``elapsed_ms`` under exponential decay."""
        if self.half_life_ms <= 0:
            return max(0.0, min(1.0, self.initial_fidelity))
        return max(
            0.0,
            min(1.0, self.initial_fidelity * (2.0 ** (-elapsed_ms / self.half_life_ms))),
        )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "window_id": self.window_id,
            "qpu_id": self.qpu_id,
            "start_time": self.start_time.isoformat(),
            "expected_duration_ms": self.expected_duration_ms,
            "remaining_duration_ms": self.remaining_duration_ms,
            "current_fidelity": self.current_fidelity,
            "initial_fidelity": self.initial_fidelity,
            "decoherence_rate": self.decoherence_rate,
            "half_life_ms": self.half_life_ms,
            "status": self.status,
            "last_update_at": (
                self.last_update_at.isoformat()
                if self.last_update_at else None
            ),
            "last_alert_type": self.last_alert_type,
            "last_alert_severity": self.last_alert_severity,
            "last_alert_at": (
                self.last_alert_at.isoformat()
                if self.last_alert_at else None
            ),
            "ended_at": self.ended_at.isoformat() if self.ended_at else None,
        }

    def __repr__(self) -> str:
        return (
            f"CoherenceWindow(id={self.window_id!r}, "
            f"status={self.status!r}, "
            f"fidelity={self.current_fidelity:.3f}, "
            f"remaining={self.remaining_duration_ms:.1f}ms)"
        )


@dataclass
class CoherenceAlert:
    """Alert emitted by the coherence manager."""

    alert_id: str
    window_id: str
    alert_type: str
    severity: str
    remaining_time_ms: float
    current_fidelity: float
    timestamp: datetime
    recommended_action: str
    qpu_id: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "alert_id": self.alert_id,
            "window_id": self.window_id,
            "qpu_id": self.qpu_id,
            "alert_type": self.alert_type,
            "severity": self.severity,
            "remaining_time_ms": self.remaining_time_ms,
            "current_fidelity": self.current_fidelity,
            "timestamp": self.timestamp.isoformat(),
            "recommended_action": self.recommended_action,
        }

    def __str__(self) -> str:
        return (
            f"[{self.severity.upper()}] {self.alert_type} "
            f"window={self.window_id} "
            f"fidelity={self.current_fidelity:.3f} "
            f"remaining={self.remaining_time_ms:.1f}ms"
        )


# =============================================================================
# Manager
# =============================================================================

class CoherenceManager:
    """Quantum-coherence window tracker.

    Parameters
    ----------
    qpu_id
        Identifier of the QPU this manager is responsible for.
    coherence_threshold_ms
        Reserved for backward compatibility; currently unused.
    fidelity_threshold
        Fidelity below which ``low_fidelity`` alerts fire. Must be in
        ``(critical_fidelity_floor, 1]``.
    warning_buffer_ms
        Remaining-time threshold below which ``expiring_soon`` fires.
    critical_fidelity_floor
        Fidelity below which the window is considered decohered. Must be
        in ``(0, fidelity_threshold)``.
    default_half_life_ms
        Default half-life for new windows. ``None`` → equals the
        window's ``expected_duration_ms``.
    poll_interval_s
        Monitoring loop cadence.
    alert_callback_timeout_s
        Per-callback timeout.
    max_consecutive_loop_errors
        After this many consecutive errors, the monitoring loop gives
        up (rather than spinning silently).
    max_alert_history
        Bound on ``get_recent_alerts``.
    enable_critical_refire
        If True, critical alerts re-fire after
        ``critical_refire_interval_s`` while still critical.
    critical_refire_interval_s
        Cooldown between re-fires.
    """

    def __init__(
        self,
        qpu_id: str,
        coherence_threshold_ms: float = 100.0,
        fidelity_threshold: float = 0.95,
        warning_buffer_ms: float = 10.0,
        *,
        critical_fidelity_floor: float = 0.5,
        default_half_life_ms: Optional[float] = None,
        poll_interval_s: float = 1.0,
        alert_callback_timeout_s: float = 2.0,
        max_consecutive_loop_errors: int = 5,
        max_alert_history: int = 1024,
        enable_critical_refire: bool = False,
        critical_refire_interval_s: float = 60.0,
    ):
        # ---- Validation ----
        if not isinstance(qpu_id, str) or not qpu_id:
            raise ValueError("qpu_id must be a non-empty string")
        if not isinstance(fidelity_threshold, (int, float)) or not (
            0.0 < float(fidelity_threshold) <= 1.0
        ):
            raise ValueError(
                f"fidelity_threshold must be in (0, 1], got "
                f"{fidelity_threshold}"
            )
        if not isinstance(critical_fidelity_floor, (int, float)) or not (
            0.0 < float(critical_fidelity_floor) < float(fidelity_threshold)
        ):
            raise ValueError(
                f"critical_fidelity_floor must be in "
                f"(0, fidelity_threshold={fidelity_threshold}), got "
                f"{critical_fidelity_floor}"
            )
        if not isinstance(warning_buffer_ms, (int, float)) or warning_buffer_ms < 0:
            raise ValueError(
                f"warning_buffer_ms must be >= 0, got {warning_buffer_ms}"
            )
        if (
            default_half_life_ms is not None
            and (
                not isinstance(default_half_life_ms, (int, float))
                or default_half_life_ms <= 0
            )
        ):
            raise ValueError(
                f"default_half_life_ms must be > 0 or None, got "
                f"{default_half_life_ms}"
            )
        if not isinstance(poll_interval_s, (int, float)) or poll_interval_s <= 0:
            raise ValueError(
                f"poll_interval_s must be > 0, got {poll_interval_s}"
            )
        if not isinstance(max_consecutive_loop_errors, int) or max_consecutive_loop_errors <= 0:
            raise ValueError(
                f"max_consecutive_loop_errors must be a positive int, got "
                f"{max_consecutive_loop_errors}"
            )

        # ---- Config ----
        self.qpu_id: str = qpu_id
        self.coherence_threshold = float(coherence_threshold_ms)
        self.fidelity_threshold: float = float(fidelity_threshold)
        self.warning_buffer: float = float(warning_buffer_ms)
        self.critical_fidelity_floor: float = float(critical_fidelity_floor)
        self.default_half_life_ms: Optional[float] = (
            float(default_half_life_ms)
            if default_half_life_ms is not None else None
        )
        self.poll_interval_s: float = float(poll_interval_s)
        self.alert_callback_timeout_s: float = float(alert_callback_timeout_s)
        self.max_consecutive_loop_errors: int = int(max_consecutive_loop_errors)
        self.max_alert_history: int = int(max_alert_history)
        self.enable_critical_refire: bool = bool(enable_critical_refire)
        self.critical_refire_interval_s: float = float(critical_refire_interval_s)

        # ---- State ----
        self._lock: threading.RLock = threading.RLock()
        self._active_windows: Dict[str, CoherenceWindow] = {}
        self._alert_callbacks: List[Callable[..., Any]] = []
        self._alert_history: Deque[CoherenceAlert] = deque(
            maxlen=self.max_alert_history,
        )
        self._running: bool = False
        self._task: Optional[asyncio.Task] = None
        self._id_counter: int = 0
        self._started_at: Optional[datetime] = None
        self._last_loop_at: Optional[datetime] = None

        self._metrics: Dict[str, Any] = {
            "windows_started": 0,
            "windows_ended": 0,
            "alerts_emitted": 0,
            "alerts_by_type": defaultdict(int),
            "loop_iterations": 0,
            "loop_errors": 0,
            "callback_timeouts": 0,
            "callback_errors": 0,
        }

        logger.info(
            "CoherenceManager initialized (qpu=%s, fidelity_threshold=%.2f, "
            "critical_floor=%.2f, warning_buffer_ms=%.1f)",
            qpu_id, self.fidelity_threshold,
            self.critical_fidelity_floor, self.warning_buffer,
        )

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def start(self) -> None:
        """Start the background monitoring loop. Idempotent."""
        with self._lock:
            if self._task is not None and not self._task.done():
                logger.warning(
                    "CoherenceManager.start() called while already running "
                    "(qpu=%s)", self.qpu_id,
                )
                return
            self._running = True
            self._started_at = _utcnow()
            self._task = asyncio.create_task(
                self._monitoring_loop(),
                name=f"coherence-monitor-{self.qpu_id}",
            )
        logger.info("Coherence monitoring started (qpu=%s)", self.qpu_id)

    async def stop(self) -> None:
        """Stop the background monitoring loop. Idempotent."""
        self._running = False
        with self._lock:
            task = self._task
            self._task = None
        if task is not None and not task.done():
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
            except Exception:
                logger.exception(
                    "Monitoring task raised during shutdown (qpu=%s)",
                    self.qpu_id,
                )
        logger.info("Coherence monitoring stopped (qpu=%s)", self.qpu_id)

    async def __aenter__(self) -> "CoherenceManager":
        await self.start()
        return self

    async def __aexit__(self, *exc_info: Any) -> None:
        await self.stop()

    def close_sync(self) -> None:
        """Best-effort sync teardown (e.g. from a signal handler).

        Sets ``_running = False`` so the loop exits at its next
        iteration. Does not await the task; prefer ``await stop()``.
        """
        self._running = False
        logger.info(
            "CoherenceManager.close_sync() called (qpu=%s); "
            "task will exit at next iteration", self.qpu_id,
        )

    # ------------------------------------------------------------------
    # Callback management
    # ------------------------------------------------------------------

    def register_alert_callback(self, callback: Callable[..., Any]) -> None:
        """Register a callback to receive :class:`CoherenceAlert` objects.

        Callbacks may be sync or async. Sync callbacks are executed in a
        worker thread with a timeout so a slow callback cannot stall the
        monitoring loop.
        """
        if not callable(callback):
            raise TypeError("callback must be callable")
        with self._lock:
            if callback not in self._alert_callbacks:
                self._alert_callbacks.append(callback)
        logger.debug(
            "Registered alert callback %r (qpu=%s, total=%d)",
            callback, self.qpu_id, len(self._alert_callbacks),
        )

    def unregister_alert_callback(self, callback: Callable[..., Any]) -> bool:
        """Remove a previously registered callback. Returns True if found."""
        with self._lock:
            try:
                self._alert_callbacks.remove(callback)
                return True
            except ValueError:
                return False

    def clear_alert_callbacks(self) -> None:
        with self._lock:
            self._alert_callbacks.clear()

    # ------------------------------------------------------------------
    # Window management
    # ------------------------------------------------------------------

    async def start_coherence_window(
        self,
        expected_duration_ms: float = 100.0,
        initial_fidelity: float = 0.99,
        half_life_ms: Optional[float] = None,
    ) -> str:
        """Create a new coherence window.

        Parameters
        ----------
        expected_duration_ms
            Nominal duration of the window. Must be > 0.
        initial_fidelity
            Fidelity at ``t = 0``. Must be in ``(0, 1]``.
        half_life_ms
            Half-life of the exponential decay. Defaults to
            ``default_half_life_ms`` (or ``expected_duration_ms`` if that
            is unset). Must be > 0.

        Returns
        -------
        str
            The generated ``window_id``.
        """
        # ---- Validation ----
        if (
            not isinstance(expected_duration_ms, (int, float))
            or not math.isfinite(expected_duration_ms)
            or expected_duration_ms <= 0
        ):
            raise ValueError(
                f"expected_duration_ms must be a finite positive number, "
                f"got {expected_duration_ms!r}"
            )
        if (
            not isinstance(initial_fidelity, (int, float))
            or not math.isfinite(initial_fidelity)
            or not (0.0 < initial_fidelity <= 1.0)
        ):
            raise ValueError(
                f"initial_fidelity must be a finite number in (0, 1], "
                f"got {initial_fidelity!r}"
            )
        if half_life_ms is None:
            half_life_ms = (
                self.default_half_life_ms
                if self.default_half_life_ms is not None
                else float(expected_duration_ms)
            )
        if (
            not isinstance(half_life_ms, (int, float))
            or not math.isfinite(half_life_ms)
            or half_life_ms <= 0
        ):
            raise ValueError(
                f"half_life_ms must be a finite positive number, "
                f"got {half_life_ms!r}"
            )

        window_id = self._generate_window_id()
        decoherence_rate = math.log(2.0) / float(half_life_ms)

        window = CoherenceWindow(
            window_id=window_id,
            qpu_id=self.qpu_id,
            start_time=_utcnow(),
            expected_duration_ms=float(expected_duration_ms),
            remaining_duration_ms=float(expected_duration_ms),
            current_fidelity=float(initial_fidelity),
            initial_fidelity=float(initial_fidelity),
            decoherence_rate=decoherence_rate,
            half_life_ms=float(half_life_ms),
            status=WindowStatus.ACTIVE.value,
            start_monotonic=time.monotonic(),
            last_update_at=_utcnow(),
        )

        with self._lock:
            self._active_windows[window_id] = window
            self._metrics["windows_started"] += 1

        logger.info(
            "Started coherence window %s (qpu=%s, duration_ms=%.1f, "
            "initial_fidelity=%.3f, half_life_ms=%.1f)",
            window_id, self.qpu_id, expected_duration_ms,
            initial_fidelity, half_life_ms,
        )
        return window_id

    async def update_coherence(
        self,
        window_id: str,
        elapsed_ms: float,
        measured_fidelity: Optional[float] = None,
    ) -> CoherenceWindow:
        """Update a window with the caller-claimed elapsed time.

        The caller's ``elapsed_ms`` is authoritative for the fidelity
        projection and ``remaining_duration_ms``. ``get_coherence_status``
        reports the caller-claimed value plus a separate wall-clock
        ``real_elapsed_ms`` for staleness detection.

        If ``measured_fidelity`` is provided, the window's half-life is
        **recalibrated** from the observed point; subsequent projections
        use the updated model.
        """
        if not isinstance(elapsed_ms, (int, float)) or not math.isfinite(elapsed_ms):
            raise ValueError(
                f"elapsed_ms must be finite, got {elapsed_ms!r}"
            )
        if elapsed_ms < 0:
            raise ValueError(
                f"elapsed_ms must be >= 0, got {elapsed_ms!r}"
            )
        if measured_fidelity is not None and (
            not isinstance(measured_fidelity, (int, float))
            or not math.isfinite(measured_fidelity)
            or not (0.0 <= measured_fidelity <= 1.0)
        ):
            raise ValueError(
                f"measured_fidelity must be in [0, 1], got "
                f"{measured_fidelity!r}"
            )

        fire_snapshot: Optional[Tuple[CoherenceWindow, str, str]] = None

        with self._lock:
            window = self._active_windows.get(window_id)
            if window is None:
                raise ValueError(f"Window {window_id} not found")

            # ---- Update remaining time ----
            window.remaining_duration_ms = max(
                0.0, window.expected_duration_ms - float(elapsed_ms),
            )

            # ---- Update fidelity ----
            if measured_fidelity is not None:
                window.current_fidelity = float(measured_fidelity)
                # Recalibrate half-life from the measured point.
                if (
                    elapsed_ms > 0
                    and 0.0 < window.current_fidelity < window.initial_fidelity
                ):
                    ratio = window.current_fidelity / window.initial_fidelity
                    window.half_life_ms = elapsed_ms / -math.log2(ratio)
                    window.decoherence_rate = (
                        math.log(2.0) / window.half_life_ms
                    )
            else:
                window.current_fidelity = window.fidelity_at(elapsed_ms)

            window.last_update_at = _utcnow()

            # ---- Evaluate state ----
            new_status, alert_type, severity = self._evaluate_state(window)
            status_changed = new_status != window.status
            window.status = new_status
            if status_changed:
                logger.debug(
                    "Window %s status → %s", window_id, new_status,
                )

            # ---- Decide alert ----
            if alert_type is not None and self._should_fire(
                window, alert_type, severity,
            ):
                window.last_alert_type = alert_type
                window.last_alert_at = _utcnow()
                window.last_alert_severity = severity
                fire_snapshot = (replace(window), alert_type, severity)

            snapshot = replace(window)

        # ---- Fire alert OUTSIDE the lock (avoids deadlocks if the
        #      callback calls back into the manager) ----
        if fire_snapshot is not None:
            win, atype, sev = fire_snapshot
            await self._trigger_alert(win, atype, sev)

        return snapshot

    async def end_coherence_window(
        self,
        window_id: str,
        final_fidelity: float,
        successful: bool = True,
    ) -> Optional[CoherenceWindow]:
        """End a window. Returns the ended window (or None if unknown)."""
        if (
            not isinstance(final_fidelity, (int, float))
            or not math.isfinite(final_fidelity)
            or not (0.0 <= final_fidelity <= 1.0)
        ):
            raise ValueError(
                f"final_fidelity must be in [0, 1], got {final_fidelity!r}"
            )

        with self._lock:
            window = self._active_windows.pop(window_id, None)
            if window is None:
                logger.warning(
                    "end_coherence_window: unknown window_id=%s",
                    window_id,
                )
                return None
            window.current_fidelity = float(final_fidelity)
            window.status = (
                WindowStatus.COMPLETED.value
                if successful else WindowStatus.FAILED.value
            )
            window.ended_at = _utcnow()
            self._metrics["windows_ended"] += 1
            snapshot = replace(window)

        logger.info(
            "Ended coherence window %s (status=%s, final_fidelity=%.3f)",
            window_id, snapshot.status, final_fidelity,
        )
        return snapshot

    async def get_coherence_status(self, window_id: str) -> Dict[str, Any]:
        """Return a status snapshot for the given window."""
        with self._lock:
            window = self._active_windows.get(window_id)
            if window is None:
                return {"window_id": window_id, "status": "not_found"}
            real_elapsed_ms = (
                time.monotonic() - window.start_monotonic
            ) * 1000.0
            claimed_elapsed_ms = (
                window.expected_duration_ms - window.remaining_duration_ms
            )
            return {
                "window_id": window_id,
                "qpu_id": window.qpu_id,
                "status": window.status,
                # Caller-claimed elapsed (source of truth for fidelity math).
                "elapsed_ms": max(0.0, claimed_elapsed_ms),
                # Wall-clock elapsed since window creation (informational;
                # if it diverges from ``elapsed_ms``, the caller has not
                # been updating the window).
                "real_elapsed_ms": max(0.0, real_elapsed_ms),
                "remaining_ms": window.remaining_duration_ms,
                "expected_duration_ms": window.expected_duration_ms,
                "current_fidelity": window.current_fidelity,
                "initial_fidelity": window.initial_fidelity,
                "fidelity_threshold": self.fidelity_threshold,
                "critical_fidelity_floor": self.critical_fidelity_floor,
                "half_life_ms": window.half_life_ms,
                "utilization_percent": (
                    (claimed_elapsed_ms / window.expected_duration_ms) * 100.0
                    if window.expected_duration_ms > 0 else 0.0
                ),
                "last_update_at": (
                    window.last_update_at.isoformat()
                    if window.last_update_at else None
                ),
                "last_alert_type": window.last_alert_type,
                "last_alert_severity": window.last_alert_severity,
            }

    def list_windows(self) -> List[Dict[str, Any]]:
        """Snapshot of every active window, most urgent first."""
        with self._lock:
            windows = [replace(w) for w in self._active_windows.values()]

        def _urgency(w: CoherenceWindow) -> Tuple[int, float]:
            # Lower status rank = more urgent.
            rank = {
                WindowStatus.DECOHERED.value: 0,
                WindowStatus.EXPIRING.value: 1,
                WindowStatus.ACTIVE.value: 2,
            }.get(w.status, 3)
            return (rank, w.remaining_duration_ms)

        windows.sort(key=_urgency)
        return [
            {
                "window_id": w.window_id,
                "status": w.status,
                "current_fidelity": w.current_fidelity,
                "remaining_ms": w.remaining_duration_ms,
                "half_life_ms": w.half_life_ms,
                "last_alert_type": w.last_alert_type,
                "last_alert_severity": w.last_alert_severity,
            }
            for w in windows
        ]

    # ------------------------------------------------------------------
    # Alert history
    # ------------------------------------------------------------------

    def get_recent_alerts(self, n: int = 50) -> List[CoherenceAlert]:
        """Return up to ``n`` most-recent alerts (oldest-first)."""
        with self._lock:
            history = list(self._alert_history)
        return history[-n:] if n > 0 else []

    # ------------------------------------------------------------------
    # Observability
    # ------------------------------------------------------------------

    def health(self) -> Dict[str, Any]:
        with self._lock:
            n_active = len(self._active_windows)
            n_decohered = sum(
                1 for w in self._active_windows.values()
                if w.status == WindowStatus.DECOHERED.value
            )
            n_expiring = sum(
                1 for w in self._active_windows.values()
                if w.status == WindowStatus.EXPIRING.value
            )
            task = self._task
            task_alive = task is not None and not task.done()
            task_exc: Optional[str] = None
            if task is not None and task.done():
                try:
                    exc = task.exception()
                    if exc is not None:
                        task_exc = f"{type(exc).__name__}: {exc}"
                except (asyncio.CancelledError, Exception):
                    pass
            return {
                "qpu_id": self.qpu_id,
                "ready": self._running and task_alive,
                "running": self._running,
                "task_alive": task_alive,
                "task_exception": task_exc,
                "active_windows": n_active,
                "expiring_windows": n_expiring,
                "decohered_windows": n_decohered,
                "alerts_emitted": self._metrics["alerts_emitted"],
                "loop_iterations": self._metrics["loop_iterations"],
                "loop_errors": self._metrics["loop_errors"],
                "started_at": (
                    self._started_at.isoformat()
                    if self._started_at else None
                ),
                "last_loop_at": (
                    self._last_loop_at.isoformat()
                    if self._last_loop_at else None
                ),
            }

    def metrics(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "windows_started": self._metrics["windows_started"],
                "windows_ended": self._metrics["windows_ended"],
                "alerts_emitted": self._metrics["alerts_emitted"],
                "alerts_by_type": dict(self._metrics["alerts_by_type"]),
                "loop_iterations": self._metrics["loop_iterations"],
                "loop_errors": self._metrics["loop_errors"],
                "callback_timeouts": self._metrics["callback_timeouts"],
                "callback_errors": self._metrics["callback_errors"],
                "alert_history_size": len(self._alert_history),
                "active_windows": len(self._active_windows),
            }

    # ------------------------------------------------------------------
    # Internal — state machine
    # ------------------------------------------------------------------

    def _evaluate_state(
        self, window: CoherenceWindow,
    ) -> Tuple[str, Optional[str], Optional[str]]:
        """Determine (status, alert_type, severity) for the window.

        Priority order (highest severity first):

        1. **Decohered** (critical): ``remaining <= 0`` OR
           ``current_fidelity < critical_fidelity_floor``.
        2. **Low fidelity** (critical): ``current_fidelity <
           fidelity_threshold``.
        3. **Expiring soon** (warning): ``remaining < warning_buffer_ms``.
        4. **Active**: no alert.
        """
        if (
            window.remaining_duration_ms <= 0.0
            or window.current_fidelity < self.critical_fidelity_floor
        ):
            return (
                WindowStatus.DECOHERED.value,
                AlertType.DECOHERED.value,
                AlertSeverity.CRITICAL.value,
            )
        if window.current_fidelity < self.fidelity_threshold:
            return (
                WindowStatus.EXPIRING.value,
                AlertType.LOW_FIDELITY.value,
                AlertSeverity.CRITICAL.value,
            )
        if window.remaining_duration_ms < self.warning_buffer:
            return (
                WindowStatus.EXPIRING.value,
                AlertType.EXPIRING_SOON.value,
                AlertSeverity.WARNING.value,
            )
        return (WindowStatus.ACTIVE.value, None, None)

    def _should_fire(
        self,
        window: CoherenceWindow,
        alert_type: str,
        severity: str,
    ) -> bool:
        """Return True if a new alert should be emitted (dedupe gate).

        Fires when:

        * It's the first alert for this window.
        * The alert type changed (e.g. ``low_fidelity`` → ``decohered``).
        * The severity increased (e.g. ``warning`` → ``critical``).
        * The window is critical and the refire cooldown has elapsed
          (only if ``enable_critical_refire`` is True).
        """
        if window.last_alert_type is None:
            return True
        if window.last_alert_type != alert_type:
            return True
        prev_rank = _SEVERITY_RANK.get(window.last_alert_severity or "", 0)
        curr_rank = _SEVERITY_RANK.get(severity, 0)
        if curr_rank > prev_rank:
            return True
        if (
            self.enable_critical_refire
            and severity == AlertSeverity.CRITICAL.value
            and window.last_alert_at is not None
            and (_utcnow() - window.last_alert_at).total_seconds()
            >= self.critical_refire_interval_s
        ):
            return True
        return False

    def _get_recommended_action(self, alert_type: str) -> str:
        actions = {
            AlertType.LOW_FIDELITY.value: (
                "Consider error mitigation or fallback to classical"
            ),
            AlertType.EXPIRING_SOON.value: (
                "Complete current operation immediately"
            ),
            AlertType.DECOHERED.value: (
                "Fallback to classical simulation, reinitialize quantum state"
            ),
        }
        return actions.get(alert_type, "Monitor closely")

    # ------------------------------------------------------------------
    # Internal — alert emission
    # ------------------------------------------------------------------

    async def _trigger_alert(
        self,
        window: CoherenceWindow,
        alert_type: str,
        severity: str,
    ) -> None:
        alert = CoherenceAlert(
            alert_id=self._generate_alert_id(),
            window_id=window.window_id,
            alert_type=alert_type,
            severity=severity,
            remaining_time_ms=window.remaining_duration_ms,
            current_fidelity=window.current_fidelity,
            timestamp=_utcnow(),
            recommended_action=self._get_recommended_action(alert_type),
            qpu_id=window.qpu_id,
        )

        with self._lock:
            self._alert_history.append(alert)
            self._metrics["alerts_emitted"] += 1
            self._metrics["alerts_by_type"][alert_type] += 1
            callbacks = list(self._alert_callbacks)

        logger.warning("Coherence alert: %s", alert)

        await self._emit_callbacks(alert, callbacks)

    async def _emit_callbacks(
        self,
        alert: CoherenceAlert,
        callbacks: List[Callable[..., Any]],
    ) -> None:
        for cb in callbacks:
            try:
                if inspect.iscoroutinefunction(cb):
                    await asyncio.wait_for(
                        cb(alert),
                        timeout=self.alert_callback_timeout_s,
                    )
                else:
                    await asyncio.wait_for(
                        asyncio.to_thread(cb, alert),
                        timeout=self.alert_callback_timeout_s,
                    )
            except asyncio.TimeoutError:
                with self._lock:
                    self._metrics["callback_timeouts"] += 1
                logger.warning(
                    "Alert callback %r timed out after %.1fs",
                    cb, self.alert_callback_timeout_s,
                )
            except asyncio.CancelledError:
                raise
            except Exception:
                with self._lock:
                    self._metrics["callback_errors"] += 1
                logger.exception("Alert callback %r failed", cb)

    # ------------------------------------------------------------------
    # Internal — ID generation
    # ------------------------------------------------------------------

    def _next_id(self) -> int:
        with self._lock:
            self._id_counter += 1
            return self._id_counter

    def _generate_window_id(self) -> str:
        seq = self._next_id()
        raw = f"cw:{self.qpu_id}:{time.monotonic_ns()}:{seq}"
        return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:16]

    def _generate_alert_id(self) -> str:
        seq = self._next_id()
        raw = f"ca:{self.qpu_id}:{time.monotonic_ns()}:{seq}"
        return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:16]

    # ------------------------------------------------------------------
    # Internal — monitoring loop
    # ------------------------------------------------------------------

    async def _monitoring_loop(self) -> None:
        consecutive_errors = 0
        logger.info("Monitoring loop started (qpu=%s)", self.qpu_id)
        try:
            while self._running:
                try:
                    # ---- Snapshot windows under lock ----
                    with self._lock:
                        windows = list(self._active_windows.values())
                        self._last_loop_at = _utcnow()
                        self._metrics["loop_iterations"] += 1

                    # ---- Update each window ----
                    for w in windows:
                        # Skip if the window disappeared between snapshot
                        # and update (e.g. end_coherence_window ran).
                        with self._lock:
                            if w.window_id not in self._active_windows:
                                continue
                            elapsed_ms = (
                                time.monotonic() - w.start_monotonic
                            ) * 1000.0

                        try:
                            await self.update_coherence(
                                w.window_id, elapsed_ms,
                            )
                        except ValueError:
                            # Window was deleted mid-iteration; fine.
                            continue

                    consecutive_errors = 0
                    await asyncio.sleep(self.poll_interval_s)
                except asyncio.CancelledError:
                    raise
                except Exception:
                    consecutive_errors += 1
                    with self._lock:
                        self._metrics["loop_errors"] += 1
                    logger.exception(
                        "Coherence monitoring error (consecutive=%d, qpu=%s)",
                        consecutive_errors, self.qpu_id,
                    )
                    if consecutive_errors >= self.max_consecutive_loop_errors:
                        logger.critical(
                            "Coherence monitoring giving up after %d "
                            "consecutive errors (qpu=%s)",
                            consecutive_errors, self.qpu_id,
                        )
                        self._running = False
                        break
                    await asyncio.sleep(min(5.0 * consecutive_errors, 30.0))
        finally:
            logger.info("Monitoring loop exited (qpu=%s)", self.qpu_id)

    # ------------------------------------------------------------------
    # Repr
    # ------------------------------------------------------------------

    def __repr__(self) -> str:
        with self._lock:
            return (
                f"CoherenceManager(qpu={self.qpu_id!r}, "
                f"running={self._running}, "
                f"windows={len(self._active_windows)}, "
                f"alerts={self._metrics['alerts_emitted']})"
            )


__all__ = [
    "AlertSeverity",
    "AlertType",
    "CoherenceAlert",
    "CoherenceManager",
    "CoherenceWindow",
    "WindowStatus",
]


# =============================================================================
# Smoke test: python -m qcs.coherence_manager
# =============================================================================

if __name__ == "__main__":  # pragma: no cover
    logging.basicConfig(level=logging.INFO)

    def _expect_raises(fn, exc_type: type, label: str) -> None:
        try:
            fn()
        except exc_type as e:
            print(f"  ✓ {label}: {exc_type.__name__}: {e}")
        except Exception as e:
            print(
                f"  ✗ {label}: got {type(e).__name__} "
                f"(expected {exc_type.__name__}): {e}"
            )
        else:
            print(f"  ✗ {label}: did not raise (expected {exc_type.__name__})")

    async def _main() -> None:
        # ---- 1. Basic lifecycle ----
        print("\n=== Basic lifecycle ===")
        mgr = CoherenceManager(
            qpu_id="qpu-test-1",
            fidelity_threshold=0.95,
            warning_buffer_ms=10.0,
            critical_fidelity_floor=0.5,
            enable_critical_refire=False,
        )
        received: List[CoherenceAlert] = []

        def _sync_cb(alert: CoherenceAlert) -> None:
            received.append(alert)
            print(f"  [sync cb] {alert}")

        async def _async_cb(alert: CoherenceAlert) -> None:
            received.append(alert)
            print(f"  [async cb] {alert}")

        mgr.register_alert_callback(_sync_cb)
        mgr.register_alert_callback(_async_cb)

        wid = await mgr.start_coherence_window(
            expected_duration_ms=100.0,
            initial_fidelity=0.99,
            half_life_ms=50.0,
        )
        print(f"  started window: {wid}")
        assert wid in {w["window_id"] for w in mgr.list_windows()}

        # t=0 → initial fidelity, no alert
        w0 = await mgr.update_coherence(wid, 0.0)
        assert w0.current_fidelity == 0.99
        assert w0.status == WindowStatus.ACTIVE.value
        assert len(received) == 0

        # t=5 → fidelity drops slightly (still >= 0.95)
        w5 = await mgr.update_coherence(wid, 5.0)
        assert w5.status == WindowStatus.ACTIVE.value
        assert w5.current_fidelity >= 0.95
        assert len(received) == 0

        # t=20 → fidelity ≈ 0.99 * 2^(-20/50) ≈ 0.758 → low_fidelity (critical)
        w20 = await mgr.update_coherence(wid, 20.0)
        print(f"  t=20: fidelity={w20.current_fidelity:.3f} status={w20.status}")
        assert w20.status == WindowStatus.EXPIRING.value
        assert w20.last_alert_type == AlertType.LOW_FIDELITY.value
        assert w20.last_alert_severity == AlertSeverity.CRITICAL.value
        assert len(received) == 2  # both callbacks fired once
        assert received[-1].alert_type == AlertType.LOW_FIDELITY.value

        # t=30 → still expiring; alert should NOT re-fire (dedup)
        w30 = await mgr.update_coherence(wid, 30.0)
        assert w30.status == WindowStatus.EXPIRING.value
        assert len(received) == 2  # no re-fire

        # t=100 → remaining = 0 → decohered (critical, type change → fires)
        w100 = await mgr.update_coherence(wid, 100.0)
        print(f"  t=100: remaining={w100.remaining_duration_ms:.1f} "
              f"status={w100.status}")
        assert w100.status == WindowStatus.DECOHERED.value
        assert w100.last_alert_type == AlertType.DECOHERED.value
        assert len(received) == 4  # type change → re-fire

        # ---- 2. Recovery: fidelity restored → active ----
        print("\n=== Recovery to active ===")
        wid2 = await mgr.start_coherence_window(
            expected_duration_ms=100.0,
            initial_fidelity=0.99,
            half_life_ms=50.0,
        )
        # Drop into expiring via measured fidelity
        w = await mgr.update_coherence(wid2, 10.0, measured_fidelity=0.90)
        assert w.status == WindowStatus.EXPIRING.value
        n_alerts_after_drop = len(received)

        # Recover via measured fidelity
        w = await mgr.update_coherence(wid2, 15.0, measured_fidelity=0.99)
        print(f"  recovered: status={w.status} "
              f"fidelity={w.current_fidelity:.3f}")
        assert w.status == WindowStatus.ACTIVE.value

        # Alert fired on type change (low_fidelity → None means no fire);
        # because we don't emit an "all clear" alert, count stays the same.
        assert len(received) == n_alerts_after_drop

        # ---- 3. Priority ordering: decohered wins over low_fidelity ----
        print("\n=== Priority ordering ===")
        wid3 = await mgr.start_coherence_window(
            expected_duration_ms=10.0,
            initial_fidelity=0.99,
            half_life_ms=50.0,
        )
        # Force BOTH: elapsed > duration AND fidelity very low
        w = await mgr.update_coherence(
            wid3, 100.0, measured_fidelity=0.30,
        )
        print(f"  elapsed=100 duration=10 fidelity=0.30 → "
              f"status={w.status} alert={w.last_alert_type}")
        assert w.status == WindowStatus.DECOHERED.value
        assert w.last_alert_type == AlertType.DECOHERED.value

        # ---- 4. End window ----
        print("\n=== End window ===")
        ended = await mgr.end_coherence_window(
            wid3, final_fidelity=0.28, successful=False,
        )
        assert ended is not None
        assert ended.status == WindowStatus.FAILED.value
        assert ended.ended_at is not None
        # Ended windows disappear from the active set
        assert wid3 not in {w["window_id"] for w in mgr.list_windows()}
        # Calling again returns None (logged warning)
        ended_again = await mgr.end_coherence_window(
            wid3, final_fidelity=0.28, successful=False,
        )
        assert ended_again is None

        # ---- 5. Validation errors ----
        print("\n=== Validation errors ===")
        _expect_raises(
            lambda: CoherenceManager(qpu_id=""),
            ValueError, "empty qpu_id",
        )
        _expect_raises(
            lambda: CoherenceManager(
                qpu_id="qpu", fidelity_threshold=1.5,
            ),
            ValueError, "fidelity_threshold > 1",
        )
        _expect_raises(
            lambda: CoherenceManager(
                qpu_id="qpu", fidelity_threshold=0.95,
                critical_fidelity_floor=0.96,
            ),
            ValueError, "critical_fidelity_floor >= fidelity_threshold",
        )
        _expect_raises(
            lambda: asyncio.get_event_loop().run_until_complete(
                mgr.start_coherence_window(expected_duration_ms=0)
            ),
            ValueError, "expected_duration_ms=0",
        )
        _expect_raises(
            lambda: asyncio.get_event_loop().run_until_complete(
                mgr.start_coherence_window(initial_fidelity=1.5)
            ),
            ValueError, "initial_fidelity > 1",
        )
        _expect_raises(
            lambda: asyncio.get_event_loop().run_until_complete(
                mgr.update_coherence(wid2, -1.0)
            ),
            ValueError, "negative elapsed_ms",
        )
        _expect_raises(
            lambda: asyncio.get_event_loop().run_until_complete(
                mgr.update_coherence("nonexistent", 1.0)
            ),
            ValueError, "unknown window",
        )

        # ---- 6. Timeout on slow callback ----
        print("\n=== Slow callback timeout ===")
        import time as _time

        def _slow_cb(alert: CoherenceAlert) -> None:
            _time.sleep(5.0)  # will be cancelled by wait_for

        mgr2 = CoherenceManager(
            qpu_id="qpu-timeout",
            alert_callback_timeout_s=0.2,
        )
        mgr2.register_alert_callback(_slow_cb)
        wid_slow = await mgr2.start_coherence_window(
            expected_duration_ms=10.0, initial_fidelity=0.99,
            half_life_ms=5.0,
        )
        t0 = _time.monotonic()
        await mgr2.update_coherence(wid_slow, 5.0)
        elapsed = _time.monotonic() - t0
        print(f"  slow callback wall time: {elapsed:.2f}s (expected ~0.2s)")
        assert elapsed < 1.0  # did not block for 5s
        assert mgr2.metrics()["callback_timeouts"] >= 1

        # ---- 7. Lifecycle / monitoring loop ----
        print("\n=== Monitoring loop lifecycle ===")
        mgr3 = CoherenceManager(
            qpu_id="qpu-loop", poll_interval_s=0.05,
        )
        await mgr3.start()
        await mgr3.start()  # idempotent
        assert mgr3.health()["running"] is True
        assert mgr3.health()["task_alive"] is True
        await asyncio.sleep(0.15)
        assert mgr3.health()["loop_iterations"] >= 2
        await mgr3.stop()
        assert mgr3.health()["running"] is False
        assert mgr3.health()["task_alive"] is False

        # ---- 8. Context manager ----
        print("\n=== Context manager ===")
        async with CoherenceManager(qpu_id="qpu-ctx") as mgr_ctx:
            wid_ctx = await mgr_ctx.start_coherence_window()
            assert wid_ctx in {w["window_id"] for w in mgr_ctx.list_windows()}
        assert mgr_ctx.health()["running"] is False

        # ---- 9. Alert history + dedup on ID generation ----
        print("\n=== Alert history ===")
        recent = mgr.get_recent_alerts(10)
        print(f"  recent alerts: {len(recent)}")
        assert len(recent) >= 3

        # ---- 10. Repr + metrics ----
        print("\n=== Repr / metrics ===")
        print(f"  {mgr!r}")
        m = mgr.metrics()
        print(f"  metrics: {m}")
        assert m["windows_started"] >= 3
        assert m["alerts_emitted"] >= 3

        print("\nSmoke test passed.")

    asyncio.run(_main())
