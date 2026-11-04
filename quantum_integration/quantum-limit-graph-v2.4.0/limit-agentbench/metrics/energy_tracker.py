# -*- coding: utf-8 -*-
"""Sampling-based energy monitoring utilities.

Energy is computed by integrating measured power over elapsed time. Supply a
``power_reader`` callable that returns the current power draw in watts for
automatic sampling, or call :meth:`EnergyTracker.record_power` with measured
readings yourself. No power or energy values are fabricated when no readings
are available.
"""

from __future__ import annotations

import logging
import math
import threading
import time
from typing import Any, Callable, Dict, Optional

logger = logging.getLogger(__name__)

PowerReader = Callable[[], float]


def _non_negative_finite(value: Any, name: str) -> float:
    """Convert a value to a finite, non-negative float."""
    if isinstance(value, bool):
        raise ValueError(f"{name} must be a non-negative finite number")
    try:
        result = float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{name} must be a non-negative finite number") from exc
    if not math.isfinite(result) or result < 0:
        raise ValueError(f"{name} must be a non-negative finite number")
    return result


class EnergyTracker:
    """Measure power and integrate it into energy consumption.

    Args:
        power_reader: Optional zero-argument callable returning measured power
            in watts. When provided, a background sampler calls it at the
            configured interval. Without it, readings can be supplied manually
            with :meth:`record_power`.
        sampling_interval: Seconds between automatic readings; must be > 0.

    Energy is accumulated using trapezoidal integration and reported in kWh.
    The tracker is a context manager, and ``start``/``stop`` can also be used
    directly. It is safe to submit readings from another thread.
    """

    def __init__(
        self,
        power_reader: Optional[PowerReader] = None,
        sampling_interval: float = 1.0,
    ) -> None:
        self.sampling_interval = _non_negative_finite(
            sampling_interval, "sampling_interval"
        )
        if self.sampling_interval == 0:
            raise ValueError("sampling_interval must be greater than zero")
        if power_reader is not None and not callable(power_reader):
            raise TypeError("power_reader must be callable or None")

        self.power_reader = power_reader
        self._lock = threading.RLock()
        self._stop_event = threading.Event()
        self._sampler_thread: Optional[threading.Thread] = None
        self._running = False
        self._start_time: Optional[float] = None
        self._end_time: Optional[float] = None
        self._last_sample_time: Optional[float] = None
        self._last_power_watts: Optional[float] = None
        self._energy_joules = 0.0
        self._peak_power_watts = 0.0
        self._sample_count = 0
        self._operation_count = 0

    @property
    def is_running(self) -> bool:
        with self._lock:
            return self._running

    def start(self) -> "EnergyTracker":
        """Start a new measurement window and return this tracker."""
        with self._lock:
            if self._running:
                raise RuntimeError("energy tracking is already running")
            self._energy_joules = 0.0
            self._peak_power_watts = 0.0
            self._sample_count = 0
            self._operation_count = 0
            self._last_sample_time = None
            self._last_power_watts = None
            self._start_time = time.monotonic()
            self._end_time = None
            self._running = True
            self._stop_event.clear()

            if self.power_reader is not None:
                self._sampler_thread = threading.Thread(
                    target=self._sample_loop,
                    name="EnergyTrackerSampler",
                    daemon=True,
                )
                self._sampler_thread.start()
        return self

    def _sample_loop(self) -> None:
        while not self._stop_event.wait(self.sampling_interval):
            self._read_power()

    def _read_power(self) -> None:
        reader = self.power_reader
        if reader is None:
            return
        try:
            power = _non_negative_finite(reader(), "power_watts")
            self.record_power(power)
        except Exception as exc:  # noqa: BLE001 - keep monitoring after bad samples
            logger.warning("EnergyTracker power sample failed: %s", exc)

    def record_power(
        self, power_watts: float, timestamp: Optional[float] = None
    ) -> None:
        """Record a measured power reading in watts while tracking is active.

        Consecutive readings are integrated with the trapezoidal rule. An
        optional timestamp is useful for deterministic tests and must use the
        same monotonic-time units as ``time.monotonic``.
        """
        power = _non_negative_finite(power_watts, "power_watts")
        sample_time = time.monotonic() if timestamp is None else float(timestamp)
        if not math.isfinite(sample_time):
            raise ValueError("timestamp must be finite")

        with self._lock:
            if not self._running:
                raise RuntimeError("call start() before recording power")
            if self._last_sample_time is not None:
                elapsed = sample_time - self._last_sample_time
                if elapsed < 0:
                    raise ValueError("timestamps must be non-decreasing")
                self._energy_joules += (
                    (self._last_power_watts + power) * 0.5 * elapsed
                )
            self._last_sample_time = sample_time
            self._last_power_watts = power
            self._peak_power_watts = max(self._peak_power_watts, power)
            self._sample_count += 1

    def record_operation(self, count: int = 1) -> None:
        """Record the number of completed operations for energy-per-operation."""
        if isinstance(count, bool) or not isinstance(count, int) or count < 0:
            raise ValueError("count must be a non-negative integer")
        with self._lock:
            if not self._running:
                raise RuntimeError("call start() before recording operations")
            self._operation_count += count

    def stop(self) -> Dict[str, Any]:
        """Stop sampling and return the final energy metrics."""
        with self._lock:
            if not self._running:
                return self.get_metrics()
            thread = self._sampler_thread
            self._stop_event.set()

        # Wait outside the lock so the sampler can finish its current reading.
        if thread is not None and thread is not threading.current_thread():
            thread.join()

        # Take a final reading at the end of the measurement window.
        if self.power_reader is not None:
            self._read_power()

        with self._lock:
            self._end_time = time.monotonic()
            self._running = False
            self._sampler_thread = None
            return self._metrics_locked()

    def reset(self) -> None:
        """Clear measurements. Tracking must be stopped first."""
        with self._lock:
            if self._running:
                raise RuntimeError("stop tracking before resetting")
            self._start_time = None
            self._end_time = None
            self._last_sample_time = None
            self._last_power_watts = None
            self._energy_joules = 0.0
            self._peak_power_watts = 0.0
            self._sample_count = 0
            self._operation_count = 0

    def _metrics_locked(self) -> Dict[str, Any]:
        end = self._end_time
        if end is None:
            end = time.monotonic() if self._running else self._start_time
        duration = (
            max(0.0, end - self._start_time)
            if self._start_time is not None and end is not None
            else 0.0
        )
        energy_kwh = self._energy_joules / 3_600_000.0
        average_power = (
            self._energy_joules / duration if duration > 0 else 0.0
        )
        energy_per_operation = (
            energy_kwh / self._operation_count
            if self._operation_count > 0 else 0.0
        )
        return {
            "energy_kwh": energy_kwh,
            "energy_joules": self._energy_joules,
            "power_watts": average_power,
            "peak_power_watts": self._peak_power_watts,
            "duration_seconds": duration,
            "sample_count": self._sample_count,
            "operation_count": self._operation_count,
            "energy_per_operation_kwh": energy_per_operation,
            "is_running": self._running,
        }

    def get_metrics(self) -> Dict[str, Any]:
        """Return a snapshot of the current energy and power measurements."""
        with self._lock:
            return self._metrics_locked()

    @staticmethod
    def get_energy_score(energy_kwh: float, scale_kwh: float = 1.0) -> float:
        """Return a bounded energy-efficiency score in [0, 1].

        Zero energy maps to 1; increasing energy decreases the score smoothly.
        ``scale_kwh`` controls the energy level at which the score is 0.5.
        """
        energy = _non_negative_finite(energy_kwh, "energy_kwh")
        scale = _non_negative_finite(scale_kwh, "scale_kwh")
        if scale == 0:
            raise ValueError("scale_kwh must be greater than zero")
        return 1.0 / (1.0 + energy / scale)

    def __enter__(self) -> "EnergyTracker":
        return self.start()

    def __exit__(self, exc_type: Any, exc_value: Any, traceback: Any) -> bool:
        self.stop()
        return False
