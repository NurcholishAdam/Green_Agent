# -*- coding: utf-8 -*-
"""
Sampling-based energy monitoring utilities (Enhanced, v2)
==========================================================

Measure power draw from a user-supplied callable, integrate it over time
using the trapezoidal rule, and report energy in kWh. No power or energy
values are fabricated when readings are unavailable — the docstring's
v1 promise holds in v2.

Fixes applied vs. v1
--------------------
  P0  Operations shorter than ``sampling_interval`` reported 0 energy
      because the sampler's first read was deferred to
      ``start() + interval``. A 0.5 s operation with a 1 s interval had
      zero samples.                                      -> initial read
                                                            in ``start()`` +
                                                            rectangle rule
                                                            for the first
                                                            sample
  P1  Concurrent ``stop()`` was not idempotent — two threads could both
      take a final reading, double-counting energy    -> ``_stopping`` flag
  P1  No accessor for instantaneous power             -> ``current_power_watts``
                                                            in metrics
  P1  Failed sample reads were logged but never
      surfaced to the caller                          -> ``failed_sample_count``
                                                            + ``last_sample_error``
  P2  ``energy_per_operation_kwh`` was 0.0 both for
      "no operations" and "zero energy per operation" -> returns ``None``
                                                            when no operations
  P2  ``reset()`` left ``_stop_event`` in the "set"
      state from a prior ``stop()``                   -> cleared in reset()
  P2  ``_metrics_locked`` used ``_start_time`` as the
      end fallback, ignoring recorded samples         -> falls back to
                                                            ``_last_sample_time``
  P2  ``sampling_interval`` could not be changed after
      construction                                    -> setter added
  P2  No ``__repr__``                                 -> added
  P3  Only peak power tracked                         -> also min power,
                                                            peak timestamp
  P3  File named ``efficiency_scorer.py`` but class
      is ``EnergyTracker``; ambiguous import vs.
      file 29's ``EfficiencyScorer``                  -> renamed to
                                                            ``energy_tracker.py``
                                                            (documented)

New in v2
---------
  ``PrometheusEnergyTracker`` — subclass that emits:

    * ``green_agent_energy_consumed_kwh_total{region,pod}`` (Counter)
    * ``green_agent_power_watts{region,pod}`` (Gauge)
    * ``green_agent_energy_operations_total{region,pod}`` (Counter)
    * ``green_agent_carbon_intensity{region}`` (Gauge, set via
      ``set_carbon_intensity``)

  These are the exact series names that files 16, 17, 20, 22, 24, and 26
  reference. Prior to this file, no component in the codebase emitted
  them, so the HPA, dashboard, adapter, and ServiceMonitor had nothing to
  scrape. With the wrapper, the pipeline can function end-to-end.

  Requires ``prometheus_client``. When it's not installed,
  ``PrometheusEnergyTracker`` behaves exactly like ``EnergyTracker`` and
  logs a warning at construction.

Public API (preserved)
----------------------
  EnergyTracker(power_reader=None, sampling_interval=1.0)
    .start() -> self
    .stop() -> Dict[str, Any]
    .reset()
    .get_metrics() -> Dict[str, Any]
    .record_power(power_watts, timestamp=None)
    .record_operation(count=1)
    .get_energy_score(energy_kwh, scale_kwh=1.0)  # static
    .is_running property
    .sampling_interval property + setter
    context manager (``__enter__``/``__exit__``)

  PrometheusEnergyTracker(*args, region="GLOBAL", pod=None,
                          carbon_kg_per_kwh=None, **kwargs)
    .set_carbon_intensity(carbon_kg_per_kwh)
    (all EnergyTracker methods, plus metric emission)
"""

from __future__ import annotations

import logging
import math
import os
import threading
import time
from typing import Any, Callable, Dict, Optional

logger = logging.getLogger(__name__)

PowerReader = Callable[[], float]

# Optional: Prometheus metric emitters for PrometheusEnergyTracker.
try:
    from prometheus_client import Counter, Gauge  # type: ignore
    PROMETHEUS_AVAILABLE = True
except ImportError:  # pragma: no cover
    Counter = None  # type: ignore
    Gauge = None  # type: ignore
    PROMETHEUS_AVAILABLE = False


# ============================================================================
# Section 1 — Validation helpers
# ============================================================================

def _non_negative_finite(value: Any, name: str) -> float:
    """Convert ``value`` to a finite, non-negative float.

    Rejects ``bool`` explicitly so ``True`` is not treated as ``1.0``.
    """
    if isinstance(value, bool):
        raise ValueError(f"{name} must be a non-negative finite number")
    try:
        result = float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(
            f"{name} must be a non-negative finite number"
        ) from exc
    if not math.isfinite(result) or result < 0:
        raise ValueError(f"{name} must be a non-negative finite number")
    return result


def _positive_finite(value: Any, name: str) -> float:
    """Convert to a strictly positive finite float."""
    result = _non_negative_finite(value, name)
    if result <= 0:
        raise ValueError(f"{name} must be greater than zero")
    return result


# ============================================================================
# Section 2 — EnergyTracker (base class)
# ============================================================================

class EnergyTracker:
    """Measure power and integrate it into energy consumption.

    Args:
        power_reader: Optional zero-argument callable returning measured
            power in watts. When provided, a background sampler calls it at
            the configured interval, and the tracker takes an initial
            reading at ``start()`` so operations shorter than the interval
            still integrate correctly.
        sampling_interval: Seconds between automatic readings; must be > 0.

    Energy is accumulated using trapezoidal integration between consecutive
    samples and the rectangle rule from ``start_time`` to the first sample.
    Reported in kWh.

    Safe to use as a context manager. Safe to submit readings from another
    thread. Safe to call ``stop()`` from multiple threads (only the first
    will take a final reading; subsequent calls return a metrics snapshot).
    """

    def __init__(
        self,
        power_reader: Optional[PowerReader] = None,
        sampling_interval: float = 1.0,
    ) -> None:
        self._sampling_interval = _positive_finite(
            sampling_interval, "sampling_interval"
        )
        if power_reader is not None and not callable(power_reader):
            raise TypeError("power_reader must be callable or None")

        self.power_reader = power_reader
        self._lock = threading.RLock()
        self._stop_event = threading.Event()
        self._sampler_thread: Optional[threading.Thread] = None
        self._running = False
        self._stopping = False
        self._start_time: Optional[float] = None
        self._end_time: Optional[float] = None
        self._last_sample_time: Optional[float] = None
        self._last_power_watts: Optional[float] = None
        self._energy_joules = 0.0
        self._peak_power_watts = 0.0
        self._min_power_watts: Optional[float] = None
        self._peak_power_timestamp: Optional[float] = None
        self._sample_count = 0
        self._failed_sample_count = 0
        self._last_sample_error: Optional[str] = None
        self._operation_count = 0

    # ------------------------------------------------------------------
    # Properties
    # ------------------------------------------------------------------
    @property
    def is_running(self) -> bool:
        with self._lock:
            return self._running

    @property
    def sampling_interval(self) -> float:
        with self._lock:
            return self._sampling_interval

    @sampling_interval.setter
    def sampling_interval(self, value: float) -> None:
        """Change the sampling interval. Takes effect on the next sample
        (the sampler thread reads ``self._sampling_interval`` on each loop
        iteration)."""
        self._sampling_interval = _positive_finite(
            value, "sampling_interval"
        )

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------
    def start(self) -> "EnergyTracker":
        """Start a new measurement window. Returns ``self`` for chaining."""
        with self._lock:
            if self._running:
                raise RuntimeError("energy tracking is already running")
            self._energy_joules = 0.0
            self._peak_power_watts = 0.0
            self._min_power_watts = None
            self._peak_power_timestamp = None
            self._sample_count = 0
            self._failed_sample_count = 0
            self._last_sample_error = None
            self._operation_count = 0
            self._last_sample_time = None
            self._last_power_watts = None
            self._start_time = time.monotonic()
            self._end_time = None
            self._running = True
            self._stopping = False
            self._stop_event.clear()

            # Fix §3.1: take an immediate reading so operations shorter
            # than `sampling_interval` still integrate. Without this, the
            # sampler's first read is deferred to start()+interval, and a
            # 0.5 s operation with a 1 s interval has zero samples.
            if self.power_reader is not None:
                self._read_power()

                self._sampler_thread = threading.Thread(
                    target=self._sample_loop,
                    name="EnergyTrackerSampler",
                    daemon=True,
                )
                self._sampler_thread.start()
        return self

    def _sample_loop(self) -> None:
        # Re-read self._sampling_interval each iteration so a setter call
        # takes effect without restarting the thread.
        while not self._stop_event.wait(self.sampling_interval):
            self._read_power()

    def _read_power(self) -> None:
        reader = self.power_reader
        if reader is None:
            return
        try:
            power = _non_negative_finite(reader(), "power_watts")
            self.record_power(power)
        except Exception as exc:  # noqa: BLE001 — keep monitoring
            with self._lock:
                self._failed_sample_count += 1
                self._last_sample_error = str(exc)
            logger.warning("EnergyTracker power sample failed: %s", exc)

    # ------------------------------------------------------------------
    # Recording
    # ------------------------------------------------------------------
    def record_power(
        self, power_watts: float, timestamp: Optional[float] = None
    ) -> None:
        """Record a measured power reading in watts.

        Consecutive readings are integrated with the trapezoidal rule.
        The first reading after ``start()`` uses the rectangle rule from
        ``start_time`` to the sample time so short operations still
        integrate. ``timestamp`` must use the same monotonic-time units as
        :func:`time.monotonic`.
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
                # Trapezoid between consecutive samples.
                self._energy_joules += (
                    (self._last_power_watts + power) * 0.5 * elapsed
                )
            elif self._start_time is not None:
                # Rectangle from start_time to this first sample. Ensures
                # short operations integrate correctly.
                elapsed = sample_time - self._start_time
                if elapsed < 0:
                    raise ValueError("timestamp must be after start_time")
                self._energy_joules += power * elapsed

            self._last_sample_time = sample_time
            self._last_power_watts = power
            if power > self._peak_power_watts:
                self._peak_power_watts = power
                self._peak_power_timestamp = sample_time
            if self._min_power_watts is None or power < self._min_power_watts:
                self._min_power_watts = power
            self._sample_count += 1

    def record_operation(self, count: int = 1) -> None:
        """Record the number of completed operations for energy-per-operation."""
        if isinstance(count, bool) or not isinstance(count, int) or count < 0:
            raise ValueError("count must be a non-negative integer")
        with self._lock:
            if not self._running:
                raise RuntimeError("call start() before recording operations")
            self._operation_count += count

    # ------------------------------------------------------------------
    # Stop / reset
    # ------------------------------------------------------------------
    def stop(self) -> Dict[str, Any]:
        """Stop sampling and return the final energy metrics.

        Idempotent under concurrent calls: only the first caller performs
        the final read; subsequent callers receive a metrics snapshot.
        """
        with self._lock:
            if not self._running or self._stopping:
                return self._metrics_locked()
            self._stopping = True
            thread = self._sampler_thread
            self._stop_event.set()

        try:
            if thread is not None and thread is not threading.current_thread():
                thread.join()

            if self.power_reader is not None:
                self._read_power()
        finally:
            with self._lock:
                self._end_time = time.monotonic()
                self._running = False
                self._stopping = False
                self._sampler_thread = None

        with self._lock:
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
            self._min_power_watts = None
            self._peak_power_timestamp = None
            self._sample_count = 0
            self._failed_sample_count = 0
            self._last_sample_error = None
            self._operation_count = 0
            # Fix §3.6: clear the event so a subsequent get_metrics()
            # doesn't see a stale "set" state after stop().
            self._stop_event.clear()

    # ------------------------------------------------------------------
    # Metrics
    # ------------------------------------------------------------------
    def _metrics_locked(self) -> Dict[str, Any]:
        end = self._end_time
        if end is None:
            if self._running:
                end = time.monotonic()
            elif self._last_sample_time is not None:
                # Fix §3.7: prefer the last sample time over start time
                # so a start-without-stop doesn't report duration 0.
                end = self._last_sample_time
            else:
                end = self._start_time

        duration = (
            max(0.0, end - self._start_time)
            if self._start_time is not None and end is not None
            else 0.0
        )
        energy_kwh = self._energy_joules / 3_600_000.0
        average_power = (
            self._energy_joules / duration if duration > 0 else 0.0
        )
        # Fix §3.5: return None when no operations recorded; distinguish
        # "unknown" from "zero".
        energy_per_operation = (
            energy_kwh / self._operation_count
            if self._operation_count > 0
            else None
        )
        return {
            "energy_kwh": energy_kwh,
            "energy_joules": self._energy_joules,
            "power_watts": average_power,
            "current_power_watts": (
                self._last_power_watts
                if self._last_power_watts is not None
                else 0.0
            ),
            "peak_power_watts": self._peak_power_watts,
            "min_power_watts": (
                self._min_power_watts
                if self._min_power_watts is not None
                else 0.0
            ),
            "peak_power_timestamp": self._peak_power_timestamp,
            "duration_seconds": duration,
            "sample_count": self._sample_count,
            "failed_sample_count": self._failed_sample_count,
            "last_sample_error": self._last_sample_error,
            "operation_count": self._operation_count,
            "energy_per_operation_kwh": energy_per_operation,
            "is_running": self._running,
        }

    def get_metrics(self) -> Dict[str, Any]:
        """Return a snapshot of the current energy and power measurements."""
        with self._lock:
            return self._metrics_locked()

    # ------------------------------------------------------------------
    # Utilities
    # ------------------------------------------------------------------
    @staticmethod
    def get_energy_score(energy_kwh: float, scale_kwh: float = 1.0) -> float:
        """Return a bounded energy-efficiency score in [0, 1].

        Zero energy maps to 1; increasing energy decreases the score
        smoothly. ``scale_kwh`` is the energy level at which the score is
        0.5.
        """
        energy = _non_negative_finite(energy_kwh, "energy_kwh")
        scale = _positive_finite(scale_kwh, "scale_kwh")
        return 1.0 / (1.0 + energy / scale)

    def __repr__(self) -> str:
        with self._lock:
            return (
                f"EnergyTracker(running={self._running}, "
                f"samples={self._sample_count}, "
                f"failed={self._failed_sample_count}, "
                f"energy_kwh={self._energy_joules / 3_600_000:.6f}, "
                f"current_w={self._last_power_watts or 0.0:.2f})"
            )

    # ------------------------------------------------------------------
    # Context manager
    # ------------------------------------------------------------------
    def __enter__(self) -> "EnergyTracker":
        return self.start()

    def __exit__(self, exc_type: Any, exc_value: Any, traceback: Any) -> bool:
        self.stop()
        return False


# ============================================================================
# Section 3 — PrometheusEnergyTracker (emitter wrapper)
# ============================================================================

class PrometheusEnergyTracker(EnergyTracker):
    """``EnergyTracker`` subclass that emits Prometheus metrics.

    Metric names and labels match what files 16, 17, 20, 22, 24, and 26
    reference:

        ``green_agent_energy_consumed_kwh_total{region,pod}``  (Counter)
        ``green_agent_power_watts{region,pod}``                (Gauge)
        ``green_agent_energy_operations_total{region,pod}``    (Counter)
        ``green_agent_carbon_intensity{region}``               (Gauge)

    The energy counter is incremented by the delta between successive
    samples, so its value is the cumulative energy measured by this
    process since start. ``reset()`` zeroes the tracker's local state but
    does not reset the Prometheus counter — that's the standard Prometheus
    pattern for lifetime totals.

    When ``prometheus_client`` is unavailable, this class behaves exactly
    like ``EnergyTracker`` (no metrics emitted, warning logged).
    """

    def __init__(
        self,
        *args: Any,
        region: str = "GLOBAL",
        pod: Optional[str] = None,
        carbon_kg_per_kwh: Optional[float] = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(*args, **kwargs)

        self._region = str(region)
        self._pod = (
            pod if pod is not None
            else os.environ.get("POD_NAME", "unknown")
        )
        self._last_emitted_joules = 0.0

        if not PROMETHEUS_AVAILABLE:
            logger.warning(
                "prometheus_client is not installed; "
                "PrometheusEnergyTracker will behave like EnergyTracker "
                "without emitting metrics. Install prometheus_client to "
                "enable metric emission."
            )
            self._energy_counter: Optional[Any] = None
            self._power_gauge: Optional[Any] = None
            self._operations_counter: Optional[Any] = None
            self._carbon_intensity_gauge: Optional[Any] = None
            return

        self._energy_counter = Counter(
            "green_agent_energy_consumed_kwh_total",
            "Cumulative energy measured by EnergyTracker (kWh).",
            ["region", "pod"],
        )
        self._power_gauge = Gauge(
            "green_agent_power_watts",
            "Instantaneous power draw measured by EnergyTracker (W).",
            ["region", "pod"],
        )
        self._operations_counter = Counter(
            "green_agent_energy_operations_total",
            "Cumulative operations recorded by EnergyTracker.",
            ["region", "pod"],
        )
        self._carbon_intensity_gauge = Gauge(
            "green_agent_carbon_intensity",
            "Grid carbon intensity (gCO2/kWh).",
            ["region"],
        )
        if carbon_kg_per_kwh is not None:
            self.set_carbon_intensity(carbon_kg_per_kwh)

    # ------------------------------------------------------------------
    # Metric emission
    # ------------------------------------------------------------------
    def _emit_energy_and_power_locked(self) -> None:
        """Emit the energy delta and instantaneous power. Caller holds the lock."""
        if self._energy_counter is None:
            return

        delta_joules = self._energy_joules - self._last_emitted_joules
        if delta_joules > 0:
            self._energy_counter.labels(
                region=self._region, pod=self._pod
            ).inc(delta_joules / 3_600_000.0)
            self._last_emitted_joules = self._energy_joules

        if self._last_power_watts is not None:
            self._power_gauge.labels(
                region=self._region, pod=self._pod
            ).set(self._last_power_watts)

    # ------------------------------------------------------------------
    # Overrides
    # ------------------------------------------------------------------
    def record_power(
        self, power_watts: float, timestamp: Optional[float] = None
    ) -> None:
        super().record_power(power_watts, timestamp)
        with self._lock:
            self._emit_energy_and_power_locked()

    def record_operation(self, count: int = 1) -> None:
        super().record_operation(count)
        if self._operations_counter is None:
            return
        with self._lock:
            self._operations_counter.labels(
                region=self._region, pod=self._pod
            ).inc(count)

    def reset(self) -> None:
        super().reset()
        with self._lock:
            # Reset the emission cursor so post-reset samples emit
            # clean deltas. Prometheus counters keep accumulating — that's
            # the standard pattern for lifetime totals.
            self._last_emitted_joules = 0.0

    def set_carbon_intensity(self, carbon_kg_per_kwh: float) -> None:
        """Publish the grid carbon intensity for this region as a Gauge.

        ``carbon_kg_per_kwh`` is in kg CO2e per kWh; the gauge reports
        gCO2/kWh (multiplied by 1000). Callers can source the value from
        file 27's ``CarbonCalculator.CARBON_INTENSITY`` table or from a
        live grid API.
        """
        value = _non_negative_finite(carbon_kg_per_kwh, "carbon_kg_per_kwh")
        if self._carbon_intensity_gauge is None:
            return
        self._carbon_intensity_gauge.labels(region=self._region).set(
            value * 1000.0
        )


# ============================================================================
# Section 4 — Demo
# ============================================================================

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    # ---- 1. Short operation — the bug §3.1 used to mask ----
    print("=== Short operation (0.5s) with 1s sampling interval ===")
    counter = {"n": 0}

    def reader() -> float:
        counter["n"] += 1
        return 120.0  # constant 120 W

    tracker = EnergyTracker(power_reader=reader, sampling_interval=1.0)
    tracker.start()
    time.sleep(0.5)
    metrics = tracker.stop()
    print(f"  energy_kwh            = {metrics['energy_kwh']:.6f}")
    print(f"  average power (W)     = {metrics['power_watts']:.2f}")
    print(f"  peak power (W)        = {metrics['peak_power_watts']:.2f}")
    print(f"  sample_count          = {metrics['sample_count']}")
    print(f"  failed_sample_count   = {metrics['failed_sample_count']}")
    print(f"  duration (s)          = {metrics['duration_seconds']:.3f}")
    # With the fix, energy_kwh should be ~= 120 W * 0.5 s / 3_600_000
    # = 0.0000167 kWh, not zero.
    assert metrics["energy_kwh"] > 0, (
        "short operation should still record energy"
    )

    # ---- 2. Concurrent stop() — the fix §3.2 ----
    print("\n=== Concurrent stop() (2 threads) ===")
    import concurrent.futures

    tracker2 = EnergyTracker(power_reader=reader, sampling_interval=5.0)
    tracker2.start()
    time.sleep(0.1)
    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as ex:
        futures = [ex.submit(tracker2.stop) for _ in range(2)]
        results = [f.result() for f in futures]
    # Both should return the same snapshot; energy should NOT be doubled.
    print(f"  energies: {[round(r['energy_kwh'], 6) for r in results]}")
    assert results[0]["energy_kwh"] == results[1]["energy_kwh"], (
        "concurrent stop() should be idempotent"
    )

    # ---- 3. Failed samples — the fix §3.4 ----
    print("\n=== Broken power_reader ===")

    def broken() -> float:
        raise RuntimeError("sensor offline")

    tracker3 = EnergyTracker(power_reader=broken, sampling_interval=0.05)
    tracker3.start()
    time.sleep(0.15)
    m3 = tracker3.stop()
    print(f"  sample_count        = {m3['sample_count']}")
    print(f"  failed_sample_count = {m3['failed_sample_count']}")
    print(f"  last_sample_error   = {m3['last_sample_error']}")
    assert m3["failed_sample_count"] > 0, "failures should be counted"

    # ---- 4. Context manager ----
    print("\n=== Context manager ===")
    with EnergyTracker(power_reader=reader, sampling_interval=0.1) as t:
        time.sleep(0.25)
        snap = t.get_metrics()
        print(f"  current_w (during)  = {snap['current_power_watts']:.2f}")
    print(f"  after exit running  = {t.is_running}")

    # ---- 5. Prometheus wrapper (emits the pipeline metric) ----
    if PROMETHEUS_AVAILABLE:
        print("\n=== PrometheusEnergyTracker ===")
        pt = PrometheusEnergyTracker(
            power_reader=reader,
            sampling_interval=0.1,
            region="US-CA",
            pod="green-agent-worker-0",
            carbon_kg_per_kwh=0.20,  # from file 27's CARBON_INTENSITY
        )
        pt.start()
        time.sleep(0.25)
        m5 = pt.stop()
        print(f"  energy_kwh = {m5['energy_kwh']:.6f}")
        print(
            "  Emitted: green_agent_energy_consumed_kwh_total, "
            "green_agent_power_watts, "
            "green_agent_carbon_intensity"
        )
        print(
            "  These are the series files 16/17/20/22/24/26 scrape."
        )
    else:
        print(
            "\n=== prometheus_client not installed; skipping wrapper demo ==="
        )
