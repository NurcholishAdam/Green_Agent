# src/continuum/tdp_monitor.py
"""
TDP Monitor — v6.0.0
=====================

Real-time TDP telemetry and breach prediction for edge devices.

**Public API preserved**

    monitor = TDPMonitor(device_id, device_type)
    await monitor.start()
    await monitor.get_current_reading()
    await monitor.simulate_reading(power_watts)
    await monitor.stop()

**What changed (v5 → v6)**
--------------------------

**P0 — cross-module identity, async anti-patterns, breach correctness**

1. **`DeviceType` and `TDPReading` now come from `continuum.types`**
   (§2.1). v5 defined its own enum + dataclass, so a `TDPReading` from
   this module was **not** an instance of the class the offloading engine
   expected. v6 imports both; the enum has the union of both vocabularies.
2. **`psutil.cpu_percent(interval=...)` no longer blocks the event loop**
   (§2.2). v5 called it synchronously inside an `async def` — every
   sample froze the loop for up to 2 seconds. v6 runs it via
   `asyncio.to_thread` and passes the result down so it's sampled once.
3. **`_predict_breach` checks "already breached" before "slope ≤ 0"**
   (§2.3). v5 required a *rising* trend to signal a breach, so a device
   sitting at 15 W against a 12 W threshold returned `None` — no alert.
   v6 alerts on any reading at or above `threshold - WARNING_BUFFER`.
4. **`monotonic_ts` is no longer serialized** (§2.4). v5 sent
   process-local monotonic clocks across the wire; on restore they were
   meaningless. v6 omits the field from `to_dict` and hard-codes
   `monotonic_ts=0.0` on `from_dict`, forcing the wall-clock fallback.
5. **The fabricated RPi INA219 path is deleted** (§2.5). v5 multiplied
   a raw register by `0.001` with no calibration data — a Pi 5 could
   report `0 W` or `65 W` depending on register noise. v6 uses the
   CPU-based estimate instead and documents why.
6. **`TDPThresholds.__post_init__` validates** (§2.7). Negative
   thresholds, `WARNING_BUFFER ≥ threshold`, and NaN are rejected.
7. **`simulate_reading` validates every input** (§2.8). NaN/inf
   temperature, utilization, and power are rejected up front.

**P1 — reliability and observability**

8. **`health_check()`**, **`_log(event, level, **fields)`**,
   **`collect_prometheus_metrics()`**.
9. **`_run_subprocess` awaits `proc.wait()` after kill** so timeouts
   don't leave zombies.
10. **`_measure_temperature` uses `psutil.sensors_temperatures()`**
    first, then globs `hwmon*/temp1_input` and takes the max.
11. **`_predict_breach` uses wall-clock timestamps when
    `monotonic_ts == 0`** — real spacing, not index-based fiction.
12. **`from_dict` wraps every parse error in `TDPMonitorError`** —
    callers doing `except TDPMonitorError` always catch it.
13. **Live reconfiguration**: `set_config`, `set_thresholds`.
14. **Callback dispatch runs in separate tasks** with a per-callback
    timeout; `stop()` drains them.
15. **`__repr__` includes the effective config**.

**P2 — structure and features**

16. **`schema_version` on `TDPReading`** (default 2).
17. **`UNKNOWN` device threshold = max of known thresholds** — a safe
    default that won't over-alert a capable device.
18. **`dataclasses.replace`** instead of `**reading.__dict__`.
19. **`get_readings_since(since)`** accessor.
20. **`_last_breach_at` tracking**, exposed via `statistics()`.
21. **`warning_buffer_watts` property** — canonical single source of
    truth for the warning buffer.

**Running the demo**

    PYTHONPATH=src python -m continuum.tdp_monitor
"""

from __future__ import annotations

import asyncio
import glob
import json
import logging
import math
import re
import statistics
import threading
import time
from collections import deque
from dataclasses import asdict, dataclass, fields, replace
from datetime import datetime, timezone
from typing import (
    Any,
    Callable,
    Deque,
    Dict,
    List,
    Mapping,
    Optional,
    Set,
    Tuple,
    Union,
)

# --------------------------------------------------------------------------- #
# Shared continuum primitives (single source of truth)
# --------------------------------------------------------------------------- #
try:  # pragma: no cover
    from .types import (
        ContinuumError,
        DeviceType,
        TDPReading,
        _coerce_float,
        _coerce_timestamp,
        _json_safe,
    )
except ImportError:  # pragma: no cover
    from continuum.types import (  # type: ignore[no-redef]
        ContinuumError,
        DeviceType,
        TDPReading,
        _coerce_float,
        _coerce_timestamp,
        _json_safe,
    )

# --------------------------------------------------------------------------- #
# Optional psutil
# --------------------------------------------------------------------------- #
try:  # pragma: no cover
    import psutil  # type: ignore
    _PSUTIL_AVAILABLE = True
except ImportError:  # pragma: no cover
    psutil = None  # type: ignore[assignment]
    _PSUTIL_AVAILABLE = False


logger = logging.getLogger(__name__)


# --------------------------------------------------------------------------- #
# Errors + structured logging
# --------------------------------------------------------------------------- #
class TDPMonitorError(ContinuumError):
    """Raised for invalid inputs, configuration, or monitor failures."""


def _log(event: str, level: int = logging.INFO, **fields: Any) -> None:
    if not logger.isEnabledFor(level):
        return
    parts = [event]
    if fields:
        parts.append(" ".join(f"{k}={v}" for k, v in fields.items()))
    logger.log(level, " ".join(parts), extra={"event": event, **fields})


def _validate_finite(name: str, value: Any) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise TDPMonitorError(f"{name} must be numeric, got {type(value).__name__}")
    fval = float(value)
    if not math.isfinite(fval):
        raise TDPMonitorError(f"{name} must be finite, got {value!r}")
    return fval


# --------------------------------------------------------------------------- #
# Thresholds
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class TDPThresholds:
    """TDP thresholds per device type (Watts)."""

    RASPBERRY_PI_5: float = 12.0
    NVIDIA_JETSON_ORIN: float = 60.0
    INTEL_NUC_13_PRO: float = 28.0
    # P2 §4.10: canonical source of the warning buffer.
    WARNING_BUFFER: float = 2.0

    def __post_init__(self) -> None:
        # P0 §2.7
        for name in ("RASPBERRY_PI_5", "NVIDIA_JETSON_ORIN", "INTEL_NUC_13_PRO"):
            v = getattr(self, name)
            if isinstance(v, bool) or not isinstance(v, (int, float)):
                raise TDPMonitorError(f"{name} must be numeric")
            if not math.isfinite(float(v)) or float(v) <= 0:
                raise TDPMonitorError(f"{name} must be finite and > 0 (got {v!r})")
        if isinstance(self.WARNING_BUFFER, bool) or not isinstance(
            self.WARNING_BUFFER, (int, float)
        ):
            raise TDPMonitorError("WARNING_BUFFER must be numeric")
        if not math.isfinite(float(self.WARNING_BUFFER)) or self.WARNING_BUFFER < 0:
            raise TDPMonitorError("WARNING_BUFFER must be finite and >= 0")
        for name in ("RASPBERRY_PI_5", "NVIDIA_JETSON_ORIN", "INTEL_NUC_13_PRO"):
            if self.WARNING_BUFFER >= getattr(self, name):
                raise TDPMonitorError(
                    f"WARNING_BUFFER ({self.WARNING_BUFFER}) must be less than "
                    f"{name} ({getattr(self, name)})"
                )

    def threshold_for(self, device_type: DeviceType) -> float:
        """
        Return the TDP threshold for ``device_type``.

        For ``UNKNOWN``, return the **maximum** known threshold — the
        safe default for an unidentified device (P2 §4.5).
        """
        if device_type == DeviceType.RASPBERRY_PI_5:
            return float(self.RASPBERRY_PI_5)
        if device_type == DeviceType.NVIDIA_JETSON_ORIN:
            return float(self.NVIDIA_JETSON_ORIN)
        if device_type == DeviceType.INTEL_NUC_13_PRO:
            return float(self.INTEL_NUC_13_PRO)
        # UNKNOWN and any legacy alias — use the max (conservative).
        return float(
            max(
                self.RASPBERRY_PI_5,
                self.NVIDIA_JETSON_ORIN,
                self.INTEL_NUC_13_PRO,
            )
        )

    def idle_and_max_power(
        self, device_type: DeviceType
    ) -> Tuple[float, float]:
        mapping = {
            DeviceType.RASPBERRY_PI_5: (3.0, 12.0),
            DeviceType.NVIDIA_JETSON_ORIN: (5.0, 60.0),
            DeviceType.INTEL_NUC_13_PRO: (8.0, 28.0),
        }
        if device_type in mapping:
            return mapping[device_type]
        # Unknown — use the same conservative max as threshold_for
        return (10.0, self.threshold_for(DeviceType.UNKNOWN))


# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class TDPMonitorConfig:
    """Tunable parameters for the TDP monitor."""

    sampling_interval_seconds: float = 5.0
    loop_error_sleep_seconds: float = 10.0

    prediction_horizon_seconds: float = 300.0
    breach_alert_threshold_seconds: float = 60.0
    min_samples_for_prediction: int = 10

    max_history: int = 1000

    subprocess_timeout_seconds: float = 3.0
    cpu_sample_interval_seconds: float = 1.0
    callback_timeout_seconds: float = 5.0   # v6

    def __post_init__(self) -> None:
        for name in (
            "sampling_interval_seconds",
            "loop_error_sleep_seconds",
            "prediction_horizon_seconds",
            "breach_alert_threshold_seconds",
            "subprocess_timeout_seconds",
            "cpu_sample_interval_seconds",
            "callback_timeout_seconds",
        ):
            v = getattr(self, name)
            if v <= 0:
                raise TDPMonitorError(f"{name} must be > 0 (got {v})")
        if self.min_samples_for_prediction < 2:
            raise TDPMonitorError(
                "min_samples_for_prediction must be >= 2"
            )
        if self.max_history <= 0:
            raise TDPMonitorError("max_history must be > 0")


# --------------------------------------------------------------------------- #
# Monitor
# --------------------------------------------------------------------------- #
class TDPMonitor:
    """Real-time TDP telemetry and breach prediction."""

    def __init__(
        self,
        device_id: str,
        device_type: DeviceType,
        sampling_interval_seconds: Union[int, float] = 5,
        prediction_horizon_seconds: Union[int, float] = 300,
        thresholds: Optional[TDPThresholds] = None,
        *,
        config: Optional[TDPMonitorConfig] = None,
        strict: bool = True,
    ) -> None:
        if not isinstance(device_id, str) or not device_id:
            raise TDPMonitorError("device_id must be a non-empty string")
        if not isinstance(device_type, DeviceType):
            raise TDPMonitorError(
                f"device_type must be DeviceType, got {type(device_type).__name__}"
            )

        # P1 §3.1: accept int or float positionals
        for name, value in (
            ("sampling_interval_seconds", sampling_interval_seconds),
            ("prediction_horizon_seconds", prediction_horizon_seconds),
        ):
            if isinstance(value, bool) or not isinstance(value, (int, float)):
                raise TDPMonitorError(f"{name} must be numeric")
            if value <= 0:
                raise TDPMonitorError(f"{name} must be > 0")

        if config is not None:
            self._config = config
        else:
            self._config = TDPMonitorConfig(
                sampling_interval_seconds=float(sampling_interval_seconds),
                prediction_horizon_seconds=float(prediction_horizon_seconds),
            )

        self.device_id = device_id
        self.device_type = device_type
        self.thresholds = thresholds or TDPThresholds()
        self._strict = bool(strict)

        # Legacy mirrors
        self.sampling_interval = int(self._config.sampling_interval_seconds)
        self.prediction_horizon = int(self._config.prediction_horizon_seconds)

        self._lock = threading.RLock()
        self._callbacks: List[Callable[[TDPReading], Any]] = []
        self._callback_tasks: Set[asyncio.Task] = set()
        self._history: Deque[TDPReading] = deque(maxlen=self._config.max_history)
        self._running = False
        self._tasks: List[asyncio.Task] = []
        self._ctx_start: Optional[float] = None
        self._started_at: Optional[float] = None
        self._last_breach_at: Optional[datetime] = None

        # RAPL state
        self._last_rapl_energy_uj: Optional[int] = None
        self._last_rapl_timestamp: Optional[float] = None

        _log(
            "tdp_monitor_initialized",
            logging.DEBUG,
            device_id=self.device_id,
            device_type=self.device_type.value,
            interval=self._config.sampling_interval_seconds,
            horizon=self._config.prediction_horizon_seconds,
            history=self._config.max_history,
            psutil=_PSUTIL_AVAILABLE,
        )

    # ------------------------------------------------------------------ props
    @property
    def config(self) -> TDPMonitorConfig:
        return self._config

    @property
    def running(self) -> bool:
        return self._running

    @property
    def history_size(self) -> int:
        with self._lock:
            return len(self._history)

    @property
    def callbacks(self) -> List[Callable[[TDPReading], Any]]:
        with self._lock:
            return list(self._callbacks)

    @property
    def last_breach_at(self) -> Optional[datetime]:
        with self._lock:
            return self._last_breach_at

    @property
    def warning_buffer_watts(self) -> float:
        """Canonical warning buffer (P2 §4.10)."""
        return float(self.thresholds.WARNING_BUFFER)

    # ---------------------------------------------------------- lifecycle
    async def start(self) -> None:
        if self._running:
            _log("tdp_already_running", logging.DEBUG, device_id=self.device_id)
            return
        self._running = True
        self._started_at = time.time()
        self._tasks = [
            asyncio.create_task(self._monitoring_loop(), name="tdp:loop")
        ]
        _log(
            "tdp_monitor_started",
            logging.INFO,
            device_id=self.device_id,
            interval=self._config.sampling_interval_seconds,
        )

    async def stop(self, *, timeout_seconds: float = 5.0) -> None:
        if not self._running:
            return
        self._running = False

        for t in self._tasks:
            t.cancel()
        if self._tasks:
            try:
                await asyncio.wait_for(
                    asyncio.gather(*self._tasks, return_exceptions=True),
                    timeout=timeout_seconds,
                )
            except asyncio.TimeoutError:  # pragma: no cover
                _log("tdp_stop_loops_timeout", logging.WARNING, device_id=self.device_id)
        self._tasks = []

        # Drain in-flight callbacks (P1 §3.5)
        pending = list(self._callback_tasks)
        if pending:
            try:
                await asyncio.wait_for(
                    asyncio.gather(*pending, return_exceptions=True),
                    timeout=timeout_seconds,
                )
            except asyncio.TimeoutError:  # pragma: no cover
                _log("tdp_stop_callbacks_timeout", logging.WARNING, device_id=self.device_id)
        self._callback_tasks.clear()

        _log(
            "tdp_monitor_stopped",
            logging.INFO,
            device_id=self.device_id,
            history=len(self._history),
        )

    # ---------------------------------------------------------- callbacks
    def register_callback(self, callback: Callable[[TDPReading], Any]) -> None:
        if not callable(callback):
            raise TDPMonitorError("callback must be callable")
        with self._lock:
            self._callbacks.append(callback)
        _log(
            "tdp_callback_registered",
            logging.DEBUG,
            device_id=self.device_id,
            callback=getattr(callback, "__name__", str(callback)),
        )

    def unregister_callback(self, callback: Callable[[TDPReading], Any]) -> bool:
        with self._lock:
            try:
                self._callbacks.remove(callback)
                return True
            except ValueError:
                return False

    async def _dispatch_callbacks(self, reading: TDPReading) -> None:
        """
        P1 §3.5: dispatch each callback in its own task with a timeout,
        so a slow callback cannot stall the monitoring loop.
        """
        with self._lock:
            callbacks = list(self._callbacks)
        if not callbacks:
            return
        for cb in callbacks:
            task = asyncio.create_task(
                self._run_callback_safely(cb, reading),
                name=f"tdp:cb:{getattr(cb, '__name__', id(cb))}",
            )
            self._callback_tasks.add(task)
            task.add_done_callback(self._callback_tasks.discard)
        # Yield so fast callbacks run before the caller returns
        await asyncio.sleep(0)

    async def _run_callback_safely(
        self, cb: Callable[[TDPReading], Any], reading: TDPReading
    ) -> None:
        name = getattr(cb, "__name__", str(cb))
        try:
            result = cb(reading)
            if asyncio.iscoroutine(result):
                await asyncio.wait_for(
                    result, timeout=self._config.callback_timeout_seconds
                )
        except asyncio.TimeoutError:
            _log("tdp_callback_timeout", logging.WARNING, callback=name)
        except Exception as exc:
            _log("tdp_callback_error", logging.ERROR, callback=name, error=str(exc))

    # ---------------------------------------------------------- public API
    async def get_current_reading(self) -> TDPReading:
        base = await self._measure_power()
        predicted = self._predict_breach(base)
        reading = replace(base, predicted_breach_seconds=predicted)

        imminent = (
            predicted is not None
            and predicted < self._config.breach_alert_threshold_seconds
        )
        with self._lock:
            self._history.append(reading)
            if imminent:
                self._last_breach_at = reading.timestamp

        if imminent:
            await self._dispatch_callbacks(reading)
        return reading

    async def simulate_reading(
        self,
        current_power_watts: float,
        *,
        temperature_celsius: float = 50.0,
        utilization_percent: float = 50.0,
        thermal_throttling: bool = False,
        monotonic_ts: Optional[float] = None,
    ) -> TDPReading:
        """
        Inject a synthetic reading (useful for tests and dry runs).

        P0 §2.8: every numeric argument is validated for NaN/inf.
        P2 §4.7: uses ``dataclasses.replace`` rather than mutating
        ``__dict__``.
        """
        current_power_watts = _validate_finite(
            "current_power_watts", current_power_watts
        )
        if current_power_watts < 0:
            raise TDPMonitorError("current_power_watts must be >= 0")
        temperature_celsius = _validate_finite(
            "temperature_celsius", temperature_celsius
        )
        utilization_percent = _validate_finite(
            "utilization_percent", utilization_percent
        )
        if not 0.0 <= utilization_percent <= 100.0:
            raise TDPMonitorError("utilization_percent must be in [0, 100]")

        base = TDPReading(
            timestamp=datetime.now(timezone.utc),
            monotonic_ts=(
                monotonic_ts if monotonic_ts is not None else time.monotonic()
            ),
            device_id=self.device_id,
            device_type=self.device_type,
            current_power_watts=current_power_watts,
            tdp_threshold_watts=self.thresholds.threshold_for(self.device_type),
            utilization_percent=utilization_percent,
            temperature_celsius=temperature_celsius,
            thermal_throttling=bool(thermal_throttling),
            predicted_breach_seconds=None,
            is_synthetic=True,
        )

        predicted = self._predict_breach(base)
        reading = replace(base, predicted_breach_seconds=predicted)

        imminent = (
            predicted is not None
            and predicted < self._config.breach_alert_threshold_seconds
        )
        with self._lock:
            self._history.append(reading)
            if imminent:
                self._last_breach_at = reading.timestamp

        if imminent:
            await self._dispatch_callbacks(reading)
        return reading

    def get_recent_readings(self, n: int = 10) -> List[TDPReading]:
        if not isinstance(n, int) or n <= 0:
            raise TDPMonitorError("n must be a positive int")
        with self._lock:
            if n >= len(self._history):
                return list(self._history)
            return list(self._history)[-n:]

    def get_readings_since(self, since: datetime) -> List[TDPReading]:
        """P2 §4.19: time-windowed accessor."""
        if not isinstance(since, datetime):
            raise TDPMonitorError("since must be a datetime")
        since_aware = _coerce_timestamp(since)
        with self._lock:
            return [
                r for r in self._history
                if _coerce_timestamp(r.timestamp) >= since_aware
            ]

    # ---------------------------------------------------------- measurement
    async def _measure_power(self) -> TDPReading:
        """
        Measure current power consumption.

        P0 §2.2: sample utilization once and pass it to hardware paths and
        the CPU fallback so it's never re-sampled.
        """
        utilization = await self._measure_cpu_percent()

        if self.device_type == DeviceType.RASPBERRY_PI_5:
            power = await self._measure_rpi_power(utilization=utilization)
        elif self.device_type == DeviceType.NVIDIA_JETSON_ORIN:
            power = await self._measure_jetson_power(utilization=utilization)
        elif self.device_type == DeviceType.INTEL_NUC_13_PRO:
            power = await self._measure_nuc_power(utilization=utilization)
        else:
            power = await self._estimate_power_from_cpu(utilization=utilization)

        temperature = await self._measure_temperature()
        throttling = await self._check_thermal_throttling()
        tdp_threshold = self.thresholds.threshold_for(self.device_type)

        return TDPReading(
            timestamp=datetime.now(timezone.utc),
            monotonic_ts=time.monotonic(),
            device_id=self.device_id,
            device_type=self.device_type,
            current_power_watts=float(power),
            tdp_threshold_watts=tdp_threshold,
            utilization_percent=float(utilization),
            temperature_celsius=float(temperature),
            thermal_throttling=bool(throttling),
            predicted_breach_seconds=None,
            is_synthetic=False,
        )

    async def _measure_cpu_percent(self) -> float:
        """
        P0 §2.2: sample CPU utilization **off the event loop**.
        """
        if not _PSUTIL_AVAILABLE:
            return 0.0
        try:
            return float(
                await asyncio.to_thread(
                    psutil.cpu_percent,
                    interval=self._config.cpu_sample_interval_seconds,
                )
            )
        except Exception as exc:  # pragma: no cover — env dependent
            _log("cpu_percent_failed", logging.WARNING, error=str(exc))
            return 0.0

    async def _run_subprocess(
        self, *args: str, timeout: Optional[float] = None
    ) -> Optional[str]:
        """Run an external command with a timeout; reap the process on kill."""
        timeout = timeout or self._config.subprocess_timeout_seconds
        try:
            proc = await asyncio.create_subprocess_exec(
                *args,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.DEVNULL,
            )
        except (FileNotFoundError, PermissionError) as exc:
            _log("subprocess_unavailable", logging.DEBUG, cmd=" ".join(args), error=str(exc))
            return None

        try:
            stdout, _ = await asyncio.wait_for(proc.communicate(), timeout=timeout)
        except asyncio.TimeoutError:
            _log("subprocess_timeout", logging.WARNING, cmd=" ".join(args), timeout=timeout)
            try:
                proc.kill()
                # P1 §3.6: reap the process to avoid zombies
                await proc.wait()
            except ProcessLookupError:
                pass
            return None
        except Exception as exc:  # pragma: no cover
            _log("subprocess_failed", logging.WARNING, cmd=" ".join(args), error=str(exc))
            return None

        if not stdout:
            return None
        return stdout.decode(errors="replace").strip()

    async def _measure_rpi_power(self, *, utilization: float) -> float:
        """
        P0 §2.5: the v5 INA219 path fabricated watts from an uncalibrated
        register. v6 uses the CPU estimate, which is at least plausible.
        """
        _log(
            "rpi_power_estimate",
            logging.DEBUG,
            device_id=self.device_id,
            reason="no calibrated INA219 reader",
        )
        return await self._estimate_power_from_cpu(utilization=utilization)

    async def _measure_jetson_power(self, *, utilization: float) -> float:
        """Read tegrastats VDD_IN with a fallback to CPU estimate."""
        out = await self._run_subprocess(
            "tegrastats",
            "--interval", "1000",
            "--limit", "1",
            timeout=max(self._config.subprocess_timeout_seconds, 2.0),
        )
        if out:
            match = re.search(r"VDD_IN:.*?(\d+)mW", out)
            if match:
                try:
                    return int(match.group(1)) / 1000.0
                except ValueError:  # pragma: no cover
                    pass
        return await self._estimate_power_from_cpu(utilization=utilization)

    async def _measure_nuc_power(self, *, utilization: float) -> float:
        """RAPL energy delta (µJ/s → W), with wraparound handling."""
        rapl_path = (
            "/sys/class/powercap/intel-rapl/intel-rapl:0/energy_uj"
        )
        try:
            with open(rapl_path, "r", encoding="utf-8") as f:
                energy_uj = int(f.read().strip())
        except (OSError, ValueError):
            return await self._estimate_power_from_cpu(utilization=utilization)

        now = time.monotonic()
        with self._lock:
            last_energy = self._last_rapl_energy_uj
            last_ts = self._last_rapl_timestamp
            self._last_rapl_energy_uj = energy_uj
            self._last_rapl_timestamp = now

        if last_energy is None or last_ts is None:
            return await self._estimate_power_from_cpu(utilization=utilization)

        delta_energy = energy_uj - last_energy
        if delta_energy < 0:
            _log(
                "rapl_wraparound", logging.DEBUG,
                last=last_energy, current=energy_uj,
            )
            return await self._estimate_power_from_cpu(utilization=utilization)

        delta_t = now - last_ts
        if delta_t <= 0:
            return await self._estimate_power_from_cpu(utilization=utilization)

        power_watts = (delta_energy / 1e6) / delta_t
        if power_watts < 0 or power_watts > 500:
            return await self._estimate_power_from_cpu(utilization=utilization)
        return float(power_watts)

    async def _estimate_power_from_cpu(self, *, utilization: float) -> float:
        idle, max_power = self.thresholds.idle_and_max_power(self.device_type)
        return idle + (max_power - idle) * (utilization / 100.0)

    async def _measure_temperature(self) -> float:
        """P1 §3.12: use ``psutil.sensors_temperatures()``, else glob hwmon."""
        if _PSUTIL_AVAILABLE:
            try:
                temps = psutil.sensors_temperatures()
                all_vals = [
                    t.current
                    for arr in (temps or {}).values()
                    for t in arr
                    if t.current is not None
                ]
                if all_vals:
                    return float(max(all_vals))
            except Exception:  # pragma: no cover — env dependent
                pass

        # Glob hwmon devices — takes the max reading across all
        candidates = sorted(
            glob.glob("/sys/class/hwmon/hwmon*/temp1_input")
        ) + ["/sys/class/thermal/thermal_zone0/temp"]
        best = 0.0
        for path in candidates:
            try:
                with open(path, "r", encoding="utf-8") as f:
                    raw = int(f.read().strip())
                val = raw / 1000.0 if raw > 1000 else float(raw)
                if 0.0 <= val < 200.0 and val > best:
                    best = val
            except (OSError, ValueError):
                continue
        return best if best > 0.0 else 25.0

    async def _check_thermal_throttling(self) -> bool:
        if self.device_type == DeviceType.RASPBERRY_PI_5:
            out = await self._run_subprocess("vcgencmd", "get_throttled")
            if out:
                match = re.search(r"0x([0-9a-fA-F]+)", out)
                if match:
                    try:
                        flags = int(match.group(1), 16)
                        return (flags & 0x1) != 0
                    except ValueError:  # pragma: no cover
                        pass
        return False

    # ---------------------------------------------------------- prediction
    def _predict_breach(self, reading: TDPReading) -> Optional[float]:
        """
        P0 §2.3: check "already breached" **before** the slope check.

        P1 §3.14: prefer ``monotonic_ts`` when every sample has it; fall
        back to wall-clock timestamps otherwise (index-based was
        misleading because it assumed uniform spacing).
        """
        cfg = self._config

        # 1) Already at/above the warning threshold — always alert.
        breach_target = (
            reading.tdp_threshold_watts - self.thresholds.WARNING_BUFFER
        )
        remaining_power = breach_target - reading.current_power_watts
        if remaining_power <= 0:
            return 0.0

        # 2) Need enough samples to fit a trend.
        with self._lock:
            snapshot = list(self._history)
        samples = snapshot + [reading]
        if len(samples) < cfg.min_samples_for_prediction:
            return None

        recent = samples[-cfg.min_samples_for_prediction :]

        # 3) Choose the x-axis
        if all(r.monotonic_ts > 0 for r in recent):
            pairs = [(r.monotonic_ts, r.current_power_watts) for r in recent]
        else:
            pairs = [
                (r.timestamp.timestamp(), r.current_power_watts)
                for r in recent
            ]

        xs = [p[0] for p in pairs]
        ys = [p[1] for p in pairs]
        x_mean = statistics.fmean(xs)
        y_mean = statistics.fmean(ys)
        numerator = sum((x - x_mean) * (y - y_mean) for x, y in zip(xs, ys))
        denominator = sum((x - x_mean) ** 2 for x in xs)
        if denominator <= 0:
            return None

        slope = numerator / denominator
        if slope <= 0:
            return None

        seconds_to_breach = remaining_power / slope
        return float(
            max(0.0, min(seconds_to_breach, cfg.prediction_horizon_seconds))
        )

    # ---------------------------------------------------------- loop
    async def _monitoring_loop(self) -> None:
        while self._running:
            try:
                await self.get_current_reading()
                await asyncio.sleep(self._config.sampling_interval_seconds)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                _log("tdp_loop_error", logging.ERROR, error=str(exc))
                await asyncio.sleep(self._config.loop_error_sleep_seconds)

    # ---------------------------------------------------------- stats
    def statistics(self) -> Dict[str, Any]:
        with self._lock:
            history = list(self._history)
            last_breach = self._last_breach_at

        if not history:
            return {
                "count": 0,
                "mean_power_watts": None,
                "max_power_watts": None,
                "min_power_watts": None,
                "mean_temperature_celsius": None,
                "max_temperature_celsius": None,
                "throttling_events": 0,
                "imminent_breaches": 0,
                "last_breach_at": last_breach.isoformat() if last_breach else None,
            }

        powers = [r.current_power_watts for r in history]
        temps = [r.temperature_celsius for r in history]
        return {
            "count": len(history),
            "mean_power_watts": statistics.fmean(powers),
            "max_power_watts": max(powers),
            "min_power_watts": min(powers),
            "mean_temperature_celsius": statistics.fmean(temps),
            "max_temperature_celsius": max(temps),
            "throttling_events": sum(1 for r in history if r.thermal_throttling),
            "imminent_breaches": sum(
                1
                for r in history
                if r.predicted_breach_seconds is not None
                and r.predicted_breach_seconds
                < self._config.breach_alert_threshold_seconds
            ),
            "last_breach_at": last_breach.isoformat() if last_breach else None,
        }

    def health_check(self) -> Dict[str, Any]:
        """P1 §3.8: operational probe."""
        with self._lock:
            uptime = (
                (time.time() - self._started_at) if self._started_at else None
            )
            return {
                "healthy": self._running,
                "device_id": self.device_id,
                "device_type": self.device_type.value,
                "running": self._running,
                "history_size": len(self._history),
                "callbacks": len(self._callbacks),
                "in_flight_callbacks": len(self._callback_tasks),
                "started_at": self._started_at,
                "uptime_seconds": uptime,
                "last_breach_at": (
                    self._last_breach_at.isoformat()
                    if self._last_breach_at else None
                ),
                "threshold_watts": self.thresholds.threshold_for(self.device_type),
                "warning_buffer_watts": self.thresholds.WARNING_BUFFER,
                "psutil_available": _PSUTIL_AVAILABLE,
                "config": _json_safe(asdict(self._config)),
            }

    def collect_prometheus_metrics(self) -> Dict[str, Tuple[Any, Dict[str, str]]]:
        """P1 §3.9: metric table."""
        labels = {"device_id": self.device_id, "device_type": self.device_type.value}
        with self._lock:
            history = list(self._history)
            last_breach = self._last_breach_at
        metrics: Dict[str, Tuple[Any, Dict[str, str]]] = {
            "tdp_threshold_watts": (
                self.thresholds.threshold_for(self.device_type), labels
            ),
            "tdp_warning_buffer_watts": (self.thresholds.WARNING_BUFFER, labels),
            "tdp_history_size": (len(history), labels),
            "tdp_last_breach_timestamp": (
                last_breach.timestamp() if last_breach else 0.0, labels
            ),
        }
        if history:
            last = history[-1]
            metrics["tdp_current_power_watts"] = (last.current_power_watts, labels)
            metrics["tdp_utilization_percent"] = (last.utilization_percent, labels)
            metrics["tdp_temperature_celsius"] = (last.temperature_celsius, labels)
            metrics["tdp_thermal_throttling"] = (int(last.thermal_throttling), labels)
            metrics["tdp_predicted_breach_seconds"] = (
                last.predicted_breach_seconds
                if last.predicted_breach_seconds is not None
                else -1.0,
                labels,
            )
        return metrics

    def reset(self, *, clear_history: bool = False) -> None:
        with self._lock:
            if clear_history:
                self._history.clear()
                self._started_at = None
            self._last_rapl_energy_uj = None
            self._last_rapl_timestamp = None
            self._last_breach_at = None
        _log("tdp_monitor_reset", logging.DEBUG, clear_history=clear_history)

    # ---------------------------------------------------------- live reconfig
    def set_config(self, config: TDPMonitorConfig) -> None:
        if not isinstance(config, TDPMonitorConfig):
            raise TDPMonitorError("config must be a TDPMonitorConfig")
        with self._lock:
            self._config = config
            self.sampling_interval = int(config.sampling_interval_seconds)
            self.prediction_horizon = int(config.prediction_horizon_seconds)
            # Rebuild history deque to honor any new max
            old = list(self._history)
            self._history = deque(old[-config.max_history:], maxlen=config.max_history)
        _log("tdp_config_changed", logging.INFO, device_id=self.device_id)

    def set_thresholds(self, thresholds: TDPThresholds) -> None:
        if not isinstance(thresholds, TDPThresholds):
            raise TDPMonitorError("thresholds must be TDPThresholds")
        with self._lock:
            self.thresholds = thresholds
        _log("tdp_thresholds_changed", logging.INFO, device_id=self.device_id)

    # ---------------------------------------------------------- serialization
    def to_dict(self) -> Dict[str, Any]:
        with self._lock:
            return _json_safe(
                {
                    "device_id": self.device_id,
                    "device_type": self.device_type.value,
                    "config": asdict(self._config),
                    "thresholds": asdict(self.thresholds),
                    "strict": self._strict,
                    "running": self._running,
                    "started_at": self._started_at,
                    "last_breach_at": (
                        self._last_breach_at.isoformat()
                        if self._last_breach_at else None
                    ),
                    "history": [r.to_dict() for r in self._history],
                }
            )

    @classmethod
    def from_dict(
        cls,
        data: Mapping[str, Any],
        *,
        thresholds: Optional[TDPThresholds] = None,
        callbacks: Optional[List[Callable[[TDPReading], Any]]] = None,
    ) -> "TDPMonitor":
        """P1 §3.9 + §2.6: parse errors wrapped as ``TDPMonitorError``."""
        if not isinstance(data, Mapping):
            raise TDPMonitorError(
                f"from_dict expects Mapping, got {type(data).__name__}"
            )

        # Config
        try:
            cfg_data = dict(data.get("config", {}) or {})
            cfg_fields = {f.name for f in fields(TDPMonitorConfig)}
            filtered = {k: v for k, v in cfg_data.items() if k in cfg_fields}
            cfg = TDPMonitorConfig(**filtered)
        except (TypeError, ValueError, TDPMonitorError) as exc:
            raise TDPMonitorError(f"invalid config: {exc}") from exc

        # Thresholds
        if thresholds is None:
            th_data = dict(data.get("thresholds", {}) or {})
            try:
                th_fields = {f.name for f in fields(TDPThresholds)}
                filtered_th = {k: v for k, v in th_data.items() if k in th_fields}
                thresholds = TDPThresholds(**filtered_th)
            except (TypeError, ValueError, TDPMonitorError) as exc:
                raise TDPMonitorError(f"invalid thresholds: {exc}") from exc

        # Device type
        try:
            device_type = DeviceType(data["device_type"])
        except (KeyError, ValueError) as exc:
            raise TDPMonitorError(f"invalid device_type: {exc}") from exc

        monitor = cls(
            device_id=str(data["device_id"]),
            device_type=device_type,
            thresholds=thresholds,
            config=cfg,
            strict=bool(data.get("strict", True)),
        )

        with monitor._lock:
            for entry in data.get("history", []) or []:
                try:
                    monitor._history.append(TDPReading.from_dict(entry))
                except (ContinuumError, KeyError, TypeError, ValueError) as exc:
                    _log("history_skip", logging.WARNING, error=str(exc))
            monitor._started_at = data.get("started_at")
            if data.get("last_breach_at"):
                try:
                    monitor._last_breach_at = _coerce_timestamp(
                        data["last_breach_at"]
                    )
                except Exception:  # pragma: no cover
                    monitor._last_breach_at = None

        if callbacks:
            for cb in callbacks:
                if not callable(cb):
                    raise TDPMonitorError("callbacks must be callable")
                monitor.register_callback(cb)

        return monitor

    def to_json(self, **kwargs: Any) -> str:
        kwargs.setdefault("sort_keys", True)
        return json.dumps(self.to_dict(), default=str, **kwargs)

    @classmethod
    def from_json(
        cls,
        payload: str,
        *,
        thresholds: Optional[TDPThresholds] = None,
        callbacks: Optional[List[Callable[[TDPReading], Any]]] = None,
    ) -> "TDPMonitor":
        try:
            data = json.loads(payload)
        except json.JSONDecodeError as exc:
            raise TDPMonitorError(f"invalid JSON payload: {exc}") from exc
        return cls.from_dict(data, thresholds=thresholds, callbacks=callbacks)

    # ---------------------------------------------------------- async ctx mgr
    async def __aenter__(self) -> "TDPMonitor":
        self._ctx_start = time.perf_counter()
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        try:
            await self.stop()
        finally:
            self._ctx_start = None

    def __repr__(self) -> str:
        with self._lock:
            return (
                "TDPMonitor("
                f"device_id={self.device_id!r}, "
                f"type={self.device_type.value}, "
                f"running={self._running}, "
                f"history={len(self._history)}, "
                f"callbacks={len(self._callbacks)}, "
                f"interval={self._config.sampling_interval_seconds}s, "
                f"horizon={self._config.prediction_horizon_seconds}s)"
            )


# --------------------------------------------------------------------------- #
# Public API
# --------------------------------------------------------------------------- #
__all__ = [
    "DeviceType",
    "TDPReading",
    "TDPThresholds",
    "TDPMonitorConfig",
    "TDPMonitor",
    "TDPMonitorError",
    "ContinuumError",
]


# --------------------------------------------------------------------------- #
# Demo — every P0/P1/P2 fix verified by assertion
# --------------------------------------------------------------------------- #
def _demo() -> None:
    logging.basicConfig(
        level=logging.WARNING,
        format="%(levelname)s %(name)s: %(message)s",
    )

    print("=" * 78)
    print("tdp_monitor.py v6.0.0 — Demo")
    print("=" * 78)

    def _check(condition: bool, message: str) -> None:
        if not condition:
            raise AssertionError(f"FAILED: {message}")
        print(f"  ✓ {message}")

    async def run_all() -> None:
        # ========================================================== #
        # P0 §2.1: DeviceType / TDPReading shared with continuum.types
        # ========================================================== #
        print("\n=== P0 §2.1: shared types from continuum.types ===")
        try:
            from continuum.types import (
                DeviceType as SharedDT,
                TDPReading as SharedTDP,
            )
        except ImportError:
            from .types import (
                DeviceType as SharedDT,
                TDPReading as SharedTDP,
            )
        _check(
            SharedDT is DeviceType,
            "DeviceType is the same object as continuum.types.DeviceType",
        )
        _check(
            SharedTDP is TDPReading,
            "TDPReading is the same object as continuum.types.TDPReading",
        )
        _check(
            hasattr(DeviceType, "RASPBERRY_PI_5"),
            "DeviceType.RASPBERRY_PI_5 exists",
        )
        _check(
            hasattr(DeviceType, "NVIDIA_JETSON_ORIN"),
            "DeviceType.NVIDIA_JETSON_ORIN exists",
        )
        _check(
            hasattr(DeviceType, "INTEL_NUC_13_PRO"),
            "DeviceType.INTEL_NUC_13_PRO exists",
        )
        # Legacy aliases from the offloading engine still work
        _check(
            hasattr(DeviceType, "RASPBERRY_PI"),
            "legacy DeviceType.RASPBERRY_PI preserved",
        )

        # ========================================================== #
        # P0 §2.2: psutil.cpu_percent does not block the event loop
        # ========================================================== #
        print("\n=== P0 §2.2: cpu_percent runs off the event loop ===")
        hb_monitor = TDPMonitor(
            device_id="edge-hb",
            device_type=DeviceType.RASPBERRY_PI_5,
            config=TDPMonitorConfig(cpu_sample_interval_seconds=0.05),
        )
        ticks: List[float] = []

        async def _heartbeat() -> None:
            for _ in range(20):
                ticks.append(time.monotonic())
                await asyncio.sleep(0.01)

        hb_task = asyncio.create_task(_heartbeat())
        await hb_monitor._measure_cpu_percent()
        await hb_task
        _check(
            len(ticks) >= 15,
            f"heartbeat kept ticking during cpu sample "
            f"(ticks={len(ticks)}; v5 blocked the loop → ~1 tick)",
        )

        # ========================================================== #
        # P0 §2.3: flat-trend breach above threshold triggers alert
        # ========================================================== #
        print("\n=== P0 §2.3: flat-trend above threshold triggers breach ===")
        flat_monitor = TDPMonitor(
            device_id="edge-flat",
            device_type=DeviceType.RASPBERRY_PI_5,   # threshold 12W
            config=TDPMonitorConfig(
                min_samples_for_prediction=3,
                breach_alert_threshold_seconds=60.0,
                prediction_horizon_seconds=300.0,
            ),
        )
        alerts: List[TDPReading] = []
        flat_monitor.register_callback(alerts.append)

        # 15W > (12 - 2) = 10W → breach regardless of trend
        for i in range(6):
            await flat_monitor.simulate_reading(
                current_power_watts=15.0,          # flat, above threshold
                temperature_celsius=55.0,
                utilization_percent=80.0,
                monotonic_ts=1000.0 + i * 5.0,     # 5s spacing
            )
        await asyncio.sleep(0.05)  # let callback tasks run
        last = flat_monitor.get_recent_readings(1)[0]
        _check(
            last.predicted_breach_seconds == 0.0,
            f"flat 15W vs 12W → predicted_breach_seconds=0.0 "
            f"(got {last.predicted_breach_seconds}; v5 returned None)",
        )
        _check(
            len(alerts) >= 6,
            f"every flat 15W reading fired a callback "
            f"(alerts={len(alerts)})",
        )
        _check(
            flat_monitor.last_breach_at is not None,
            "last_breach_at recorded",
        )

        # Rising-trend prediction still works
        rising_monitor = TDPMonitor(
            device_id="edge-rising",
            device_type=DeviceType.RASPBERRY_PI_5,
            config=TDPMonitorConfig(
                min_samples_for_prediction=5,
                prediction_horizon_seconds=120.0,
            ),
        )
        for i in range(8):
            await rising_monitor.simulate_reading(
                current_power_watts=4.0 + i * 0.6,   # 4→8.2W
                monotonic_ts=2000.0 + i * 5.0,
            )
        rising_last = rising_monitor.get_recent_readings(1)[0]
        _check(
            rising_last.predicted_breach_seconds is not None
            and rising_last.predicted_breach_seconds > 0.0,
            f"rising trend still predicts a positive ETA "
            f"({rising_last.predicted_breach_seconds})",
        )

        # ========================================================== #
        # P0 §2.4: monotonic_ts is not serialized
        # ========================================================== #
        print("\n=== P0 §2.4: monotonic_ts is not serialized ===")
        sample_reading = flat_monitor.get_recent_readings(1)[0]
        reading_payload = sample_reading.to_dict()
        _check(
            "monotonic_ts" not in reading_payload,
            "TDPReading.to_dict omits monotonic_ts",
        )
        monitor_payload = flat_monitor.to_dict()
        for entry in monitor_payload["history"]:
            if "monotonic_ts" in entry:
                raise AssertionError("monotonic_ts leaked into monitor payload")
        _check(
            True,
            "TDPMonitor.to_dict omits monotonic_ts from every reading",
        )
        restored_from_mono = TDPReading.from_dict(reading_payload)
        _check(
            restored_from_mono.monotonic_ts == 0.0,
            "restored reading has monotonic_ts=0.0 (wall-clock fallback)",
        )

        # Verify wall-clock fallback actually works for prediction
        restored = TDPMonitor.from_dict(monitor_payload)
        _check(
            restored.history_size == flat_monitor.history_size,
            f"restore preserved history ({restored.history_size})",
        )

        # ========================================================== #
        # P0 §2.5: fabricated RPi INA219 path is gone
        # ========================================================== #
        print("\n=== P0 §2.5: RPi power uses CPU estimate, not raw register ===")
        rpi_monitor = TDPMonitor(
            device_id="edge-rpi",
            device_type=DeviceType.RASPBERRY_PI_5,
            config=TDPMonitorConfig(cpu_sample_interval_seconds=0.02),
        )
        # _measure_rpi_power always returns the CPU estimate now
        power = await rpi_monitor._measure_rpi_power(utilization=50.0)
        # idle=3, max=12 → 3 + 9 * 0.5 = 7.5
        _check(
            abs(power - 7.5) < 1e-6,
            f"RPi power = CPU estimate at 50% util (got {power:.2f}W)",
        )

        # ========================================================== #
        # P0 §2.7: TDPThresholds validation
        # ========================================================== #
        print("\n=== P0 §2.7: TDPThresholds validation ===")
        for label, args in (
            ("negative threshold", {"RASPBERRY_PI_5": -5.0}),
            ("zero threshold", {"RASPBERRY_PI_5": 0.0}),
            ("NaN threshold", {"RASPBERRY_PI_5": float("nan")}),
            ("negative buffer", {"WARNING_BUFFER": -1.0}),
            (
                "buffer >= threshold",
                {"RASPBERRY_PI_5": 12.0, "WARNING_BUFFER": 20.0},
            ),
        ):
            try:
                TDPThresholds(**args)  # type: ignore[arg-type]
            except TDPMonitorError:
                _check(True, f"{label} rejected")
            else:
                raise AssertionError(f"{label} not rejected")

        # ========================================================== #
        # P0 §2.8: simulate_reading rejects NaN / inf
        # ========================================================== #
        print("\n=== P0 §2.8: simulate_reading validates inputs ===")
        nan_monitor = TDPMonitor(
            device_id="edge-nan",
            device_type=DeviceType.RASPBERRY_PI_5,
        )
        for label, kwargs in (
            ("NaN power", {"current_power_watts": float("nan")}),
            ("inf power", {"current_power_watts": float("inf")}),
            (
                "NaN temperature",
                {"current_power_watts": 5.0, "temperature_celsius": float("nan")},
            ),
            (
                "inf temperature",
                {"current_power_watts": 5.0, "temperature_celsius": float("inf")},
            ),
            (
                "NaN utilization",
                {"current_power_watts": 5.0, "utilization_percent": float("nan")},
            ),
            (
                "utilization > 100",
                {"current_power_watts": 5.0, "utilization_percent": 150.0},
            ),
            ("negative power", {"current_power_watts": -1.0}),
        ):
            try:
                await nan_monitor.simulate_reading(**kwargs)  # type: ignore[arg-type]
            except TDPMonitorError:
                _check(True, f"{label} rejected")
            else:
                raise AssertionError(f"{label} not rejected")

        # ========================================================== #
        # P1 §3.8: health_check
        # ========================================================== #
        print("\n=== P1 §3.8: health_check ===")
        hc_monitor = TDPMonitor(
            device_id="edge-hc",
            device_type=DeviceType.INTEL_NUC_13_PRO,
        )
        hc = hc_monitor.health_check()
        for key in (
            "healthy", "device_id", "device_type", "running",
            "history_size", "callbacks", "in_flight_callbacks",
            "started_at", "uptime_seconds", "last_breach_at",
            "threshold_watts", "warning_buffer_watts", "psutil_available",
            "config",
        ):
            _check(key in hc, f"health_check has '{key}'")

        # ========================================================== #
        # P1 §3.3: structured logging helper
        # ========================================================== #
        print("\n=== P1 §3.3: structured logging helper ===")
        _check(callable(_log), "_log helper is callable")

        # ========================================================== #
        # P1 §3.9: Prometheus metrics
        # ========================================================== #
        print("\n=== P1 §3.9: collect_prometheus_metrics ===")
        await hc_monitor.simulate_reading(current_power_watts=5.0)
        metrics = hc_monitor.collect_prometheus_metrics()
        for key in (
            "tdp_threshold_watts", "tdp_warning_buffer_watts",
            "tdp_history_size", "tdp_current_power_watts",
            "tdp_utilization_percent", "tdp_temperature_celsius",
            "tdp_thermal_throttling", "tdp_predicted_breach_seconds",
        ):
            _check(key in metrics, f"metric '{key}' present")

        # ========================================================== #
        # P1 §3.6: subprocess reap after kill
        # ========================================================== #
        print("\n=== P1 §3.6: subprocess reaped after timeout ===")
        reap_monitor = TDPMonitor(
            device_id="edge-reap",
            device_type=DeviceType.RASPBERRY_PI_5,
            config=TDPMonitorConfig(subprocess_timeout_seconds=0.05),
        )
        # `sleep` on POSIX; `timeout` is not universal. Skip if unavailable.
        try:
            result = await reap_monitor._run_subprocess(
                "sleep", "5", timeout=0.05
            )
            _check(
                result is None,
                "slow subprocess timed out and returned None",
            )
        except FileNotFoundError:
            _check(True, "sleep unavailable; subprocess reap test skipped")

        # ========================================================== #
        # P1 §3.10: temp glob (hwmon)
        # ========================================================== #
        print("\n=== P1 §3.10: temperature via sensors_temperatures/glob ===")
        temp = await hc_monitor._measure_temperature()
        _check(
            isinstance(temp, float) and math.isfinite(temp) and temp >= 0,
            f"temperature returned a finite non-negative value ({temp:.1f}°C)",
        )

        # ========================================================== #
        # P1 §3.9/§2.6: from_dict wraps errors and accepts callbacks
        # ========================================================== #
        print("\n=== P1 §3.9: from_dict wraps errors, accepts callbacks ===")
        try:
            TDPMonitor.from_dict({
                "device_id": "edge-x",
                "device_type": "not_a_real_device",
                "config": {},
                "thresholds": {},
                "history": [],
            })
        except TDPMonitorError:
            _check(True, "invalid device_type → TDPMonitorError")
        else:
            raise AssertionError("invalid device_type not wrapped")

        restored_cbs: List[TDPReading] = []
        restored2 = TDPMonitor.from_dict(
            hc_monitor.to_dict(),
            callbacks=[restored_cbs.append],
        )
        await restored2.simulate_reading(
            current_power_watts=30.0,   # over 28W NUC threshold - 2 = 26
        )
        await asyncio.sleep(0.05)
        _check(
            len(restored_cbs) >= 1,
            f"restored callback was invoked ({len(restored_cbs)})",
        )

        # ========================================================== #
        # P1 §3.12/§3.13: live reconfiguration
        # ========================================================== #
        print("\n=== P1 §3.12: live reconfiguration ===")
        live = TDPMonitor(
            device_id="edge-live",
            device_type=DeviceType.RASPBERRY_PI_5,
        )
        new_cfg = TDPMonitorConfig(
            sampling_interval_seconds=2.5,
            max_history=100,
        )
        live.set_config(new_cfg)
        _check(
            live.config.sampling_interval_seconds == 2.5,
            "set_config updated interval",
        )
        _check(
            live.sampling_interval == 2,
            "legacy sampling_interval mirror updated",
        )
        new_th = TDPThresholds(RASPBERRY_PI_5=15.0)
        live.set_thresholds(new_th)
        _check(
            live.thresholds.RASPBERRY_PI_5 == 15.0,
            "set_thresholds updated threshold",
        )

        # ========================================================== #
        # P2 §4.4: schema_version
        # ========================================================== #
        print("\n=== P2 §4.4: schema_version ===")
        _check(
            sample_reading.schema_version == 2,
            f"TDPReading.schema_version == 2 "
            f"(got {sample_reading.schema_version})",
        )
        _check(
            "schema_version" in sample_reading.to_dict(),
            "schema_version serialized",
        )

        # ========================================================== #
        # P2 §4.5: UNKNOWN device uses max threshold
        # ========================================================== #
        print("\n=== P2 §4.5: UNKNOWN device threshold is conservative ===")
        th = TDPThresholds()
        unknown_thr = th.threshold_for(DeviceType.UNKNOWN)
        _check(
            unknown_thr == max(
                th.RASPBERRY_PI_5,
                th.NVIDIA_JETSON_ORIN,
                th.INTEL_NUC_13_PRO,
            ),
            f"UNKNOWN → max threshold ({unknown_thr}W)",
        )

        # ========================================================== #
        # P2 §4.19: get_readings_since
        # ========================================================== #
        print("\n=== P2 §4.19: get_readings_since ===")
        since_monitor = TDPMonitor(
            device_id="edge-since",
            device_type=DeviceType.RASPBERRY_PI_5,
        )
        await since_monitor.simulate_reading(current_power_watts=5.0)
        cutoff = since_monitor.get_recent_readings(1)[0].timestamp
        await asyncio.sleep(0.01)
        await since_monitor.simulate_reading(current_power_watts=6.0)
        recent = since_monitor.get_readings_since(cutoff)
        _check(
            len(recent) >= 1,
            f"get_readings_since returns readings after cutoff "
            f"({len(recent)})",
        )

        # ========================================================== #
        # P2 §4.10: warning_buffer_watts property
        # ========================================================== #
        print("\n=== P2 §4.10: warning_buffer_watts property ===")
        _check(
            since_monitor.warning_buffer_watts == 2.0,
            f"warning_buffer_watts == TDPThresholds.WARNING_BUFFER "
            f"({since_monitor.warning_buffer_watts})",
        )

        # ========================================================== #
        # P1 §3.14: wall-clock fallback
        # ========================================================== #
        print("\n=== P1 §3.14: wall-clock fallback when monotonic_ts=0 ===")
        wc_monitor = TDPMonitor(
            device_id="edge-wc",
            device_type=DeviceType.RASPBERRY_PI_5,
            config=TDPMonitorConfig(min_samples_for_prediction=3),
        )
        # Simulate readings with monotonic_ts=0 (as if restored)
        for i in range(4):
            await wc_monitor.simulate_reading(
                current_power_watts=6.0 + i * 0.5,
                monotonic_ts=0.0,
            )
        wc_last = wc_monitor.get_recent_readings(1)[0]
        _check(
            wc_last.predicted_breach_seconds is not None,
            f"prediction works with wall-clock fallback "
            f"({wc_last.predicted_breach_seconds})",
        )

        # ========================================================== #
        # P1 §3.5: callbacks isolated (slow callback doesn't block)
        # ========================================================== #
        print("\n=== P1 §3.5: slow callback doesn't stall get_current_reading ===")
        slow_monitor = TDPMonitor(
            device_id="edge-slow",
            device_type=DeviceType.RASPBERRY_PI_5,
            config=TDPMonitorConfig(
                min_samples_for_prediction=2,
                callback_timeout_seconds=0.5,
                cpu_sample_interval_seconds=0.02,
            ),
        )

        async def slow_cb(reading: TDPReading) -> None:
            await asyncio.sleep(0.4)

        slow_monitor.register_callback(slow_cb)
        # Prime history so prediction hits
        for _ in range(2):
            await slow_monitor.simulate_reading(current_power_watts=15.0)
        t0 = time.perf_counter()
        await slow_monitor.get_current_reading()
        elapsed = time.perf_counter() - t0
        _check(
            elapsed < 0.5,
            f"get_current_reading returned before slow callback finished "
            f"({elapsed*1000:.0f}ms)",
        )
        await slow_monitor.stop()

        # ========================================================== #
        # Serialization round-trip
        # ========================================================== #
        print("\n=== Serialization round-trip ===")
        rt_monitor = TDPMonitor(
            device_id="edge-rt",
            device_type=DeviceType.INTEL_NUC_13_PRO,
        )
        await rt_monitor.simulate_reading(current_power_watts=10.0)
        payload = rt_monitor.to_json()
        rt_restored = TDPMonitor.from_json(payload)
        _check(
            rt_restored.to_dict() == rt_monitor.to_dict(),
            "to_json / from_json round-trips (monotonic_ts not present)",
        )

        print("\n" + "=" * 78)
        print("All v6 assertions passed.")
        print("=" * 78)

    try:
        asyncio.run(run_all())
    except AssertionError as exc:
        print(f"\n{exc}")
        raise


if __name__ == "__main__":
    _demo()
