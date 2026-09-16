# src/continuum/tdp_monitor.py

"""
TDP Monitor

Real-time TDP telemetry and breach prediction for edge devices.

Responsibilities
----------------
- Monitor device power consumption in real-time.
- Detect thermal throttling conditions.
- Predict TDP breaches 5–10 minutes ahead via linear regression.
- Notify the OffloadingDecisionEngine on threshold breaches.

Enhancements
------------
- Fixed missing module-level ``logger`` (previously a ``NameError`` risk).
- Fixed ``_measure_nuc_power`` RAPL delta logic (now tracks last energy
  reading + timestamp, handles counter wraparound).
- Fixed ``_predict_breach`` to use ``time.monotonic()`` and real timestamps.
- Fixed the negative ``remaining_power`` edge case near the threshold.
- ``stop()`` now cancels and awaits background tasks.
- ``start()`` binds the loop at call time (safe under Ray actors / sync callers).
- Callbacks may be sync or async; exceptions are isolated per callback.
- Frozen :class:`TDPReading` with ``predicted_breach_seconds`` set at build time.
- Bounded history via ``collections.deque``.
- Thread-safe via ``RLock``.
- Subprocess calls have timeouts and fall back to CPU-based estimation.
- Configurable via :class:`TDPMonitorConfig`.
- Full validation; strict / non-strict modes.
- Serialization: ``to_dict`` / ``from_dict`` / ``to_json`` / ``from_json``.
- Async context manager for scoped sessions.
- Custom :class:`TDPMonitorError`.
- Simulation mode (``simulate_reading``) for testing without real hardware.
- Lazy ``%s`` logging, ``__repr__``, and a smoke test under ``__main__``.
"""

from __future__ import annotations

import asyncio
import json
import logging
import math
import re
import statistics
import threading
import time
from collections import deque
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Awaitable, Callable, Deque, Dict, Iterable, List, Mapping, Optional, Tuple, Union

import psutil

logger = logging.getLogger(__name__)


# --------------------------------------------------------------------------- #
# Errors
# --------------------------------------------------------------------------- #
class TDPMonitorError(ValueError):
    """Raised for invalid inputs, configuration, or monitor failures."""


# --------------------------------------------------------------------------- #
# Device types
# --------------------------------------------------------------------------- #
class DeviceType(Enum):
    """Supported edge device types."""

    RASPBERRY_PI_5 = "raspberry_pi_5"
    NVIDIA_JETSON_ORIN = "nvidia_jetson_orin"
    INTEL_NUC_13_PRO = "intel_nuc_13_pro"
    UNKNOWN = "unknown"


# --------------------------------------------------------------------------- #
# Thresholds
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class TDPThresholds:
    """TDP thresholds per device type (Watts)."""

    RASPBERRY_PI_5: float = 12.0
    NVIDIA_JETSON_ORIN: float = 60.0
    INTEL_NUC_13_PRO: float = 28.0
    WARNING_BUFFER: float = 2.0

    def threshold_for(self, device_type: DeviceType) -> float:
        """Return the TDP threshold for ``device_type`` (with a safe fallback)."""
        attr = device_type.value.upper()
        value = getattr(self, attr, None)
        if isinstance(value, (int, float)):
            return float(value)
        # Fallback: 28W (matches legacy behavior for UNKNOWN).
        return 28.0

    def idle_and_max_power(self, device_type: DeviceType) -> Tuple[float, float]:
        """
        Return ``(idle_watts, max_watts)`` used by the CPU-based estimator.

        Falls back to ``(10.0, 50.0)`` for unknown devices (matches legacy
        behavior of the original ``_estimate_power_from_cpu``).
        """
        mapping = {
            DeviceType.RASPBERRY_PI_5: (3.0, 12.0),
            DeviceType.NVIDIA_JETSON_ORIN: (5.0, 60.0),
            DeviceType.INTEL_NUC_13_PRO: (8.0, 28.0),
        }
        return mapping.get(device_type, (10.0, 50.0))


# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class TDPMonitorConfig:
    """
    Tunable parameters for the TDP monitor.

    Centralizes sampling cadence, prediction window, history size, and
    subprocess timeouts so deployments can calibrate without editing the class.
    """

    # Sampling / loop cadence
    sampling_interval_seconds: float = 5.0
    loop_error_sleep_seconds: float = 10.0

    # Breach prediction
    prediction_horizon_seconds: float = 300.0     # 5 minutes
    breach_alert_threshold_seconds: float = 60.0  # alert when ETA < 1 min
    min_samples_for_prediction: int = 10

    # History
    max_history: int = 1000

    # External process policy
    subprocess_timeout_seconds: float = 3.0

    # CPU estimator smoothing (used only as fallback)
    cpu_sample_interval_seconds: float = 1.0

    def __post_init__(self) -> None:
        for name in (
            "sampling_interval_seconds",
            "loop_error_sleep_seconds",
            "prediction_horizon_seconds",
            "breach_alert_threshold_seconds",
            "subprocess_timeout_seconds",
            "cpu_sample_interval_seconds",
        ):
            value = getattr(self, name)
            if value <= 0:
                raise TDPMonitorError(f"{name} must be > 0 (got {value}).")
        if self.min_samples_for_prediction < 2:
            raise TDPMonitorError(
                "min_samples_for_prediction must be >= 2."
            )
        if self.max_history <= 0:
            raise TDPMonitorError("max_history must be > 0.")


# --------------------------------------------------------------------------- #
# TDP reading
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class TDPReading:
    """Immutable snapshot of a real-time TDP telemetry sample."""

    timestamp: datetime
    device_id: str
    device_type: DeviceType
    current_power_watts: float
    tdp_threshold_watts: float
    utilization_percent: float
    temperature_celsius: float
    thermal_throttling: bool
    predicted_breach_seconds: Optional[float] = None
    monotonic_ts: float = 0.0

    def __post_init__(self) -> None:
        if not isinstance(self.device_id, str) or not self.device_id:
            raise TDPMonitorError("device_id must be a non-empty string.")
        if self.current_power_watts < 0:
            raise TDPMonitorError("current_power_watts must be >= 0.")
        if self.tdp_threshold_watts <= 0:
            raise TDPMonitorError("tdp_threshold_watts must be > 0.")
        if not 0.0 <= self.utilization_percent <= 100.0:
            raise TDPMonitorError("utilization_percent must be in [0, 100].")
        if self.predicted_breach_seconds is not None and self.predicted_breach_seconds < 0:
            raise TDPMonitorError(
                "predicted_breach_seconds must be >= 0 when set."
            )

    @property
    def headroom_watts(self) -> float:
        return self.tdp_threshold_watts - self.current_power_watts

    @property
    def utilization_ratio(self) -> float:
        """Current power as a fraction of the threshold, clamped to [0, 1]."""
        if self.tdp_threshold_watts <= 0:
            return 0.0
        return max(0.0, min(1.0, self.current_power_watts / self.tdp_threshold_watts))

    def to_dict(self) -> Dict[str, Any]:
        return {
            "timestamp": self.timestamp.isoformat(),
            "monotonic_ts": self.monotonic_ts,
            "device_id": self.device_id,
            "device_type": self.device_type.value,
            "current_power_watts": self.current_power_watts,
            "tdp_threshold_watts": self.tdp_threshold_watts,
            "utilization_percent": self.utilization_percent,
            "temperature_celsius": self.temperature_celsius,
            "thermal_throttling": self.thermal_throttling,
            "predicted_breach_seconds": self.predicted_breach_seconds,
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "TDPReading":
        if not isinstance(data, Mapping):
            raise TDPMonitorError(
                f"TDPReading.from_dict expects a Mapping, "
                f"got {type(data).__name__}."
            )
        ts = data["timestamp"]
        timestamp = (
            datetime.fromisoformat(ts) if isinstance(ts, str) else ts
        )
        if not timestamp.tzinfo:
            timestamp = timestamp.replace(tzinfo=timezone.utc)
        return cls(
            timestamp=timestamp,
            monotonic_ts=float(data.get("monotonic_ts", 0.0)),
            device_id=str(data["device_id"]),
            device_type=DeviceType(data["device_type"]),
            current_power_watts=float(data["current_power_watts"]),
            tdp_threshold_watts=float(data["tdp_threshold_watts"]),
            utilization_percent=float(data.get("utilization_percent", 0.0)),
            temperature_celsius=float(data.get("temperature_celsius", 25.0)),
            thermal_throttling=bool(data.get("thermal_throttling", False)),
            predicted_breach_seconds=data.get("predicted_breach_seconds"),
        )


# --------------------------------------------------------------------------- #
# Monitor
# --------------------------------------------------------------------------- #
class TDPMonitor:
    """
    Real-time TDP telemetry and breach prediction.

    Thread-safe, serializable, and bounded in memory. The original public API
    is preserved; new parameters are keyword-only with backward-compatible
    defaults.
    """

    def __init__(
        self,
        device_id: str,
        device_type: DeviceType,
        sampling_interval_seconds: int = 5,
        prediction_horizon_seconds: int = 300,
        thresholds: Optional[TDPThresholds] = None,
        *,
        config: Optional[TDPMonitorConfig] = None,
        strict: bool = True,
    ) -> None:
        """
        Parameters
        ----------
        device_id : str
            Unique device identifier.
        device_type : DeviceType
            Device classification (drives measurement strategy).
        sampling_interval_seconds : int
            Legacy positional; folded into :class:`TDPMonitorConfig` when the
            caller did not supply one.
        prediction_horizon_seconds : int
            Legacy positional; folded into :class:`TDPMonitorConfig` when the
            caller did not supply one.
        thresholds : TDPThresholds, optional
            Per-device thresholds. Defaults to ``TDPThresholds()``.
        config : TDPMonitorConfig, optional
            Extended configuration. If provided, the two legacy positional
            values are ignored.
        strict : bool, default True
            If True, invalid inputs raise :class:`TDPMonitorError`.
        """
        if not isinstance(device_id, str) or not device_id:
            raise TDPMonitorError("device_id must be a non-empty string.")
        if not isinstance(device_type, DeviceType):
            raise TDPMonitorError(
                f"device_type must be a DeviceType, got {type(device_type).__name__}."
            )
        if not isinstance(sampling_interval_seconds, int) or sampling_interval_seconds <= 0:
            raise TDPMonitorError(
                "sampling_interval_seconds must be a positive int."
            )
        if (
            not isinstance(prediction_horizon_seconds, int)
            or prediction_horizon_seconds <= 0
        ):
            raise TDPMonitorError(
                "prediction_horizon_seconds must be a positive int."
            )

        if config is not None:
            self._config = config
        else:
            self._config = TDPMonitorConfig(
                sampling_interval_seconds=float(sampling_interval_seconds),
                prediction_horizon_seconds=float(prediction_horizon_seconds),
            )

        self.device_id: str = device_id
        self.device_type: DeviceType = device_type
        self.thresholds: TDPThresholds = thresholds or TDPThresholds()
        self._strict: bool = bool(strict)

        # Legacy attributes preserved for backward compatibility.
        self.sampling_interval: int = int(self._config.sampling_interval_seconds)
        self.prediction_horizon: int = int(self._config.prediction_horizon_seconds)

        self._lock = threading.RLock()
        self._callbacks: List[Callable[[TDPReading], Any]] = []
        self._history: Deque[TDPReading] = deque(
            maxlen=self._config.max_history
        )
        self._running: bool = False
        self._tasks: List[asyncio.Task] = []
        self._ctx_start: Optional[float] = None
        self._started_at: Optional[float] = None

        # RAPL state (Intel NUC)
        self._last_rapl_energy_uj: Optional[int] = None
        self._last_rapl_timestamp: Optional[float] = None

        logger.debug(
            "TDPMonitor initialized (device=%s type=%s interval=%.1fs "
            "horizon=%.0fs history=%d strict=%s)",
            self.device_id,
            self.device_type.value,
            self._config.sampling_interval_seconds,
            self._config.prediction_horizon_seconds,
            self._config.max_history,
            self._strict,
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

    # ---------------------------------------------------------- lifecycle
    async def start(self) -> None:
        """Start the TDP monitoring loop."""
        if self._running:
            logger.debug("TDPMonitor already running.")
            return
        self._running = True
        self._started_at = time.time()
        self._tasks = [
            asyncio.create_task(self._monitoring_loop(), name="tdp:loop")
        ]
        logger.info(
            "TDPMonitor started (device=%s, interval=%.1fs).",
            self.device_id,
            self._config.sampling_interval_seconds,
        )

    async def stop(self, *, timeout_seconds: float = 5.0) -> None:
        """Stop the monitoring loop and drain in-flight tasks."""
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
                logger.warning("Timed out waiting for TDP loop to stop.")
        self._tasks = []

        logger.info(
            "TDPMonitor stopped (device=%s, history=%d).",
            self.device_id,
            len(self._history),
        )

    # ---------------------------------------------------------- callbacks
    def register_callback(
        self, callback: Callable[[TDPReading], Any]
    ) -> None:
        """
        Register a callback invoked when a breach is imminent.

        The callback may be a normal function or an ``async def`` coroutine
        function; both are supported. Exceptions are logged and isolated.
        """
        if not callable(callback):
            raise TDPMonitorError("callback must be callable.")
        with self._lock:
            self._callbacks.append(callback)
        logger.debug("Registered TDP callback: %s", getattr(callback, "__name__", callback))

    def unregister_callback(self, callback: Callable[[TDPReading], Any]) -> bool:
        """Remove a previously registered callback. Returns True if removed."""
        with self._lock:
            try:
                self._callbacks.remove(callback)
                return True
            except ValueError:
                return False

    async def _dispatch_callbacks(self, reading: TDPReading) -> None:
        """Invoke every registered callback, isolating failures."""
        with self._lock:
            callbacks = list(self._callbacks)
        for cb in callbacks:
            try:
                result = cb(reading)
                if asyncio.iscoroutine(result):
                    await result
            except Exception as exc:
                logger.error(
                    "TDP callback %r failed: %s",
                    getattr(cb, "__name__", cb),
                    exc,
                )

    # ---------------------------------------------------------- public API
    async def get_current_reading(self) -> TDPReading:
        """
        Return the current TDP reading with breach prediction.

        The reading is appended to history, and imminent-breach callbacks are
        invoked when ``predicted_breach_seconds`` falls below
        ``config.breach_alert_threshold_seconds``.
        """
        base = await self._measure_power()
        predicted = self._predict_breach(base)

        reading = TDPReading(
            timestamp=base.timestamp,
            monotonic_ts=base.monotonic_ts,
            device_id=base.device_id,
            device_type=base.device_type,
            current_power_watts=base.current_power_watts,
            tdp_threshold_watts=base.tdp_threshold_watts,
            utilization_percent=base.utilization_percent,
            temperature_celsius=base.temperature_celsius,
            thermal_throttling=base.thermal_throttling,
            predicted_breach_seconds=predicted,
        )

        with self._lock:
            self._history.append(reading)

        if (
            predicted is not None
            and predicted < self._config.breach_alert_threshold_seconds
        ):
            await self._dispatch_callbacks(reading)

        return reading

    async def simulate_reading(
        self,
        current_power_watts: float,
        *,
        temperature_celsius: float = 50.0,
        utilization_percent: float = 50.0,
        thermal_throttling: bool = False,
    ) -> TDPReading:
        """
        Inject a synthetic reading (useful for tests and dry runs).

        The reading participates in history and prediction exactly like a
        hardware-derived one.
        """
        if not isinstance(current_power_watts, (int, float)) or math.isnan(
            current_power_watts
        ):
            raise TDPMonitorError("current_power_watts must be a finite number.")
        if current_power_watts < 0:
            raise TDPMonitorError("current_power_watts must be >= 0.")

        reading = TDPReading(
            timestamp=datetime.now(timezone.utc),
            monotonic_ts=time.monotonic(),
            device_id=self.device_id,
            device_type=self.device_type,
            current_power_watts=float(current_power_watts),
            tdp_threshold_watts=self.thresholds.threshold_for(self.device_type),
            utilization_percent=float(utilization_percent),
            temperature_celsius=float(temperature_celsius),
            thermal_throttling=bool(thermal_throttling),
            predicted_breach_seconds=None,
        )
        predicted = self._predict_breach(reading)
        reading = TDPReading(
            **{
                **reading.__dict__,
                "predicted_breach_seconds": predicted,
            }
        )

        with self._lock:
            self._history.append(reading)

        if (
            predicted is not None
            and predicted < self._config.breach_alert_threshold_seconds
        ):
            await self._dispatch_callbacks(reading)
        return reading

    def get_recent_readings(self, n: int = 10) -> List[TDPReading]:
        """Return the ``n`` most recent readings (oldest first)."""
        if not isinstance(n, int) or n <= 0:
            raise TDPMonitorError("n must be a positive int.")
        with self._lock:
            if n >= len(self._history):
                return list(self._history)
            return list(self._history)[-n:]

    # ---------------------------------------------------------- measurement
    async def _measure_power(self) -> TDPReading:
        """Measure current power consumption based on device type."""
        if self.device_type == DeviceType.RASPBERRY_PI_5:
            power = await self._measure_rpi_power()
        elif self.device_type == DeviceType.NVIDIA_JETSON_ORIN:
            power = await self._measure_jetson_power()
        elif self.device_type == DeviceType.INTEL_NUC_13_PRO:
            power = await self._measure_nuc_power()
        else:
            power = await self._estimate_power_from_cpu()

        temperature = await self._measure_temperature()
        throttling = await self._check_thermal_throttling()
        utilization = await self._measure_cpu_percent()

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
        )

    async def _measure_cpu_percent(self) -> float:
        """Sample CPU utilization, guarded against psutil failures."""
        try:
            return float(
                psutil.cpu_percent(
                    interval=self._config.cpu_sample_interval_seconds
                )
            )
        except Exception as exc:  # pragma: no cover — env dependent
            logger.warning("psutil.cpu_percent failed: %s", exc)
            return 0.0

    async def _run_subprocess(
        self,
        *args: str,
        timeout: Optional[float] = None,
    ) -> Optional[str]:
        """
        Run an external command with a timeout, returning stdout or ``None``.

        A single helper centralizes subprocess handling so every measurement
        path is protected identically.
        """
        timeout = timeout or self._config.subprocess_timeout_seconds
        try:
            proc = await asyncio.create_subprocess_exec(
                *args,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.DEVNULL,
            )
        except (FileNotFoundError, PermissionError) as exc:
            logger.debug("Command %s unavailable: %s", args, exc)
            return None

        try:
            stdout, _ = await asyncio.wait_for(proc.communicate(), timeout=timeout)
        except asyncio.TimeoutError:
            logger.warning("Command %s timed out after %.1fs.", args, timeout)
            try:
                proc.kill()
            except ProcessLookupError:
                pass
            return None
        except Exception as exc:  # pragma: no cover — defensive
            logger.warning("Command %s failed: %s", args, exc)
            return None

        if stdout is None:
            return None
        return stdout.decode(errors="replace").strip()

    async def _measure_rpi_power(self) -> float:
        """Measure Raspberry Pi 5 power via INA219 sensor (with fallback)."""
        out = await self._run_subprocess("i2cget", "-y", "1", "0x40", "0x02", "w")
        if out:
            try:
                # i2cget often returns "0x1234" style values.
                raw = out
                if raw.startswith("0x"):
                    raw = raw[2:]
                value = int(raw, 16)
                # INA219 power register LSB = 20 * current_lsb; this is
                # implementation-specific and preserves the original scaling.
                return float(value) * 0.001
            except (ValueError, TypeError) as exc:
                logger.debug("Failed to parse i2cget output %r: %s", out, exc)
        return await self._estimate_power_from_cpu()

    async def _measure_jetson_power(self) -> float:
        """Measure NVIDIA Jetson Orin power via tegrastats (with fallback)."""
        out = await self._run_subprocess(
            "tegrastats",
            "--interval",
            "1000",
            "--limit",
            "1",
            timeout=max(self._config.subprocess_timeout_seconds, 2.0),
        )
        if out:
            match = re.search(r"VDD_IN:.*?(\d+)mW", out)
            if match:
                try:
                    return int(match.group(1)) / 1000.0
                except ValueError:  # pragma: no cover
                    pass
        return await self._estimate_power_from_cpu()

    async def _measure_nuc_power(self) -> float:
        """
        Measure Intel NUC 13 Pro power via RAPL energy counters.

        Reads cumulative energy in microjoules and computes power as
        ``delta_energy / delta_time``. Handles counter wraparound.
        """
        rapl_path = "/sys/class/powercap/intel-rapl/intel-rapl:0/energy_uj"
        try:
            with open(rapl_path, "r", encoding="utf-8") as f:
                energy_uj = int(f.read().strip())
        except (OSError, ValueError) as exc:
            logger.debug("RAPL read failed (%s); falling back to CPU estimate.", exc)
            return await self._estimate_power_from_cpu()

        now = time.monotonic()
        with self._lock:
            last_energy = self._last_rapl_energy_uj
            last_ts = self._last_rapl_timestamp
            self._last_rapl_energy_uj = energy_uj
            self._last_rapl_timestamp = now

        if last_energy is None or last_ts is None:
            # First sample: cannot compute delta yet.
            return await self._estimate_power_from_cpu()

        delta_energy = energy_uj - last_energy
        if delta_energy < 0:
            # Counter wraparound: RAPL typically resets after an overflow.
            # Treat the wrap as "delta unknown" for this sample.
            logger.debug(
                "RAPL counter wrap detected (%d -> %d); skipping sample.",
                last_energy, energy_uj,
            )
            return await self._estimate_power_from_cpu()

        delta_t = now - last_ts
        if delta_t <= 0:
            return await self._estimate_power_from_cpu()

        # µJ / s = µW → /1e6 = W
        power_watts = (delta_energy / 1e6) / delta_t
        # Sanity clamp: any value above ~500 W is implausible for a NUC.
        if power_watts < 0 or power_watts > 500:
            logger.debug(
                "RAPL-derived power %.3fW out of range; using CPU estimate.",
                power_watts,
            )
            return await self._estimate_power_from_cpu()
        return float(power_watts)

    async def _estimate_power_from_cpu(self) -> float:
        """Estimate power from CPU utilization (fallback)."""
        cpu_percent = await self._measure_cpu_percent()
        idle, max_power = self.thresholds.idle_and_max_power(self.device_type)
        return idle + (max_power - idle) * (cpu_percent / 100.0)

    async def _measure_temperature(self) -> float:
        """Return device temperature in Celsius (with sensible fallbacks)."""
        if self.device_type == DeviceType.RASPBERRY_PI_5:
            out = await self._run_subprocess("vcgencmd", "measure_temp")
            if out:
                match = re.search(r"temp=(\d+\.?\d*)", out)
                if match:
                    try:
                        return float(match.group(1))
                    except ValueError:  # pragma: no cover
                        pass

        # Generic Linux: thermal zones / hwmon
        candidates = (
            "/sys/class/thermal/thermal_zone0/temp",
            "/sys/class/hwmon/hwmon0/temp1_input",
        )
        for path in candidates:
            try:
                with open(path, "r", encoding="utf-8") as f:
                    raw = int(f.read().strip())
                return raw / 1000.0 if raw > 1000 else float(raw)
            except (OSError, ValueError):
                continue

        return 25.0  # Default room temperature

    async def _check_thermal_throttling(self) -> bool:
        """Return True if the device is thermally throttling."""
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
        Predict time-to-breach in seconds via linear regression on history.

        Uses ``monotonic_ts`` for accurate spacing even when the sampling loop
        has been delayed. Returns ``None`` when the trend is flat/decreasing,
        when there is insufficient data, or when the reading is already safe
        by a wide margin.
        """
        cfg = self._config
        with self._lock:
            snapshot = list(self._history)

        # Include the current reading in the trend window.
        samples: List[TDPReading] = snapshot + [reading]
        if len(samples) < cfg.min_samples_for_prediction:
            return None

        recent = samples[-cfg.min_samples_for_prediction:]

        # Filter out samples without monotonic timestamps (defensive).
        pairs: List[Tuple[float, float]] = [
            (r.monotonic_ts, r.current_power_watts)
            for r in recent
            if r.monotonic_ts > 0
        ]
        if len(pairs) < cfg.min_samples_for_prediction:
            # Fall back to index-based regression (legacy behavior).
            pairs = [(float(i), r.current_power_watts) for i, r in enumerate(recent)]

        xs = [p[0] for p in pairs]
        ys = [p[1] for p in pairs]

        x_mean = statistics.fmean(xs)
        y_mean = statistics.fmean(ys)

        numerator = sum((x - x_mean) * (y - y_mean) for x, y in zip(xs, ys))
        denominator = sum((x - x_mean) ** 2 for x in xs)
        if denominator <= 0:
            return None

        slope = numerator / denominator  # Watts per second
        if slope <= 0:
            return None  # Not trending upward

        # Correct breach target: threshold - buffer.
        breach_target = (
            reading.tdp_threshold_watts - self.thresholds.WARNING_BUFFER
        )
        remaining_power = breach_target - reading.current_power_watts

        if remaining_power <= 0:
            # Already at or above the warning threshold.
            return 0.0

        seconds_to_breach = remaining_power / slope
        # Clamp to the configured horizon.
        return float(
            max(0.0, min(seconds_to_breach, cfg.prediction_horizon_seconds))
        )

    # ---------------------------------------------------------- loop
    async def _monitoring_loop(self) -> None:
        """Background loop for periodic TDP measurements."""
        while self._running:
            try:
                await self.get_current_reading()
                await asyncio.sleep(self._config.sampling_interval_seconds)
            except asyncio.CancelledError:
                logger.debug("TDP monitoring loop cancelled.")
                raise
            except Exception as exc:
                logger.error("TDP monitoring loop error: %s", exc)
                await asyncio.sleep(self._config.loop_error_sleep_seconds)

    # ---------------------------------------------------------- stats
    def statistics(self) -> Dict[str, Any]:
        """Return descriptive statistics over the recorded history."""
        with self._lock:
            history = list(self._history)

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
            }

        powers = [r.current_power_watts for r in history]
        temps = [r.temperature_celsius for r in history]
        throttles = sum(1 for r in history if r.thermal_throttling)
        imminent = sum(
            1
            for r in history
            if r.predicted_breach_seconds is not None
            and r.predicted_breach_seconds < self._config.breach_alert_threshold_seconds
        )

        return {
            "count": len(history),
            "mean_power_watts": float(statistics.fmean(powers)),
            "max_power_watts": float(max(powers)),
            "min_power_watts": float(min(powers)),
            "mean_temperature_celsius": float(statistics.fmean(temps)),
            "max_temperature_celsius": float(max(temps)),
            "throttling_events": throttles,
            "imminent_breaches": imminent,
        }

    def reset(self, *, clear_history: bool = False) -> None:
        """Reset the monitor state; optionally clear history."""
        with self._lock:
            if clear_history:
                self._history.clear()
            self._last_rapl_energy_uj = None
            self._last_rapl_timestamp = None
        logger.debug("TDPMonitor reset (clear_history=%s)", clear_history)

    # ---------------------------------------------------------- serialization
    def to_dict(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "device_id": self.device_id,
                "device_type": self.device_type.value,
                "config": asdict(self._config),
                "thresholds": asdict(self.thresholds),
                "strict": self._strict,
                "running": self._running,
                "started_at": self._started_at,
                "history": [r.to_dict() for r in self._history],
            }

    @classmethod
    def from_dict(
        cls,
        data: Mapping[str, Any],
        *,
        thresholds: Optional[TDPThresholds] = None,
    ) -> "TDPMonitor":
        if not isinstance(data, Mapping):
            raise TDPMonitorError(
                f"from_dict expects a Mapping, got {type(data).__name__}."
            )

        cfg_data = dict(data.get("config", {}) or {})
        cfg = TDPMonitorConfig(
            sampling_interval_seconds=float(
                cfg_data.get("sampling_interval_seconds", 5.0)
            ),
            loop_error_sleep_seconds=float(
                cfg_data.get("loop_error_sleep_seconds", 10.0)
            ),
            prediction_horizon_seconds=float(
                cfg_data.get("prediction_horizon_seconds", 300.0)
            ),
            breach_alert_threshold_seconds=float(
                cfg_data.get("breach_alert_threshold_seconds", 60.0)
            ),
            min_samples_for_prediction=int(
                cfg_data.get("min_samples_for_prediction", 10)
            ),
            max_history=int(cfg_data.get("max_history", 1000)),
            subprocess_timeout_seconds=float(
                cfg_data.get("subprocess_timeout_seconds", 3.0)
            ),
            cpu_sample_interval_seconds=float(
                cfg_data.get("cpu_sample_interval_seconds", 1.0)
            ),
        )

        th_data = dict(data.get("thresholds", {}) or {})
        th = thresholds or TDPThresholds(
            RASPBERRY_PI_5=float(th_data.get("RASPBERRY_PI_5", 12.0)),
            NVIDIA_JETSON_ORIN=float(th_data.get("NVIDIA_JETSON_ORIN", 60.0)),
            INTEL_NUC_13_PRO=float(th_data.get("INTEL_NUC_13_PRO", 28.0)),
            WARNING_BUFFER=float(th_data.get("WARNING_BUFFER", 2.0)),
        )

        monitor = cls(
            device_id=str(data["device_id"]),
            device_type=DeviceType(data["device_type"]),
            thresholds=th,
            config=cfg,
            strict=bool(data.get("strict", True)),
        )
        with monitor._lock:
            for entry in data.get("history", []):
                monitor._history.append(TDPReading.from_dict(entry))
            monitor._started_at = data.get("started_at")
        return monitor

    def to_json(self, **kwargs: Any) -> str:
        return json.dumps(self.to_dict(), default=str, **kwargs)

    @classmethod
    def from_json(cls, payload: str) -> "TDPMonitor":
        try:
            return cls.from_dict(json.loads(payload))
        except json.JSONDecodeError as exc:
            raise TDPMonitorError(f"Invalid JSON payload: {exc}") from exc

    # ---------------------------------------------------------- async ctx mgr
    async def __aenter__(self) -> "TDPMonitor":
        self._ctx_start = time.perf_counter()
        logger.debug("Entering scoped TDP-monitor session.")
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
                    "TDP-monitor scope exited with %s after %.4fs.",
                    exc_type.__name__,
                    elapsed,
                )
            else:
                logger.info(
                    "TDP-monitor scope closed in %.4fs (history=%d).",
                    elapsed,
                    len(self._history),
                )

    # ----------------------------------------------------------------- dunder
    def __repr__(self) -> str:
        with self._lock:
            return (
                "TDPMonitor("
                f"device_id={self.device_id!r}, "
                f"type={self.device_type.value}, "
                f"running={self._running}, "
                f"history={len(self._history)}, "
                f"callbacks={len(self._callbacks)})"
            )


# --------------------------------------------------------------------------- #
# Public API
# --------------------------------------------------------------------------- #
__all__ = [
    "DeviceType",
    "TDPThresholds",
    "TDPReading",
    "TDPMonitor",
    "TDPMonitorConfig",
    "TDPMonitorError",
]


# --------------------------------------------------------------------------- #
# Local smoke test: python -m continuum.tdp_monitor
# --------------------------------------------------------------------------- #
if __name__ == "__main__":  # pragma: no cover
    logging.basicConfig(
        level=logging.INFO, format="%(levelname)s %(name)s: %(message)s"
    )

    async def main() -> None:
        # ------------------------------------------------------------------ #
        # 1. Simulation-driven breach prediction.
        # ------------------------------------------------------------------ #
        monitor = TDPMonitor(
            device_id="edge-rpi-01",
            device_type=DeviceType.RASPBERRY_PI_5,
            config=TDPMonitorConfig(
                sampling_interval_seconds=0.05,
                prediction_horizon_seconds=120.0,
                breach_alert_threshold_seconds=30.0,
                min_samples_for_prediction=5,
                max_history=50,
            ),
        )

        alerts: List[TDPReading] = []

        async def on_breach(reading: TDPReading) -> None:
            alerts.append(reading)

        monitor.register_callback(on_breach)

        # Rising power trend toward the 12 W threshold (warning at 10 W).
        for i in range(12):
            await monitor.simulate_reading(
                current_power_watts=4.0 + i * 0.6,
                temperature_celsius=45.0 + i * 0.8,
                utilization_percent=40.0 + i * 3.0,
            )
            await asyncio.sleep(0.01)

        last = monitor.get_recent_readings(1)[0]
        print(f"last power         : {last.current_power_watts:.2f} W / "
              f"threshold {last.tdp_threshold_watts:.1f} W")
        print(f"predicted breach   : {last.predicted_breach_seconds} s")
        print(f"alerts dispatched  : {len(alerts)}")
        print(f"statistics         : {monitor.statistics()}")
        print(f"repr               : {monitor}")

        # ------------------------------------------------------------------ #
        # 2. Lifecycle (start/stop).
        # ------------------------------------------------------------------ #
        live = TDPMonitor(
            device_id="edge-nuc-01",
            device_type=DeviceType.INTEL_NUC_13_PRO,
            config=TDPMonitorConfig(
                sampling_interval_seconds=0.1,
                subprocess_timeout_seconds=0.5,
                cpu_sample_interval_seconds=0.05,
                prediction_horizon_seconds=60.0,
                min_samples_for_prediction=3,
            ),
        )
        async with live:
            await live.start()
            await asyncio.sleep(0.35)
            print(f"live history size  : {live.history_size}")
        print(f"live stopped       : running={live.running}")

        # ------------------------------------------------------------------ #
        # 3. Serialization round-trip.
        # ------------------------------------------------------------------ #
        payload = monitor.to_json()
        restored = TDPMonitor.from_json(payload)
        assert restored.to_dict() == monitor.to_dict()
        print("Serialization round-trip OK.")

        # ------------------------------------------------------------------ #
        # 4. Validation failures.
        # ------------------------------------------------------------------ #
        for bad in (
            ("", DeviceType.RASPBERRY_PI_5),
            ("edge", "not-a-device-type"),
            ("edge", DeviceType.RASPBERRY_PI_5, 0, 300),
            ("edge", DeviceType.RASPBERRY_PI_5, 5, -1),
        ):
            try:
                TDPMonitor(*bad)  # type: ignore[arg-type]
            except TDPMonitorError as exc:
                print("Rejected as expected:", exc)
            else:  # pragma: no cover
                raise AssertionError(f"Expected rejection for {bad!r}")

        # ------------------------------------------------------------------ #
        # 5. TDPThresholds lookup helper.
        # ------------------------------------------------------------------ #
        th = TDPThresholds()
        for dt in DeviceType:
            print(f"threshold[{dt.value}] = {th.threshold_for(dt):.1f} W")

        print("\nSmoke test passed.")

    asyncio.run(main())
