"""
gpu_profiler.py

Real-time hardware metrics collector using pynvml and psutil.
Fully integrated with Phase 1 requirements.
"""
import time
import psutil
import threading
from typing import Dict, Any, Optional

# NVML is optional; fail gracefully if not installed
try:
    import pynvml
    NVML_AVAILABLE = True
    pynvml.nvmlInit()
except (ImportError, pynvml.NVMLError):
    NVML_AVAILABLE = False


class GPUProfiler:
    """Collects GPU, CPU, and Disk metrics with minimal overhead."""

    def __init__(self, sample_interval_sec: float = 0.5):
        self.sample_interval = sample_interval_sec
        self._running = False
        self._thread = None
        self._latest_metrics = {}
        self._disk_io_start = psutil.disk_io_counters()
        self._last_disk_time = time.time()

    def start(self):
        """Start background sampling."""
        if self._running:
            return
        self._running = True
        self._thread = threading.Thread(target=self._sample_loop, daemon=True)
        self._thread.start()

    def stop(self):
        self._running = False
        if self._thread:
            self._thread.join(timeout=2.0)

    def _sample_loop(self):
        while self._running:
            self._latest_metrics = self._snapshot()
            time.sleep(self.sample_interval)

    def _snapshot(self) -> Dict[str, Any]:
        """Take a one-shot snapshot (used if background is off)."""
        metrics = {}

        # ---- GPU Metrics (NVML) ----
        if NVML_AVAILABLE:
            try:
                device_count = pynvml.nvmlDeviceGetCount()
                if device_count > 0:
                    handle = pynvml.nvmlDeviceGetHandleByIndex(0)
                    mem_info = pynvml.nvmlDeviceGetMemoryInfo(handle)
                    util = pynvml.nvmlDeviceGetUtilizationRates(handle)
                    power = pynvml.nvmlDeviceGetPowerUsage(handle) / 1000.0  # mW -> W

                    metrics["gpu_name"] = pynvml.nvmlDeviceGetName(handle)
                    metrics["gpu_memory_total_mb"] = mem_info.total / 1024**2
                    metrics["gpu_memory_free_mb"] = mem_info.free / 1024**2
                    metrics["gpu_memory_used_mb"] = mem_info.used / 1024**2
                    metrics["gpu_utilization_pct"] = util.gpu / 100.0
                    metrics["gpu_power_watts"] = power
                    metrics["gpu_temp_c"] = pynvml.nvmlDeviceGetTemperature(handle, pynvml.NVML_TEMPERATURE_GPU)
            except Exception:
                # Fallback if NVML fails mid-flight
                pass
        else:
            metrics["gpu_available"] = False

        # ---- CPU & System Memory (PSUTIL) ----
        vm = psutil.virtual_memory()
        metrics["cpu_memory_total_mb"] = vm.total / 1024**2
        metrics["cpu_memory_free_mb"] = vm.available / 1024**2
        metrics["cpu_utilization_pct"] = psutil.cpu_percent(interval=None) / 100.0

        # ---- Disk I/O Bandwidth ----
        now = time.time()
        disk_io = psutil.disk_io_counters()
        if self._disk_io_start and now - self._last_disk_time > 0.5:
            delta_time = now - self._last_disk_time
            read_bytes = disk_io.read_bytes - self._disk_io_start.read_bytes
            write_bytes = disk_io.write_bytes - self._disk_io_start.write_bytes
            metrics["disk_read_bandwidth_gbps"] = (read_bytes / delta_time) * 8 / 1e9
            metrics["disk_write_bandwidth_gbps"] = (write_bytes / delta_time) * 8 / 1e9
        self._disk_io_start = disk_io
        self._last_disk_time = now

        return metrics

    def get_current_metrics(self) -> Dict[str, Any]:
        """Return latest sampled metrics or take a fresh snapshot."""
        if self._running:
            return self._latest_metrics.copy()
        return self._snapshot()
