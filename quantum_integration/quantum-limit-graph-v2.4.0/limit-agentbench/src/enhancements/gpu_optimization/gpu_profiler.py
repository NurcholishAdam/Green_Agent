"""
Real GPU profiler using NVML (pynvml). Provides GPU memory, utilization,
power, and temperature. Falls back to dummy data if NVML is unavailable.
"""

import logging
from typing import Dict, Optional, Any

logger = logging.getLogger(__name__)

try:
    import pynvml
    NVML_AVAILABLE = True
except ImportError:
    NVML_AVAILABLE = False


class GPUProfiler:
    def __init__(self):
        self.handle = None
        self.nvml_initialized = False
        if NVML_AVAILABLE:
            try:
                pynvml.nvmlInit()
                self.nvml_initialized = True
                # Get first available GPU
                device_count = pynvml.nvmlDeviceGetCount()
                if device_count > 0:
                    self.handle = pynvml.nvmlDeviceGetHandleByIndex(0)
            except Exception as e:
                logger.warning(f"NVML init failed: {e}")

    def get_gpu_metrics(self) -> Dict[str, Any]:
        """Return current GPU metrics or dummy if unavailable."""
        if not self.nvml_initialized or self.handle is None:
            # Return dummy data for development
            return {
                "gpu_name": "dummy_gpu",
                "gpu_memory_total_mb": 16384,
                "gpu_memory_free_mb": 12000,
                "gpu_memory_used_mb": 4384,
                "gpu_utilization_pct": 45.0,
                "gpu_power_watts": 65.0,
                "gpu_temperature_c": 55.0,
                "cuda_transfer_bandwidth_gbps": 12.0,
            }
        try:
            mem_info = pynvml.nvmlDeviceGetMemoryInfo(self.handle)
            util = pynvml.nvmlDeviceGetUtilizationRates(self.handle)
            power = pynvml.nvmlDeviceGetPowerUsage(self.handle)
            temp = pynvml.nvmlDeviceGetTemperature(self.handle, pynvml.NVML_TEMPERATURE_GPU)
            name = pynvml.nvmlDeviceGetName(self.handle)
            # Note: bandwidth not directly available, use PCIe gen approximation
            return {
                "gpu_name": name,
                "gpu_memory_total_mb": mem_info.total // (1024 * 1024),
                "gpu_memory_free_mb": mem_info.free // (1024 * 1024),
                "gpu_memory_used_mb": mem_info.used // (1024 * 1024),
                "gpu_utilization_pct": util.gpu,
                "gpu_power_watts": power / 1000.0,  # mW -> W
                "gpu_temperature_c": temp,
                "cuda_transfer_bandwidth_gbps": 12.0,  # placeholder
            }
        except Exception as e:
            logger.error(f"GPU metric collection failed: {e}")
            return {
                "gpu_name": "error",
                "gpu_memory_total_mb": 0,
                "gpu_memory_free_mb": 0,
                "gpu_memory_used_mb": 0,
                "gpu_utilization_pct": 0.0,
                "gpu_power_watts": 0.0,
                "gpu_temperature_c": 0.0,
                "cuda_transfer_bandwidth_gbps": 0.0,
            }

    def shutdown(self):
        if self.nvml_initialized:
            try:
                pynvml.nvmlShutdown()
            except:
                pass
