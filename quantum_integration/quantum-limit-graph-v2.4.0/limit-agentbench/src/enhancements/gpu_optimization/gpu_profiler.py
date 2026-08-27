"""
Real GPU profiler using NVML (pynvml). Enhanced with:
- Multi-GPU support (list all GPUs or specific index)
- Continuous monitoring with async loop
- Integration with FeedbackEvent and AsyncMessageQueue
- Energy and carbon estimation
- History tracking for drift detection
- Fallback to dummy data with explicit flag
- Integration with NodeDescriptor
"""

import logging
import asyncio
import time
from typing import Dict, List, Optional, Any, Deque
from collections import deque

try:
    import pynvml
    NVML_AVAILABLE = True
except ImportError:
    NVML_AVAILABLE = False

try:
    from ..async_message_queue import AsyncMessageQueue
except ImportError:
    AsyncMessageQueue = None

try:
    from ..schemas.feedback_event import FeedbackEvent
except ImportError:
    FeedbackEvent = None

try:
    from ..schemas.node_descriptor import NodeDescriptor
except ImportError:
    NodeDescriptor = None

logger = logging.getLogger(__name__)


class GPUProfiler:
    """
    GPU profiler that provides real-time metrics for NVIDIA GPUs.
    Falls back to dummy data if NVML not available.
    """

    def __init__(
        self,
        device_index: Optional[int] = None,
        carbon_intensity_g_per_kwh: float = 400.0,
        message_queue: Optional[AsyncMessageQueue] = None,
        history_size: int = 100,
    ):
        """
        Args:
            device_index: Specific GPU index to monitor. If None, monitor all.
            carbon_intensity_g_per_kwh: Carbon intensity for energy-to-carbon conversion.
            message_queue: Optional queue for publishing metrics.
            history_size: Number of recent metric snapshots to keep.
        """
        self.device_index = device_index
        self.carbon_intensity = carbon_intensity_g_per_kwh
        self.message_queue = message_queue
        self.history: Deque[Dict[str, Any]] = deque(maxlen=history_size)
        self.nvml_initialized = False
        self.handles: List[Any] = []
        self._lock = asyncio.Lock()
        self._monitor_task: Optional[asyncio.Task] = None
        self._last_sample_time: Optional[float] = None
        self._last_power_watts: Optional[float] = None

        if NVML_AVAILABLE:
            try:
                pynvml.nvmlInit()
                self.nvml_initialized = True
                device_count = pynvml.nvmlDeviceGetCount()
                if self.device_index is not None:
                    if self.device_index < device_count:
                        self.handles = [pynvml.nvmlDeviceGetHandleByIndex(self.device_index)]
                    else:
                        logger.error(f"GPU index {self.device_index} out of range (count={device_count})")
                else:
                    self.handles = [pynvml.nvmlDeviceGetHandleByIndex(i) for i in range(device_count)]
                logger.info(f"NVML initialized, monitoring {len(self.handles)} GPU(s)")
            except Exception as e:
                logger.warning(f"NVML init failed: {e}")

    async def _safe_get_metrics(self) -> List[Dict[str, Any]]:
        """Get metrics for all monitored GPUs. Returns list of dicts."""
        if not self.nvml_initialized or not self.handles:
            # Return dummy data for development
            return self._get_dummy_metrics()

        metrics_list = []
        try:
            for idx, handle in enumerate(self.handles):
                mem_info = pynvml.nvmlDeviceGetMemoryInfo(handle)
                util = pynvml.nvmlDeviceGetUtilizationRates(handle)
                power_mw = pynvml.nvmlDeviceGetPowerUsage(handle)
                temp = pynvml.nvmlDeviceGetTemperature(handle, pynvml.NVML_TEMPERATURE_GPU)
                name = pynvml.nvmlDeviceGetName(handle)
                # Additional metrics (may not be available on all GPUs)
                max_power_w = 0
                mem_clock_mhz = 0
                sm_clock_mhz = 0
                try:
                    max_power_w = pynvml.nvmlDeviceGetEnforcedPowerLimit(handle) / 1000.0
                except:
                    pass
                try:
                    mem_clock_mhz = pynvml.nvmlDeviceGetClockInfo(handle, pynvml.NVML_CLOCK_MEM)
                    sm_clock_mhz = pynvml.nvmlDeviceGetClockInfo(handle, pynvml.NVML_CLOCK_SM)
                except:
                    pass

                metrics = {
                    "gpu_name": name,
                    "gpu_index": idx if self.device_index is None else self.device_index,
                    "gpu_memory_total_mb": mem_info.total // (1024 * 1024),
                    "gpu_memory_free_mb": mem_info.free // (1024 * 1024),
                    "gpu_memory_used_mb": mem_info.used // (1024 * 1024),
                    "gpu_utilization_pct": util.gpu,
                    "gpu_power_watts": power_mw / 1000.0,
                    "gpu_temperature_c": temp,
                    "gpu_max_power_watts": max_power_w,
                    "gpu_memory_clock_mhz": mem_clock_mhz,
                    "gpu_sm_clock_mhz": sm_clock_mhz,
                    "cuda_transfer_bandwidth_gbps": 12.0,  # placeholder
                    "is_dummy": False,
                }
                metrics_list.append(metrics)
        except Exception as e:
            logger.error(f"GPU metric collection failed: {e}")
            metrics_list = self._get_dummy_metrics()
        return metrics_list

    def _get_dummy_metrics(self) -> List[Dict[str, Any]]:
        """Return dummy data for development."""
        return [{
            "gpu_name": "dummy_gpu",
            "gpu_index": 0,
            "gpu_memory_total_mb": 16384,
            "gpu_memory_free_mb": 12000,
            "gpu_memory_used_mb": 4384,
            "gpu_utilization_pct": 45.0,
            "gpu_power_watts": 65.0,
            "gpu_temperature_c": 55.0,
            "gpu_max_power_watts": 250.0,
            "gpu_memory_clock_mhz": 6000,
            "gpu_sm_clock_mhz": 1500,
            "cuda_transfer_bandwidth_gbps": 12.0,
            "is_dummy": True,
        }]

    def get_gpu_metrics(self, device_index: Optional[int] = None) -> Dict[str, Any]:
        """
        Synchronous wrapper for getting metrics of a specific GPU (or first if None).
        """
        if device_index is not None and self.device_index is None:
            # Temporary override
            old_handles = self.handles
            self.handles = [pynvml.nvmlDeviceGetHandleByIndex(device_index)]
            result = asyncio.run(self._safe_get_metrics())
            self.handles = old_handles
            return result[0] if result else {}
        result = asyncio.run(self._safe_get_metrics())
        return result[0] if result else {}

    async def get_all_gpu_metrics(self) -> List[Dict[str, Any]]:
        """Asynchronously get metrics for all monitored GPUs."""
        async with self._lock:
            return await self._safe_get_metrics()

    async def update_node_descriptor(self, node: NodeDescriptor) -> NodeDescriptor:
        """Populate node metadata with GPU information from profiler."""
        metrics = await self.get_all_gpu_metrics()
        if metrics:
            gpu = metrics[0]  # use first GPU
            node.metadata["gpu_name"] = gpu["gpu_name"]
            node.metadata["gpu_memory_gb"] = gpu["gpu_memory_total_mb"] / 1024.0
            node.metadata["gpu_max_power_w"] = gpu.get("gpu_max_power_watts", 250)
            node.metadata["gpu_cpu_bandwidth_gbps"] = gpu.get("cuda_transfer_bandwidth_gbps", 12.0)
        return node

    async def start_monitoring(self, interval_sec: float = 5.0):
        """Start continuous monitoring loop, publishing metrics to message queue."""
        if self._monitor_task is not None:
            logger.warning("Monitoring already active")
            return
        self._monitor_task = asyncio.create_task(self._monitor_loop(interval_sec))
        logger.info(f"GPU monitoring started (interval={interval_sec}s)")

    async def stop_monitoring(self):
        """Stop the monitoring loop."""
        if self._monitor_task:
            self._monitor_task.cancel()
            try:
                await self._monitor_task
            except asyncio.CancelledError:
                pass
            self._monitor_task = None
            logger.info("GPU monitoring stopped")

    async def _monitor_loop(self, interval_sec: float):
        """Internal loop for periodic metrics collection and publication."""
        while True:
            try:
                metrics_list = await self.get_all_gpu_metrics()
                for metrics in metrics_list:
                    # Compute energy and carbon since last sample
                    energy_j = 0.0
                    carbon_g = 0.0
                    current_time = time.time()
                    if self._last_sample_time is not None and self._last_power_watts is not None:
                        dt = current_time - self._last_sample_time
                        avg_power = (self._last_power_watts + metrics["gpu_power_watts"]) / 2
                        energy_j = avg_power * dt
                        energy_kwh = energy_j / 3.6e6
                        carbon_g = energy_kwh * self.carbon_intensity
                    self._last_sample_time = current_time
                    self._last_power_watts = metrics["gpu_power_watts"]

                    # Add energy/carbon to metrics
                    metrics["energy_joules"] = energy_j
                    metrics["carbon_g"] = carbon_g
                    metrics["timestamp"] = current_time

                    # Store in history
                    self.history.append(metrics.copy())

                    # Publish FeedbackEvent if queue available
                    if self.message_queue and FeedbackEvent:
                        event = FeedbackEvent(
                            source="gpu_profiler",
                            feedback_type="telemetry",
                            task_id="gpu_monitor",
                            context={"gpu_index": metrics.get("gpu_index", 0),
                                     "carbon_intensity": self.carbon_intensity},
                            action={"selected_action": "monitor",
                                    "selected_rank": 0,
                                    "confidence_score": 1.0},
                            performance={"quality_score": 1.0,
                                         "latency_ms": 0,
                                         "energy_joules": energy_j,
                                         "carbon_g": carbon_g,
                                         "helium_cost": 0,
                                         "duration_ms": 0},
                            adaptive_cost_value=0.0,
                            tags=["gpu", "monitoring", "energy", "carbon"],
                        )
                        await self.message_queue.publish("gpu_metrics", event.to_json())
            except Exception as e:
                logger.error(f"Monitoring loop error: {e}")
            await asyncio.sleep(interval_sec)

    def get_history(self, limit: Optional[int] = None) -> List[Dict[str, Any]]:
        """Return recent metric history."""
        if limit:
            return list(self.history)[-limit:]
        return list(self.history)

    def shutdown(self):
        """Clean up resources."""
        if self._monitor_task:
            self._monitor_task.cancel()
        if self.nvml_initialized:
            try:
                pynvml.nvmlShutdown()
            except:
                pass
