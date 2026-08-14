"""
metric_aggregator.py

Wraps the FlexGen executor to capture accurate energy, latency, and throughput.
"""
import time
from typing import Dict, Any, Callable

from .gpu_profiler import GPUProfiler


class MetricAggregator:
    def __init__(self, gpu_profiler: GPUProfiler, executor_fn: Callable):
        self.profiler = gpu_profiler
        self.executor = executor_fn

    def run(self, task: Dict[str, Any], policy: Dict[str, Any]) -> Dict[str, Any]:
        """Execute inference and capture real metrics."""
        
        # Snapshot before execution
        start_metrics = self.profiler.get_current_metrics()
        start_time = time.time()
        start_gpu_energy = start_metrics.get("gpu_power_watts", 0.0) * start_time  # rough cumulative

        # ---- Execute the actual inference (Phase 3) ----
        try:
            output, raw_inference_metrics = self.executor(task, policy)
            success = True
        except Exception as e:
            output = None
            raw_inference_metrics = {"error": str(e)}
            success = False

        # Snapshot after execution
        end_metrics = self.profiler.get_current_metrics()
        end_time = time.time()

        # ---- Compute Derived Metrics ----
        elapsed_sec = end_time - start_time
        avg_power = (start_metrics.get("gpu_power_watts", 0.0) + end_metrics.get("gpu_power_watts", 0.0)) / 2.0
        total_energy_kwh = (avg_power * elapsed_sec) / 3600.0 / 1000.0  # Watts * sec -> kWh

        tokens_generated = raw_inference_metrics.get("tokens_generated", 0)
        tokens_per_sec = tokens_generated / elapsed_sec if elapsed_sec > 0 else 0.0

        # Memory efficiency
        gpu_total = end_metrics.get("gpu_memory_total_mb", 1.0)
        gpu_used = end_metrics.get("gpu_memory_used_mb", 0.0)
        memory_efficiency = gpu_used / gpu_total if gpu_total > 0 else 0.0

        return {
            "success": success,
            "output": output,
            "inference_metrics": raw_inference_metrics,
            "elapsed_sec": elapsed_sec,
            "tokens_per_sec": tokens_per_sec,
            "total_energy_kwh": total_energy_kwh,
            "gpu_power_avg_watts": avg_power,
            "gpu_memory_peak_mb": max(start_metrics.get("gpu_memory_used_mb", 0),
                                      end_metrics.get("gpu_memory_used_mb", 0)),
            "memory_efficiency": memory_efficiency,
            "gpu_oom": not success and "CUDA out of memory" in str(raw_inference_metrics.get("error", "")),
            "real_metrics": end_metrics,  # full snapshot
        }
