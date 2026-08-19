"""
metric_aggregator.py

Enhanced wrapper for the FlexGen executor that captures accurate energy, latency,
and throughput metrics, and integrates with MODP, bio_inspired, and moe_system.

Features:
- MODP utility computation (multi‑objective reward).
- Bio‑inspired fitness evaluation.
- MoE context vector generation.
- Streaming callbacks for real‑time notification.
- Enhanced derived metrics using the profiler's built‑in methods.
- Integration with the profiler's history and cumulative energy.
"""

import time
from typing import Dict, Any, Callable, List, Optional

from .gpu_profiler import GPUProfiler

# Optional imports with fallback stubs
try:
    from .MODP import ParetoOptimizer
except ImportError:
    class ParetoOptimizer:
        def evaluate(self, objectives, weights):
            return sum(objectives.get(k, 0) * weights.get(k, 1) for k in objectives)

try:
    from .bio_inspired import FitnessEvaluator
except ImportError:
    class FitnessEvaluator:
        def evaluate(self, metrics, policy):
            return 0.0

try:
    from .moe_system import ContextEncoder
except ImportError:
    class ContextEncoder:
        def encode(self, metrics):
            return [metrics.get("gpu_utilization_pct", 0),
                    metrics.get("cpu_utilization_pct", 0),
                    metrics.get("gpu_memory_used_mb", 0) / 1000]


class MetricAggregator:
    """
    Enhanced metric aggregator with MODP, bio, and MoE integration.
    """

    def __init__(
        self,
        gpu_profiler: GPUProfiler,
        executor_fn: Callable,
        modp_weights: Optional[Dict[str, float]] = None,
        bio_evaluator: Optional[Any] = None,
        moe_encoder: Optional[Any] = None,
        enable_callbacks: bool = True,
    ):
        """
        Args:
            gpu_profiler: Enhanced GPUProfiler instance.
            executor_fn: The underlying FlexGen executor.
            modp_weights: Weights for MODP objectives (quality, throughput, energy, carbon, memory).
            bio_evaluator: Fitness evaluator for bio‑inspired module.
            moe_encoder: Context encoder for MoE module.
            enable_callbacks: Whether to trigger callbacks after each run.
        """
        self.profiler = gpu_profiler
        self.executor = executor_fn

        # MODP
        self.modp = ParetoOptimizer()
        self.modp_weights = modp_weights or {
            "quality": 0.30,
            "throughput": 0.25,
            "energy_efficiency": 0.20,
            "carbon_efficiency": 0.15,
            "memory_efficiency": 0.10,
        }

        # Bio
        self.bio = bio_evaluator if bio_evaluator else FitnessEvaluator()

        # MoE
        self.moe = moe_encoder if moe_encoder else ContextEncoder()

        # Callbacks
        self._callbacks = []  # list of (callback_fn, cooldown, last_call)
        self.enable_callbacks = enable_callbacks

    # --------------------- Callback System ---------------------
    def register_callback(
        self, callback: Callable[[Dict[str, Any]], None], cooldown: float = 0.1
    ):
        """Register a function to be called after each run with aggregated metrics."""
        self._callbacks.append((callback, cooldown, 0.0))

    def _trigger_callbacks(self, metrics: Dict[str, Any]):
        """Call registered callbacks, respecting cooldowns."""
        now = time.time()
        for i, (cb, cd, last) in enumerate(self._callbacks):
            if now - last >= cd:
                try:
                    cb(metrics)
                except Exception as e:
                    # Log error but do not break
                    print(f"Callback error: {e}")
                self._callbacks[i] = (cb, cd, now)

    # --------------------- Core Run Method ---------------------
    def run(self, task: Dict[str, Any], policy: Dict[str, Any]) -> Dict[str, Any]:
        """
        Execute inference and capture real metrics.
        Returns aggregated metrics dictionary.
        """
        # ---- Pre‑execution snapshot ----
        start_metrics = self.profiler.get_current_metrics()
        start_time = time.time()

        # ---- Execute inference ----
        try:
            output, raw_inference_metrics = self.executor(task, policy)
            success = True
        except Exception as e:
            output = None
            raw_inference_metrics = {"error": str(e)}
            success = False

        # ---- Post‑execution snapshot ----
        end_metrics = self.profiler.get_current_metrics()
        end_time = time.time()

        # ---- Compute derived metrics ----
        elapsed_sec = end_time - start_time
        tokens_generated = raw_inference_metrics.get("tokens_generated", 0)
        tokens_per_sec = tokens_generated / elapsed_sec if elapsed_sec > 0 else 0.0

        # Use profiler's cumulative energy (more accurate)
        gpu_energy_joules = end_metrics.get("energy_gpu_joules", 0.0) - start_metrics.get("energy_gpu_joules", 0.0)
        cpu_energy_joules = end_metrics.get("energy_cpu_joules", 0.0) - start_metrics.get("energy_cpu_joules", 0.0)
        total_energy_kwh = (gpu_energy_joules + cpu_energy_joules) / 3600.0 / 1000.0

        # Memory
        gpu_total = end_metrics.get("gpu_memory_total_mb", 1.0)
        gpu_used = end_metrics.get("gpu_memory_used_mb", 0.0)
        memory_efficiency = gpu_used / gpu_total if gpu_total > 0 else 0.0

        # GPU power (average)
        avg_gpu_power = (start_metrics.get("gpu_power_watts", 0.0) + end_metrics.get("gpu_power_watts", 0.0)) / 2.0

        # Carbon intensity (optional, can be passed from task)
        carbon_intensity = task.get("carbon_intensity_gco2_kwh", 200.0)
        carbon_kg = total_energy_kwh * carbon_intensity / 1000.0

        # Compose aggregated metrics
        metrics = {
            "success": success,
            "output": output,
            "inference_metrics": raw_inference_metrics,
            "elapsed_sec": elapsed_sec,
            "tokens_per_sec": tokens_per_sec,
            "total_energy_kwh": total_energy_kwh,
            "gpu_energy_joules": gpu_energy_joules,
            "cpu_energy_joules": cpu_energy_joules,
            "gpu_power_avg_watts": avg_gpu_power,
            "gpu_memory_peak_mb": max(start_metrics.get("gpu_memory_used_mb", 0),
                                      end_metrics.get("gpu_memory_used_mb", 0)),
            "memory_efficiency": memory_efficiency,
            "carbon_kg": carbon_kg,
            "gpu_oom": (not success and "CUDA out of memory" in str(raw_inference_metrics.get("error", ""))),
            "start_metrics": start_metrics,
            "end_metrics": end_metrics,
            # Additional derived metrics (already computed by profiler)
            "energy_efficiency": end_metrics.get("energy_efficiency", 0.0),
            "carbon_efficiency": end_metrics.get("carbon_efficiency", 0.0),
            "quality_score": raw_inference_metrics.get("quality_score", 1.0),
        }

        # ---- Store metrics in profiler's history (if enabled) ----
        # The profiler already stores its own snapshots; we can add a flag to store run‑level data.
        # For simplicity, we rely on the profiler's built‑in history.

        # ---- Trigger callbacks ----
        if self.enable_callbacks:
            self._trigger_callbacks(metrics)

        return metrics

    # --------------------- Integration Interfaces ---------------------
    def compute_modp_utility(self, metrics: Optional[Dict[str, Any]] = None) -> float:
        """
        Compute a scalar utility using MODP weights from the given metrics.
        If no metrics provided, uses the most recent run's metrics (if available).
        """
        if metrics is None:
            # Use the last run? We could store the last metrics, but we'll require passing.
            raise ValueError("Must provide metrics or store last run data.")
        objectives = {
            "quality": metrics.get("quality_score", 1.0),
            "throughput": metrics.get("tokens_per_sec", 0.0) / 100.0,  # normalize
            "energy_efficiency": metrics.get("energy_efficiency", 0.0),
            "carbon_efficiency": metrics.get("carbon_efficiency", 0.0),
            "memory_efficiency": metrics.get("memory_efficiency", 0.0),
        }
        return self.modp.evaluate(objectives, self.modp_weights)

    def compute_bio_fitness(self, metrics: Dict[str, Any], policy: Dict[str, Any]) -> float:
        """
        Delegate to the bio‑inspired fitness evaluator.
        """
        return self.bio.evaluate(metrics, policy)

    def get_moe_context(self, metrics: Dict[str, Any]) -> List[float]:
        """
        Generate a context vector for the MoE router.
        """
        return self.moe.encode(metrics)

    # --------------------- Utility ---------------------
    def get_last_run_metrics(self) -> Optional[Dict[str, Any]]:
        """
        Return the metrics from the most recent run (if stored).
        For simplicity, we don't store; we could cache the last metrics.
        """
        # We could implement caching if needed.
        return None
