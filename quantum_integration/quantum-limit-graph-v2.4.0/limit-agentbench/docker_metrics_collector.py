"""
docker_metrics_collector.py (Enhanced)

Collects runtime, memory, and CPU-based energy metrics
from inside a Docker container for green benchmarking.

Enhancements (enabled via `use_enhancements` flag):
  - MODP (Multi‑Objective Decision Process) composite score
  - LIMIT Graph metrics (centrality, connectivity) in results
  - RLHF human feedback score
  - Optional distillation/MoE-related stats from the enhancements folder
  - FlexGen-related energy tracking (if FlexGen is used)

When enhancements are disabled (default), the class behaves exactly as the original.
"""

import os
import time
import statistics
from typing import Callable, Dict, List, Optional, Any

# Optional imports for advanced enhancements (graceful fallback)
try:
    import numpy as np
    NUMPY_AVAILABLE = True
except ImportError:
    NUMPY_AVAILABLE = False

# Try importing enhancements schemas (may not be installed in container)
try:
    from enhancements.schemas.feedback_event import FeedbackEvent
    from enhancements.schemas.node_descriptor import NodeDescriptor
    from enhancements.schemas.workload_descriptor import WorkloadDescriptor
    ENHANCEMENTS_AVAILABLE = True
except ImportError:
    ENHANCEMENTS_AVAILABLE = False
    FeedbackEvent = None
    NodeDescriptor = None
    WorkloadDescriptor = None


class DockerMetricsCollector:
    def __init__(
        self,
        carbon_intensity: float = 0.0004,  # kgCO2 per Wh (configurable)
        cpu_tdp_watts: float = 65.0,        # conservative default
        use_enhancements: bool = False,     # enable advanced enhancements
        enhancement_config: Optional[Dict[str, Any]] = None,
    ):
        """
        Parameters
        ----------
        carbon_intensity : float
            Carbon intensity of electricity (kg CO2 per Wh)
        cpu_tdp_watts : float
            Approximate CPU TDP used for energy estimation
        use_enhancements : bool
            If True, integrate advanced enhancements (MODP, graph metrics, RLHF, etc.)
        enhancement_config : dict, optional
            Configuration for enhancements, may include:
                - modp_weights: list of 4 floats [accuracy, energy, latency, carbon]
                - graph_metrics: dict with centrality, connectivity
                - human_feedback_score: float (0-1)
                - distillation_stats: dict (for tracking distillation updates)
                - flexgen_energy_joules: float (FlexGen energy consumed)
        """
        self.carbon_intensity = carbon_intensity
        self.cpu_tdp_watts = cpu_tdp_watts
        self.use_enhancements = use_enhancements and ENHANCEMENTS_AVAILABLE
        self.enhancement_config = enhancement_config or {}
        # Default MODP weights if not provided
        self.modp_weights = self.enhancement_config.get(
            'modp_weights', [0.4, 0.3, 0.2, 0.1]  # accuracy, energy, latency, carbon
        )
        # Normalize weights
        total = sum(self.modp_weights)
        if total > 0:
            self.modp_weights = [w / total for w in self.modp_weights]
        else:
            self.modp_weights = [0.25, 0.25, 0.25, 0.25]

    # -------------------------
    # Core metric collectors (unchanged)
    # -------------------------

    def _read_cgroup_memory_peak(self) -> float:
        """
        Returns peak memory usage in MB.
        Supports cgroups v1 and v2.
        """
        paths = [
            "/sys/fs/cgroup/memory.max_usage_in_bytes",     # cgroup v1
            "/sys/fs/cgroup/memory.current",                # cgroup v2
        ]

        for path in paths:
            if os.path.exists(path):
                with open(path, "r") as f:
                    return int(f.read().strip()) / (1024 ** 2)

        raise RuntimeError("Unable to read cgroup memory stats")

    def _read_cpu_time(self) -> float:
        """
        Returns CPU time used by process in seconds.
        """
        with open("/proc/self/stat", "r") as f:
            fields = f.read().split()
            utime = float(fields[13])
            stime = float(fields[14])

        clock_ticks = os.sysconf(os.sysconf_names["SC_CLK_TCK"])
        return (utime + stime) / clock_ticks

    # -------------------------
    # Enhanced metric computation helpers
    # -------------------------

    def _compute_modp_score(self, accuracy: float, energy_wh: float,
                            latency_s: float, carbon_kg: float) -> float:
        """
        Compute a multi‑objective composite score (MODP) using configurable weights.
        All objectives are normalized to [0,1] with higher = better.
        """
        if not NUMPY_AVAILABLE:
            # Fallback to simple average if numpy not available
            return (accuracy + (1.0 - min(energy_wh, 1.0)) + (1.0 - min(latency_s, 10.0) / 10.0) + (1.0 - min(carbon_kg, 1.0))) / 4.0

        # Normalize each metric (lower is better for energy, latency, carbon)
        acc_norm = min(accuracy, 1.0)
        energy_norm = 1.0 - min(energy_wh / 1.0, 1.0)      # assume 1 Wh max for typical task
        latency_norm = 1.0 - min(latency_s / 10.0, 1.0)    # assume 10 s max
        carbon_norm = 1.0 - min(carbon_kg / 0.1, 1.0)      # assume 0.1 kg max

        return float(self.modp_weights[0] * acc_norm +
                     self.modp_weights[1] * energy_norm +
                     self.modp_weights[2] * latency_norm +
                     self.modp_weights[3] * carbon_norm)

    def _get_graph_metrics(self) -> Dict[str, float]:
        """Retrieve LIMIT Graph metrics from enhancement_config or default."""
        gm = self.enhancement_config.get('graph_metrics')
        if gm is None:
            # Fallback to defaults
            gm = {"centrality": 0.5, "connectivity": 0.5}
        return gm

    def _get_human_feedback_score(self) -> Optional[float]:
        """Retrieve RLHF human feedback score from config."""
        return self.enhancement_config.get('human_feedback_score')

    # -------------------------
    # Public API (enhanced)
    # -------------------------

    def run_and_measure(
        self,
        fn: Callable[[], float],
        runs: int = 5,
    ) -> Dict[str, float]:
        """
        Executes a callable multiple times and collects metrics.
        If enhancements are enabled, additional fields are added:
            - modp_score: composite multi‑objective score
            - graph_metrics: LIMIT Graph metrics
            - human_feedback_score: RLHF feedback (if provided)
            - distillation_stats: distillation update count (if provided)
            - flexgen_energy_joules: FlexGen energy (if provided)
        """
        latencies: List[float] = []
        cpu_times: List[float] = []
        accuracies: List[float] = []

        for _ in range(runs):
            start_cpu = self._read_cpu_time()
            start_time = time.perf_counter()

            acc = fn()

            end_time = time.perf_counter()
            end_cpu = self._read_cpu_time()

            latencies.append(end_time - start_time)
            cpu_times.append(end_cpu - start_cpu)
            accuracies.append(acc)

        avg_latency = statistics.mean(latencies)
        latency_variance = statistics.pvariance(latencies)
        avg_cpu_time = statistics.mean(cpu_times)
        avg_accuracy = statistics.mean(accuracies)

        energy_wh = (avg_cpu_time * self.cpu_tdp_watts) / 3600.0
        carbon_kg = energy_wh * self.carbon_intensity

        peak_memory_mb = self._read_cgroup_memory_peak()

        result = {
            "accuracy": avg_accuracy,
            "latency": avg_latency,
            "latency_variance": latency_variance,
            "cpu_time": avg_cpu_time,
            "energy": energy_wh,
            "carbon": carbon_kg,
            "memory": peak_memory_mb,
        }

        # Enhanced additions
        if self.use_enhancements:
            # MODP score
            result["modp_score"] = self._compute_modp_score(
                accuracy=avg_accuracy,
                energy_wh=energy_wh,
                latency_s=avg_latency,
                carbon_kg=carbon_kg,
            )

            # Graph metrics
            result["graph_metrics"] = self._get_graph_metrics()

            # RLHF human feedback
            hf = self._get_human_feedback_score()
            if hf is not None:
                result["human_feedback_score"] = hf

            # Distillation stats (optional)
            if 'distillation_stats' in self.enhancement_config:
                result["distillation_stats"] = self.enhancement_config['distillation_stats']

            # FlexGen energy (optional)
            if 'flexgen_energy_joules' in self.enhancement_config:
                result["flexgen_energy_joules"] = self.enhancement_config['flexgen_energy_joules']

        return result
