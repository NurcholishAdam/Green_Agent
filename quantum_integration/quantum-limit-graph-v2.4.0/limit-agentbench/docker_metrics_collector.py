"""
docker_metrics_collector.py

Collects runtime, memory, and CPU-based energy metrics
from inside a Docker container for green benchmarking.
"""

import os
import time
import statistics
from typing import Callable, Dict, List


class DockerMetricsCollector:
    def __init__(
        self,
        carbon_intensity: float = 0.0004,  # kgCO2 per Wh (configurable)
        cpu_tdp_watts: float = 65.0,        # conservative default
    ):
        """
        Parameters
        ----------
        carbon_intensity : float
            Carbon intensity of electricity (kg CO2 per Wh)
        cpu_tdp_watts : float
            Approximate CPU TDP used for energy estimation
        """
        self.carbon_intensity = carbon_intensity
        self.cpu_tdp_watts = cpu_tdp_watts

    # -------------------------
    # Core metric collectors
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
    # Public API
    # -------------------------

    def run_and_measure(
        self,
        fn: Callable[[], float],
        runs: int = 5,
    ) -> Dict[str, float]:
        """
        Executes a callable multiple times and collects metrics.

        Parameters
        ----------
        fn : Callable
            Function that runs agent inference and returns accuracy
        runs : int
            Number of repeated executions for variance

        Returns
        -------
        Dict[str, float]
            Collected metrics
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

        return {
            "accuracy": avg_accuracy,
            "latency": avg_latency,
            "latency_variance": latency_variance,
            "cpu_time": avg_cpu_time,
            "energy": energy_wh,
            "carbon": carbon_kg,
            "memory": peak_memory_mb,
        }
