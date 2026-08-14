"""
reward_calculator.py

Computes the final reward using real metrics from MetricAggregator.
"""
from typing import Dict, Any


class RewardCalculator:
    def __init__(self, weights: Dict[str, float] = None):
        self.weights = weights or {
            "quality": 0.30,
            "throughput": 0.25,
            "energy_efficiency": 0.20,
            "carbon_efficiency": 0.15,
            "memory_efficiency": 0.10,
        }

    def compute(self, aggregated_metrics: Dict[str, Any],
                constraints: Dict[str, Any],
                carbon_intensity_gco2_kwh: float = 0.0) -> float:
        """Return a reward between -10.0 and 10.0."""
        # 1. Extract metrics
        quality = aggregated_metrics.get("quality_score", 1.0)  # must be passed from task eval
        throughput = aggregated_metrics.get("tokens_per_sec", 0.0)
        total_energy_kwh = aggregated_metrics.get("total_energy_kwh", 0.0)
        mem_eff = aggregated_metrics.get("memory_efficiency", 0.0)
        oom = aggregated_metrics.get("gpu_oom", False)

        # 2. Carbon efficiency: lower carbon per token is better
        if throughput > 0 and total_energy_kwh > 0:
            carbon_per_token = (total_energy_kwh * carbon_intensity_gco2_kwh) / throughput
            # Normalize to 0-1 (assuming 100gCO2/token is terrible, 0 is perfect)
            carbon_eff = max(0.0, 1.0 - (carbon_per_token / 100.0))
        else:
            carbon_eff = 0.0

        # 3. Energy efficiency: higher tokens per kWh is better
        if total_energy_kwh > 0 and throughput > 0:
            energy_eff = min(1.0, throughput / (total_energy_kwh * 1000))  # tokens per kWh
        else:
            energy_eff = 0.0

        # 4. Penalties for constraint violations
        penalty = 0.0
        if oom:
            penalty -= 10.0
        max_latency = constraints.get("max_latency_ms", 1e9)
        if aggregated_metrics.get("elapsed_sec", 0) * 1000 > max_latency:
            penalty -= 5.0
        if quality < constraints.get("min_quality", 0.5):
            penalty -= 5.0

        # 5. Weighted sum
        reward = (
            self.weights["quality"] * quality +
            self.weights["throughput"] * min(1.0, throughput / 100.0) +
            self.weights["energy_efficiency"] * energy_eff +
            self.weights["carbon_efficiency"] * carbon_eff +
            self.weights["memory_efficiency"] * mem_eff
        ) + penalty

        return max(-10.0, min(10.0, reward))
