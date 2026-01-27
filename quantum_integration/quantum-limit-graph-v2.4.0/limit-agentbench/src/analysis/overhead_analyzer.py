# src/analysis/overhead_analyzer.py

class OverheadAnalyzer:
    """
    Computes framework overhead by subtracting baseline costs.
    """

    def __init__(self, baseline_latency: float, baseline_energy: float):
        self.baseline_latency = baseline_latency
        self.baseline_energy = baseline_energy

    def compute(self, metrics: dict) -> dict:
        overhead_latency = max(
            0.0, metrics["latency"] - self.baseline_latency
        )
        overhead_energy = max(
            0.0, metrics["energy"] - self.baseline_energy
        )

        metrics["framework_overhead_latency"] = overhead_latency
        metrics["framework_overhead_energy"] = overhead_energy
        return metrics
