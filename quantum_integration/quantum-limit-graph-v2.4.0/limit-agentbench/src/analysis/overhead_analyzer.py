# src/analysis/overhead_analyzer.py
class OverheadAnalyzer:
    def __init__(
        self,
        tool_call_cost_joules: float = 0.05,
        tool_call_latency_sec: float = 0.2,
    ):
        self.tool_energy = tool_call_cost_joules
        self.tool_latency = tool_call_latency_sec

    def compute(self, metrics: dict) -> dict:
        tool_calls = metrics.get("tool_calls", 0)

        metrics["framework_overhead_energy"] = (
            tool_calls * self.tool_energy
        )
        metrics["framework_overhead_latency"] = (
            tool_calls * self.tool_latency
        )

        return metrics
