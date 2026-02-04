"""
Normalizes overhead costs for different runtimes (LangChain, AutoGen).
Helps isolate agent logic cost from framework cost.
"""

BASELINES = {
    "langchain": {"latency": 0.15, "energy_wh": 0.02},
    "autogen": {"latency": 0.20, "energy_wh": 0.03},
}

def apply_overhead(metrics: dict, framework: str) -> dict:
    baseline = BASELINES.get(framework, {})
    metrics["framework_overhead_latency"] = baseline.get("latency", 0.0)
    metrics["framework_overhead_energy_wh"] = baseline.get("energy_wh", 0.0)

    metrics["effective_latency_s"] = max(
        0.0, metrics["latency_s"] - metrics["framework_overhead_latency"]
    )
    metrics["effective_energy_wh"] = max(
        0.0, metrics["energy_wh"] - metrics["framework_overhead_energy_wh"]
    )
    return metrics
