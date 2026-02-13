def attach_provenance(metrics: dict) -> dict:
    metrics["metric_provenance"] = {
        "energy": "measured",
        "latency": "measured",
        "carbon": "estimated",
        "framework_overhead": "estimated"
    }
    return metrics
