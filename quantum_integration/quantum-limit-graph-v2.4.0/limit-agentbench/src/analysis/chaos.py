"""
Chaos testing module.

Used ONLY in evaluation mode, never production.
"""

import random


def inject_chaos(metrics: dict, enabled: bool = True) -> dict:
    if not enabled:
        return metrics

    if random.random() < 0.2:
        metrics["energy_wh"] *= 1.3
        metrics.setdefault("chaos_events", []).append("energy_spike")

    if random.random() < 0.1:
        metrics["latency_s"] *= 1.5
        metrics.setdefault("chaos_events", []).append("latency_spike")

    return metrics
