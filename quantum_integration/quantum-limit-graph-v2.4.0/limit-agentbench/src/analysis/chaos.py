"""
Chaos testing utilities for budget exhaustion.
"""

import random

def inject_energy_spike(metrics: dict, probability: float = 0.1) -> dict:
    if random.random() < probability:
        metrics["energy"] *= 1.5
        metrics["chaos_event"] = "energy_spike"
    return metrics
