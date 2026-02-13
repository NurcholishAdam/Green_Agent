import random

def inject_energy_spike(metrics, probability=0.1):
    if random.random() < probability:
        metrics["energy"] *= 1.5
        metrics["chaos_event"] = "energy_spike"
    return metrics
