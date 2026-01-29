import random

class ChaosInjector:
    def __init__(self, enabled=False, severity=0.1):
        self.enabled = enabled
        self.severity = severity

    def perturb(self, metrics: dict) -> dict:
        if not self.enabled:
            return metrics

        def noise(x):
            return x * (1 + random.uniform(-self.severity, self.severity))

        for k in ["energy", "latency", "carbon"]:
            if k in metrics:
                metrics[k] = noise(metrics[k])

        return metrics
