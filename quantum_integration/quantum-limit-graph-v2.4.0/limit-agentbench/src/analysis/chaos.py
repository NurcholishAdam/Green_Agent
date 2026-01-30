import random


class BudgetChaos:
    def __init__(self, probability=0.15):
        self.probability = probability

    def inject(self, metrics):
        if random.random() < self.probability:
            metrics["energy"] *= 2.5
            metrics["latency"] *= 2.0
        return metrics
