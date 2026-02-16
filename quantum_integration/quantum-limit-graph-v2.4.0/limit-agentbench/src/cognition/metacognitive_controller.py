class MetaCognitiveController:

    def __init__(self):
        self.anomaly_score = 0.0

    def evaluate(self, metrics):
        if metrics["energy"] > 2 * metrics["baseline_energy"]:
            self.anomaly_score += 1
        else:
            self.anomaly_score *= 0.9

        return self.anomaly_score

    def should_trigger_recovery(self):
        return self.anomaly_score > 5
