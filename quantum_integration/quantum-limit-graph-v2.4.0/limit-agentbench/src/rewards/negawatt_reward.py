from typing import Optional


class NegawattReward:
    """
    Sustainability reward shaping module.

    Provides:
    - Accuracy per Watt
    - Negawatt score (relative energy savings)
    - RL-compatible combined reward
    """

    def __init__(self, baseline_energy: float):
        self.baseline_energy = baseline_energy

    def accuracy_per_watt(self, accuracy: float, energy: float) -> float:
        if energy <= 0:
            return 0.0
        return accuracy / energy

    def negawatt_score(self, accuracy: float, energy: float) -> float:
        if self.baseline_energy <= 0:
            return 0.0

        saved_ratio = (self.baseline_energy - energy) / self.baseline_energy
        return max(0.0, accuracy * saved_ratio)

    def combined_reward(
        self,
        accuracy: float,
        energy: float,
        alpha: float = 1.0,
        beta: float = 2.0,
    ) -> float:
        return alpha * accuracy + beta * self.negawatt_score(accuracy, energy)
