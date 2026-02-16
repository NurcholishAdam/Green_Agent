import numpy as np


class RewardNormalizer:

    def __init__(self):
        self.mean = 0.0
        self.var = 1.0
        self.count = 1e-4

    def update(self, reward):
        self.count += 1
        delta = reward - self.mean
        self.mean += delta / self.count
        self.var += delta * (reward - self.mean)

    def normalize(self, reward):
        std = np.sqrt(self.var / self.count)
        return (reward - self.mean) / (std + 1e-8)
