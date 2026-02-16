import torch


class HealthMonitor:

    def check_loss(self, loss):
        if torch.isnan(loss):
            raise RuntimeError("NaN detected in PPO loss")

        if loss.abs() > 1e6:
            raise RuntimeError("Loss explosion detected")

    def check_energy(self, energy, budget):
        if energy > budget * 2:
            print("Warning: extreme energy spike")

    def check_reward(self, reward):
        if abs(reward) > 100:
            print("Warning: abnormal reward magnitude")
