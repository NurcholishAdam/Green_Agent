import random
import time
import os
import signal


def random_failure_injection(prob=0.05):
    if random.random() < prob:
        print("[STRESS] Injecting artificial crash")
        os.kill(os.getpid(), signal.SIGTERM)


def energy_spike_simulation(energy):
    if random.random() < 0.1:
        return energy * 5
    return energy


def reward_spike_simulation(reward):
    if random.random() < 0.1:
        return reward * 10
    return reward
