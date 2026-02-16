from dataclasses import dataclass


@dataclass
class PPOConfig:
    lr: float = 3e-4
    clip_epsilon: float = 0.2
    entropy_coef: float = 0.01
    value_coef: float = 0.5
    max_grad_norm: float = 0.5


@dataclass
class SystemConfig:
    energy_budget: float = 100.0
    reward_clip_min: float = -10.0
    reward_clip_max: float = 10.0
    coordinator_sync_interval: int = 10
    save_interval: int = 5
