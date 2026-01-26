"""
Green-aware reward shaping for RLHF.
"""

def apply_green_penalty(
    base_reward: float,
    energy: float,
    carbon: float,
    energy_weight: float = 0.05,
    carbon_weight: float = 1.0,
) -> float:
    """
    Penalizes reward based on environmental impact.
    """
    penalty = (energy_weight * energy) + (carbon_weight * carbon)
    return base_reward - penalty
