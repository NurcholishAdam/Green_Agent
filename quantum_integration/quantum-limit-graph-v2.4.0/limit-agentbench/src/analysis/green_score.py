"""
Green score computation combining accuracy and efficiency.
"""

def compute_green_score(
    accuracy: float,
    energy: float,
    latency: float,
    alpha: float = 0.6,
    beta: float = 0.2,
) -> float:
    """
    Higher is better.

    alpha controls importance of accuracy
    beta controls latency penalty
    """
    return (alpha * accuracy) - ((1 - alpha) * energy) - (beta * latency)
