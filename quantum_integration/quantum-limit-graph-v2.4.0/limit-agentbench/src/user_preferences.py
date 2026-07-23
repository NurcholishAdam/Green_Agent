# File: src/user_preferences.py
"""
User preferences for multi‑objective routing.
"""

from typing import Dict, Any, Optional

class UserPreferences:
    """
    Stores user‑defined weights for the Pareto router.
    """

    def __init__(self, weights: Optional[Dict[str, float]] = None):
        self.weights = weights or {
            'alpha': 1.0,
            'beta': 1.0,
            'gamma': 0.5,
            'delta': 0.3,
            'epsilon': 0.1,
            'zeta': -0.1
        }

    def get_weights(self) -> Dict[str, float]:
        """Return the current weight dictionary."""
        return self.weights

    def set_weights(self, weights: Dict[str, float]) -> None:
        """Update the weights."""
        self.weights = weights

    def to_dict(self) -> Dict[str, Any]:
        """Serialize for storage."""
        return {'weights': self.weights}
