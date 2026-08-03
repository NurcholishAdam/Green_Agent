# user_preferences.py
"""
User preferences for multi‑objective routing.

This module defines a validated set of weights for Pareto‑based routing decisions.
It includes persistence, normalization, and preset profiles.
"""

import json
import os
import logging
from typing import Dict, Optional, Union
from pathlib import Path
from datetime import datetime

# ---------- Pydantic ----------
from pydantic import BaseModel, Field, field_validator, model_validator, ConfigDict

# ---------- Optional: for persistence ----------
try:
    import aiofiles
except ImportError:
    aiofiles = None

logger = logging.getLogger(__name__)

# ============================================================================
# Pydantic Model for User Preferences
# ============================================================================

class UserPreferences(BaseModel):
    """
    Validated user preferences for multi‑objective routing.

    The weights represent the relative importance of different objectives:
    - alpha:  (e.g., energy efficiency)
    - beta:   (e.g., carbon intensity)
    - gamma:  (e.g., helium scarcity)
    - delta:  (e.g., cost)
    - epsilon: (e.g., latency)
    - zeta:   (e.g., accuracy – may be negative for trade‑offs)

    Weights are automatically normalized to sum to 1.0 if they don't already.
    """

    alpha: float = Field(1.0, ge=-1.0, le=1.0, description="Energy efficiency weight")
    beta: float = Field(1.0, ge=-1.0, le=1.0, description="Carbon intensity weight")
    gamma: float = Field(0.5, ge=-1.0, le=1.0, description="Helium scarcity weight")
    delta: float = Field(0.3, ge=-1.0, le=1.0, description="Cost weight")
    epsilon: float = Field(0.1, ge=-1.0, le=1.0, description="Latency weight")
    zeta: float = Field(-0.1, ge=-1.0, le=1.0, description="Accuracy weight (negative means trade‑off)")

    # Version for forward compatibility
    version: str = Field("2.0.0", description="Schema version")

    model_config = ConfigDict(extra="forbid")  # Prevent unknown keys

    # ---------- Validation ----------
    @model_validator(mode='after')
    def validate_weights(self):
        # Ensure that at least some weights are non‑zero to avoid a zero vector.
        if all(abs(w) < 1e-9 for w in [self.alpha, self.beta, self.gamma, self.delta, self.epsilon, self.zeta]):
            raise ValueError("All weights are zero; at least one must be non‑zero")
        return self

    # ---------- Utility Methods ----------
    def normalize(self) -> 'UserPreferences':
        """
        Return a new UserPreferences instance with weights normalized to sum to 1.0.
        Negative weights are preserved but the sum is adjusted.
        """
        total = abs(self.alpha) + abs(self.beta) + abs(self.gamma) + abs(self.delta) + abs(self.epsilon) + abs(self.zeta)
        if total == 0:
            return self.copy()  # Avoid division by zero
        return UserPreferences(
            alpha=self.alpha / total,
            beta=self.beta / total,
            gamma=self.gamma / total,
            delta=self.delta / total,
            epsilon=self.epsilon / total,
            zeta=self.zeta / total,
        )

    def to_dict(self) -> Dict[str, float]:
        """Return a dictionary of weights (without version)."""
        return {
            'alpha': self.alpha,
            'beta': self.beta,
            'gamma': self.gamma,
            'delta': self.delta,
            'epsilon': self.epsilon,
            'zeta': self.zeta,
        }

    # ---------- Presets ----------
    @classmethod
    def balanced(cls) -> 'UserPreferences':
        """A balanced profile with equal weights."""
        return cls(alpha=1.0, beta=1.0, gamma=1.0, delta=1.0, epsilon=1.0, zeta=0.0)

    @classmethod
    def energy_first(cls) -> 'UserPreferences':
        """Prioritize energy efficiency."""
        return cls(alpha=2.0, beta=0.5, gamma=0.2, delta=0.3, epsilon=0.1, zeta=-0.1)

    @classmethod
    def carbon_first(cls) -> 'UserPreferences':
        """Prioritize carbon reduction."""
        return cls(alpha=0.2, beta=2.0, gamma=0.3, delta=0.2, epsilon=0.1, zeta=-0.1)

    @classmethod
    def helium_aware(cls) -> 'UserPreferences':
        """Prioritize helium conservation."""
        return cls(alpha=0.3, beta=0.3, gamma=2.0, delta=0.1, epsilon=0.1, zeta=-0.1)

    # ---------- Persistence ----------
    def save(self, path: Union[str, Path], async_write: bool = False) -> bool:
        """
        Save preferences to a JSON file.

        Args:
            path: File path.
            async_write: If True and aiofiles is available, write asynchronously.

        Returns:
            True on success, False on failure.
        """
        path = Path(path)
        data = self.model_dump()
        try:
            if async_write and aiofiles:
                import asyncio
                async def write():
                    async with aiofiles.open(path, 'w') as f:
                        await f.write(json.dumps(data, indent=2))
                asyncio.run(write())
            else:
                with open(path, 'w') as f:
                    json.dump(data, f, indent=2)
            logger.info(f"User preferences saved to {path}")
            return True
        except Exception as e:
            logger.error(f"Failed to save preferences: {e}")
            return False

    @classmethod
    def load(cls, path: Union[str, Path], async_read: bool = False) -> Optional['UserPreferences']:
        """
        Load preferences from a JSON file.

        Args:
            path: File path.
            async_read: If True and aiofiles is available, read asynchronously.

        Returns:
            UserPreferences instance or None if loading failed.
        """
        path = Path(path)
        if not path.exists():
            logger.warning(f"Preferences file {path} not found")
            return None
        try:
            if async_read and aiofiles:
                import asyncio
                async def read():
                    async with aiofiles.open(path, 'r') as f:
                        content = await f.read()
                    return json.loads(content)
                data = asyncio.run(read())
            else:
                with open(path, 'r') as f:
                    data = json.load(f)
            # Validate and create instance
            prefs = cls(**data)
            logger.info(f"User preferences loaded from {path}")
            return prefs
        except Exception as e:
            logger.error(f"Failed to load preferences: {e}")
            return None

    # ---------- Application to Scoring ----------
    def apply(self, objective_values: Dict[str, float]) -> float:
        """
        Compute a weighted sum of objective values.

        Args:
            objective_values: Dictionary with keys matching the weight names.

        Returns:
            Weighted sum.

        Raises:
            ValueError: If a required objective is missing.
        """
        weight_dict = self.to_dict()
        total = 0.0
        for key, val in objective_values.items():
            if key in weight_dict:
                total += weight_dict[key] * val
            else:
                raise ValueError(f"Unknown objective key: {key}")
        return total

    # ---------- Copy and Versioning ----------
    def copy(self) -> 'UserPreferences':
        """Return a copy of the instance."""
        return self.model_copy()


# ============================================================================
# Example Usage
# ============================================================================

if __name__ == "__main__":
    # Create a balanced profile
    prefs = UserPreferences.balanced()
    print("Balanced weights:", prefs.to_dict())

    # Normalize (optional, if you need sum to 1)
    normalized = prefs.normalize()
    print("Normalized weights:", normalized.to_dict())

    # Save to file
    prefs.save("preferences.json")

    # Load from file
    loaded = UserPreferences.load("preferences.json")
    if loaded:
        print("Loaded weights:", loaded.to_dict())

    # Apply to scoring
    objectives = {"alpha": 0.8, "beta": 0.6, "gamma": 0.4, "delta": 0.7, "epsilon": 0.3, "zeta": 0.5}
    score = loaded.apply(objectives) if loaded else 0
    print("Weighted score:", score)
