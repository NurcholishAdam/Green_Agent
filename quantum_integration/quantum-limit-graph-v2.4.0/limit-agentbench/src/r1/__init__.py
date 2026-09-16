# src/r1/__init__.py
"""
Reinforcement Learning Subsystem (R1)
=====================================

Energy-aware RL components for adaptive policy tuning.

Components
----------
- :class:`CentralCoordinator` — global sustainability ranking.
- :class:`EnergyRLAgent` — Q-learning for energy policy weights.
- :class:`PPOAgent` / :class:`PPOPolicy` — PPO for continuous/discrete actions.
- :class:`QLearningAgent` — Q-learning with persistent storage.
- :class:`QMemory` — JSON-backed Q-table memory.
- :class:`GreenRewardModel` — multi-objective reward function.
- :class:`RewardNormalizer` — running mean/std normalization.
- :class:`RLStorage` — Q-table persistence.
"""

from __future__ import annotations
import logging, os as _os
from typing import Any, Dict, Type

from .centralized_coordinator import CentralCoordinator, CoordinatorConfig, CoordinatorError
from .energy_rl_agent import EnergyRLAgent, EnergyRLConfig, EnergyRLError
from .ppo_agent import PPOAgent, PPOAgentConfig, PPOAgentError, _TORCH_AVAILABLE
from .ppo_policy import PPOPolicy, PPOPolicyConfig, PPOPolicyError
from .q_learning import QLearningAgent, QLearningConfig, QLearningError
from .q_memory import QMemory, QMemoryConfig, QMemoryError
from .reward_model import GreenRewardModel, RewardModelConfig, RewardModelError
from .reward_normalizer import RewardNormalizer, RewardNormalizerConfig, RewardNormalizerError
from .rl_storage import RLStorage, RLStorageConfig, RLStorageError

logger = logging.getLogger(__name__)

__all__ = [
    "CentralCoordinator", "CoordinatorConfig", "CoordinatorError",
    "EnergyRLAgent", "EnergyRLConfig", "EnergyRLError",
    "PPOAgent", "PPOAgentConfig", "PPOAgentError",
    "PPOPolicy", "PPOPolicyConfig", "PPOPolicyError",
    "QLearningAgent", "QLearningConfig", "QLearningError",
    "QMemory", "QMemoryConfig", "QMemoryError",
    "GreenRewardModel", "RewardModelConfig", "RewardModelError",
    "RewardNormalizer", "RewardNormalizerConfig", "RewardNormalizerError",
    "RLStorage", "RLStorageConfig", "RLStorageError",
    "RL_REGISTRY", "get_rl_component", "list_rl_components", "register_rl_component",
]

RL_REGISTRY: Dict[str, Type[Any]] = {
    "coordinator": CentralCoordinator,
    "energy_rl":   EnergyRLAgent,
    "ppo_agent":   PPOAgent,
    "ppo_policy":  PPOPolicy,
    "q_learning":  QLearningAgent,
    "q_memory":    QMemory,
    "reward":      GreenRewardModel,
    "normalizer":  RewardNormalizer,
    "storage":     RLStorage,
}

def register_rl_component(name: str, cls: Type[Any]) -> None:
    """Register an RL component class under ``name``."""
    if not isinstance(name, str) or not name:
        raise ValueError("name must be a non-empty string.")
    if not isinstance(cls, type):
        raise TypeError(f"Expected a class, got {type(cls).__name__}.")
    existing = RL_REGISTRY.get(name)
    if existing is not None and existing is not cls:
        raise ValueError(
            f"RL component '{name}' already registered to {existing.__name__}."
        )
    RL_REGISTRY[name] = cls
    logger.debug("Registered RL component '%s' -> %s", name, cls.__name__)

def get_rl_component(name: str) -> Type[Any]:
    """Return the RL component class registered under ``name``."""
    try:
        return RL_REGISTRY[name]
    except KeyError as exc:
        raise KeyError(
            f"Unknown RL component '{name}'. Available: {sorted(RL_REGISTRY)}"
        ) from exc

def list_rl_components() -> Dict[str, str]:
    """Return a mapping of registered name -> class name."""
    return {n: c.__name__ for n, c in RL_REGISTRY.items()}

if _os.environ.get("GREEN_AGENT_VALIDATE_RL") == "1":
    for _name, _cls in RL_REGISTRY.items():
        try:
            logger.info("RL component '%s' importable.", _name)
        except Exception:
            logger.exception("RL component '%s' failed validation.", _name)
            raise
