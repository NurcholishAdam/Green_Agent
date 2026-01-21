"""
RLHF Reward Shaping for Green_Agent

Provides scenario-dependent reward functions for policy optimization.
Transforms Green_Agent into a policy evaluation environment.

Formula: R = TaskSuccess - λ₁·Energy - λ₂·CO₂ - λ₃·Latency - λ₄·Cost
"""

from typing import Dict, List, Optional
from dataclasses import dataclass
from enum import Enum
import numpy as np
import logging

logger = logging.getLogger(__name__)


class ExecutionMode(Enum):
    """
    Different optimization modes for agent execution
    
    Each mode has different penalty weights for resources:
    - ECO_MODE: Minimize energy/carbon (heavy penalties)
    - FAST_MODE: Minimize latency (light energy penalties)
    - ACCURACY_MODE: Maximize accuracy (minimal resource penalties)
    - BALANCED_MODE: Balance all factors
    - CUSTOM: User-defined weights
    """
    ECO_MODE = "eco"
    FAST_MODE = "fast"
    ACCURACY_MODE = "accuracy"
    BALANCED_MODE = "balanced"
    CUSTOM = "custom"


@dataclass
class RewardConfig:
    """
    Configuration for reward function
    
    Lambda (λ) values determine how much each resource is penalized.
    Higher λ = stronger penalty for that resource.
    
    Attributes:
        mode: Execution mode
        lambda_energy: Penalty weight for energy consumption
        lambda_carbon: Penalty weight for carbon emissions
        lambda_latency: Penalty weight for latency
        lambda_cost: Penalty weight for cost (optional)
        energy_scale: Scaling factor for energy (to normalize units)
        carbon_scale: Scaling factor for carbon
        latency_scale: Scaling factor for latency
    """
    mode: ExecutionMode
    lambda_energy: float      # Penalty weight for energy
    lambda_carbon: float      # Penalty weight for carbon
    lambda_latency: float     # Penalty weight for latency
    lambda_cost: float = 0.0  # Penalty weight for cost
    
    # Normalization factors (scale raw metrics to reasonable penalty range)
    energy_scale: float = 1000.0    # Scale kWh to ~1.0 penalty range
    carbon_scale: float = 100.0     # Scale kg to ~1.0 penalty range
    latency_scale: float = 0.001    # Scale ms to ~1.0 penalty range
    cost_scale: float = 1.0         # USD already reasonable scale


class RewardShaper:
    """
    Shapes rewards for RLHF policy optimization
    
    Provides scenario-dependent reward functions that enable agents
    to be optimized for different deployment contexts (eco, fast, accuracy).
    """
    
    # Predefined mode configurations
    MODE_CONFIGS = {
        ExecutionMode.ECO_MODE: RewardConfig(
            mode=ExecutionMode.ECO_MODE,
            lambda_energy=10.0,   # Heavy penalty on energy
            lambda_carbon=10.0,   # Heavy penalty on carbon
            lambda_latency=1.0,   # Light penalty on latency
            lambda_cost=2.0       # Moderate penalty on cost
        ),
        ExecutionMode.FAST_MODE: RewardConfig(
            mode=ExecutionMode.FAST_MODE,
            lambda_energy=1.0,    # Light penalty on energy
            lambda_carbon=1.0,    # Light penalty on carbon
            lambda_latency=10.0,  # Heavy penalty on latency
            lambda_cost=1.0       # Light penalty on cost
        ),
        ExecutionMode.ACCURACY_MODE: RewardConfig(
            mode=ExecutionMode.ACCURACY_MODE,
            lambda_energy=0.5,    # Minimal penalty on energy
            lambda_carbon=0.5,    # Minimal penalty on carbon
            lambda_latency=0.5,   # Minimal penalty on latency
            lambda_cost=0.5       # Minimal penalty on cost
        ),
        ExecutionMode.BALANCED_MODE: RewardConfig(
            mode=ExecutionMode.BALANCED_MODE,
            lambda_energy=3.0,    # Balanced penalties
            lambda_carbon=3.0,
            lambda_latency=3.0,
            lambda_cost=2.0
        )
    }
    
    def __init__(self,
                 mode: ExecutionMode = ExecutionMode.BALANCED_MODE,
                 custom_config: Optional[RewardConfig] = None):
        """
        Initialize reward shaper
        
        Args:
            mode: Execution mode (eco/fast/accuracy/balanced/custom)
            custom_config: Override with custom configuration
        
        Example:
            # Use predefined eco mode
            shaper = RewardShaper(ExecutionMode.ECO_MODE)
            
            # Use custom configuration
            custom = RewardConfig(
                mode=ExecutionMode.CUSTOM,
                lambda_energy=5.0,
                lambda_carbon=8.0,
                lambda_latency=2.0
            )
            shaper = RewardShaper(ExecutionMode.CUSTOM, custom)
        """
        if mode == ExecutionMode.CUSTOM and custom_config is None:
            raise ValueError("Custom mode requires custom_config")
        
        self.config = custom_config if custom_config else self.MODE_CONFIGS[mode]
        logger.info(f"Initialized RewardShaper with {self.config.mode.value} mode")
    
    def compute_reward(self,
                      task_success: float,
                      energy_kwh: float,
                      carbon_kg: float,
                      latency_ms: float,
                      cost_usd: float = 0.0) -> Dict:
        """
        Compute reward with scenario-dependent penalties
        
        Formula:
            R = TaskSuccess - λ₁·Energy - λ₂·CO₂ - λ₃·Latency - λ₄·Cost
        
        Where each resource is scaled to reasonable penalty range.
        
        Args:
            task_success: Task accuracy or completion (0.0 to 1.0)
            energy_kwh: Energy consumed in kWh
            carbon_kg: Carbon emitted in kg CO₂e
            latency_ms: Task latency in milliseconds
            cost_usd: Cost in USD (optional)
        
        Returns:
            Dict with:
            {
                'reward': float,              # Total reward
                'components': {               # Breakdown of components
                    'task_success': float,
                    'energy_penalty': float,
                    'carbon_penalty': float,
                    'latency_penalty': float,
                    'cost_penalty': float
                },
                'penalties': {
                    'total_penalty': float
                },
                'mode': str
            }
        
        Example:
            reward_data = shaper.compute_reward(
                task_success=0.95,
                energy_kwh=0.003,
                carbon_kg=0.0006,
                latency_ms=150
            )
            
            print(f"Reward: {reward_data['reward']:.3f}")
            print(f"Mode: {reward_data['mode']}")
        """
        # Normalize metrics by scaling factors
        energy_penalty = self.config.lambda_energy * (energy_kwh * self.config.energy_scale)
        carbon_penalty = self.config.lambda_carbon * (carbon_kg * self.config.carbon_scale)
        latency_penalty = self.config.lambda_latency * (latency_ms * self.config.latency_scale)
        cost_penalty = self.config.lambda_cost * (cost_usd * self.config.cost_scale)
        
        # Compute total reward
        total_penalty = energy_penalty + carbon_penalty + latency_penalty + cost_penalty
        reward = task_success - total_penalty
        
        logger.debug(f"Computed reward: {reward:.3f} "
                    f"(success={task_success:.3f}, penalty={total_penalty:.3f})")
        
        return {
            'reward': reward,
            'components': {
                'task_success': task_success,
                'energy_penalty': -energy_penalty,
                'carbon_penalty': -carbon_penalty,
                'latency_penalty': -latency_penalty,
                'cost_penalty': -cost_penalty
            },
            'penalties': {
                'total_penalty': total_penalty,
                'energy_penalty': energy_penalty,
                'carbon_penalty': carbon_penalty,
                'latency_penalty': latency_penalty,
                'cost_penalty': cost_penalty
            },
            'mode': self.config.mode.value,
            'lambda_values': {
                'energy': self.config.lambda_energy,
                'carbon': self.config.lambda_carbon,
                'latency': self.config.lambda_latency,
                'cost': self.config.lambda_cost
            }
        }
    
    def compute_batch_rewards(self, results: List[Dict]) -> List[Dict]:
        """
        Compute rewards for batch of results
        
        Args:
            results: List of result dictionaries with keys:
                'task_success', 'energy_kwh', 'carbon_kg', 'latency_ms'
        
        Returns:
            List of reward dictionaries (same as compute_reward)
        """
        rewards = []
        
        for result in results:
            reward_data = self.compute_reward(
                task_success=result.get('task_success', result.get('accuracy', 0.0)),
                energy_kwh=result.get('energy_kwh', 0.0),
                carbon_kg=result.get('carbon_kg', 0.0),
                latency_ms=result.get('latency_ms', 0.0),
                cost_usd=result.get('cost_usd', 0.0)
            )
            
            rewards.append({
                **reward_data,
                'agent_id': result.get('agent_id'),
                'task_id': result.get('task_id')
            })
        
        return rewards
    
    def compare_policies(self, results: List[Dict]) -> Dict:
        """
        Compare different agent policies using reward
        
        Args:
            results: List of result dictionaries with:
                {
                    'agent_id': str,
                    'task_success': float,
                    'energy_kwh': float,
                    'carbon_kg': float,
                    'latency_ms': float,
                    'cost_usd': float (optional)
                }
        
        Returns:
            Dict with ranked policies:
            {
                'rankings': List[Dict],  # Sorted by reward
                'mode': str,
                'best_agent': str,
                'summary': Dict
            }
        
        Example:
            results = [
                {'agent_id': 'A', 'task_success': 0.95, 'energy_kwh': 0.005, ...},
                {'agent_id': 'B', 'task_success': 0.90, 'energy_kwh': 0.002, ...}
            ]
            
            comparison = shaper.compare_policies(results)
            print(f"Best: {comparison['best_agent']}")
        """
        ranked = []
        
        for result in results:
            reward_data = self.compute_reward(
                task_success=result.get('task_success', result.get('accuracy', 0.0)),
                energy_kwh=result.get('energy_kwh', 0.0),
                carbon_kg=result.get('carbon_kg', 0.0),
                latency_ms=result.get('latency_ms', 0.0),
                cost_usd=result.get('cost_usd', 0.0)
            )
            
            ranked.append({
                'agent_id': result['agent_id'],
                **reward_data,
                'raw_metrics': {
                    'task_success': result.get('task_success', result.get('accuracy')),
                    'energy_kwh': result.get('energy_kwh'),
                    'carbon_kg': result.get('carbon_kg'),
                    'latency_ms': result.get('latency_ms')
                }
            })
        
        # Sort by reward (descending)
        ranked.sort(key=lambda x: x['reward'], reverse=True)
        
        # Add ranks
        for i, agent in enumerate(ranked):
            agent['rank'] = i + 1
        
        # Summary statistics
        rewards = [r['reward'] for r in ranked]
        
        return {
            'rankings': ranked,
            'mode': self.config.mode.value,
            'best_agent': ranked[0]['agent_id'] if ranked else None,
            'summary': {
                'mean_reward': np.mean(rewards),
                'std_reward': np.std(rewards),
                'min_reward': np.min(rewards),
                'max_reward': np.max(rewards),
                'reward_range': np.max(rewards) - np.min(rewards)
            }
        }
    
    def recommend_mode(self, constraints: Dict[str, float]) -> ExecutionMode:
        """
        Recommend execution mode based on constraints
        
        Args:
            constraints: Dict with constraint priorities (0.0-1.0):
                {
                    'energy_priority': float,
                    'carbon_priority': float,
                    'latency_priority': float,
                    'accuracy_priority': float
                }
        
        Returns:
            Recommended ExecutionMode
        
        Example:
            # For deployment where latency is critical
            mode = shaper.recommend_mode({
                'energy_priority': 0.2,
                'latency_priority': 0.8,
                'accuracy_priority': 0.6
            })
            # Returns: ExecutionMode.FAST_MODE
        """
        energy_priority = constraints.get('energy_priority', 0.33)
        latency_priority = constraints.get('latency_priority', 0.33)
        accuracy_priority = constraints.get('accuracy_priority', 0.33)
        
        # Simple heuristic based on dominant priority
        if accuracy_priority > 0.6:
            return ExecutionMode.ACCURACY_MODE
        elif latency_priority > 0.6:
            return ExecutionMode.FAST_MODE
        elif energy_priority > 0.6:
            return ExecutionMode.ECO_MODE
        else:
            return ExecutionMode.BALANCED_MODE
    
    def optimize_lambda_values(self,
                               training_data: List[Dict],
                               target_metric: str = 'reward') -> RewardConfig:
        """
        Optimize lambda values based on training data
        
        Uses simple grid search to find lambda values that maximize
        correlation with target metric.
        
        Args:
            training_data: List of result dicts with human ratings/preferences
            target_metric: Metric to optimize ('reward', 'preference', etc.)
        
        Returns:
            Optimized RewardConfig
        
        Note: This is a simplified optimization. For production,
              consider using more sophisticated methods like Bayesian optimization.
        """
        logger.info(f"Optimizing lambda values for {target_metric}")
        
        # Grid search over lambda values
        best_config = None
        best_score = -float('inf')
        
        for lambda_energy in [0.5, 1.0, 3.0, 5.0, 10.0]:
            for lambda_carbon in [0.5, 1.0, 3.0, 5.0, 10.0]:
                for lambda_latency in [0.5, 1.0, 3.0, 5.0, 10.0]:
                    
                    # Create config
                    config = RewardConfig(
                        mode=ExecutionMode.CUSTOM,
                        lambda_energy=lambda_energy,
                        lambda_carbon=lambda_carbon,
                        lambda_latency=lambda_latency
                    )
                    
                    # Compute rewards with this config
                    temp_shaper = RewardShaper(ExecutionMode.CUSTOM, config)
                    rewards = temp_shaper.compute_batch_rewards(training_data)
                    
                    # Compute correlation with target
                    reward_values = [r['reward'] for r in rewards]
                    target_values = [d[target_metric] for d in training_data]
                    
                    correlation = np.corrcoef(reward_values, target_values)[0, 1]
                    
                    if correlation > best_score:
                        best_score = correlation
                        best_config = config
        
        logger.info(f"Optimized lambda values: "
                   f"energy={best_config.lambda_energy}, "
                   f"carbon={best_config.lambda_carbon}, "
                   f"latency={best_config.lambda_latency}")
        
        return best_config
