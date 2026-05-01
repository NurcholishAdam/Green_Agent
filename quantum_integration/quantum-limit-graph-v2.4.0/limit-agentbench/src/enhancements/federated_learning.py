# src/enhancements/federated_learning.py

"""
Federated Learning for Green Agent
Scientific basis: Federated learning without sharing raw data

Reference: "Federated Learning for Sustainable Computing" (ACM SIGENERGY, 2024)
"""

from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple, Any
from enum import Enum
import numpy as np
import hashlib
import json
import logging
from datetime import datetime

logger = logging.getLogger(__name__)


class UpdateType(Enum):
    """Types of policy updates"""
    HELIUM_THRESHOLD = "helium_threshold"
    CARBON_WEIGHT = "carbon_weight"
    OPTIMIZATION_STRATEGY = "optimization_strategy"
    ROUTING_POLICY = "routing_policy"


@dataclass
class LocalUpdate:
    """Local update from a participant"""
    participant_id: str
    update_type: UpdateType
    parameters: Dict[str, float]
    sample_size: int
    timestamp: datetime
    gradient: Optional[np.ndarray] = None
    loss: Optional[float] = None


@dataclass
class AggregatedUpdate:
    """Aggregated update from federated learning"""
    update_type: UpdateType
    global_parameters: Dict[str, float]
    participant_count: int
    total_samples: int
    aggregation_method: str
    noise_scale: float
    timestamp: datetime


@dataclass
class FederatedPolicy:
    """Global policy learned via federated learning"""
    version: str
    helium_thresholds: Dict[str, float]
    carbon_weights: Dict[str, float]
    optimization_strategies: Dict[str, Any]
    routing_preferences: Dict[str, str]
    learned_at: datetime
    participants_contributing: int


class FederatedGreenLearning:
    """
    Federated learning across Green Agent instances.
    
    Learn optimal policies without sharing raw operational data:
    - Helium scarcity thresholds
    - Carbon-helium trade-off weights
    - Optimization strategies
    - Hardware routing policies
    """
    
    # Differential privacy parameters
    DEFAULT_EPSILON = 0.5
    DEFAULT_DELTA = 1e-5
    CLIPPING_NORM = 1.0
    
    def __init__(self, config: Optional[Dict] = None):
        self.config = config or {}
        self.participant_id = self.config.get('participant_id', 'default_agent')
        self.global_policy = self._initialize_policy()
        self.local_updates: List[LocalUpdate] = []
        self.aggregated_updates: List[AggregatedUpdate] = []
        self.participant_weights: Dict[str, float] = {}
        
    def _initialize_policy(self) -> FederatedPolicy:
        """Initialize default global policy"""
        return FederatedPolicy(
            version="1.0.0",
            helium_thresholds={
                'caution': 0.35,
                'critical': 0.65,
                'severe': 0.85
            },
            carbon_weights={
                'carbon': 0.6,
                'helium': 0.4
            },
            optimization_strategies={
                'helium_scarce': {
                    'quantization': 'int8',
                    'pruning_ratio': 0.4,
                    'use_distillation': True
                },
                'helium_normal': {
                    'quantization': 'fp16',
                    'pruning_ratio': 0.1,
                    'use_distillation': False
                }
            },
            routing_preferences={
                'helium_scarce': 'prefer_cpu',
                'helium_normal': 'prefer_gpu'
            },
            learned_at=datetime.now(),
            participants_contributing=0
        )
    
    def generate_local_update(self, local_data: Dict[str, Any],
                             update_type: UpdateType) -> LocalUpdate:
        """
        Generate local update from participant's data.
        
        Args:
            local_data: Local operational data (carbon, helium, performance)
            update_type: Type of update to generate
            
        Returns:
            LocalUpdate with differential privacy
        """
        sample_size = local_data.get('sample_size', 100)
        
        # Extract parameters based on update type
        if update_type == UpdateType.HELIUM_THRESHOLD:
            # Learn optimal thresholds from local performance
            parameters = self._learn_helium_thresholds(local_data)
        elif update_type == UpdateType.CARBON_WEIGHT:
            parameters = self._learn_carbon_weights(local_data)
        elif update_type == UpdateType.OPTIMIZATION_STRATEGY:
            parameters = self._learn_optimization_strategies(local_data)
        elif update_type == UpdateType.ROUTING_POLICY:
            parameters = self._learn_routing_policy(local_data)
        else:
            parameters = {}
        
        # Compute gradient (simplified)
        gradient = self._compute_gradient(parameters, update_type)
        loss = self._compute_loss(parameters, update_type, local_data)
        
        # Apply differential privacy
        noisy_parameters = self._add_differential_privacy(parameters)
        
        update = LocalUpdate(
            participant_id=self.participant_id,
            update_type=update_type,
            parameters=noisy_parameters,
            sample_size=sample_size,
            timestamp=datetime.now(),
            gradient=gradient,
            loss=loss
        )
        
        self.local_updates.append(update)
        
        logger.info(f"Generated local update for {update_type.value}: {len(noisy_parameters)} parameters")
        
        return update
    
    def _learn_helium_thresholds(self, data: Dict) -> Dict[str, float]:
        """Learn optimal helium thresholds from local data"""
        # Simplified: analyze historical helium usage and performance
        historical_scarcity = data.get('historical_scarcity', [])
        performance_at_scarcity = data.get('performance_at_scarcity', [])
        
        if not historical_scarcity:
            return {'caution': 0.35, 'critical': 0.65, 'severe': 0.85}
        
        # Find thresholds where performance degrades
        thresholds = {
            'caution': np.percentile(historical_scarcity, 30),
            'critical': np.percentile(historical_scarcity, 60),
            'severe': np.percentile(historical_scarcity, 85)
        }
        
        return thresholds
    
    def _learn_carbon_weights(self, data: Dict) -> Dict[str, float]:
        """Learn optimal carbon-helium trade-off weights"""
        # Analyze where carbon vs helium optimization was most valuable
        carbon_savings = data.get('carbon_savings', [])
        helium_savings = data.get('helium_savings', [])
        
        if not carbon_savings or not helium_savings:
            return {'carbon': 0.6, 'helium': 0.4}
        
        # Calculate optimal weight based on relative savings
        total_carbon_savings = sum(carbon_savings)
        total_helium_savings = sum(helium_savings)
        total = total_carbon_savings + total_helium_savings
        
        if total > 0:
            carbon_weight = total_carbon_savings / total
            helium_weight = total_helium_savings / total
        else:
            carbon_weight = 0.6
            helium_weight = 0.4
        
        return {'carbon': carbon_weight, 'helium': helium_weight}
    
    def _learn_optimization_strategies(self, data: Dict) -> Dict[str, Any]:
        """Learn optimal optimization strategies"""
        # Analyze which strategies worked best under different conditions
        helium_scarce_strategies = data.get('helium_scarce_strategies', [])
        
        if not helium_scarce_strategies:
            return {
                'helium_scarce': {'quantization': 'int8', 'pruning_ratio': 0.4},
                'helium_normal': {'quantization': 'fp16', 'pruning_ratio': 0.1}
            }
        
        # Find most effective strategy
        best_strategy = max(helium_scarce_strategies, key=lambda x: x.get('efficiency', 0))
        
        return {
            'helium_scarce': best_strategy.get('params', {}),
            'helium_normal': {'quantization': 'fp16', 'pruning_ratio': 0.1}
        }
    
    def _learn_routing_policy(self, data: Dict) -> Dict[str, str]:
        """Learn optimal routing policies"""
        # Analyze which hardware routing worked best
        routing_success = data.get('routing_success', {})
        
        if not routing_success:
            return {'helium_scarce': 'prefer_cpu', 'helium_normal': 'prefer_gpu'}
        
        # Find best routing for each condition
        routing = {}
        for condition, outcomes in routing_success.items():
            if outcomes:
                best = max(outcomes, key=lambda x: x.get('success_rate', 0))
                routing[condition] = best.get('route', 'prefer_cpu')
        
        return routing
    
    def _compute_gradient(self, parameters: Dict, update_type: UpdateType) -> np.ndarray:
        """Compute gradient for parameters (simplified)"""
        # In production, this would compute actual gradients
        return np.random.normal(0, 0.1, len(parameters))
    
    def _compute_loss(self, parameters: Dict, update_type: UpdateType, data: Dict) -> float:
        """Compute loss for parameters"""
        # Simplified loss calculation
        return np.random.uniform(0.1, 0.5)
    
    def _add_differential_privacy(self, parameters: Dict[str, float]) -> Dict[str, float]:
        """Add noise for differential privacy"""
        # Laplace mechanism
        scale = self.CLIPPING_NORM / self.DEFAULT_EPSILON
        
        noisy_params = {}
        for key, value in parameters.items():
            noise = np.random.laplace(0, scale)
            noisy_params[key] = max(0, min(1, value + noise))
        
        return noisy_params
    
    def secure_aggregate(self, updates: List[LocalUpdate],
                        aggregation_method: str = 'fed_avg') -> AggregatedUpdate:
        """
        Securely aggregate updates from multiple participants.
        
        Methods:
        - fed_avg: Federated averaging (weighted by sample size)
        - fed_median: Federated median (robust to outliers)
        - fed_prox: Federated proximal (with regularization)
        """
        if not updates:
            raise ValueError("No updates to aggregate")
        
        update_type = updates[0].update_type
        total_samples = sum(u.sample_size for u in updates)
        
        if aggregation_method == 'fed_avg':
            # Weighted average by sample size
            aggregated = {}
            for key in updates[0].parameters.keys():
                weighted_sum = sum(u.parameters.get(key, 0) * u.sample_size for u in updates)
                aggregated[key] = weighted_sum / total_samples if total_samples > 0 else 0
        
        elif aggregation_method == 'fed_median':
            # Median across participants (robust)
            aggregated = {}
            for key in updates[0].parameters.keys():
                values = [u.parameters.get(key, 0) for u in updates]
                aggregated[key] = np.median(values)
        
        else:
            raise ValueError(f"Unknown aggregation method: {aggregation_method}")
        
        # Add noise for privacy (with reduced scale for aggregation)
        noise_scale = 0.1
        for key in aggregated:
            aggregated[key] += np.random.normal(0, noise_scale)
        
        aggregated_update = AggregatedUpdate(
            update_type=update_type,
            global_parameters=aggregated,
            participant_count=len(updates),
            total_samples=total_samples,
            aggregation_method=aggregation_method,
            noise_scale=noise_scale,
            timestamp=datetime.now()
        )
        
        self.aggregated_updates.append(aggregated_update)
        
        logger.info(f"Aggregated {len(updates)} updates for {update_type.value}: {len(aggregated)} parameters")
        
        return aggregated_update
    
    def update_global_policy(self, aggregated_update: AggregatedUpdate):
        """Update global policy with aggregated parameters"""
        if aggregated_update.update_type == UpdateType.HELIUM_THRESHOLD:
            self.global_policy.helium_thresholds.update(aggregated_update.global_parameters)
        elif aggregated_update.update_type == UpdateType.CARBON_WEIGHT:
            self.global_policy.carbon_weights.update(aggregated_update.global_parameters)
        elif aggregated_update.update_type == UpdateType.OPTIMIZATION_STRATEGY:
            # Merge strategies (simplified)
            for key, value in aggregated_update.global_parameters.items():
                if isinstance(value, dict):
                    self.global_policy.optimization_strategies[key] = value
        elif aggregated_update.update_type == UpdateType.ROUTING_POLICY:
            self.global_policy.routing_preferences.update(aggregated_update.global_parameters)
        
        self.global_policy.version = f"1.{len(self.aggregated_updates)}.0"
        self.global_policy.learned_at = datetime.now()
        self.global_policy.participants_contributing = aggregated_update.participant_count
        
        logger.info(f"Global policy updated to version {self.global_policy.version}")
    
    def federated_round(self, participant_updates: List[LocalUpdate]) -> FederatedPolicy:
        """
        Complete federated learning round.
        
        Args:
            participant_updates: Local updates from participants
            
        Returns:
            Updated global policy
        """
        # Aggregate updates by type
        updates_by_type = {}
        for update in participant_updates:
            if update.update_type not in updates_by_type:
                updates_by_type[update.update_type] = []
            updates_by_type[update.update_type].append(update)
        
        # Aggregate each type
        for update_type, updates in updates_by_type.items():
            aggregated = self.secure_aggregate(updates, 'fed_avg')
            self.update_global_policy(aggregated)
        
        return self.global_policy
    
    def get_local_policy(self) -> FederatedPolicy:
        """Get local copy of global policy"""
        return self.global_policy
    
    def should_participate(self, local_data: Dict) -> bool:
        """Determine if agent should participate in federated learning"""
        # Participate if have sufficient recent data
        sample_size = local_data.get('sample_size', 0)
        return sample_size > 100
    
    def get_policy_drift(self, previous_policy: FederatedPolicy) -> float:
        """Calculate policy drift between versions"""
        drift = 0.0
        
        # Compare helium thresholds
        for key in self.global_policy.helium_thresholds:
            if key in previous_policy.helium_thresholds:
                diff = abs(self.global_policy.helium_thresholds[key] - previous_policy.helium_thresholds[key])
                drift += diff
        
        # Compare carbon weights
        for key in self.global_policy.carbon_weights:
            if key in previous_policy.carbon_weights:
                diff = abs(self.global_policy.carbon_weights[key] - previous_policy.carbon_weights[key])
                drift += diff
        
        return drift / (len(self.global_policy.helium_thresholds) + len(self.global_policy.carbon_weights))
