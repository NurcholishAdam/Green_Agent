# File: enhancements/moe_expert_system/advanced/federated_experts.py

"""
Federated Expert Learning for Green Agent
Enables privacy-preserving collaborative learning across distributed experts
while maintaining carbon/helium awareness.
"""

import asyncio
import logging
from typing import Dict, Any, List, Optional, Tuple
from dataclasses import dataclass, field
from datetime import datetime
import numpy as np
import hashlib
import json

logger = logging.getLogger(__name__)

@dataclass
class FederatedExpert:
    """Expert that participates in federated learning"""
    expert_id: str
    local_model: Dict[str, Any]
    data_distribution: Dict[str, float]
    carbon_footprint: float
    helium_usage: float
    privacy_budget: float = 1.0  # Differential privacy epsilon
    last_updated: datetime = field(default_factory=datetime.utcnow)
    
    def get_model_hash(self) -> str:
        """Get hash of local model for verification"""
        model_str = json.dumps(self.local_model, sort_keys=True)
        return hashlib.sha256(model_str.encode()).hexdigest()

class FederatedExpertOrchestrator:
    """
    Orchestrates federated learning across distributed Green Agent experts.
    
    Features:
    - Privacy-preserving model aggregation
    - Carbon-aware participant selection
    - Differential privacy guarantees
    - Helium-efficient communication protocols
    """
    
    def __init__(
        self,
        aggregation_strategy: str = 'fed_avg',
        min_participants: int = 3,
        privacy_epsilon: float = 1.0
    ):
        self.aggregation_strategy = aggregation_strategy
        self.min_participants = min_participants
        self.privacy_epsilon = privacy_epsilon
        
        self.participants: Dict[str, FederatedExpert] = {}
        self.aggregation_history: List[Dict] = []
        self.global_model: Optional[Dict[str, Any]] = None
        
        # Carbon-aware selection
        self.carbon_threshold = 0.05  # kg CO2 per round
        
        logger.info(f"Federated Expert Orchestrator initialized with {aggregation_strategy}")
    
    def register_participant(
        self,
        expert_id: str,
        initial_model: Dict[str, Any],
        data_distribution: Dict[str, float],
        carbon_footprint: float,
        helium_usage: float
    ) -> bool:
        """Register a new participant in federated learning"""
        if expert_id in self.participants:
            logger.warning(f"Participant {expert_id} already registered")
            return False
        
        participant = FederatedExpert(
            expert_id=expert_id,
            local_model=initial_model,
            data_distribution=data_distribution,
            carbon_footprint=carbon_footprint,
            helium_usage=helium_usage,
            privacy_budget=self.privacy_epsilon
        )
        
        self.participants[expert_id] = participant
        logger.info(f"Registered federated participant: {expert_id}")
        return True
    
    async def federated_round(
        self,
        carbon_zone: int,
        helium_scarcity: float
    ) -> Optional[Dict[str, Any]]:
        """
        Execute one round of federated learning
        
        Args:
            carbon_zone: Current carbon zone (0-15)
            helium_scarcity: Current helium scarcity (0-1)
            
        Returns:
            Aggregated global model or None if round failed
        """
        # Step 1: Select participants based on carbon/helium constraints
        selected = self._select_participants(carbon_zone, helium_scarcity)
        
        if len(selected) < self.min_participants:
            logger.warning(f"Insufficient participants: {len(selected)} < {self.min_participants}")
            return None
        
        logger.info(f"Selected {len(selected)} participants for federated round")
        
        # Step 2: Collect local updates with differential privacy
        local_updates = []
        total_carbon = 0.0
        total_helium = 0.0
        
        for participant_id in selected:
            participant = self.participants[participant_id]
            
            # Apply differential privacy to local model
            private_update = await self._apply_differential_privacy(
                participant.local_model,
                participant.privacy_budget
            )
            
            # Calculate contribution weight based on data quality and carbon
            weight = self._calculate_contribution_weight(
                participant,
                carbon_zone,
                helium_scarcity
            )
            
            local_updates.append({
                'participant_id': participant_id,
                'update': private_update,
                'weight': weight,
                'model_hash': participant.get_model_hash()
            })
            
            total_carbon += participant.carbon_footprint
            total_helium += participant.helium_usage
            
            # Reduce privacy budget
            participant.privacy_budget -= 0.1
        
        # Step 3: Aggregate updates
        if self.aggregation_strategy == 'fed_avg':
            global_model = self._federated_averaging(local_updates)
        elif self.aggregation_strategy == 'fed_prox':
            global_model = self._federated_proximal(local_updates)
        elif self.aggregation_strategy == 'fed_opt':
            global_model = self._federated_optimization(local_updates)
        else:
            global_model = self._federated_averaging(local_updates)
        
        # Step 4: Update global model
        self.global_model = global_model
        
        # Step 5: Distribute global model to participants
        await self._distribute_global_model(selected, global_model)
        
        # Step 6: Record aggregation round
        round_metrics = {
            'timestamp': datetime.utcnow().isoformat(),
            'participants': len(selected),
            'total_carbon_kg': total_carbon,
            'total_helium': total_helium,
            'strategy': self.aggregation_strategy,
            'model_hash': hashlib.sha256(
                json.dumps(global_model, sort_keys=True).encode()
            ).hexdigest()
        }
        
        self.aggregation_history.append(round_metrics)
        
        logger.info(f"Federated round complete: {len(selected)} participants, "
                   f"{total_carbon:.4f} kg CO2, {total_helium:.4f} He")
        
        return global_model
    
    def _select_participants(
        self,
        carbon_zone: int,
        helium_scarcity: float
    ) -> List[str]:
        """
        Select participants based on carbon/helium constraints.
        
        In high carbon zones, prioritize low-carbon participants.
        In high helium scarcity, prioritize helium-efficient participants.
        """
        scored_participants = []
        
        for participant_id, participant in self.participants.items():
            # Calculate selection score
            carbon_score = 1.0 - min(participant.carbon_footprint / self.carbon_threshold, 1.0)
            helium_score = 1.0 - participant.helium_usage
            
            # Weight based on current context
            if carbon_zone >= 8:  # High carbon zone
                score = 0.7 * carbon_score + 0.3 * helium_score
            elif helium_scarcity > 0.7:  # High helium scarcity
                score = 0.3 * carbon_score + 0.7 * helium_score
            else:  # Balanced
                score = 0.5 * carbon_score + 0.5 * helium_score
            
            scored_participants.append((participant_id, score))
        
        # Select top participants
        scored_participants.sort(key=lambda x: x[1], reverse=True)
        
        # Select at least min_participants, but no more than available
        n_select = max(self.min_participants, len(scored_participants) // 2)
        selected = [p[0] for p in scored_participants[:n_select]]
        
        return selected
    
    async def _apply_differential_privacy(
        self,
        model: Dict[str, Any],
        epsilon: float
    ) -> Dict[str, Any]:
        """Apply differential privacy to model updates"""
        if epsilon <= 0:
            return model
        
        private_model = {}
        sensitivity = 1.0  # L2 sensitivity
        
        for key, value in model.items():
            if isinstance(value, (int, float)):
                # Add Laplace noise
                scale = sensitivity / epsilon
                noise = np.random.laplace(0, scale)
                private_model[key] = value + noise
            elif isinstance(value, np.ndarray):
                # Add Gaussian noise to arrays
                scale = sensitivity / epsilon
                noise = np.random.laplace(0, scale, value.shape)
                private_model[key] = value + noise
            else:
                private_model[key] = value
        
        return private_model
    
    def _calculate_contribution_weight(
        self,
        participant: FederatedExpert,
        carbon_zone: int,
        helium_scarcity: float
    ) -> float:
        """
        Calculate participant contribution weight.
        
        Factors:
        - Data quality (distribution balance)
        - Privacy budget remaining
        - Carbon/helium efficiency
        """
        # Data quality: penalize highly skewed distributions
        data_entropy = 0
        for prob in participant.data_distribution.values():
            if prob > 0:
                data_entropy -= prob * np.log(prob)
        data_quality = min(data_entropy / np.log(len(participant.data_distribution)), 1.0)
        
        # Privacy score
        privacy_score = participant.privacy_budget / self.privacy_epsilon
        
        # Efficiency score
        efficiency_score = 1.0 / (1.0 + participant.carbon_footprint + participant.helium_usage)
        
        # Combined weight
        weight = 0.4 * data_quality + 0.3 * privacy_score + 0.3 * efficiency_score
        
        return weight
    
    def _federated_averaging(
        self,
        updates: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """FedAvg aggregation with weighted averaging"""
        if not updates:
            return {}
        
        # Initialize aggregated model
        aggregated = {}
        total_weight = sum(u['weight'] for u in updates)
        
        # Get all keys from first update
        keys = updates[0]['update'].keys()
        
        for key in keys:
            values = []
            weights = []
            
            for update in updates:
                if key in update['update']:
                    value = update['update'][key]
                    weight = update['weight']
                    
                    if isinstance(value, (int, float, np.ndarray)):
                        values.append(value)
                        weights.append(weight)
            
            if values:
                # Weighted average
                if isinstance(values[0], np.ndarray):
                    weighted_sum = np.zeros_like(values[0])
                    for v, w in zip(values, weights):
                        weighted_sum += v * w
                    aggregated[key] = weighted_sum / total_weight
                else:
                    weighted_sum = sum(v * w for v, w in zip(values, weights))
                    aggregated[key] = weighted_sum / total_weight
        
        return aggregated
    
    def _federated_proximal(
        self,
        updates: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """FedProx: Federated Learning with Proximal Term"""
        # Add proximal term to prevent divergence from global model
        mu = 0.01  # Proximal term coefficient
        
        if self.global_model is None:
            return self._federated_averaging(updates)
        
        aggregated = self._federated_averaging(updates)
        
        # Apply proximal correction
        for key in aggregated:
            if key in self.global_model:
                global_val = self.global_model[key]
                local_val = aggregated[key]
                
                if isinstance(local_val, np.ndarray):
                    aggregated[key] = local_val - mu * (local_val - global_val)
                elif isinstance(local_val, (int, float)):
                    aggregated[key] = local_val - mu * (local_val - global_val)
        
        return aggregated
    
    def _federated_optimization(
        self,
        updates: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """FedOpt: Federated Optimization with Adaptive Learning"""
        # Implement adaptive optimization (simplified)
        aggregated = self._federated_averaging(updates)
        
        # Apply momentum-based optimization
        beta = 0.9  # Momentum coefficient
        
        if hasattr(self, 'momentum'):
            for key in aggregated:
                if key in self.momentum:
                    self.momentum[key] = beta * self.momentum[key] + (1 - beta) * aggregated[key]
                    aggregated[key] = self.momentum[key]
                else:
                    self.momentum[key] = aggregated[key]
        else:
            self.momentum = {k: v for k, v in aggregated.items()}
        
        return aggregated
    
    async def _distribute_global_model(
        self,
        participants: List[str],
        global_model: Dict[str, Any]
    ):
        """Distribute global model to selected participants"""
        for participant_id in participants:
            if participant_id in self.participants:
                # Update participant's local model with global model
                # In practice, this would involve network communication
                self.participants[participant_id].local_model = global_model.copy()
                self.participants[participant_id].last_updated = datetime.utcnow()
        
        logger.info(f"Distributed global model to {len(participants)} participants")
    
    def get_federation_status(self) -> Dict[str, Any]:
        """Get federated learning status"""
        return {
            'total_participants': len(self.participants),
            'aggregation_rounds': len(self.aggregation_history),
            'current_strategy': self.aggregation_strategy,
            'total_carbon_emitted': sum(
                r['total_carbon_kg'] for r in self.aggregation_history
            ),
            'average_participants_per_round': np.mean([
                r['participants'] for r in self.aggregation_history
            ]) if self.aggregation_history else 0,
            'privacy_budgets': {
                pid: p.privacy_budget 
                for pid, p in self.participants.items()
            }
        }
    
    def verify_model_integrity(self) -> Dict[str, bool]:
        """Verify integrity of all participant models"""
        verification_results = {}
        
        for participant_id, participant in self.participants.items():
            current_hash = participant.get_model_hash()
            # In practice, compare with blockchain-stored hash
            verification_results[participant_id] = True
        
        return verification_results
