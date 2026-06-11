# File: enhancements/moe_expert_system/advanced/self_evolving_gates.py

"""
Self-Evolving Gating Network for Green Agent MoE
Implements continuous learning and adaptation of routing decisions
based on environmental feedback and performance metrics.
"""

import numpy as np
from typing import Dict, Any, List, Optional, Tuple
from collections import deque
import logging
from datetime import datetime
import torch
import torch.nn as nn

logger = logging.getLogger(__name__)

class SelfEvolvingGate(nn.Module):
    """
    Gating network that evolves based on environmental feedback.
    
    Features:
    - Online learning from routing outcomes
    - Environmental awareness adaptation
    - Concept drift detection
    - Multi-timescale memory
    """
    
    def __init__(
        self,
        input_dim: int,
        num_experts: int,
        memory_size: int = 10000,
        adaptation_rate: float = 0.01
    ):
        super().__init__()
        self.num_experts = num_experts
        self.adaptation_rate = adaptation_rate
        
        # Core gating network
        self.gate_network = nn.Sequential(
            nn.Linear(input_dim, 256),
            nn.LayerNorm(256),
            nn.ReLU(),
            nn.Dropout(0.1),
            nn.Linear(256, 128),
            nn.LayerNorm(128),
            nn.ReLU(),
            nn.Dropout(0.1),
            nn.Linear(128, num_experts)
        )
        
        # Evolutionary components
        self.memory = deque(maxlen=memory_size)
        self.concept_drift_detector = ConceptDriftDetector()
        self.environmental_encoder = EnvironmentalEncoder(input_dim)
        
        # Performance tracking
        self.performance_history = []
        self.adaptation_history = []
        
        # Initialize optimizer for online learning
        self.optimizer = torch.optim.Adam(
            self.gate_network.parameters(),
            lr=adaptation_rate
        )
    
    def forward(
        self,
        x: torch.Tensor,
        environmental_context: Optional[Dict[str, Any]] = None
    ) -> Tuple[torch.Tensor, Dict[str, Any]]:
        """
        Forward pass with environmental adaptation
        
        Args:
            x: Input features
            environmental_context: Environmental state for adaptation
            
        Returns:
            routing_weights, adaptation_metadata
        """
        # Check for concept drift
        drift_detected = self.concept_drift_detector.check_drift(x)
        
        # Encode environmental context if available
        if environmental_context:
            env_features = self.environmental_encoder(environmental_context)
            x = torch.cat([x, env_features], dim=-1)
        
        # Get routing decisions
        logits = self.gate_network(x)
        routing_weights = torch.softmax(logits, dim=-1)
        
        # Adaptation metadata
        metadata = {
            'drift_detected': drift_detected,
            'environmental_adaptation': environmental_context is not None,
            'gate_entropy': self._calculate_entropy(routing_weights)
        }
        
        return routing_weights, metadata
    
    def adapt(
        self,
        state: torch.Tensor,
        chosen_expert: int,
        reward: float,
        environmental_feedback: Dict[str, Any]
    ):
        """
        Adapt gating network based on feedback
        
        Args:
            state: Input state when decision was made
            chosen_expert: Index of chosen expert
            reward: Reward signal from environment
            environmental_feedback: Environmental metrics
        """
        # Store experience in memory
        self.memory.append({
            'state': state.detach().clone(),
            'action': chosen_expert,
            'reward': reward,
            'environmental': environmental_feedback
        })
        
        # Online learning from recent experiences
        if len(self.memory) >= 32:  # Mini-batch size
            self._learn_from_experiences()
        
        # Check if structural adaptation is needed
        if self.concept_drift_detector.should_adapt():
            self._structural_adaptation(environmental_feedback)
        
        # Record adaptation
        self.adaptation_history.append({
            'timestamp': datetime.utcnow().isoformat(),
            'reward': reward,
            'expert': chosen_expert,
            'drift': self.concept_drift_detector.drift_score
        })
    
    def _learn_from_experiences(self):
        """Learn from stored experiences using reinforcement learning"""
        if len(self.memory) < 32:
            return
        
        # Sample batch from memory
        batch_size = min(32, len(self.memory))
        indices = np.random.choice(len(self.memory), batch_size, replace=False)
        batch = [self.memory[i] for i in indices]
        
        # Prepare batch
        states = torch.stack([b['state'] for b in batch])
        actions = torch.tensor([b['action'] for b in batch])
        rewards = torch.tensor([b['reward'] for b in batch])
        
        # Get current routing probabilities
        logits = self.gate_network(states)
        probs = torch.softmax(logits, dim=-1)
        
        # Calculate policy gradient loss
        action_probs = probs[range(batch_size), actions]
        loss = -torch.mean(torch.log(action_probs + 1e-8) * rewards)
        
        # Backpropagate
        self.optimizer.zero_grad()
        loss.backward()
        torch.nn.utils.clip_grad_norm_(self.gate_network.parameters(), 1.0)
        self.optimizer.step()
        
        self.performance_history.append({
            'loss': loss.item(),
            'avg_reward': rewards.mean().item()
        })
    
    def _structural_adaptation(
        self,
        environmental_feedback: Dict[str, Any]
    ):
        """
        Perform structural adaptation when concept drift is detected.
        
        This could involve:
        - Adding/removing neurons
        - Adjusting network architecture
        - Resetting certain layers
        """
        carbon_zone = environmental_feedback.get('carbon_zone', 0)
        helium_scarcity = environmental_feedback.get('helium_scarcity', 0)
        
        if carbon_zone >= 12 or helium_scarcity > 0.8:
            # In critical conditions, simplify network to save resources
            logger.info("Structural adaptation: Simplifying network for efficiency")
            self._simplify_network()
        elif carbon_zone <= 4 and helium_scarcity < 0.3:
            # In favorable conditions, can afford more complex network
            logger.info("Structural adaptation: Enhancing network for accuracy")
            self._enhance_network()
    
    def _simplify_network(self):
        """Simplify network architecture for efficiency"""
        # Increase dropout to reduce computation
        for module in self.gate_network:
            if isinstance(module, nn.Dropout):
                module.p = min(module.p + 0.1, 0.5)
    
    def _enhance_network(self):
        """Enhance network architecture for accuracy"""
        # Decrease dropout for better accuracy
        for module in self.gate_network:
            if isinstance(module, nn.Dropout):
                module.p = max(module.p - 0.05, 0.05)
    
    def _calculate_entropy(self, probs: torch.Tensor) -> float:
        """Calculate entropy of routing distribution"""
        entropy = -torch.sum(probs * torch.log(probs + 1e-8), dim=-1)
        return entropy.mean().item()
    
    def get_evolution_metrics(self) -> Dict[str, Any]:
        """Get metrics about gate evolution"""
        return {
            'adaptation_count': len(self.adaptation_history),
            'drift_score': self.concept_drift_detector.drift_score,
            'recent_rewards': [
                h['reward'] for h in self.adaptation_history[-100:]
            ],
            'entropy_history': [
                h.get('entropy', 0) for h in self.adaptation_history[-100:]
            ]
        }


class ConceptDriftDetector:
    """Detects concept drift in input distribution"""
    
    def __init__(self, window_size: int = 100, threshold: float = 0.1):
        self.window_size = window_size
        self.threshold = threshold
        self.reference_window = deque(maxlen=window_size)
        self.current_window = deque(maxlen=window_size)
        self.drift_score = 0.0
    
    def check_drift(self, x: torch.Tensor) -> bool:
        """Check for concept drift in input data"""
        # Add to current window
        self.current_window.append(x.detach().mean(dim=0))
        
        if len(self.reference_window) < self.window_size:
            # Build reference window
            self.reference_window.append(x.detach().mean(dim=0))
            return False
        
        # Calculate distribution difference
        ref_mean = torch.stack(list(self.reference_window)).mean(dim=0)
        curr_mean = torch.stack(list(self.current_window)).mean(dim=0)
        
        self.drift_score = torch.norm(curr_mean - ref_mean).item()
        
        drift_detected = self.drift_score > self.threshold
        
        if drift_detected:
            # Update reference window
            self.reference_window = deque(
                list(self.current_window)[-self.window_size:],
                maxlen=self.window_size
            )
        
        return drift_detected
    
    def should_adapt(self) -> bool:
        """Determine if structural adaptation is needed"""
        return self.drift_score > self.threshold * 2


class EnvironmentalEncoder:
    """Encodes environmental context for gate adaptation"""
    
    def __init__(self, output_dim: int):
        self.output_dim = output_dim
        
        # Environmental feature extractor
        self.encoder = nn.Sequential(
            nn.Linear(10, 32),  # 10 environmental features
            nn.ReLU(),
            nn.Linear(32, output_dim)
        )
    
    def __call__(self, context: Dict[str, Any]) -> torch.Tensor:
        """Encode environmental context into feature vector"""
        features = [
            context.get('carbon_zone', 0) / 15.0,
            context.get('helium_scarcity', 0),
            context.get('grid_intensity', 400) / 1000.0,
            context.get('temperature', 20) / 50.0,
            context.get('renewable_percentage', 0.3),
            context.get('time_of_day', 12) / 24.0,
            context.get('day_of_week', 0) / 7.0,
            context.get('network_load', 0.5),
            context.get('storage_level', 0.8),
            context.get('backup_power', 0.0)
        ]
        
        x = torch.tensor(features, dtype=torch.float32)
        return self.encoder(x)
