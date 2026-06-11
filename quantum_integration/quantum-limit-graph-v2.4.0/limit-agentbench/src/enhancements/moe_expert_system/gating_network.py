# File: enhancements/moe_expert_system/gating_network.py

import numpy as np
from typing import Dict, List, Tuple, Optional
import torch
import torch.nn as nn
import torch.nn.functional as F
import logging
from dataclasses import dataclass

logger = logging.getLogger(__name__)

@dataclass
class GatingContext:
    """Context features for the gating network"""
    # Layer 0: Workload features
    task_type: str
    task_complexity: float
    input_size_mb: float
    
    # Layer 1: Meta-cognitive state
    carbon_budget_remaining: float
    helium_budget_remaining: float
    latency_budget_ms: float
    historical_success_rate: float
    
    # Layer 3: Dual-axis features
    carbon_zone: int  # 0-15
    helium_scarcity: float  # 0.0-1.0
    
    # Additional context
    time_of_day: int  # 0-23
    grid_carbon_intensity: float  # gCO2/kWh
    hardware_availability: Dict[str, float]  # hardware_type -> availability_score
    
    def to_tensor(self) -> torch.Tensor:
        """Convert context to tensor for gating network"""
        # Encode categorical features
        task_encoding = {
            'inference': 0,
            'training': 1,
            'data_processing': 2,
            'optimization': 3,
            'simulation': 4
        }.get(self.task_type, 5)
        
        # Normalize and encode hardware availability
        hw_features = [
            self.hardware_availability.get('cpu', 0.0),
            self.hardware_availability.get('gpu', 0.0),
            self.hardware_availability.get('quantum', 0.0),
            self.hardware_availability.get('edge', 0.0)
        ]
        
        # Create feature vector
        features = [
            task_encoding / 5.0,  # Normalize to [0,1]
            self.task_complexity,
            np.log1p(self.input_size_mb) / 10.0,  # Log-normalize
            self.carbon_budget_remaining,
            self.helium_budget_remaining,
            self.latency_budget_ms / 10000.0,  # Normalize assuming max 10s
            self.historical_success_rate,
            self.carbon_zone / 15.0,
            self.helium_scarcity,
            self.time_of_day / 23.0,
            self.grid_carbon_intensity / 1000.0,  # Normalize assuming max 1000 gCO2/kWh
            *hw_features
        ]
        
        return torch.tensor(features, dtype=torch.float32)

class SparseMoEGate(nn.Module):
    """
    Sparse Mixture of Experts Gating Network
    Implements top-k routing with load balancing
    """
    
    def __init__(
        self,
        input_dim: int,
        num_experts: int,
        top_k: int = 2,
        capacity_factor: float = 1.25,
        noise_std: float = 0.1
    ):
        super().__init__()
        self.num_experts = num_experts
        self.top_k = top_k
        self.capacity_factor = capacity_factor
        
        # Gating layers
        self.gate = nn.Sequential(
            nn.Linear(input_dim, 128),
            nn.LayerNorm(128),
            nn.ReLU(),
            nn.Dropout(0.1),
            nn.Linear(128, 64),
            nn.LayerNorm(64),
            nn.ReLU(),
            nn.Dropout(0.1),
            nn.Linear(64, num_experts)
        )
        
        # Noise for exploration
        self.noise_std = noise_std
        
        # Load balancing loss weight
        self.load_balance_weight = 0.01
        
        # Initialize weights
        self._init_weights()
    
    def _init_weights(self):
        """Initialize network weights"""
        for module in self.gate:
            if isinstance(module, nn.Linear):
                nn.init.xavier_uniform_(module.weight)
                nn.init.zeros_(module.bias)
    
    def forward(
        self,
        x: torch.Tensor,
        training: bool = False
    ) -> Tuple[torch.Tensor, torch.Tensor, torch.Tensor]:
        """
        Forward pass with sparse routing
        
        Returns:
            routing_weights: weights for selected experts
            expert_indices: indices of selected experts
            load_balance_loss: auxiliary loss for load balancing
        """
        # Compute logits
        logits = self.gate(x)
        
        # Add noise during training for exploration
        if training:
            noise = torch.randn_like(logits) * self.noise_std
            logits = logits + noise
        
        # Get top-k experts
        top_k_logits, top_k_indices = torch.topk(logits, self.top_k, dim=-1)
        
        # Apply softmax to get routing weights
        routing_weights = F.softmax(top_k_logits, dim=-1)
        
        # Compute load balancing loss
        load_balance_loss = self._compute_load_balance_loss(logits, top_k_indices)
        
        return routing_weights, top_k_indices, load_balance_loss
    
    def _compute_load_balance_loss(
        self,
        logits: torch.Tensor,
        indices: torch.Tensor
    ) -> torch.Tensor:
        """
        Compute auxiliary load balancing loss
        Encourages uniform expert utilization
        """
        # Compute fraction of tokens dispatched to each expert
        expert_mask = F.one_hot(indices, num_classes=self.num_experts).float()
        expert_fraction = expert_mask.mean(dim=0)
        
        # Compute average routing probability for each expert
        routing_probs = F.softmax(logits, dim=-1)
        avg_routing_prob = routing_probs.mean(dim=0)
        
        # Load balancing loss
        load_balance_loss = self.num_experts * torch.sum(
            expert_fraction * avg_routing_prob
        )
        
        return load_balance_loss

class MoEGatingNetwork:
    """
    Main gating network for expert selection.
    Integrates with Layer 1 (Meta-Cognition) and Layer 3 (Dual-Axis Decision Core).
    """
    
    def __init__(
        self,
        num_experts: int = 5,
        top_k: int = 2,
        device: str = 'cpu'
    ):
        self.num_experts = num_experts
        self.top_k = top_k
        self.device = device
        
        # Initialize sparse gating
        self.sparse_gate = SparseMoEGate(
            input_dim=15,  # Size of GatingContext features
            num_experts=num_experts,
            top_k=top_k
        ).to(device)
        
        # Expert utilization tracking
        self.expert_usage_count = {i: 0 for i in range(num_experts)}
        self.total_routing_calls = 0
        
        # Routing history for meta-cognitive analysis
        self.routing_history: List[Dict] = []
        
        logger.info(f"Initialized MoE Gating Network with {num_experts} experts")
    
    def route(
        self,
        context: GatingContext,
        expert_constraints: Optional[List[int]] = None,
        training: bool = False
    ) -> List[Tuple[int, float]]:
        """
        Route to top-k experts based on context
        
        Args:
            context: GatingContext with all feature information
            expert_constraints: Optional list of allowed expert indices
            training: Whether in training mode
            
        Returns:
            List of (expert_index, routing_weight) tuples
        """
        # Convert context to tensor
        x = context.to_tensor().unsqueeze(0).to(self.device)
        
        # Get routing decisions
        with torch.set_grad_enabled(training):
            routing_weights, expert_indices, load_balance_loss = self.sparse_gate(
                x, training=training
            )
        
        # Convert to list of tuples
        routing_weights = routing_weights.squeeze(0).detach().cpu().numpy()
        expert_indices = expert_indices.squeeze(0).detach().cpu().numpy()
        
        routing_decisions = list(zip(expert_indices, routing_weights))
        
        # Apply constraints if provided
        if expert_constraints is not None:
            routing_decisions = [
                (idx, weight) for idx, weight in routing_decisions
                if idx in expert_constraints
            ]
        
        # Update usage statistics
        for idx, _ in routing_decisions:
            self.expert_usage_count[idx] = self.expert_usage_count.get(idx, 0) + 1
        
        self.total_routing_calls += 1
        
        # Store in history
        self.routing_history.append({
            'context': context,
            'decisions': routing_decisions,
            'load_balance_loss': load_balance_loss.item(),
            'timestamp': self.total_routing_calls
        })
        
        # Keep only last 1000 routing decisions
        if len(self.routing_history) > 1000:
            self.routing_history = self.routing_history[-1000:]
        
        return routing_decisions
    
    def get_expert_utilization(self) -> Dict[int, float]:
        """Calculate expert utilization percentages"""
        if self.total_routing_calls == 0:
            return {i: 0.0 for i in range(self.num_experts)}
        
        return {
            idx: count / (self.total_routing_calls * self.top_k)
            for idx, count in self.expert_usage_count.items()
        }
    
    def get_load_balance_score(self) -> float:
        """
        Calculate load balance score (0-1).
        1 means perfectly balanced, 0 means completely imbalanced.
        """
        utilization = self.get_expert_utilization()
        if not utilization:
            return 0.0
        
        values = list(utilization.values())
        if sum(values) == 0:
            return 0.0
        
        # Normalized entropy as balance score
        entropy = 0
        for p in values:
            if p > 0:
                entropy -= p * np.log(p)
        
        max_entropy = np.log(len(values))
        return entropy / max_entropy if max_entropy > 0 else 0.0
    
    def update_routing_weights(
        self,
        expert_id: int,
        performance_feedback: float
    ):
        """
        Update routing preferences based on meta-cognitive feedback.
        This is where Layer 1 (Meta-Cognition) influences future routing.
        """
        # This would typically involve reinforcement learning or
        # gradient-based updates in a full implementation
        # For now, we track the feedback
        if len(self.routing_history) > 0:
            last_decision = self.routing_history[-1]
            last_decision['feedback'] = performance_feedback
    
    def save_state(self, path: str):
        """Save gating network state"""
        torch.save({
            'model_state_dict': self.sparse_gate.state_dict(),
            'expert_usage_count': self.expert_usage_count,
            'total_routing_calls': self.total_routing_calls
        }, path)
    
    def load_state(self, path: str):
        """Load gating network state"""
        checkpoint = torch.load(path, map_location=self.device)
        self.sparse_gate.load_state_dict(checkpoint['model_state_dict'])
        self.expert_usage_count = checkpoint['expert_usage_count']
        self.total_routing_calls = checkpoint['total_routing_calls']
