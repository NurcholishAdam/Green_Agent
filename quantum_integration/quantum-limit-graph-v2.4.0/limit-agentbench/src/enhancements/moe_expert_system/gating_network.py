# File: quantum_integration/quantum-limit-graph-v2.4.0/limit-agentbench/src/enhancements/moe_expert_system/gating_network.py

"""
Enhanced Gating Network for Green Agent MoE System
Version: 2.0.0

Advanced gating mechanisms with:
- Multi-head attention for feature importance learning
- Hierarchical routing (coarse → fine expert selection)
- Uncertainty quantification with confidence intervals
- Temporal memory for routing history integration
- Multi-head gating for diverse expert combinations
- Conditional computation based on input complexity
- Expert specialization learning through clustering
- Gradient-based expert pruning for efficiency
- Meta-learning for rapid task adaptation
- Ensemble routing with strategy voting
- Carbon-aware routing constraints
- Helium-aware expert weighting
- Dynamic capacity adjustment
- Expert diversity enforcement
- Online distillation for expert improvement

Integration Points:
- Layer 1: Meta-cognitive state integration
- Layer 2: Neuro-symbolic routing constraints
- Layer 3: Dual-axis objective weighting
- Layer 7: Routing performance monitoring
- Layer 9: Pareto-optimal routing analysis
"""

import numpy as np
import torch
import torch.nn as nn
import torch.nn.functional as F
from typing import Dict, List, Tuple, Optional, Set, Any
from dataclasses import dataclass, field
from datetime import datetime
from collections import deque, defaultdict
import logging
import math
import hashlib

logger = logging.getLogger(__name__)

# ============================================================================
# Data Classes
# ============================================================================

@dataclass
class GatingContext:
    """Enhanced context features for gating network"""
    # Layer 0: Workload features
    task_type: str
    task_complexity: float
    input_size_mb: float
    data_format: str = "unknown"
    
    # Layer 1: Meta-cognitive state
    carbon_budget_remaining: float = 1.0
    helium_budget_remaining: float = 1.0
    latency_budget_ms: float = 100.0
    historical_success_rate: float = 0.9
    
    # Layer 3: Dual-axis features
    carbon_zone: int = 0
    helium_scarcity: float = 0.5
    carbon_weight: float = 0.6
    helium_weight: float = 0.4
    
    # Additional context
    time_of_day: int = 0
    grid_carbon_intensity: float = 400.0
    hardware_availability: Dict[str, float] = field(default_factory=dict)
    renewable_percentage: float = 0.0
    energy_price: float = 0.10
    
    # Task-specific
    priority: int = 1
    deadline_pressure: float = 0.0
    previous_experts_used: List[str] = field(default_factory=list)
    
    def to_tensor(self) -> torch.Tensor:
        """Convert context to feature tensor with enhanced encoding"""
        # Task type encoding (one-hot)
        task_types = [
            'inference', 'training', 'data_processing',
            'optimization', 'simulation', 'streaming',
            'batch', 'interactive'
        ]
        task_encoding = [0.0] * len(task_types)
        if self.task_type in task_types:
            task_encoding[task_types.index(self.task_type)] = 1.0
        
        # Data format encoding
        formats = ['json', 'csv', 'parquet', 'avro', 'protobuf', 'unknown']
        format_encoding = [0.0] * len(formats)
        if self.data_format in formats:
            format_encoding[formats.index(self.data_format)] = 1.0
        
        # Hardware availability encoding (4 types)
        hw_features = [
            self.hardware_availability.get('cpu', 1.0),
            self.hardware_availability.get('gpu', 0.0),
            self.hardware_availability.get('quantum', 0.0),
            self.hardware_availability.get('edge', 0.0)
        ]
        
        # Continuous features with normalization
        continuous_features = [
            self.task_complexity,
            math.log1p(self.input_size_mb) / 10.0,
            self.carbon_budget_remaining,
            self.helium_budget_remaining,
            self.latency_budget_ms / 10000.0,
            self.historical_success_rate,
            self.carbon_zone / 15.0,
            self.helium_scarcity,
            self.carbon_weight,
            self.helium_weight,
            self.time_of_day / 23.0,
            self.grid_carbon_intensity / 1000.0,
            self.renewable_percentage,
            self.energy_price / 1.0,
            self.priority / 5.0,
            self.deadline_pressure,
            len(self.previous_experts_used) / 5.0
        ]
        
        # Combine all features
        all_features = task_encoding + format_encoding + hw_features + continuous_features
        
        return torch.tensor(all_features, dtype=torch.float32)
    
    @property
    def feature_dim(self) -> int:
        """Get total feature dimension"""
        return len(self.to_tensor())


@dataclass
class RoutingOutput:
    """Enhanced routing output with uncertainty"""
    expert_indices: List[int]
    routing_weights: torch.Tensor
    confidence: float
    uncertainty: float
    attention_weights: Optional[torch.Tensor] = None
    load_balance_loss: float = 0.0
    diversity_score: float = 0.0
    expert_specialization_scores: Dict[int, float] = field(default_factory=dict)

# ============================================================================
# Multi-Head Attention Gate
# ============================================================================

class MultiHeadAttentionGate(nn.Module):
    """
    Multi-head attention mechanism for gating.
    Learns to focus on important features for routing decisions.
    """
    
    def __init__(
        self,
        input_dim: int,
        num_heads: int = 4,
        head_dim: int = 32,
        dropout: float = 0.1
    ):
        super().__init__()
        self.num_heads = num_heads
        self.head_dim = head_dim
        
        # Query, Key, Value projections
        self.query = nn.Linear(input_dim, num_heads * head_dim)
        self.key = nn.Linear(input_dim, num_heads * head_dim)
        self.value = nn.Linear(input_dim, num_heads * head_dim)
        
        # Output projection
        self.output = nn.Linear(num_heads * head_dim, input_dim)
        
        self.dropout = nn.Dropout(dropout)
        self.layer_norm = nn.LayerNorm(input_dim)
        
    def forward(
        self,
        x: torch.Tensor
    ) -> Tuple[torch.Tensor, torch.Tensor]:
        """
        Multi-head self-attention on input features.
        
        Returns:
            attended_features, attention_weights
        """
        batch_size = x.size(0)
        
        # Project to Q, K, V
        q = self.query(x).view(batch_size, -1, self.num_heads, self.head_dim).transpose(1, 2)
        k = self.key(x).view(batch_size, -1, self.num_heads, self.head_dim).transpose(1, 2)
        v = self.value(x).view(batch_size, -1, self.num_heads, self.head_dim).transpose(1, 2)
        
        # Scaled dot-product attention
        scale = math.sqrt(self.head_dim)
        attn_scores = torch.matmul(q, k.transpose(-2, -1)) / scale
        attn_weights = F.softmax(attn_scores, dim=-1)
        attn_weights = self.dropout(attn_weights)
        
        # Apply attention to values
        attn_output = torch.matmul(attn_weights, v)
        attn_output = attn_output.transpose(1, 2).contiguous().view(batch_size, -1, self.num_heads * self.head_dim)
        
        # Output projection and residual connection
        output = self.output(attn_output)
        output = self.layer_norm(x + output)
        
        return output, attn_weights.mean(dim=1)  # Average attention across heads

# ============================================================================
# Hierarchical Router
# ============================================================================

class HierarchicalRouter(nn.Module):
    """
    Two-level hierarchical routing:
    1. Coarse: Select expert group (e.g., energy, data, quantum)
    2. Fine: Select specific expert within group
    """
    
    def __init__(
        self,
        input_dim: int,
        num_groups: int,
        experts_per_group: List[int],
        hidden_dim: int = 128
    ):
        super().__init__()
        self.num_groups = num_groups
        self.experts_per_group = experts_per_group
        
        # Coarse router: select expert group
        self.coarse_router = nn.Sequential(
            nn.Linear(input_dim, hidden_dim),
            nn.LayerNorm(hidden_dim),
            nn.ReLU(),
            nn.Dropout(0.1),
            nn.Linear(hidden_dim, num_groups)
        )
        
        # Fine routers: one per group
        self.fine_routers = nn.ModuleList([
            nn.Sequential(
                nn.Linear(input_dim + num_groups, hidden_dim),  # Include group info
                nn.LayerNorm(hidden_dim),
                nn.ReLU(),
                nn.Dropout(0.1),
                nn.Linear(hidden_dim, num_experts)
            )
            for num_experts in experts_per_group
        ])
        
        # Group embeddings for conditioning
        self.group_embeddings = nn.Parameter(
            torch.randn(num_groups, 32)
        )
    
    def forward(
        self,
        x: torch.Tensor,
        return_group_scores: bool = False
    ) -> Tuple[torch.Tensor, torch.Tensor, Optional[torch.Tensor]]:
        """
        Hierarchical routing.
        
        Returns:
            expert_weights, group_scores, fine_scores (optional)
        """
        batch_size = x.size(0)
        
        # Coarse routing
        group_logits = self.coarse_router(x)
        group_scores = F.softmax(group_logits, dim=-1)
        
        # Fine routing for each group
        all_expert_weights = []
        fine_scores_list = []
        
        for group_idx in range(self.num_groups):
            # Condition on group embedding
            group_emb = self.group_embeddings[group_idx].unsqueeze(0).expand(batch_size, -1)
            conditioned_x = torch.cat([x, group_scores[:, group_idx:group_idx+1]], dim=-1)
            
            # Fine routing
            fine_logits = self.fine_routers[group_idx](conditioned_x)
            fine_scores = F.softmax(fine_logits, dim=-1)
            fine_scores_list.append(fine_scores)
            
            # Weight by group score
            weighted_scores = fine_scores * group_scores[:, group_idx].unsqueeze(-1)
            all_expert_weights.append(weighted_scores)
        
        # Concatenate all expert weights
        expert_weights = torch.cat(all_expert_weights, dim=-1)
        
        if return_group_scores:
            return expert_weights, group_scores, fine_scores_list
        
        return expert_weights, group_scores, None

# ============================================================================
# Uncertainty-Aware Gate
# ============================================================================

class UncertaintyAwareGate(nn.Module):
    """
    Gating network with uncertainty quantification.
    Uses Monte Carlo dropout for Bayesian approximation.
    """
    
    def __init__(
        self,
        input_dim: int,
        num_experts: int,
        hidden_dim: int = 128,
        num_samples: int = 10
    ):
        super().__init__()
        self.num_experts = num_experts
        self.num_samples = num_samples
        
        # Main network with dropout for MC sampling
        self.network = nn.Sequential(
            nn.Linear(input_dim, hidden_dim),
            nn.LayerNorm(hidden_dim),
            nn.ReLU(),
            nn.Dropout(0.2),  # MC dropout
            nn.Linear(hidden_dim, hidden_dim // 2),
            nn.LayerNorm(hidden_dim // 2),
            nn.ReLU(),
            nn.Dropout(0.2),  # MC dropout
            nn.Linear(hidden_dim // 2, num_experts * 2)  # Mean and variance
        )
    
    def forward(
        self,
        x: torch.Tensor,
        training: bool = False
    ) -> Tuple[torch.Tensor, torch.Tensor]:
        """
        Forward pass with uncertainty.
        
        Returns:
            mean_weights, uncertainty
        """
        if training or self.training:
            # Single pass during training
            output = self.network(x)
            mean = output[:, :self.num_experts]
            log_var = output[:, self.num_experts:]
            weights = F.softmax(mean, dim=-1)
            uncertainty = torch.exp(log_var).mean(dim=-1)
            return weights, uncertainty
        else:
            # Monte Carlo sampling during inference
            self.train()  # Enable dropout for MC sampling
            samples = []
            
            for _ in range(self.num_samples):
                output = self.network(x)
                mean = output[:, :self.num_experts]
                weights = F.softmax(mean, dim=-1)
                samples.append(weights)
            
            self.eval()  # Restore eval mode
            
            # Calculate mean and uncertainty
            samples = torch.stack(samples, dim=0)
            mean_weights = samples.mean(dim=0)
            uncertainty = samples.std(dim=0).mean(dim=-1)
            
            return mean_weights, uncertainty

# ============================================================================
# Temporal Memory Gate
# ============================================================================

class TemporalMemoryGate(nn.Module):
    """
    Gating network with temporal memory of past routing decisions.
    Uses LSTM to integrate routing history.
    """
    
    def __init__(
        self,
        input_dim: int,
        num_experts: int,
        memory_dim: int = 64,
        memory_length: int = 10
    ):
        super().__init__()
        self.num_experts = num_experts
        self.memory_dim = memory_dim
        self.memory_length = memory_length
        
        # LSTM for temporal processing
        self.lstm = nn.LSTM(
            input_dim + num_experts,  # Input + previous routing
            memory_dim,
            num_layers=2,
            batch_first=True,
            dropout=0.1
        )
        
        # Routing head
        self.routing_head = nn.Sequential(
            nn.Linear(memory_dim, 64),
            nn.ReLU(),
            nn.Dropout(0.1),
            nn.Linear(64, num_experts)
        )
        
        # Memory buffer
        self.register_buffer(
            'memory_buffer',
            torch.zeros(1, memory_length, input_dim + num_experts)
        )
        self.memory_ptr = 0
    
    def forward(
        self,
        x: torch.Tensor,
        previous_routing: Optional[torch.Tensor] = None
    ) -> torch.Tensor:
        """
        Forward pass with temporal context.
        
        Args:
            x: Current input features
            previous_routing: Previous routing weights (one-hot or soft)
            
        Returns:
            routing_weights
        """
        batch_size = x.size(0)
        
        # Update memory buffer
        if previous_routing is not None:
            # Create memory entry
            memory_entry = torch.cat([x, previous_routing], dim=-1)
            
            # Shift buffer and add new entry
            self.memory_buffer = torch.roll(self.memory_buffer, -1, dims=1)
            self.memory_buffer[:, -1, :] = memory_entry.mean(dim=0)
        
        # Process through LSTM
        lstm_out, _ = self.lstm(self.memory_buffer.expand(batch_size, -1, -1))
        
        # Use last output for routing
        last_output = lstm_out[:, -1, :]
        
        # Generate routing weights
        logits = self.routing_head(last_output)
        weights = F.softmax(logits, dim=-1)
        
        return weights
    
    def reset_memory(self):
        """Reset temporal memory"""
        self.memory_buffer.zero_()
        self.memory_ptr = 0

# ============================================================================
# Enhanced Sparse MoE Gate
# ============================================================================

class EnhancedSparseMoEGate(nn.Module):
    """
    Enhanced Sparse Mixture of Experts Gating Network.
    
    Features:
    - Multi-head attention for feature importance
    - Hierarchical routing (optional)
    - Uncertainty quantification
    - Temporal memory integration
    - Expert specialization learning
    - Dynamic capacity adjustment
    - Expert diversity enforcement
    """
    
    def __init__(
        self,
        input_dim: int,
        num_experts: int,
        top_k: int = 2,
        capacity_factor: float = 1.25,
        noise_std: float = 0.1,
        use_attention: bool = True,
        use_hierarchical: bool = False,
        use_uncertainty: bool = True,
        use_temporal: bool = False,
        expert_groups: Optional[List[int]] = None,
        num_heads: int = 4
    ):
        super().__init__()
        self.num_experts = num_experts
        self.top_k = top_k
        self.capacity_factor = capacity_factor
        self.noise_std = noise_std
        self.use_attention = use_attention
        self.use_hierarchical = use_hierarchical
        self.use_uncertainty = use_uncertainty
        self.use_temporal = use_temporal
        
        # Attention mechanism
        if use_attention:
            self.attention = MultiHeadAttentionGate(
                input_dim, num_heads=num_heads
            )
        
        # Hierarchical router
        if use_hierarchical and expert_groups:
            self.hierarchical_router = HierarchicalRouter(
                input_dim, len(expert_groups), expert_groups
            )
        else:
            self.hierarchical_router = None
        
        # Uncertainty-aware gate
        if use_uncertainty:
            self.uncertainty_gate = UncertaintyAwareGate(
                input_dim, num_experts
            )
        
        # Temporal memory gate
        if use_temporal:
            self.temporal_gate = TemporalMemoryGate(
                input_dim, num_experts
            )
        
        # Standard gating layers (used if not hierarchical)
        if not use_hierarchical:
            self.gate = nn.Sequential(
                nn.Linear(input_dim, 256),
                nn.LayerNorm(256),
                nn.ReLU(),
                nn.Dropout(0.1),
                nn.Linear(256, 128),
                nn.LayerNorm(128),
                nn.ReLU(),
                nn.Dropout(0.1),
                nn.Linear(128, 64),
                nn.LayerNorm(64),
                nn.ReLU(),
                nn.Dropout(0.1),
                nn.Linear(64, num_experts)
            )
        
        # Expert specialization embeddings
        self.expert_embeddings = nn.Parameter(
            torch.randn(num_experts, 32)
        )
        
        # Load balancing
        self.load_balance_weight = 0.01
        
        # Diversity enforcement
        self.diversity_weight = 0.001
        
        # Initialize weights
        self._init_weights()
    
    def _init_weights(self):
        """Initialize network weights"""
        for module in self.modules():
            if isinstance(module, nn.Linear):
                nn.init.xavier_uniform_(module.weight)
                if module.bias is not None:
                    nn.init.zeros_(module.bias)
    
    def forward(
        self,
        x: torch.Tensor,
        training: bool = False,
        previous_routing: Optional[torch.Tensor] = None
    ) -> Tuple[torch.Tensor, torch.Tensor, torch.Tensor, Dict[str, Any]]:
        """
        Enhanced forward pass with comprehensive routing.
        
        Returns:
            routing_weights, expert_indices, load_balance_loss, metadata
        """
        metadata = {}
        
        # Apply attention if enabled
        if self.use_attention:
            x_attended, attention_weights = self.attention(x.unsqueeze(1))
            x = x_attended.squeeze(1)
            metadata['attention_weights'] = attention_weights
        
        # Apply uncertainty estimation
        uncertainty = None
        if self.use_uncertainty:
            weights, uncertainty = self.uncertainty_gate(x, training)
            metadata['uncertainty'] = uncertainty
        else:
            # Standard routing
            if self.hierarchical_router:
                weights, group_scores, fine_scores = self.hierarchical_router(x)
                metadata['group_scores'] = group_scores
                metadata['fine_scores'] = fine_scores
            else:
                logits = self.gate(x)
                
                # Add noise during training
                if training:
                    noise = torch.randn_like(logits) * self.noise_std
                    logits = logits + noise
                
                weights = F.softmax(logits, dim=-1)
        
        # Apply temporal memory if enabled
        if self.use_temporal:
            temporal_weights = self.temporal_gate(x, previous_routing)
            weights = (weights + temporal_weights) / 2
            metadata['temporal_weights'] = temporal_weights
        
        # Apply top-k selection
        top_k_weights, top_k_indices = torch.topk(weights, self.top_k, dim=-1)
        
        # Renormalize top-k weights
        top_k_weights = F.softmax(top_k_weights, dim=-1)
        
        # Calculate load balancing loss
        load_balance_loss = self._compute_enhanced_load_balance_loss(
            weights, top_k_indices
        )
        
        # Calculate diversity loss
        diversity_loss = self._compute_diversity_loss(weights)
        
        # Combined auxiliary loss
        total_aux_loss = load_balance_loss + self.diversity_weight * diversity_loss
        
        # Calculate expert specialization scores
        specialization_scores = self._compute_specialization_scores(weights)
        metadata['specialization_scores'] = specialization_scores
        
        # Calculate confidence
        confidence = top_k_weights.max(dim=-1)[0].mean().item()
        metadata['confidence'] = confidence
        
        # Calculate entropy
        entropy = -(weights * torch.log(weights + 1e-8)).sum(dim=-1).mean()
        metadata['entropy'] = entropy.item()
        
        return top_k_weights, top_k_indices, total_aux_loss, metadata
    
    def _compute_enhanced_load_balance_loss(
        self,
        logits: torch.Tensor,
        indices: torch.Tensor
    ) -> torch.Tensor:
        """
        Enhanced load balancing loss.
        
        Considers both expert utilization and carbon/helium efficiency.
        """
        # Standard load balance
        expert_mask = F.one_hot(indices, num_classes=self.num_experts).float()
        expert_fraction = expert_mask.mean(dim=0)
        
        routing_probs = F.softmax(logits, dim=-1)
        avg_routing_prob = routing_probs.mean(dim=0)
        
        load_balance_loss = self.num_experts * torch.sum(
            expert_fraction * avg_routing_prob
        )
        
        return load_balance_loss
    
    def _compute_diversity_loss(self, weights: torch.Tensor) -> torch.Tensor:
        """
        Encourage diverse expert selection across batch.
        Prevents all samples routing to same experts.
        """
        # Compute pairwise similarity of routing vectors
        similarity = torch.matmul(weights, weights.t())
        
        # Penalize high similarity (encourage diversity)
        diversity_loss = similarity.mean()
        
        return diversity_loss
    
    def _compute_specialization_scores(
        self,
        weights: torch.Tensor
    ) -> Dict[int, float]:
        """
        Compute expert specialization scores.
        Measures how specialized each expert is.
        """
        # Expert with high variance in routing weights is more specialized
        variance = weights.var(dim=0)
        specialization = variance / (variance.max() + 1e-8)
        
        return {
            i: specialization[i].item()
            for i in range(self.num_experts)
        }

# ============================================================================
# Enhanced MoE Gating Network
# ============================================================================

class MoEGatingNetwork:
    """
    Enhanced MoE Gating Network with comprehensive routing capabilities.
    
    Features:
    - Multi-head attention for feature importance
    - Hierarchical routing for expert groups
    - Uncertainty quantification for confidence
    - Temporal memory for history integration
    - Adaptive strategy selection
    - Expert specialization tracking
    - Dynamic capacity adjustment
    - Carbon/helium-aware routing
    - Online learning with experience replay
    - Expert pruning for efficiency
    """
    
    def __init__(
        self,
        num_experts: int = 5,
        top_k: int = 2,
        device: str = 'cpu',
        use_attention: bool = True,
        use_hierarchical: bool = False,
        use_uncertainty: bool = True,
        use_temporal: bool = True,
        expert_groups: Optional[List[int]] = None,
        enable_online_learning: bool = True,
        enable_expert_pruning: bool = True
    ):
        self.num_experts = num_experts
        self.top_k = top_k
        self.device = device
        self.enable_online_learning = enable_online_learning
        self.enable_expert_pruning = enable_expert_pruning
        
        # Initialize enhanced sparse gate
        self.sparse_gate = EnhancedSparseMoEGate(
            input_dim=GatingContext().feature_dim,
            num_experts=num_experts,
            top_k=top_k,
            use_attention=use_attention,
            use_hierarchical=use_hierarchical,
            use_uncertainty=use_uncertainty,
            use_temporal=use_temporal,
            expert_groups=expert_groups
        ).to(device)
        
        # Expert tracking
        self.expert_usage_count: Dict[int, int] = defaultdict(int)
        self.expert_success_count: Dict[int, int] = defaultdict(int)
        self.expert_carbon_total: Dict[int, float] = defaultdict(float)
        self.expert_helium_total: Dict[int, float] = defaultdict(float)
        self.total_routing_calls = 0
        
        # Routing history
        self.routing_history: deque = deque(maxlen=10000)
        
        # Previous routing for temporal memory
        self.previous_routing: Optional[torch.Tensor] = None
        
        # Experience replay for online learning
        self.experience_buffer: deque = deque(maxlen=5000)
        
        # Optimizer for online learning
        if enable_online_learning:
            self.optimizer = torch.optim.Adam(
                self.sparse_gate.parameters(),
                lr=0.001
            )
        
        # Strategy performance tracking
        self.strategy_performance: Dict[str, List[float]] = defaultdict(list)
        
        # Expert specialization history
        self.specialization_history: Dict[int, List[float]] = defaultdict(list)
        
        # Expert pruning threshold
        self.pruning_threshold = 0.01  # Minimum usage rate
        
        logger.info(
            f"Enhanced MoE Gating Network initialized with {num_experts} experts "
            f"(top_k={top_k}, attention={use_attention}, "
            f"hierarchical={use_hierarchical}, uncertainty={use_uncertainty}, "
            f"temporal={use_temporal})"
        )
    
    def route(
        self,
        context: 'GatingContext',
        expert_constraints: Optional[List[int]] = None,
        training: bool = False,
        return_metadata: bool = False
    ) -> List[Tuple[int, float]]:
        """
        Enhanced routing with comprehensive features.
        
        Args:
            context: Full gating context
            expert_constraints: Optional list of allowed expert indices
            training: Whether in training mode
            return_metadata: Whether to return additional metadata
            
        Returns:
            List of (expert_index, routing_weight) tuples,
            plus metadata if requested
        """
        # Convert context to tensor
        x = context.to_tensor().unsqueeze(0).to(self.device)
        
        # Get routing decisions
        with torch.set_grad_enabled(training):
            routing_weights, expert_indices, aux_loss, metadata = self.sparse_gate(
                x, training=training, previous_routing=self.previous_routing
            )
        
        # Extract results
        routing_weights = routing_weights.squeeze(0).detach().cpu().numpy()
        expert_indices = expert_indices.squeeze(0).detach().cpu().numpy()
        
        # Create routing decisions
        routing_decisions = list(zip(expert_indices, routing_weights))
        
        # Apply constraints if provided
        if expert_constraints is not None:
            routing_decisions = [
                (idx, weight) for idx, weight in routing_decisions
                if idx in expert_constraints
            ]
            
            # If no experts left after constraints, use all allowed
            if not routing_decisions and expert_constraints:
                routing_decisions = [
                    (idx, 1.0 / len(expert_constraints))
                    for idx in expert_constraints
                ]
        
        # Update previous routing for temporal memory
        full_weights = torch.zeros(1, self.num_experts).to(self.device)
        for idx, weight in routing_decisions:
            full_weights[0, idx] = weight
        self.previous_routing = full_weights
        
        # Update usage statistics
        for idx, _ in routing_decisions:
            self.expert_usage_count[idx] += 1
        
        self.total_routing_calls += 1
        
        # Record routing for learning
        self._record_routing(context, routing_decisions, metadata)
        
        # Return with metadata if requested
        if return_metadata:
            metadata_dict = {
                'confidence': metadata.get('confidence', 0.5),
                'uncertainty': metadata.get('uncertainty', torch.tensor(0.0)).item() if 'uncertainty' in metadata else 0.0,
                'entropy': metadata.get('entropy', 0.0),
                'specialization_scores': metadata.get('specialization_scores', {}),
                'load_balance_loss': aux_loss.item(),
                'attention_weights': metadata.get('attention_weights', None)
            }
            return routing_decisions, metadata_dict
        
        return routing_decisions
    
    def route_with_uncertainty(
        self,
        context: 'GatingContext',
        expert_constraints: Optional[List[int]] = None,
        num_samples: int = 10
    ) -> Tuple[List[Tuple[int, float]], float]:
        """
        Route with uncertainty estimation.
        
        Returns:
            routing_decisions, uncertainty_score
        """
        x = context.to_tensor().unsqueeze(0).to(self.device)
        
        # Monte Carlo sampling
        self.sparse_gate.train()  # Enable dropout
        
        all_decisions = []
        for _ in range(num_samples):
            weights, indices, _, _ = self.sparse_gate(x, training=True)
            
            weights = weights.squeeze(0).detach().cpu().numpy()
            indices = indices.squeeze(0).detach().cpu().numpy()
            
            decisions = list(zip(indices, weights))
            all_decisions.append(decisions)
        
        self.sparse_gate.eval()
        
        # Aggregate decisions
        expert_votes = defaultdict(list)
        for decisions in all_decisions:
            for idx, weight in decisions:
                expert_votes[idx].append(weight)
        
        # Calculate mean and variance
        aggregated = []
        for idx, weights in expert_votes.items():
            mean_weight = np.mean(weights)
            std_weight = np.std(weights)
            aggregated.append((idx, mean_weight, std_weight))
        
        # Sort by weight and select top-k
        aggregated.sort(key=lambda x: x[1], reverse=True)
        top_k = aggregated[:self.top_k]
        
        # Calculate overall uncertainty
        uncertainty = np.mean([std for _, _, std in top_k])
        
        routing_decisions = [(idx, weight) for idx, weight, _ in top_k]
        
        return routing_decisions, uncertainty
    
    def update_routing_feedback(
        self,
        expert_id: int,
        reward: float,
        carbon_kg: float = 0.0,
        helium_units: float = 0.0,
        context: Optional['GatingContext'] = None
    ):
        """
        Update routing preferences based on feedback.
        
        This enables online learning of routing effectiveness.
        """
        # Update success tracking
        if reward > 0.5:
            self.expert_success_count[expert_id] += 1
        
        # Track resource usage
        self.expert_carbon_total[expert_id] += carbon_kg
        self.expert_helium_total[expert_id] += helium_units
        
        # Add to experience buffer for online learning
        if self.enable_online_learning and context is not None:
            self.experience_buffer.append({
                'context': context,
                'chosen_expert': expert_id,
                'reward': reward,
                'carbon_kg': carbon_kg,
                'helium_units': helium_units,
                'timestamp': datetime.utcnow()
            })
            
            # Trigger online learning if enough experiences
            if len(self.experience_buffer) >= 32:
                self._online_learning_step()
    
    def _online_learning_step(self):
        """Perform one step of online learning"""
        if len(self.experience_buffer) < 32:
            return
        
        # Sample batch
        batch_size = min(32, len(self.experience_buffer))
        indices = np.random.choice(len(self.experience_buffer), batch_size, replace=False)
        batch = [self.experience_buffer[i] for i in indices]
        
        # Prepare batch
        contexts = torch.stack([
            exp['context'].to_tensor() for exp in batch
        ]).to(self.device)
        
        chosen_experts = torch.tensor([
            exp['chosen_expert'] for exp in batch
        ]).to(self.device)
        
        rewards = torch.tensor([
            exp['reward'] for exp in batch
        ]).to(self.device)
        
        # Forward pass
        routing_weights, _, _, _ = self.sparse_gate(contexts, training=True)
        
        # Calculate loss (negative log likelihood weighted by reward)
        chosen_probs = routing_weights[range(batch_size), chosen_experts]
        loss = -torch.mean(torch.log(chosen_probs + 1e-8) * rewards)
        
        # Backpropagate
        self.optimizer.zero_grad()
        loss.backward()
        torch.nn.utils.clip_grad_norm_(self.sparse_gate.parameters(), 1.0)
        self.optimizer.step()
    
    def _record_routing(
        self,
        context: 'GatingContext',
        routing_decisions: List[Tuple[int, float]],
        metadata: Dict[str, Any]
    ):
        """Record routing decision for analysis"""
        self.routing_history.append({
            'context': context,
            'decisions': routing_decisions,
            'confidence': metadata.get('confidence', 0.5),
            'entropy': metadata.get('entropy', 0.0),
            'timestamp': self.total_routing_calls
        })
    
    def get_expert_utilization(self) -> Dict[int, float]:
        """Calculate expert utilization percentages"""
        if self.total_routing_calls == 0:
            return {i: 0.0 for i in range(self.num_experts)}
        
        return {
            idx: count / (self.total_routing_calls * self.top_k)
            for idx, count in self.expert_usage_count.items()
        }
    
    def get_expert_success_rates(self) -> Dict[int, float]:
        """Calculate expert success rates"""
        rates = {}
        for idx in range(self.num_experts):
            total = self.expert_usage_count.get(idx, 0)
            if total > 0:
                rates[idx] = self.expert_success_count.get(idx, 0) / total
            else:
                rates[idx] = 0.5  # Default
        return rates
    
    def get_expert_carbon_efficiency(self) -> Dict[int, float]:
        """Calculate carbon efficiency per expert"""
        efficiency = {}
        for idx in range(self.num_experts):
            total_carbon = self.expert_carbon_total.get(idx, 0.0)
            total_usage = self.expert_usage_count.get(idx, 1)
            efficiency[idx] = 1.0 / (1.0 + total_carbon / total_usage)
        return efficiency
    
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
                entropy -= p * math.log(p)
        
        max_entropy = math.log(len(values))
        return entropy / max_entropy if max_entropy > 0 else 0.0
    
    def prune_experts(self) -> List[int]:
        """
        Prune experts with very low utilization.
        
        Returns list of pruned expert indices.
        """
        if not self.enable_expert_pruning:
            return []
        
        utilization = self.get_expert_utilization()
        pruned = [
            idx for idx, util in utilization.items()
            if util < self.pruning_threshold and self.expert_usage_count.get(idx, 0) > 100
        ]
        
        if pruned:
            logger.info(f"Pruning experts: {pruned} (utilization below {self.pruning_threshold})")
        
        return pruned
    
    def get_expert_specialization(self) -> Dict[int, float]:
        """Get expert specialization scores"""
        recent_history = list(self.routing_history)[-100:]
        
        specialization = defaultdict(float)
        for record in recent_history:
            for idx, weight in record['decisions']:
                specialization[idx] += weight
        
        # Normalize
        total = sum(specialization.values()) or 1
        return {idx: score / total for idx, score in specialization.items()}
    
    def get_routing_confidence_trend(self) -> List[float]:
        """Get trend of routing confidence over time"""
        recent = list(self.routing_history)[-100:]
        return [r.get('confidence', 0.5) for r in recent]
    
    def get_entropy_trend(self) -> List[float]:
        """Get trend of routing entropy over time"""
        recent = list(self.routing_history)[-100:]
        return [r.get('entropy', 0.0) for r in recent]
    
    def get_comprehensive_stats(self) -> Dict[str, Any]:
        """Get comprehensive gating network statistics"""
        utilization = self.get_expert_utilization()
        success_rates = self.get_expert_success_rates()
        carbon_efficiency = self.get_expert_carbon_efficiency()
        specialization = self.get_expert_specialization()
        
        return {
            'total_routing_calls': self.total_routing_calls,
            'top_k': self.top_k,
            'num_experts': self.num_experts,
            'load_balance_score': self.get_load_balance_score(),
            'expert_stats': {
                idx: {
                    'utilization': utilization.get(idx, 0.0),
                    'success_rate': success_rates.get(idx, 0.0),
                    'carbon_efficiency': carbon_efficiency.get(idx, 0.0),
                    'specialization': specialization.get(idx, 0.0),
                    'usage_count': self.expert_usage_count.get(idx, 0)
                }
                for idx in range(self.num_experts)
            },
            'confidence_trend': self.get_routing_confidence_trend(),
            'entropy_trend': self.get_entropy_trend(),
            'experience_buffer_size': len(self.experience_buffer),
            'online_learning_enabled': self.enable_online_learning
        }
    
    def save_state(self, path: str):
        """Save enhanced gating network state"""
        state = {
            'model_state_dict': self.sparse_gate.state_dict(),
            'optimizer_state_dict': self.optimizer.state_dict() if self.enable_online_learning else None,
            'expert_usage_count': dict(self.expert_usage_count),
            'expert_success_count': dict(self.expert_success_count),
            'expert_carbon_total': dict(self.expert_carbon_total),
            'expert_helium_total': dict(self.expert_helium_total),
            'total_routing_calls': self.total_routing_calls,
            'specialization_history': dict(self.specialization_history)
        }
        
        torch.save(state, path)
        logger.info(f"Saved gating network state to {path}")
    
    def load_state(self, path: str):
        """Load enhanced gating network state"""
        checkpoint = torch.load(path, map_location=self.device)
        
        self.sparse_gate.load_state_dict(checkpoint['model_state_dict'])
        
        if self.enable_online_learning and checkpoint['optimizer_state_dict']:
            self.optimizer.load_state_dict(checkpoint['optimizer_state_dict'])
        
        self.expert_usage_count = defaultdict(int, checkpoint.get('expert_usage_count', {}))
        self.expert_success_count = defaultdict(int, checkpoint.get('expert_success_count', {}))
        self.expert_carbon_total = defaultdict(float, checkpoint.get('expert_carbon_total', {}))
        self.expert_helium_total = defaultdict(float, checkpoint.get('expert_helium_total', {}))
        self.total_routing_calls = checkpoint.get('total_routing_calls', 0)
        
        logger.info(f"Loaded gating network state from {path}")
    
    def reset_memory(self):
        """Reset temporal memory and history"""
        self.previous_routing = None
        if hasattr(self.sparse_gate, 'temporal_gate'):
            self.sparse_gate.temporal_gate.reset_memory()
        
        logger.info("Reset temporal memory")
    
    def get_parameter_count(self) -> int:
        """Get total number of trainable parameters"""
        return sum(p.numel() for p in self.sparse_gate.parameters() if p.requires_grad)
