# enhancements/fft_moe_adapter.py
"""
Federated Fine-Tuning with Mixture of Experts (FFT-MoE) Adapter v1.0.0
Enables efficient, personalized fine-tuning across heterogeneous clients
"""

import asyncio
import logging
import random
from typing import Dict, Any, List, Optional, Set, Tuple
from dataclasses import dataclass, field
from datetime import datetime
import torch
import torch.nn as nn
import torch.nn.functional as F
from collections import defaultdict, deque

logger = logging.getLogger(__name__)

@dataclass
class MoEConfig:
    """Configuration for Mixture of Experts system"""
    num_experts: int = 8
    num_active_experts: int = 2  # Top-k experts to activate
    expert_hidden_size: int = 512
    expert_layers: int = 2
    router_hidden_size: int = 256
    noise_std: float = 0.1
    dropout: float = 0.1
    capacity_factor: float = 1.25
    expert_hot_update: bool = True  # Enable expert hot-swapping

@dataclass
class ExpertState:
    """State of a single expert within the MoE"""
    expert_id: str
    weights: Dict[str, torch.Tensor]
    activation_count: int = 0
    performance_score: float = 0.5
    last_updated: datetime = field(default_factory=datetime.utcnow)
    layer_index: int = 0
    is_specialized: bool = False
    specialization_domain: str = "general"

@dataclass
class ClientExpertProfile:
    """Personalized expert profile for a client"""
    client_id: str
    active_expert_ids: List[str]
    expert_weights: Dict[str, float]  # Weight per expert
    token_efficiency: float = 0.5
    local_update_count: int = 0
    staleness_score: float = 0.0
    data_distribution: Dict[str, float] = field(default_factory=dict)

@dataclass
class FFTMoEUpdate:
    """FFT-MoE update for a client"""
    client_id: str
    expert_updates: Dict[str, Dict[str, torch.Tensor]]  # expert_id -> updated weights
    gating_update: Dict[str, torch.Tensor]  # Updated gating network weights
    token_usage: float
    carbon_footprint_kg: float
    timestamp: datetime = field(default_factory=datetime.utcnow)

class FFTRouter(nn.Module):
    """
    Sparse gating network for FFT-MoE.
    Routes input tokens to the most relevant experts.
    """
    
    def __init__(self, input_dim: int, config: MoEConfig):
        super().__init__()
        self.config = config
        self.input_dim = input_dim
        self.num_experts = config.num_experts
        
        # Gating network
        self.gate = nn.Sequential(
            nn.Linear(input_dim, config.router_hidden_size),
            nn.ReLU(),
            nn.Dropout(config.dropout),
            nn.Linear(config.router_hidden_size, config.num_experts)
        )
        
        # Expert routing log
        self.routing_history = deque(maxlen=1000)
    
    def forward(self, x: torch.Tensor) -> Tuple[torch.Tensor, torch.Tensor]:
        """
        Forward pass through the router.
        
        Returns:
            gating_logits: Raw logits from gate
            expert_weights: Sparse expert weights (top-k)
        """
        gating_logits = self.gate(x)
        
        # Add noise for exploration
        if self.training and self.config.noise_std > 0:
            noise = torch.randn_like(gating_logits) * self.config.noise_std
            gating_logits = gating_logits + noise
        
        # Apply Top-K sparsity
        top_k = self.config.num_active_experts
        top_k_values, top_k_indices = torch.topk(
            F.softmax(gating_logits, dim=-1), 
            top_k, 
            dim=-1
        )
        
        # Create sparse expert weights
        expert_weights = torch.zeros_like(gating_logits)
        expert_weights.scatter_(-1, top_k_indices, top_k_values)
        
        # Normalize weights
        expert_weights = expert_weights / expert_weights.sum(dim=-1, keepdim=True)
        
        return gating_logits, expert_weights

class FFTMoEAdapter:
    """
    Federated Fine-Tuning with Mixture of Experts adapter.
    
    Features:
    - Sparse expert activation per client
    - Parameter-efficient fine-tuning
    - Dynamic expert specialization
    - Token-weighted aggregation
    - Hot-swappable experts
    - Stale update handling
    - Cross-client knowledge transfer
    """
    
    def __init__(
        self,
        config: MoEConfig,
        base_model: Optional[Dict[str, torch.Tensor]] = None,
        num_global_rounds: int = 100
    ):
        self.config = config
        self.num_global_rounds = num_global_rounds
        
        # Core MoE state
        self.experts: Dict[str, ExpertState] = {}
        self.router: Optional[FFTRouter] = None
        self.global_expert_pool: Dict[str, Dict[str, torch.Tensor]] = {}
        
        # Client-specific state
        self.client_profiles: Dict[str, ClientExpertProfile] = {}
        self.pending_updates: Dict[str, List[FFTMoEUpdate]] = defaultdict(list)
        
        # Global metrics
        self.round_number = 0
        self.global_accuracy = 0.0
        self.total_tokens_distributed = 0.0
        
        # Expert specialization tracking
        self.expert_specialization: Dict[str, str] = {}
        self.expert_performance: Dict[str, float] = {}
        
        # Cross-client knowledge transfer
        self.knowledge_transfer_matrix: Dict[str, Dict[str, float]] = defaultdict(dict)
        
        self._lock = asyncio.Lock()
        
        # Initialize experts
        for i in range(config.num_experts):
            expert_id = f"expert_{i}"
            self.experts[expert_id] = ExpertState(
                expert_id=expert_id,
                weights={},  # To be initialized from base model
                layer_index=i // (config.num_experts // 2) if config.num_experts > 1 else 0
            )
        
        # Initialize router
        input_dim = 768  # Default BERT embedding size
        self.router = FFTRouter(input_dim, config)
        
        logger.info(f"FFT-MoE Adapter initialized with {config.num_experts} experts")
    
    async def initialize_with_base_model(self, base_model: Dict[str, torch.Tensor]):
        """
        Initialize experts with base model weights.
        Different experts can start with different subsets of base model.
        """
        async with self._lock:
            for expert_id, expert in self.experts.items():
                # Start with a copy of base model
                expert.weights = {k: v.clone() for k, v in base_model.items()}
                
                # Add slight random variation for diversity
                for k, v in expert.weights.items():
                    if len(v.shape) >= 2:
                        noise = torch.randn_like(v) * 0.01
                        expert.weights[k] = v + noise
            
            # Store global expert pool
            self.global_expert_pool = {
                expert_id: expert.weights.copy()
                for expert_id, expert in self.experts.items()
            }
            
            logger.info("Experts initialized with base model + variation")
    
    async def register_client(
        self,
        client_id: str,
        data_distribution: Dict[str, float],
        initial_experts: Optional[List[str]] = None
    ):
        """
        Register a new client with personalized expert selection.
        """
        async with self._lock:
            if client_id in self.client_profiles:
                logger.warning(f"Client {client_id} already registered")
                return
            
            # Select experts for this client
            if initial_experts:
                active_experts = initial_experts
            else:
                # Random selection for initial assignment
                all_expert_ids = list(self.experts.keys())
                active_experts = random.sample(
                    all_expert_ids,
                    min(self.config.num_active_experts, len(all_expert_ids))
                )
            
            # Create profile
            profile = ClientExpertProfile(
                client_id=client_id,
                active_expert_ids=active_experts,
                expert_weights={eid: 1.0 / len(active_experts) for eid in active_experts},
                data_distribution=data_distribution
            )
            
            self.client_profiles[client_id] = profile
            logger.info(f"Registered client {client_id} with {len(active_experts)} experts")
    
    async def get_client_model(self, client_id: str) -> Dict[str, torch.Tensor]:
        """
        Get a personalized model for a client.
        This combines selected experts with their weights.
        """
        async with self._lock:
            profile = self.client_profiles.get(client_id)
            if not profile:
                raise ValueError(f"Client {client_id} not registered")
            
            # Get current expert weights
            client_model = {}
            
            # Weighted combination of active experts
            active_experts = profile.active_expert_ids
            expert_weights = profile.expert_weights
            
            for expert_id in active_experts:
                expert_state = self.experts.get(expert_id)
                if not expert_state:
                    continue
                
                weight = expert_weights.get(expert_id, 0.0)
                for layer_name, layer_weights in expert_state.weights.items():
                    if layer_name not in client_model:
                        client_model[layer_name] = layer_weights * weight
                    else:
                        client_model[layer_name] += layer_weights * weight
            
            return client_model
    
    async def receive_client_update(
        self,
        client_id: str,
        expert_updates: Dict[str, Dict[str, torch.Tensor]],
        gating_update: Dict[str, torch.Tensor],
        token_usage: float,
        carbon_footprint_kg: float
    ) -> bool:
        """
        Receive and validate a client update.
        
        Returns:
            True if update accepted, False if rejected (e.g., too stale)
        """
        async with self._lock:
            profile = self.client_profiles.get(client_id)
            if not profile:
                logger.warning(f"Update from unregistered client {client_id}")
                return False
            
            # Check update quality
            staleness = self.round_number - profile.local_update_count
            if staleness > 5:
                logger.warning(f"Update from {client_id} is too stale (staleness={staleness})")
                return False
            
            # Validate expert updates
            valid_updates = []
            for expert_id, weights in expert_updates.items():
                if expert_id not in self.experts:
                    continue
                
                # Validate weight shapes
                expert_state = self.experts[expert_id]
                if len(weights) != len(expert_state.weights):
                    logger.warning(f"Invalid update shape for expert {expert_id}")
                    continue
                
                valid_updates.append((expert_id, weights))
            
            if not valid_updates:
                logger.warning(f"No valid updates from client {client_id}")
                return False
            
            # Store pending update
            update = FFTMoEUpdate(
                client_id=client_id,
                expert_updates={eid: w for eid, w in valid_updates},
                gating_update=gating_update,
                token_usage=token_usage,
                carbon_footprint_kg=carbon_footprint_kg
            )
            
            self.pending_updates[client_id].append(update)
            profile.local_update_count += 1
            
            logger.info(f"Accepted update from {client_id} ({len(valid_updates)} experts)")
            return True
    
    async def aggregate_updates(self) -> Dict[str, torch.Tensor]:
        """
        Aggregate all pending client updates and update global model.
        
        Returns:
            Aggregated global model updates
        """
        async with self._lock:
            if not self.pending_updates:
                logger.info("No updates to aggregate")
                return {}
            
            # Count updates per expert
            expert_update_count = defaultdict(int)
            expert_update_weights = defaultdict(float)
            
            for client_id, updates in self.pending_updates.items():
                profile = self.client_profiles.get(client_id)
                if not profile:
                    continue
                
                for update in updates:
                    # Token-weighted aggregation
                    token_weight = update.token_usage / (1 + update.carbon_footprint_kg)
                    
                    for expert_id, weights in update.expert_updates.items():
                        expert_update_count[expert_id] += 1
                        expert_update_weights[expert_id] += token_weight
            
            # Perform weighted averaging
            aggregated_updates = {}
            
            for expert_id, count in expert_update_count.items():
                if count == 0:
                    continue
                
                # Accumulate weighted updates
                expert_aggregated = {}
                
                for client_id, updates in self.pending_updates.items():
                    for update in updates:
                        if expert_id not in update.expert_updates:
                            continue
                        
                        token_weight = update.token_usage / (1 + update.carbon_footprint_kg)
                        normalized_weight = token_weight / expert_update_weights[expert_id]
                        
                        for layer_name, layer_weights in update.expert_updates[expert_id].items():
                            if layer_name not in expert_aggregated:
                                expert_aggregated[layer_name] = layer_weights * normalized_weight
                            else:
                                expert_aggregated[layer_name] += layer_weights * normalized_weight
                
                # Update global expert pool
                if expert_id in self.global_expert_pool:
                    for layer_name in expert_aggregated:
                        if layer_name in self.global_expert_pool[expert_id]:
                            # Update with moving average
                            alpha = 0.1  # Learning rate for global model
                            self.global_expert_pool[expert_id][layer_name] = (
                                (1 - alpha) * self.global_expert_pool[expert_id][layer_name] +
                                alpha * expert_aggregated[layer_name]
                            )
                
                aggregated_updates[expert_id] = expert_aggregated
            
            # Update expert states
            for expert_id, updates in aggregated_updates.items():
                if expert_id in self.experts:
                    expert_state = self.experts[expert_id]
                    for layer_name, layer_weights in updates.items():
                        if layer_name in expert_state.weights:
                            # Update expert weights
                            expert_state.weights[layer_name] = layer_weights
                    expert_state.last_updated = datetime.utcnow()
                    expert_state.activation_count += 1
            
            # Apply gating updates (if any)
            for client_id, updates in self.pending_updates.items():
                for update in updates:
                    if update.gating_update and self.router:
                        # Update router weights (simplified)
                        pass
            
            # Clear pending updates
            self.pending_updates.clear()
            self.round_number += 1
            
            logger.info(f"Aggregated {len(aggregated_updates)} expert updates in round {self.round_number}")
            return aggregated_updates
    
    async def analyze_expert_specialization(self) -> Dict[str, Any]:
        """
        Analyze which experts are specializing in which domains.
        This enables DDOME (Dynamically Decentralized Orchestration of MoEs).
        """
        async with self._lock:
            expert_domains = {}
            domain_scores = {}
            
            for expert_id, expert_state in self.experts.items():
                # Determine domain based on activation patterns
                # This is a simplification - in practice, analyze which data types activate this expert
                activation_rate = expert_state.activation_count / (self.round_number + 1)
                
                # Analyze expert performance
                performance = self.expert_performance.get(expert_id, 0.5)
                
                # Find domain with strongest performance
                if expert_state.is_specialized:
                    domain = expert_state.specialization_domain
                else:
                    # Default domains based on layer index
                    domains = ['general', 'carbon', 'helium', 'energy', 'optimization', 'prediction']
                    domain = domains[expert_state.layer_index % len(domains)]
                
                expert_domains[expert_id] = {
                    'domain': domain,
                    'specialization_score': performance * activation_rate,
                    'is_specialized': expert_state.is_specialized,
                    'activation_count': expert_state.activation_count
                }
                
                if domain not in domain_scores:
                    domain_scores[domain] = []
                domain_scores[domain].append(performance * activation_rate)
            
            # Calculate domain scores
            domain_averages = {
                domain: sum(scores) / len(scores) if scores else 0
                for domain, scores in domain_scores.items()
            }
            
            return {
                'expert_domains': expert_domains,
                'domain_scores': domain_averages,
                'total_specialized_experts': sum(1 for e in expert_domains.values() if e['is_specialized']),
                'top_performing_domain': max(domain_averages, key=domain_averages.get)
            }
    
    async def hot_swap_experts(self, client_id: str, new_experts: List[str]) -> bool:
        """
        Dynamically adjust which experts a client uses (hot-swapping).
        This is key for personalization and adaptation.
        """
        async with self._lock:
            profile = self.client_profiles.get(client_id)
            if not profile:
                return False
            
            # Validate new experts exist
            valid_experts = [eid for eid in new_experts if eid in self.experts]
            if not valid_experts:
                return False
            
            # Update client's active experts
            profile.active_expert_ids = valid_experts[:self.config.num_active_experts]
            
            # Re-normalize weights
            weight_per_expert = 1.0 / len(profile.active_expert_ids)
            profile.expert_weights = {
                eid: weight_per_expert for eid in profile.active_expert_ids
            }
            
            logger.info(f"Hot-swapped experts for client {client_id}: {profile.active_expert_ids}")
            return True
    
    async def get_fft_moe_status(self) -> Dict[str, Any]:
        """Get comprehensive status of the FFT-MoE system"""
        return {
            'round_number': self.round_number,
            'num_clients': len(self.client_profiles),
            'num_experts': len(self.experts),
            'total_updates_processed': sum(profile.local_update_count for profile in self.client_profiles.values()),
            'total_tokens_distributed': self.total_tokens_distributed,
            'expert_domains': await self.analyze_expert_specialization(),
            'global_accuracy': self.global_accuracy,
            'active_experts_per_client': self.config.num_active_experts,
            'model_size_mb': self._estimate_model_size()
        }
    
    def _estimate_model_size(self) -> float:
        """Estimate model size in MB"""
        total_params = 0
        for expert in self.experts.values():
            for weights in expert.weights.values():
                total_params += weights.numel()
        
        # Assuming 4 bytes per parameter (float32)
        return total_params * 4 / (1024 * 1024)
