# File: quantum_integration/quantum-limit-graph-v2.4.0/limit-agentbench/src/enhancements/moe_expert_system/gating_network.py
# Enhanced with complete bio-inspired integration - Allosteric Enzyme System v4.0.0

"""
Enhanced Gating Network v4.0.0 - Allosteric Enzyme System

Complete bio-inspired integration with:
- Gradient-modulated routing weights (allosteric regulation)
- Token-aware expert selection (substrate affinity)
- Energy-based exploration rate (metabolic state)
- Compartment health feedback (cellular health)
- Biomass reserve awareness (resource storage)
- Second messenger modulation (signal confidence)
- Environmental signal response (photosynthetic awareness)
- Cooperative binding for expert pairs
"""

import numpy as np
import torch
import torch.nn as nn
import torch.nn.functional as F
from typing import Dict, List, Tuple, Optional, Any
from dataclasses import dataclass, field
from datetime import datetime
from collections import deque, defaultdict
import logging
import math
import hashlib

logger = logging.getLogger(__name__)

# ============================================================================
# Try importing bio-inspired modules
# ============================================================================

try:
    from enhancements.bio_inspired.eco_atp_currency import (
        EcoATPTokenManager, DynamicExchangeRate, EcoATPSource, EcoATPConsumer
    )
    from enhancements.bio_inspired.proton_gradient_fields import (
        GradientFieldManager, GradientField
    )
    from enhancements.bio_inspired.atp_synthase_scheduler import (
        ATPSynthaseScheduler, SynthaseConfig
    )
    from enhancements.bio_inspired.chromatophore_compartments import (
        CompartmentManager, ChromatophoreCompartment, CompartmentState
    )
    from enhancements.bio_inspired.biomass_storage import (
        BiomassStorage, StorageTier, GuaranteeLevel
    )
    from enhancements.bio_inspired.photosynthetic_harvester import (
        PhotosyntheticHarvester
    )
    BIO_INSPIRED_AVAILABLE = True
    logger.info("Bio-inspired modules loaded for Gating Network integration")
except ImportError as e:
    BIO_INSPIRED_AVAILABLE = False
    logger.warning(f"Bio-inspired modules not available: {str(e)} - using standard routing")

# ============================================================================
# Gating Context (Enhanced with Bio-Inspired Features)
# ============================================================================

@dataclass
class GatingContext:
    """Enhanced context features for gating network with bio-inspired data"""
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
    
    # BIO-INSPIRED: Gradient levels
    carbon_gradient: float = 0.5
    helium_gradient: float = 0.5
    trust_gradient: float = 0.5
    opportunity_gradient: float = 0.5
    
    # BIO-INSPIRED: Token and energy state
    token_availability: float = 0.5
    ecoatp_rate: float = 50.0
    biomass_reserve_level: float = 0.3
    
    # BIO-INSPIRED: Compartment health
    expert_health_scores: Dict[str, float] = field(default_factory=dict)
    
    # BIO-INSPIRED: Second messenger levels
    camp_level: float = 0.1
    calcium_level: float = 0.05
    ip3_level: float = 0.05
    
    def to_tensor(self) -> torch.Tensor:
        """Convert context to feature tensor with bio-inspired features"""
        # Task type encoding (one-hot)
        task_types = ['inference', 'training', 'data_processing', 'optimization', 
                     'simulation', 'streaming', 'batch', 'interactive']
        task_encoding = [0.0] * len(task_types)
        if self.task_type in task_types:
            task_encoding[task_types.index(self.task_type)] = 1.0
        
        # Data format encoding
        formats = ['json', 'csv', 'parquet', 'avro', 'protobuf', 'unknown']
        format_encoding = [0.0] * len(formats)
        if self.data_format in formats:
            format_encoding[formats.index(self.data_format)] = 1.0
        
        # Hardware availability encoding
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
            len(self.previous_experts_used) / 5.0,
            # BIO-INSPIRED: Gradient features
            self.carbon_gradient,
            self.helium_gradient,
            self.trust_gradient,
            self.opportunity_gradient,
            # BIO-INSPIRED: Token and energy features
            self.token_availability,
            self.ecoatp_rate / 200.0,
            self.biomass_reserve_level,
            # BIO-INSPIRED: Second messenger features
            self.camp_level,
            self.calcium_level,
            self.ip3_level
        ]
        
        # Combine all features
        all_features = task_encoding + format_encoding + hw_features + continuous_features
        
        return torch.tensor(all_features, dtype=torch.float32)
    
    @property
    def feature_dim(self) -> int:
        """Get total feature dimension"""
        return len(self.to_tensor())


# ============================================================================
# Enhanced Sparse MoE Gate with Bio-Inspired Modulation
# ============================================================================

class EnhancedSparseMoEGate(nn.Module):
    """
    Enhanced Sparse MoE Gate with bio-inspired allosteric regulation.
    
    Features:
    - Gradient-modulated attention weights
    - Token-aware expert affinity
    - Energy-based exploration
    - Cooperative binding between expert pairs
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
        use_uncertainty: bool = True
    ):
        super().__init__()
        self.num_experts = num_experts
        self.top_k = top_k
        self.capacity_factor = capacity_factor
        self.noise_std = noise_std
        self.use_attention = use_attention
        self.use_hierarchical = use_hierarchical
        self.use_uncertainty = use_uncertainty
        
        # Core gating network
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
        
        # BIO-INSPIRED: Allosteric modulation layers
        self.gradient_modulator = nn.Sequential(
            nn.Linear(4, 32),  # 4 gradient inputs
            nn.ReLU(),
            nn.Linear(32, num_experts),
            nn.Tanh()  # -1 to 1 modulation
        )
        
        # BIO-INSPIRED: Token affinity layer
        self.token_affinity = nn.Sequential(
            nn.Linear(3, 16),  # token_availability, ecoatp_rate, biomass_level
            nn.ReLU(),
            nn.Linear(16, num_experts),
            nn.Sigmoid()  # 0 to 1 affinity
        )
        
        # BIO-INSPIRED: Cooperative binding matrix
        self.cooperative_matrix = nn.Parameter(
            torch.eye(num_experts) * 0.1
        )
        
        # Expert specialization embeddings
        self.expert_embeddings = nn.Parameter(
            torch.randn(num_experts, 32)
        )
        
        # Load balancing
        self.load_balance_weight = 0.01
        
        # BIO-INSPIRED: Biomass-aware load balance weight
        self.biomass_load_balance_multiplier = 1.0
        
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
        gradient_levels: Optional[Dict[str, float]] = None,
        token_state: Optional[Dict[str, float]] = None
    ) -> Tuple[torch.Tensor, torch.Tensor, torch.Tensor, Dict[str, Any]]:
        """
        Enhanced forward pass with bio-inspired modulation.
        
        Args:
            x: Input features [B, input_dim]
            training: Whether in training mode
            gradient_levels: Real-time gradient field strengths
            token_state: Real-time token economy state
            
        Returns:
            routing_weights, expert_indices, load_balance_loss, metadata
        """
        metadata = {}
        
        # Standard routing logits
        logits = self.gate(x)
        
        # BIO-INSPIRED: Apply gradient modulation (allosteric regulation)
        if gradient_levels:
            gradient_tensor = torch.tensor([
                gradient_levels.get('carbon', 0.5),
                gradient_levels.get('helium', 0.5),
                gradient_levels.get('trust', 0.5),
                gradient_levels.get('opportunity', 0.5)
            ], dtype=torch.float32).to(x.device)
            
            if x.dim() > 1:
                gradient_tensor = gradient_tensor.unsqueeze(0).expand(x.size(0), -1)
            
            gradient_mod = self.gradient_modulator(gradient_tensor)
            logits = logits + gradient_mod * 0.3  # 30% gradient influence
            
            metadata['gradient_modulation'] = gradient_mod.detach().cpu().numpy()
        
        # BIO-INSPIRED: Apply token affinity modulation
        if token_state:
            token_tensor = torch.tensor([
                token_state.get('token_availability', 0.5),
                token_state.get('ecoatp_rate', 50.0) / 200.0,
                token_state.get('biomass_reserve', 0.3)
            ], dtype=torch.float32).to(x.device)
            
            if x.dim() > 1:
                token_tensor = token_tensor.unsqueeze(0).expand(x.size(0), -1)
            
            token_aff = self.token_affinity(token_tensor)
            logits = logits * (0.7 + token_aff * 0.6)  # 60% token influence
            
            metadata['token_affinity'] = token_aff.detach().cpu().numpy()
        
        # BIO-INSPIRED: Apply cooperative binding bonus
        if x.dim() > 1 and x.size(0) > 1:
            # Cooperative binding between expert pairs across batch
            coop_bonus = torch.matmul(
                F.softmax(logits, dim=-1),
                self.cooperative_matrix
            )
            logits = logits + coop_bonus * 0.1  # 10% cooperative influence
        
        # Add noise during training for exploration
        if training:
            noise = torch.randn_like(logits) * self.noise_std
            logits = logits + noise
        
        # Get top-k experts
        top_k_logits, top_k_indices = torch.topk(logits, self.top_k, dim=-1)
        routing_weights = F.softmax(top_k_logits, dim=-1)
        
        # Compute load balancing loss with biomass awareness
        load_balance_loss = self._compute_enhanced_load_balance_loss(
            logits, top_k_indices
        ) * self.biomass_load_balance_multiplier
        
        # Calculate metadata
        metadata['entropy'] = -(F.softmax(logits, dim=-1) * 
                               torch.log(F.softmax(logits, dim=-1) + 1e-8)).sum(dim=-1).mean().item()
        metadata['confidence'] = routing_weights.max(dim=-1)[0].mean().item()
        metadata['cooperative_matrix'] = self.cooperative_matrix.detach().cpu().numpy()
        
        return routing_weights, top_k_indices, load_balance_loss, metadata
    
    def _compute_enhanced_load_balance_loss(
        self, logits: torch.Tensor, indices: torch.Tensor
    ) -> torch.Tensor:
        """Enhanced load balancing loss considering expert utilization"""
        expert_mask = F.one_hot(indices, num_classes=self.num_experts).float()
        expert_fraction = expert_mask.mean(dim=0)
        routing_probs = F.softmax(logits, dim=-1)
        avg_routing_prob = routing_probs.mean(dim=0)
        load_balance_loss = self.num_experts * torch.sum(expert_fraction * avg_routing_prob)
        return load_balance_loss
    
    def update_cooperative_binding(self, expert_pairs: Dict[Tuple[int, int], float]):
        """Update cooperative binding strengths based on observed performance"""
        with torch.no_grad():
            for (expert_a, expert_b), strength in expert_pairs.items():
                if expert_a < self.num_experts and expert_b < self.num_experts:
                    self.cooperative_matrix[expert_a, expert_b] = strength
                    self.cooperative_matrix[expert_b, expert_a] = strength


# ============================================================================
# Enhanced MoE Gating Network with Complete Bio-Inspired Integration
# ============================================================================

class MoEGatingNetwork:
    """
    Enhanced MoE Gating Network v4.0.0 - Allosteric Enzyme System
    
    Complete bio-inspired integration:
    - Gradient-modulated routing (allosteric regulation)
    - Token-aware expert selection (substrate affinity)
    - Energy-based exploration (metabolic state)
    - Compartment health feedback (cellular health)
    - Biomass reserve awareness (resource storage)
    - Second messenger modulation (signal confidence)
    - Environmental signal response (photosynthetic awareness)
    - Cooperative binding for expert pairs
    """
    
    def __init__(
        self,
        num_experts: int = 5,
        top_k: int = 2,
        device: str = 'cpu',
        use_attention: bool = True,
        use_uncertainty: bool = True,
        enable_bio_integration: bool = True
    ):
        self.num_experts = num_experts
        self.top_k = top_k
        self.device = device
        self.enable_bio_integration = enable_bio_integration and BIO_INSPIRED_AVAILABLE
        
        # Initialize enhanced sparse gate
        self.sparse_gate = EnhancedSparseMoEGate(
            input_dim=GatingContext().feature_dim,
            num_experts=num_experts,
            top_k=top_k,
            use_attention=use_attention,
            use_uncertainty=use_uncertainty
        ).to(device)
        
        # Expert tracking
        self.expert_usage_count: Dict[int, int] = defaultdict(int)
        self.expert_success_count: Dict[int, int] = defaultdict(int)
        self.expert_carbon_total: Dict[int, float] = defaultdict(float)
        self.expert_helium_total: Dict[int, float] = defaultdict(float)
        self.total_routing_calls = 0
        
        # Expert index mapping
        self.expert_index_map: Dict[int, str] = {}
        
        # Routing history
        self.routing_history: deque = deque(maxlen=10000)
        
        # BIO-INSPIRED: Module references (injected)
        self.token_manager: Optional[EcoATPTokenManager] = None
        self.gradient_manager: Optional[GradientFieldManager] = None
        self.scheduler: Optional[ATPSynthaseScheduler] = None
        self.compartment_manager: Optional[CompartmentManager] = None
        self.biomass_storage: Optional[BiomassStorage] = None
        self.harvester: Optional[PhotosyntheticHarvester] = None
        
        # BIO-INSPIRED: Cooperative binding tracking
        self.cooperative_pairs: Dict[Tuple[int, int], float] = {}
        self.cooperative_history: deque = deque(maxlen=1000)
        
        # BIO-INSPIRED: Environmental signal history
        self.environmental_history: deque = deque(maxlen=500)
        
        # Optimizer for online learning
        self.optimizer = torch.optim.Adam(
            self.sparse_gate.parameters(), lr=0.001
        )
        
        # Experience buffer for online learning
        self.experience_buffer: deque = deque(maxlen=5000)
        
        # Load balance weight (will be modulated by biomass)
        self.load_balance_weight = 0.01
        
        # Previous routing for temporal memory
        self.previous_routing: Optional[torch.Tensor] = None
        
        logger.info(
            f"Enhanced MoE Gating Network v4.0.0 initialized: "
            f"experts={num_experts}, top_k={top_k}, "
            f"bio_integration={self.enable_bio_integration}"
        )
    
    # ========================================================================
    # Bio-Inspired Module Injection
    # ========================================================================
    
    def inject_bio_core(self, bio_core: Any = None, **kwargs):
        """
        Inject bio-inspired modules for complete correlation.
        
        Connects gating network to real bio-inspired systems.
        """
        if bio_core:
            self.token_manager = getattr(bio_core, 'token_manager', None)
            self.gradient_manager = getattr(bio_core, 'gradient_manager', None)
            self.scheduler = getattr(bio_core, 'scheduler', None)
            self.compartment_manager = getattr(bio_core, 'compartment_manager', None)
            self.biomass_storage = getattr(bio_core, 'biomass_storage', None)
            self.harvester = getattr(bio_core, 'harvester', None)
        else:
            self.token_manager = kwargs.get('token_manager')
            self.gradient_manager = kwargs.get('gradient_manager')
            self.scheduler = kwargs.get('scheduler')
            self.compartment_manager = kwargs.get('compartment_manager')
            self.biomass_storage = kwargs.get('biomass_storage')
            self.harvester = kwargs.get('harvester')
        
        injections = {
            'token_manager': self.token_manager is not None,
            'gradient_manager': self.gradient_manager is not None,
            'scheduler': self.scheduler is not None,
            'compartment_manager': self.compartment_manager is not None,
            'biomass_storage': self.biomass_storage is not None,
            'harvester': self.harvester is not None
        }
        logger.info(f"Bio-inspired injections into Gating Network: {injections}")
        
        if any(injections.values()):
            self.enable_bio_integration = True
    
    # ========================================================================
    # Bio-Inspired Data Access Methods
    # ========================================================================
    
    def _get_real_gradient_levels(self) -> Dict[str, float]:
        """Get REAL gradient levels from bio-inspired system"""
        if self.gradient_manager:
            return self.gradient_manager.get_field_strengths()
        return {'carbon': 0.5, 'helium': 0.5, 'trust': 0.5, 'opportunity': 0.5}
    
    def _get_real_token_state(self) -> Dict[str, float]:
        """Get REAL token economy state"""
        state = {'token_availability': 0.5, 'ecoatp_rate': 50.0, 'biomass_reserve': 0.3}
        
        if self.token_manager:
            summary = self.token_manager.get_system_summary()
            state['token_availability'] = min(1.0, summary.get('total_balance', 500) / 1000.0)
        
        if self.scheduler:
            driving_force = self.scheduler.calculate_gradient_driving_force()
            rotation_speed = self.scheduler.calculate_rotation_speed(driving_force)
            state['ecoatp_rate'] = self.scheduler.calculate_atp_production_rate(rotation_speed)
        
        if self.biomass_storage:
            stats = self.biomass_storage.get_storage_stats()
            total_stored = stats.get('total_stored', 0)
            state['biomass_reserve'] = min(1.0, total_stored / 10000.0)
        
        return state
    
    def _get_compartment_health_scores(self) -> Dict[str, float]:
        """Get REAL compartment health scores"""
        scores = {}
        if self.compartment_manager:
            for idx, expert_id in self.expert_index_map.items():
                compartment = self.compartment_manager.find_best_compartment(expert_id)
                if compartment:
                    scores[expert_id] = compartment.health_score
                else:
                    scores[expert_id] = 0.7  # Default healthy
        return scores
    
    def _get_bio_modulated_exploration(self) -> float:
        """Get exploration rate modulated by bio-inspired state"""
        base_exploration = 0.1
        
        if self.gradient_manager:
            carbon = self.gradient_manager.fields.get('carbon')
            if carbon and carbon.gradient_strength > 0.7:
                base_exploration *= 0.5  # Reduce exploration in high carbon stress
        
        if self.scheduler:
            driving_force = self.scheduler.calculate_gradient_driving_force()
            rotation_speed = self.scheduler.calculate_rotation_speed(driving_force)
            ecoatp_rate = self.scheduler.calculate_atp_production_rate(rotation_speed)
            
            if ecoatp_rate > 100:
                base_exploration *= 1.5  # More exploration when energy abundant
            elif ecoatp_rate < 20:
                base_exploration *= 0.3  # Less exploration when energy scarce
        
        return min(0.5, max(0.01, base_exploration))
    
    def _get_expert_bio_scores(self, expert_indices: List[int]) -> Dict[int, float]:
        """Get bio-inspired scores for experts"""
        scores = {}
        
        for idx in expert_indices:
            expert_id = self.expert_index_map.get(idx)
            if not expert_id:
                scores[idx] = 0.5
                continue
            
            score = 0.5  # Base score
            
            # Token efficiency bonus
            if self.token_manager:
                account = self.token_manager.get_account_summary(f"expert_{expert_id}")
                if account:
                    efficiency = account.get('efficiency_rating', 0.5)
                    balance = account.get('balance', 0)
                    score += efficiency * 0.2
                    if balance > 100:
                        score += 0.1  # Well-funded experts get bonus
            
            # Compartment health bonus
            if self.compartment_manager:
                compartment = self.compartment_manager.find_best_compartment(expert_id)
                if compartment:
                    score += compartment.health_score * 0.15
            
            # Gradient alignment bonus
            if self.gradient_manager:
                trust = self.gradient_manager.fields.get('trust')
                if trust:
                    score += trust.gradient_strength * 0.1
            
            # Success rate bonus from history
            success_rate = self.get_expert_success_rates().get(idx, 0.5)
            score += success_rate * 0.05
            
            scores[idx] = min(1.0, score)
        
        return scores
    
    def _get_biomass_aware_load_balance(self) -> float:
        """Adjust load balancing based on biomass storage levels"""
        if not self.biomass_storage:
            return 0.01
        
        stats = self.biomass_storage.get_storage_stats()
        total_stored = stats.get('total_stored', 0)
        glycogen = stats.get('tiers', {}).get('glycogen_queue', 0)
        
        if total_stored > 5000 or glycogen > 500:
            return 0.05  # Increase load balancing when storage is full
        elif total_stored < 1000:
            return 0.005  # Decrease when storage is empty
        
        return 0.01
    
    def _update_cooperative_binding(self, expert_a: int, expert_b: int, success: bool):
        """Update cooperative binding based on pair performance"""
        key = (expert_a, expert_b)
        current = self.cooperative_pairs.get(key, 0.0)
        
        # Reinforce successful pairs, penalize failed ones
        alpha = 0.1
        if success:
            self.cooperative_pairs[key] = current + alpha * (1.0 - current)
        else:
            self.cooperative_pairs[key] = current * (1.0 - alpha)
        
        # Update the cooperative matrix in the gate
        self.sparse_gate.update_cooperative_binding({key: self.cooperative_pairs[key]})
        
        self.cooperative_history.append({
            'expert_a': expert_a,
            'expert_b': expert_b,
            'success': success,
            'strength': self.cooperative_pairs[key],
            'timestamp': datetime.utcnow().isoformat()
        })
    
    # ========================================================================
    # Enhanced Routing Method
    # ========================================================================
    
    def route(
        self,
        context: 'GatingContext',
        expert_constraints: Optional[List[int]] = None,
        training: bool = False,
        return_metadata: bool = False
    ) -> List[Tuple[int, float]]:
        """
        Enhanced routing with complete bio-inspired modulation.
        
        Args:
            context: Full gating context with bio-inspired features
            expert_constraints: Optional list of allowed expert indices
            training: Whether in training mode
            return_metadata: Whether to return additional metadata
            
        Returns:
            List of (expert_index, routing_weight) tuples
        """
        # BIO-INSPIRED: Enrich context with real bio-inspired data
        if self.enable_bio_integration:
            gradient_levels = self._get_real_gradient_levels()
            context.carbon_gradient = gradient_levels.get('carbon', 0.5)
            context.helium_gradient = gradient_levels.get('helium', 0.5)
            context.trust_gradient = gradient_levels.get('trust', 0.5)
            context.opportunity_gradient = gradient_levels.get('opportunity', 0.5)
            
            token_state = self._get_real_token_state()
            context.token_availability = token_state['token_availability']
            context.ecoatp_rate = token_state['ecoatp_rate']
            context.biomass_reserve_level = token_state['biomass_reserve']
            
            # Update load balance weight from biomass
            self.sparse_gate.biomass_load_balance_multiplier = (
                self._get_biomass_aware_load_balance() / 0.01
            )
        
        # Convert context to tensor
        x = context.to_tensor().unsqueeze(0).to(self.device)
        
        # BIO-INSPIRED: Get exploration rate
        exploration = self._get_bio_modulated_exploration() if self.enable_bio_integration else 0.1
        
        # BIO-INSPIRED: Exploratory routing based on metabolic state
        if training and np.random.random() < exploration:
            available = expert_constraints or list(range(self.num_experts))
            selected = list(np.random.choice(
                available, size=min(self.top_k, len(available)), replace=False
            ))
            routing_decisions = [(idx, 1.0 / len(selected)) for idx in selected]
            
            metadata = {
                'exploratory': True,
                'exploration_rate': exploration,
                'method': 'bio_modulated_exploration'
            }
        else:
            # Standard routing with bio-inspired modulation
            with torch.set_grad_enabled(training):
                gradient_levels_dict = None
                token_state_dict = None
                
                if self.enable_bio_integration:
                    gradient_levels_dict = self._get_real_gradient_levels()
                    token_state_dict = self._get_real_token_state()
                
                routing_weights, expert_indices, aux_loss, metadata = self.sparse_gate(
                    x, training=training,
                    gradient_levels=gradient_levels_dict,
                    token_state=token_state_dict
                )
            
            # Extract results
            routing_weights = routing_weights.squeeze(0).detach().cpu().numpy()
            expert_indices = expert_indices.squeeze(0).detach().cpu().numpy()
            
            routing_decisions = list(zip(expert_indices, routing_weights))
        
        # BIO-INSPIRED: Apply expert bio-scores
        if self.enable_bio_integration:
            bio_scores = self._get_expert_bio_scores([d[0] for d in routing_decisions])
            routing_decisions = [
                (idx, weight * bio_scores.get(idx, 0.5))
                for idx, weight in routing_decisions
            ]
            
            # Renormalize weights
            total_weight = sum(w for _, w in routing_decisions)
            if total_weight > 0:
                routing_decisions = [(idx, w / total_weight) for idx, w in routing_decisions]
        
        # Apply constraints if provided
        if expert_constraints is not None:
            routing_decisions = [
                (idx, weight) for idx, weight in routing_decisions
                if idx in expert_constraints
            ]
            if not routing_decisions and expert_constraints:
                routing_decisions = [
                    (idx, 1.0 / len(expert_constraints))
                    for idx in expert_constraints
                ]
        
        # Update previous routing for temporal memory
        full_weights = torch.zeros(1, self.num_experts).to(self.device)
        for idx, weight in routing_decisions:
            if idx < self.num_experts:
                full_weights[0, idx] = weight
        self.previous_routing = full_weights
        
        # Update usage statistics
        for idx, _ in routing_decisions:
            self.expert_usage_count[idx] = self.expert_usage_count.get(idx, 0) + 1
        
        self.total_routing_calls += 1
        
        # BIO-INSPIRED: Update cooperative binding for pairs
        if len(routing_decisions) >= 2:
            for i, (idx_a, _) in enumerate(routing_decisions):
                for idx_b, _ in routing_decisions[i+1:]:
                    self._update_cooperative_binding(
                        idx_a, idx_b,
                        success=metadata.get('confidence', 0.5) > 0.6
                    )
        
        # Record routing for learning
        self._record_routing(context, routing_decisions, metadata)
        
        if return_metadata:
            metadata_dict = {
                'confidence': metadata.get('confidence', 0.5),
                'entropy': metadata.get('entropy', 0.0),
                'exploration_rate': exploration,
                'gradient_modulation': metadata.get('gradient_modulation', None),
                'token_affinity': metadata.get('token_affinity', None),
                'cooperative_matrix': metadata.get('cooperative_matrix', None),
                'bio_integration_active': self.enable_bio_integration
            }
            return routing_decisions, metadata_dict
        
        return routing_decisions
    
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
    
    # ========================================================================
    # Expert Statistics Methods
    # ========================================================================
    
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
                rates[idx] = 0.5
        return rates
    
    def get_load_balance_score(self) -> float:
        """Calculate load balance score (0-1)"""
        utilization = self.get_expert_utilization()
        if not utilization:
            return 0.0
        values = list(utilization.values())
        if sum(values) == 0:
            return 0.0
        entropy = 0
        for p in values:
            if p > 0:
                entropy -= p * math.log(p)
        max_entropy = math.log(len(values))
        return entropy / max_entropy if max_entropy > 0 else 0.0
    
    def get_comprehensive_stats(self) -> Dict[str, Any]:
        """Get comprehensive gating network statistics with bio-inspired metrics"""
        stats = {
            'total_routing_calls': self.total_routing_calls,
            'top_k': self.top_k,
            'num_experts': self.num_experts,
            'load_balance_score': self.get_load_balance_score(),
            'bio_integration_active': self.enable_bio_integration,
            'bio_modules_available': BIO_INSPIRED_AVAILABLE,
            'expert_stats': {
                idx: {
                    'utilization': self.get_expert_utilization().get(idx, 0.0),
                    'success_rate': self.get_expert_success_rates().get(idx, 0.0)
                }
                for idx in range(self.num_experts)
            }
        }
        
        # BIO-INSPIRED: Add gradient levels
        if self.enable_bio_integration:
            stats['gradient_levels'] = self._get_real_gradient_levels()
            stats['token_state'] = self._get_real_token_state()
            stats['exploration_rate'] = self._get_bio_modulated_exploration()
            stats['cooperative_pairs'] = len(self.cooperative_pairs)
            stats['biomass_aware_load_balance'] = self._get_biomass_aware_load_balance()
        
        return stats
    
    def update_routing_feedback(
        self,
        expert_id: int,
        reward: float,
        carbon_kg: float = 0.0,
        helium_units: float = 0.0,
        context: Optional['GatingContext'] = None
    ):
        """Update routing preferences based on feedback"""
        if reward > 0.5:
            self.expert_success_count[expert_id] = self.expert_success_count.get(expert_id, 0) + 1
        
        self.expert_carbon_total[expert_id] = self.expert_carbon_total.get(expert_id, 0.0) + carbon_kg
        self.expert_helium_total[expert_id] = self.expert_helium_total.get(expert_id, 0.0) + helium_units
        
        if context is not None:
            self.experience_buffer.append({
                'context': context,
                'chosen_expert': expert_id,
                'reward': reward,
                'carbon_kg': carbon_kg,
                'helium_units': helium_units,
                'timestamp': datetime.utcnow()
            })
            
            if len(self.experience_buffer) >= 32:
                self._online_learning_step()
    
    def _online_learning_step(self):
        """Perform one step of online learning"""
        if len(self.experience_buffer) < 32:
            return
        
        batch_size = min(32, len(self.experience_buffer))
        indices = np.random.choice(len(self.experience_buffer), batch_size, replace=False)
        batch = [self.experience_buffer[i] for i in indices]
        
        contexts = torch.stack([
            exp['context'].to_tensor() for exp in batch
        ]).to(self.device)
        
        chosen_experts = torch.tensor([
            exp['chosen_expert'] for exp in batch
        ]).to(self.device)
        
        rewards = torch.tensor([
            exp['reward'] for exp in batch
        ]).to(self.device)
        
        routing_weights, _, _, _ = self.sparse_gate(contexts, training=True)
        chosen_probs = routing_weights[range(batch_size), chosen_experts]
        loss = -torch.mean(torch.log(chosen_probs + 1e-8) * rewards)
        
        self.optimizer.zero_grad()
        loss.backward()
        torch.nn.utils.clip_grad_norm_(self.sparse_gate.parameters(), 1.0)
        self.optimizer.step()
    
    def save_state(self, path: str):
        """Save enhanced gating network state"""
        state = {
            'model_state_dict': self.sparse_gate.state_dict(),
            'optimizer_state_dict': self.optimizer.state_dict(),
            'expert_usage_count': dict(self.expert_usage_count),
            'expert_success_count': dict(self.expert_success_count),
            'total_routing_calls': self.total_routing_calls,
            'cooperative_pairs': dict(self.cooperative_pairs)
        }
        torch.save(state, path)
        logger.info(f"Saved gating network state to {path}")
    
    def load_state(self, path: str):
        """Load enhanced gating network state"""
        checkpoint = torch.load(path, map_location=self.device)
        self.sparse_gate.load_state_dict(checkpoint['model_state_dict'])
        self.optimizer.load_state_dict(checkpoint['optimizer_state_dict'])
        self.expert_usage_count = defaultdict(int, checkpoint.get('expert_usage_count', {}))
        self.expert_success_count = defaultdict(int, checkpoint.get('expert_success_count', {}))
        self.total_routing_calls = checkpoint.get('total_routing_calls', 0)
        self.cooperative_pairs = defaultdict(float, checkpoint.get('cooperative_pairs', {}))
        logger.info(f"Loaded gating network state from {path}")
    
    def get_parameter_count(self) -> int:
        """Get total number of trainable parameters"""
        return sum(p.numel() for p in self.sparse_gate.parameters() if p.requires_grad)
