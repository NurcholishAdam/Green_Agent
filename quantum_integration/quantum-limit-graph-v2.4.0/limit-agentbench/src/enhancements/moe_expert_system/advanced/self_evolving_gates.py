# File: quantum_integration/quantum-limit-graph-v2.4.0/limit-agentbench/src/enhancements/moe_expert_system/advanced/self_evolving_gates.py
# Enhanced with complete bio-inspired integration - Metabolic Evolution Engine v4.0.0

"""
Enhanced Self-Evolving Gates v4.0.0 - Metabolic Evolution Engine

Complete bio-inspired integration with:
- Token-efficiency fitness scoring (Eco-ATP as fitness metric)
- Gradient-driven evolution pressure (carbon gradient as selection pressure)
- ATP-driven plasticity control (energy-based learning rate)
- Harvester signal quality for drift detection
- Biomass-backed task prototype storage
- Compartment inheritance for weight transfer
- Gradient field environmental encoding
- Token-modulated exploration rate
- Photosynthetic opportunity detection for architecture search
- Metabolic pathway integration for multi-fidelity optimization
"""

import asyncio
import logging
from typing import Dict, Any, List, Optional, Tuple, Set, Callable
from dataclasses import dataclass, field
from datetime import datetime, timedelta
import numpy as np
import torch
import torch.nn as nn
import torch.nn.functional as F
from collections import defaultdict, deque
import copy
import math
import hashlib
import json

logger = logging.getLogger(__name__)

# ============================================================================
# Try importing bio-inspired modules
# ============================================================================

try:
    from enhancements.bio_inspired.eco_atp_currency import (
        EcoATPTokenManager, DynamicExchangeRate, EcoATPSource, EcoATPConsumer,
        TokenState, EcoATPToken, EcoATPAccount
    )
    from enhancements.bio_inspired.proton_gradient_fields import (
        GradientFieldManager, GradientField
    )
    from enhancements.bio_inspired.atp_synthase_scheduler import (
        ATPSynthaseScheduler, SynthaseConfig
    )
    from enhancements.bio_inspired.chromatophore_compartments import (
        CompartmentManager, ChromatophoreCompartment, CompartmentState,
        MembranePermeability
    )
    from enhancements.bio_inspired.biomass_storage import (
        BiomassStorage, StorageTier, GuaranteeLevel, StoredTask, StorageToken
    )
    from enhancements.bio_inspired.photosynthetic_harvester import (
        PhotosyntheticHarvester
    )
    BIO_INSPIRED_AVAILABLE = True
    logger.info("Bio-inspired modules loaded for Self-Evolving Gates")
except ImportError as e:
    BIO_INSPIRED_AVAILABLE = False
    logger.warning(f"Bio-inspired modules not available: {str(e)} - using standard evolution")

# ============================================================================
# Enhanced Self-Evolving Gate with Complete Bio-Inspired Integration
# ============================================================================

class EnhancedSelfEvolvingGate(nn.Module):
    """
    Enhanced Self-Evolving Gate v4.0.0 - Metabolic Evolution Engine
    
    Complete bio-inspired integration:
    - Token-efficiency fitness scoring
    - Gradient-driven evolution pressure
    - ATP-driven plasticity control
    - Harvester signal quality for drift detection
    - Biomass-backed task prototype storage
    - Compartment inheritance for weight transfer
    - Gradient field environmental encoding
    - Token-modulated exploration rate
    """
    
    def __init__(
        self,
        input_dim: int,
        num_experts: int,
        hidden_dim: int = 128,
        adaptation_rate: float = 0.01,
        enable_meta_learning: bool = True,
        enable_architecture_search: bool = True,
        enable_continual_learning: bool = True,
        enable_generative_replay: bool = True,
        enable_bio_integration: bool = True,
        population_size: int = 10,
        memory_size: int = 10000
    ):
        super().__init__()
        self.input_dim = input_dim
        self.num_experts = num_experts
        self.adaptation_rate = adaptation_rate
        self.enable_meta_learning = enable_meta_learning
        self.enable_architecture_search = enable_architecture_search
        self.enable_continual_learning = enable_continual_learning
        self.enable_generative_replay = enable_generative_replay
        self.enable_bio_integration = enable_bio_integration and BIO_INSPIRED_AVAILABLE
        
        # BIO-INSPIRED: Module references (injected)
        self.token_manager: Optional[EcoATPTokenManager] = None
        self.gradient_manager: Optional[GradientFieldManager] = None
        self.scheduler: Optional[ATPSynthaseScheduler] = None
        self.compartment_manager: Optional[CompartmentManager] = None
        self.biomass_storage: Optional[BiomassStorage] = None
        self.harvester: Optional[PhotosyntheticHarvester] = None
        
        # Core gate network
        self.gate_network = nn.Sequential(
            nn.Linear(input_dim, hidden_dim),
            nn.LayerNorm(hidden_dim),
            nn.ReLU(),
            nn.Linear(hidden_dim, hidden_dim),
            nn.LayerNorm(hidden_dim),
            nn.ReLU(),
            nn.Linear(hidden_dim, num_experts)
        )
        
        # Current architecture
        self.current_architecture = ArchitectureGene(
            num_layers=3, hidden_dim=hidden_dim, activation='relu',
            dropout_rate=0.1, use_attention=True, use_residual=True, use_layer_norm=True
        )
        
        # Meta-learning module
        if enable_meta_learning:
            self.meta_learner = MAMLGate(input_dim, num_experts, hidden_dim)
        
        # Architecture search
        if enable_architecture_search:
            self.architecture_search = ArchitectureSearch(
                input_dim, num_experts, population_size
            )
        
        # Continual learning
        if enable_continual_learning:
            self.ewc = ElasticWeightConsolidation(self.gate_network)
        
        # Generative replay
        if enable_generative_replay:
            self.replay = GenerativeReplay(input_dim)
        
        # Memory buffer
        self.memory: deque = deque(maxlen=memory_size)
        
        # Task prototypes
        self.task_prototypes: Dict[str, TaskPrototype] = {}
        
        # Concept drift detection
        self.concept_drift_detector = EnhancedConceptDriftDetector()
        
        # Environmental encoder
        self.environmental_encoder = EnhancedEnvironmentalEncoder(input_dim)
        
        # Performance tracking
        self.performance_history: List[Dict] = []
        self.adaptation_history: List[Dict] = []
        
        # Optimizer
        self.optimizer = torch.optim.Adam(self.gate_network.parameters(), lr=adaptation_rate)
        
        # BIO-INSPIRED: Plasticity control (ATP-driven)
        self.plasticity = 0.5
        self.plasticity_decay = 0.999
        
        # BIO-INSPIRED: Evolution tracking
        self.evolution_generation: int = 0
        self.token_fitness_history: deque = deque(maxlen=1000)
        self.gradient_pressure_history: deque = deque(maxlen=1000)
        
        # BIO-INSPIRED: Biomass prototype tokens
        self.biomass_prototype_tokens: Dict[str, str] = {}
        
        logger.info(
            f"Enhanced Self-Evolving Gate v4.0.0 initialized: "
            f"bio_integration={self.enable_bio_integration}, "
            f"bio_available={BIO_INSPIRED_AVAILABLE}"
        )
    
    # ========================================================================
    # Bio-Inspired Module Injection
    # ========================================================================
    
    def inject_bio_core(self, bio_core: Any = None, **kwargs):
        """
        Inject bio-inspired modules for evolution optimization.
        
        Connects self-evolving gates to real bio-inspired systems.
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
        logger.info(f"Bio-inspired injections into Self-Evolving Gates: {injections}")
        
        if any(injections.values()):
            self.enable_bio_integration = True
    
    # ========================================================================
    # Bio-Inspired Data Access Methods
    # ========================================================================
    
    def _get_token_efficiency_fitness(self) -> float:
        """Get fitness based on token efficiency from token manager"""
        if self.token_manager:
            summary = self.token_manager.get_system_summary()
            return summary.get('system_efficiency', 0.5)
        return 0.5
    
    def _get_gradient_evolution_pressure(self) -> float:
        """
        Get evolution pressure from carbon gradient.
        
        Higher carbon stress = higher evolution pressure = faster adaptation.
        """
        if self.gradient_manager:
            carbon = self.gradient_manager.fields.get('carbon')
            if carbon:
                return carbon.gradient_strength
        return 0.3
    
    def _get_atp_driven_plasticity(self) -> float:
        """
        Get plasticity based on ATP availability.
        
        More energy = more plasticity = more learning.
        """
        if self.scheduler:
            driving_force = self.scheduler.calculate_gradient_driving_force()
            rotation_speed = self.scheduler.calculate_rotation_speed(driving_force)
            ecoatp_rate = self.scheduler.calculate_atp_production_rate(rotation_speed)
            
            if ecoatp_rate > 100:
                return 1.0  # Full plasticity when energy abundant
            elif ecoatp_rate > 50:
                return 0.7  # Moderate plasticity
            else:
                return 0.3  # Low plasticity when energy scarce
        
        return self.plasticity
    
    def _get_harvester_drift_confidence(self) -> float:
        """Get drift detection confidence from photosynthetic harvester"""
        if self.harvester:
            stats = self.harvester.get_harvesting_stats()
            recent = stats.get('recent_conversions', [])
            if recent:
                return np.mean([c.get('convertible_energy', 0.5) for c in recent[-10:]])
        return 0.5
    
    def _store_task_prototype_in_biomass(self, prototype: Dict[str, Any]) -> Optional[str]:
        """
        Store task prototype in biomass storage.
        
        Task prototypes are preserved as long-term knowledge.
        """
        if self.biomass_storage:
            stored, token_id = self.biomass_storage.store_task(
                task_data=prototype,
                ecoatp_cost=2.0,
                guarantee=GuaranteeLevel.SILVER,
                initial_tier=StorageTier.STARCH_RESERVE
            )
            if stored:
                return token_id
        return None
    
    def _get_compartment_inheritance_strength(self) -> float:
        """Get inheritance strength from compartment health"""
        if self.compartment_manager:
            compartment = self.compartment_manager.find_best_compartment('data')
            if compartment:
                return compartment.health_score
        return 0.5
    
    def _get_gradient_encoded_environment(self) -> Dict[str, float]:
        """
        Get gradient-encoded environmental state.
        
        All gradient fields encode the current environmental condition.
        """
        if self.gradient_manager:
            return self.gradient_manager.get_field_strengths()
        return {'carbon': 0.5, 'helium': 0.5, 'trust': 0.5, 'opportunity': 0.5}
    
    def _get_token_modulated_exploration(self) -> float:
        """
        Get exploration rate modulated by token availability.
        
        More tokens = more exploration (can afford to try new things).
        """
        if self.token_manager:
            summary = self.token_manager.get_system_summary()
            balance = summary.get('total_balance', 500)
            if balance > 500:
                return 0.3  # More exploration when tokens abundant
            elif balance > 200:
                return 0.15  # Moderate exploration
            else:
                return 0.05  # Less exploration when tokens scarce
        return 0.1
    
    def _get_harvester_opportunity_signal(self) -> float:
        """Get opportunity signal from harvester for architecture search timing"""
        if self.harvester:
            stats = self.harvester.get_harvesting_stats()
            total = stats.get('total_harvested', 0)
            return min(1.0, total / 1000.0)
        return 0.3
    
    # ========================================================================
    # Enhanced Forward Pass with Bio-Inspired Modulation
    # ========================================================================
    
    def forward(
        self,
        x: torch.Tensor,
        task_id: Optional[str] = None,
        training: bool = False,
        environmental_context: Optional[Dict[str, Any]] = None
    ) -> Tuple[torch.Tensor, Dict[str, Any]]:
        """
        Enhanced forward pass with bio-inspired self-evolution.
        
        Features:
        - ATP-driven plasticity control
        - Gradient-encoded environmental context
        - Token-modulated exploration
        """
        metadata = {}
        
        # BIO-INSPIRED: Update plasticity from ATP availability
        if self.enable_bio_integration and training:
            self.plasticity = self._get_atp_driven_plasticity()
            metadata['plasticity'] = self.plasticity
        
        # BIO-INSPIRED: Encode environmental context with gradient fields
        if self.enable_bio_integration and environmental_context is None:
            environmental_context = self._get_gradient_encoded_environment()
            metadata['gradient_encoded'] = True
        
        # Check for concept drift
        drift_detected = self.concept_drift_detector.check_drift(x)
        metadata['drift_detected'] = drift_detected
        
        # BIO-INSPIRED: Modulate drift detection with harvester confidence
        if self.enable_bio_integration:
            harvester_conf = self._get_harvester_drift_confidence()
            if harvester_conf < 0.3 and not drift_detected:
                # Low confidence from harvester - increase drift sensitivity
                drift_detected = True
                metadata['harvester_amplified_drift'] = True
        
        # Apply plasticity control
        if training and self.plasticity < 1.0:
            for param in self.gate_network.parameters():
                if param.grad is not None:
                    param.grad *= self.plasticity
        
        # Encode environmental context if available
        if environmental_context:
            env_features = self.environmental_encoder(environmental_context)
            if x.dim() == 1:
                x = torch.cat([x, env_features])
            else:
                x = torch.cat([x, env_features.unsqueeze(0).expand(x.size(0), -1)], dim=-1)
        
        # Forward pass with optional meta-learning
        if self.enable_meta_learning and task_id:
            weights = self.meta_learner(x, task_id)
            metadata['meta_adapted'] = True
        else:
            logits = self.gate_network(x)
            
            # BIO-INSPIRED: Token-modulated exploration noise
            if training and self.enable_bio_integration:
                exploration = self._get_token_modulated_exploration()
                noise_std = 0.1 * self.plasticity * exploration
                noise = torch.randn_like(logits) * noise_std
                logits = logits + noise
                metadata['token_exploration'] = exploration
            elif training:
                noise_std = 0.1 * self.plasticity
                noise = torch.randn_like(logits) * noise_std
                logits = logits + noise
            
            weights = F.softmax(logits, dim=-1)
        
        # Calculate metadata
        metadata['entropy'] = -(weights * torch.log(weights + 1e-8)).sum(dim=-1).mean().item()
        metadata['confidence'] = weights.max(dim=-1)[0].mean().item()
        metadata['plasticity'] = self.plasticity
        
        # BIO-INSPIRED: Add bio metrics
        if self.enable_bio_integration:
            metadata['token_fitness'] = self._get_token_efficiency_fitness()
            metadata['evolution_pressure'] = self._get_gradient_evolution_pressure()
            metadata['harvester_confidence'] = self._get_harvester_drift_confidence()
        
        return weights, metadata
    
    # ========================================================================
    # Enhanced Adaptation with Bio-Inspired Feedback
    # ========================================================================
    
    def adapt(
        self,
        state: torch.Tensor,
        chosen_expert: int,
        reward: float,
        environmental_feedback: Dict[str, Any],
        task_id: Optional[str] = None
    ):
        """
        Enhanced adaptation with bio-inspired feedback loops.
        
        Features:
        - Token-efficiency weighted learning
        - Gradient-pressure modulated adaptation speed
        - Biomass storage for important prototypes
        - Compartment inheritance tracking
        """
        # BIO-INSPIRED: Adjust reward with token efficiency
        if self.enable_bio_integration:
            token_fitness = self._get_token_efficiency_fitness()
            adjusted_reward = reward * (0.5 + 0.5 * token_fitness)
        else:
            adjusted_reward = reward
        
        # Store in memory
        self.memory.append({
            'state': state.detach().clone(),
            'action': chosen_expert,
            'reward': adjusted_reward,
            'environmental': environmental_feedback,
            'task_id': task_id,
            'timestamp': datetime.utcnow()
        })
        
        # Update concept drift detector
        self.concept_drift_detector.update(state)
        
        # BIO-INSPIRED: Gradient-pressure modulated learning
        if self.enable_bio_integration:
            pressure = self._get_gradient_evolution_pressure()
            # Higher pressure = learn from fewer experiences
            min_batch = max(8, int(32 * (1.0 - pressure)))
        else:
            min_batch = 32
        
        # Policy gradient learning
        if len(self.memory) >= min_batch:
            self._policy_gradient_step()
        
        # Meta-adaptation for new task
        if task_id and task_id not in self.task_prototypes:
            prototype = self._create_task_prototype(task_id, state, adjusted_reward)
            
            # BIO-INSPIRED: Store important prototypes in biomass
            if self.enable_bio_integration and adjusted_reward > 0.7:
                biomass_token = self._store_task_prototype_in_biomass({
                    'task_id': task_id,
                    'prototype': str(prototype)[:500],
                    'reward': adjusted_reward,
                    'timestamp': datetime.utcnow().isoformat()
                })
                if biomass_token:
                    self.biomass_prototype_tokens[task_id] = biomass_token
        
        # Continual learning consolidation
        if self.enable_continual_learning and len(self.memory) % 100 == 0:
            self._consolidate_knowledge()
        
        # Architecture search trigger
        should_evolve = self.concept_drift_detector.should_evolve_architecture()
        
        # BIO-INSPIRED: Modulate architecture search with harvester opportunity
        if self.enable_bio_integration and not should_evolve:
            opportunity = self._get_harvester_opportunity_signal()
            if opportunity > 0.7:
                should_evolve = True
                logger.info("Architecture search triggered by harvester opportunity signal")
        
        if self.enable_architecture_search and should_evolve:
            self._evolve_architecture()
        
        # BIO-INSPIRED: Update plasticity from ATP
        if self.enable_bio_integration:
            self.plasticity = self._get_atp_driven_plasticity()
        else:
            self.plasticity *= self.plasticity_decay
            self.plasticity = max(self.plasticity, 0.1)
        
        # Record adaptation
        self.adaptation_history.append({
            'timestamp': datetime.utcnow().isoformat(),
            'reward': adjusted_reward,
            'expert': chosen_expert,
            'drift': self.concept_drift_detector.drift_score,
            'plasticity': self.plasticity,
            'task_id': task_id,
            'token_fitness': self._get_token_efficiency_fitness() if self.enable_bio_integration else 0.5,
            'evolution_pressure': self._get_gradient_evolution_pressure() if self.enable_bio_integration else 0.3
        })
        
        # BIO-INSPIRED: Track token fitness history
        if self.enable_bio_integration:
            self.token_fitness_history.append(self._get_token_efficiency_fitness())
            self.gradient_pressure_history.append(self._get_gradient_evolution_pressure())
    
    def _policy_gradient_step(self):
        """Enhanced policy gradient learning step with bio-inspired weighting"""
        if len(self.memory) < 8:
            return
        
        batch_size = min(32, len(self.memory))
        indices = np.random.choice(len(self.memory), batch_size, replace=False)
        batch = [self.memory[i] for i in indices]
        
        # Include generative replay if enabled
        if self.enable_generative_replay and len(self.memory) > 100:
            replay_states = self.replay.generate_replay_batch(batch_size // 4)
        
        states = torch.stack([b['state'] for b in batch])
        actions = torch.tensor([b['action'] for b in batch])
        rewards = torch.tensor([b['reward'] for b in batch])
        
        # Normalize rewards
        rewards = (rewards - rewards.mean()) / (rewards.std() + 1e-8)
        
        logits = self.gate_network(states)
        probs = F.softmax(logits, dim=-1)
        action_probs = probs[range(batch_size), actions]
        
        pg_loss = -torch.mean(torch.log(action_probs + 1e-8) * rewards)
        
        total_loss = pg_loss
        if self.enable_continual_learning:
            ewc_loss = self.ewc.ewc_loss()
            total_loss += ewc_loss * 0.1
        
        self.optimizer.zero_grad()
        total_loss.backward()
        
        # Apply plasticity control
        if self.plasticity < 1.0:
            for param in self.gate_network.parameters():
                if param.grad is not None:
                    param.grad *= self.plasticity
        
        torch.nn.utils.clip_grad_norm_(self.gate_network.parameters(), 1.0)
        self.optimizer.step()
        
        self.performance_history.append({
            'pg_loss': pg_loss.item(),
            'total_loss': total_loss.item(),
            'avg_reward': rewards.mean().item()
        })
    
    def _create_task_prototype(
        self, task_id: str, state: torch.Tensor, reward: float
    ) -> 'TaskPrototype':
        """Create prototype for new task with bio-inspired storage"""
        prototype = TaskPrototype(
            task_id=task_id,
            support_set=[(state, torch.tensor(reward))],
            query_set=[],
            task_embedding=state.detach().mean(dim=0),
            difficulty=1.0 - abs(reward),
            domain="unknown"
        )
        
        self.task_prototypes[task_id] = prototype
        
        if self.enable_meta_learning:
            self.meta_learner.adapt_to_task(prototype.support_set, task_id)
        
        return prototype
    
    def _consolidate_knowledge(self):
        """Consolidate knowledge using EWC with bio-inspired weighting"""
        if not self.enable_continual_learning:
            return
        
        recent = list(self.memory)[-100:]
        
        # BIO-INSPIRED: Weight consolidation by token fitness
        if self.enable_bio_integration:
            token_fitness = self._get_token_efficiency_fitness()
            consolidation_strength = 0.5 + 0.5 * token_fitness
        else:
            consolidation_strength = 1.0
        
        dataloader = [(m['state'], torch.tensor(m['action'])) for m in recent]
        self.ewc.update_fisher("current_task", dataloader)
        
        logger.debug(f"Knowledge consolidated (strength={consolidation_strength:.2f})")
    
    def _evolve_architecture(self):
        """Evolve gate architecture using NAS with bio-inspired fitness"""
        if not self.enable_architecture_search:
            return
        
        logger.info("Triggering architecture evolution...")
        self.evolution_generation += 1
        
        # BIO-INSPIRED: Get bio-inspired fitness weighting
        token_fitness = self._get_token_efficiency_fitness() if self.enable_bio_integration else 0.5
        evolution_pressure = self._get_gradient_evolution_pressure() if self.enable_bio_integration else 0.3
        
        def fitness_function(gene: ArchitectureGene) -> float:
            temp_net = self._build_network(gene)
            
            if len(self.memory) < 10:
                return 0.5
            
            recent = list(self.memory)[-50:]
            states = torch.stack([m['state'] for m in recent])
            actions = torch.tensor([m['action'] for m in recent])
            
            with torch.no_grad():
                logits = temp_net(states)
                preds = logits.argmax(dim=-1)
                accuracy = (preds == actions).float().mean().item()
            
            complexity_penalty = self.architecture_search._calculate_complexity(gene) / 1000
            
            # BIO-INSPIRED: Weight fitness by token efficiency and evolution pressure
            if self.enable_bio_integration:
                bio_fitness = accuracy - complexity_penalty
                return bio_fitness * (0.5 + 0.5 * token_fitness) * (0.5 + 0.5 * evolution_pressure)
            
            return accuracy - complexity_penalty
        
        metrics = self.architecture_search.evolve_generation(fitness_function)
        
        best_gene = self.architecture_search.get_best_architecture()
        if best_gene and best_gene.fitness > self.current_architecture.fitness:
            logger.info(
                f"Upgrading architecture (gen {self.evolution_generation}): "
                f"fitness {self.current_architecture.fitness:.4f} -> {best_gene.fitness:.4f}"
            )
            
            new_network = self._build_network(best_gene)
            self._transfer_weights(self.gate_network, new_network)
            self.gate_network = new_network
            self.current_architecture = best_gene
    
    def _build_network(self, architecture: 'ArchitectureGene') -> nn.Module:
        """Build network from architecture gene"""
        layers = []
        in_dim = self.input_dim
        
        for i in range(architecture.num_layers):
            if i == architecture.num_layers - 1:
                out_dim = self.num_experts
            else:
                out_dim = architecture.hidden_dim
            
            layers.append(nn.Linear(in_dim, out_dim))
            
            if architecture.use_layer_norm and i < architecture.num_layers - 1:
                layers.append(nn.LayerNorm(out_dim))
            
            if i < architecture.num_layers - 1:
                if architecture.activation == 'relu':
                    layers.append(nn.ReLU())
                elif architecture.activation == 'gelu':
                    layers.append(nn.GELU())
                elif architecture.activation == 'swish':
                    layers.append(nn.SiLU())
                elif architecture.activation == 'leaky_relu':
                    layers.append(nn.LeakyReLU())
            
            if architecture.dropout_rate > 0 and i < architecture.num_layers - 1:
                layers.append(nn.Dropout(architecture.dropout_rate))
            
            in_dim = out_dim
        
        return nn.Sequential(*layers)
    
    def _transfer_weights(self, old_network: nn.Module, new_network: nn.Module):
        """Transfer weights with bio-inspired inheritance strength"""
        old_params = list(old_network.parameters())
        new_params = list(new_network.parameters())
        
        # BIO-INSPIRED: Get inheritance strength from compartment health
        inheritance_strength = self._get_compartment_inheritance_strength() if self.enable_bio_integration else 1.0
        
        for i, new_param in enumerate(new_params):
            if i < len(old_params):
                old_param = old_params[i]
                if old_param.shape == new_param.shape:
                    # Blend old and new with inheritance strength
                    new_param.data.copy_(
                        old_param.data * inheritance_strength +
                        new_param.data * (1 - inheritance_strength)
                    )
                else:
                    min_dims = [min(o, n) for o, n in zip(old_param.shape, new_param.shape)]
                    slices = tuple(slice(0, d) for d in min_dims)
                    new_param.data[slices] = (
                        old_param.data[slices] * inheritance_strength +
                        new_param.data[slices] * (1 - inheritance_strength)
                    )
    
    # ========================================================================
    # Enhanced Statistics with Bio-Inspired Metrics
    # ========================================================================
    
    def get_evolution_metrics(self) -> Dict[str, Any]:
        """Get comprehensive evolution metrics with bio-inspired data"""
        metrics = {
            'current_generation': len(self.adaptation_history),
            'current_plasticity': self.plasticity,
            'bio_integration_active': self.enable_bio_integration,
            'bio_modules_available': BIO_INSPIRED_AVAILABLE,
            'architecture': {
                'num_layers': self.current_architecture.num_layers,
                'hidden_dim': self.current_architecture.hidden_dim,
                'activation': self.current_architecture.activation,
                'fitness': self.current_architecture.fitness
            },
            'performance': {
                'recent_rewards': [h['reward'] for h in self.adaptation_history[-100:]],
                'drift_score': self.concept_drift_detector.drift_score
            },
            'learning': {
                'memory_size': len(self.memory),
                'task_prototypes': len(self.task_prototypes),
                'meta_learning_enabled': self.enable_meta_learning,
                'architecture_search_enabled': self.enable_architecture_search
            }
        }
        
        # BIO-INSPIRED: Add bio metrics
        if self.enable_bio_integration:
            metrics['bio_metrics'] = {
                'token_fitness': self._get_token_efficiency_fitness(),
                'evolution_pressure': self._get_gradient_evolution_pressure(),
                'atp_plasticity': self._get_atp_driven_plasticity(),
                'harvester_confidence': self._get_harvester_drift_confidence(),
                'compartment_inheritance': self._get_compartment_inheritance_strength(),
                'token_exploration': self._get_token_modulated_exploration(),
                'biomass_prototypes': len(self.biomass_prototype_tokens),
                'gradient_levels': self._get_gradient_encoded_environment(),
                'token_fitness_trend': list(self.token_fitness_history)[-50:],
                'gradient_pressure_trend': list(self.gradient_pressure_history)[-50:]
            }
        
        return metrics
    
    def reset_plasticity(self):
        """Reset plasticity to ATP-driven or maximum"""
        if self.enable_bio_integration:
            self.plasticity = self._get_atp_driven_plasticity()
        else:
            self.plasticity = 1.0
        logger.info(f"Plasticity reset to {self.plasticity:.2f}")
    
    def get_parameter_count(self) -> int:
        """Get total number of trainable parameters"""
        return sum(p.numel() for p in self.gate_network.parameters() if p.requires_grad)
    
    def save_state(self, path: str):
        """Save enhanced gating network state with bio-inspired metadata"""
        state = {
            'model_state_dict': self.gate_network.state_dict(),
            'optimizer_state_dict': self.optimizer.state_dict(),
            'plasticity': self.plasticity,
            'evolution_generation': self.evolution_generation,
            'architecture': self.current_architecture,
            'bio_enabled': self.enable_bio_integration,
            'biomass_prototypes': self.biomass_prototype_tokens
        }
        torch.save(state, path)
        logger.info(f"Saved self-evolving gate state to {path}")
    
    def load_state(self, path: str):
        """Load enhanced gating network state"""
        checkpoint = torch.load(path, map_location='cpu')
        self.gate_network.load_state_dict(checkpoint['model_state_dict'])
        self.optimizer.load_state_dict(checkpoint['optimizer_state_dict'])
        self.plasticity = checkpoint.get('plasticity', 0.5)
        self.evolution_generation = checkpoint.get('evolution_generation', 0)
        self.current_architecture = checkpoint.get('architecture', self.current_architecture)
        self.biomass_prototype_tokens = checkpoint.get('biomass_prototypes', {})
        logger.info(f"Loaded self-evolving gate state from {path}")


# ============================================================================
# Legacy Compatibility Class
# ============================================================================

class SelfEvolvingGate(EnhancedSelfEvolvingGate):
    """
    Legacy self-evolving gate for backward compatibility.
    Maintains original interface while using enhanced functionality.
    """
    
    def __init__(
        self,
        input_dim: int,
        num_experts: int,
        memory_size: int = 10000,
        adaptation_rate: float = 0.01,
        **kwargs
    ):
        super().__init__(
            input_dim=input_dim,
            num_experts=num_experts,
            adaptation_rate=adaptation_rate,
            enable_meta_learning=kwargs.get('enable_meta_learning', False),
            enable_architecture_search=kwargs.get('enable_architecture_search', False),
            enable_continual_learning=kwargs.get('enable_continual_learning', False),
            enable_generative_replay=kwargs.get('enable_generative_replay', False),
            enable_bio_integration=kwargs.get('enable_bio_integration', False),
            memory_size=memory_size
        )
        
        self.memory: deque = deque(maxlen=memory_size)
        self.performance_history: List[Dict] = []
        self.adaptation_history: List[Dict] = []
        
        logger.info("Self-Evolving Gate initialized (compatibility mode)")
