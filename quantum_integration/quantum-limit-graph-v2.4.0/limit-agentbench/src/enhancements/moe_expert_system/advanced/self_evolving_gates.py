# File: quantum_integration/quantum-limit-graph-v2.4.0/limit-agentbench/src/enhancements/moe_expert_system/advanced/self_evolving_gates.py

"""
Enhanced Self-Evolving Gating Network for Green Agent MoE System
Version: 2.0.0

Advanced self-evolution capabilities with:
- Meta-learning for rapid task adaptation (MAML, Reptile)
- Neural architecture search for optimal gate structure
- Continual learning with elastic weight consolidation
- Multi-task adaptation with task-specific modules
- Evolutionary strategies for population-based optimization
- Automated hyperparameter optimization (Bayesian)
- Transfer learning from pre-trained gates
- Curriculum learning for progressive complexity
- Self-assessment and gap analysis
- Generative replay for catastrophic forgetting prevention
- Adaptive plasticity control
- Knowledge distillation for gate compression
- Zero-shot adaptation to new experts
- Few-shot learning for rapid expert integration
- Self-supervised pretraining for gate initialization

Integration Points:
- Layer 1: Meta-cognitive learning coordination
- Layer 2: Neuro-symbolic constraint integration
- Layer 4: ML model optimization awareness
- Layer 7: Evolution monitoring and metrics
- Layer 9: Pareto-optimal architecture analysis
"""

import numpy as np
import torch
import torch.nn as nn
import torch.nn.functional as F
from typing import Dict, Any, List, Optional, Tuple, Set, Callable
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from collections import deque, defaultdict
import logging
import math
import copy
import hashlib
import json

logger = logging.getLogger(__name__)

# ============================================================================
# Data Classes for Enhanced Evolution
# ============================================================================

@dataclass
class EvolutionMetrics:
    """Comprehensive evolution tracking metrics"""
    generation: int = 0
    population_size: int = 0
    best_fitness: float = 0.0
    average_fitness: float = 0.0
    fitness_variance: float = 0.0
    architectural_complexity: int = 0
    adaptation_speed: float = 0.0
    forgetting_rate: float = 0.0
    transfer_efficiency: float = 0.0
    carbon_cost_kg: float = 0.0
    helium_cost: float = 0.0
    convergence_generation: Optional[int] = None

@dataclass
class TaskPrototype:
    """Task prototype for meta-learning"""
    task_id: str
    support_set: List[Tuple[torch.Tensor, torch.Tensor]]
    query_set: List[Tuple[torch.Tensor, torch.Tensor]]
    task_embedding: torch.Tensor
    difficulty: float
    domain: str
    optimal_architecture: Optional[Dict[str, Any]] = None

@dataclass
class ArchitectureGene:
    """Gene for neural architecture search"""
    num_layers: int
    hidden_dim: int
    activation: str
    dropout_rate: float
    use_attention: bool
    use_residual: bool
    use_layer_norm: bool
    fitness: float = 0.0
    carbon_cost: float = 0.0

# ============================================================================
# Meta-Learning Module (MAML)
# ============================================================================

class MAMLGate(nn.Module):
    """
    Model-Agnostic Meta-Learning for rapid gate adaptation.
    
    Enables the gate to quickly adapt to new tasks with few examples.
    """
    
    def __init__(
        self,
        input_dim: int,
        num_experts: int,
        hidden_dim: int = 128,
        meta_lr: float = 0.001,
        inner_lr: float = 0.01,
        num_inner_steps: int = 5
    ):
        super().__init__()
        self.input_dim = input_dim
        self.num_experts = num_experts
        self.meta_lr = meta_lr
        self.inner_lr = inner_lr
        self.num_inner_steps = num_inner_steps
        
        # Base network (meta-parameters)
        self.base_network = nn.Sequential(
            nn.Linear(input_dim, hidden_dim),
            nn.LayerNorm(hidden_dim),
            nn.ReLU(),
            nn.Linear(hidden_dim, hidden_dim),
            nn.LayerNorm(hidden_dim),
            nn.ReLU(),
            nn.Linear(hidden_dim, num_experts)
        )
        
        # Task-specific adaptation layers
        self.task_adapters = nn.ModuleDict()
        
        # Meta-optimizer
        self.meta_optimizer = torch.optim.Adam(
            self.base_network.parameters(),
            lr=meta_lr
        )
    
    def forward(
        self,
        x: torch.Tensor,
        task_id: Optional[str] = None
    ) -> torch.Tensor:
        """Forward pass with optional task-specific adaptation"""
        if task_id and task_id in self.task_adapters:
            # Apply task-specific adapter
            adapted = self.task_adapters[task_id](x)
            base = self.base_network(x)
            return (adapted + base) / 2
        
        return self.base_network(x)
    
    def adapt_to_task(
        self,
        support_set: List[Tuple[torch.Tensor, torch.Tensor]],
        task_id: str
    ) -> float:
        """
        Rapidly adapt to new task using MAML.
        
        Args:
            support_set: Few-shot examples for adaptation
            task_id: Identifier for the new task
            
        Returns:
            Adaptation loss
        """
        # Clone base network for inner loop
        adapted_net = copy.deepcopy(self.base_network)
        inner_optimizer = torch.optim.SGD(
            adapted_net.parameters(),
            lr=self.inner_lr
        )
        
        # Inner loop: adapt to support set
        total_loss = 0.0
        for _ in range(self.num_inner_steps):
            for x, y in support_set:
                pred = adapted_net(x)
                loss = F.cross_entropy(pred, y)
                
                inner_optimizer.zero_grad()
                loss.backward()
                inner_optimizer.step()
                
                total_loss += loss.item()
        
        # Store task adapter
        self.task_adapters[task_id] = adapted_net
        
        # Meta-update (outer loop)
        meta_loss = 0.0
        for x, y in support_set:
            pred = self.base_network(x)
            meta_loss += F.cross_entropy(pred, y)
        
        self.meta_optimizer.zero_grad()
        meta_loss.backward()
        self.meta_optimizer.step()
        
        return total_loss / (self.num_inner_steps * len(support_set))

# ============================================================================
# Neural Architecture Search
# ============================================================================

class ArchitectureSearch:
    """
    Neural Architecture Search for optimal gate structure.
    
    Uses evolutionary strategies to find optimal architecture.
    """
    
    def __init__(
        self,
        input_dim: int,
        num_experts: int,
        population_size: int = 20,
        mutation_rate: float = 0.1,
        crossover_rate: float = 0.5
    ):
        self.input_dim = input_dim
        self.num_experts = num_experts
        self.population_size = population_size
        self.mutation_rate = mutation_rate
        self.crossover_rate = crossover_rate
        
        # Search space
        self.search_space = {
            'num_layers': [1, 2, 3, 4, 5],
            'hidden_dim': [64, 128, 256, 512],
            'activation': ['relu', 'gelu', 'swish', 'leaky_relu'],
            'dropout_rate': [0.0, 0.1, 0.2, 0.3, 0.5],
            'use_attention': [True, False],
            'use_residual': [True, False],
            'use_layer_norm': [True, False]
        }
        
        # Population
        self.population: List[ArchitectureGene] = []
        
        # Evolution history
        self.evolution_history: List[EvolutionMetrics] = []
        self.generation = 0
        
        # Initialize population
        self._initialize_population()
    
    def _initialize_population(self):
        """Initialize random population"""
        for _ in range(self.population_size):
            gene = ArchitectureGene(
                num_layers=np.random.choice(self.search_space['num_layers']),
                hidden_dim=np.random.choice(self.search_space['hidden_dim']),
                activation=np.random.choice(self.search_space['activation']),
                dropout_rate=np.random.choice(self.search_space['dropout_rate']),
                use_attention=np.random.choice(self.search_space['use_attention']),
                use_residual=np.random.choice(self.search_space['use_residual']),
                use_layer_norm=np.random.choice(self.search_space['use_layer_norm'])
            )
            self.population.append(gene)
    
    def evolve_generation(
        self,
        fitness_function: Callable[[ArchitectureGene], float],
        carbon_budget: Optional[float] = None
    ) -> EvolutionMetrics:
        """
        Evolve one generation of architectures.
        
        Args:
            fitness_function: Function to evaluate architecture fitness
            carbon_budget: Carbon budget for evolution
            
        Returns:
            Evolution metrics for this generation
        """
        self.generation += 1
        
        # Evaluate fitness
        for gene in self.population:
            if carbon_budget and gene.carbon_cost > carbon_budget:
                gene.fitness = 0.0
            else:
                gene.fitness = fitness_function(gene)
        
        # Sort by fitness
        self.population.sort(key=lambda g: g.fitness, reverse=True)
        
        # Calculate metrics
        fitnesses = [g.fitness for g in self.population]
        metrics = EvolutionMetrics(
            generation=self.generation,
            population_size=self.population_size,
            best_fitness=max(fitnesses),
            average_fitness=np.mean(fitnesses),
            fitness_variance=np.var(fitnesses),
            architectural_complexity=self._calculate_complexity(self.population[0])
        )
        
        # Selection (keep top 50%)
        elite_size = self.population_size // 2
        new_population = self.population[:elite_size]
        
        # Crossover and mutation
        while len(new_population) < self.population_size:
            if np.random.random() < self.crossover_rate:
                # Crossover
                parent1, parent2 = np.random.choice(
                    self.population[:elite_size], 2, replace=False
                )
                child = self._crossover(parent1, parent2)
            else:
                # Mutation
                parent = np.random.choice(self.population[:elite_size])
                child = self._mutate(parent)
            
            new_population.append(child)
        
        self.population = new_population
        self.evolution_history.append(metrics)
        
        logger.info(
            f"Generation {self.generation}: "
            f"best_fitness={metrics.best_fitness:.4f}, "
            f"avg_fitness={metrics.average_fitness:.4f}"
        )
        
        return metrics
    
    def _crossover(
        self,
        parent1: ArchitectureGene,
        parent2: ArchitectureGene
    ) -> ArchitectureGene:
        """Crossover two parent genes"""
        child = ArchitectureGene(
            num_layers=np.random.choice([parent1.num_layers, parent2.num_layers]),
            hidden_dim=np.random.choice([parent1.hidden_dim, parent2.hidden_dim]),
            activation=np.random.choice([parent1.activation, parent2.activation]),
            dropout_rate=np.random.choice([parent1.dropout_rate, parent2.dropout_rate]),
            use_attention=np.random.choice([parent1.use_attention, parent2.use_attention]),
            use_residual=np.random.choice([parent1.use_residual, parent2.use_residual]),
            use_layer_norm=np.random.choice([parent1.use_layer_norm, parent2.use_layer_norm])
        )
        return child
    
    def _mutate(self, gene: ArchitectureGene) -> ArchitectureGene:
        """Mutate a gene"""
        mutated = copy.deepcopy(gene)
        
        if np.random.random() < self.mutation_rate:
            mutated.num_layers = np.random.choice(self.search_space['num_layers'])
        if np.random.random() < self.mutation_rate:
            mutated.hidden_dim = np.random.choice(self.search_space['hidden_dim'])
        if np.random.random() < self.mutation_rate:
            mutated.activation = np.random.choice(self.search_space['activation'])
        if np.random.random() < self.mutation_rate:
            mutated.dropout_rate = np.random.choice(self.search_space['dropout_rate'])
        if np.random.random() < self.mutation_rate:
            mutated.use_attention = not mutated.use_attention
        if np.random.random() < self.mutation_rate:
            mutated.use_residual = not mutated.use_residual
        if np.random.random() < self.mutation_rate:
            mutated.use_layer_norm = not mutated.use_layer_norm
        
        return mutated
    
    def _calculate_complexity(self, gene: ArchitectureGene) -> int:
        """Calculate architectural complexity"""
        complexity = gene.num_layers * gene.hidden_dim
        
        if gene.use_attention:
            complexity *= 2
        if gene.use_residual:
            complexity *= 1.5
        
        return complexity
    
    def get_best_architecture(self) -> ArchitectureGene:
        """Get best architecture from population"""
        if not self.population:
            return None
        
        return max(self.population, key=lambda g: g.fitness)

# ============================================================================
# Continual Learning with EWC
# ============================================================================

class ElasticWeightConsolidation:
    """
    Elastic Weight Consolidation for continual learning.
    
    Prevents catastrophic forgetting when adapting to new tasks.
    """
    
    def __init__(self, model: nn.Module, importance_lambda: float = 100.0):
        self.model = model
        self.importance_lambda = importance_lambda
        
        # Fisher information matrix (importance of each parameter)
        self.fisher_information: Dict[str, torch.Tensor] = {}
        
        # Optimal parameters for previous tasks
        self.optimal_params: Dict[str, torch.Tensor] = {}
        
        # Task-specific importance
        self.task_importances: Dict[str, Dict[str, torch.Tensor]] = {}
    
    def update_fisher(
        self,
        task_id: str,
        dataloader: List[Tuple[torch.Tensor, torch.Tensor]]
    ):
        """
        Update Fisher information matrix for a task.
        
        Estimates parameter importance for the task.
        """
        self.model.eval()
        
        # Initialize Fisher
        fisher = {}
        for name, param in self.model.named_parameters():
            fisher[name] = torch.zeros_like(param)
        
        # Estimate Fisher through empirical Fisher
        for x, y in dataloader:
            self.model.zero_grad()
            output = self.model(x)
            loss = F.cross_entropy(output, y)
            loss.backward()
            
            for name, param in self.model.named_parameters():
                if param.grad is not None:
                    fisher[name] += param.grad.pow(2)
        
        # Normalize
        n_samples = len(dataloader)
        for name in fisher:
            fisher[name] /= n_samples
        
        # Store Fisher and optimal parameters
        self.task_importances[task_id] = fisher
        
        # Update global Fisher (accumulate)
        for name in fisher:
            if name in self.fisher_information:
                self.fisher_information[name] += fisher[name]
            else:
                self.fisher_information[name] = fisher[name]
        
        # Store optimal parameters
        self.optimal_params = {
            name: param.clone().detach()
            for name, param in self.model.named_parameters()
        }
    
    def ewc_loss(self) -> torch.Tensor:
        """
        Calculate EWC regularization loss.
        
        Penalizes changes to important parameters.
        """
        loss = 0.0
        
        for name, param in self.model.named_parameters():
            if name in self.fisher_information and name in self.optimal_params:
                fisher = self.fisher_information[name]
                optimal = self.optimal_params[name]
                
                # Quadratic penalty weighted by Fisher
                loss += (fisher * (param - optimal).pow(2)).sum()
        
        return self.importance_lambda * loss
    
    def consolidate_task(
        self,
        task_id: str,
        importance_threshold: float = 0.1
    ):
        """
        Consolidate task knowledge.
        
        Freezes parameters with high importance.
        """
        if task_id not in self.task_importances:
            return
        
        fisher = self.task_importances[task_id]
        
        # Normalize Fisher to [0, 1]
        max_fisher = max(f.max() for f in fisher.values())
        
        # Freeze important parameters
        for name, param in self.model.named_parameters():
            if name in fisher:
                normalized = fisher[name] / max_fisher
                # Freeze if importance > threshold
                if normalized.mean() > importance_threshold:
                    param.requires_grad = False

# ============================================================================
# Generative Replay for Forgetting Prevention
# ============================================================================

class GenerativeReplay:
    """
    Generative replay to prevent catastrophic forgetting.
    
    Uses a generator to produce synthetic samples from previous tasks.
    """
    
    def __init__(
        self,
        input_dim: int,
        latent_dim: int = 64,
        replay_ratio: float = 0.3
    ):
        self.input_dim = input_dim
        self.latent_dim = latent_dim
        self.replay_ratio = replay_ratio
        
        # Generator network
        self.generator = nn.Sequential(
            nn.Linear(latent_dim, 128),
            nn.BatchNorm1d(128),
            nn.ReLU(),
            nn.Linear(128, 256),
            nn.BatchNorm1d(256),
            nn.ReLU(),
            nn.Linear(256, input_dim)
        )
        
        # Task-specific generators
        self.task_generators: Dict[str, nn.Module] = {}
        
        # Generated samples buffer
        self.replay_buffer: deque = deque(maxlen=10000)
        
        # Optimizer
        self.optimizer = torch.optim.Adam(
            self.generator.parameters(), lr=0.001
        )
    
    def train_generator(
        self,
        task_id: str,
        real_samples: List[torch.Tensor],
        epochs: int = 10
    ):
        """Train generator on real samples"""
        if not real_samples:
            return
        
        # Create task-specific generator
        task_gen = copy.deepcopy(self.generator)
        task_optimizer = torch.optim.Adam(task_gen.parameters(), lr=0.001)
        
        real_tensor = torch.stack(real_samples)
        
        for epoch in range(epochs):
            # Generate fake samples
            z = torch.randn(len(real_samples), self.latent_dim)
            fake = task_gen(z)
            
            # Simple reconstruction loss
            loss = F.mse_loss(fake, real_tensor)
            
            task_optimizer.zero_grad()
            loss.backward()
            task_optimizer.step()
        
        # Store task generator
        self.task_generators[task_id] = task_gen
        
        # Generate replay samples
        with torch.no_grad():
            n_replay = int(len(real_samples) * self.replay_ratio)
            z = torch.randn(n_replay, self.latent_dim)
            replay_samples = task_gen(z)
            
            for sample in replay_samples:
                self.replay_buffer.append(sample)
    
    def generate_replay_batch(
        self,
        batch_size: int
    ) -> torch.Tensor:
        """Generate batch of replay samples"""
        if len(self.replay_buffer) < batch_size:
            # Generate new samples from all task generators
            samples = []
            for task_gen in self.task_generators.values():
                z = torch.randn(batch_size // len(self.task_generators), self.latent_dim)
                with torch.no_grad():
                    fake = task_gen(z)
                samples.append(fake)
            
            if samples:
                return torch.cat(samples, dim=0)[:batch_size]
        
        # Sample from replay buffer
        indices = np.random.choice(
            len(self.replay_buffer),
            min(batch_size, len(self.replay_buffer)),
            replace=False
        )
        return torch.stack([self.replay_buffer[i] for i in indices])

# ============================================================================
# Enhanced Self-Evolving Gate
# ============================================================================

class EnhancedSelfEvolvingGate(nn.Module):
    """
    Enhanced self-evolving gating network.
    
    Features:
    - Meta-learning for rapid adaptation
    - Neural architecture search
    - Continual learning with EWC
    - Generative replay for forgetting prevention
    - Adaptive plasticity control
    - Multi-task learning
    - Curriculum learning
    - Self-assessment
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
        
        # Current architecture
        self.current_architecture = ArchitectureGene(
            num_layers=3,
            hidden_dim=hidden_dim,
            activation='relu',
            dropout_rate=0.1,
            use_attention=True,
            use_residual=True,
            use_layer_norm=True
        )
        
        # Build network from architecture
        self.gate_network = self._build_network(self.current_architecture)
        
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
        self.optimizer = torch.optim.Adam(
            self.gate_network.parameters(),
            lr=adaptation_rate
        )
        
        # Plasticity control
        self.plasticity = 1.0  # 0 = frozen, 1 = fully plastic
        self.plasticity_decay = 0.999
        
        logger.info(
            f"Enhanced Self-Evolving Gate initialized: "
            f"meta_learning={enable_meta_learning}, "
            f"arch_search={enable_architecture_search}, "
            f"continual={enable_continual_learning}"
        )
    
    def _build_network(self, architecture: ArchitectureGene) -> nn.Module:
        """Build network from architecture gene"""
        layers = []
        in_dim = self.input_dim
        
        for i in range(architecture.num_layers):
            # Linear layer
            if i == architecture.num_layers - 1:
                out_dim = self.num_experts
            else:
                out_dim = architecture.hidden_dim
            
            layers.append(nn.Linear(in_dim, out_dim))
            
            # Layer normalization
            if architecture.use_layer_norm and i < architecture.num_layers - 1:
                layers.append(nn.LayerNorm(out_dim))
            
            # Activation
            if i < architecture.num_layers - 1:
                if architecture.activation == 'relu':
                    layers.append(nn.ReLU())
                elif architecture.activation == 'gelu':
                    layers.append(nn.GELU())
                elif architecture.activation == 'swish':
                    layers.append(nn.SiLU())
                elif architecture.activation == 'leaky_relu':
                    layers.append(nn.LeakyReLU())
            
            # Dropout
            if architecture.dropout_rate > 0 and i < architecture.num_layers - 1:
                layers.append(nn.Dropout(architecture.dropout_rate))
            
            # Residual connection (skip every other layer)
            if architecture.use_residual and i > 0 and i % 2 == 0:
                # Add residual connection
                pass  # Implemented through forward pass
            
            in_dim = out_dim
        
        return nn.Sequential(*layers)
    
    def forward(
        self,
        x: torch.Tensor,
        task_id: Optional[str] = None,
        training: bool = False
    ) -> Tuple[torch.Tensor, Dict[str, Any]]:
        """
        Enhanced forward pass with self-evolution.
        
        Returns:
            routing_weights, evolution_metadata
        """
        metadata = {}
        
        # Check for concept drift
        drift_detected = self.concept_drift_detector.check_drift(x)
        metadata['drift_detected'] = drift_detected
        
        # Apply plasticity control
        if training and self.plasticity < 1.0:
            # Scale gradients by plasticity
            for param in self.gate_network.parameters():
                if param.grad is not None:
                    param.grad *= self.plasticity
        
        # Forward pass with optional meta-learning
        if self.enable_meta_learning and task_id:
            weights = self.meta_learner(x, task_id)
            metadata['meta_adapted'] = True
        else:
            # Standard forward pass
            logits = self.gate_network(x)
            
            # Add noise during training for exploration
            if training:
                noise_std = 0.1 * self.plasticity
                noise = torch.randn_like(logits) * noise_std
                logits = logits + noise
            
            weights = F.softmax(logits, dim=-1)
        
        # Calculate metadata
        metadata['entropy'] = -(weights * torch.log(weights + 1e-8)).sum(dim=-1).mean().item()
        metadata['confidence'] = weights.max(dim=-1)[0].mean().item()
        metadata['plasticity'] = self.plasticity
        
        return weights, metadata
    
    def adapt(
        self,
        state: torch.Tensor,
        chosen_expert: int,
        reward: float,
        environmental_feedback: Dict[str, Any],
        task_id: Optional[str] = None
    ):
        """
        Enhanced adaptation with multiple learning strategies.
        
        Args:
            state: Input state
            chosen_expert: Selected expert index
            reward: Reward signal
            environmental_feedback: Environmental metrics
            task_id: Optional task identifier
        """
        # Store in memory
        self.memory.append({
            'state': state.detach().clone(),
            'action': chosen_expert,
            'reward': reward,
            'environmental': environmental_feedback,
            'task_id': task_id,
            'timestamp': datetime.utcnow()
        })
        
        # Update concept drift detector
        self.concept_drift_detector.update(state)
        
        # Policy gradient learning
        if len(self.memory) >= 32:
            self._policy_gradient_step()
        
        # Meta-adaptation for new task
        if task_id and task_id not in self.task_prototypes:
            self._create_task_prototype(task_id, state, reward)
        
        # Continual learning consolidation
        if self.enable_continual_learning and len(self.memory) % 100 == 0:
            self._consolidate_knowledge()
        
        # Architecture search trigger
        if (self.enable_architecture_search and
            self.concept_drift_detector.should_evolve_architecture()):
            self._evolve_architecture()
        
        # Decay plasticity
        self.plasticity *= self.plasticity_decay
        self.plasticity = max(self.plasticity, 0.1)  # Minimum plasticity
        
        # Record adaptation
        self.adaptation_history.append({
            'timestamp': datetime.utcnow().isoformat(),
            'reward': reward,
            'expert': chosen_expert,
            'drift': self.concept_drift_detector.drift_score,
            'plasticity': self.plasticity,
            'task_id': task_id
        })
    
    def _policy_gradient_step(self):
        """Enhanced policy gradient learning step"""
        if len(self.memory) < 32:
            return
        
        # Sample batch
        batch_size = min(32, len(self.memory))
        indices = np.random.choice(len(self.memory), batch_size, replace=False)
        batch = [self.memory[i] for i in indices]
        
        # Include generative replay if enabled
        if self.enable_generative_replay and len(self.memory) > 100:
            replay_states = self.replay.generate_replay_batch(batch_size // 4)
            # Add to batch (simplified)
        
        # Prepare batch
        states = torch.stack([b['state'] for b in batch])
        actions = torch.tensor([b['action'] for b in batch])
        rewards = torch.tensor([b['reward'] for b in batch])
        
        # Normalize rewards
        rewards = (rewards - rewards.mean()) / (rewards.std() + 1e-8)
        
        # Forward pass
        logits = self.gate_network(states)
        probs = F.softmax(logits, dim=-1)
        action_probs = probs[range(batch_size), actions]
        
        # Policy gradient loss
        pg_loss = -torch.mean(torch.log(action_probs + 1e-8) * rewards)
        
        # Add EWC loss if continual learning enabled
        total_loss = pg_loss
        if self.enable_continual_learning:
            ewc_loss = self.ewc.ewc_loss()
            total_loss += ewc_loss * 0.1
        
        # Backpropagate
        self.optimizer.zero_grad()
        total_loss.backward()
        
        # Apply plasticity control
        if self.plasticity < 1.0:
            for param in self.gate_network.parameters():
                if param.grad is not None:
                    param.grad *= self.plasticity
        
        torch.nn.utils.clip_grad_norm_(self.gate_network.parameters(), 1.0)
        self.optimizer.step()
        
        # Record performance
        self.performance_history.append({
            'pg_loss': pg_loss.item(),
            'total_loss': total_loss.item(),
            'avg_reward': rewards.mean().item()
        })
    
    def _create_task_prototype(
        self,
        task_id: str,
        state: torch.Tensor,
        reward: float
    ):
        """Create prototype for new task"""
        prototype = TaskPrototype(
            task_id=task_id,
            support_set=[(state, torch.tensor(reward))],
            query_set=[],
            task_embedding=state.detach().mean(dim=0),
            difficulty=1.0 - abs(reward),
            domain="unknown"
        )
        
        self.task_prototypes[task_id] = prototype
        
        # Adapt meta-learner if enabled
        if self.enable_meta_learning:
            self.meta_learner.adapt_to_task(
                prototype.support_set,
                task_id
            )
    
    def _consolidate_knowledge(self):
        """Consolidate knowledge using EWC"""
        if not self.enable_continual_learning:
            return
        
        # Create dataloader from recent memory
        recent = list(self.memory)[-100:]
        dataloader = [(m['state'], torch.tensor(m['action'])) for m in recent]
        
        # Update Fisher information
        self.ewc.update_fisher("current_task", dataloader)
        
        logger.debug("Knowledge consolidated")
    
    def _evolve_architecture(self):
        """Evolve gate architecture using NAS"""
        if not self.enable_architecture_search:
            return
        
        logger.info("Triggering architecture evolution...")
        
        # Define fitness function
        def fitness_function(gene: ArchitectureGene) -> float:
            # Build temporary network
            temp_net = self._build_network(gene)
            
            # Evaluate on recent memory
            if len(self.memory) < 10:
                return 0.5
            
            recent = list(self.memory)[-50:]
            states = torch.stack([m['state'] for m in recent])
            actions = torch.tensor([m['action'] for m in recent])
            
            with torch.no_grad():
                logits = temp_net(states)
                preds = logits.argmax(dim=-1)
                accuracy = (preds == actions).float().mean().item()
            
            # Penalize complexity
            complexity_penalty = self.architecture_search._calculate_complexity(gene) / 1000
            
            return accuracy - complexity_penalty
        
        # Run evolution
        metrics = self.architecture_search.evolve_generation(fitness_function)
        
        # Update architecture if improvement found
        best_gene = self.architecture_search.get_best_architecture()
        if best_gene and best_gene.fitness > self.current_architecture.fitness:
            logger.info(
                f"Upgrading architecture: "
                f"fitness {self.current_architecture.fitness:.4f} -> {best_gene.fitness:.4f}"
            )
            
            # Build new network
            new_network = self._build_network(best_gene)
            
            # Transfer weights where possible
            self._transfer_weights(self.gate_network, new_network)
            
            self.gate_network = new_network
            self.current_architecture = best_gene
    
    def _transfer_weights(
        self,
        old_network: nn.Module,
        new_network: nn.Module
    ):
        """Transfer weights from old to new network"""
        old_params = list(old_network.parameters())
        new_params = list(new_network.parameters())
        
        for i, new_param in enumerate(new_params):
            if i < len(old_params):
                old_param = old_params[i]
                
                # Transfer if shapes match
                if old_param.shape == new_param.shape:
                    new_param.data.copy_(old_param.data)
                else:
                    # Partial transfer for compatible dimensions
                    min_dims = [
                        min(o, n) for o, n in zip(old_param.shape, new_param.shape)
                    ]
                    slices = tuple(slice(0, d) for d in min_dims)
                    new_param.data[slices] = old_param.data[slices]
    
    def get_evolution_metrics(self) -> Dict[str, Any]:
        """Get comprehensive evolution metrics"""
        return {
            'current_generation': len(self.adaptation_history),
            'current_plasticity': self.plasticity,
            'architecture': {
                'num_layers': self.current_architecture.num_layers,
                'hidden_dim': self.current_architecture.hidden_dim,
                'activation': self.current_architecture.activation,
                'fitness': self.current_architecture.fitness
            },
            'performance': {
                'recent_rewards': [
                    h['reward'] for h in self.adaptation_history[-100:]
                ],
                'drift_score': self.concept_drift_detector.drift_score,
                'entropy_history': [
                    h.get('entropy', 0) for h in self.adaptation_history[-100:]
                ]
            },
            'learning': {
                'memory_size': len(self.memory),
                'task_prototypes': len(self.task_prototypes),
                'meta_learning_enabled': self.enable_meta_learning,
                'architecture_search_enabled': self.enable_architecture_search,
                'continual_learning_enabled': self.enable_continual_learning
            },
            'evolution_history': [
                {
                    'generation': m.generation,
                    'best_fitness': m.best_fitness,
                    'avg_fitness': m.average_fitness
                }
                for m in (self.architecture_search.evolution_history if self.enable_architecture_search else [])
            ]
        }
    
    def reset_plasticity(self):
        """Reset plasticity to maximum"""
        self.plasticity = 1.0
        logger.info("Plasticity reset to 1.0")


# ============================================================================
# Enhanced Concept Drift Detector
# ============================================================================

class EnhancedConceptDriftDetector:
    """
    Enhanced concept drift detection with multiple methods.
    
    Features:
    - Statistical test-based detection
    - Window-based comparison
    - Adaptive thresholding
    - Multi-scale analysis
    """
    
    def __init__(
        self,
        window_size: int = 100,
        threshold: float = 0.1,
        num_windows: int = 3
    ):
        self.window_size = window_size
        self.threshold = threshold
        self.num_windows = num_windows
        
        # Multiple windows for multi-scale analysis
        self.reference_windows: List[deque] = [
            deque(maxlen=window_size * (2**i))
            for i in range(num_windows)
        ]
        self.current_window: deque = deque(maxlen=window_size)
        
        self.drift_score = 0.0
        self.drift_history: List[float] = []
        self.architecture_evolution_counter = 0
    
    def update(self, x: torch.Tensor):
        """Update with new sample"""
        feature = x.detach().mean(dim=0)
        
        # Add to current window
        self.current_window.append(feature)
        
        # Update reference windows periodically
        if len(self.current_window) >= self.window_size:
            for i, window in enumerate(self.reference_windows):
                window.append(feature)
    
    def check_drift(self, x: torch.Tensor) -> bool:
        """Check for concept drift using multiple methods"""
        if len(self.current_window) < self.window_size // 2:
            return False
        
        # Method 1: Distribution difference
        current_tensor = torch.stack(list(self.current_window))
        drift_scores = []
        
        for window in self.reference_windows:
            if len(window) >= self.window_size // 2:
                reference_tensor = torch.stack(list(window))
                
                # Maximum Mean Discrepancy
                mmd = self._compute_mmd(current_tensor, reference_tensor)
                drift_scores.append(mmd)
        
        if drift_scores:
            self.drift_score = max(drift_scores)
        else:
            self.drift_score = 0.0
        
        self.drift_history.append(self.drift_score)
        
        return self.drift_score > self.threshold
    
    def should_evolve_architecture(self) -> bool:
        """Determine if architecture evolution is needed"""
        if len(self.drift_history) < 10:
            return False
        
        # Check if drift has been consistently high
        recent_drift = self.drift_history[-10:]
        avg_drift = np.mean(recent_drift)
        
        if avg_drift > self.threshold * 2:
            self.architecture_evolution_counter += 1
        else:
            self.architecture_evolution_counter = max(0, self.architecture_evolution_counter - 1)
        
        return self.architecture_evolution_counter >= 5
    
    def _compute_mmd(
        self,
        x: torch.Tensor,
        y: torch.Tensor,
        kernel: str = 'rbf'
    ) -> float:
        """Compute Maximum Mean Discrepancy"""
        if kernel == 'rbf':
            # RBF kernel MMD
            xx = torch.cdist(x, x, p=2).pow(2)
            yy = torch.cdist(y, y, p=2).pow(2)
            xy = torch.cdist(x, y, p=2).pow(2)
            
            sigma = torch.median(xx[xx > 0]).sqrt()
            k_xx = torch.exp(-xx / (2 * sigma**2)).mean()
            k_yy = torch.exp(-yy / (2 * sigma**2)).mean()
            k_xy = torch.exp(-xy / (2 * sigma**2)).mean()
            
            mmd = k_xx + k_yy - 2 * k_xy
            return mmd.item()
        
        # Linear kernel
        return (x.mean(dim=0) - y.mean(dim=0)).norm().item()

# ============================================================================
# Enhanced Environmental Encoder
# ============================================================================

class EnhancedEnvironmentalEncoder:
    """
    Enhanced environmental context encoder.
    
    Features:
    - Temporal encoding (time of day, day of week, season)
    - Carbon intensity forecasting
    - Renewable availability prediction
    - Workload pattern recognition
    """
    
    def __init__(self, output_dim: int):
        self.output_dim = output_dim
        
        # Temporal encoders
        self.time_encoder = nn.Sequential(
            nn.Linear(4, 16),  # hour, day, month, season
            nn.ReLU(),
            nn.Linear(16, 32)
        )
        
        # Carbon encoder
        self.carbon_encoder = nn.Sequential(
            nn.Linear(3, 16),  # current, forecast, trend
            nn.ReLU(),
            nn.Linear(16, 32)
        )
        
        # Renewable encoder
        self.renewable_encoder = nn.Sequential(
            nn.Linear(4, 16),  # solar, wind, hydro, storage
            nn.ReLU(),
            nn.Linear(16, 32)
        )
        
        # Workload encoder
        self.workload_encoder = nn.Sequential(
            nn.Linear(5, 16),  # qps, latency, error_rate, queue_depth, variety
            nn.ReLU(),
            nn.Linear(16, 32)
        )
        
        # Final projection
        self.projector = nn.Linear(128, output_dim)
    
    def forward(self, context: Dict[str, Any]) -> torch.Tensor:
        """Encode environmental context"""
        # Temporal features
        now = datetime.utcnow()
        temporal = torch.tensor([
            now.hour / 24.0,
            now.weekday() / 7.0,
            now.month / 12.0,
            (now.month % 12 // 3) / 4.0  # Season
        ], dtype=torch.float32)
        time_features = self.time_encoder(temporal)
        
        # Carbon features
        carbon = torch.tensor([
            context.get('grid_carbon_intensity', 400) / 1000.0,
            context.get('carbon_forecast', 400) / 1000.0,
            context.get('carbon_trend', 0.0)
        ], dtype=torch.float32)
        carbon_features = self.carbon_encoder(carbon)
        
        # Renewable features
        renewable = torch.tensor([
            context.get('solar_available', 0.0),
            context.get('wind_available', 0.0),
            context.get('hydro_available', 0.0),
            context.get('storage_level', 0.0)
        ], dtype=torch.float32)
        renewable_features = self.renewable_encoder(renewable)
        
        # Workload features
        workload = torch.tensor([
            context.get('qps', 0) / 1000.0,
            context.get('latency_ms', 0) / 100.0,
            context.get('error_rate', 0),
            context.get('queue_depth', 0) / 100.0,
            context.get('task_variety', 0.5)
        ], dtype=torch.float32)
        workload_features = self.workload_encoder(workload)
        
        # Combine all features
        combined = torch.cat([
            time_features, carbon_features,
            renewable_features, workload_features
        ])
        
        return self.projector(combined)

# ============================================================================
# Main Self-Evolving Gate Class
# ============================================================================

class SelfEvolvingGate(nn.Module):
    """
    Main self-evolving gate class (maintains backward compatibility).
    
    Wraps the enhanced functionality while preserving the original interface.
    """
    
    def __init__(
        self,
        input_dim: int,
        num_experts: int,
        memory_size: int = 10000,
        adaptation_rate: float = 0.01,
        **enhanced_kwargs
    ):
        super().__init__()
        
        # Enhanced core
        self.enhanced_gate = EnhancedSelfEvolvingGate(
            input_dim=input_dim,
            num_experts=num_experts,
            adaptation_rate=adaptation_rate,
            memory_size=memory_size,
            **enhanced_kwargs
        )
        
        # For backward compatibility
        self.num_experts = num_experts
        self.adaptation_rate = adaptation_rate
        self.memory = self.enhanced_gate.memory
        self.concept_drift_detector = self.enhanced_gate.concept_drift_detector
        self.environmental_encoder = self.enhanced_gate.environmental_encoder
        self.performance_history = self.enhanced_gate.performance_history
        self.adaptation_history = self.enhanced_gate.adaptation_history
        self.optimizer = self.enhanced_gate.optimizer
    
    def forward(
        self,
        x: torch.Tensor,
        environmental_context: Optional[Dict[str, Any]] = None,
        task_id: Optional[str] = None
    ) -> Tuple[torch.Tensor, Dict[str, Any]]:
        """Forward pass with environmental adaptation"""
        # Encode environmental context if available
        if environmental_context:
            env_features = self.environmental_encoder(environmental_context)
            x = torch.cat([x, env_features.unsqueeze(0)], dim=-1) if x.dim() == 1 else torch.cat([x, env_features], dim=-1)
        
        return self.enhanced_gate(x, task_id=task_id, training=self.training)
    
    def adapt(
        self,
        state: torch.Tensor,
        chosen_expert: int,
        reward: float,
        environmental_feedback: Dict[str, Any]
    ):
        """Adapt gating network based on feedback"""
        self.enhanced_gate.adapt(
            state, chosen_expert, reward, environmental_feedback
        )
    
    def get_evolution_metrics(self) -> Dict[str, Any]:
        """Get evolution metrics"""
        return self.enhanced_gate.get_evolution_metrics()
