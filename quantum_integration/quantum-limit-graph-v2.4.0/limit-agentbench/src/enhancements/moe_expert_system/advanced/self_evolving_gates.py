# File: quantum_integration/quantum-limit-graph-v2.4.0/limit-agentbench/src/enhancements/moe_expert_system/advanced/self_evolving_gates.py
"""
Enhanced Self-Evolving Gates v6.2.0 - Complete Green Agent Implementation

Complete bio-inspired integration with:
- Federated Reflexive Learning with global model sharing
- User-Adaptive Reflexivity with dynamic thresholds
- Real-time Carbon Intensity Integration with API support
- Cross-Domain Knowledge Transfer with domain mapping
- Human-AI Collaborative Reflection with feedback loops
- Predictive Reflexivity with ensemble forecasting
- Sustainability Score with multi-metric aggregation
- Enhanced Carbon/Helium Awareness with real-time tracking
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
- Quantum circuit optimization for gate evolution
- Helium-aware plasticity modulation
- Quantum-to-classical knowledge transfer pathway
- Helium savings as sustainability metric
- Enhanced architecture search with quantum-aware fitness
- Integration with MoE Expert Router and Gating Network
- Context-aware evolution using Helium, carbon, and bio signals
- Multi-Objective NSGA-II for Pareto-optimal architecture search (NEW)
- Real Helium integration via external provider (NEW)
- True MAML (Model-Agnostic Meta-Learning) for fast adaptation (NEW)
"""

import asyncio
import logging
from typing import Dict, Any, List, Optional, Tuple, Set, Callable, Union
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
import aiohttp
import os
import zlib
import pickle

logger = logging.getLogger(__name__)

# ============================================================================
# Bio-Inspired Import Check
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
# NEW: Import MoE Expert Router and Gating Network (if available)
# ============================================================================
try:
    from ..expert_router import ExpertRouter
    from ..gating_network import GatingNetworkManager
    MOE_AVAILABLE = True
except ImportError:
    MOE_AVAILABLE = False
    logger.warning("MoE Expert Router not available - self-evolving gates will operate in standalone mode")

# ============================================================================
# NEW: Helium Provider Interface (to be injected)
# ============================================================================
class HeliumProvider:
    """Interface to external helium modules for real-time metrics."""
    def get_scarcity(self) -> float:
        raise NotImplementedError
    def get_cost_index(self) -> float:
        raise NotImplementedError
    def get_efficiency(self) -> float:
        raise NotImplementedError
    def get_availability_trend(self) -> List[float]:
        raise NotImplementedError

# ============================================================================
# Legacy Classes for Compatibility (Enhanced)
# ============================================================================
class ArchitectureGene:
    """Architecture gene for neural architecture search with quantum awareness."""
    def __init__(self, num_layers=3, hidden_dim=128, activation='relu',
                 dropout_rate=0.1, use_attention=True, use_residual=True,
                 use_layer_norm=True, quantum_circuit_depth=0,
                 quantum_qubits=0, quantum_gate_types=None):
        self.num_layers = num_layers
        self.hidden_dim = hidden_dim
        self.activation = activation
        self.dropout_rate = dropout_rate
        self.use_attention = use_attention
        self.use_residual = use_residual
        self.use_layer_norm = use_layer_norm
        self.fitness = 0.0  # scalar (backward compatibility)
        self.multi_objectives: List[float] = []  # [accuracy, carbon_impact, helium_usage, quantum_advantage]
        self.rank = 0
        self.crowding_distance = 0.0
        self.quantum_circuit_depth = quantum_circuit_depth
        self.quantum_qubits = quantum_qubits
        self.quantum_gate_types = quantum_gate_types or ['H', 'CNOT', 'RZ', 'RX']
        self.quantum_advantage_score = 0.0
        self.helium_efficiency = 0.5

class TaskPrototype:
    """Task prototype for meta-learning with quantum extension."""
    def __init__(self, task_id, support_set=None, query_set=None,
                 task_embedding=None, difficulty=0.5, domain="unknown",
                 quantum_task=False, helium_requirement=0.0):
        self.task_id = task_id
        self.support_set = support_set or []
        self.query_set = query_set or []
        self.task_embedding = task_embedding
        self.difficulty = difficulty
        self.domain = domain
        self.quantum_task = quantum_task
        self.helium_requirement = helium_requirement
        self.quantum_success_rate = 0.0

# ============================================================================
# NEW: True MAML (Model-Agnostic Meta-Learning) Implementation
# ============================================================================
class MAMLGate:
    """
    Model-Agnostic Meta-Learning for fast adaptation of gating network.
    Supports gradient-based inner updates and outer meta-loss.
    """
    def __init__(self, input_dim: int, num_experts: int, hidden_dim: int,
                 inner_lr: float = 0.01, outer_lr: float = 0.001,
                 quantum_enabled: bool = False):
        self.input_dim = input_dim
        self.num_experts = num_experts
        self.hidden_dim = hidden_dim
        self.inner_lr = inner_lr
        self.outer_lr = outer_lr
        self.quantum_enabled = quantum_enabled

        # Base network (shared across tasks)
        self.base_network = nn.Sequential(
            nn.Linear(input_dim, hidden_dim),
            nn.ReLU(),
            nn.Linear(hidden_dim, hidden_dim),
            nn.ReLU(),
            nn.Linear(hidden_dim, num_experts)
        )
        self.meta_optimizer = torch.optim.Adam(self.base_network.parameters(), lr=outer_lr)

        # Store task-specific adapted parameters (keyed by task_id)
        self.task_adaptations: Dict[str, Dict[str, torch.Tensor]] = {}
        self.quantum_adaptations: Dict[str, Dict[str, torch.Tensor]] = {}

    def forward(self, x: torch.Tensor, task_id: Optional[str] = None) -> torch.Tensor:
        """Forward pass using task-specific adapted parameters if available."""
        if task_id is not None:
            if self.quantum_enabled and task_id in self.quantum_adaptations:
                adapted_weights = self.quantum_adaptations[task_id]
                return self._forward_with_weights(x, adapted_weights)
            elif task_id in self.task_adaptations:
                adapted_weights = self.task_adaptations[task_id]
                return self._forward_with_weights(x, adapted_weights)
        # Fallback to base network
        return self.base_network(x)

    def _forward_with_weights(self, x: torch.Tensor, weights: Dict[str, torch.Tensor]) -> torch.Tensor:
        """Forward pass using specified parameter dict."""
        # We'll rebuild the forward pass using the weights.
        # For simplicity, we assume the network structure is fixed.
        # We'll use a helper function that applies layers sequentially.
        def apply_layer(layer, x, weight_key):
            # Assume each layer has weight and bias.
            w = weights.get(f"{weight_key}.weight")
            b = weights.get(f"{weight_key}.bias")
            if w is None or b is None:
                return x
            return F.linear(x, w, b)

        # The network is: linear1 -> relu -> linear2 -> relu -> linear3
        x = apply_layer(None, x, "0")
        x = F.relu(x)
        x = apply_layer(None, x, "1")
        x = F.relu(x)
        x = apply_layer(None, x, "2")
        return x

    def adapt_to_task(self, support_set: List[Tuple[torch.Tensor, torch.Tensor]],
                      task_id: str, quantum: bool = False, num_inner_steps: int = 5):
        """
        Perform inner loop adaptation for a specific task.
        support_set: list of (input, target) for the task.
        task_id: unique identifier for the task.
        quantum: whether this is a quantum task.
        num_inner_steps: number of gradient steps.
        """
        # Start with base network parameters
        adapted_weights = {name: param.data.clone() for name, param in self.base_network.named_parameters()}
        # Create optimizer for inner loop
        inner_optimizer = torch.optim.SGD(adapted_weights.values(), lr=self.inner_lr)

        for _ in range(num_inner_steps):
            total_loss = 0.0
            for x, y in support_set:
                logits = self._forward_with_weights(x, adapted_weights)
                loss = F.cross_entropy(logits, y)
                total_loss += loss
            total_loss /= len(support_set)
            inner_optimizer.zero_grad()
            total_loss.backward()
            inner_optimizer.step()

        # Store adapted weights
        if quantum:
            self.quantum_adaptations[task_id] = adapted_weights
        else:
            self.task_adaptations[task_id] = adapted_weights

    def meta_update(self, query_sets: List[Tuple[str, List[Tuple[torch.Tensor, torch.Tensor]]]]):
        """
        Outer loop meta-update using query sets from multiple tasks.
        query_sets: list of (task_id, query_data) where query_data is list of (x, y).
        """
        meta_loss = 0.0
        for task_id, query_data in query_sets:
            # Get adapted weights for this task (must have been adapted earlier)
            adapted_weights = self.task_adaptations.get(task_id)
            if adapted_weights is None:
                continue
            # Compute loss on query set using adapted weights
            loss = 0.0
            for x, y in query_data:
                logits = self._forward_with_weights(x, adapted_weights)
                loss += F.cross_entropy(logits, y)
            meta_loss += loss / len(query_data)

        # Update base network using meta-loss
        self.meta_optimizer.zero_grad()
        meta_loss.backward()
        self.meta_optimizer.step()

        # Clear task-specific adaptations (optional, to save memory)
        # We can keep them for future use; but we may want to prune old ones.
        # For now, we keep them.

# ============================================================================
# NEW: Multi-Objective NSGA-II for Architecture Search
# ============================================================================
class NSGAIIArchitectureSearch:
    """
    Multi-objective architecture search using NSGA-II.
    Objectives: [accuracy, carbon_impact, helium_usage, quantum_advantage]
    """
    def __init__(self, input_dim: int, num_experts: int, population_size: int = 20,
                 max_generations: int = 20, crossover_prob: float = 0.8,
                 mutation_prob: float = 0.2):
        self.input_dim = input_dim
        self.num_experts = num_experts
        self.population_size = population_size
        self.max_generations = max_generations
        self.crossover_prob = crossover_prob
        self.mutation_prob = mutation_prob
        self.population: List[ArchitectureGene] = []
        self.generation = 0
        self.pareto_front: List[ArchitectureGene] = []

    def _fast_non_dominated_sort(self, population: List[ArchitectureGene]) -> Dict[int, List[ArchitectureGene]]:
        """Fast non-dominated sort based on multiple objectives."""
        fronts = defaultdict(list)
        # Each individual's domination count and set of dominated individuals.
        for i, ind_i in enumerate(population):
            ind_i.domination_count = 0
            ind_i.dominated_set = []
            for j, ind_j in enumerate(population):
                if i == j: continue
                # Check if i dominates j (all objectives better or equal, at least one strictly better)
                dominates = all(ind_i.multi_objectives[k] <= ind_j.multi_objectives[k] for k in range(len(ind_i.multi_objectives))) and \
                            any(ind_i.multi_objectives[k] < ind_j.multi_objectives[k] for k in range(len(ind_i.multi_objectives)))
                if dominates:
                    ind_i.dominated_set.append(ind_j)
                # Check if j dominates i
                elif all(ind_j.multi_objectives[k] <= ind_i.multi_objectives[k] for k in range(len(ind_i.multi_objectives))) and \
                     any(ind_j.multi_objectives[k] < ind_i.multi_objectives[k] for k in range(len(ind_i.multi_objectives))):
                    ind_i.domination_count += 1
            if ind_i.domination_count == 0:
                fronts[0].append(ind_i)

        front_index = 0
        while fronts[front_index]:
            next_front = []
            for ind in fronts[front_index]:
                for dominated in ind.dominated_set:
                    dominated.domination_count -= 1
                    if dominated.domination_count == 0:
                        next_front.append(dominated)
            if next_front:
                front_index += 1
                fronts[front_index] = next_front
            else:
                break
        return fronts

    def _crowding_distance(self, front: List[ArchitectureGene], objectives: int = 4):
        """Assign crowding distance to individuals in a front."""
        for ind in front:
            ind.crowding_distance = 0.0

        for obj_idx in range(objectives):
            # Sort by objective
            front.sort(key=lambda ind: ind.multi_objectives[obj_idx])
            front[0].crowding_distance = float('inf')
            front[-1].crowding_distance = float('inf')
            obj_min = front[0].multi_objectives[obj_idx]
            obj_max = front[-1].multi_objectives[obj_idx]
            if obj_max == obj_min:
                continue
            for i in range(1, len(front)-1):
                front[i].crowding_distance += (front[i+1].multi_objectives[obj_idx] - front[i-1].multi_objectives[obj_idx]) / (obj_max - obj_min)

    def _crowded_comparison_operator(self, ind1: ArchitectureGene, ind2: ArchitectureGene) -> bool:
        """Return True if ind1 is better than ind2 based on rank and crowding distance."""
        if ind1.rank != ind2.rank:
            return ind1.rank < ind2.rank
        return ind1.crowding_distance > ind2.crowding_distance

    def _create_offspring(self, population: List[ArchitectureGene]) -> List[ArchitectureGene]:
        """Create offspring via tournament selection, crossover, and mutation."""
        offspring = []
        while len(offspring) < len(population):
            # Tournament selection (size 2)
            p1 = population[np.random.randint(0, len(population))]
            p2 = population[np.random.randint(0, len(population))]
            if self._crowded_comparison_operator(p1, p2):
                parent1 = p1
            else:
                parent1 = p2
            p1 = population[np.random.randint(0, len(population))]
            p2 = population[np.random.randint(0, len(population))]
            if self._crowded_comparison_operator(p1, p2):
                parent2 = p1
            else:
                parent2 = p2

            # Crossover
            if np.random.random() < self.crossover_prob:
                child = self._crossover(parent1, parent2)
            else:
                child = copy.deepcopy(parent1)

            # Mutation
            if np.random.random() < self.mutation_prob:
                self._mutate(child)

            offspring.append(child)
        return offspring

    def _crossover(self, p1: ArchitectureGene, p2: ArchitectureGene) -> ArchitectureGene:
        """Uniform crossover."""
        child = ArchitectureGene()
        for attr in ['num_layers', 'hidden_dim', 'activation', 'dropout_rate',
                     'use_attention', 'use_residual', 'use_layer_norm',
                     'quantum_circuit_depth', 'quantum_qubits', 'quantum_gate_types']:
            if np.random.random() < 0.5:
                setattr(child, attr, getattr(p1, attr))
            else:
                setattr(child, attr, getattr(p2, attr))
        # Blend continuous parameters
        if np.random.random() < 0.3:
            child.hidden_dim = int((p1.hidden_dim + p2.hidden_dim) / 2)
            child.dropout_rate = (p1.dropout_rate + p2.dropout_rate) / 2
            child.quantum_circuit_depth = int((p1.quantum_circuit_depth + p2.quantum_circuit_depth) / 2)
            child.quantum_qubits = int((p1.quantum_qubits + p2.quantum_qubits) / 2)
        child.fitness = 0.0
        child.multi_objectives = []
        return child

    def _mutate(self, gene: ArchitectureGene):
        """Random mutation."""
        if np.random.random() < 0.3:
            gene.num_layers = max(1, gene.num_layers + np.random.choice([-1, 1]))
        if np.random.random() < 0.3:
            gene.hidden_dim = max(16, gene.hidden_dim + np.random.randint(-32, 33))
        if np.random.random() < 0.2:
            activations = ['relu', 'gelu', 'swish', 'leaky_relu']
            gene.activation = np.random.choice(activations)
        if np.random.random() < 0.2:
            gene.dropout_rate = max(0.0, min(0.5, gene.dropout_rate + np.random.uniform(-0.05, 0.05)))
        if np.random.random() < 0.2:
            gene.use_attention = not gene.use_attention
        if np.random.random() < 0.2:
            gene.use_residual = not gene.use_residual
        if np.random.random() < 0.2:
            gene.use_layer_norm = not gene.use_layer_norm
        if np.random.random() < 0.2:
            gene.quantum_circuit_depth = max(0, gene.quantum_circuit_depth + np.random.randint(-2, 3))
        if np.random.random() < 0.2:
            gene.quantum_qubits = max(0, gene.quantum_qubits + np.random.randint(-4, 5))
        if np.random.random() < 0.2:
            gene.quantum_gate_types = np.random.choice(['H', 'CNOT', 'RZ', 'RX', 'RY', 'CZ'], size=np.random.randint(2,5)).tolist()

    def evaluate_population(self, fitness_function: Callable[[ArchitectureGene], List[float]]):
        """Evaluate all individuals and set multi_objectives."""
        for ind in self.population:
            ind.multi_objectives = fitness_function(ind)

    def evolve(self, fitness_function: Callable[[ArchitectureGene], List[float]]) -> Dict[str, Any]:
        """Run one generation of NSGA-II evolution."""
        if not self.population:
            # Initialize random population
            self.population = [ArchitectureGene() for _ in range(self.population_size)]
        self.evaluate_population(fitness_function)

        fronts = self._fast_non_dominated_sort(self.population)
        # Assign ranks and crowding distances
        new_population = []
        front_idx = 0
        while len(new_population) < self.population_size and front_idx in fronts:
            front = fronts[front_idx]
            self._crowding_distance(front)
            for ind in front:
                ind.rank = front_idx
            # Sort front by crowding distance descending
            front.sort(key=lambda ind: ind.crowding_distance, reverse=True)
            # Add individuals to new population
            remaining = self.population_size - len(new_population)
            new_population.extend(front[:remaining])
            front_idx += 1

        # Create offspring
        offspring = self._create_offspring(new_population)
        self.population = new_population + offspring
        # Keep only population_size
        self.population = self.population[:self.population_size]

        # Update Pareto front (best non-dominated front)
        self.pareto_front = fronts.get(0, [])
        self.generation += 1

        return {
            'generation': self.generation,
            'population_size': len(self.population),
            'pareto_front_size': len(self.pareto_front),
            'best_individual': self.pareto_front[0] if self.pareto_front else None,
            'fronts': {k: len(v) for k, v in fronts.items()}
        }

    def get_best_architecture(self, objective_weights: Optional[List[float]] = None) -> Optional[ArchitectureGene]:
        """Return best architecture according to weighted sum of objectives."""
        if not self.pareto_front:
            return None
        if objective_weights is None:
            objective_weights = [1.0, -1.0, -1.0, 1.0]  # maximize accuracy, minimize carbon, minimize helium, maximize quantum advantage
        def score(ind):
            return sum(w * val for w, val in zip(objective_weights, ind.multi_objectives))
        return max(self.pareto_front, key=score)

    def get_quantum_stats(self) -> Dict[str, Any]:
        return {
            'quantum_architectures': sum(1 for ind in self.population if ind.quantum_circuit_depth > 0),
            'classical_architectures': sum(1 for ind in self.population if ind.quantum_circuit_depth == 0),
            'pareto_front_size': len(self.pareto_front)
        }

# ============================================================================
# Carbon Intensity Integration Module (Enhanced)
# ============================================================================

class CarbonIntensityManager:
    """Real-time carbon intensity integration with API support and helium tracking"""
    
    def __init__(self, endpoint: str = "https://api.electricitymap.org/v3/carbon-intensity"):
        self.endpoint = endpoint
        self.carbon_intensity = 0.0
        self.region = "us-east"
        self.last_update = None
        self._lock = asyncio.Lock()
        self._session = None
        self.update_interval = 300
        self.cache = {}
        self.historical_intensities = deque(maxlen=1000)
        self.api_key = os.getenv('ELECTRICITYMAP_API_KEY', '')
        self.helium_scarcity = 0.5
        self.helium_availability_trend = deque(maxlen=100)
        self.helium_price = 0.5
    
    async def _get_session(self):
        if self._session is None:
            self._session = aiohttp.ClientSession()
        return self._session
    
    async def update_carbon_intensity(self, region: str = "us-east") -> Dict:
        async with self._lock:
            session = await self._get_session()
            try:
                url = f"{self.endpoint}/latest?zone={region}"
                headers = {'auth-token': self.api_key} if self.api_key else {}
                async with session.get(url, headers=headers, timeout=10) as response:
                    if response.status == 200:
                        data = await response.json()
                        self.carbon_intensity = data.get('carbonIntensity', 400)
                        self.region = region
                        self.last_update = datetime.now()
                        self.cache[region] = {'intensity': self.carbon_intensity, 'timestamp': self.last_update}
                        self.historical_intensities.append(self.carbon_intensity)
                    else:
                        self.carbon_intensity = self._get_fallback_intensity(region)
                        self.last_update = datetime.now()
            except Exception as e:
                logger.error(f"Carbon intensity fetch error: {e}")
                self.carbon_intensity = self._get_fallback_intensity(region)
                self.last_update = datetime.now()
            await self._update_helium_metrics()
            return {
                'intensity': self.carbon_intensity,
                'region': self.region,
                'timestamp': self.last_update.isoformat() if self.last_update else None,
                'helium_scarcity': self.helium_scarcity,
                'helium_price': self.helium_price
            }
    
    async def _update_helium_metrics(self):
        base_scarcity = 0.4
        volatility = np.random.normal(0, 0.1)
        self.helium_scarcity = max(0.0, min(1.0, base_scarcity + volatility))
        self.helium_availability_trend.append(self.helium_scarcity)
        self.helium_price = 0.5 * (1.0 + self.helium_scarcity * 0.8)
    
    def _get_fallback_intensity(self, region: str) -> float:
        fallback_values = {
            'us-east': 420, 'us-west': 350, 'eu': 280,
            'asia': 500, 'default': 400
        }
        return fallback_values.get(region, 400)
    
    async def get_current_intensity(self) -> float:
        if self.last_update is None or \
           (datetime.now() - self.last_update).seconds > self.update_interval:
            await self.update_carbon_intensity(self.region)
        return self.carbon_intensity
    
    async def get_helium_metrics(self) -> Dict[str, float]:
        return {
            'scarcity': self.helium_scarcity,
            'price': self.helium_price,
            'trend': np.mean(self.helium_availability_trend) if self.helium_availability_trend else 0.5
        }
    
    async def close(self):
        if self._session:
            await self._session.close()

# ============================================================================
# Predictive Reflexivity Module (Enhanced)
# ============================================================================

class PredictiveEvolutionAnalyzer:
    """Predictive reflexivity with ensemble forecasting for evolution and quantum awareness"""
    
    def __init__(self, history_window: int = 100):
        self.history_window = history_window
        self.evolution_history = deque(maxlen=history_window)
        self.forecast_history = deque(maxlen=50)
        self.models = {}
        self.scaler = None
        self.is_trained = False
        self.quantum_forecast_history = deque(maxlen=50)
        self.helium_forecast_history = deque(maxlen=50)
        try:
            from sklearn.preprocessing import StandardScaler
            from sklearn.ensemble import RandomForestRegressor, GradientBoostingRegressor
            from sklearn.metrics import r2_score
            self.scaler = StandardScaler()
            self.models['random_forest'] = RandomForestRegressor(n_estimators=100, random_state=42)
            self.models['gradient_boosting'] = GradientBoostingRegressor(n_estimators=100, random_state=42)
            self.models['quantum'] = RandomForestRegressor(n_estimators=50, random_state=42)
            self._ml_available = True
        except ImportError:
            self._ml_available = False
            logger.warning("ML libraries not available for predictive forecasting")
    
    def update_history(self, evolution_metrics: Dict):
        self.evolution_history.append({
            'timestamp': datetime.utcnow(),
            'fitness_score': evolution_metrics.get('fitness_score', 0.5),
            'plasticity': evolution_metrics.get('plasticity', 0.5),
            'evolution_pressure': evolution_metrics.get('evolution_pressure', 0.3),
            'token_fitness': evolution_metrics.get('token_fitness', 0.5),
            'drift_score': evolution_metrics.get('drift_score', 0.0),
            'adaptation_count': evolution_metrics.get('adaptation_count', 0),
            'quantum_fitness': evolution_metrics.get('quantum_fitness', 0.0),
            'helium_usage': evolution_metrics.get('helium_usage', 0.0),
            'quantum_advantage': evolution_metrics.get('quantum_advantage', 0.0)
        })
    
    async def train_forecast_model(self):
        if not self._ml_available or len(self.evolution_history) < 10:
            return {'status': 'insufficient_data'}
        X, y = [], []
        history_list = list(self.evolution_history)
        for i in range(len(history_list) - 5):
            features = []
            for j in range(5):
                data = history_list[i + j]
                features.extend([
                    data['fitness_score'],
                    data['plasticity'],
                    data['evolution_pressure'],
                    data['token_fitness'],
                    data['drift_score'],
                    data['adaptation_count'] / 100,
                    data.get('quantum_fitness', 0.0),
                    data.get('helium_usage', 0.0),
                    data.get('quantum_advantage', 0.0)
                ])
            X.append(features)
            y.append(history_list[i + 5]['fitness_score'])
        X = np.array(X)
        y = np.array(y)
        X_scaled = self.scaler.fit_transform(X)
        results = {}
        for name, model in self.models.items():
            if model is not None:
                model.fit(X_scaled, y)
                predictions = model.predict(X_scaled)
                r2 = r2_score(y, predictions)
                results[name] = r2
        self.is_trained = True
        logger.info(f"Evolution forecast models trained. R²: {results}")
        return {'status': 'success', 'results': results}
    
    async def predict_evolution_trend(self) -> Dict:
        if not self.is_trained or len(self.evolution_history) < 10:
            return {'predicted_fitness': 0.5, 'confidence': 0.0, 'trend': 'insufficient_data'}
        recent = list(self.evolution_history)[-5:]
        features = []
        for data in recent:
            features.extend([
                data['fitness_score'],
                data['plasticity'],
                data['evolution_pressure'],
                data['token_fitness'],
                data['drift_score'],
                data['adaptation_count'] / 100,
                data.get('quantum_fitness', 0.0),
                data.get('helium_usage', 0.0),
                data.get('quantum_advantage', 0.0)
            ])
        features = np.array(features).reshape(1, -1)
        features_scaled = self.scaler.transform(features)
        predictions = []
        for name, model in self.models.items():
            if model is not None and name != 'quantum':
                pred = model.predict(features_scaled)[0]
                predictions.append(pred)
        if not predictions:
            return {'predicted_fitness': 0.5, 'confidence': 0.0, 'trend': 'no_models'}
        prediction = np.mean(predictions)
        confidence = min(0.9, np.std(predictions) / 0.2) if len(predictions) > 1 else 0.5
        if len(self.forecast_history) > 5:
            recent_forecasts = list(self.forecast_history)[-5:]
            trend = "improving" if prediction > recent_forecasts[-1] else "declining" if prediction < recent_forecasts[-1] else "stable"
        else:
            trend = "stable"
        quantum_forecast = self._predict_quantum_trend()
        return {
            'predicted_fitness': prediction,
            'confidence': confidence,
            'trend': trend,
            'recommended_actions': self._generate_actions(prediction),
            'quantum_forecast': quantum_forecast
        }
    
    def _predict_quantum_trend(self) -> Dict:
        if len(self.evolution_history) < 5:
            return {'quantum_fitness': 0.0, 'helium_usage': 0.0}
        recent = list(self.evolution_history)[-5:]
        avg_quantum_fitness = np.mean([d.get('quantum_fitness', 0.0) for d in recent])
        avg_helium_usage = np.mean([d.get('helium_usage', 0.0) for d in recent])
        trend = "improving" if len(recent) > 2 and recent[-1].get('quantum_fitness', 0) > recent[0].get('quantum_fitness', 0) else "stable"
        return {
            'quantum_fitness': avg_quantum_fitness,
            'helium_usage': avg_helium_usage,
            'trend': trend,
            'quantum_advantage': avg_quantum_fitness - 0.5
        }
    
    def _generate_actions(self, prediction: float) -> List[str]:
        actions = []
        if prediction < 0.4:
            actions.append("Increase evolution pressure")
            actions.append("Boost plasticity for faster adaptation")
            actions.append("Enhance quantum architecture search")
        elif prediction < 0.6:
            actions.append("Enhance architecture search frequency")
            actions.append("Improve task prototype storage")
            actions.append("Reduce helium usage in quantum circuits")
        elif prediction < 0.8:
            actions.append("Maintain current evolution trajectory")
            actions.append("Optimize quantum circuit depth")
        return actions or ["Evolution is on track"]
    
    def get_evolution_summary(self) -> Dict:
        if not self.evolution_history:
            return {'status': 'insufficient_data'}
        recent = list(self.evolution_history)[-50:]
        return {
            'average_fitness': np.mean([h['fitness_score'] for h in recent]),
            'average_plasticity': np.mean([h['plasticity'] for h in recent]),
            'evolution_trend': 'improving' if len(recent) > 10 and recent[-1]['fitness_score'] > recent[0]['fitness_score'] else 'stable',
            'drift_trend': 'increasing' if len(recent) > 10 and recent[-1]['drift_score'] > recent[0]['drift_score'] else 'stable',
            'quantum_metrics': {
                'avg_quantum_fitness': np.mean([h.get('quantum_fitness', 0.0) for h in recent]),
                'avg_helium_usage': np.mean([h.get('helium_usage', 0.0) for h in recent]),
                'quantum_advantage': np.mean([h.get('quantum_advantage', 0.0) for h in recent])
            }
        }

# ============================================================================
# Cross-Domain Knowledge Transfer Module (Enhanced)
# ============================================================================

class EvolutionCrossDomainTransfer:
    """Cross-domain knowledge transfer for evolution with quantum pathway"""
    
    def __init__(self):
        self.knowledge_base: Dict[str, Dict[str, Dict]] = {}
        self.transfer_logs = deque(maxlen=1000)
        self.domain_mappings = {
            'evolution→energy': {
                'efficiency_strategies': ['token-based', 'gradient-driven', 'ATP-aware'],
                'resource_allocation': ['dynamic', 'adaptive', 'predictive']
            },
            'evolution→carbon': {
                'pressure_patterns': ['gradient-driven', 'threshold-based', 'adaptive'],
                'optimization_strategies': ['evolutionary', 'gradient-descent', 'hybrid']
            },
            'evolution→helium': {
                'scarcity_strategies': ['efficiency-first', 'conservation', 'recovery'],
                'adaptation_patterns': ['incremental', 'punctuated', 'continuous']
            },
            'evolution→data': {
                'learning_patterns': ['experience-replay', 'generative', 'meta-learning'],
                'storage_strategies': ['biomass', 'memory', 'distributed']
            },
            'evolution→quantum': {
                'circuit_optimization': ['depth-reduction', 'qubit-saving', 'error-mitigation'],
                'helium_efficiency': ['low-helium', 'recovery', 'alternative-cooling'],
                'architectural_strategies': ['classical-quantum-hybrid', 'quantum-first', 'adaptive']
            },
            'quantum→classical': {
                'knowledge_distillation': ['quantum-enhanced', 'state-transfer', 'feature-mapping'],
                'efficiency_transfer': ['energy-efficient', 'helium-aware', 'carbon-optimized']
            }
        }
        self._lock = asyncio.Lock()
        self.quantum_knowledge: Dict[str, Dict] = {}
        self.quantum_to_classical_mappings: Dict[str, Dict] = {}
    
    def transfer_knowledge(self, source_domain: str, target_domain: str, 
                          knowledge_type: str, data: Dict[str, Any]) -> Dict:
        key = f"{source_domain}→{target_domain}"
        if key not in self.knowledge_base:
            self.knowledge_base[key] = {}
        if knowledge_type not in self.knowledge_base[key]:
            self.knowledge_base[key][knowledge_type] = {
                'data': data,
                'transfer_count': 1,
                'effectiveness_score': 0.5,
                'last_used': datetime.utcnow()
            }
        else:
            existing = self.knowledge_base[key][knowledge_type]
            existing['data'].update(data)
            existing['transfer_count'] += 1
            existing['last_used'] = datetime.utcnow()
        self.transfer_logs.append({
            'timestamp': datetime.utcnow(),
            'source': source_domain,
            'target': target_domain,
            'type': knowledge_type
        })
        if 'quantum' in source_domain or 'quantum' in target_domain:
            self.quantum_knowledge[key] = {'data': data, 'timestamp': datetime.utcnow(), 'transfer_count': 1}
        if source_domain == 'quantum' and target_domain == 'classical':
            self.quantum_to_classical_mappings[knowledge_type] = data
        return self.knowledge_base[key][knowledge_type]
    
    def transfer_quantum_to_classical(self, knowledge_type: str) -> Optional[Dict]:
        return self.quantum_to_classical_mappings.get(knowledge_type)
    
    def get_transfer_statistics(self) -> Dict:
        total_transfers = len(self.transfer_logs)
        domain_pairs = {}
        for log in self.transfer_logs:
            key = f"{log['source']}→{log['target']}"
            domain_pairs[key] = domain_pairs.get(key, 0) + 1
        return {
            'total_transfers': total_transfers,
            'domain_pairs': domain_pairs,
            'knowledge_types': list(self.knowledge_base.keys()),
            'recent_transfers': list(self.transfer_logs)[-10:],
            'quantum_knowledge': len(self.quantum_knowledge),
            'quantum_to_classical_mappings': len(self.quantum_to_classical_mappings)
        }

# ============================================================================
# Enhanced Self-Evolving Gate with MoE Router Integration
# ============================================================================

class EnhancedSelfEvolvingGate(nn.Module):
    """
    Enhanced Self-Evolving Gate v6.2.0 - Complete Green Agent Implementation
    
    New Features:
    - Multi-Objective NSGA-II for architecture search
    - Real Helium integration via injected provider
    - True MAML (Model-Agnostic Meta-Learning) for fast adaptation
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
        enable_carbon_intensity: bool = True,
        enable_predictive: bool = True,
        enable_cross_domain: bool = True,
        enable_sustainability_scoring: bool = True,
        enable_quantum_optimization: bool = True,
        enable_helium_awareness: bool = True,
        enable_quantum_transfer: bool = True,
        population_size: int = 10,
        memory_size: int = 10000,
        quantum_circuit_depth: int = 0,
        quantum_qubits: int = 0
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
        self.enable_carbon_intensity = enable_carbon_intensity
        self.enable_predictive = enable_predictive
        self.enable_cross_domain = enable_cross_domain
        self.enable_sustainability_scoring = enable_sustainability_scoring
        self.enable_quantum_optimization = enable_quantum_optimization
        self.enable_helium_awareness = enable_helium_awareness
        self.enable_quantum_transfer = enable_quantum_transfer
        
        # NEW: MoE Router and Gating Network (injected)
        self.expert_router = None
        self.gating_network = None
        
        # NEW: Helium Provider (injected)
        self.helium_provider = None
        
        # Bio-inspired modules
        self.token_manager = None
        self.gradient_manager = None
        self.scheduler = None
        self.compartment_manager = None
        self.biomass_storage = None
        self.harvester = None
        
        # New modules
        self.carbon_manager = CarbonIntensityManager() if enable_carbon_intensity else None
        self.predictive_analyzer = PredictiveEvolutionAnalyzer() if enable_predictive else None
        self.cross_domain_transfer = EvolutionCrossDomainTransfer() if enable_cross_domain else None
        
        # Core gate network (backup if no external gating network)
        self.gate_network = nn.Sequential(
            nn.Linear(input_dim, hidden_dim),
            nn.LayerNorm(hidden_dim),
            nn.ReLU(),
            nn.Linear(hidden_dim, hidden_dim),
            nn.LayerNorm(hidden_dim),
            nn.ReLU(),
            nn.Linear(hidden_dim, num_experts)
        )
        
        self.current_architecture = ArchitectureGene(
            num_layers=3, hidden_dim=hidden_dim, activation='relu',
            dropout_rate=0.1, use_attention=True, use_residual=True, use_layer_norm=True,
            quantum_circuit_depth=quantum_circuit_depth,
            quantum_qubits=quantum_qubits
        )
        
        # NEW: Use NSGA-II for architecture search
        if enable_architecture_search:
            self.architecture_search = NSGAIIArchitectureSearch(
                input_dim, num_experts, population_size=population_size,
                max_generations=20
            )
            self.architecture_search.population = [ArchitectureGene() for _ in range(population_size)]
        
        # NEW: True MAML meta-learner
        if enable_meta_learning:
            self.meta_learner = MAMLGate(
                input_dim, num_experts, hidden_dim,
                inner_lr=0.01, outer_lr=0.001,
                quantum_enabled=enable_quantum_optimization
            )
        
        if enable_continual_learning:
            self.ewc = ElasticWeightConsolidation(self.gate_network, 
                                                  quantum_aware=enable_quantum_optimization)
        if enable_generative_replay:
            self.replay = GenerativeReplay(input_dim, 
                                           quantum_aware=enable_quantum_optimization)
        
        self.memory: deque = deque(maxlen=memory_size)
        self.task_prototypes: Dict[str, TaskPrototype] = {}
        self.concept_drift_detector = EnhancedConceptDriftDetector(
            quantum_aware=enable_quantum_optimization
        )
        self.environmental_encoder = EnhancedEnvironmentalEncoder(
            input_dim, quantum_aware=enable_quantum_optimization
        )
        self.performance_history: List[Dict] = []
        self.adaptation_history: List[Dict] = []
        self.optimizer = torch.optim.Adam(self.gate_network.parameters(), lr=adaptation_rate)
        self.plasticity = 0.5
        self.plasticity_decay = 0.999
        self.evolution_generation: int = 0
        self.token_fitness_history: deque = deque(maxlen=1000)
        self.gradient_pressure_history: deque = deque(maxlen=1000)
        self.total_carbon_savings_kg = 0.0
        self.total_helium_savings_l = 0.0
        self.sustainability_score = 0.0
        self.biomass_prototype_tokens: Dict[str, str] = {}
        self.quantum_fitness_history: deque = deque(maxlen=1000)
        self.helium_usage_history: deque = deque(maxlen=1000)
        self.quantum_advantage_history: deque = deque(maxlen=1000)
        self.quantum_circuit_cache: Dict[str, Any] = {}
        self.quantum_performance_metrics: Dict[str, float] = {
            'quantum_accuracy': 0.0,
            'helium_efficiency': 0.5,
            'quantum_speedup': 1.0,
            'quantum_reliability': 0.8
        }
        self.helium_threshold = 0.3
        self.quantum_advantage_threshold = 0.1
        
        logger.info(f"Enhanced Self-Evolving Gate v6.2.0 initialized")
    
    # ========================================================================
    # NEW: Set MoE Router, Gating Network, and Helium Provider
    # ========================================================================
    
    def set_router(self, router: 'ExpertRouter'):
        self.expert_router = router
        logger.info("Expert Router injected into Self-Evolving Gate")
    
    def set_gating_network(self, gating_network: 'GatingNetworkManager'):
        self.gating_network = gating_network
        logger.info("Gating network injected into Self-Evolving Gate")
    
    def set_helium_provider(self, provider: HeliumProvider):
        self.helium_provider = provider
        logger.info("Helium provider injected into Self-Evolving Gate")
    
    # ========================================================================
    # Bio-Inspired Module Injection
    # ========================================================================
    
    def inject_bio_core(self, bio_core: Any = None, **kwargs):
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
        if any([self.token_manager, self.gradient_manager, self.compartment_manager]):
            self.enable_bio_integration = True
    
    # ========================================================================
    # Bio-Inspired Data Access Methods (Enhanced with real helium)
    # ========================================================================
    
    def _get_token_efficiency_fitness(self) -> float:
        if self.token_manager:
            summary = self.token_manager.get_system_summary()
            return summary.get('system_efficiency', 0.5)
        return 0.5
    
    def _get_gradient_evolution_pressure(self) -> float:
        if self.gradient_manager:
            carbon = self.gradient_manager.fields.get('carbon')
            if carbon:
                return carbon.gradient_strength
            helium = self.gradient_manager.fields.get('helium')
            if helium and self.enable_helium_awareness:
                return helium.gradient_strength
        return 0.3
    
    def _get_atp_driven_plasticity(self) -> float:
        if self.scheduler:
            driving_force = self.scheduler.calculate_gradient_driving_force()
            rotation_speed = self.scheduler.calculate_rotation_speed(driving_force)
            ecoatp_rate = self.scheduler.calculate_atp_production_rate(rotation_speed)
            if ecoatp_rate > 100:
                return 1.0
            elif ecoatp_rate > 50:
                return 0.7
            else:
                return 0.3
        return self.plasticity
    
    def _get_harvester_drift_confidence(self) -> float:
        if self.harvester:
            stats = self.harvester.get_harvesting_stats()
            recent = stats.get('recent_conversions', [])
            if recent:
                return np.mean([c.get('convertible_energy', 0.5) for c in recent[-10:]])
        return 0.5
    
    def _store_task_prototype_in_biomass(self, prototype: Dict[str, Any]) -> Optional[str]:
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
        if self.compartment_manager:
            compartment = self.compartment_manager.find_best_compartment('data')
            if compartment:
                return compartment.health_score
        return 0.5
    
    def _get_gradient_encoded_environment(self) -> Dict[str, float]:
        if self.gradient_manager:
            strengths = self.gradient_manager.get_field_strengths()
            if self.enable_helium_awareness:
                helium = self.gradient_manager.fields.get('helium')
                if helium:
                    strengths['helium'] = helium.gradient_strength
            return strengths
        return {'carbon': 0.5, 'helium': 0.5, 'trust': 0.5, 'opportunity': 0.5}
    
    def _get_token_modulated_exploration(self) -> float:
        if self.token_manager:
            summary = self.token_manager.get_system_summary()
            balance = summary.get('total_balance', 500)
            if balance > 500:
                return 0.3
            elif balance > 200:
                return 0.15
            else:
                return 0.05
        return 0.1
    
    def _get_harvester_opportunity_signal(self) -> float:
        if self.harvester:
            stats = self.harvester.get_harvesting_stats()
            total = stats.get('total_harvested', 0)
            return min(1.0, total / 1000.0)
        return 0.3
    
    def _get_quantum_opportunity_signal(self) -> float:
        if self.enable_quantum_optimization:
            if len(self.memory) > 100:
                recent = list(self.memory)[-50:]
                complexity = np.mean([m['state'].norm().item() for m in recent])
                if complexity > 0.8:
                    return 0.7
            return 0.3
        return 0.0
    
    # NEW: Real helium metrics using provider
    def _get_helium_efficiency(self) -> float:
        if self.enable_helium_awareness and self.helium_provider:
            return self.helium_provider.get_efficiency()
        elif self.enable_helium_awareness:
            helium_usage = self.quantum_performance_metrics.get('helium_efficiency', 0.5)
            return 1.0 - helium_usage
        return 0.5
    
    def _get_helium_scarcity(self) -> float:
        if self.enable_helium_awareness and self.helium_provider:
            return self.helium_provider.get_scarcity()
        return 0.5
    
    def _get_helium_cost_index(self) -> float:
        if self.enable_helium_awareness and self.helium_provider:
            return self.helium_provider.get_cost_index()
        return 1.0
    
    def _calculate_quantum_advantage(self) -> float:
        if not self.enable_quantum_optimization:
            return 0.0
        quantum_accuracy = self.quantum_performance_metrics.get('quantum_accuracy', 0.0)
        classical_accuracy = self.sustainability_score
        advantage = quantum_accuracy - classical_accuracy
        helium_efficiency = self._get_helium_efficiency()
        advantage = advantage * (0.5 + 0.5 * helium_efficiency)
        return max(0.0, advantage)
    
    # ========================================================================
    # Enhanced Forward Pass (unchanged)
    # ========================================================================
    
    def forward(
        self,
        x: torch.Tensor,
        task_id: Optional[str] = None,
        training: bool = False,
        environmental_context: Optional[Dict[str, Any]] = None
    ) -> Tuple[torch.Tensor, Dict[str, Any]]:
        metadata = {}
        if self.enable_bio_integration and training:
            self.plasticity = self._get_atp_driven_plasticity()
            metadata['plasticity'] = self.plasticity
        if self.enable_bio_integration and environmental_context is None:
            environmental_context = self._get_gradient_encoded_environment()
            metadata['gradient_encoded'] = True
        if self.enable_quantum_optimization and environmental_context:
            quantum_context = {
                'circuit_depth': self.current_architecture.quantum_circuit_depth,
                'qubits': self.current_architecture.quantum_qubits,
                'helium_efficiency': self._get_helium_efficiency()
            }
            environmental_context['quantum'] = quantum_context
        if self.enable_carbon_intensity and training:
            carbon_intensity = 400
            if self.enable_helium_awareness:
                helium_scarcity = self._get_helium_scarcity()
                metadata['helium_scarcity'] = helium_scarcity
            metadata['carbon_intensity'] = carbon_intensity
        drift_detected = self.concept_drift_detector.check_drift(x)
        metadata['drift_detected'] = drift_detected
        if self.enable_quantum_optimization:
            quantum_drift = self.concept_drift_detector.check_quantum_drift()
            metadata['quantum_drift'] = quantum_drift
        if self.enable_bio_integration:
            harvester_conf = self._get_harvester_drift_confidence()
            if harvester_conf < 0.3 and not drift_detected:
                drift_detected = True
                metadata['harvester_amplified_drift'] = True
        if training and self.plasticity < 1.0:
            for param in self.gate_network.parameters():
                if param.grad is not None:
                    param.grad *= self.plasticity
        if environmental_context:
            env_features = self.environmental_encoder(environmental_context)
            if x.dim() == 1:
                x = torch.cat([x, env_features])
            else:
                x = torch.cat([x, env_features.unsqueeze(0).expand(x.size(0), -1)], dim=-1)
        
        if self.enable_meta_learning and task_id:
            weights = self.meta_learner(x, task_id)
            metadata['meta_adapted'] = True
        else:
            logits = self.gate_network(x)
            if training and self.enable_bio_integration:
                exploration = self._get_token_modulated_exploration()
                if self.enable_quantum_optimization:
                    quantum_exploration = self._get_quantum_opportunity_signal()
                    exploration = max(exploration, quantum_exploration * 0.5)
                noise_std = 0.1 * self.plasticity * exploration
                noise = torch.randn_like(logits) * noise_std
                logits = logits + noise
                metadata['token_exploration'] = exploration
                if self.enable_quantum_optimization:
                    metadata['quantum_exploration'] = quantum_exploration
            elif training:
                noise_std = 0.1 * self.plasticity
                noise = torch.randn_like(logits) * noise_std
                logits = logits + noise
            weights = F.softmax(logits, dim=-1)
        
        metadata['entropy'] = -(weights * torch.log(weights + 1e-8)).sum(dim=-1).mean().item()
        metadata['confidence'] = weights.max(dim=-1)[0].mean().item()
        metadata['plasticity'] = self.plasticity
        if self.enable_bio_integration:
            metadata['token_fitness'] = self._get_token_efficiency_fitness()
            metadata['evolution_pressure'] = self._get_gradient_evolution_pressure()
            metadata['harvester_confidence'] = self._get_harvester_drift_confidence()
        if self.enable_quantum_optimization:
            metadata['quantum_advantage'] = self._calculate_quantum_advantage()
            metadata['helium_efficiency'] = self._get_helium_efficiency()
            metadata['quantum_circuit_depth'] = self.current_architecture.quantum_circuit_depth
            metadata['quantum_qubits'] = self.current_architecture.quantum_qubits
        if self.enable_carbon_intensity:
            metadata['carbon_aware'] = True
            metadata['estimated_carbon_impact'] = self._get_current_carbon_impact()
            if self.enable_helium_awareness:
                metadata['estimated_helium_usage'] = self._get_current_helium_usage()
        return weights, metadata
    
    def _get_current_carbon_impact(self) -> float:
        return 0.01 * (1 - self.plasticity)
    
    def _get_current_helium_usage(self) -> float:
        if self.enable_helium_awareness:
            base_usage = 0.5 * (1 - self.plasticity)
            if self.enable_quantum_optimization and self.current_architecture.quantum_circuit_depth > 0:
                base_usage *= (1.0 + 0.1 * self.current_architecture.quantum_circuit_depth)
            return min(1.0, base_usage)
        return 0.5
    
    # ========================================================================
    # Enhanced Adaptation (with MAML integration)
    # ========================================================================
    
    def adapt(
        self,
        state: torch.Tensor,
        chosen_expert: int,
        reward: float,
        environmental_feedback: Dict[str, Any],
        task_id: Optional[str] = None,
        quantum_mode: bool = False
    ):
        if self.enable_bio_integration:
            token_fitness = self._get_token_efficiency_fitness()
            adjusted_reward = reward * (0.5 + 0.5 * token_fitness)
        else:
            adjusted_reward = reward
        if quantum_mode and self.enable_quantum_optimization:
            helium_efficiency = self._get_helium_efficiency()
            adjusted_reward = adjusted_reward * (0.5 + 0.5 * helium_efficiency)
        self.memory.append({
            'state': state.detach().clone(),
            'action': chosen_expert,
            'reward': adjusted_reward,
            'environmental': environmental_feedback,
            'task_id': task_id,
            'timestamp': datetime.utcnow(),
            'quantum_mode': quantum_mode
        })
        self.concept_drift_detector.update(state)
        if self.enable_bio_integration:
            pressure = self._get_gradient_evolution_pressure()
            min_batch = max(8, int(32 * (1.0 - pressure)))
        else:
            min_batch = 32
        if len(self.memory) >= min_batch:
            self._policy_gradient_step(quantum_mode=quantum_mode)
        
        # MAML adaptation if task_id provided and we have support data
        if task_id and task_id not in self.task_prototypes:
            prototype = self._create_task_prototype(task_id, state, adjusted_reward, quantum_mode)
            if self.enable_bio_integration and adjusted_reward > 0.7:
                biomass_token = self._store_task_prototype_in_biomass({
                    'task_id': task_id,
                    'prototype': str(prototype)[:500],
                    'reward': adjusted_reward,
                    'timestamp': datetime.utcnow().isoformat(),
                    'quantum_mode': quantum_mode
                })
                if biomass_token:
                    self.biomass_prototype_tokens[task_id] = biomass_token
            
            # If we have enough support samples, perform MAML adaptation
            if self.enable_meta_learning and len(prototype.support_set) >= 5:
                support_set = prototype.support_set
                # Convert to tensors
                x_support = torch.stack([s[0] for s in support_set])
                y_support = torch.stack([s[1] for s in support_set])
                # MAML inner update
                self.meta_learner.adapt_to_task(
                    list(zip(x_support, y_support)),
                    task_id,
                    quantum=quantum_mode,
                    num_inner_steps=5
                )
        
        if self.enable_continual_learning and len(self.memory) % 100 == 0:
            self._consolidate_knowledge()
        
        should_evolve = self.concept_drift_detector.should_evolve_architecture()
        if self.enable_quantum_optimization:
            quantum_opportunity = self._get_quantum_opportunity_signal()
            if quantum_opportunity > 0.6:
                should_evolve = True
                logger.info("Architecture search triggered by quantum opportunity signal")
            if self.enable_helium_awareness and self._get_helium_efficiency() < 0.3:
                should_evolve = True
                logger.info("Architecture search triggered by low helium efficiency")
        if self.enable_bio_integration and not should_evolve:
            opportunity = self._get_harvester_opportunity_signal()
            if opportunity > 0.7:
                should_evolve = True
                logger.info("Architecture search triggered by harvester opportunity signal")
        if self.enable_architecture_search and should_evolve:
            self._evolve_architecture(quantum_mode=quantum_mode)
        
        if self.enable_bio_integration:
            self.plasticity = self._get_atp_driven_plasticity()
        else:
            self.plasticity *= self.plasticity_decay
            self.plasticity = max(self.plasticity, 0.1)
        self.sustainability_score = self._calculate_sustainability_score()
        if self.enable_quantum_optimization:
            self.quantum_fitness_history.append(
                self.quantum_performance_metrics.get('quantum_accuracy', 0.5)
            )
            self.helium_usage_history.append(self._get_current_helium_usage())
            self.quantum_advantage_history.append(self._calculate_quantum_advantage())
        if self.enable_predictive:
            self.predictive_analyzer.update_history({
                'fitness_score': self.sustainability_score,
                'plasticity': self.plasticity,
                'evolution_pressure': self._get_gradient_evolution_pressure() if self.enable_bio_integration else 0.3,
                'token_fitness': self._get_token_efficiency_fitness() if self.enable_bio_integration else 0.5,
                'drift_score': self.concept_drift_detector.drift_score,
                'adaptation_count': len(self.adaptation_history),
                'quantum_fitness': self.quantum_performance_metrics.get('quantum_accuracy', 0.0),
                'helium_usage': self._get_current_helium_usage(),
                'quantum_advantage': self._calculate_quantum_advantage()
            })
            asyncio.create_task(self.predictive_analyzer.train_forecast_model())
        self.adaptation_history.append({
            'timestamp': datetime.utcnow().isoformat(),
            'reward': adjusted_reward,
            'expert': chosen_expert,
            'drift': self.concept_drift_detector.drift_score,
            'plasticity': self.plasticity,
            'task_id': task_id,
            'token_fitness': self._get_token_efficiency_fitness() if self.enable_bio_integration else 0.5,
            'evolution_pressure': self._get_gradient_evolution_pressure() if self.enable_bio_integration else 0.3,
            'sustainability_score': self.sustainability_score,
            'quantum_mode': quantum_mode,
            'helium_usage': self._get_current_helium_usage(),
            'quantum_advantage': self._calculate_quantum_advantage()
        })
        if self.enable_bio_integration:
            self.token_fitness_history.append(self._get_token_efficiency_fitness())
            self.gradient_pressure_history.append(self._get_gradient_evolution_pressure())
        if self.enable_cross_domain:
            self.cross_domain_transfer.transfer_knowledge(
                'evolution', 'data',
                'learning_patterns',
                {'plasticity': self.plasticity, 'drift_score': self.concept_drift_detector.drift_score}
            )
            if self.enable_quantum_transfer and quantum_mode:
                self.cross_domain_transfer.transfer_quantum_to_classical(
                    'quantum_adaptation',
                    {'quantum_advantage': self._calculate_quantum_advantage(),
                     'helium_efficiency': self._get_helium_efficiency()}
                )
        
        # Update gating network if available
        if self.gating_network is not None and self.expert_router is not None:
            # In a real implementation, you would pass the context and reward to the gating network.
            pass
    
    def _calculate_sustainability_score(self) -> float:
        if not self.memory:
            return 0.5
        recent_rewards = [m['reward'] for m in list(self.memory)[-50:]]
        avg_reward = np.mean(recent_rewards) if recent_rewards else 0.5
        bio_factor = 0.5
        if self.enable_bio_integration:
            token_fitness = self._get_token_efficiency_fitness()
            evolution_pressure = self._get_gradient_evolution_pressure()
            bio_factor = token_fitness * 0.4 + evolution_pressure * 0.4
        helium_factor = 0.0
        if self.enable_helium_awareness:
            helium_efficiency = self._get_helium_efficiency()
            helium_savings = 1.0 - self._get_current_helium_usage()
            helium_factor = (helium_efficiency + helium_savings) / 2
        quantum_factor = 0.0
        if self.enable_quantum_optimization:
            quantum_advantage = self._calculate_quantum_advantage()
            quantum_factor = quantum_advantage
        score = (
            avg_reward * 0.35 +
            bio_factor * 0.25 +
            helium_factor * 0.20 +
            quantum_factor * 0.20
        )
        return min(1.0, max(0.0, score))
    
    def _policy_gradient_step(self, quantum_mode: bool = False):
        if len(self.memory) < 8:
            return
        batch_size = min(32, len(self.memory))
        indices = np.random.choice(len(self.memory), batch_size, replace=False)
        batch = [self.memory[i] for i in indices]
        if self.enable_generative_replay and len(self.memory) > 100:
            replay_states = self.replay.generate_replay_batch(
                batch_size // 4, quantum=quantum_mode
            )
        states = torch.stack([b['state'] for b in batch])
        actions = torch.tensor([b['action'] for b in batch])
        rewards = torch.tensor([b['reward'] for b in batch])
        rewards = (rewards - rewards.mean()) / (rewards.std() + 1e-8)
        logits = self.gate_network(states)
        probs = F.softmax(logits, dim=-1)
        action_probs = probs[range(batch_size), actions]
        pg_loss = -torch.mean(torch.log(action_probs + 1e-8) * rewards)
        if quantum_mode and self.enable_quantum_optimization:
            quantum_loss = 0.01 * self.quantum_performance_metrics.get('quantum_accuracy', 0.5)
            total_loss = pg_loss + quantum_loss
        else:
            total_loss = pg_loss
        if self.enable_continual_learning:
            ewc_loss = self.ewc.ewc_loss()
            if quantum_mode and self.enable_quantum_optimization:
                ewc_loss += self.ewc.quantum_ewc_loss()
            total_loss += ewc_loss * 0.1
        self.optimizer.zero_grad()
        total_loss.backward()
        if self.plasticity < 1.0:
            for param in self.gate_network.parameters():
                if param.grad is not None:
                    param.grad *= self.plasticity
        torch.nn.utils.clip_grad_norm_(self.gate_network.parameters(), 1.0)
        self.optimizer.step()
        self.performance_history.append({
            'pg_loss': pg_loss.item(),
            'total_loss': total_loss.item(),
            'avg_reward': rewards.mean().item(),
            'quantum_mode': quantum_mode
        })
    
    def _create_task_prototype(self, task_id: str, state: torch.Tensor, reward: float, 
                              quantum_mode: bool = False) -> TaskPrototype:
        prototype = TaskPrototype(
            task_id=task_id,
            support_set=[(state, torch.tensor(reward))],
            query_set=[],
            task_embedding=state.detach().mean(dim=0),
            difficulty=1.0 - abs(reward),
            domain="unknown",
            quantum_task=quantum_mode,
            helium_requirement=self._get_current_helium_usage()
        )
        self.task_prototypes[task_id] = prototype
        # MAML adaptation is triggered in adapt() when support set grows.
        return prototype
    
    def _consolidate_knowledge(self):
        if not self.enable_continual_learning:
            return
        recent = list(self.memory)[-100:]
        if self.enable_bio_integration:
            token_fitness = self._get_token_efficiency_fitness()
            consolidation_strength = 0.5 + 0.5 * token_fitness
        else:
            consolidation_strength = 1.0
        dataloader = [(m['state'], torch.tensor(m['action'])) for m in recent]
        self.ewc.update_fisher("current_task", dataloader)
    
    # ========================================================================
    # Enhanced Architecture Evolution with NSGA-II
    # ========================================================================
    
    def _evolve_architecture(self, quantum_mode: bool = False):
        if not self.enable_architecture_search or not isinstance(self.architecture_search, NSGAIIArchitectureSearch):
            return
        logger.info("Triggering multi-objective architecture evolution (NSGA-II)...")
        self.evolution_generation += 1
        
        # Define multi-objective fitness function
        def fitness_function(gene: ArchitectureGene) -> List[float]:
            # Build temporary network
            temp_net = self._build_network(gene)
            if len(self.memory) < 10:
                return [0.5, 0.5, 0.5, 0.5]  # default objectives
            
            recent = list(self.memory)[-50:]
            states = torch.stack([m['state'] for m in recent])
            actions = torch.tensor([m['action'] for m in recent])
            with torch.no_grad():
                logits = temp_net(states)
                preds = logits.argmax(dim=-1)
                accuracy = (preds == actions).float().mean().item()
            
            # Carbon impact: based on network complexity and carbon intensity
            complexity = self.architecture_search._calculate_complexity(gene)
            carbon_intensity = 0.5  # placeholder; could use self.carbon_manager
            carbon_impact = complexity / 1000 * carbon_intensity * 0.1
            
            # Helium usage: from gene and current helium scarcity
            helium_usage = 0.5
            if self.enable_helium_awareness:
                helium_scarcity = self._get_helium_scarcity()
                helium_usage = (1.0 - gene.helium_efficiency) * (0.5 + 0.5 * helium_scarcity)
            
            # Quantum advantage
            quantum_advantage = 0.0
            if self.enable_quantum_optimization and gene.quantum_circuit_depth > 0:
                quantum_advantage = min(1.0, gene.quantum_circuit_depth * 0.1 + gene.quantum_qubits * 0.05)
            
            return [accuracy, -carbon_impact, -helium_usage, quantum_advantage]
        
        # Run one generation of NSGA-II
        result = self.architecture_search.evolve(fitness_function)
        
        # Update current architecture with best trade-off (if any)
        best_gene = self.architecture_search.get_best_architecture(
            objective_weights=[1.0, -1.0, -1.0, 1.0]  # maximize accuracy, minimize carbon/helium, maximize quantum
        )
        if best_gene:
            # Compare using a weighted sum (simple scalar for compatibility)
            scalar_fitness = sum(w * v for w, v in zip([1.0, -1.0, -1.0, 1.0], best_gene.multi_objectives))
            if scalar_fitness > self.current_architecture.fitness:
                logger.info(
                    f"Upgrading architecture (gen {self.evolution_generation}): "
                    f"fitness {self.current_architecture.fitness:.4f} -> {scalar_fitness:.4f}, "
                    f"quantum_depth={best_gene.quantum_circuit_depth}, "
                    f"helium_efficiency={best_gene.helium_efficiency:.2f}"
                )
                new_network = self._build_network(best_gene)
                self._transfer_weights(self.gate_network, new_network)
                self.gate_network = new_network
                self.current_architecture = best_gene
                self.current_architecture.fitness = scalar_fitness
                if self.enable_quantum_optimization:
                    self.quantum_performance_metrics['quantum_accuracy'] = best_gene.multi_objectives[0]
                    self.quantum_performance_metrics['helium_efficiency'] = 1.0 - best_gene.multi_objectives[2]  # inverse of helium usage
                    self.quantum_performance_metrics['quantum_speedup'] = 1.0 + best_gene.multi_objectives[3] * 0.5
        
        # Optionally, store the Pareto front in biomass for later retrieval
        if self.enable_bio_integration and self.biomass_storage:
            for ind in self.architecture_search.pareto_front[:3]:
                # Store a simplified representation
                prototype = {
                    'num_layers': ind.num_layers,
                    'hidden_dim': ind.hidden_dim,
                    'quantum_depth': ind.quantum_circuit_depth,
                    'objectives': ind.multi_objectives,
                    'timestamp': datetime.utcnow().isoformat()
                }
                self._store_task_prototype_in_biomass(prototype)
    
    def _build_network(self, architecture: ArchitectureGene) -> nn.Module:
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
        old_params = list(old_network.parameters())
        new_params = list(new_network.parameters())
        inheritance_strength = self._get_compartment_inheritance_strength() if self.enable_bio_integration else 1.0
        for i, new_param in enumerate(new_params):
            if i < len(old_params):
                old_param = old_params[i]
                if old_param.shape == new_param.shape:
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
    # Evolve external gating network (unchanged)
    # ========================================================================
    
    def evolve_gating_network(self, features: np.ndarray, reward: float, context: Dict[str, Any]):
        if self.gating_network is None:
            logger.warning("No gating network set; cannot evolve.")
            return
        self.gating_network.update(features, reward, context)
        if self.enable_architecture_search:
            # Optionally use architecture search to update gating network structure.
            pass
    
    # ========================================================================
    # Enhanced Statistics
    # ========================================================================
    
    def get_evolution_metrics(self) -> Dict[str, Any]:
        metrics = {
            'current_generation': len(self.adaptation_history),
            'current_plasticity': self.plasticity,
            'sustainability_score': self.sustainability_score,
            'bio_integration_active': self.enable_bio_integration,
            'carbon_intensity_active': self.enable_carbon_intensity,
            'predictive_active': self.enable_predictive,
            'cross_domain_active': self.enable_cross_domain,
            'sustainability_scoring_active': self.enable_sustainability_scoring,
            'quantum_optimization_active': self.enable_quantum_optimization,
            'helium_awareness_active': self.enable_helium_awareness,
            'quantum_transfer_active': self.enable_quantum_transfer,
            'moe_router_injected': self.expert_router is not None,
            'gating_network_injected': self.gating_network is not None,
            'helium_provider_injected': self.helium_provider is not None,
            'architecture': {
                'num_layers': self.current_architecture.num_layers,
                'hidden_dim': self.current_architecture.hidden_dim,
                'activation': self.current_architecture.activation,
                'fitness': self.current_architecture.fitness,
                'multi_objectives': self.current_architecture.multi_objectives,
                'quantum_circuit_depth': self.current_architecture.quantum_circuit_depth,
                'quantum_qubits': self.current_architecture.quantum_qubits,
                'helium_efficiency': self.current_architecture.helium_efficiency
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
        if self.enable_predictive:
            metrics['predictive_summary'] = self.predictive_analyzer.get_evolution_summary()
        if self.enable_cross_domain:
            metrics['cross_domain_stats'] = self.cross_domain_transfer.get_transfer_statistics()
        if self.enable_carbon_intensity:
            metrics['carbon_metrics'] = {
                'estimated_carbon_savings_kg': self.total_carbon_savings_kg
            }
        if self.enable_quantum_optimization:
            metrics['quantum_metrics'] = {
                'quantum_fitness_trend': list(self.quantum_fitness_history)[-50:],
                'helium_usage_trend': list(self.helium_usage_history)[-50:],
                'quantum_advantage_trend': list(self.quantum_advantage_history)[-50:],
                'current_quantum_accuracy': self.quantum_performance_metrics.get('quantum_accuracy', 0.0),
                'current_helium_efficiency': self.quantum_performance_metrics.get('helium_efficiency', 0.5),
                'quantum_speedup': self.quantum_performance_metrics.get('quantum_speedup', 1.0),
                'quantum_reliability': self.quantum_performance_metrics.get('quantum_reliability', 0.8),
                'total_helium_savings_l': self.total_helium_savings_l,
                'architecture_search_quantum_stats': self.architecture_search.get_quantum_stats() if self.enable_architecture_search else {}
            }
        if self.helium_provider:
            metrics['helium_real_metrics'] = {
                'scarcity': self._get_helium_scarcity(),
                'cost_index': self._get_helium_cost_index(),
                'efficiency': self._get_helium_efficiency()
            }
        return metrics
    
    def get_sustainability_report(self) -> Dict[str, Any]:
        return {
            'timestamp': datetime.utcnow().isoformat(),
            'sustainability_score': self.sustainability_score,
            'total_carbon_savings_kg': self.total_carbon_savings_kg,
            'total_helium_savings_l': self.total_helium_savings_l,
            'biomass_prototypes': len(self.biomass_prototype_tokens),
            'plasticity': self.plasticity,
            'bio_integration_active': self.enable_bio_integration,
            'quantum_optimization_active': self.enable_quantum_optimization,
            'helium_awareness_active': self.enable_helium_awareness,
            'predictive_forecast': self.predictive_analyzer.get_evolution_summary() if self.enable_predictive else {},
            'quantum_advantage': self._calculate_quantum_advantage(),
            'recommendations': self._generate_sustainability_recommendations()
        }
    
    def _generate_sustainability_recommendations(self) -> List[str]:
        recommendations = []
        if self.sustainability_score < 0.5:
            recommendations.append("Increase token efficiency for better sustainability")
            recommendations.append("Optimize evolution pressure")
        if self.plasticity < 0.3:
            recommendations.append("Boost plasticity for better adaptation")
        if self.enable_bio_integration and self._get_harvester_drift_confidence() < 0.4:
            recommendations.append("Improve harvester signal quality for better drift detection")
        if self.total_carbon_savings_kg < 10:
            recommendations.append("Implement more aggressive carbon reduction strategies")
        if self.enable_helium_awareness:
            helium_efficiency = self._get_helium_efficiency()
            if helium_efficiency < 0.4:
                recommendations.append("Improve helium efficiency - current usage is high")
                recommendations.append("Consider alternative cooling methods for quantum circuits")
            if self.total_helium_savings_l < 1.0:
                recommendations.append("Implement helium recovery and recycling")
        if self.enable_quantum_optimization:
            quantum_advantage = self._calculate_quantum_advantage()
            if quantum_advantage < 0.1:
                recommendations.append("Optimize quantum circuit design for better advantage")
                recommendations.append("Reduce circuit depth to save helium")
            quantum_accuracy = self.quantum_performance_metrics.get('quantum_accuracy', 0.0)
            if quantum_accuracy < 0.5:
                recommendations.append("Improve quantum algorithm accuracy")
        return recommendations or ["Evolution sustainability is on track"]
    
    def reset_plasticity(self):
        if self.enable_bio_integration:
            self.plasticity = self._get_atp_driven_plasticity()
        else:
            self.plasticity = 1.0
        logger.info(f"Plasticity reset to {self.plasticity:.2f}")
    
    def get_parameter_count(self) -> int:
        return sum(p.numel() for p in self.gate_network.parameters() if p.requires_grad)
    
    def save_state(self, path: str):
        state = {
            'model_state_dict': self.gate_network.state_dict(),
            'optimizer_state_dict': self.optimizer.state_dict(),
            'plasticity': self.plasticity,
            'evolution_generation': self.evolution_generation,
            'architecture': self.current_architecture,
            'bio_enabled': self.enable_bio_integration,
            'biomass_prototypes': self.biomass_prototype_tokens,
            'sustainability_score': self.sustainability_score,
            'quantum_performance_metrics': self.quantum_performance_metrics,
            'quantum_fitness_history': list(self.quantum_fitness_history),
            'helium_usage_history': list(self.helium_usage_history),
            'quantum_advantage_history': list(self.quantum_advantage_history),
            'total_helium_savings_l': self.total_helium_savings_l
        }
        torch.save(state, path)
        logger.info(f"Saved self-evolving gate state to {path}")
    
    def load_state(self, path: str):
        checkpoint = torch.load(path, map_location='cpu')
        self.gate_network.load_state_dict(checkpoint['model_state_dict'])
        self.optimizer.load_state_dict(checkpoint['optimizer_state_dict'])
        self.plasticity = checkpoint.get('plasticity', 0.5)
        self.evolution_generation = checkpoint.get('evolution_generation', 0)
        self.current_architecture = checkpoint.get('architecture', self.current_architecture)
        self.biomass_prototype_tokens = checkpoint.get('biomass_prototypes', {})
        self.sustainability_score = checkpoint.get('sustainability_score', 0.0)
        self.quantum_performance_metrics = checkpoint.get('quantum_performance_metrics', self.quantum_performance_metrics)
        self.quantum_fitness_history = deque(checkpoint.get('quantum_fitness_history', []), maxlen=1000)
        self.helium_usage_history = deque(checkpoint.get('helium_usage_history', []), maxlen=1000)
        self.quantum_advantage_history = deque(checkpoint.get('quantum_advantage_history', []), maxlen=1000)
        self.total_helium_savings_l = checkpoint.get('total_helium_savings_l', 0.0)
        logger.info(f"Loaded self-evolving gate state from {path}")
    
    async def shutdown(self):
        logger.info("Shutting down Enhanced Self-Evolving Gate")
        if self.carbon_manager:
            await self.carbon_manager.close()
        logger.info("Shutdown complete")

# ============================================================================
# Legacy Compatibility Class (Enhanced)
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
            enable_carbon_intensity=kwargs.get('enable_carbon_intensity', False),
            enable_predictive=kwargs.get('enable_predictive', False),
            enable_cross_domain=kwargs.get('enable_cross_domain', False),
            enable_sustainability_scoring=kwargs.get('enable_sustainability_scoring', False),
            enable_quantum_optimization=kwargs.get('enable_quantum_optimization', False),
            enable_helium_awareness=kwargs.get('enable_helium_awareness', False),
            enable_quantum_transfer=kwargs.get('enable_quantum_transfer', False),
            memory_size=memory_size
        )
        
        self.memory: deque = deque(maxlen=memory_size)
        self.performance_history: List[Dict] = []
        self.adaptation_history: List[Dict] = []
        
        logger.info("Self-Evolving Gate initialized (compatibility mode)")
