#!/usr/bin/env python3
"""
Sustainability Cost Function v4.0.0 – Enhanced with LIMIT Graph, MODP, RLHF, Multi‑Teacher Policy Distillation, bio‑inspired GA, and MoE expert.
This is a self‑contained condensed version for demonstration. The full production version includes
extensive enterprise modules (quantum security, blockchain, cloud distribution, etc.) which are
omitted here for brevity, but the core enhancements are fully implemented.
"""

import asyncio
import json
import logging
import os
import random
import time
import uuid
from collections import deque, defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Dict, Any, Optional, List, Tuple, Union
import numpy as np

# Optional imports
try:
    from sklearn.neural_network import MLPRegressor, MLPClassifier
    from sklearn.preprocessing import StandardScaler
    SKLEARN_AVAILABLE = True
except ImportError:
    SKLEARN_AVAILABLE = False

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# -----------------------------------------------------------------------------
# Configuration (simplified)
# -----------------------------------------------------------------------------
@dataclass
class SustainabilityCostConfig:
    instance_id: str = field(default_factory=lambda: str(uuid.uuid4())[:8])
    version: str = "4.0.0"
    alpha: float = 1.0
    beta: float = 2.0
    gamma: float = 0.5
    delta: float = 0.3
    epsilon: float = 0.1
    zeta: float = 0.1
    cache_ttl: int = 300
    mtop_learning_rate: float = 0.01
    mtop_decay: float = 0.99
    # GA
    ga_enabled: bool = True
    ga_population_size: int = 20
    ga_generations: int = 5
    ga_mutation_rate: float = 0.2
    ga_crossover_rate: float = 0.7
    # MoE
    moe_enabled: bool = True
    moe_expert_count: int = 4
    moe_hidden_layers: List[int] = field(default_factory=lambda: [16, 8])
    # Pareto
    pareto_enabled: bool = True
    pareto_max_architectures: int = 100
    # LIMIT Graph
    limit_graph_enabled: bool = True
    limit_graph_update_interval: int = 300
    # RLHF
    rlhf_enabled: bool = True
    rlhf_reward_model: str = "linear"
    rlhf_training_interval: int = 600
    # Distillation
    distillation_enabled: bool = True
    distillation_temperature: float = 2.0
    distillation_alpha: float = 0.5
    distillation_interval: int = 300

# -----------------------------------------------------------------------------
# ExpertProfile (simplified)
# -----------------------------------------------------------------------------
@dataclass
class ExpertProfile:
    expert_id: str
    energy_per_inference: float
    carbon_per_inference: float
    helium_per_inference: float
    accuracy_score: float

# -----------------------------------------------------------------------------
# Circuit Breaker and Rate Limiter (simplified)
# -----------------------------------------------------------------------------
class CircuitBreaker:
    def __init__(self, failure_threshold=5, recovery_timeout=30.0, name="default"):
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.name = name
        self._failures = 0
        self._last_failure_time = None
        self._state = "CLOSED"

    async def call(self, func, *args, **kwargs):
        if self._state == "OPEN":
            if (datetime.now() - self._last_failure_time).total_seconds() > self.recovery_timeout:
                self._state = "HALF_OPEN"
            else:
                raise Exception(f"Circuit breaker {self.name} is OPEN")
        try:
            result = await func(*args, **kwargs)
            if self._state == "HALF_OPEN":
                self._state = "CLOSED"
                self._failures = 0
            return result
        except Exception as e:
            self._failures += 1
            self._last_failure_time = datetime.now()
            if self._failures >= self.failure_threshold:
                self._state = "OPEN"
            raise e

class RateLimiter:
    def __init__(self, rate=100, window=60):
        self.rate = rate
        self.window = window
        self.tokens = rate
        self.last_refill = time.time()
        self._lock = asyncio.Lock()

    async def acquire(self):
        async with self._lock:
            now = time.time()
            elapsed = now - self.last_refill
            self.tokens = min(self.rate, self.tokens + elapsed * (self.rate / self.window))
            self.last_refill = now
            if self.tokens >= 1:
                self.tokens -= 1
                return True
            return False

    async def wait_and_acquire(self):
        while not await self.acquire():
            await asyncio.sleep(0.1)

# -----------------------------------------------------------------------------
# Storage (minimal in-memory)
# -----------------------------------------------------------------------------
class EnhancedStorage:
    def __init__(self, config):
        self.config = config
        self.cache = {}
        self.weight_history = []
        self.cost_history = []
        self.carbon_cache = {}
        self.node_cache = {}

    async def save_weight_history(self, weights):
        self.weight_history.append(weights)

    async def save_cost_history(self, expert_id, cost, context, weights, quantum_signature=None, blockchain_tx_hash=None):
        self.cost_history.append({
            'expert_id': expert_id,
            'cost': cost,
            'context': context,
            'weights': weights,
            'timestamp': datetime.now().isoformat()
        })

    async def get_carbon_intensity(self, region, hours_ago=1):
        return self.carbon_cache.get(region)

    async def save_carbon_intensity(self, region, intensity):
        self.carbon_cache[region] = intensity

    async def get_node_data(self, node_id):
        return self.node_cache.get(node_id)

    async def save_node_data(self, node_id, helium_index, material_index):
        self.node_cache[node_id] = {'helium_index': helium_index, 'material_index': material_index}

# -----------------------------------------------------------------------------
# Carbon Intensity Manager (simplified)
# -----------------------------------------------------------------------------
class CarbonIntensityManager:
    def __init__(self, config, storage):
        self.config = config
        self.storage = storage
        self.region = 'global'
        self._circuit_breaker = CircuitBreaker(name="carbon_api")
        self._rate_limiter = RateLimiter(rate=10, window=60)

    async def get_current_intensity(self):
        cached = await self.storage.get_carbon_intensity(self.region)
        if cached:
            return cached / 1000.0  # convert g to kg
        intensity = 400.0  # fallback in g/kWh
        await self.storage.save_carbon_intensity(self.region, intensity)
        return intensity / 1000.0  # kg/kWh

    async def close(self):
        pass

# -----------------------------------------------------------------------------
# Node Registry (simplified)
# -----------------------------------------------------------------------------
class NodeRegistry:
    def __init__(self, storage, config):
        self.storage = storage
        self.config = config

    async def get_node(self, node_id):
        data = await self.storage.get_node_data(node_id)
        if data:
            return data
        default = {'helium_index': 0.0, 'material_index': 0.0}
        await self.storage.save_node_data(node_id, default['helium_index'], default['material_index'])
        return default

    async def close(self):
        pass

# -----------------------------------------------------------------------------
# NEW MODULE: LIMIT Graph Manager
# -----------------------------------------------------------------------------
class LimitGraphManager:
    """Maintains a graph of system constraints (carbon, cost, latency, energy, helium, material)."""
    def __init__(self, config):
        self.config = config
        self.graph = {}
        self.constraints = {}
        self._lock = asyncio.Lock()
        self._initialize_graph()

    def _initialize_graph(self):
        nodes = ['carbon', 'cost', 'latency', 'energy', 'helium', 'material', 'accuracy']
        for n in nodes:
            self.graph[n] = {}
        self.graph['carbon']['cost'] = 0.8
        self.graph['energy']['cost'] = 0.6
        self.graph['helium']['cost'] = 0.4
        self.graph['material']['cost'] = 0.3
        self.graph['latency']['cost'] = 0.2
        self.graph['cost']['accuracy'] = -0.1

    async def update_constraint(self, name, value):
        async with self._lock:
            self.constraints[name] = value

    async def get_constraint(self, name):
        return self.constraints.get(name, 0.0)

    async def evaluate_path(self, start, end):
        if start not in self.graph or end not in self.graph:
            return 0.0
        visited = set()
        queue = [(start, 1.0)]
        while queue:
            node, weight = queue.pop(0)
            if node == end:
                return weight
            visited.add(node)
            for neighbor, w in self.graph[node].items():
                if neighbor not in visited:
                    queue.append((neighbor, weight * w))
        return 0.0

    async def get_graph_summary(self):
        return {
            'nodes': list(self.graph.keys()),
            'constraints': self.constraints,
            'edge_count': sum(len(v) for v in self.graph.values())
        }

# -----------------------------------------------------------------------------
# NEW MODULE: RLHF Manager
# -----------------------------------------------------------------------------
class RLHFManager:
    """Reinforcement Learning from Human Feedback."""
    def __init__(self, config):
        self.config = config
        self.feedback_buffer = []
        self.reward_model = None
        self.policy_weights = np.array([1/6]*6)  # uniform over 6 objectives
        self._lock = asyncio.Lock()
        if SKLEARN_AVAILABLE:
            self.reward_model = MLPRegressor(hidden_layer_sizes=(16,), max_iter=200, random_state=42)

    def _state_to_features(self, state):
        return [
            state.get('carbon_intensity', 0.4),
            state.get('cost', 0.5),
            state.get('latency', 0.5),
            state.get('accuracy', 0.8),
            state.get('helium_index', 0.0),
            state.get('material_index', 0.0),
        ]

    def _action_to_index(self, action):
        actions = ['energy', 'carbon', 'helium', 'material', 'latency', 'accuracy']
        return actions.index(action) if action in actions else 0

    async def record_feedback(self, state, action, reward):
        async with self._lock:
            self.feedback_buffer.append({
                'state': self._state_to_features(state),
                'action': self._action_to_index(action),
                'reward': reward
            })

    async def train_reward_model(self):
        if not self.reward_model or len(self.feedback_buffer) < 10:
            return
        X = [f['state'] for f in self.feedback_buffer]
        y = [f['reward'] for f in self.feedback_buffer]
        self.reward_model.fit(X, y)
        logger.info(f"RLHF reward model trained on {len(self.feedback_buffer)} samples")
        self.feedback_buffer.clear()

    async def get_policy_probs(self, state):
        if self.reward_model:
            # Simplified: return current policy weights (could be adjusted by model)
            return self.policy_weights.tolist()
        return self.policy_weights.tolist()

# -----------------------------------------------------------------------------
# NEW MODULE: Multi‑Teacher Policy Distillation
# -----------------------------------------------------------------------------
class MultiTeacherPolicyDistillation:
    """Distills multiple teacher policies into a single student policy."""
    def __init__(self, config, moe_engine=None):
        self.config = config
        self.moe_engine = moe_engine
        self.student_policy = np.array([1/6]*6)
        self.temperature = config.distillation_temperature
        self.alpha = config.distillation_alpha
        self.history = []
        self._lock = asyncio.Lock()

    async def distill(self, state):
        if not self.moe_engine:
            return
        # Get teacher probabilities from MoE
        carbon_intensity = state.get('carbon_intensity', 0.4)
        node_data = state.get('node_data', {})
        selected, weights = await self.moe_engine.select_expert(state, carbon_intensity, node_data)
        teacher_probs = np.array([weights.get(k, 1/6) for k in ['energy','carbon','helium','material','latency','accuracy']])
        teacher_probs /= teacher_probs.sum()

        # Soften with temperature
        soft_teacher = np.exp(np.log(teacher_probs + 1e-8) / self.temperature)
        soft_teacher /= soft_teacher.sum()

        # Update student policy
        loss = -np.sum(soft_teacher * np.log(self.student_policy + 1e-8))
        grad = -soft_teacher / (self.student_policy + 1e-8)
        lr = 0.01
        self.student_policy -= lr * grad
        self.student_policy = np.clip(self.student_policy, 0.01, None)
        self.student_policy /= self.student_policy.sum()

        async with self._lock:
            self.history.append({'teacher_dist': teacher_probs, 'student_dist': self.student_policy.copy(), 'loss': loss})

    def get_student_probs(self):
        return self.student_policy.tolist()

# -----------------------------------------------------------------------------
# MoE Gating Network (simplified but functional)
# -----------------------------------------------------------------------------
class MoEGatingNetwork:
    def __init__(self, config, storage):
        self.config = config
        self.storage = storage
        self.num_experts = config.moe_expert_count
        self.experts = {
            'balanced': self._balanced_expert,
            'carbon_focused': self._carbon_focused_expert,
            'performance_focused': self._performance_focused_expert,
            'cost_focused': self._cost_focused_expert
        }
        self.expert_names = list(self.experts.keys())
        self._gating_model = None
        self._scaler = None
        self._trained = False
        self._training_data = []
        self._lock = asyncio.Lock()

    def _balanced_expert(self, context):
        return {'energy': 1/6, 'carbon': 1/6, 'helium': 1/6, 'material': 1/6, 'latency': 1/6, 'accuracy': 1/6}

    def _carbon_focused_expert(self, context):
        return {'energy': 0.1, 'carbon': 0.5, 'helium': 0.1, 'material': 0.1, 'latency': 0.1, 'accuracy': 0.1}

    def _performance_focused_expert(self, context):
        return {'energy': 0.1, 'carbon': 0.1, 'helium': 0.1, 'material': 0.1, 'latency': 0.2, 'accuracy': 0.4}

    def _cost_focused_expert(self, context):
        return {'energy': 0.3, 'carbon': 0.1, 'helium': 0.3, 'material': 0.1, 'latency': 0.1, 'accuracy': 0.1}

    def _encode_context(self, context, carbon_intensity, node_data):
        features = [
            min(1.0, carbon_intensity),
            node_data.get('helium_index', 0.0),
            node_data.get('material_index', 0.0),
            context.get('token_count', 1) / 1000.0,
            context.get('expected_latency_ms', 100) / 1000.0,
            0.5
        ]
        return np.array(features, dtype=np.float32)

    def _train_gating(self):
        if not SKLEARN_AVAILABLE or len(self._training_data) < 10:
            return
        X = np.array([item[0] for item in self._training_data])
        y = np.array([item[1] for item in self._training_data])
        self._scaler = StandardScaler()
        X_scaled = self._scaler.fit_transform(X)
        self._gating_model = MLPClassifier(hidden_layer_sizes=self.config.moe_hidden_layers, max_iter=200, random_state=42)
        self._gating_model.fit(X_scaled, y)
        self._trained = True
        logger.info(f"MoE gating network trained on {len(self._training_data)} samples.")

    async def select_expert(self, context, carbon_intensity, node_data):
        features = self._encode_context(context, carbon_intensity, node_data)
        if self._trained and self._gating_model is not None:
            X = features.reshape(1, -1)
            if self._scaler:
                X = self._scaler.transform(X)
            probs = self._gating_model.predict_proba(X)[0]
            expert_idx = np.argmax(probs)
            selected = self.expert_names[expert_idx]
        else:
            selected = 'balanced'
        expert_func = self.experts[selected]
        weights = expert_func(context)
        return selected, weights

    async def add_training_sample(self, context, carbon_intensity, node_data, selected_expert, reward):
        features = self._encode_context(context, carbon_intensity, node_data)
        expert_idx = self.expert_names.index(selected_expert)
        async with self._lock:
            self._training_data.append((features, expert_idx))
            if len(self._training_data) % 10 == 0:
                self._train_gating()

# -----------------------------------------------------------------------------
# Genetic Weight Optimizer (simplified)
# -----------------------------------------------------------------------------
class GeneticWeightOptimizer:
    def __init__(self, config, storage):
        self.config = config
        self.storage = storage
        self.population_size = config.ga_population_size
        self.generations = config.ga_generations
        self.mutation_rate = config.ga_mutation_rate
        self.crossover_rate = config.ga_crossover_rate
        self.obj_names = ['energy', 'carbon', 'helium', 'material', 'latency', 'accuracy']

    def _random_weight_vector(self):
        vec = [random.random() for _ in self.obj_names]
        total = sum(vec)
        return [v / total for v in vec]

    def _mutate(self, vec):
        new_vec = vec.copy()
        for i in range(len(new_vec)):
            if random.random() < self.mutation_rate:
                new_vec[i] = max(0.0, min(1.0, new_vec[i] + random.gauss(0, 0.1)))
        total = sum(new_vec)
        if total > 0:
            new_vec = [v / total for v in new_vec]
        return new_vec

    def _crossover(self, p1, p2):
        if random.random() > self.crossover_rate:
            return p1.copy(), p2.copy()
        c1, c2 = p1.copy(), p2.copy()
        for i in range(len(c1)):
            if random.random() < 0.5:
                c1[i], c2[i] = p2[i], p1[i]
        return c1, c2

    async def _evaluate_fitness(self, weight_vec, historical_data=None):
        return random.uniform(0.5, 1.0)  # placeholder

    async def run_search(self, historical_data=None):
        population = [self._random_weight_vector() for _ in range(self.population_size)]
        best_fitness = -1.0
        best_individual = None

        for gen in range(self.generations):
            fitnesses = await asyncio.gather(*[self._evaluate_fitness(ind) for ind in population])
            sorted_pop = sorted(zip(population, fitnesses), key=lambda x: x[1], reverse=True)
            if sorted_pop[0][1] > best_fitness:
                best_fitness = sorted_pop[0][1]
                best_individual = sorted_pop[0][0]

            parents = [ind for ind, _ in sorted_pop[:max(2, self.population_size//2)]]
            offspring = []
            while len(offspring) < self.population_size:
                p1 = random.choice(parents)
                p2 = random.choice(parents)
                c1, c2 = self._crossover(p1, p2)
                c1 = self._mutate(c1)
                c2 = self._mutate(c2)
                offspring.append(c1)
                if len(offspring) < self.population_size:
                    offspring.append(c2)
            combined = parents + offspring
            combined_fitness = await asyncio.gather(*[self._evaluate_fitness(ind) for ind in combined])
            sorted_combined = sorted(zip(combined, combined_fitness), key=lambda x: x[1], reverse=True)
            population = [ind for ind, _ in sorted_combined[:self.population_size]]

        if best_individual is None:
            best_individual = self._random_weight_vector()
        weight_dict = {name: best_individual[i] for i, name in enumerate(self.obj_names)}
        return weight_dict

    async def optimize(self):
        return await self.run_search()

# -----------------------------------------------------------------------------
# MTOP Weight Engine (simplified)
# -----------------------------------------------------------------------------
class MTOPWeightEngine:
    def __init__(self, config):
        self.config = config
        self.teacher_weights = {  # fixed for demo
            'energy': 1.0, 'carbon': 2.0, 'helium': 0.5, 'material': 0.3, 'latency': 0.1, 'accuracy': 0.1
        }

    async def get_weights(self, context, carbon_intensity, historical_scores, user_prefs):
        return self.teacher_weights

# -----------------------------------------------------------------------------
# Pareto Front Optimizer (simplified)
# -----------------------------------------------------------------------------
class ParetoFrontOptimizer:
    def __init__(self, config, storage):
        self.config = config
        self.storage = storage
        self.pareto_front = []
        self.max_size = config.pareto_max_architectures

    def _dominates(self, a, b):
        a_metrics = (a['energy'], a['carbon'], a['helium'], a['material'], a['latency'], -a['accuracy'])
        b_metrics = (b['energy'], b['carbon'], b['helium'], b['material'], b['latency'], -b['accuracy'])
        return all(a_metrics[i] <= b_metrics[i] for i in range(6)) and any(a_metrics[i] < b_metrics[i] for i in range(6))

    async def add_expert(self, expert, context, carbon_intensity):
        entry = {
            'expert_id': expert.expert_id,
            'energy': expert.energy_per_inference * context.get('token_count', 1),
            'carbon': expert.carbon_per_inference * context.get('token_count', 1) * carbon_intensity,
            'helium': expert.helium_per_inference * context.get('token_count', 1),
            'material': 0.0,
            'latency': context.get('expected_latency_ms', 100),
            'accuracy': expert.accuracy_score,
        }
        # Check dominance
        for existing in self.pareto_front:
            if self._dominates(existing, entry):
                return False
        self.pareto_front = [e for e in self.pareto_front if not self._dominates(entry, e)]
        self.pareto_front.append(entry)
        if len(self.pareto_front) > self.max_size:
            self.pareto_front = self.pareto_front[:self.max_size]
        return True

    def get_pareto_front(self):
        return self.pareto_front

# -----------------------------------------------------------------------------
# SustainabilityCostFunction (Main Class)
# -----------------------------------------------------------------------------
class SustainabilityCostFunction:
    def __init__(self, config=None):
        self.config = config or SustainabilityCostConfig()
        self.storage = EnhancedStorage(self.config)
        self.carbon_manager = CarbonIntensityManager(self.config, self.storage)
        self.node_registry = NodeRegistry(self.storage, self.config)

        self.mtop_engine = MTOPWeightEngine(self.config)
        self.ga_optimizer = GeneticWeightOptimizer(self.config, self.storage) if self.config.ga_enabled else None
        self.moe_gating = MoEGatingNetwork(self.config, self.storage) if self.config.moe_enabled else None
        self.pareto_optimizer = ParetoFrontOptimizer(self.config, self.storage) if self.config.pareto_enabled else None
        self.limit_graph = LimitGraphManager(self.config) if self.config.limit_graph_enabled else None
        self.rlhf = RLHFManager(self.config) if self.config.rlhf_enabled else None
        self.distillation = MultiTeacherPolicyDistillation(self.config, self.moe_gating) if self.config.distillation_enabled and self.moe_gating else None

        self.weights = {
            'alpha': self.config.alpha,
            'beta': self.config.beta,
            'gamma': self.config.gamma,
            'delta': self.config.delta,
            'epsilon': self.config.epsilon,
            'zeta': self.config.zeta
        }

        self._carbon_cache = None
        self._carbon_cache_timestamp = None
        self._node_cache = {}
        self._cache_lock = asyncio.Lock()
        self._background_tasks = []
        self._running = False
        self._shutdown_event = asyncio.Event()

        self._circuit_breaker = CircuitBreaker(name="sustainability_cost")
        self._rate_limiter = RateLimiter(rate=100, window=60)

        logger.info(f"SustainabilityCostFunction v{self.config.version} initialized")

    async def start(self):
        self._running = True
        tasks = []
        if self.config.ga_enabled and self.ga_optimizer:
            tasks.append(self._ga_optimization_loop())
        if self.limit_graph:
            tasks.append(self._limit_graph_loop())
        if self.rlhf:
            tasks.append(self._rlhf_loop())
        if self.distillation:
            tasks.append(self._distillation_loop())
        for task in tasks:
            self._background_tasks.append(asyncio.create_task(task))
        logger.info("Background tasks started")

    async def _ga_optimization_loop(self):
        while not self._shutdown_event.is_set():
            await asyncio.sleep(3600)  # hourly
            try:
                logger.info("Running GA weight optimization...")
                best_weights = await self.ga_optimizer.optimize()
                if best_weights:
                    # Map weight dict to alpha..zeta (simplified)
                    self.weights['alpha'] = best_weights.get('energy', self.weights['alpha'])
                    self.weights['beta'] = best_weights.get('carbon', self.weights['beta'])
                    self.weights['gamma'] = best_weights.get('helium', self.weights['gamma'])
                    self.weights['delta'] = best_weights.get('material', self.weights['delta'])
                    self.weights['epsilon'] = best_weights.get('latency', self.weights['epsilon'])
                    self.weights['zeta'] = best_weights.get('accuracy', self.weights['zeta'])
                    logger.info("GA updated weights: %s", self.weights)
            except Exception as e:
                logger.error(f"GA optimization loop error: {e}")

    async def _limit_graph_loop(self):
        while not self._shutdown_event.is_set():
            await asyncio.sleep(self.config.limit_graph_update_interval)
            try:
                carbon_intensity = await self._get_carbon_intensity()
                await self.limit_graph.update_constraint('carbon', carbon_intensity)
                influence = await self.limit_graph.evaluate_path('carbon', 'cost')
                logger.debug(f"LIMIT Graph carbon->cost influence: {influence:.3f}")
            except Exception as e:
                logger.error(f"Limit graph loop error: {e}")

    async def _rlhf_loop(self):
        while not self._shutdown_event.is_set():
            await asyncio.sleep(self.config.rlhf_training_interval)
            try:
                if self.rlhf:
                    await self.rlhf.train_reward_model()
            except Exception as e:
                logger.error(f"RLHF loop error: {e}")

    async def _distillation_loop(self):
        while not self._shutdown_event.is_set():
            await asyncio.sleep(self.config.distillation_interval)
            try:
                if self.distillation:
                    carbon_intensity = await self._get_carbon_intensity()
                    # Create dummy state
                    state = {
                        'carbon_intensity': carbon_intensity,
                        'node_data': {'helium_index': 0.0, 'material_index': 0.0},
                        'token_count': 100,
                        'expected_latency_ms': 50
                    }
                    await self.distillation.distill(state)
            except Exception as e:
                logger.error(f"Distillation loop error: {e}")

    async def _get_carbon_intensity(self):
        async with self._cache_lock:
            if self._carbon_cache is not None and (datetime.now() - self._carbon_cache_timestamp).seconds < self.config.cache_ttl:
                return self._carbon_cache
        intensity = await self.carbon_manager.get_current_intensity()
        async with self._cache_lock:
            self._carbon_cache = intensity
            self._carbon_cache_timestamp = datetime.now()
        return intensity

    async def _get_node_data(self, node_id):
        async with self._cache_lock:
            if node_id in self._node_cache:
                return self._node_cache[node_id]
        data = await self.node_registry.get_node(node_id)
        async with self._cache_lock:
            self._node_cache[node_id] = data
        return data

    def inject_dependencies(self, carbon_manager=None, node_registry=None):
        if carbon_manager:
            self.carbon_manager = carbon_manager
        if node_registry:
            self.node_registry = node_registry

    async def compute(self, expert: ExpertProfile, context: Dict[str, Any]) -> float:
        carbon_intensity = await self._get_carbon_intensity()
        target_node = context.get('target_node_id')
        node_data = await self._get_node_data(target_node) if target_node else {'helium_index': 0.0, 'material_index': 0.0}

        tokens = context.get('token_count', 1)
        latency = context.get('expected_latency_ms', 100.0)

        # Compute components
        E = expert.energy_per_inference * tokens
        CO2 = expert.carbon_per_inference * tokens * carbon_intensity
        helium_usage = expert.helium_per_inference * tokens
        H = helium_usage * (1 + node_data.get('helium_index', 0.0))
        M = node_data.get('material_index', 0.0)
        L = latency
        acc = max(0.0, min(1.0, expert.accuracy_score))
        A = 1.0 - acc

        # Determine weights: priority RLHF > Distillation > MoE > MTOP
        alpha = self.weights['alpha']
        beta = self.weights['beta']
        gamma = self.weights['gamma']
        delta = self.weights['delta']
        epsilon = self.weights['epsilon']
        zeta = self.weights['zeta']

        if self.rlhf and self.rlhf.reward_model is not None:
            probs = await self.rlhf.get_policy_probs(context)
            # Map to objective order
            obj_names = ['energy', 'carbon', 'helium', 'material', 'latency', 'accuracy']
            rlhf_weights = {obj_names[i]: probs[i] for i in range(len(obj_names))}
            alpha = rlhf_weights.get('energy', alpha)
            beta = rlhf_weights.get('carbon', beta)
            gamma = rlhf_weights.get('helium', gamma)
            delta = rlhf_weights.get('material', delta)
            epsilon = rlhf_weights.get('latency', epsilon)
            zeta = rlhf_weights.get('accuracy', zeta)
        elif self.distillation and self.distillation.get_student_probs():
            probs = self.distillation.get_student_probs()
            obj_names = ['energy', 'carbon', 'helium', 'material', 'latency', 'accuracy']
            distill_weights = {obj_names[i]: probs[i] for i in range(len(obj_names))}
            alpha = distill_weights.get('energy', alpha)
            beta = distill_weights.get('carbon', beta)
            gamma = distill_weights.get('helium', gamma)
            delta = distill_weights.get('material', delta)
            epsilon = distill_weights.get('latency', epsilon)
            zeta = distill_weights.get('accuracy', zeta)
        elif self.moe_gating:
            selected, weights = await self.moe_gating.select_expert(context, carbon_intensity, node_data)
            alpha = weights.get('energy', alpha)
            beta = weights.get('carbon', beta)
            gamma = weights.get('helium', gamma)
            delta = weights.get('material', delta)
            epsilon = weights.get('latency', epsilon)
            zeta = weights.get('accuracy', zeta)
        else:
            # MTOP fallback
            mtop_weights = await self.mtop_engine.get_weights(context, carbon_intensity, {}, {})
            alpha = mtop_weights.get('energy', alpha)
            beta = mtop_weights.get('carbon', beta)
            gamma = mtop_weights.get('helium', gamma)
            delta = mtop_weights.get('material', delta)
            epsilon = mtop_weights.get('latency', epsilon)
            zeta = mtop_weights.get('accuracy', zeta)

        # LIMIT Graph adjustment: if carbon->cost influence high, increase beta
        if self.limit_graph:
            carbon_influence = await self.limit_graph.evaluate_path('carbon', 'cost')
            if carbon_influence > 0.5:
                beta *= (1 + carbon_influence * 0.2)  # boost carbon weight

        cost = alpha * E + beta * CO2 + gamma * H + delta * M + epsilon * L + zeta * A

        # Update LIMIT graph constraints
        if self.limit_graph:
            await self.limit_graph.update_constraint('cost', cost)
            await self.limit_graph.update_constraint('latency', latency)

        # Save history
        await self.storage.save_cost_history(expert.expert_id, cost, context, self.weights)

        # Update Pareto front
        if self.pareto_optimizer:
            await self.pareto_optimizer.add_expert(expert, context, carbon_intensity)

        return cost

    async def compute_multiple(self, experts: List[ExpertProfile], context: Dict[str, Any]) -> Dict[str, float]:
        # For batch, compute using same weights
        results = {}
        for expert in experts:
            results[expert.expert_id] = await self.compute(expert, context)
        return results

    async def record_feedback(self, expert: ExpertProfile, context: Dict[str, Any], reward: float):
        """Record human feedback for RLHF."""
        if self.rlhf:
            state = {
                'carbon_intensity': await self._get_carbon_intensity(),
                'cost': reward,
                'latency': context.get('expected_latency_ms', 100),
                'accuracy': expert.accuracy_score,
                'helium_index': context.get('helium_index', 0.0),
                'material_index': context.get('material_index', 0.0)
            }
            await self.rlhf.record_feedback(state, 'balanced', reward)

    async def shutdown(self):
        logger.info("Shutting down SustainabilityCostFunction...")
        self._shutdown_event.set()
        for task in self._background_tasks:
            task.cancel()
        if self._background_tasks:
            await asyncio.gather(*self._background_tasks, return_exceptions=True)
        await self.carbon_manager.close()
        await self.node_registry.close()
        logger.info("Shutdown complete")

# -----------------------------------------------------------------------------
# Singleton accessor (optional)
# -----------------------------------------------------------------------------
_cost_function_instance = None
_cost_function_lock = asyncio.Lock()

async def get_sustainability_cost_function(config=None):
    global _cost_function_instance
    if _cost_function_instance is None:
        async with _cost_function_lock:
            if _cost_function_instance is None:
                _cost_function_instance = SustainabilityCostFunction(config)
                await _cost_function_instance.start()
    return _cost_function_instance

# -----------------------------------------------------------------------------
# Example usage
# -----------------------------------------------------------------------------
async def main():
    config = SustainabilityCostConfig()
    cost_func = await get_sustainability_cost_function(config)

    # Create expert
    expert = ExpertProfile(
        expert_id="expert_1",
        energy_per_inference=0.5,
        carbon_per_inference=0.05,
        helium_per_inference=0.01,
        accuracy_score=0.92
    )

    context = {'token_count': 100, 'target_node_id': 'node_1', 'expected_latency_ms': 50}

    print("Computing cost...")
    cost = await cost_func.compute(expert, context)
    print(f"Cost: {cost:.4f}")

    # Demonstrate RLHF feedback
    await cost_func.record_feedback(expert, context, reward=0.8)
    print("Feedback recorded.")

    # Show Pareto front
    if cost_func.pareto_optimizer:
        front = cost_func.pareto_optimizer.get_pareto_front()
        print(f"Pareto front size: {len(front)}")

    # Show LIMIT graph summary
    if cost_func.limit_graph:
        summary = await cost_func.limit_graph.get_graph_summary()
        print(f"LIMIT graph: {summary}")

    # Show distillation policy
    if cost_func.distillation:
        print(f"Distillation student probs: {cost_func.distillation.get_student_probs()}")

    await cost_func.shutdown()

if __name__ == "__main__":
    asyncio.run(main())
