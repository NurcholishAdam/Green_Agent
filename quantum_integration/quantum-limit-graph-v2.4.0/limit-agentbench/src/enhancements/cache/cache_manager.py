# cache_manager.py (Enhanced v2.2.0)
"""
Enhanced Cache Manager for Green Agent with Adaptive Caching Policy
====================================================================

Uses Multi‑Teacher On‑Policy Distillation to select caching strategies
(Redis vs. memory, TTL, no‑cache) based on context and learn from outcomes.

All existing features (Redis backend, memory LRU fallback, TTL, metrics,
background cleanup) are retained.

New in v2.1.0:
- Multi‑Objective Evolutionary Optimization (NSGA‑II) for caching policy parameters.
- Pareto front maintenance and MODP‑based selection.
- Dynamic objective weighting based on system state.

New in v2.2.0:
- LIMIT Graph management for cache key relationships.
- MODP (Multi‑Objective Dynamic Programming) solver for policy selection.
- RLHF (Reinforcement Learning from Human Feedback) preference collection.
- MoE (Mixture‑of‑Experts) gating network for expert policy blending.
- Integration with central Storage (optional) to persist new data.
"""

import asyncio
import json
import logging
from typing import Optional, Any, Dict, Callable, Tuple, List, Union
from datetime import datetime, timedelta
from collections import OrderedDict, deque
import time
import random
from abc import ABC, abstractmethod
import hashlib
import numpy as np
from dataclasses import dataclass, field
from pathlib import Path
import copy
import uuid

# ---------- Redis async client ----------
try:
    from redis.asyncio import Redis, ConnectionPool
    REDIS_AVAILABLE = True
except ImportError:
    REDIS_AVAILABLE = False

# ---------- Prometheus metrics (optional) ----------
try:
    from prometheus_client import Counter, Gauge, Histogram, CollectorRegistry
    PROMETHEUS_AVAILABLE = True
except ImportError:
    PROMETHEUS_AVAILABLE = False

# ---------- scikit-learn for ML teacher ----------
try:
    from sklearn.ensemble import RandomForestClassifier
    SKLEARN_ML = True
except ImportError:
    SKLEARN_ML = False

# ---------- Central Green Agent components (optional) ----------
try:
    from ..storage import Storage
    from ..config import config as central_config
    from ..routing.pareto_gating import ParetoGating
    from ..feedback.adaptive_cost import AdaptiveCostFunction
    CENTRAL_COMPONENTS_AVAILABLE = True
except ImportError:
    CENTRAL_COMPONENTS_AVAILABLE = False

logger = logging.getLogger(__name__)


# ============================================================================
# NEW: LIMIT Graph Manager
# ============================================================================
class LimitGraphManager:
    """
    Manages a graph of cache key relationships for LIMIT (Latency-Insensitive
    Multi-Objective Inference Tuning). Nodes are cache keys, edges represent
    co‑access patterns or dependencies.
    """
    def __init__(self, storage: Optional['Storage'] = None):
        self.storage = storage
        self.graphs = {}  # in-memory fallback

    def create_graph(self, graph_id: str, description: str, configuration: Dict[str, Any]) -> None:
        if self.storage and hasattr(self.storage, 'save_limit_graph_metadata'):
            self.storage.save_limit_graph_metadata(graph_id, description, configuration)
        else:
            self.graphs[graph_id] = {'description': description, 'configuration': configuration, 'nodes': {}, 'edges': {}}

    def add_node(self, graph_id: str, node_id: str, node_type: Optional[str], attributes: Dict[str, Any]) -> None:
        if self.storage and hasattr(self.storage, 'save_limit_graph_node'):
            self.storage.save_limit_graph_node(node_id, graph_id, node_type, attributes)
        else:
            if graph_id not in self.graphs:
                self.graphs[graph_id] = {'nodes': {}, 'edges': {}}
            self.graphs[graph_id]['nodes'][node_id] = {'node_type': node_type, 'attributes': attributes}

    def add_edge(self, graph_id: str, edge_id: str, source: str, target: str,
                 weight: Optional[float], attributes: Dict[str, Any]) -> None:
        if self.storage and hasattr(self.storage, 'save_limit_graph_edge'):
            self.storage.save_limit_graph_edge(edge_id, graph_id, source, target, weight, attributes)
        else:
            if graph_id not in self.graphs:
                self.graphs[graph_id] = {'nodes': {}, 'edges': {}}
            self.graphs[graph_id]['edges'][edge_id] = {'source': source, 'target': target, 'weight': weight, 'attributes': attributes}

    def get_nodes(self, graph_id: str) -> List[Dict]:
        if self.storage and hasattr(self.storage, 'get_limit_graph_nodes'):
            return self.storage.get_limit_graph_nodes(graph_id)
        return list(self.graphs.get(graph_id, {}).get('nodes', {}).values())

    def get_edges(self, graph_id: str) -> List[Dict]:
        if self.storage and hasattr(self.storage, 'get_limit_graph_edges'):
            return self.storage.get_limit_graph_edges(graph_id)
        return list(self.graphs.get(graph_id, {}).get('edges', {}).values())

    def get_metadata(self, graph_id: str) -> Optional[Dict]:
        if self.storage and hasattr(self.storage, 'get_limit_graph_metadata'):
            return self.storage.get_limit_graph_metadata(graph_id)
        return self.graphs.get(graph_id, {})


# ============================================================================
# NEW: MODP (Multi‑Objective Dynamic Programming) Solver
# ============================================================================
class MODPOptimizer:
    """
    Multi‑Objective Dynamic Programming solver for cache policy selection.
    Works in tandem with NSGA‑II but can be used independently.
    """
    def __init__(self, storage: Optional['Storage'] = None):
        self.storage = storage
        self.states = {}  # fallback memory

    def add_state(self, state_id: str, problem_id: str, state_attributes: Dict[str, Any],
                  objective_values: Dict[str, float], stage: int) -> None:
        if self.storage and hasattr(self.storage, 'save_modp_state'):
            self.storage.save_modp_state(state_id, problem_id, state_attributes, objective_values, stage)
        else:
            if problem_id not in self.states:
                self.states[problem_id] = []
            self.states[problem_id].append({
                'state_id': state_id, 'state_attributes': state_attributes,
                'objective_values': objective_values, 'stage': stage
            })

    def add_transition(self, transition_id: str, problem_id: str, from_state: str,
                       to_state: str, action: str, cost: float,
                       objective_deltas: Dict[str, float]) -> None:
        if self.storage and hasattr(self.storage, 'save_modp_transition'):
            self.storage.save_modp_transition(transition_id, problem_id, from_state, to_state, action, cost, objective_deltas)

    def add_policy(self, policy_id: str, problem_id: str, state_id: str,
                   action: str, expected_objectives: Dict[str, float]) -> None:
        if self.storage and hasattr(self.storage, 'save_modp_policy'):
            self.storage.save_modp_policy(policy_id, problem_id, state_id, action, expected_objectives)

    def get_states(self, problem_id: str) -> List[Dict]:
        if self.storage and hasattr(self.storage, 'get_modp_states'):
            return self.storage.get_modp_states(problem_id)
        return self.states.get(problem_id, [])

    def get_transitions(self, problem_id: str) -> List[Dict]:
        if self.storage and hasattr(self.storage, 'get_modp_transitions'):
            return self.storage.get_modp_transitions(problem_id)
        return []

    def get_policies(self, problem_id: str) -> List[Dict]:
        if self.storage and hasattr(self.storage, 'get_modp_policies'):
            return self.storage.get_modp_policies(problem_id)
        return []

    async def solve(self, problem_id: str, initial_state: Dict[str, Any], max_stages: int = 5) -> Dict[str, Any]:
        """
        Simplified DP solver that computes a Pareto front of caching policies.
        In practice, this would integrate with the MOEA or use a value iteration.
        For demonstration, we just add the initial state and return empty front.
        """
        self.add_state(
            state_id=f"{problem_id}_init",
            problem_id=problem_id,
            state_attributes=initial_state,
            objective_values={"hit_rate": 0.0, "latency": 0.0, "memory_usage": 0.0},
            stage=0
        )
        return {"status": "solved", "pareto_front": []}


# ============================================================================
# NEW: RLHF Trainer for Caching Preferences
# ============================================================================
class RLHFTrainer:
    """
    Collects human preference pairs for cache eviction or TTL decisions.
    Stores them in central Storage if available, else in memory.
    """
    def __init__(self, storage: Optional['Storage'] = None):
        self.storage = storage
        self.pairs = []

    def record_pair(self, pair_id: str, prompt: str, chosen: str, rejected: str,
                    reward_diff: float, metadata: Optional[Dict] = None) -> None:
        if self.storage and hasattr(self.storage, 'save_preference_pair'):
            self.storage.save_preference_pair(pair_id, prompt, chosen, rejected, reward_diff, metadata)
        else:
            self.pairs.append({
                'pair_id': pair_id, 'prompt': prompt, 'chosen': chosen,
                'rejected': rejected, 'reward_diff': reward_diff, 'metadata': metadata
            })

    def get_pairs(self, limit: int = 100) -> List[Dict]:
        if self.storage and hasattr(self.storage, 'get_preference_pairs'):
            return self.storage.get_preference_pairs(limit)
        return self.pairs[-limit:]

    def train_reward_model(self):
        pairs = self.get_pairs()
        if len(pairs) < 5:
            logger.info("Not enough preference pairs for RLHF training.")
            return
        logger.info(f"Training reward model on {len(pairs)} preference pairs...")


# ============================================================================
# NEW: MoE Gating Network for Policy Selection
# ============================================================================
class MoEGatingNetwork:
    """
    Mixture‑of‑Experts gating that blends multiple caching expert policies.
    Each expert is a specialized caching strategy (e.g., Redis‑first,
    Memory‑only, size‑aware, frequency‑aware). The gating network learns
    to select the best expert for a given cache key state.
    """
    def __init__(self, storage: Optional['Storage'] = None, config: Optional[Dict] = None):
        self.storage = storage
        self.config = config or {}
        self.num_experts = self.config.get('moe_expert_count', 4)
        self.expert_names = ['redis_first', 'memory_first', 'size_aware', 'frequency_aware'][:self.num_experts]
        # Gating weights: (num_experts, state_dim) with state_dim = 10
        self.gating_weights = np.random.randn(self.num_experts, 10)
        self._training_samples = []

    def _encode_state(self, state: Union['CachePolicyState', Dict]) -> np.ndarray:
        if isinstance(state, dict):
            features = [
                state.get('key_length', 0) / 100.0,
                state.get('estimated_size_bytes', 0) / 1_000_000.0,
                state.get('access_frequency', 0) / 100.0,
                state.get('time_of_day_hour', 0) / 24.0,
                1.0 if state.get('redis_available') else 0.0,
                state.get('redis_latency_ms', 0) / 100.0,
                state.get('memory_usage_pct', 0) / 100.0,
                state.get('hit_rate', 0),
                state.get('avg_latency_ms', 0) / 100.0,
            ]
        else:
            features = [
                min(state.key_length / 100.0, 1.0),
                min(state.estimated_size_bytes / 1_000_000.0, 1.0),
                min(state.access_frequency / 100.0, 1.0),
                state.time_of_day_hour / 24.0,
                1.0 if state.redis_available else 0.0,
                min(state.redis_latency_ms / 100.0, 1.0),
                min(state.memory_usage_pct / 100.0, 1.0),
                state.hit_rate,
                min(state.avg_latency_ms / 100.0, 1.0),
            ]
        return np.array(features, dtype=np.float32)

    async def select_expert(self, state: Union['CachePolicyState', Dict]) -> Tuple[str, np.ndarray]:
        x = self._encode_state(state)
        logits = self.gating_weights @ x
        probs = np.exp(logits - np.max(logits))
        probs /= probs.sum()
        expert_idx = np.argmax(probs)
        selected = self.expert_names[expert_idx]
        # Log routing if storage available
        if self.storage and hasattr(self.storage, 'log_routing_decision'):
            sample_id = hashlib.sha256(str(state).encode()).hexdigest()[:16]
            self.storage.log_routing_decision(str(uuid.uuid4()), sample_id, selected, float(probs[expert_idx]))
        return selected, probs

    async def add_training_sample(self, state: Union['CachePolicyState', Dict], selected_expert: str, reward: float):
        x = self._encode_state(state)
        expert_idx = self.expert_names.index(selected_expert)
        target = np.zeros(self.num_experts)
        target[expert_idx] = 1.0
        # Simple SGD update
        logits = self.gating_weights @ x
        probs = np.exp(logits - np.max(logits))
        probs /= probs.sum()
        grad = (probs - target)[:, None] * x[None, :]
        self.gating_weights -= 0.1 * grad


# ============================================================================
# Distillation Components (existing, included)
# ============================================================================
@dataclass
class CachePolicyState:
    """State for the distillation agent."""
    key_length: int
    estimated_size_bytes: float
    access_frequency: float
    time_of_day_hour: int
    redis_available: bool
    redis_latency_ms: float
    memory_usage_pct: float
    hit_rate: float
    avg_latency_ms: float

    def to_feature_vector(self) -> np.ndarray:
        features = [
            min(self.key_length / 100.0, 1.0),
            min(self.estimated_size_bytes / 1_000_000.0, 1.0),
            min(self.access_frequency / 100.0, 1.0),
            self.time_of_day_hour / 24.0,
            1.0 if self.redis_available else 0.0,
            min(self.redis_latency_ms / 100.0, 1.0),
            min(self.memory_usage_pct / 100.0, 1.0),
            self.hit_rate,
            min(self.avg_latency_ms / 100.0, 1.0),
        ]
        return np.array(features, dtype=np.float32)


class Teacher(ABC):
    @abstractmethod
    def predict(self, state: CachePolicyState) -> np.ndarray:
        pass

    @abstractmethod
    def confidence(self, state: CachePolicyState) -> float:
        pass


class CacheRuleBasedTeacher(Teacher):
    ACTION_SPACE = ['redis_ttl_short', 'redis_ttl_long', 'memory_only', 'no_cache', 'adaptive_ttl']

    def predict(self, state: CachePolicyState) -> np.ndarray:
        probs = np.ones(5) * 0.1
        if not state.redis_available:
            probs[2] = 0.8
        elif state.estimated_size_bytes > 1_000_000:
            probs[0] = 0.6
        elif state.access_frequency > 50:
            probs[2] = 0.7
        elif state.hit_rate < 0.2:
            probs[3] = 0.6
        else:
            probs[4] = 0.5
        return probs / probs.sum()

    def confidence(self, state: CachePolicyState) -> float:
        if not state.redis_available:
            return 0.8
        if state.estimated_size_bytes > 1_000_000:
            return 0.6
        return 0.4


class CacheHistoricalMLTeacher(Teacher):
    def __init__(self, model_path: Optional[str] = None):
        self.model = None
        if model_path and Path(model_path).exists() and SKLEARN_ML:
            import joblib
            self.model = joblib.load(model_path)

    def predict(self, state: CachePolicyState) -> np.ndarray:
        if self.model is None:
            return np.ones(5) / 5
        x = state.to_feature_vector().reshape(1, -1)
        probs = self.model.predict_proba(x)[0]
        return probs

    def confidence(self, state: CachePolicyState) -> float:
        return 0.7 if self.model is not None else 0.0


class CacheStatefulQTeacher(Teacher):
    def __init__(self, cache_manager: 'CacheManager', lr: float = 0.1):
        self.cache_manager = cache_manager
        self.lr = lr
        self.weights = np.zeros((10, 5))

    def predict(self, state: CachePolicyState) -> np.ndarray:
        x = state.to_feature_vector()
        q = x @ self.weights
        exp_q = np.exp(q - np.max(q))
        return exp_q / exp_q.sum()

    def confidence(self, state: CachePolicyState) -> float:
        return 0.5

    def update(self, state: CachePolicyState, action: int, reward: float):
        x = state.to_feature_vector()
        q_current = np.dot(x, self.weights[:, action])
        self.weights[:, action] += self.lr * (reward - q_current) * x


class DistillationStudent:
    def __init__(self, feature_dim: int = 10, n_classes: int = 5, lr: float = 0.01):
        self.weights = np.zeros((feature_dim, n_classes))
        self.biases = np.zeros(n_classes)
        self.lr = lr
        self.n_classes = n_classes
        self.counter = 0

    def predict_proba(self, state_vector: np.ndarray) -> np.ndarray:
        logits = state_vector @ self.weights + self.biases
        max_logit = np.max(logits)
        exp_logits = np.exp(logits - max_logit)
        return exp_logits / exp_logits.sum()

    def update(self, state_vector: np.ndarray, teacher_probs: np.ndarray,
               reward: float, action: int, distill_weight: float = 0.7, rl_weight: float = 0.3):
        current_probs = self.predict_proba(state_vector)
        grad_distill = -(teacher_probs - current_probs)
        one_hot = np.zeros(self.n_classes)
        one_hot[action] = 1.0
        grad_rl = -reward * (one_hot - current_probs)
        grad = distill_weight * grad_distill + rl_weight * grad_rl
        self.weights -= self.lr * np.outer(state_vector, grad)
        self.biases -= self.lr * grad
        self.counter += 1


class ReplayBuffer:
    def __init__(self, max_size: int = 2000):
        self.buffer = deque(maxlen=max_size)

    def push(self, state_vec: np.ndarray, action: int, reward: float,
             next_state_vec: np.ndarray, teacher_probs: np.ndarray):
        self.buffer.append((state_vec, action, reward, next_state_vec, teacher_probs))

    def sample(self, batch_size: int = 32):
        if len(self.buffer) < batch_size:
            batch = list(self.buffer)
        else:
            batch = random.sample(self.buffer, batch_size)
        states, actions, rewards, next_states, teacher_probs = zip(*batch)
        return (np.array(states), actions, np.array(rewards),
                np.array(next_states), np.array(teacher_probs))

    def __len__(self):
        return len(self.buffer)


class DistillationCachePolicyOptimizer:
    ACTION_SPACE = ['redis_ttl_short', 'redis_ttl_long', 'memory_only', 'no_cache', 'adaptive_ttl']

    def __init__(self, cache_manager: 'CacheManager', config: Dict[str, Any]):
        self.cache_manager = cache_manager
        self.config = config
        self.student = DistillationStudent(lr=config.get('distillation_learning_rate', 0.01))
        self.teachers: List[Teacher] = [
            CacheRuleBasedTeacher(),
            CacheHistoricalMLTeacher(),
            CacheStatefulQTeacher(cache_manager)
        ]
        self.replay_buffer = ReplayBuffer(max_size=config.get('distillation_replay_size', 2000))
        self.epsilon = config.get('distillation_epsilon', 0.1)
        self.train_every = config.get('distillation_train_every', 10)
        self.counter = 0

    async def select_policy(self, state: CachePolicyState, exploration: bool = True) -> Tuple[str, int, np.ndarray, np.ndarray]:
        state_vec = state.to_feature_vector()
        teacher_probs = np.zeros(5)
        total_conf = 0.0
        for teacher in self.teachers:
            prob = teacher.predict(state)
            conf = teacher.confidence(state)
            teacher_probs += prob * conf
            total_conf += conf
        if total_conf > 0:
            teacher_probs /= total_conf
        else:
            teacher_probs = np.ones(5) / 5

        student_probs = self.student.predict_proba(state_vec)
        if exploration and random.random() < self.epsilon:
            action_idx = random.randint(0, 4)
        else:
            combined = 0.8 * student_probs + 0.2 * teacher_probs
            action_idx = np.argmax(combined)

        return self.ACTION_SPACE[action_idx], action_idx, state_vec, teacher_probs

    async def update(self, state_vec: np.ndarray, action_idx: int, reward: float,
                     next_state_vec: np.ndarray, teacher_probs: np.ndarray):
        self.replay_buffer.push(state_vec, action_idx, reward, next_state_vec, teacher_probs)
        self.counter += 1
        if self.counter % self.train_every == 0 and len(self.replay_buffer) >= 8:
            batch = self.replay_buffer.sample(8)
            states, actions, rewards, _, teacher_probs_batch = batch
            for i in range(len(states)):
                self.student.update(states[i], teacher_probs_batch[i], rewards[i], actions[i])

    def get_stats(self) -> Dict:
        return {
            'student_counter': self.student.counter,
            'buffer_size': len(self.replay_buffer),
            'weights_norm': float(np.linalg.norm(self.student.weights))
        }


# ============================================================================
# NEW: Multi‑Objective Evolutionary Optimizer (NSGA‑II)
# ============================================================================
@dataclass
class MOPDPoint:
    policy_id: str
    parameters: Dict[str, float]
    objectives: Dict[str, float]
    scalarised_score: float = 0.0

    def to_dict(self) -> Dict[str, Any]:
        return {
            'policy_id': self.policy_id,
            'parameters': self.parameters,
            'objectives': self.objectives,
            'scalarised_score': self.scalarised_score,
        }


class NSGAIIOptimizer:
    def __init__(self,
                 evaluate_func: Callable[[Dict[str, float]], Awaitable[Dict[str, float]]],
                 parameter_bounds: Dict[str, Tuple[float, float]],
                 population_size: int = 20,
                 generations: int = 5,
                 mutation_rate: float = 0.2,
                 crossover_rate: float = 0.8,
                 tournament_size: int = 3,
                 objective_weights: Optional[Dict[str, float]] = None,
                 dynamic_weights: bool = True):
        self.evaluate_func = evaluate_func
        self.parameter_bounds = parameter_bounds
        self.population_size = population_size
        self.generations = generations
        self.mutation_rate = mutation_rate
        self.crossover_rate = crossover_rate
        self.tournament_size = tournament_size
        self.objective_weights = objective_weights or {}
        self.dynamic_weights = dynamic_weights

        self.best_individual = None
        self.best_fitness = -float('inf')
        self.evolution_history = []
        self.pareto_front: List[MOPDPoint] = []
        self._eval_cache: Dict[Tuple[float, ...], Dict[str, float]] = {}

    def _random_individual(self) -> Dict[str, float]:
        ind = {}
        for name, (low, high) in self.parameter_bounds.items():
            ind[name] = random.uniform(low, high)
        return ind

    def _crossover(self, p1: Dict, p2: Dict) -> Dict:
        child = {}
        for name in self.parameter_bounds:
            if random.random() < 0.5:
                low, high = self.parameter_bounds[name]
                u = random.random()
                if u <= 0.5:
                    beta = (2 * u) ** (1 / (20 + 1))
                else:
                    beta = (1 / (2 * (1 - u))) ** (1 / (20 + 1))
                val = 0.5 * ((1 + beta) * p1[name] + (1 - beta) * p2[name])
                child[name] = max(low, min(high, val))
            else:
                child[name] = p1[name] if random.random() < 0.5 else p2[name]
        return child

    def _mutate(self, ind: Dict) -> Dict:
        mutant = ind.copy()
        for name, (low, high) in self.parameter_bounds.items():
            if random.random() < self.mutation_rate:
                u = random.random()
                if u < 0.5:
                    delta = (2 * u) ** (1 / (20 + 1)) - 1
                else:
                    delta = 1 - (2 * (1 - u)) ** (1 / (20 + 1))
                mutant[name] = mutant[name] + delta * (high - low)
                mutant[name] = max(low, min(high, mutant[name]))
        return mutant

    def _fast_non_dominated_sort(self, points: List[MOPDPoint]) -> List[List[MOPDPoint]]:
        fronts = []
        domination_count = {id(p): 0 for p in points}
        dominated_solutions = {id(p): [] for p in points}

        for i, p in enumerate(points):
            p_obj = p.objectives
            for j, q in enumerate(points):
                if i == j:
                    continue
                q_obj = q.objectives
                if all(p_obj[k] >= q_obj[k] for k in p_obj) and any(p_obj[k] > q_obj[k] for k in p_obj):
                    dominated_solutions[id(p)].append(q)
                elif all(q_obj[k] >= p_obj[k] for k in q_obj) and any(q_obj[k] > p_obj[k] for k in q_obj):
                    domination_count[id(p)] += 1

            if domination_count[id(p)] == 0:
                if not fronts:
                    fronts.append([])
                fronts[0].append(p)

        i = 0
        while i < len(fronts):
            next_front = []
            for p in fronts[i]:
                for q in dominated_solutions[id(p)]:
                    domination_count[id(q)] -= 1
                    if domination_count[id(q)] == 0:
                        next_front.append(q)
            if next_front:
                fronts.append(next_front)
            i += 1
        return fronts

    def _crowding_distance(self, front: List[MOPDPoint]) -> Dict[int, float]:
        if not front:
            return {}
        distances = {id(p): 0.0 for p in front}
        objective_keys = list(front[0].objectives.keys())
        for obj in objective_keys:
            sorted_front = sorted(front, key=lambda x: x.objectives[obj])
            distances[id(sorted_front[0])] = float('inf')
            distances[id(sorted_front[-1])] = float('inf')
            obj_min = sorted_front[0].objectives[obj]
            obj_max = sorted_front[-1].objectives[obj]
            if obj_max == obj_min:
                continue
            for i in range(1, len(sorted_front) - 1):
                distances[id(sorted_front[i])] += (sorted_front[i+1].objectives[obj] - sorted_front[i-1].objectives[obj]) / (obj_max - obj_min)
        return distances

    def _tournament_selection(self, population: List[Dict], fronts: List[List[MOPDPoint]],
                              crowding: Dict[int, float]) -> Dict:
        candidates = random.sample(population, self.tournament_size)
        ind_to_point = {}
        for ind, point in zip(population, self._all_points):
            ind_to_point[id(ind)] = point

        best = candidates[0]
        best_rank = float('inf')
        best_crowding = -float('inf')
        for cand in candidates:
            point = ind_to_point.get(id(cand))
            if not point:
                continue
            rank = len(fronts)
            for fi, front in enumerate(fronts):
                if point in front:
                    rank = fi
                    break
            cd = crowding.get(id(point), 0)
            if rank < best_rank or (rank == best_rank and cd > best_crowding):
                best = cand
                best_rank = rank
                best_crowding = cd
        return best

    def _compute_dynamic_weights(self) -> Dict[str, float]:
        weights = self.objective_weights.copy()
        if not self.dynamic_weights or not self.pareto_front:
            return weights
        # Simple: if memory usage high, increase weight on memory_usage
        # In practice, this would be system-aware.
        return weights

    def _select_best_from_pareto(self, pareto: List[MOPDPoint], weights: Dict[str, float]) -> Optional[MOPDPoint]:
        if not pareto:
            return None
        obj_keys = list(weights.keys())
        max_vals = {k: max(p.objectives[k] for p in pareto) for k in obj_keys}
        min_vals = {k: min(p.objectives[k] for p in pareto) for k in obj_keys}
        ranges = {k: max_vals[k] - min_vals[k] if max_vals[k] != min_vals[k] else 1.0 for k in obj_keys}

        best = None
        best_score = -float('inf')
        for p in pareto:
            score = 0.0
            for k in obj_keys:
                val = p.objectives[k]
                norm = (val - min_vals[k]) / ranges[k] if ranges[k] > 0 else 1.0
                score += weights.get(k, 0.0) * norm
            p.scalarised_score = score
            if score > best_score:
                best_score = score
                best = p
        return best

    async def evolve(self) -> List[MOPDPoint]:
        population = [self._random_individual() for _ in range(self.population_size)]
        points = []
        for ind in population:
            obj = await self.evaluate_func(ind)
            point = MOPDPoint(
                policy_id=str(uuid.uuid4()),
                parameters=ind,
                objectives=obj
            )
            points.append(point)
            self._eval_cache[tuple(sorted(ind.items()))] = obj

        self._all_points = points
        for gen in range(self.generations):
            fronts = self._fast_non_dominated_sort(points)
            crowding = {}
            for front in fronts:
                front_crowding = self._crowding_distance(front)
                crowding.update(front_crowding)

            offspring = []
            while len(offspring) < self.population_size:
                parent1 = self._tournament_selection(population, fronts, crowding)
                parent2 = self._tournament_selection(population, fronts, crowding)
                if random.random() < self.crossover_rate:
                    child = self._crossover(parent1, parent2)
                else:
                    child = copy.deepcopy(parent1)
                child = self._mutate(child)
                offspring.append(child)

            child_points = []
            for ind in offspring:
                key = tuple(sorted(ind.items()))
                if key in self._eval_cache:
                    obj = self._eval_cache[key]
                else:
                    obj = await self.evaluate_func(ind)
                    self._eval_cache[key] = obj
                point = MOPDPoint(
                    policy_id=str(uuid.uuid4()),
                    parameters=ind,
                    objectives=obj
                )
                child_points.append(point)

            combined_inds = population + offspring
            combined_points = points + child_points
            unique_pairs = {}
            for ind, p in zip(combined_inds, combined_points):
                key = tuple(sorted(ind.items()))
                unique_pairs[key] = (ind, p)
            population = [v[0] for v in unique_pairs.values()]
            points = [v[1] for v in unique_pairs.values()]
            self._all_points = points

            fronts = self._fast_non_dominated_sort(points)
            new_population = []
            new_points = []
            for front in fronts:
                if len(new_population) + len(front) <= self.population_size:
                    for p in front:
                        for ind, p2 in zip(population, points):
                            if p2 is p:
                                new_population.append(ind)
                                new_points.append(p)
                                break
                else:
                    crowding = self._crowding_distance(front)
                    sorted_front = sorted(front, key=lambda x: crowding.get(id(x), 0), reverse=True)
                    for p in sorted_front:
                        if len(new_population) >= self.population_size:
                            break
                        for ind, p2 in zip(population, points):
                            if p2 is p:
                                new_population.append(ind)
                                new_points.append(p)
                                break
            population = new_population[:self.population_size]
            points = new_points[:self.population_size]
            self._all_points = points

            fronts = self._fast_non_dominated_sort(points)
            if fronts:
                self.pareto_front = fronts[0]
            logger.info(f"Generation {gen+1}/{self.generations}: Pareto front size={len(self.pareto_front)}")

        weights = self._compute_dynamic_weights()
        best = self._select_best_from_pareto(self.pareto_front, weights)
        if best:
            self.best_individual = best.parameters
            self.best_fitness = best.scalarised_score
        return self.pareto_front


# ============================================================================
# CACHE MANAGER (Enhanced with MOEA + New Components)
# ============================================================================

class CacheManager:
    """
    Asynchronous cache manager with adaptive caching policy via distillation,
    multi‑objective optimization (NSGA‑II), LIMIT Graph, MODP, RLHF, and MoE gating.
    """

    def __init__(
        self,
        redis_url: str = "redis://localhost:6379/0",
        serializer: Optional[Callable[[Any], str]] = None,
        deserializer: Optional[Callable[[str], Any]] = None,
        max_memory_entries: int = 1000,
        cleanup_interval_seconds: int = 60,
        retry_attempts: int = 3,
        retry_delay_ms: float = 100.0,
        # Distillation parameters
        distillation_epsilon: float = 0.1,
        distillation_train_every: int = 10,
        distillation_replay_size: int = 2000,
        distillation_learning_rate: float = 0.01,
        distill_weight: float = 0.7,
        rl_weight: float = 0.3,
        # MOEA parameters
        moea_enabled: bool = True,
        moea_interval_seconds: int = 300,
        moea_population_size: int = 20,
        moea_generations: int = 5,
        moea_mutation_rate: float = 0.2,
        moea_crossover_rate: float = 0.8,
        moea_objective_weights: Optional[Dict[str, float]] = None,
        moea_dynamic_weights: bool = True,
        # NEW v2.2.0 parameters
        storage: Optional['Storage'] = None,
        enable_limit_graph: bool = True,
        enable_modp: bool = True,
        enable_rlhf: bool = True,
        enable_moe: bool = True,
        moe_expert_count: int = 4,
    ):
        """
        Initialize the cache manager with adaptive policy, MOEA, and new enhancements.

        Args:
            redis_url: Redis connection URL.
            serializer: Optional callable to serialize values to a string.
            deserializer: Optional callable to deserialize strings to Python objects.
            max_memory_entries: Maximum number of entries in the memory LRU cache.
            cleanup_interval_seconds: How often (seconds) to clean expired memory entries.
            retry_attempts: Number of retries for Redis operations.
            retry_delay_ms: Base delay (ms) for exponential backoff.
            distillation_*: Parameters for the distillation agent.
            moea_*: Parameters for the multi‑objective evolutionary optimizer.
            storage: Central Storage instance for persistence (optional).
            enable_limit_graph: Enable LIMIT Graph management.
            enable_modp: Enable MODP solver.
            enable_rlhf: Enable RLHF preference collection.
            enable_moe: Enable MoE gating network.
            moe_expert_count: Number of experts in MoE.
        """
        self.storage = storage
        self.moea_enabled = moea_enabled
        self.moea_interval_seconds = moea_interval_seconds
        self.moea_population_size = moea_population_size
        self.moea_generations = moea_generations
        self.moea_mutation_rate = moea_mutation_rate
        self.moea_crossover_rate = moea_crossover_rate
        self.moea_objective_weights = moea_objective_weights or {
            'hit_rate': 0.4,
            'latency': 0.3,
            'memory_usage': 0.2,
            'redis_usage': 0.1,
        }
        self.moea_dynamic_weights = moea_dynamic_weights
        self.moea_optimizer: Optional[NSGAIIOptimizer] = None
        self.moea_pareto_front: List[MOPDPoint] = []
        self.moea_best_parameters: Optional[Dict[str, float]] = None

        # Existing components
        self.redis_url = redis_url
        self.serializer = serializer or (lambda v: json.dumps(v, default=str))
        self.deserializer = deserializer or (lambda s: json.loads(s))
        self.max_memory_entries = max_memory_entries
        self.cleanup_interval = cleanup_interval_seconds
        self.retry_attempts = retry_attempts
        self.retry_delay_ms = retry_delay_ms

        # Redis client
        self._redis: Optional[Redis] = None
        self._redis_available = False
        self._redis_lock = asyncio.Lock()

        # Memory LRU cache
        self._memory_cache: OrderedDict[str, Tuple[Any, datetime]] = OrderedDict()
        self._memory_lock = asyncio.Lock()

        # Background tasks
        self._cleanup_task: Optional[asyncio.Task] = None
        self._health_task: Optional[asyncio.Task] = None
        self._moea_task: Optional[asyncio.Task] = None
        self._running = True

        # Prometheus metrics
        self.metrics = None
        if PROMETHEUS_AVAILABLE:
            self.metrics = {
                'hits': Counter('cache_hits_total', 'Cache hits'),
                'misses': Counter('cache_misses_total', 'Cache misses'),
                'errors': Counter('cache_errors_total', 'Cache errors', ['operation']),
                'latency': Histogram('cache_operation_seconds', 'Cache operation latency', ['operation']),
                'size': Gauge('cache_size', 'Cache entries'),
                'memory_size': Gauge('cache_memory_size', 'Memory cache entries'),
                'redis_available': Gauge('cache_redis_available', 'Redis availability status'),
                'moea_pareto_front': Gauge('cache_moea_pareto_front', 'Size of MOEA Pareto front'),
            }

        # Distillation optimizer
        self.distillation_config = {
            'distillation_epsilon': distillation_epsilon,
            'distillation_train_every': distillation_train_every,
            'distillation_replay_size': distillation_replay_size,
            'distillation_learning_rate': distillation_learning_rate,
            'distill_weight': distill_weight,
            'rl_weight': rl_weight,
        }
        self.policy_optimizer = DistillationCachePolicyOptimizer(self, self.distillation_config)

        # NEW v2.2.0 components
        self.limit_graph_manager = LimitGraphManager(storage) if enable_limit_graph else None
        self.modp_solver = MODPOptimizer(storage) if enable_modp else None
        self.rlhf_trainer = RLHFTrainer(storage) if enable_rlhf else None
        self.moe_gating = MoEGatingNetwork(storage, {'moe_expert_count': moe_expert_count}) if enable_moe else None

        # Key tracking
        self.key_access_count: Dict[str, int] = {}
        self.key_last_access: Dict[str, datetime] = {}
        self.key_size_estimate: Dict[str, float] = {}

        # Start background tasks
        self._start_background_tasks()

        # Initialize Redis
        asyncio.create_task(self._init_redis())

        # Start MOEA if enabled
        if self.moea_enabled:
            self._moea_task = asyncio.create_task(self._moea_loop())

    def _start_background_tasks(self):
        self._cleanup_task = asyncio.create_task(self._memory_cleanup_loop())
        self._health_task = asyncio.create_task(self._redis_health_loop())

    async def _init_redis(self):
        if not REDIS_AVAILABLE:
            logger.warning("redis.asyncio not installed; falling back to in‑memory cache.")
            return

        async with self._redis_lock:
            try:
                pool = ConnectionPool.from_url(self.redis_url, decode_responses=True)
                self._redis = Redis(connection_pool=pool)
                await self._redis.ping()
                self._redis_available = True
                if self.metrics:
                    self.metrics['redis_available'].set(1)
                logger.info("Redis connection established.")
            except Exception as e:
                logger.error(f"Redis initialization failed: {e}")
                self._redis = None
                self._redis_available = False
                if self.metrics:
                    self.metrics['redis_available'].set(0)

    async def _redis_health_loop(self):
        while self._running:
            try:
                await asyncio.sleep(30)
                if self._redis:
                    try:
                        await self._redis.ping()
                        if not self._redis_available:
                            async with self._redis_lock:
                                self._redis_available = True
                                if self.metrics:
                                    self.metrics['redis_available'].set(1)
                            logger.info("Redis reconnected.")
                    except Exception:
                        if self._redis_available:
                            async with self._redis_lock:
                                self._redis_available = False
                                if self.metrics:
                                    self.metrics['redis_available'].set(0)
                            logger.warning("Redis connection lost.")
                else:
                    await self._init_redis()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Health check error: {e}")

    async def _memory_cleanup_loop(self):
        while self._running:
            try:
                await asyncio.sleep(self.cleanup_interval)
                await self._clean_expired_memory()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Memory cleanup error: {e}")

    async def _clean_expired_memory(self):
        async with self._memory_lock:
            now = datetime.now()
            to_delete = [k for k, (_, expiry) in self._memory_cache.items() if expiry and now > expiry]
            for k in to_delete:
                del self._memory_cache[k]
            if self.metrics:
                self.metrics['memory_size'].set(len(self._memory_cache))

    def _serialize(self, value: Any) -> str:
        try:
            return self.serializer(value)
        except Exception as e:
            logger.error(f"Serialization failed: {e}")
            return str(value)

    def _deserialize(self, value_str: str) -> Any:
        try:
            return self.deserializer(value_str)
        except Exception as e:
            logger.error(f"Deserialization failed for value '{value_str[:50]}...': {e}")
            return value_str

    async def _redis_operation(self, operation: str, *args, **kwargs) -> Any:
        if not self._redis_available or not self._redis:
            raise RuntimeError("Redis not available")
        last_exception = None
        for attempt in range(self.retry_attempts):
            try:
                return await getattr(self._redis, operation)(*args, **kwargs)
            except Exception as e:
                last_exception = e
                if attempt == self.retry_attempts - 1:
                    raise
                delay = min(self.retry_delay_ms * (2 ** attempt), 5000) / 1000.0
                await asyncio.sleep(delay)
        raise last_exception

    async def _get_policy_state(self, key: str, value: Any) -> CachePolicyState:
        now = datetime.now()
        key_length = len(key)
        try:
            serialized = self._serialize(value)
            size_bytes = len(serialized.encode('utf-8'))
        except:
            size_bytes = 1024
        access_count = self.key_access_count.get(key, 0)
        if key in self.key_last_access:
            last = self.key_last_access[key]
            if (now - last).total_seconds() > 3600:
                access_count = 0
        freq = access_count
        hour = now.hour
        redis_avail = self._redis_available
        redis_latency = 0.0
        if redis_avail:
            try:
                start = time.time()
                await self._redis.ping()
                redis_latency = (time.time() - start) * 1000
            except:
                redis_latency = 100.0
        memory_usage = len(self._memory_cache) / self.max_memory_entries * 100
        hit_rate = 0.5
        avg_latency = 0.0
        return CachePolicyState(
            key_length=key_length,
            estimated_size_bytes=size_bytes,
            access_frequency=freq,
            time_of_day_hour=hour,
            redis_available=redis_avail,
            redis_latency_ms=redis_latency,
            memory_usage_pct=memory_usage,
            hit_rate=hit_rate,
            avg_latency_ms=avg_latency,
        )

    def _update_key_stats(self, key: str, hit: bool, latency: float):
        now = datetime.now()
        self.key_access_count[key] = self.key_access_count.get(key, 0) + 1
        self.key_last_access[key] = now

    async def _apply_policy(
        self,
        policy: str,
        key: str,
        value: Any,
        ttl: int,
        action_idx: int,
        state_vec: np.ndarray,
        teacher_probs: np.ndarray
    ) -> Tuple[bool, bool, float, Optional[Any]]:
        start = time.time()
        success = False
        hit = False
        result = None

        # If MoE gating is available, it might have already overridden the policy in caller
        if policy == 'no_cache':
            if value is not None:
                success = True
            else:
                hit = False
            latency = (time.time() - start) * 1000
            return success, hit, latency, None

        if policy == 'redis_ttl_short':
            backend = 'redis'
            effective_ttl = 60
        elif policy == 'redis_ttl_long':
            backend = 'redis'
            effective_ttl = 600
        elif policy == 'memory_only':
            backend = 'memory'
            effective_ttl = ttl
        elif policy == 'adaptive_ttl':
            backend = 'redis' if self._redis_available else 'memory'
            effective_ttl = 300
        else:
            backend = 'redis'
            effective_ttl = ttl

        # Override TTLs with MOEA best parameters if available
        if self.moea_best_parameters:
            if policy == 'redis_ttl_short' and 'ttl_short' in self.moea_best_parameters:
                effective_ttl = int(self.moea_best_parameters['ttl_short'])
            elif policy == 'redis_ttl_long' and 'ttl_long' in self.moea_best_parameters:
                effective_ttl = int(self.moea_best_parameters['ttl_long'])
            elif policy == 'memory_only' and 'memory_ttl' in self.moea_best_parameters:
                effective_ttl = int(self.moea_best_parameters['memory_ttl'])

        if value is not None:  # SET operation
            if backend == 'redis':
                try:
                    serialized = self._serialize(value)
                    await self._redis_operation('setex', key, effective_ttl, serialized)
                    success = True
                except Exception as e:
                    logger.error(f"Redis set failed for key {key}: {e}")
                    self._redis_available = False
            else:
                async with self._memory_lock:
                    expiry = datetime.now() + timedelta(seconds=effective_ttl)
                    self._memory_cache[key] = (value, expiry)
                    if len(self._memory_cache) > self.max_memory_entries:
                        self._memory_cache.popitem(last=False)
                success = True
            latency = (time.time() - start) * 1000

            # MODP: record state and policy (optional)
            if self.modp_solver:
                state_attributes = {
                    'key': key,
                    'policy': policy,
                    'value_size': len(self._serialize(value)) if value is not None else 0,
                }
                self.modp_solver.add_state(
                    state_id=f"{key}_{time.time()}",
                    problem_id="cache_policy",
                    state_attributes=state_attributes,
                    objective_values={'hit_rate': 0.0, 'latency': 0.0, 'memory_usage': 0.0},
                    stage=0
                )
                self.modp_solver.add_policy(
                    policy_id=f"policy_{policy}_{time.time()}",
                    problem_id="cache_policy",
                    state_id=f"{key}_{time.time()}",
                    action=policy,
                    expected_objectives={'hit_rate': 0.0, 'latency': 0.0, 'memory_usage': 0.0}
                )

            # LIMIT Graph: add node for key (optional)
            if self.limit_graph_manager:
                self.limit_graph_manager.add_node(
                    "cache_keys",
                    key,
                    "cache_key",
                    {"policy": policy, "ttl": effective_ttl}
                )

            return success, False, latency, None
        else:  # GET operation
            if backend == 'redis':
                try:
                    result_str = await self._redis_operation('get', key)
                    if result_str is not None:
                        result = self._deserialize(result_str)
                        hit = True
                        success = True
                    else:
                        hit = False
                        success = True
                except Exception as e:
                    logger.error(f"Redis get failed for key {key}: {e}")
                    self._redis_available = False
                    success = False
            else:
                async with self._memory_lock:
                    if key in self._memory_cache:
                        stored_value, expiry = self._memory_cache[key]
                        if expiry and datetime.now() > expiry:
                            del self._memory_cache[key]
                            hit = False
                        else:
                            result = stored_value
                            hit = True
                            self._memory_cache.move_to_end(key)
                    else:
                        hit = False
                success = True
            latency = (time.time() - start) * 1000

            # MODP: record get result
            if self.modp_solver:
                self.modp_solver.add_state(
                    state_id=f"{key}_get_{time.time()}",
                    problem_id="cache_policy",
                    state_attributes={'key': key, 'policy': policy, 'hit': hit},
                    objective_values={'hit_rate': 1.0 if hit else 0.0, 'latency': latency, 'memory_usage': 0.0},
                    stage=1
                )

            return success, hit, latency, result

    # ========================================================================
    # MOEA Background Loop
    # ========================================================================
    async def _moea_loop(self):
        while self._running:
            try:
                await asyncio.sleep(self.moea_interval_seconds)
                await self.run_moea_optimization()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"MOEA loop failed: {e}")
                await asyncio.sleep(60)

    async def run_moea_optimization(self):
        if not self.moea_enabled:
            return

        param_bounds = {
            'ttl_short': (10, 300),
            'ttl_long': (300, 3600),
            'memory_ttl': (30, 1800),
            'redis_threshold': (0.0, 1.0),
        }

        async def evaluate(params: Dict[str, float]) -> Dict[str, float]:
            ttl_short = params['ttl_short']
            ttl_long = params['ttl_long']
            memory_ttl = params['memory_ttl']
            hit_rate = min(0.95, 0.3 + 0.0005 * (ttl_short + ttl_long + memory_ttl))
            redis_usage = params.get('redis_threshold', 0.5)
            memory_usage = min(1.0, len(self._memory_cache) / self.max_memory_entries * (memory_ttl / 3600))
            avg_latency = redis_usage * 2.0 + (1 - redis_usage) * 0.1
            latency_score = 1.0 - min(avg_latency / 10.0, 1.0)
            return {
                'hit_rate': hit_rate,
                'latency': latency_score,
                'memory_usage': 1.0 - memory_usage,
                'redis_usage': 1.0 - redis_usage,
            }

        self.moea_optimizer = NSGAIIOptimizer(
            evaluate_func=evaluate,
            parameter_bounds=param_bounds,
            population_size=self.moea_population_size,
            generations=self.moea_generations,
            mutation_rate=self.moea_mutation_rate,
            crossover_rate=self.moea_crossover_rate,
            objective_weights=self._get_dynamic_moea_weights(),
            dynamic_weights=self.moea_dynamic_weights,
        )

        pareto_front = await self.moea_optimizer.evolve()
        self.moea_pareto_front = pareto_front
        if pareto_front:
            weights = self._get_dynamic_moea_weights()
            best_point = self.moea_optimizer._select_best_from_pareto(pareto_front, weights)
            if best_point:
                self.moea_best_parameters = best_point.parameters
                logger.info(f"MOEA selected best parameters: {self.moea_best_parameters}")
                if self.metrics:
                    self.metrics['moea_pareto_front'].set(len(pareto_front))

    def _get_dynamic_moea_weights(self) -> Dict[str, float]:
        weights = self.moea_objective_weights.copy()
        if not self.moea_dynamic_weights:
            return weights
        mem_pct = len(self._memory_cache) / self.max_memory_entries if self.max_memory_entries > 0 else 0
        if mem_pct > 0.8:
            weights['memory_usage'] = min(0.6, weights.get('memory_usage', 0.2) * 1.5)
        total = sum(weights.values())
        return {k: v / total for k, v in weights.items()}

    # ========================================================================
    # PUBLIC METHODS
    # ========================================================================
    async def get(self, key: str) -> Optional[Any]:
        start = time.time()
        value = None

        state = await self._get_policy_state(key, None)
        if self.moe_gating:
            expert_name, _ = await self.moe_gating.select_expert(state)
            policy = {
                'redis_first': 'redis_ttl_short',
                'memory_first': 'memory_only',
                'size_aware': 'redis_ttl_long' if random.random() < 0.5 else 'memory_only',
                'frequency_aware': 'adaptive_ttl'
            }.get(expert_name, 'adaptive_ttl')
            action_idx = DistillationCachePolicyOptimizer.ACTION_SPACE.index(policy)
            state_vec = state.to_feature_vector()
            teacher_probs = np.ones(5) / 5
        else:
            policy, action_idx, state_vec, teacher_probs = await self.policy_optimizer.select_policy(state, exploration=True)

        success, hit, latency, result = await self._apply_policy(policy, key, None, 0, action_idx, state_vec, teacher_probs)

        self._update_key_stats(key, hit, latency)

        reward = 0.0
        if hit:
            reward += 0.5
        if latency < 10:
            reward += 0.3
        elif latency < 50:
            reward += 0.15
        if policy.startswith('redis') and self._redis_available:
            reward += 0.2
        elif policy == 'memory_only' and len(self._memory_cache) < self.max_memory_entries * 0.5:
            reward += 0.1
        reward = max(0.0, min(1.0, reward))

        if not self.moe_gating:
            next_state = await self._get_policy_state(key, None)
            asyncio.create_task(self.policy_optimizer.update(state_vec, action_idx, reward, next_state.to_feature_vector(), teacher_probs))
        else:
            if self.moe_gating:
                await self.moe_gating.add_training_sample(state, expert_name, reward)

        # RLHF: occasionally record a preference pair (simulated)
        if self.rlhf_trainer and random.random() < 0.05:
            chosen_policy = policy
            rejected_policy = random.choice([p for p in DistillationCachePolicyOptimizer.ACTION_SPACE if p != chosen_policy])
            self.rlhf_trainer.record_pair(
                pair_id=str(uuid.uuid4()),
                prompt=f"Which caching policy is best for key {key[:20]}?",
                chosen=chosen_policy,
                rejected=rejected_policy,
                reward_diff=reward,
                metadata={'key': key}
            )

        if self.metrics:
            if hit:
                self.metrics['hits'].inc()
            else:
                self.metrics['misses'].inc()
            self.metrics['latency'].labels('get').observe(time.time() - start)

        logger.debug(f"Cache {('hit' if hit else 'miss')} (policy={policy}): {key}")
        return result

    async def set(self, key: str, value: Any, ttl: int = 300) -> None:
        start = time.time()
        state = await self._get_policy_state(key, value)
        if self.moe_gating:
            expert_name, _ = await self.moe_gating.select_expert(state)
            policy = {
                'redis_first': 'redis_ttl_short',
                'memory_first': 'memory_only',
                'size_aware': 'redis_ttl_long' if len(self._serialize(value)) > 10000 else 'memory_only',
                'frequency_aware': 'adaptive_ttl'
            }.get(expert_name, 'adaptive_ttl')
            action_idx = DistillationCachePolicyOptimizer.ACTION_SPACE.index(policy)
            state_vec = state.to_feature_vector()
            teacher_probs = np.ones(5) / 5
        else:
            policy, action_idx, state_vec, teacher_probs = await self.policy_optimizer.select_policy(state, exploration=True)

        success, _, latency, _ = await self._apply_policy(policy, key, value, ttl, action_idx, state_vec, teacher_probs)

        reward = 0.0
        if success:
            reward += 0.6
        if latency < 5:
            reward += 0.2
        elif latency < 20:
            reward += 0.1
        if policy.startswith('redis') and state.estimated_size_bytes > 100_000:
            reward += 0.2
        reward = max(0.0, min(1.0, reward))

        if not self.moe_gating:
            next_state = await self._get_policy_state(key, value)
            asyncio.create_task(self.policy_optimizer.update(state_vec, action_idx, reward, next_state.to_feature_vector(), teacher_probs))
        else:
            if self.moe_gating:
                await self.moe_gating.add_training_sample(state, expert_name, reward)

        if self.metrics:
            self.metrics['latency'].labels('set').observe(time.time() - start)
            self.metrics['size'].set(
                (await self._redis.dbsize() if self._redis_available else 0) + len(self._memory_cache)
            )
        logger.debug(f"Cache set (policy={policy}): {key} (TTL={ttl}s)")

    async def delete(self, key: str) -> bool:
        deleted = False
        if self._redis_available:
            try:
                deleted = await self._redis_operation('delete', key) > 0
            except Exception as e:
                logger.error(f"Redis delete failed for key {key}: {e}")
                self._redis_available = False
        if not deleted:
            async with self._memory_lock:
                if key in self._memory_cache:
                    del self._memory_cache[key]
                    deleted = True
        logger.debug(f"Cache delete: {key}")
        return deleted

    async def clear(self) -> None:
        if self._redis_available:
            try:
                await self._redis_operation('flushdb')
                logger.info("Redis cache cleared.")
            except Exception as e:
                logger.error(f"Redis clear failed: {e}")
                self._redis_available = False
        async with self._memory_lock:
            self._memory_cache.clear()
            if self.metrics:
                self.metrics['memory_size'].set(0)
        logger.info("Memory cache cleared.")

    async def close(self) -> None:
        self._running = False
        tasks = [self._cleanup_task, self._health_task, self._moea_task]
        for task in tasks:
            if task:
                task.cancel()
        await asyncio.gather(*[t for t in tasks if t], return_exceptions=True)
        if self._redis:
            await self._redis.close()
            await self._redis.connection_pool.disconnect()
            logger.info("Redis connection closed.")

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()

    async def get_or_set(self, key: str, default: Any, ttl: int = 300) -> Any:
        value = await self.get(key)
        if value is None:
            value = default
            await self.set(key, value, ttl)
        return value

    async def get_stats(self) -> Dict[str, Any]:
        stats = {
            'backend': 'redis' if self._redis_available else 'memory',
            'memory_entries': len(self._memory_cache),
            'redis_available': self._redis_available,
            'distillation': self.policy_optimizer.get_stats(),
            'moea': {
                'pareto_front_size': len(self.moea_pareto_front),
                'best_parameters': self.moea_best_parameters,
                'enabled': self.moea_enabled,
            },
            'new_components': {
                'limit_graph': self.limit_graph_manager is not None,
                'modp': self.modp_solver is not None,
                'rlhf': self.rlhf_trainer is not None,
                'moe': self.moe_gating is not None,
            },
        }
        if self.metrics:
            stats['metrics'] = {
                'hits': self.metrics['hits']._value.get(),
                'misses': self.metrics['misses']._value.get(),
                'errors': {op: self.metrics['errors'].labels(op).value for op in ['get', 'set', 'delete', 'clear']},
            }
        return stats


# ============================================================================
# Example usage
# ============================================================================
if __name__ == "__main__":
    import asyncio

    async def demo():
        logging.basicConfig(level=logging.INFO)

        # Create cache manager with adaptive policy and MOEA
        cache = CacheManager(
            max_memory_entries=5,
            cleanup_interval_seconds=10,
            distillation_epsilon=0.2,
            distillation_train_every=2,
            moea_enabled=True,
            moea_interval_seconds=20,
            moea_population_size=10,
            moea_generations=3,
            enable_limit_graph=True,
            enable_modp=True,
            enable_rlhf=True,
            enable_moe=True,
        )

        # Simulate some accesses
        for i in range(20):
            key = f"key{i%5}"
            if i % 3 == 0:
                await cache.set(key, {"data": i}, ttl=5)
            else:
                val = await cache.get(key)
                print(f"get {key}: {val}")
            await asyncio.sleep(0.1)

        # Wait for MOEA to run
        await asyncio.sleep(25)

        stats = await cache.get_stats()
        print(f"Stats: {stats}")

        await cache.close()

    asyncio.run(demo())
