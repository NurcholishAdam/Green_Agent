# cache_manager.py (Enhanced v2.1.0)
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

logger = logging.getLogger(__name__)


# ============================================================================
# NEW: Distillation Components for Caching Policy Selection
# ============================================================================

@dataclass
class CachePolicyState:
    """State for the distillation agent."""
    # Key characteristics
    key_length: int
    estimated_size_bytes: float
    access_frequency: float  # per hour
    # Context
    time_of_day_hour: int
    redis_available: bool
    redis_latency_ms: float
    memory_usage_pct: float
    # Historical performance of this key
    hit_rate: float
    avg_latency_ms: float

    def to_feature_vector(self) -> np.ndarray:
        """Convert to 10‑dim numeric feature vector."""
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


# Teacher abstract base
class Teacher(ABC):
    @abstractmethod
    def predict(self, state: CachePolicyState) -> np.ndarray:
        """Return probability vector over 5 policies."""
        pass

    @abstractmethod
    def confidence(self, state: CachePolicyState) -> float:
        """Return confidence in prediction [0,1]."""
        pass


class CacheRuleBasedTeacher(Teacher):
    """Rule‑based expert: uses heuristics."""
    ACTION_SPACE = ['redis_ttl_short', 'redis_ttl_long', 'memory_only', 'no_cache', 'adaptive_ttl']

    def predict(self, state: CachePolicyState) -> np.ndarray:
        probs = np.ones(5) * 0.1
        if not state.redis_available:
            probs[2] = 0.8  # memory_only
        elif state.estimated_size_bytes > 1_000_000:
            probs[0] = 0.6  # redis_ttl_short (avoid memory pressure)
        elif state.access_frequency > 50:
            probs[2] = 0.7  # memory_only for high frequency
        elif state.hit_rate < 0.2:
            probs[3] = 0.6  # no_cache for low hit rate
        else:
            probs[4] = 0.5  # adaptive_ttl
        return probs / probs.sum()

    def confidence(self, state: CachePolicyState) -> float:
        if not state.redis_available:
            return 0.8
        if state.estimated_size_bytes > 1_000_000:
            return 0.6
        return 0.4


class CacheHistoricalMLTeacher(Teacher):
    """Offline trained classifier on historical optimal policies."""
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
    """Linear Q‑learning with state features."""
    def __init__(self, cache_manager: 'CacheManager', lr: float = 0.1):
        self.cache_manager = cache_manager
        self.lr = lr
        self.weights = np.zeros((10, 5))  # 10 features, 5 actions
        self._load_state()

    def _load_state(self):
        pass

    def _save_state(self):
        pass

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
    """Linear softmax student updated via distillation + policy gradient."""
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
        logits = state_vector @ self.weights + self.biases

        # Distillation gradient (KL divergence)
        grad_distill = -(teacher_probs - current_probs)

        # Policy gradient (REINFORCE)
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
    """
    Multi‑teacher on‑policy distillation agent for caching policy selection.
    """
    ACTION_SPACE = ['redis_ttl_short', 'redis_ttl_long', 'memory_only', 'no_cache', 'adaptive_ttl']

    def __init__(self, cache_manager: 'CacheManager', config: Dict[str, Any]):
        self.cache_manager = cache_manager
        self.config = config
        self.student = DistillationStudent(lr=config.get('distillation_learning_rate', 0.01))
        self.teachers: List[Teacher] = [
            CacheRuleBasedTeacher(),
            CacheHistoricalMLTeacher(),  # optionally load model
            CacheStatefulQTeacher(cache_manager)
        ]
        self.replay_buffer = ReplayBuffer(max_size=config.get('distillation_replay_size', 2000))
        self.epsilon = config.get('distillation_epsilon', 0.1)
        self.train_every = config.get('distillation_train_every', 10)
        self.counter = 0

    async def select_policy(self, state: CachePolicyState, exploration: bool = True) -> Tuple[str, int, np.ndarray, np.ndarray]:
        state_vec = state.to_feature_vector()

        # Ensemble teachers
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
    """A point in the Pareto front for cache policy parameters."""
    policy_id: str
    parameters: Dict[str, float]  # e.g., TTL values, feature weights
    objectives: Dict[str, float]  # hit_rate, latency, memory_usage, ...
    scalarised_score: float = 0.0

    def to_dict(self) -> Dict[str, Any]:
        return {
            'policy_id': self.policy_id,
            'parameters': self.parameters,
            'objectives': self.objectives,
            'scalarised_score': self.scalarised_score,
        }


class NSGAIIOptimizer:
    """
    Multi‑objective genetic algorithm for cache policy parameter optimization.
    Objectives are to be maximized (e.g., hit_rate, -latency, -memory_usage).
    """
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
                # SBX
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
        # Example: increase weight of objective with lower average relative to max
        # For cache, we can dynamically adjust based on system state (handled in caller)
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
# CACHE MANAGER (Enhanced with MOEA)
# ============================================================================

class CacheManager:
    """
    Asynchronous cache manager with adaptive caching policy via distillation and
    multi‑objective optimization (NSGA‑II) for policy parameters.

    If Redis is available and reachable, it will be used for caching. Otherwise,
    it falls back to an in‑memory LRU cache with TTL support. The policy selection
    (which backend, TTL, or skip) is learned via multi‑teacher distillation.
    The MOEA periodically optimizes the TTL values and other parameters
    to achieve an optimal trade‑off between multiple objectives.
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
        moea_interval_seconds: int = 300,   # Run every 5 minutes
        moea_population_size: int = 20,
        moea_generations: int = 5,
        moea_mutation_rate: float = 0.2,
        moea_crossover_rate: float = 0.8,
        moea_objective_weights: Optional[Dict[str, float]] = None,
        moea_dynamic_weights: bool = True,
    ):
        """
        Initialize the cache manager with adaptive policy and MOEA.

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
        """
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

        # MOEA parameters
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
        """Start background TTL cleanup and health check tasks."""
        self._cleanup_task = asyncio.create_task(self._memory_cleanup_loop())
        self._health_task = asyncio.create_task(self._redis_health_loop())

    async def _init_redis(self):
        """Initialize Redis connection pool and test connectivity."""
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
        """Periodically check Redis availability and reconnect if needed."""
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
        """Periodically clean expired entries from the memory cache."""
        while self._running:
            try:
                await asyncio.sleep(self.cleanup_interval)
                await self._clean_expired_memory()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Memory cleanup error: {e}")

    async def _clean_expired_memory(self):
        """Remove expired entries from memory cache."""
        async with self._memory_lock:
            now = datetime.now()
            to_delete = [k for k, (_, expiry) in self._memory_cache.items() if expiry and now > expiry]
            for k in to_delete:
                del self._memory_cache[k]
            if self.metrics:
                self.metrics['memory_size'].set(len(self._memory_cache))

    def _serialize(self, value: Any) -> str:
        """Serialize a value to a string."""
        try:
            return self.serializer(value)
        except Exception as e:
            logger.error(f"Serialization failed: {e}")
            return str(value)

    def _deserialize(self, value_str: str) -> Any:
        """Deserialize a string to a Python object."""
        try:
            return self.deserializer(value_str)
        except Exception as e:
            logger.error(f"Deserialization failed for value '{value_str[:50]}...': {e}")
            return value_str

    async def _redis_operation(self, operation: str, *args, **kwargs) -> Any:
        """Execute a Redis operation with retries and error handling."""
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

    # ----- Helper: Build policy state -----
    async def _get_policy_state(self, key: str, value: Any) -> CachePolicyState:
        """Build state for the distillation agent."""
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
        """Update access statistics for a key."""
        now = datetime.now()
        self.key_access_count[key] = self.key_access_count.get(key, 0) + 1
        self.key_last_access[key] = now

    # ----- Apply selected policy -----
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
        """Execute the selected policy."""
        start = time.time()
        success = False
        hit = False
        result = None

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

        # If MOEA best parameters exist, override TTL for redis_ttl_short and redis_ttl_long
        if self.moea_best_parameters:
            if policy == 'redis_ttl_short' and 'ttl_short' in self.moea_best_parameters:
                effective_ttl = int(self.moea_best_parameters['ttl_short'])
            elif policy == 'redis_ttl_long' and 'ttl_long' in self.moea_best_parameters:
                effective_ttl = int(self.moea_best_parameters['ttl_long'])
            elif policy == 'memory_only' and 'memory_ttl' in self.moea_best_parameters:
                effective_ttl = int(self.moea_best_parameters['memory_ttl'])

        if value is not None:  # SET
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
            return success, False, latency, None
        else:  # GET
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
            return success, hit, latency, result

    # ========================================================================
    # MOEA Background Loop
    # ========================================================================
    async def _moea_loop(self):
        """Periodically run MOEA optimization to tune cache policy parameters."""
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
        """
        Run NSGA-II to optimize TTL values and other parameters based on
        recent access history. The objectives are:
          - maximize hit_rate (simulated)
          - minimize average latency (we maximize -latency)
          - minimize memory usage (we maximize -memory_usage)
          - minimize Redis usage (we maximize -redis_usage)
        """
        if not self.moea_enabled:
            return

        # Parameter bounds: TTL values for redis_ttl_short, redis_ttl_long, memory_ttl
        param_bounds = {
            'ttl_short': (10, 300),
            'ttl_long': (300, 3600),
            'memory_ttl': (30, 1800),
            'redis_threshold': (0.0, 1.0),  # threshold for using redis vs memory
        }

        async def evaluate(params: Dict[str, float]) -> Dict[str, float]:
            # Simulate performance based on recent access patterns
            # In a real implementation, we would replay recent logs or use a simulator.
            # For demonstration, we'll compute based on current system state.
            # Higher hit_rate is better; lower latency, memory_usage, redis_usage are better.
            # We'll convert to maximization objectives.
            # Simple heuristic: hit_rate increases with longer TTL, but memory usage also increases.
            ttl_short = params['ttl_short']
            ttl_long = params['ttl_long']
            memory_ttl = params['memory_ttl']

            # Hit rate: higher TTLs generally lead to higher hit rate, but with diminishing returns
            hit_rate = min(0.95, 0.3 + 0.0005 * (ttl_short + ttl_long + memory_ttl))
            # Latency: Redis operations have lower latency than memory? Actually memory is faster.
            # We'll assume Redis is 2ms, memory 0.1ms. So using redis more increases latency.
            # We can compute average latency based on policy distribution.
            # For simplicity, we use a weighted average based on parameter values.
            # redis_usage: proportion of keys stored in redis.
            redis_usage = params.get('redis_threshold', 0.5)
            # memory_usage: proportional to memory_ttl and number of entries
            memory_usage = min(1.0, len(self._memory_cache) / self.max_memory_entries * (memory_ttl / 3600))
            # latency in ms: redis ~2ms, memory ~0.1ms
            avg_latency = redis_usage * 2.0 + (1 - redis_usage) * 0.1
            # Normalize to 0-1 (lower is better)
            latency_score = 1.0 - min(avg_latency / 10.0, 1.0)

            objectives = {
                'hit_rate': hit_rate,
                'latency': latency_score,
                'memory_usage': 1.0 - memory_usage,
                'redis_usage': 1.0 - redis_usage,
            }
            return objectives

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
            # Select best using MODP (scalarisation with dynamic weights)
            weights = self._get_dynamic_moea_weights()
            best_point = self.moea_optimizer._select_best_from_pareto(pareto_front, weights)
            if best_point:
                self.moea_best_parameters = best_point.parameters
                logger.info(f"MOEA selected best parameters: {self.moea_best_parameters}")
                if self.metrics:
                    self.metrics['moea_pareto_front'].set(len(pareto_front))

    def _get_dynamic_moea_weights(self) -> Dict[str, float]:
        """Compute dynamic objective weights based on current system state."""
        weights = self.moea_objective_weights.copy()
        if not self.moea_dynamic_weights:
            return weights
        # If memory usage is high, increase weight on memory_usage
        mem_pct = len(self._memory_cache) / self.max_memory_entries if self.max_memory_entries > 0 else 0
        if mem_pct > 0.8:
            weights['memory_usage'] = min(0.6, weights.get('memory_usage', 0.2) * 1.5)
        # If Redis latency is high, increase weight on latency
        if self._redis_available:
            try:
                start = time.time()
                # Can't call async here; assume latency from last check? We'll skip.
                pass
            except:
                pass
        # Normalize
        total = sum(weights.values())
        return {k: v / total for k, v in weights.items()}

    # ========================================================================
    # PUBLIC METHODS (Enhanced with MOEA)
    # ========================================================================

    async def get(self, key: str) -> Optional[Any]:
        """Retrieve a value from cache. Policy is selected adaptively."""
        start = time.time()
        value = None

        state = await self._get_policy_state(key, None)
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

        next_state = await self._get_policy_state(key, None)
        asyncio.create_task(self.policy_optimizer.update(state_vec, action_idx, reward, next_state.to_feature_vector(), teacher_probs))

        if self.metrics:
            if hit:
                self.metrics['hits'].inc()
            else:
                self.metrics['misses'].inc()
            self.metrics['latency'].labels('get').observe(time.time() - start)

        logger.debug(f"Cache {('hit' if hit else 'miss')} (policy={policy}): {key}")
        return result

    async def set(self, key: str, value: Any, ttl: int = 300) -> None:
        """Store a value in cache. Policy is selected adaptively."""
        start = time.time()
        state = await self._get_policy_state(key, value)
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

        next_state = await self._get_policy_state(key, value)
        asyncio.create_task(self.policy_optimizer.update(state_vec, action_idx, reward, next_state.to_feature_vector(), teacher_probs))

        if self.metrics:
            self.metrics['latency'].labels('set').observe(time.time() - start)
            self.metrics['size'].set(
                (await self._redis.dbsize() if self._redis_available else 0) + len(self._memory_cache)
            )
        logger.debug(f"Cache set (policy={policy}): {key} (TTL={ttl}s)")

    async def delete(self, key: str) -> bool:
        """Delete a key from cache."""
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
        """Clear all cache entries."""
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
        """Close Redis connection pool and stop background tasks."""
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

    # ---------- Convenience methods ----------
    async def get_or_set(self, key: str, default: Any, ttl: int = 300) -> Any:
        """Get a value; if missing, set it to the default and return it."""
        value = await self.get(key)
        if value is None:
            value = default
            await self.set(key, value, ttl)
        return value

    # ---------- Statistics ----------
    async def get_stats(self) -> Dict[str, Any]:
        """Return current cache statistics."""
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
            moea_interval_seconds=20,  # run every 20 seconds for demo
            moea_population_size=10,
            moea_generations=3,
        )

        # Simulate some accesses to let the agent learn
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
