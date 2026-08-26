# src/enhancements/data_integration/helium_collector_v2_3_0.py
"""
Enhanced Helium Collector v2.3.0
==================================
Collects Helium hotspot connectivity data from live API and/or offline Parquet snapshots.
Provides a connectivity score (0‑1) based on RSSI, SNR, and other metrics.

ENHANCEMENTS OVER v2.2.0:
- Added Multi‑Objective Evolutionary Optimization (MOEA) using NSGA‑II to evolve
  source selection strategies (weights for combining success rate, latency, snapshot availability, etc.).
- Maintains a Pareto front of non‑dominated strategies.
- MODP‑based selection of best strategy with dynamic weights.
- Background task for periodic evolution.
- New configuration parameters for MOEA.
- Persistence of evolved strategies.

All previous features (distillation, circuit breakers, caching, fallback) are retained.
"""

import asyncio
import logging
import time
import os
from pathlib import Path
from typing import Dict, List, Optional, Any, Union, Tuple
from datetime import datetime
import aiohttp
from aiohttp import ClientTimeout, ClientError
import random
import json
import numpy as np
from abc import ABC, abstractmethod
from collections import deque
import pickle
import pandas as pd
import copy

# ---------- Pydantic ----------
try:
    from pydantic import BaseModel, Field, field_validator, ValidationError
    PYDANTIC_AVAILABLE = True
except ImportError:
    PYDANTIC_AVAILABLE = False

# ---------- Tenacity (retry) ----------
try:
    from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type, before_sleep_log, RetryError
    TENACITY_AVAILABLE = True
except ImportError:
    TENACITY_AVAILABLE = False

# ---------- Circuit breaker ----------
from enum import Enum

class CircuitBreakerState(Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"

class CircuitBreaker:
    """In‑memory circuit breaker with half‑open state."""
    def __init__(self, name: str, failure_threshold: int = 5, recovery_timeout: float = 30.0):
        self.name = name
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self._state = CircuitBreakerState.CLOSED
        self._failure_count = 0
        self._last_failure_time: Optional[datetime] = None
        self._lock = asyncio.Lock()

    async def call(self, func, *args, **kwargs):
        async with self._lock:
            now = datetime.utcnow()
            if self._state == CircuitBreakerState.OPEN:
                if self._last_failure_time and (now - self._last_failure_time).total_seconds() >= self.recovery_timeout:
                    self._state = CircuitBreakerState.HALF_OPEN
                    logger.info(f"Circuit breaker {self.name} entering HALF_OPEN")
                else:
                    raise RuntimeError(f"Circuit breaker {self.name} is OPEN")

        try:
            result = await func(*args, **kwargs)
            async with self._lock:
                if self._state == CircuitBreakerState.HALF_OPEN:
                    self._state = CircuitBreakerState.CLOSED
                    self._failure_count = 0
                    logger.info(f"Circuit breaker {self.name} closed after success")
                else:
                    self._failure_count = 0
            return result
        except Exception as e:
            async with self._lock:
                self._failure_count += 1
                self._last_failure_time = datetime.utcnow()
                if self._failure_count >= self.failure_threshold:
                    self._state = CircuitBreakerState.OPEN
                    logger.warning(f"Circuit breaker {self.name} opened after {self._failure_count} failures")
            raise e

# ---------- Prometheus ----------
try:
    from prometheus_client import Counter, Gauge, Histogram
    PROMETHEUS_AVAILABLE = True
except ImportError:
    PROMETHEUS_AVAILABLE = False

# ---------- Structlog ----------
try:
    import structlog
    logger = structlog.get_logger(__name__)
except ImportError:
    logger = logging.getLogger(__name__)
    logging.basicConfig(level=logging.INFO)

# ---------- scikit-learn for ML teacher ----------
try:
    from sklearn.ensemble import RandomForestClassifier
    from sklearn.preprocessing import LabelEncoder
    SKLEARN_ML = True
except ImportError:
    SKLEARN_ML = False

# ---------- Local imports ----------
from ..cache.cache_manager import CacheManager

# ============================================================================
# Configuration
# ============================================================================
if PYDANTIC_AVAILABLE:
    class HeliumConfig(BaseModel):
        """Configuration for HeliumCollector."""
        # API endpoint
        api_url: str = Field("https://api.helium.io/v1/")
        api_key: Optional[str] = None
        snapshot_path: Optional[Path] = None
        cache_ttl: int = Field(600, ge=0)
        retry_attempts: int = Field(3, ge=0)
        retry_min_wait: float = Field(1.0, gt=0)
        retry_max_wait: float = Field(10.0, gt=0)
        circuit_breaker_threshold: int = Field(5, ge=1)
        circuit_breaker_timeout: float = Field(30.0, ge=1)
        request_timeout: float = Field(10.0, ge=1)
        rssi_min: float = Field(-120.0)
        rssi_max: float = Field(-30.0)
        snr_min: float = Field(-10.0)
        snr_max: float = Field(30.0)
        enable_prometheus: bool = True
        default_score: float = 0.5

        # Distillation parameters
        distillation_epsilon: float = Field(0.1, ge=0, le=1)
        distillation_train_every: int = Field(10, ge=1)
        distillation_replay_size: int = Field(2000, ge=10)
        distillation_learning_rate: float = Field(0.01, ge=0.0001, le=1)
        distill_weight: float = Field(0.7, ge=0, le=1)
        rl_weight: float = Field(0.3, ge=0, le=1)

        # MOEA parameters
        moea_enabled: bool = Field(True)
        moea_interval_seconds: int = Field(300, ge=60)
        moea_population_size: int = Field(30, ge=10)
        moea_generations: int = Field(10, ge=2)
        moea_mutation_rate: float = Field(0.2, ge=0.0, le=1.0)
        moea_crossover_rate: float = Field(0.8, ge=0.0, le=1.0)
        moea_tournament_size: int = Field(3, ge=2)
        moea_objective_weights: Dict[str, float] = Field(
            default_factory=lambda: {
                'success_rate': 0.4,
                'latency': 0.3,
                'snapshot_usage': 0.2,
                'cost': 0.1,
            }
        )
        moea_dynamic_weights: bool = Field(True)

        # Persistence paths
        q_weights_path: str = Field("./helium_q_weights.json")
        interaction_logs_path: str = Field("./helium_interactions.csv")
        historical_model_path: str = Field("./helium_historical_model.pkl")
        moea_pareto_path: str = Field("./helium_moea_pareto.json")

        @field_validator('api_url')
        @classmethod
        def validate_api_url(cls, v):
            if not v.endswith('/'):
                v += '/'
            return v

        class Config:
            env_prefix = "HELIUM_"
else:
    # Fallback dict
    HELIUM_CONFIG = {
        "api_url": "https://api.helium.io/v1/",
        "api_key": None,
        "snapshot_path": None,
        "cache_ttl": 600,
        "retry_attempts": 3,
        "retry_min_wait": 1.0,
        "retry_max_wait": 10.0,
        "circuit_breaker_threshold": 5,
        "circuit_breaker_timeout": 30.0,
        "request_timeout": 10.0,
        "rssi_min": -120.0,
        "rssi_max": -30.0,
        "snr_min": -10.0,
        "snr_max": 30.0,
        "enable_prometheus": True,
        "default_score": 0.5,
        # Distillation defaults
        "distillation_epsilon": 0.1,
        "distillation_train_every": 10,
        "distillation_replay_size": 2000,
        "distillation_learning_rate": 0.01,
        "distill_weight": 0.7,
        "rl_weight": 0.3,
        # MOEA defaults
        "moea_enabled": True,
        "moea_interval_seconds": 300,
        "moea_population_size": 30,
        "moea_generations": 10,
        "moea_mutation_rate": 0.2,
        "moea_crossover_rate": 0.8,
        "moea_tournament_size": 3,
        "moea_objective_weights": {
            'success_rate': 0.4,
            'latency': 0.3,
            'snapshot_usage': 0.2,
            'cost': 0.1,
        },
        "moea_dynamic_weights": True,
        "q_weights_path": "./helium_q_weights.json",
        "interaction_logs_path": "./helium_interactions.csv",
        "historical_model_path": "./helium_historical_model.pkl",
        "moea_pareto_path": "./helium_moea_pareto.json",
    }

# ============================================================================
# Response Models (Pydantic) - unchanged
# ============================================================================
if PYDANTIC_AVAILABLE:
    class HeliumStatsResponse(BaseModel):
        rssi: float
        snr: float
        timestamp: Optional[str] = None

    class HeliumHotspotResponse(BaseModel):
        data: Optional[HeliumStatsResponse] = None

# ============================================================================
# DISTILLATION COMPONENTS FOR SOURCE SELECTION (unchanged)
# ============================================================================

@dataclass
class SourceSelectionState:
    """State for the distillation agent."""
    snapshot_exists: float
    hour_of_day: float
    day_of_week: float
    success_snapshot: float
    success_api: float
    success_fallback: float
    cb_state: float
    api_latency: float

    def to_feature_vector(self) -> np.ndarray:
        features = [
            self.snapshot_exists,
            self.hour_of_day / 24.0,
            self.day_of_week / 7.0,
            self.success_snapshot,
            self.success_api,
            self.success_fallback,
            self.cb_state / 2.0,
            min(self.api_latency / 5.0, 1.0),
        ]
        return np.array(features, dtype=np.float32)


class Teacher(ABC):
    @abstractmethod
    def predict(self, state: SourceSelectionState) -> np.ndarray: ...
    @abstractmethod
    def confidence(self, state: SourceSelectionState) -> float: ...

class SourceRuleBasedTeacher(Teacher):
    SOURCES = ['snapshot', 'api', 'fallback']
    def predict(self, state):
        probs = np.ones(3) * 0.1
        if state.snapshot_exists > 0.5 and state.success_snapshot > 0.7:
            probs[0] = 0.8
        elif state.cb_state > 1.5:
            probs[2] = 0.8
        elif state.success_api > 0.7 and state.api_latency < 2.0:
            probs[1] = 0.8
        else:
            probs[2] = 0.6
        return probs / probs.sum()
    def confidence(self, state):
        if state.snapshot_exists > 0.5 and state.success_snapshot > 0.7:
            return 0.6
        return 0.4

class SourceHistoricalMLTeacher(Teacher):
    def __init__(self, model_path=None):
        self.model = None; self.label_encoder = None
        self.model_path = model_path or Path(HELIUM_CONFIG['historical_model_path'])
        if self.model_path.exists():
            try:
                with open(self.model_path,'rb') as f:
                    self.model, self.label_encoder = pickle.load(f)
            except Exception as e:
                logger.error(f"Failed to load historical model: {e}")
    def predict(self, state):
        if self.model is None:
            return np.ones(3)/3
        x = state.to_feature_vector().reshape(1,-1)
        return self.model.predict_proba(x)[0]
    def confidence(self, state):
        return 0.7 if self.model is not None else 0.0

class SourceStatefulQTeacher(Teacher):
    def __init__(self, lr=0.1):
        self.lr = lr
        self.weights = np.zeros((8,3))
        self._load_state()
    def _load_state(self):
        path = Path(HELIUM_CONFIG['q_weights_path'])
        if path.exists():
            try:
                with open(path,'r') as f:
                    self.weights = np.array(json.load(f))
            except Exception as e:
                logger.error(f"Failed to load Q-weights: {e}")
    def _save_state(self):
        path = Path(HELIUM_CONFIG['q_weights_path'])
        with open(path,'w') as f:
            json.dump(self.weights.tolist(), f, indent=2)
    def predict(self, state):
        x = state.to_feature_vector()
        q = x @ self.weights
        exp_q = np.exp(q - np.max(q))
        return exp_q/exp_q.sum()
    def confidence(self, state):
        return 0.5
    def update(self, state, action, reward):
        x = state.to_feature_vector()
        q_current = np.dot(x, self.weights[:,action])
        self.weights[:,action] += self.lr*(reward-q_current)*x
        self._save_state()

class DistillationStudent:
    def __init__(self, feature_dim=8, n_classes=3, lr=0.01):
        self.weights = np.zeros((feature_dim,n_classes)); self.biases=np.zeros(n_classes)
        self.lr=lr; self.n_classes=n_classes; self.counter=0
    def predict_proba(self, state_vector, num_classes):
        if num_classes != self.n_classes:
            new_weights = np.zeros((self.weights.shape[0],num_classes)); new_biases=np.zeros(num_classes)
            min_dim = min(self.n_classes,num_classes)
            new_weights[:,:min_dim]=self.weights[:,:min_dim]; new_biases[:min_dim]=self.biases[:min_dim]
            self.weights=new_weights; self.biases=new_biases; self.n_classes=num_classes
        logits = state_vector @ self.weights + self.biases
        max_logit=np.max(logits); exp_logits=np.exp(logits-max_logit)
        return exp_logits/exp_logits.sum()
    def update(self, state_vector, teacher_probs, reward, action, distill_weight=0.7, rl_weight=0.3):
        current_probs = self.predict_proba(state_vector, self.n_classes)
        logits = state_vector @ self.weights + self.biases
        grad_distill = -(teacher_probs - current_probs)
        one_hot = np.zeros(self.n_classes); one_hot[action]=1.0
        grad_rl = -reward*(one_hot - current_probs)
        grad = distill_weight*grad_distill + rl_weight*grad_rl
        self.weights -= self.lr * np.outer(state_vector, grad)
        self.biases -= self.lr * grad
        self.counter += 1

class ReplayBuffer:
    def __init__(self, max_size=2000):
        self.buffer = deque(maxlen=max_size)
    def push(self, state_vec, action, reward, next_state_vec, teacher_probs):
        self.buffer.append((state_vec, action, reward, next_state_vec, teacher_probs))
    def sample(self, batch_size=32):
        if len(self.buffer) < batch_size:
            batch = list(self.buffer)
        else:
            batch = random.sample(self.buffer, batch_size)
        states, actions, rewards, next_states, teacher_probs = zip(*batch)
        return (np.array(states), actions, np.array(rewards), np.array(next_states), np.array(teacher_probs))
    def __len__(self): return len(self.buffer)

class DistillationSourceOptimizer:
    SOURCES = ['snapshot','api','fallback']
    def __init__(self, config):
        self.config = config
        self.student = DistillationStudent(lr=config.get('distillation_learning_rate',0.01))
        self.teachers = [SourceRuleBasedTeacher(), SourceHistoricalMLTeacher(), SourceStatefulQTeacher()]
        self.replay_buffer = ReplayBuffer(max_size=config.get('distillation_replay_size',2000))
        self.epsilon = config.get('distillation_epsilon',0.1)
        self.train_every = config.get('distillation_train_every',10)
        self.counter = 0
    async def select_source(self, state, exploration=True):
        state_vec = state.to_feature_vector(); n=3
        teacher_probs=np.zeros(n); total_conf=0.0
        for teacher in self.teachers:
            prob=teacher.predict(state); conf=teacher.confidence(state)
            if len(prob)!=n:
                if len(prob)<n: prob=np.pad(prob,(0,n-len(prob)),'constant')
                else: prob=prob[:n]
            teacher_probs += prob*conf; total_conf += conf
        if total_conf>0: teacher_probs/=total_conf
        else: teacher_probs = np.ones(n)/n
        student_probs = self.student.predict_proba(state_vec,n)
        if exploration and random.random()<self.epsilon:
            action_idx = random.randint(0,n-1)
        else:
            combined = 0.8*student_probs+0.2*teacher_probs
            action_idx = np.argmax(combined)
        return self.SOURCES[action_idx], action_idx, state_vec, teacher_probs
    async def update(self, state_vec, action_idx, reward, next_state_vec, teacher_probs):
        self.replay_buffer.push(state_vec, action_idx, reward, next_state_vec, teacher_probs)
        self.counter+=1
        if self.counter%self.train_every==0 and len(self.replay_buffer)>=8:
            batch = self.replay_buffer.sample(8)
            states, actions, rewards, _, teacher_probs_batch = batch
            for i in range(len(states)):
                self.student.update(states[i], teacher_probs_batch[i], rewards[i], actions[i])
    def get_stats(self):
        return {'student_counter':self.student.counter,'buffer_size':len(self.replay_buffer)}


# ============================================================================
# NEW: Multi‑Objective Source Strategy Evolution (NSGA‑II)
# ============================================================================

@dataclass
class MOPDSourceStrategy:
    """A source selection strategy: a weight vector for combining source metrics."""
    strategy_id: str
    weights: Dict[str, float]  # Keys: success_rate, latency, snapshot_usage, cost
    objectives: Dict[str, float]  # achieved values for these metrics (all maximized)
    scalarised_score: float = 0.0

    def to_dict(self):
        return {
            'strategy_id': self.strategy_id,
            'weights': self.weights,
            'objectives': self.objectives,
            'scalarised_score': self.scalarised_score,
        }

    @classmethod
    def from_dict(cls, data):
        return cls(**data)


class NSGAIISourceOptimizer:
    """
    Multi‑objective genetic algorithm for evolving source selection strategies.
    Decision variables: weights for success_rate, latency, snapshot_usage, cost.
    Objectives: maximize success_rate, minimize latency (convert to max), maximize snapshot_usage, minimize cost.
    The evaluation function replays historical interactions to estimate these metrics.
    """
    def __init__(self,
                 evaluate_func: Callable[[Dict[str, float]], Awaitable[Dict[str, float]]],
                 population_size: int = 20,
                 generations: int = 10,
                 mutation_rate: float = 0.2,
                 crossover_rate: float = 0.8,
                 tournament_size: int = 3,
                 objective_weights: Optional[Dict[str, float]] = None,
                 dynamic_weights: bool = True):
        self.evaluate_func = evaluate_func
        self.population_size = population_size
        self.generations = generations
        self.mutation_rate = mutation_rate
        self.crossover_rate = crossover_rate
        self.tournament_size = tournament_size
        self.objective_weights = objective_weights or {
            'success_rate': 0.4,
            'latency': 0.3,
            'snapshot_usage': 0.2,
            'cost': 0.1,
        }
        self.dynamic_weights = dynamic_weights

        self.best_individual = None
        self.best_fitness = -float('inf')
        self.evolution_history = []
        self.pareto_front: List[MOPDSourceStrategy] = []
        self._eval_cache: Dict[Tuple[float, ...], Dict[str, float]] = {}

    def _random_individual(self) -> Dict[str, float]:
        weights = {
            'success_rate': random.random(),
            'latency': random.random(),
            'snapshot_usage': random.random(),
            'cost': random.random(),
        }
        total = sum(weights.values())
        if total > 0:
            weights = {k: v / total for k, v in weights.items()}
        return weights

    def _crossover(self, p1, p2):
        child = {}
        for key in p1:
            if random.random() < 0.5:
                u = random.random()
                if u <= 0.5:
                    beta = (2 * u) ** (1 / (20 + 1))
                else:
                    beta = (1 / (2 * (1 - u))) ** (1 / (20 + 1))
                child[key] = max(0.0, min(1.0, 0.5 * ((1 + beta) * p1[key] + (1 - beta) * p2[key])))
            else:
                child[key] = p1[key] if random.random() < 0.5 else p2[key]
        total = sum(child.values())
        if total > 0:
            child = {k: v / total for k, v in child.items()}
        return child

    def _mutate(self, ind):
        mutant = ind.copy()
        for key in mutant:
            if random.random() < self.mutation_rate:
                u = random.random()
                if u < 0.5:
                    delta = (2 * u) ** (1 / (20 + 1)) - 1
                else:
                    delta = 1 - (2 * (1 - u)) ** (1 / (20 + 1))
                mutant[key] = mutant[key] + delta
                mutant[key] = max(0.0, min(1.0, mutant[key]))
        total = sum(mutant.values())
        if total > 0:
            mutant = {k: v / total for k, v in mutant.items()}
        return mutant

    def _fast_non_dominated_sort(self, points):
        fronts = []
        domination_count = {id(p): 0 for p in points}
        dominated_solutions = {id(p): [] for p in points}
        for i, p in enumerate(points):
            p_obj = p.objectives
            for j, q in enumerate(points):
                if i == j: continue
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

    def _crowding_distance(self, front):
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

    def _tournament_selection(self, population, fronts, crowding):
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

    def _compute_dynamic_weights(self):
        weights = self.objective_weights.copy()
        if not self.dynamic_weights or not self.pareto_front:
            return weights
        obj_keys = list(weights.keys())
        avg = {k: np.mean([p.objectives[k] for p in self.pareto_front]) for k in obj_keys}
        max_val = {k: np.max([p.objectives[k] for p in self.pareto_front]) for k in obj_keys}
        for k in obj_keys:
            if max_val[k] > 0 and avg[k] < 0.5 * max_val[k]:
                weights[k] = min(0.6, weights.get(k, 0.0) * 1.5)
        total = sum(weights.values())
        if total > 0:
            weights = {k: v / total for k, v in weights.items()}
        return weights

    def _select_best_from_pareto(self, pareto, weights):
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

    async def evolve(self):
        population = [self._random_individual() for _ in range(self.population_size)]
        points = []
        eval_tasks = [self.evaluate_func(ind) for ind in population]
        eval_results = await asyncio.gather(*eval_tasks)
        for ind, obj in zip(population, eval_results):
            point = MOPDSourceStrategy(strategy_id=str(uuid.uuid4()), weights=ind, objectives=obj)
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
            child_tasks = [self.evaluate_func(ind) for ind in offspring]
            child_results = await asyncio.gather(*child_tasks)
            child_points = []
            for ind, obj in zip(offspring, child_results):
                point = MOPDSourceStrategy(strategy_id=str(uuid.uuid4()), weights=ind, objectives=obj)
                child_points.append(point)
                self._eval_cache[tuple(sorted(ind.items()))] = obj
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
            self.best_individual = best.weights
            self.best_fitness = best.scalarised_score
        return self.pareto_front


# ============================================================================
# HeliumCollector (Enhanced with MOEA)
# ============================================================================
class HeliumCollector:
    """
    Enhanced Helium collector with adaptive source selection and multi‑objective evolution.
    """

    def __init__(
        self,
        cache: CacheManager,
        config: Optional[Union[Dict[str, Any], HeliumConfig]] = None,
    ):
        """
        Initialize the collector.
        """
        if config is None:
            if PYDANTIC_AVAILABLE:
                self.config = HeliumConfig()
            else:
                self.config = HELIUM_CONFIG
        elif isinstance(config, dict):
            if PYDANTIC_AVAILABLE:
                self.config = HeliumConfig(**config)
            else:
                self.config = config
        else:
            self.config = config

        self.cache = cache
        self.api_url = self.config.get("api_url", "https://api.helium.io/v1/")
        self.api_key = self.config.get("api_key") or os.environ.get("HELIUM_API_KEY")
        self.snapshot_path = self._resolve_snapshot_path(self.config.get("snapshot_path"))
        self.cache_ttl = self.config.get("cache_ttl", 600)
        self.request_timeout = self.config.get("request_timeout", 10.0)
        self.rssi_min = self.config.get("rssi_min", -120.0)
        self.rssi_max = self.config.get("rssi_max", -30.0)
        self.snr_min = self.config.get("snr_min", -10.0)
        self.snr_max = self.config.get("snr_max", 30.0)
        self.default_score = self.config.get("default_score", 0.5)

        # Session management
        self._session: Optional[aiohttp.ClientSession] = None
        self._session_lock = asyncio.Lock()

        # Circuit breaker for API calls
        self._circuit_breaker = CircuitBreaker(
            name="helium_api",
            failure_threshold=self.config.get("circuit_breaker_threshold", 5),
            recovery_timeout=self.config.get("circuit_breaker_timeout", 30.0),
        )

        # Distillation source optimizer
        self.source_optimizer = DistillationSourceOptimizer({
            'distillation_epsilon': self.config.get('distillation_epsilon', 0.1),
            'distillation_train_every': self.config.get('distillation_train_every', 10),
            'distillation_replay_size': self.config.get('distillation_replay_size', 2000),
            'distillation_learning_rate': self.config.get('distillation_learning_rate', 0.01),
        })

        # Interaction tracking
        self.interaction_log: List[Dict] = []
        self.last_state_vec: Optional[np.ndarray] = None
        self.last_action_idx: Optional[int] = None
        self.last_teacher_probs: Optional[np.ndarray] = None

        # MOEA parameters
        self.moea_enabled = self.config.get('moea_enabled', True)
        self.moea_interval_seconds = self.config.get('moea_interval_seconds', 300)
        self.moea_population_size = self.config.get('moea_population_size', 30)
        self.moea_generations = self.config.get('moea_generations', 10)
        self.moea_mutation_rate = self.config.get('moea_mutation_rate', 0.2)
        self.moea_crossover_rate = self.config.get('moea_crossover_rate', 0.8)
        self.moea_tournament_size = self.config.get('moea_tournament_size', 3)
        self.moea_objective_weights = self.config.get('moea_objective_weights', {
            'success_rate': 0.4,
            'latency': 0.3,
            'snapshot_usage': 0.2,
            'cost': 0.1,
        })
        self.moea_dynamic_weights = self.config.get('moea_dynamic_weights', True)
        self.moea_optimizer: Optional[NSGAIISourceOptimizer] = None
        self.evolved_pareto_front: List[MOPDSourceStrategy] = []
        self.best_evolved_strategy: Optional[MOPDSourceStrategy] = None
        self._moea_task: Optional[asyncio.Task] = None

        # Prometheus metrics
        if PROMETHEUS_AVAILABLE and self.config.get("enable_prometheus", True):
            self.metrics = {
                'calls': Counter('helium_api_calls_total', 'Helium API calls', ['status']),
                'errors': Counter('helium_api_errors_total', 'Helium API errors'),
                'latency': Histogram('helium_api_latency_seconds', 'Helium API latency'),
                'cache_hits': Counter('helium_cache_hits_total', 'Cache hits'),
                'cache_misses': Counter('helium_cache_misses_total', 'Cache misses'),
                'snapshot_hits': Counter('helium_snapshot_hits_total', 'Snapshot hits'),
                'fallback_usage': Counter('helium_fallback_usage_total', 'Fallback to default score'),
                'connectivity_score': Gauge('helium_connectivity_score', 'Hotspot connectivity score', ['hotspot_id']),
                'circuit_breaker_state': Gauge('helium_circuit_breaker_state', 'Circuit breaker state'),
                'source_selection': Counter('helium_source_selection', 'Source selected', ['source']),
                'source_reward': Histogram('helium_source_reward', 'Reward per source selection'),
                'moea_pareto_front': Gauge('helium_moea_pareto_front', 'MOEA Pareto front size'),
            }
        else:
            self.metrics = None

        if self.moea_enabled:
            self._moea_task = asyncio.create_task(self._moea_loop())

        logger.info("HeliumCollector initialized with adaptive source selection and MOEA", snapshot=self.snapshot_path)

    def _resolve_snapshot_path(self, path: Optional[Union[str, Path]]) -> Optional[Path]:
        if not path:
            return None
        if isinstance(path, str):
            path = Path(path)
        if path.exists():
            return path
        logger.warning("Snapshot path does not exist", path=str(path))
        return None

    async def _get_session(self) -> aiohttp.ClientSession:
        async with self._session_lock:
            if self._session is None or self._session.closed:
                timeout = ClientTimeout(total=self.request_timeout)
                connector = aiohttp.TCPConnector(limit=10, ttl_dns_cache=300)
                self._session = aiohttp.ClientSession(
                    connector=connector,
                    timeout=timeout,
                    raise_for_status=True,
                )
            return self._session

    async def close(self):
        if self._moea_task:
            self._moea_task.cancel()
            await asyncio.gather(self._moea_task, return_exceptions=True)
        if self._session and not self._session.closed:
            await self._session.close()
            self._session = None

    # ---------- State building ----------
    def _build_state(self, hotspot_id: str) -> SourceSelectionState:
        snapshot_exists = 1.0 if self.snapshot_path is not None and self.snapshot_path.exists() else 0.0
        now = datetime.utcnow()
        hour = now.hour
        dow = now.weekday()
        success_counts = {'snapshot': 0, 'api': 0, 'fallback': 0}
        total_counts = {'snapshot': 0, 'api': 0, 'fallback': 0}
        for entry in self.interaction_log[-100:]:
            src = entry['source']
            if src in success_counts:
                total_counts[src] += 1
                if entry['success']:
                    success_counts[src] += 1
        success_rates = {src: success_counts[src] / max(total_counts[src], 1) for src in success_counts}
        cb_state = 0.0
        if self._circuit_breaker._state == CircuitBreakerState.CLOSED:
            cb_state = 0.0
        elif self._circuit_breaker._state == CircuitBreakerState.HALF_OPEN:
            cb_state = 1.0
        else:
            cb_state = 2.0
        api_latencies = [entry['latency'] for entry in self.interaction_log if entry['source'] == 'api' and entry['latency'] is not None]
        avg_api_latency = np.mean(api_latencies) if api_latencies else 0.0
        return SourceSelectionState(
            snapshot_exists=snapshot_exists,
            hour_of_day=hour,
            day_of_week=dow,
            success_snapshot=success_rates.get('snapshot', 0.5),
            success_api=success_rates.get('api', 0.5),
            success_fallback=success_rates.get('fallback', 0.5),
            cb_state=cb_state,
            api_latency=avg_api_latency,
        )

    # ---------- Main get_connectivity_score (enhanced) ----------
    async def get_connectivity_score(self, hotspot_id: str, force_refresh: bool = False) -> float:
        cache_key = f"helium:score:{hotspot_id}"
        if not force_refresh:
            cached = await self.cache.get(cache_key)
            if cached is not None:
                if self.metrics:
                    self.metrics['cache_hits'].inc()
                logger.debug("Cache hit", hotspot_id=hotspot_id)
                return float(cached)
        if self.metrics:
            self.metrics['cache_misses'].inc()

        state = self._build_state(hotspot_id)
        source, action_idx, state_vec, teacher_probs = await self.source_optimizer.select_source(state, exploration=True)
        self.last_state_vec = state_vec
        self.last_action_idx = action_idx
        self.last_teacher_probs = teacher_probs

        data = None
        success = False
        latency = 0.0
        start_time = time.time()

        if source == 'snapshot':
            data = await self._fetch_from_snapshot(hotspot_id)
            if data is not None and len(data) > 0:
                success = True
                if self.metrics:
                    self.metrics['snapshot_hits'].inc()
        elif source == 'api':
            try:
                data = await self._fetch_from_api(hotspot_id)
                if data is not None and len(data) > 0:
                    success = True
                    latency = time.time() - start_time
            except Exception as e:
                logger.warning("API fetch failed", hotspot_id=hotspot_id, error=str(e))
        else:  # fallback
            success = False

        if data:
            score = self._compute_score(data)
        else:
            score = self.default_score
            if self.metrics:
                self.metrics['fallback_usage'].inc()

        reward = 1.0 if success else 0.0
        self._log_interaction(source, success, reward, latency)
        if self.last_state_vec is not None and self.last_action_idx is not None:
            next_state = self._build_state(hotspot_id)
            next_state_vec = next_state.to_feature_vector()
            await self.source_optimizer.update(
                self.last_state_vec,
                self.last_action_idx,
                reward,
                next_state_vec,
                self.last_teacher_probs
            )

        await self.cache.set(cache_key, str(score), ttl=self.cache_ttl)
        if self.metrics:
            self.metrics['connectivity_score'].labels(hotspot_id=hotspot_id).set(score)
            self.metrics['source_selection'].labels(source=source).inc()
            self.metrics['source_reward'].observe(reward)

        return score

    # ---------- Data fetching methods ----------
    async def _fetch_from_snapshot(self, hotspot_id: str) -> List[Dict]:
        if not self.snapshot_path:
            return None
        try:
            df = pd.read_parquet(self.snapshot_path)
            if 'hotspot_id' in df.columns:
                filtered = df[df['hotspot_id'] == hotspot_id]
                if not filtered.empty:
                    return filtered.to_dict('records')
            else:
                logger.warning("Snapshot missing 'hotspot_id' column")
        except Exception as e:
            logger.warning("Failed to read snapshot", error=str(e))
        return None

    async def _fetch_from_api(self, hotspot_id: str) -> List[Dict]:
        async def fetch():
            session = await self._get_session()
            url = f"{self.api_url}hotspots/{hotspot_id}/stats"
            headers = {}
            if self.api_key:
                headers["Authorization"] = f"Bearer {self.api_key}"

            async with session.get(url, headers=headers) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    if PYDANTIC_AVAILABLE:
                        try:
                            validated = HeliumHotspotResponse(**data)
                            if validated.data:
                                return [{
                                    'hotspot_id': hotspot_id,
                                    'rssi': validated.data.rssi,
                                    'snr': validated.data.snr,
                                    'timestamp': validated.data.timestamp or datetime.now().isoformat(),
                                }]
                        except ValidationError as e:
                            logger.warning("Response validation failed", error=str(e))
                    else:
                        stats = data.get('data', {})
                        if 'rssi' in stats and 'snr' in stats:
                            return [{
                                'hotspot_id': hotspot_id,
                                'rssi': stats['rssi'],
                                'snr': stats['snr'],
                                'timestamp': datetime.now().isoformat(),
                            }]
                    logger.warning("Unexpected API response structure", hotspot_id=hotspot_id)
                    return []
                elif resp.status == 429:
                    raise aiohttp.ClientResponseError(
                        request_info=resp.request_info,
                        history=resp.history,
                        status=resp.status,
                        message="Rate limit exceeded"
                    )
                else:
                    logger.warning("API returned error", status=resp.status, hotspot_id=hotspot_id)
                    return []

        if TENACITY_AVAILABLE:
            @retry(
                stop=stop_after_attempt(self.config.get("retry_attempts", 3)),
                wait=wait_exponential(
                    multiplier=1,
                    min=self.config.get("retry_min_wait", 1.0),
                    max=self.config.get("retry_max_wait", 10.0),
                ),
                retry=retry_if_exception_type((aiohttp.ClientError, asyncio.TimeoutError, aiohttp.ClientResponseError)),
                before_sleep=before_sleep_log(logger, logging.WARNING),
            )
            async def fetch_with_retry():
                return await fetch()
        else:
            async def fetch_with_retry():
                for attempt in range(self.config.get("retry_attempts", 3)):
                    try:
                        return await fetch()
                    except Exception as e:
                        if attempt == self.config.get("retry_attempts", 3) - 1:
                            raise
                        wait = min(
                            self.config.get("retry_min_wait", 1.0) * (2 ** attempt),
                            self.config.get("retry_max_wait", 10.0),
                        )
                        await asyncio.sleep(wait)

        start_time = time.time()
        data = await self._circuit_breaker.call(fetch_with_retry)
        if self.metrics and data is not None:
            self.metrics['calls'].labels(status='success').inc()
            self.metrics['latency'].observe(time.time() - start_time)
        return data

    # ---------- Score computation ----------
    def _compute_score(self, data: List[Dict]) -> float:
        if not data:
            return self.default_score
        rssi_values = [entry['rssi'] for entry in data if 'rssi' in entry]
        snr_values = [entry['snr'] for entry in data if 'snr' in entry]
        if not rssi_values or not snr_values:
            return self.default_score
        avg_rssi = sum(rssi_values) / len(rssi_values)
        avg_snr = sum(snr_values) / len(snr_values)
        rssi_score = (avg_rssi - self.rssi_min) / (self.rssi_max - self.rssi_min)
        rssi_score = max(0.0, min(1.0, rssi_score))
        snr_score = (avg_snr - self.snr_min) / (self.snr_max - self.snr_min)
        snr_score = max(0.0, min(1.0, snr_score))
        score = 0.6 * rssi_score + 0.4 * snr_score
        return max(0.0, min(1.0, score))

    # ---------- Batch fetch ----------
    async def fetch_batch_scores(self, hotspot_ids: List[str], max_concurrency: int = 10) -> Dict[str, float]:
        semaphore = asyncio.Semaphore(max_concurrency)
        async def fetch_with_semaphore(hid: str) -> Tuple[str, float]:
            async with semaphore:
                score = await self.get_connectivity_score(hid)
                return hid, score
        tasks = [fetch_with_semaphore(hid) for hid in hotspot_ids]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        scores = {}
        for idx, result in enumerate(results):
            if isinstance(result, Exception):
                logger.error("Batch fetch error", error=str(result))
                scores[hotspot_ids[idx]] = self.default_score
            else:
                hid, score = result
                scores[hid] = score
        return scores

    # ---------- Interaction logging ----------
    def _log_interaction(self, source: str, success: bool, reward: float, latency: float = 0.0):
        entry = {
            'timestamp': datetime.utcnow().isoformat(),
            'source': source,
            'success': success,
            'reward': reward,
            'latency': latency,
        }
        self.interaction_log.append(entry)
        log_path = Path(self.config.get('interaction_logs_path', './helium_interactions.csv'))
        df_log = pd.DataFrame([entry])
        if log_path.exists():
            df_log.to_csv(log_path, mode='a', header=False, index=False)
        else:
            df_log.to_csv(log_path, index=False)

    # ---------- Offline training for Historical ML ----------
    @classmethod
    def train_historical_model(cls, log_path: Path = Path("./helium_interactions.csv"), model_path: Path = Path("./helium_historical_model.pkl")):
        if not log_path.exists():
            logger.warning(f"Interaction logs not found at {log_path}. No model trained.")
            return
        df_logs = pd.read_csv(log_path)
        if len(df_logs) < 10:
            logger.warning("Not enough logs to train historical model (need at least 10).")
            return
        logger.info("Historical ML training requires state vectors in logs. Please implement logging of state vectors.")

    # ---------- Utility ----------
    async def update_snapshot(self, snapshot_path: Union[str, Path]) -> None:
        self.snapshot_path = self._resolve_snapshot_path(snapshot_path)
        logger.info("Snapshot path updated", path=snapshot_path)

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()


# ============================================================================
# NEW: MOEA Background Loop and Evolution
# ============================================================================
async def _moea_loop(self):
    while True:
        try:
            await asyncio.sleep(self.moea_interval_seconds)
            await self.run_source_evolution()
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error(f"MOEA loop failed: {e}")
            await asyncio.sleep(60)

async def run_source_evolution(self) -> List[MOPDSourceStrategy]:
    """Run NSGA-II to evolve source selection strategies."""
    if not self.moea_enabled:
        logger.info("MOEA is disabled.")
        return []

    # Placeholder evaluation function: use historical interaction logs to estimate objectives
    async def evaluate(weights: Dict[str, float]) -> Dict[str, float]:
        if len(self.interaction_log) < 10:
            return {'success_rate': 0.0, 'latency': 0.0, 'snapshot_usage': 0.0, 'cost': 0.0}
        # Simple heuristic: success rate from logs, latency average, snapshot usage (from logs)
        success_rate = np.mean([entry['success'] for entry in self.interaction_log[-100:]])
        latency = 1.0 - np.mean([entry['latency'] for entry in self.interaction_log if entry['latency'] is not None]) if any(entry['latency'] is not None for entry in self.interaction_log) else 0.0
        snapshot_usage = np.mean([1.0 if entry['source'] == 'snapshot' else 0.0 for entry in self.interaction_log])
        cost = 0.5  # placeholder
        return {
            'success_rate': success_rate,
            'latency': latency,
            'snapshot_usage': snapshot_usage,
            'cost': cost,
        }

    bounds = {
        'success_rate': (0.0, 1.0),
        'latency': (0.0, 1.0),
        'snapshot_usage': (0.0, 1.0),
        'cost': (0.0, 1.0),
    }

    self.moea_optimizer = NSGAIISourceOptimizer(
        evaluate_func=evaluate,
        population_size=self.moea_population_size,
        generations=self.moea_generations,
        mutation_rate=self.moea_mutation_rate,
        crossover_rate=self.moea_crossover_rate,
        tournament_size=self.moea_tournament_size,
        objective_weights=self.moea_objective_weights,
        dynamic_weights=self.moea_dynamic_weights,
    )

    pareto = await self.moea_optimizer.evolve()
    self.evolved_pareto_front = pareto
    if pareto:
        best = self.moea_optimizer._select_best_from_pareto(pareto, self._get_dynamic_moea_weights())
        if best:
            self.best_evolved_strategy = best
            logger.info(f"Best evolved strategy weights: {best.weights}")
        if self.metrics:
            self.metrics['moea_pareto_front'].set(len(pareto))
    return pareto

def _get_dynamic_moea_weights(self) -> Dict[str, float]:
    weights = self.moea_objective_weights.copy()
    if len(self.interaction_log) > 20:
        recent = self.interaction_log[-20:]
        success_rate = np.mean([entry['success'] for entry in recent])
        if success_rate < 0.5:
            weights['success_rate'] = min(0.6, weights['success_rate'] * 1.5)
        total = sum(weights.values())
        if total > 0:
            weights = {k: v / total for k, v in weights.items()}
    return weights

HeliumCollector._moea_loop = _moea_loop
HeliumCollector.run_source_evolution = run_source_evolution
HeliumCollector._get_dynamic_moea_weights = _get_dynamic_moea_weights


# ============================================================================
# Convenience factory
# ============================================================================
def create_helium_collector(
    cache: CacheManager,
    config: Optional[Dict[str, Any]] = None,
) -> HeliumCollector:
    return HeliumCollector(cache, config)


# ============================================================================
# UNIT TESTS (Phase 10)
# ============================================================================
import unittest
from unittest import IsolatedAsyncioTestCase

class TestDistillationComponents(IsolatedAsyncioTestCase):
    def setUp(self):
        self.config = {
            'distillation_epsilon': 0.0,
            'distillation_replay_size': 10,
            'distillation_learning_rate': 0.01,
            'distillation_train_every': 10,
        }
        self.optimizer = DistillationSourceOptimizer(self.config)

    def test_state_feature_vector(self):
        state = SourceSelectionState(
            snapshot_exists=1.0, hour_of_day=12, day_of_week=3,
            success_snapshot=0.9, success_api=0.5, success_fallback=0.3,
            cb_state=0.0, api_latency=1.5,
        )
        vec = state.to_feature_vector()
        self.assertEqual(len(vec), 8)

    def test_rule_based_teacher(self):
        teacher = SourceRuleBasedTeacher()
        state = SourceSelectionState(
            snapshot_exists=1.0, hour_of_day=12, day_of_week=3,
            success_snapshot=0.9, success_api=0.5, success_fallback=0.3,
            cb_state=0.0, api_latency=1.5,
        )
        probs = teacher.predict(state)
        self.assertAlmostEqual(sum(probs), 1.0)
        self.assertGreater(probs[0], probs[1])

    async def test_select_source(self):
        state = SourceSelectionState(
            snapshot_exists=1.0, hour_of_day=12, day_of_week=3,
            success_snapshot=0.9, success_api=0.5, success_fallback=0.3,
            cb_state=0.0, api_latency=1.5,
        )
        source, idx, state_vec, teacher_probs = await self.optimizer.select_source(state, exploration=False)
        self.assertIn(source, ['snapshot', 'api', 'fallback'])

    def test_replay_buffer(self):
        buffer = ReplayBuffer(max_size=5)
        state_vec = np.random.randn(8)
        buffer.push(state_vec, 0, 1.0, state_vec, np.ones(3)/3)
        self.assertEqual(len(buffer), 1)
        batch = buffer.sample(1)
        self.assertEqual(len(batch[0]), 1)


# ============================================================================
# Example usage
# ============================================================================
if __name__ == "__main__":
    import asyncio
    import sys
    sys.path.append('../')

    from ..cache.cache_manager import CacheManager

    async def main():
        cache = CacheManager()
        config = {
            "api_url": "https://api.helium.io/v1/",
            "api_key": "your_key_here",
            "cache_ttl": 600,
            "distillation_epsilon": 0.1,
            "distillation_train_every": 5,
            "moea_enabled": True,
            "moea_interval_seconds": 60,  # demo: run evolution every 60s
        }
        collector = create_helium_collector(cache, config)

        for _ in range(5):
            score = await collector.get_connectivity_score("hotspot_123")
            print(f"Score: {score}")

        stats = collector.source_optimizer.get_stats()
        print("Distillation stats:", stats)

        # Trigger evolution manually (also runs in background)
        pareto = await collector.run_source_evolution()
        print(f"Evolved Pareto front size: {len(pareto)}")
        if collector.best_evolved_strategy:
            print("Best strategy weights:", collector.best_evolved_strategy.weights)

        await collector.close()

    asyncio.run(main())
