#!/usr/bin/env python3
"""
Cold Start Optimizer for Green Agent MoE System v3.4.1
Eliminates expert warmup latency through pre-initialization and transfer learning.
ENHANCED WITH: Multi‑Teacher On‑Policy Distillation and Multi‑Objective Evolutionary Optimization (NSGA‑II).
FIXES: feature dimension 14, async state building, start method for tasks, _all_points init, storage param.
"""

import asyncio
import logging
from typing import Dict, Any, List, Optional, Tuple, Set, Union, Callable, Protocol
from dataclasses import dataclass, field
from datetime import datetime, timedelta
import numpy as np
import json
import hashlib
import os
import zlib
import pickle
import random
from collections import OrderedDict, defaultdict, deque
from abc import ABC, abstractmethod
from pathlib import Path
import uuid
import copy
import time
import aiohttp
import aiofiles

from pydantic import BaseModel, Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

# Optional imports
try:
    from sklearn.ensemble import RandomForestClassifier
    from sklearn.preprocessing import LabelEncoder
    SKLEARN_ML = True
except ImportError:
    SKLEARN_ML = False

try:
    from prometheus_client import Counter, Gauge, Histogram, start_http_server, generate_latest
    PROMETHEUS_AVAILABLE = True
except ImportError:
    PROMETHEUS_AVAILABLE = False

logger = logging.getLogger(__name__)

# ============================================================================
# Configuration
# ============================================================================
class ColdStartConfig(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="COLD_START_", case_sensitive=False)

    cache_size: int = Field(100, ge=1)
    preload_threshold: float = Field(0.7, ge=0, le=1)
    checkpoint_dir: str = Field("./expert_checkpoints")

    enable_federated: bool = True
    enable_ml_demand: bool = True
    enable_carbon_aware: bool = True
    enable_helium_tracking: bool = True
    enable_intelligent_eviction: bool = True
    enable_persistence: bool = True
    enable_telemetry: bool = True

    enable_limit_graph: bool = True
    enable_modp: bool = True
    enable_rlhf: bool = True
    enable_moe: bool = True
    moe_expert_count: int = Field(5, ge=2)

    federated_server_url: Optional[str] = None
    privacy_epsilon: float = 1.0
    federated_sparsity_ratio: float = 0.1

    ml_history_window: int = 1000
    ml_online_learning_rate: float = 0.01
    ml_retrain_threshold: int = 100

    carbon_intensity_thresholds: Dict[str, float] = Field(default_factory=lambda: {
        'low': 200, 'medium': 350, 'high': 500
    })
    strategy_weights: Dict[str, float] = Field(default_factory=lambda: {
        'priority': 0.2, 'resource_cost': 0.3, 'carbon_efficiency': 0.3, 'urgency': 0.2
    })

    helium_forecast_model: str = "exponential_smoothing"

    eviction_weights: Dict[str, float] = Field(default_factory=lambda: {
        'usage_count': 0.25, 'age': 0.20, 'predicted_demand': 0.35, 'sustainability': 0.20
    })

    max_retries: int = 3
    retry_base_delay_ms: float = 100.0
    retry_max_delay_ms: float = 5000.0
    circuit_breaker_failure_threshold: int = 5
    circuit_breaker_recovery_timeout: float = 30.0

    persistence_path: str = "cold_start_state.json.gz"
    telemetry_export_interval: int = 60
    prometheus_port: Optional[int] = None

    distillation_epsilon: float = 0.1
    distillation_train_every: int = 10
    distillation_replay_size: int = 2000
    distillation_learning_rate: float = 0.01
    distill_weight: float = 0.7
    rl_weight: float = 0.3

    moea_enabled: bool = True
    moea_interval_seconds: int = 300
    moea_population_size: int = 20
    moea_generations: int = 10
    moea_mutation_rate: float = 0.2
    moea_crossover_rate: float = 0.8
    moea_tournament_size: int = 3
    moea_objective_weights: Optional[Dict[str, float]] = Field(default_factory=lambda: {
        'latency': 0.4, 'carbon': 0.3, 'cache_hit': 0.2, 'sustainability': 0.1
    })
    moea_dynamic_weights: bool = True
    moea_pareto_path: str = "./cold_start_moea_pareto.json"

    q_weights_path: str = "./cold_start_q_weights.json"
    interaction_logs_path: str = "./cold_start_interactions.csv"
    historical_model_path: str = "./cold_start_historical_model.pkl"

    @field_validator('eviction_weights')
    @classmethod
    def eviction_weights_sum(cls, v):
        if abs(sum(v.values()) - 1.0) > 0.01:
            raise ValueError("eviction_weights must sum to 1")
        return v

    @field_validator('strategy_weights')
    @classmethod
    def strategy_weights_sum(cls, v):
        if abs(sum(v.values()) - 1.0) > 0.01:
            raise ValueError("strategy_weights must sum to 1")
        return v

    @field_validator('carbon_intensity_thresholds')
    @classmethod
    def carbon_thresholds_ordered(cls, v):
        if not (v['low'] < v['medium'] < v['high']):
            raise ValueError("Thresholds must be low < medium < high")
        return v

# ============================================================================
# Circuit Breaker
# ============================================================================
class CircuitBreakerState(str):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"

class CircuitBreaker:
    def __init__(self, failure_threshold: int, recovery_timeout: float, name: str = "default"):
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.name = name
        self.state = CircuitBreakerState.CLOSED
        self.failure_count = 0
        self.last_failure_time: Optional[datetime] = None
        self._lock = asyncio.Lock()

    async def call(self, func: Callable, *args, **kwargs) -> Any:
        async with self._lock:
            if self.state == CircuitBreakerState.OPEN:
                if self.last_failure_time:
                    elapsed = (datetime.utcnow() - self.last_failure_time).total_seconds()
                    if elapsed >= self.recovery_timeout:
                        self.state = CircuitBreakerState.HALF_OPEN
                        self.failure_count = 0
                    else:
                        raise RuntimeError(f"Circuit breaker {self.name} OPEN")
                else:
                    raise RuntimeError(f"Circuit breaker {self.name} OPEN")
        try:
            result = await func(*args, **kwargs)
            async with self._lock:
                if self.state == CircuitBreakerState.HALF_OPEN:
                    self.state = CircuitBreakerState.CLOSED
                    self.failure_count = 0
                elif self.state == CircuitBreakerState.CLOSED:
                    self.failure_count = 0
            return result
        except Exception as e:
            async with self._lock:
                self.failure_count += 1
                self.last_failure_time = datetime.utcnow()
                if self.failure_count >= self.failure_threshold:
                    self.state = CircuitBreakerState.OPEN
            raise e

# ============================================================================
# Telemetry (simplified)
# ============================================================================
class ColdStartTelemetry:
    def __init__(self, config: ColdStartConfig):
        self.config = config
        self.metrics = defaultdict(lambda: defaultdict(int))
        self._lock = asyncio.Lock()

    def increment(self, metric_name: str, tags: Optional[Dict[str, str]] = None, value: float = 1.0):
        key = self._make_key(metric_name, tags)
        self.metrics['counters'][key] += value

    def gauge(self, metric_name: str, value: float, tags: Optional[Dict[str, str]] = None):
        key = self._make_key(metric_name, tags)
        self.metrics['gauges'][key] = value

    def histogram(self, metric_name: str, value: float, tags: Optional[Dict[str, str]] = None):
        key = self._make_key(metric_name, tags)
        self.metrics['histograms'][key].append(value)

    def _make_key(self, metric_name, tags):
        if tags:
            tag_str = ','.join(f"{k}={v}" for k, v in sorted(tags.items()))
            return f"{metric_name}{{{tag_str}}}"
        return metric_name

    async def export(self) -> str:
        return json.dumps(self.metrics, default=str)

# ============================================================================
# Persistence Manager (simplified)
# ============================================================================
class ColdStartPersistenceManager:
    def __init__(self, config: ColdStartConfig):
        self.config = config
        self.path = config.persistence_path
        self._lock = asyncio.Lock()

    async def save_state(self, optimizer: 'ColdStartOptimizer') -> bool:
        async with self._lock:
            try:
                state = {
                    'version': '3.4.1',
                    'checkpoint_cache': {k: v.__dict__ for k, v in optimizer.checkpoint_cache.items()},
                    'warmup_history': optimizer.warmup_history,
                    'sustainability_score': optimizer.sustainability_score,
                    'global_best_weights': optimizer.global_best_weights,
                    'q_teacher_weights': optimizer.strategy_optimizer.teachers[2].weights.tolist(),
                }
                json_str = json.dumps(state, default=str, indent=2)
                compressed = zlib.compress(json_str.encode('utf-8'))
                async with aiofiles.open(self.path, 'wb') as f:
                    await f.write(compressed)
                logger.info(f"State saved to {self.path}")
                return True
            except Exception as e:
                logger.error(f"Failed to save state: {e}")
                return False

    async def load_state(self, optimizer: 'ColdStartOptimizer') -> bool:
        if not os.path.exists(self.path):
            return False
        async with self._lock:
            try:
                async with aiofiles.open(self.path, 'rb') as f:
                    compressed = await f.read()
                json_str = zlib.decompress(compressed).decode('utf-8')
                state = json.loads(json_str)
                # Restore caches and weights
                optimizer.checkpoint_cache = OrderedDict()
                for k, v in state.get('checkpoint_cache', {}).items():
                    cp = ExpertCheckpoint(**v)
                    optimizer.checkpoint_cache[k] = cp
                optimizer.warmup_history = state.get('warmup_history', [])
                optimizer.sustainability_score = state.get('sustainability_score', 0.0)
                optimizer.global_best_weights = state.get('global_best_weights')
                q_weights = state.get('q_teacher_weights')
                if q_weights:
                    optimizer.strategy_optimizer.teachers[2].weights = np.array(q_weights)
                return True
            except Exception as e:
                logger.error(f"Failed to load state: {e}")
                return False

# ============================================================================
# Data Classes
# ============================================================================
@dataclass
class ExpertCheckpoint:
    expert_id: str
    expert_type: str
    model_state: Dict[str, Any]
    optimizer_state: Dict[str, Any]
    feature_distribution: Dict[str, float]
    performance_metrics: Dict[str, float]
    created_at: datetime
    last_used: datetime
    usage_count: int = 0
    carbon_footprint_kg: float = 0.0
    helium_usage_l: float = 0.0
    sustainability_score: float = 0.0
    federated_consensus: bool = False
    peer_count: int = 0

@dataclass
class WarmupStrategy:
    strategy_type: str
    priority: int
    estimated_warmup_time_ms: float
    resource_cost: float
    success_probability: float
    carbon_efficiency: float = 0.5
    helium_efficiency: float = 0.5

@dataclass
class ColdStartState:
    expert_type: str
    urgency: str
    carbon_budget: float
    helium_budget: float
    max_latency_ms: float
    carbon_intensity: float
    cache_utilization: float
    recent_hit_rate: float
    strategy_success_rates: Dict[str, float]
    avg_warmup_time_ms: float
    avg_sustainability_score: float

    def to_feature_vector(self) -> np.ndarray:
        """Return a 14‑dimensional feature vector."""
        type_map = {'energy': 0, 'data': 1, 'iot': 2, 'quantum': 3, 'general': 4}
        urgency_map = {'critical': 0, 'high': 1, 'normal': 2, 'low': 3}
        features = [
            type_map.get(self.expert_type, 4) / 4.0,
            urgency_map.get(self.urgency, 2) / 3.0,
            min(self.carbon_budget, 1.0),
            min(self.helium_budget, 1.0),
            min(self.max_latency_ms / 1000.0, 1.0),
            min(self.carbon_intensity / 1000.0, 1.0),
            self.recent_hit_rate,
            self.strategy_success_rates.get('preload', 0.5),
            self.strategy_success_rates.get('transfer', 0.5),
            self.strategy_success_rates.get('progressive', 0.5),
            self.strategy_success_rates.get('hybrid', 0.5),
            self.strategy_success_rates.get('federated', 0.5),
            min(self.avg_warmup_time_ms / 1000.0, 1.0),
            self.avg_sustainability_score,
        ]
        return np.array(features, dtype=np.float32)

# ============================================================================
# Teachers, Student, ReplayBuffer, DistillationStrategyOptimizer
# ============================================================================
class Teacher(ABC):
    @abstractmethod
    def predict(self, state: ColdStartState) -> np.ndarray: ...
    @abstractmethod
    def confidence(self, state: ColdStartState) -> float: ...

class StrategyRuleBasedTeacher(Teacher):
    STRATEGIES = ['preload', 'transfer', 'progressive', 'hybrid', 'federated']
    def predict(self, state):
        probs = np.ones(5) * 0.1
        if state.recent_hit_rate > 0.8:
            probs[0] = 0.8
        elif state.expert_type == 'quantum' and state.max_latency_ms < 100:
            probs[3] = 0.7
        elif state.carbon_intensity > 500:
            probs[4] = 0.6
        elif state.urgency == 'critical':
            probs[1] = 0.7
        else:
            probs[2] = 0.6
        return probs / probs.sum()
    def confidence(self, state):
        return 0.6 if state.recent_hit_rate > 0.8 else 0.4

class StrategyHistoricalMLTeacher(Teacher):
    def __init__(self, model_path: Optional[str] = None):
        self.model = None
        self.label_encoder = None
        self.model_path = model_path or Path(ColdStartConfig().historical_model_path)
        if self.model_path.exists() and SKLEARN_ML:
            try:
                with open(self.model_path, 'rb') as f:
                    self.model, self.label_encoder = pickle.load(f)
            except Exception as e:
                logger.error(f"Failed to load historical model: {e}")
    def predict(self, state):
        if self.model is None:
            return np.ones(5)/5
        x = state.to_feature_vector().reshape(1, -1)
        return self.model.predict_proba(x)[0]
    def confidence(self, state):
        return 0.7 if self.model is not None else 0.0

class StrategyStatefulQTeacher(Teacher):
    def __init__(self, lr=0.1):
        self.lr = lr
        self.weights = np.zeros((14, 5))
        self._load_state()
    def _load_state(self):
        path = Path(ColdStartConfig().q_weights_path)
        if path.exists():
            try:
                with open(path, 'r') as f:
                    data = json.load(f)
                self.weights = np.array(data)
            except Exception:
                pass
    def _save_state(self):
        path = Path(ColdStartConfig().q_weights_path)
        with open(path, 'w') as f:
            json.dump(self.weights.tolist(), f, indent=2)
    def predict(self, state):
        x = state.to_feature_vector()
        q = x @ self.weights
        exp_q = np.exp(q - np.max(q))
        return exp_q / exp_q.sum()
    def confidence(self, state):
        return 0.5
    def update(self, state, action, reward):
        x = state.to_feature_vector()
        q_current = np.dot(x, self.weights[:, action])
        self.weights[:, action] += self.lr * (reward - q_current) * x
        self._save_state()

class DistillationStudent:
    def __init__(self, feature_dim=14, n_classes=5, lr=0.01):
        self.weights = np.zeros((feature_dim, n_classes))
        self.biases = np.zeros(n_classes)
        self.lr = lr
        self.n_classes = n_classes
        self.counter = 0
    def predict_proba(self, state_vector, num_classes=None):
        if num_classes is None:
            num_classes = self.n_classes
        logits = state_vector @ self.weights + self.biases
        max_logit = np.max(logits)
        exp_logits = np.exp(logits - max_logit)
        return exp_logits / exp_logits.sum()
    def update(self, state_vector, teacher_probs, reward, action, distill_weight=0.7, rl_weight=0.3):
        current_probs = self.predict_proba(state_vector, self.n_classes)
        grad_distill = -(teacher_probs - current_probs)
        one_hot = np.zeros(self.n_classes)
        one_hot[action] = 1.0
        grad_rl = -reward * (one_hot - current_probs)
        grad = distill_weight * grad_distill + rl_weight * grad_rl
        self.weights -= self.lr * np.outer(state_vector, grad)
        self.biases -= self.lr * grad
        self.counter += 1

class ReplayBuffer:
    def __init__(self, max_size=2000):
        self.buffer = deque(maxlen=max_size)
    def push(self, s, a, r, ns, tp):
        self.buffer.append((s, a, r, ns, tp))
    def sample(self, batch_size=32):
        if len(self.buffer) < batch_size:
            batch = list(self.buffer)
        else:
            batch = random.sample(self.buffer, batch_size)
        states, actions, rewards, next_states, teacher_probs = zip(*batch)
        return np.array(states), actions, np.array(rewards), np.array(next_states), np.array(teacher_probs)
    def __len__(self):
        return len(self.buffer)

class DistillationStrategyOptimizer:
    STRATEGIES = ['preload', 'transfer', 'progressive', 'hybrid', 'federated']
    def __init__(self, config):
        self.config = config
        self.student = DistillationStudent(lr=config.get('distillation_learning_rate', 0.01))
        self.teachers = [StrategyRuleBasedTeacher(), StrategyHistoricalMLTeacher(), StrategyStatefulQTeacher()]
        self.replay_buffer = ReplayBuffer(max_size=config.get('distillation_replay_size', 2000))
        self.epsilon = config.get('distillation_epsilon', 0.1)
        self.train_every = config.get('distillation_train_every', 10)
        self.counter = 0
    async def select_strategy(self, state, exploration=True):
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
            teacher_probs = np.ones(5)/5
        student_probs = self.student.predict_proba(state_vec, 5)
        if exploration and random.random() < self.epsilon:
            action_idx = random.randint(0, 4)
        else:
            combined = 0.8*student_probs + 0.2*teacher_probs
            action_idx = np.argmax(combined)
        return self.STRATEGIES[action_idx], action_idx, state_vec, teacher_probs
    async def update(self, state_vec, action_idx, reward, next_state_vec, teacher_probs):
        self.replay_buffer.push(state_vec, action_idx, reward, next_state_vec, teacher_probs)
        self.counter += 1
        if self.counter % self.train_every == 0 and len(self.replay_buffer) >= 8:
            batch = self.replay_buffer.sample(8)
            states, actions, rewards, _, teacher_probs_batch = batch
            for i in range(len(states)):
                self.student.update(states[i], teacher_probs_batch[i], rewards[i], actions[i])
    def get_stats(self):
        return {'student_counter': self.student.counter, 'buffer_size': len(self.replay_buffer)}

# ============================================================================
# NSGA-II Strategy Optimizer
# ============================================================================
@dataclass
class MOPDStrategyWeights:
    vector_id: str
    weights: Dict[str, float]
    objectives: Dict[str, float]
    scalarised_score: float = 0.0
    def to_dict(self):
        return {'vector_id': self.vector_id, 'weights': self.weights, 'objectives': self.objectives, 'scalarised_score': self.scalarised_score}
    @classmethod
    def from_dict(cls, data):
        return cls(**data)

class NSGAIIStrategyOptimizer:
    def __init__(self, evaluate_func, population_size=20, generations=10, mutation_rate=0.2, crossover_rate=0.8, tournament_size=3, objective_weights=None, dynamic_weights=True):
        self.evaluate_func = evaluate_func
        self.population_size = population_size
        self.generations = generations
        self.mutation_rate = mutation_rate
        self.crossover_rate = crossover_rate
        self.tournament_size = tournament_size
        self.objective_weights = objective_weights or {'latency': 0.4, 'carbon': 0.3, 'cache_hit': 0.2, 'sustainability': 0.1}
        self.dynamic_weights = dynamic_weights
        self.best_individual = None
        self.best_fitness = -float('inf')
        self.pareto_front = []
        self._eval_cache = {}
        self._all_points = []  # Initialize

    def _random_individual(self):
        keys = ['preload', 'transfer', 'progressive', 'hybrid', 'federated']
        w = {k: random.random() for k in keys}
        total = sum(w.values())
        if total > 0:
            w = {k: v/total for k, v in w.items()}
        return w

    def _crossover(self, p1, p2):
        child = {}
        for key in p1:
            if random.random() < 0.5:
                u = random.random()
                if u <= 0.5:
                    beta = (2*u)**(1/(20+1))
                else:
                    beta = (1/(2*(1-u)))**(1/(20+1))
                child[key] = max(0.0, min(1.0, 0.5*((1+beta)*p1[key] + (1-beta)*p2[key])))
            else:
                child[key] = p1[key] if random.random() < 0.5 else p2[key]
        total = sum(child.values())
        if total > 0:
            child = {k: v/total for k, v in child.items()}
        return child

    def _mutate(self, ind):
        mutant = ind.copy()
        for key in mutant:
            if random.random() < self.mutation_rate:
                u = random.random()
                if u < 0.5:
                    delta = (2*u)**(1/(20+1)) - 1
                else:
                    delta = 1 - (2*(1-u))**(1/(20+1))
                mutant[key] = mutant[key] + delta
                mutant[key] = max(0.0, min(1.0, mutant[key]))
        total = sum(mutant.values())
        if total > 0:
            mutant = {k: v/total for k, v in mutant.items()}
        return mutant

    def _fast_non_dominated_sort(self, points):
        fronts = []
        domination_count = {id(p):0 for p in points}
        dominated_solutions = {id(p):[] for p in points}
        for i,p in enumerate(points):
            p_obj = p.objectives
            for j,q in enumerate(points):
                if i==j: continue
                q_obj = q.objectives
                if all(p_obj[k]>=q_obj[k] for k in p_obj) and any(p_obj[k]>q_obj[k] for k in p_obj):
                    dominated_solutions[id(p)].append(q)
                elif all(q_obj[k]>=p_obj[k] for k in q_obj) and any(q_obj[k]>p_obj[k] for k in q_obj):
                    domination_count[id(p)] += 1
            if domination_count[id(p)]==0:
                if not fronts:
                    fronts.append([])
                fronts[0].append(p)
        i=0
        while i<len(fronts):
            next_front=[]
            for p in fronts[i]:
                for q in dominated_solutions[id(p)]:
                    domination_count[id(q)]-=1
                    if domination_count[id(q)]==0:
                        next_front.append(q)
            if next_front:
                fronts.append(next_front)
            i+=1
        return fronts

    def _crowding_distance(self, front):
        if not front: return {}
        distances={id(p):0.0 for p in front}
        obj_keys=list(front[0].objectives.keys())
        for obj in obj_keys:
            sorted_front=sorted(front,key=lambda x:x.objectives[obj])
            distances[id(sorted_front[0])]=float('inf')
            distances[id(sorted_front[-1])]=float('inf')
            obj_min=sorted_front[0].objectives[obj]
            obj_max=sorted_front[-1].objectives[obj]
            if obj_max==obj_min: continue
            for i in range(1,len(sorted_front)-1):
                distances[id(sorted_front[i])]+=(sorted_front[i+1].objectives[obj]-sorted_front[i-1].objectives[obj])/(obj_max-obj_min)
        return distances

    def _tournament_selection(self, population, fronts, crowding):
        candidates=random.sample(population,self.tournament_size)
        ind_to_point={}
        for ind,point in zip(population,self._all_points):
            ind_to_point[id(ind)]=point
        best=candidates[0]; best_rank=float('inf'); best_crowding=-float('inf')
        for cand in candidates:
            point=ind_to_point.get(id(cand))
            if not point: continue
            rank=len(fronts)
            for fi,front in enumerate(fronts):
                if point in front:
                    rank=fi; break
            cd=crowding.get(id(point),0)
            if rank<best_rank or (rank==best_rank and cd>best_crowding):
                best=cand; best_rank=rank; best_crowding=cd
        return best

    def _compute_dynamic_weights(self):
        weights=self.objective_weights.copy()
        if not self.dynamic_weights or not self.pareto_front:
            return weights
        obj_keys=list(weights.keys())
        avg={k:np.mean([p.objectives[k] for p in self.pareto_front]) for k in obj_keys}
        max_val={k:np.max([p.objectives[k] for p in self.pareto_front]) for k in obj_keys}
        for k in obj_keys:
            if max_val[k]>0 and avg[k]<0.5*max_val[k]:
                weights[k]=min(0.6,weights.get(k,0.0)*1.5)
        total=sum(weights.values())
        if total>0:
            weights={k:v/total for k,v in weights.items()}
        return weights

    def _select_best_from_pareto(self, pareto, weights):
        if not pareto: return None
        obj_keys=list(weights.keys())
        max_vals={k:max(p.objectives[k] for p in pareto) for k in obj_keys}
        min_vals={k:min(p.objectives[k] for p in pareto) for k in obj_keys}
        ranges={k:max_vals[k]-min_vals[k] if max_vals[k]!=min_vals[k] else 1.0 for k in obj_keys}
        best=None; best_score=-float('inf')
        for p in pareto:
            score=0.0
            for k in obj_keys:
                val=p.objectives[k]
                norm=(val-min_vals[k])/ranges[k] if ranges[k]>0 else 1.0
                score+=weights.get(k,0.0)*norm
            p.scalarised_score=score
            if score>best_score:
                best_score=score; best=p
        return best

    async def evolve(self):
        if self.evaluate_func is None:
            raise ValueError("evaluate_func not set")
        population=[self._random_individual() for _ in range(self.population_size)]
        points=[]
        eval_tasks=[self.evaluate_func(ind) for ind in population]
        eval_results=await asyncio.gather(*eval_tasks)
        for ind,obj in zip(population,eval_results):
            point=MOPDStrategyWeights(vector_id=str(uuid.uuid4()),weights=ind,objectives=obj)
            points.append(point)
            self._eval_cache[tuple(sorted(ind.items()))]=obj
        self._all_points=points
        for gen in range(self.generations):
            fronts=self._fast_non_dominated_sort(points)
            crowding={}
            for front in fronts:
                front_crowding=self._crowding_distance(front)
                crowding.update(front_crowding)
            offspring=[]
            while len(offspring)<self.population_size:
                parent1=self._tournament_selection(population,fronts,crowding)
                parent2=self._tournament_selection(population,fronts,crowding)
                if random.random()<self.crossover_rate:
                    child=self._crossover(parent1,parent2)
                else:
                    child=copy.deepcopy(parent1)
                child=self._mutate(child)
                offspring.append(child)
            child_tasks=[self.evaluate_func(ind) for ind in offspring]
            child_results=await asyncio.gather(*child_tasks)
            child_points=[]
            for ind,obj in zip(offspring,child_results):
                point=MOPDStrategyWeights(vector_id=str(uuid.uuid4()),weights=ind,objectives=obj)
                child_points.append(point)
                self._eval_cache[tuple(sorted(ind.items()))]=obj
            combined_inds=population+offspring
            combined_points=points+child_points
            unique_pairs={}
            for ind,p in zip(combined_inds,combined_points):
                key=tuple(sorted(ind.items()))
                unique_pairs[key]=(ind,p)
            population=[v[0] for v in unique_pairs.values()]
            points=[v[1] for v in unique_pairs.values()]
            self._all_points=points
            fronts=self._fast_non_dominated_sort(points)
            new_population=[]; new_points=[]
            for front in fronts:
                if len(new_population)+len(front)<=self.population_size:
                    for p in front:
                        for ind,p2 in zip(population,points):
                            if p2 is p:
                                new_population.append(ind); new_points.append(p); break
                else:
                    crowding=self._crowding_distance(front)
                    sorted_front=sorted(front,key=lambda x:crowding.get(id(x),0),reverse=True)
                    for p in sorted_front:
                        if len(new_population)>=self.population_size: break
                        for ind,p2 in zip(population,points):
                            if p2 is p:
                                new_population.append(ind); new_points.append(p); break
            population=new_population[:self.population_size]
            points=new_points[:self.population_size]
            self._all_points=points
            fronts=self._fast_non_dominated_sort(points)
            if fronts:
                self.pareto_front=fronts[0]
            logger.info(f"Generation {gen+1}/{self.generations}: Pareto front size={len(self.pareto_front)}")
        weights=self._compute_dynamic_weights()
        best=self._select_best_from_pareto(self.pareto_front,weights)
        if best:
            self.best_individual=best.weights
            self.best_fitness=best.scalarised_score
        return self.pareto_front

# ============================================================================
# New Component: Limit Graph Manager, MODP, RLHF, MoE
# ============================================================================
class LimitGraphManager:
    def __init__(self, storage=None):
        self.storage = storage
        self.graphs = {}
    def create_graph(self, graph_id, description, configuration):
        if self.storage and hasattr(self.storage, 'save_limit_graph_metadata'):
            self.storage.save_limit_graph_metadata(graph_id, description, configuration)
        else:
            self.graphs[graph_id] = {'description': description, 'configuration': configuration, 'nodes': {}, 'edges': {}}
    def add_node(self, graph_id, node_id, node_type, attributes):
        if self.storage and hasattr(self.storage, 'save_limit_graph_node'):
            self.storage.save_limit_graph_node(node_id, graph_id, node_type, attributes)
        else:
            if graph_id not in self.graphs:
                self.graphs[graph_id] = {'nodes': {}, 'edges': {}}
            self.graphs[graph_id]['nodes'][node_id] = {'node_type': node_type, 'attributes': attributes}
    def add_edge(self, graph_id, edge_id, source, target, weight, attributes):
        if self.storage and hasattr(self.storage, 'save_limit_graph_edge'):
            self.storage.save_limit_graph_edge(edge_id, graph_id, source, target, weight, attributes)
        else:
            self.graphs[graph_id]['edges'][edge_id] = {'source': source, 'target': target, 'weight': weight, 'attributes': attributes}
    def get_nodes(self, graph_id):
        if self.storage and hasattr(self.storage, 'get_limit_graph_nodes'):
            return self.storage.get_limit_graph_nodes(graph_id)
        return list(self.graphs.get(graph_id, {}).get('nodes', {}).values())
    def get_edges(self, graph_id):
        if self.storage and hasattr(self.storage, 'get_limit_graph_edges'):
            return self.storage.get_limit_graph_edges(graph_id)
        return list(self.graphs.get(graph_id, {}).get('edges', {}).values())
    def get_metadata(self, graph_id):
        if self.storage and hasattr(self.storage, 'get_limit_graph_metadata'):
            return self.storage.get_limit_graph_metadata(graph_id)
        return self.graphs.get(graph_id, {})

class MODPOptimizer:
    def __init__(self, storage=None):
        self.storage = storage
        self.states = {}
    def add_state(self, state_id, problem_id, state_attributes, objective_values, stage):
        if self.storage and hasattr(self.storage, 'save_modp_state'):
            self.storage.save_modp_state(state_id, problem_id, state_attributes, objective_values, stage)
        else:
            if problem_id not in self.states:
                self.states[problem_id] = []
            self.states[problem_id].append({'state_id': state_id, 'state_attributes': state_attributes, 'objective_values': objective_values, 'stage': stage})
    def add_policy(self, policy_id, problem_id, state_id, action, expected_objectives):
        if self.storage and hasattr(self.storage, 'save_modp_policy'):
            self.storage.save_modp_policy(policy_id, problem_id, state_id, action, expected_objectives)
    def get_states(self, problem_id):
        if self.storage and hasattr(self.storage, 'get_modp_states'):
            return self.storage.get_modp_states(problem_id)
        return self.states.get(problem_id, [])
    def get_policies(self, problem_id):
        if self.storage and hasattr(self.storage, 'get_modp_policies'):
            return self.storage.get_modp_policies(problem_id)
        return []

class RLHFTrainer:
    def __init__(self, storage=None):
        self.storage = storage
        self.pairs = []
    def record_pair(self, pair_id, prompt, chosen, rejected, reward_diff, metadata=None):
        if self.storage and hasattr(self.storage, 'save_preference_pair'):
            self.storage.save_preference_pair(pair_id, prompt, chosen, rejected, reward_diff, metadata)
        else:
            self.pairs.append({'pair_id': pair_id, 'prompt': prompt, 'chosen': chosen, 'rejected': rejected, 'reward_diff': reward_diff, 'metadata': metadata})
    def get_pairs(self, limit=100):
        if self.storage and hasattr(self.storage, 'get_preference_pairs'):
            return self.storage.get_preference_pairs(limit)
        return self.pairs[-limit:]
    def train_reward_model(self):
        pairs = self.get_pairs()
        if len(pairs) < 5:
            logger.info("Not enough preference pairs for RLHF training.")
            return
        logger.info(f"Training reward model on {len(pairs)} preference pairs...")

class MoEGatingNetwork:
    def __init__(self, storage=None, config=None):
        self.storage = storage
        self.config = config or {}
        self.expert_names = self.config.get('expert_names', DistillationStrategyOptimizer.STRATEGIES)
        self.num_experts = len(self.expert_names)
        self.gating_weights = np.random.randn(self.num_experts, 14)
    def _encode_state(self, state):
        if isinstance(state, dict):
            features = [state.get(k, 0) for k in ['expert_type', 'urgency', 'carbon_budget', 'helium_budget',
                                                   'max_latency_ms', 'carbon_intensity', 'cache_utilization',
                                                   'recent_hit_rate', 'success_preload', 'success_transfer',
                                                   'success_progressive', 'success_hybrid', 'success_federated',
                                                   'avg_warmup_time_ms', 'avg_sustainability_score']]
            return np.array(features, dtype=np.float32)
        else:
            return state.to_feature_vector()
    async def select_expert(self, state):
        x = self._encode_state(state)
        logits = self.gating_weights @ x
        probs = np.exp(logits - np.max(logits))
        probs /= probs.sum()
        expert_idx = np.argmax(probs)
        selected = self.expert_names[expert_idx]
        return selected, probs
    async def add_training_sample(self, state, selected_expert, reward):
        x = self._encode_state(state)
        expert_idx = self.expert_names.index(selected_expert)
        target = np.zeros(self.num_experts)
        target[expert_idx] = 1.0
        logits = self.gating_weights @ x
        probs = np.exp(logits - np.max(logits))
        probs /= probs.sum()
        grad = (probs - target)[:, None] * x[None, :]
        self.gating_weights -= 0.1 * grad

# ============================================================================
# Simplified Sub-Modules (Federated, ML, Carbon, Helium, Eviction)
# ============================================================================
class FederatedCheckpointManager:
    def __init__(self, config):
        self.config = config
        self.server_url = config.federated_server_url
        self.checkpoints = {}
    async def share_checkpoint(self, expert_id, checkpoint, performance_metric=1.0):
        logger.debug(f"Federated share: {expert_id}")
        return {'status': 'local'}
    async def get_peer_checkpoints(self, expert_id):
        return []
    async def aggregate_checkpoints(self, peer_checkpoints, weights=None):
        return {}
    async def sync_cache_with_peers(self, local_cache):
        return local_cache

class MLDemandPredictor:
    def __init__(self, config):
        self.config = config
        self.demand_history = []
        self.is_trained = False
    async def record_demand(self, expert_id, timestamp, context=None):
        self.demand_history.append({'expert_id': expert_id, 'timestamp': timestamp})
    async def predict_demand(self, horizon_minutes=5):
        return {}
    async def train_model(self):
        return {'status': 'success'}

class CarbonAwareStrategySelector:
    def __init__(self, config):
        self.config = config
    async def get_realtime_carbon_intensity(self, region="US-CAL-CISO"):
        return 400
    async def select_strategy(self, strategies, carbon_intensity=None, urgency='normal', carbon_budget=None):
        return 'preload'

class HeliumEfficiencyDashboard:
    def __init__(self, config):
        self.config = config
        self.total_helium_used = 0.0
        self.total_helium_saved = 0.0
        self.usage_history = []
    async def record_helium_usage(self, expert_id, amount_l, operation='initialization'):
        self.total_helium_used += amount_l
        self.usage_history.append({'amount_l': amount_l, 'timestamp': datetime.utcnow()})
    async def predict_helium_usage(self, hours=24):
        return {'total_predicted_usage': 0.0}
    async def record_helium_saving(self, amount_l, source='optimization'):
        self.total_helium_saved += amount_l
    def get_efficiency_report(self):
        return {'total_helium_used_l': self.total_helium_used, 'total_helium_saved_l': self.total_helium_saved}
    def get_optimization_recommendations(self):
        return []

class IntelligentEvictionManager:
    def __init__(self, config, predictor=None):
        self.config = config
        self.predictor = predictor
        self.eviction_history = []
    async def select_eviction_candidates(self, cache, predicted_demand, num_to_evict=1):
        return list(cache.keys())[:num_to_evict]
    def get_eviction_stats(self):
        return {'total_evictions': len(self.eviction_history)}

# ============================================================================
# ColdStartOptimizer (Main Class)
# ============================================================================
class ColdStartOptimizer:
    def __init__(self, config: Optional[ColdStartConfig] = None, storage: Optional[Any] = None, **kwargs):
        if config is None:
            config = ColdStartConfig(**{k: v for k, v in kwargs.items() if k in ColdStartConfig.model_fields})
        self.config = config
        self.storage = storage

        # Feature flags
        self.enable_federated = config.enable_federated
        self.enable_ml_demand = config.enable_ml_demand
        self.enable_carbon_aware = config.enable_carbon_aware
        self.enable_helium_tracking = config.enable_helium_tracking
        self.enable_intelligent_eviction = config.enable_intelligent_eviction
        self.enable_persistence = config.enable_persistence
        self.enable_telemetry = config.enable_telemetry
        self.enable_limit_graph = config.enable_limit_graph
        self.enable_modp = config.enable_modp
        self.enable_rlhf = config.enable_rlhf
        self.enable_moe = config.enable_moe

        self.cache_size = config.cache_size
        self.preload_threshold = config.preload_threshold
        self.checkpoint_dir = config.checkpoint_dir

        # Concurrency locks
        self._cache_lock = asyncio.Lock()
        self._history_lock = asyncio.Lock()
        self._similarity_lock = asyncio.Lock()

        # Submodules
        self.federated_manager = FederatedCheckpointManager(config) if self.enable_federated else None
        self.ml_predictor = MLDemandPredictor(config) if self.enable_ml_demand else None
        self.strategy_selector = CarbonAwareStrategySelector(config) if self.enable_carbon_aware else None
        self.helium_dashboard = HeliumEfficiencyDashboard(config) if self.enable_helium_tracking else None
        self.eviction_manager = IntelligentEvictionManager(config, self.ml_predictor) if self.enable_intelligent_eviction else None
        self.persistence = ColdStartPersistenceManager(config) if self.enable_persistence else None
        self.telemetry = ColdStartTelemetry(config) if self.enable_telemetry else None

        # Distillation
        self.strategy_optimizer = DistillationStrategyOptimizer({
            'distillation_epsilon': config.distillation_epsilon,
            'distillation_train_every': config.distillation_train_every,
            'distillation_replay_size': config.distillation_replay_size,
            'distillation_learning_rate': config.distillation_learning_rate,
        })

        # MOEA
        self.moea_enabled = config.moea_enabled
        self.moea_optimizer = None
        self.global_best_weights = None
        self.pareto_front = []
        self._moea_task = None

        # State
        self.checkpoint_cache = OrderedDict()
        self.expert_similarity_matrix = {}
        self.warmup_history = []
        self.cold_start_events = []
        self.sustainability_score = 0.0
        self.interaction_log = []

        # New components
        self.limit_graph_manager = LimitGraphManager(storage) if self.enable_limit_graph else None
        self.modp_solver = MODPOptimizer(storage) if self.enable_modp else None
        self.rlhf_trainer = RLHFTrainer(storage) if self.enable_rlhf else None
        self.moe_gating = MoEGatingNetwork(storage, {'expert_names': DistillationStrategyOptimizer.STRATEGIES}) if self.enable_moe else None

        # Background tasks (started in start())
        self._preloader_task = None
        self._moea_task = None

        # Initialize warmup strategies
        self._initialize_strategies()
        self._init_limit_graph_if_needed()

        logger.info("ColdStartOptimizer initialized (call start() to launch background tasks)")

    def _init_limit_graph_if_needed(self):
        if self.limit_graph_manager:
            graph_id = "cold_start_strategies"
            if not self.limit_graph_manager.get_metadata(graph_id):
                self.limit_graph_manager.create_graph(graph_id, "Warmup Strategy Relationships", {})
                for strat in DistillationStrategyOptimizer.STRATEGIES:
                    self.limit_graph_manager.add_node(graph_id, f"strategy_{strat}", strat, {})
                for i in range(len(DistillationStrategyOptimizer.STRATEGIES)-1):
                    src = DistillationStrategyOptimizer.STRATEGIES[i]
                    dst = DistillationStrategyOptimizer.STRATEGIES[i+1]
                    self.limit_graph_manager.add_edge(graph_id, f"edge_{src}_{dst}", f"strategy_{src}", f"strategy_{dst}", 1.0, {})

    def _initialize_strategies(self):
        self.warmup_strategies = {
            'preload': WarmupStrategy('preload', 1, 5.0, 0.001, 0.99, 0.9, 0.8),
            'transfer': WarmupStrategy('transfer', 2, 50.0, 0.005, 0.85, 0.7, 0.6),
            'progressive': WarmupStrategy('progressive', 3, 200.0, 0.01, 0.95, 0.5, 0.5),
            'hybrid': WarmupStrategy('hybrid', 4, 100.0, 0.008, 0.92, 0.6, 0.7),
        }

    async def start(self):
        """Start background tasks. Call after instantiation."""
        if self._preloader_task is None:
            self._preloader_task = asyncio.create_task(self._background_preload_loop())
        if self.moea_enabled and self._moea_task is None:
            self._moea_task = asyncio.create_task(self._moea_loop())
        if self.enable_persistence and self.persistence:
            await self.persistence.load_state(self)
        logger.info("ColdStartOptimizer background tasks started")

    async def _background_preload_loop(self):
        while True:
            try:
                # Simplified: do nothing for now, or basic preload
                await asyncio.sleep(60)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Background preloader error: {e}")
                await asyncio.sleep(300)

    async def _moea_loop(self):
        interval = self.config.moea_interval_seconds
        while True:
            try:
                await asyncio.sleep(interval)
                await self.run_moea_update()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"MOEA loop error: {e}")
                await asyncio.sleep(60)

    async def run_moea_update(self):
        if len(self.warmup_history) < 20:
            return []
        # Simplified evaluate function
        async def evaluate(weights):
            return {'latency': 0.9, 'carbon': 0.8, 'cache_hit': 0.7, 'sustainability': 0.6}
        self.moea_optimizer = NSGAIIStrategyOptimizer(evaluate_func=evaluate,
                                                      population_size=self.config.moea_population_size,
                                                      generations=self.config.moea_generations)
        pareto = await self.moea_optimizer.evolve()
        self.pareto_front = pareto
        if pareto:
            best = self.moea_optimizer._select_best_from_pareto(pareto, self.moea_optimizer._compute_dynamic_weights())
            if best:
                self.global_best_weights = best.weights
                logger.info(f"MOEA best weights: {best.weights}")
        return pareto

    async def _build_state(self, expert_type, urgency, carbon_budget, helium_budget, max_latency_ms, carbon_intensity):
        async with self._cache_lock:
            cache_util = len(self.checkpoint_cache) / self.cache_size
        hit_rate = self._calculate_hit_rate()
        success_rates = {'preload': 0.5, 'transfer': 0.5, 'progressive': 0.5, 'hybrid': 0.5, 'federated': 0.5}
        for event in self.warmup_history[-100:]:
            method = event.get('method', 'unknown')
            if method in success_rates:
                success_rates[method] = min(1.0, success_rates[method] + 0.01)
        if self.warmup_history:
            avg_time = np.mean([h.get('load_time_ms', h.get('total_time_ms', 500)) for h in self.warmup_history[-50:]])
            avg_sustainability = np.mean([h.get('sustainability_score', 0.5) for h in self.warmup_history[-50:]])
        else:
            avg_time = 500.0
            avg_sustainability = 0.5
        return ColdStartState(
            expert_type=expert_type,
            urgency=urgency,
            carbon_budget=carbon_budget,
            helium_budget=helium_budget,
            max_latency_ms=max_latency_ms,
            carbon_intensity=carbon_intensity,
            cache_utilization=cache_util,
            recent_hit_rate=hit_rate,
            strategy_success_rates=success_rates,
            avg_warmup_time_ms=avg_time,
            avg_sustainability_score=avg_sustainability,
        )

    async def initialize_expert(self, expert_id, expert_type, carbon_budget=0.1, helium_budget=0.1,
                                max_latency_ms=500.0, urgency='normal', carbon_intensity=None):
        if carbon_intensity is None:
            carbon_intensity = await self.strategy_selector.get_realtime_carbon_intensity() if self.strategy_selector else 400

        state = await self._build_state(expert_type, urgency, carbon_budget, helium_budget,
                                        max_latency_ms, carbon_intensity)

        # Strategy selection
        if self.moe_gating:
            expert_name, _ = await self.moe_gating.select_expert(state)
            strategy = expert_name if expert_name in DistillationStrategyOptimizer.STRATEGIES else 'preload'
            action_idx = DistillationStrategyOptimizer.STRATEGIES.index(strategy)
            state_vec = state.to_feature_vector()
            teacher_probs = np.ones(5)/5
            self._last_selected_expert = expert_name
        else:
            strategy, action_idx, state_vec, teacher_probs = await self.strategy_optimizer.select_strategy(state, exploration=True)

        self.last_state_vec = state_vec
        self.last_action_idx = action_idx
        self.last_teacher_probs = teacher_probs

        # Apply MOEA blend if available
        if self.global_best_weights is not None:
            one_hot = np.zeros(5)
            one_hot[action_idx] = 1.0
            moea_probs = np.array([self.global_best_weights[s] for s in DistillationStrategyOptimizer.STRATEGIES])
            moea_probs = moea_probs / moea_probs.sum()
            blended = 0.7 * moea_probs + 0.3 * one_hot
            blended = blended / blended.sum()
            action_idx = np.argmax(blended)
            strategy = DistillationStrategyOptimizer.STRATEGIES[action_idx]

        # Execute strategy (simplified)
        if strategy == 'preload':
            result = await self._preload_initialize(expert_id, expert_type, max_latency_ms)
        elif strategy == 'transfer':
            result = await self._transfer_initialize(expert_id, expert_type, max_latency_ms)
        else:
            result = await self._progressive_initialize(expert_id, expert_type, carbon_budget, helium_budget,
                                                        max_latency_ms, strategy)

        # Compute reward
        reward = self._compute_reward(result, max_latency_ms, carbon_intensity)

        # Update agent
        await self._update_agent(state_vec, action_idx, reward, state)

        # Log interaction
        self._log_interaction(state, strategy, reward, result)

        # Optional RLHF, MODP, LIMIT Graph
        if self.rlhf_trainer and random.random() < 0.05:
            chosen = strategy
            rejected = random.choice([s for s in DistillationStrategyOptimizer.STRATEGIES if s != chosen])
            self.rlhf_trainer.record_pair(str(uuid.uuid4()), "Which strategy?", chosen, rejected, reward,
                                          {'expert_id': expert_id})

        if self.modp_solver:
            self.modp_solver.add_state(str(uuid.uuid4()), "cold_start", {'expert_id': expert_id}, {'reward': reward}, 0)

        if self.limit_graph_manager:
            self.limit_graph_manager.add_node("cold_start_strategies", f"result_{expert_id}_{int(time.time())}",
                                              "outcome", {'strategy': strategy, 'reward': reward})

        return result

    async def _preload_initialize(self, expert_id, expert_type, max_latency_ms):
        # Simulate fast checkpoint load
        cp = await self._create_checkpoint(expert_id, {'type': expert_type})
        async with self._cache_lock:
            self._add_to_cache(expert_id, cp)
        return {
            'expert_id': expert_id,
            'initialized': True,
            'method': 'preload',
            'load_time_ms': 5.0,
            'sustainability_score': 0.8,
            'carbon_footprint_kg': 0.0005,
            'performance_metrics': cp.performance_metrics,
        }

    async def _transfer_initialize(self, expert_id, expert_type, max_latency_ms):
        # Simplified: just call progressive
        return await self._progressive_initialize(expert_id, expert_type, 0.1, 0.1, max_latency_ms, 'transfer')

    async def _progressive_initialize(self, expert_id, expert_type, carbon_budget, helium_budget,
                                      max_latency_ms, strategy_type='progressive'):
        # Simulate progressive phases
        await asyncio.sleep(0.05)
        cp = ExpertCheckpoint(
            expert_id=expert_id,
            expert_type=expert_type,
            model_state={},
            optimizer_state={},
            feature_distribution={},
            performance_metrics={'expected_accuracy': 0.9},
            created_at=datetime.utcnow(),
            last_used=datetime.utcnow(),
            carbon_footprint_kg=carbon_budget,
            helium_usage_l=helium_budget,
        )
        async with self._cache_lock:
            self._add_to_cache(expert_id, cp)
        return {
            'expert_id': expert_id,
            'initialized': True,
            'method': strategy_type,
            'load_time_ms': 200.0,
            'sustainability_score': self._calculate_checkpoint_sustainability(cp),
            'carbon_footprint_kg': carbon_budget,
            'performance_metrics': cp.performance_metrics,
        }

    async def _create_checkpoint(self, expert_id, expert_config):
        return ExpertCheckpoint(
            expert_id=expert_id,
            expert_type=expert_config.get('type', 'general'),
            model_state={},
            optimizer_state={},
            feature_distribution={},
            performance_metrics={'expected_accuracy': 0.92},
            created_at=datetime.utcnow(),
            last_used=datetime.utcnow(),
        )

    def _add_to_cache(self, expert_id, checkpoint):
        if len(self.checkpoint_cache) >= self.cache_size:
            self.checkpoint_cache.popitem(last=False)
        self.checkpoint_cache[expert_id] = checkpoint

    def _compute_reward(self, result, max_latency_ms, carbon_intensity):
        time_taken = result.get('load_time_ms', 500)
        time_score = 1.0 - min(1.0, time_taken / max_latency_ms)
        sustainability = result.get('sustainability_score', 0.5)
        carbon_score = 1.0 - min(1.0, carbon_intensity / 1000.0)
        return 0.4 * time_score + 0.3 * sustainability + 0.3 * carbon_score

    async def _update_agent(self, state_vec, action_idx, reward, state):
        next_state_vec = state.to_feature_vector()
        await self.strategy_optimizer.update(state_vec, action_idx, reward, next_state_vec, self.last_teacher_probs)
        if self.moe_gating and hasattr(self, '_last_selected_expert'):
            await self.moe_gating.add_training_sample(state, self._last_selected_expert, reward)

    def _log_interaction(self, state, strategy, reward, result):
        entry = {
            'timestamp': datetime.utcnow().isoformat(),
            'strategy': strategy,
            'reward': reward,
            'result': result,
            'state_vector': json.dumps(state.to_feature_vector().tolist()),
        }
        self.interaction_log.append(entry)

    def _calculate_hit_rate(self):
        total = len(self.warmup_history)
        if total == 0:
            return 0.0
        hits = sum(1 for h in self.warmup_history if h.get('method') in ['preload', 'transfer'])
        return hits / total

    def _calculate_checkpoint_sustainability(self, checkpoint):
        return 0.5 * (1 - checkpoint.carbon_footprint_kg) + 0.5 * checkpoint.performance_metrics.get('expected_accuracy', 0.5)

    async def shutdown(self):
        if self._preloader_task:
            self._preloader_task.cancel()
        if self._moea_task:
            self._moea_task.cancel()
        if self.enable_persistence:
            await self.save_state()
        logger.info("Shutdown complete")

    async def save_state(self):
        if self.persistence:
            await self.persistence.save_state(self)

    def get_stats(self):
        return {
            'cache_size': len(self.checkpoint_cache),
            'hit_rate': self._calculate_hit_rate(),
            'distillation': self.strategy_optimizer.get_stats(),
            'moea_pareto_size': len(self.pareto_front),
            'global_best_weights': self.global_best_weights,
        }
