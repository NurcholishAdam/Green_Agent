"""
Enhanced Periodic Updater for Green Agent v2.2.0
=================================================
Celery tasks for periodic updates of sustainability data with adaptive scheduling
via Multi‑Teacher On‑Policy Distillation, Multi‑Objective Evolutionary Optimization (NSGA‑II),
and additional LIMIT Graph, MODP, RLHF, and MoE components.

Enhancements over v2.1.0:
- Added LIMIT Graph manager for task dependency modelling.
- Added MODP solver wrapper for storing decision states/policies.
- Added RLHF trainer for human preference collection on scheduling decisions.
- Added MoE gating network to blend experts (update_now, skip).
- Added NSGA‑II optimizer to evolve global action weights (bio‑inspired).
- New configuration flags for enabling/disabling each component.
- Integrated with central Storage (optional) for new data persistence.

All previous features (Celery tasks, retries, metrics, distillation components) retained.
"""

import asyncio
import logging
import os
import time
from typing import List, Optional, Dict, Any, Tuple, Union
from datetime import datetime, timedelta
import random
import numpy as np
from abc import ABC, abstractmethod
from collections import deque
import json
import pickle
import pandas as pd
from pathlib import Path
from dataclasses import dataclass
import copy
import uuid
import hashlib

from celery import Celery
from celery.signals import task_failure, task_success

# ---------- Pydantic ----------
from pydantic import BaseSettings, Field

# ---------- Prometheus ----------
try:
    from prometheus_client import Counter, Histogram, Gauge
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

# ---------- scikit-learn ----------
try:
    from sklearn.ensemble import RandomForestClassifier
    from sklearn.preprocessing import LabelEncoder
    SKLEARN_ML = True
except ImportError:
    SKLEARN_ML = False

# ---------- Local imports ----------
from ..cache.cache_manager import CacheManager
from ..data_integration.carbon_intensity import CarbonIntensityFetcher
from ..data_integration.material_footprint import MaterialFootprintUpdater
from ..data_integration.helium_collector import HeliumCollector

# Optional project imports
try:
    from ..schemas.feedback_event import FeedbackEvent
except ImportError:
    FeedbackEvent = None

try:
    from ..async_message_queue import AsyncMessageQueue
except ImportError:
    AsyncMessageQueue = None

# Optional central storage
try:
    from ...storage import Storage  # adjust path if needed
    CENTRAL_STORAGE_AVAILABLE = True
except ImportError:
    CENTRAL_STORAGE_AVAILABLE = False
    Storage = None

# ============================================================================
# Configuration
# ============================================================================

class UpdaterConfig(BaseSettings):
    """Configuration for periodic updater and adaptive scheduler."""
    redis_url: str = "redis://localhost:6379/0"
    regions: List[str] = Field(default_factory=lambda: ["us-east", "us-west", "eu-west", "eu-north", "asia-east", "asia-southeast"])
    helium_snapshot_url: str = "https://example.com/helium_snapshot.parquet"
    helium_snapshot_path: str = "./helium_snapshot.parquet"

    # Distillation
    distillation_epsilon: float = 0.1
    distillation_train_every: int = 10
    distillation_replay_size: int = 2000
    distillation_learning_rate: float = 0.01
    distill_weight: float = 0.7
    rl_weight: float = 0.3
    q_learning_rate: float = 0.1

    # MOEA parameters
    moea_enabled: bool = True
    moea_interval_seconds: int = 300
    moea_population_size: int = 20
    moea_generations: int = 10
    moea_mutation_rate: float = 0.2
    moea_crossover_rate: float = 0.8
    moea_tournament_size: int = 3
    moea_objective_weights: Optional[Dict[str, float]] = Field(
        default_factory=lambda: {
            'update_quality': 0.5,
            'resource_savings': 0.3,
            'timeliness': 0.2,
        }
    )
    moea_dynamic_weights: bool = True
    moea_pareto_path: str = "./updater_moea_pareto.json"

    # NEW v2.2.0 flags
    enable_limit_graph: bool = True
    enable_modp: bool = True
    enable_rlhf: bool = True
    enable_moe: bool = True
    moe_expert_count: int = Field(2, ge=2)

    # Persistence paths
    q_weights_path: str = "./update_q_weights.json"
    interaction_logs_path: str = "./update_interactions.csv"
    historical_model_path: str = "./update_historical_model.pkl"

    # Scheduler
    scheduler_interval_seconds: int = 900  # 15 minutes
    enable_message_queue: bool = False

    class Config:
        env_prefix = "UPDATER_"
        case_sensitive = False

config = UpdaterConfig()

# Celery app
app = Celery('green_agent', broker=config.redis_url)
app.conf.update(
    task_serializer='json',
    result_serializer='json',
    accept_content=['json'],
    timezone='UTC',
    enable_utc=True,
    task_track_started=True,
    task_time_limit=600,
    task_soft_time_limit=540,
    task_acks_late=True,
    task_reject_on_worker_lost=True,
    task_retry_backoff_max=600,
)

# Prometheus metrics
if PROMETHEUS_AVAILABLE:
    task_metrics = {
        'carbon_success': Counter('carbon_update_success_total', 'Carbon update success count'),
        'carbon_failure': Counter('carbon_update_failure_total', 'Carbon update failure count'),
        'material_success': Counter('material_update_success_total', 'Material update success count'),
        'material_failure': Counter('material_update_failure_total', 'Material update failure count'),
        'helium_success': Counter('helium_update_success_total', 'Helium update success count'),
        'helium_failure': Counter('helium_update_failure_total', 'Helium update failure count'),
        'task_duration': Histogram('periodic_task_duration_seconds', 'Task duration', ['task_name']),
        'update_action': Counter('update_action_selected', 'Action selected by scheduler', ['action']),
        'update_reward': Histogram('update_reward', 'Reward per update decision'),
        'moea_pareto_front': Gauge('updater_moea_pareto_front', 'MOEA Pareto front size'),
    }
else:
    task_metrics = {}

# ============================================================================
# DISTILLATION COMPONENTS
# ============================================================================

@dataclass
class UpdateState:
    """State for the distillation agent (expanded)."""
    hours_since_last_update: float
    hour_of_day: float
    day_of_week: float
    carbon_trend: float
    material_version_age_days: float
    helium_snapshot_age_days: float
    current_carbon_intensity: float
    pending_updates_count: int
    system_load: float
    last_update_success: float = 1.0
    cache_freshness: float = 1.0
    error_rate: float = 0.0

    def to_feature_vector(self) -> np.ndarray:
        """Convert to 12‑dim numeric feature vector."""
        features = [
            min(self.hours_since_last_update / 72.0, 1.0),
            self.hour_of_day / 24.0,
            self.day_of_week / 7.0,
            min(abs(self.carbon_trend) / 0.1, 1.0),
            min(self.material_version_age_days / 30.0, 1.0),
            min(self.helium_snapshot_age_days / 30.0, 1.0),
            min(self.current_carbon_intensity / 1000.0, 1.0),
            min(self.pending_updates_count / 10.0, 1.0),
            self.system_load,
            self.last_update_success,
            self.cache_freshness,
            self.error_rate,
        ]
        return np.array(features, dtype=np.float32)


class Teacher(ABC):
    @abstractmethod
    def predict(self, state: UpdateState) -> np.ndarray:
        pass

    @abstractmethod
    def confidence(self, state: UpdateState) -> float:
        pass


class UpdateRuleBasedTeacher(Teacher):
    ACTIONS = ['update_now', 'skip']

    def predict(self, state: UpdateState) -> np.ndarray:
        probs = np.ones(2) * 0.1
        if state.hours_since_last_update > 24:
            probs[0] = 0.8
        elif state.carbon_trend > 0.05:
            probs[0] = 0.7
        elif state.material_version_age_days > 14:
            probs[0] = 0.6
        elif state.system_load > 0.8:
            probs[1] = 0.8
        elif state.last_update_success < 0.5:
            probs[0] = 0.6
        else:
            probs[1] = 0.6
        return probs / probs.sum()

    def confidence(self, state: UpdateState) -> float:
        if state.hours_since_last_update > 24:
            return 0.6
        return 0.4


class UpdateHistoricalMLTeacher(Teacher):
    def __init__(self, model_path: Optional[Path] = None):
        self.model = None
        self.label_encoder = None
        self.model_path = model_path or Path(config.historical_model_path)
        if self.model_path.exists():
            try:
                with open(self.model_path, 'rb') as f:
                    self.model, self.label_encoder = pickle.load(f)
                logger.info(f"Loaded historical ML model from {self.model_path}")
            except Exception as e:
                logger.error(f"Failed to load historical model: {e}")

    def predict(self, state: UpdateState) -> np.ndarray:
        if self.model is None or self.label_encoder is None:
            return np.ones(2) / 2
        x = state.to_feature_vector().reshape(1, -1)
        probs = self.model.predict_proba(x)[0]
        return probs

    def confidence(self, state: UpdateState) -> float:
        return 0.7 if self.model is not None else 0.0

    @classmethod
    def train_from_logs(cls, log_paths: List[Path], model_path: Path,
                        state_col: str = 'state_vec', label_col: str = 'action'):
        if not SKLEARN_ML:
            logger.error("scikit-learn not available, cannot train historical model.")
            return None
        all_dfs = []
        for path in log_paths:
            if path.exists():
                df = pd.read_csv(path)
                all_dfs.append(df)
        if not all_dfs:
            logger.warning("No logs found for training.")
            return None

        df = pd.concat(all_dfs, ignore_index=True)
        if len(df) < 10:
            logger.warning("Not enough logs to train historical model.")
            return None

        def parse_state(s):
            try:
                return np.fromstring(s, sep=',')
            except:
                return None

        valid_indices = [i for i, s in enumerate(df[state_col]) if parse_state(s) is not None]
        X = np.array([parse_state(df[state_col].iloc[i]) for i in valid_indices])
        y = df[label_col].iloc[valid_indices].values

        if len(X) < 5:
            logger.warning("Too few valid samples after parsing.")
            return None

        le = LabelEncoder()
        y_enc = le.fit_transform(y)
        clf = RandomForestClassifier(n_estimators=100, random_state=42)
        clf.fit(X, y_enc)

        with open(model_path, 'wb') as f:
            pickle.dump((clf, le), f)
        logger.info(f"Trained historical model and saved to {model_path}")
        return model_path


class UpdateStatefulQTeacher(Teacher):
    def __init__(self, lr: float = 0.1, weights_path: Optional[Path] = None):
        self.lr = lr
        self.weights_path = weights_path or Path(config.q_weights_path)
        self.weights = np.zeros((12, 2))
        self._load_state()

    def _load_state(self):
        if self.weights_path.exists():
            try:
                with open(self.weights_path, 'r') as f:
                    data = json.load(f)
                self.weights = np.array(data)
                logger.info(f"Loaded Q‑teacher weights from {self.weights_path}")
            except Exception as e:
                logger.error(f"Failed to load Q‑weights: {e}")

    def _save_state(self):
        with open(self.weights_path, 'w') as f:
            json.dump(self.weights.tolist(), f, indent=2)

    def predict(self, state: UpdateState) -> np.ndarray:
        x = state.to_feature_vector()
        q = x @ self.weights
        exp_q = np.exp(q - np.max(q))
        return exp_q / exp_q.sum()

    def confidence(self, state: UpdateState) -> float:
        return 0.5

    def update(self, state: UpdateState, action: int, reward: float):
        x = state.to_feature_vector()
        q_current = np.dot(x, self.weights[:, action])
        self.weights[:, action] += self.lr * (reward - q_current) * x
        self._save_state()


class DistillationStudent:
    def __init__(self, feature_dim: int = 12, n_classes: int = 2, lr: float = 0.01):
        self.weights = np.zeros((feature_dim, n_classes))
        self.biases = np.zeros(n_classes)
        self.lr = lr
        self.n_classes = n_classes
        self.counter = 0

    def predict_proba(self, state_vector: np.ndarray, num_classes: int) -> np.ndarray:
        if num_classes != self.n_classes:
            new_weights = np.zeros((self.weights.shape[0], num_classes))
            new_biases = np.zeros(num_classes)
            min_dim = min(self.n_classes, num_classes)
            new_weights[:, :min_dim] = self.weights[:, :min_dim]
            new_biases[:min_dim] = self.biases[:min_dim]
            self.weights = new_weights
            self.biases = new_biases
            self.n_classes = num_classes
        logits = state_vector @ self.weights + self.biases
        max_logit = np.max(logits)
        exp_logits = np.exp(logits - max_logit)
        return exp_logits / exp_logits.sum()

    def update(self, state_vector: np.ndarray, teacher_probs: np.ndarray,
               reward: float, action: int, distill_weight: float = 0.7, rl_weight: float = 0.3):
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


class DistillationSchedulerOptimizer:
    """Multi‑teacher on‑policy distillation agent for update scheduling."""
    ACTIONS = ['update_now', 'skip']

    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.student = DistillationStudent(feature_dim=12, lr=config.get('distillation_learning_rate', 0.01))
        self.teachers: List[Teacher] = [
            UpdateRuleBasedTeacher(),
            UpdateHistoricalMLTeacher(model_path=config.get('historical_model_path')),
            UpdateStatefulQTeacher(lr=config.get('q_learning_rate', 0.1),
                                   weights_path=config.get('q_weights_path'))
        ]
        self.replay_buffer = ReplayBuffer(max_size=config.get('distillation_replay_size', 2000))
        self.epsilon = config.get('distillation_epsilon', 0.1)
        self.train_every = config.get('distillation_train_every', 10)
        self.counter = 0
        self.distill_weight = config.get('distill_weight', 0.7)
        self.rl_weight = config.get('rl_weight', 0.3)

    async def select_action(self, state: UpdateState, exploration: bool = True) -> Tuple[str, int, np.ndarray, np.ndarray]:
        state_vec = state.to_feature_vector()
        n = 2
        teacher_probs = np.zeros(n)
        total_conf = 0.0
        for teacher in self.teachers:
            prob = teacher.predict(state)
            conf = teacher.confidence(state)
            if len(prob) != n:
                if len(prob) < n:
                    prob = np.pad(prob, (0, n - len(prob)), 'constant')
                else:
                    prob = prob[:n]
            teacher_probs += prob * conf
            total_conf += conf
        if total_conf > 0:
            teacher_probs /= total_conf
        else:
            teacher_probs = np.ones(n) / n
        student_probs = self.student.predict_proba(state_vec, n)
        if exploration and random.random() < self.epsilon:
            action_idx = random.randint(0, n - 1)
        else:
            combined = 0.8 * student_probs + 0.2 * teacher_probs
            action_idx = np.argmax(combined)
        return self.ACTIONS[action_idx], action_idx, state_vec, teacher_probs

    async def update(self, state_vec: np.ndarray, action_idx: int, reward: float,
                     next_state_vec: np.ndarray, teacher_probs: np.ndarray):
        self.replay_buffer.push(state_vec, action_idx, reward, next_state_vec, teacher_probs)
        self.counter += 1
        if self.counter % self.train_every == 0 and len(self.replay_buffer) >= 8:
            batch = self.replay_buffer.sample(8)
            states, actions, rewards, _, teacher_probs_batch = batch
            for i in range(len(states)):
                self.student.update(states[i], teacher_probs_batch[i], rewards[i], actions[i],
                                    distill_weight=self.distill_weight, rl_weight=self.rl_weight)

    def get_stats(self) -> Dict:
        return {'student_counter': self.student.counter, 'buffer_size': len(self.replay_buffer)}


# ============================================================================
# NEW: Multi‑Objective Update Scheduler Optimizer (NSGA‑II)
# ============================================================================
@dataclass
class MOPDUpdateWeights:
    vector_id: str
    weights: Dict[str, float]
    objectives: Dict[str, float]
    scalarised_score: float = 0.0

    def to_dict(self) -> Dict[str, Any]:
        return {
            'vector_id': self.vector_id,
            'weights': self.weights,
            'objectives': self.objectives,
            'scalarised_score': self.scalarised_score,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'MOPDUpdateWeights':
        return cls(**data)


class NSGAIIUpdateOptimizer:
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
            'update_quality': 0.5,
            'resource_savings': 0.3,
            'timeliness': 0.2,
        }
        self.dynamic_weights = dynamic_weights
        self.best_individual = None
        self.best_fitness = -float('inf')
        self.pareto_front: List[MOPDUpdateWeights] = []
        self._eval_cache: Dict[Tuple[float, ...], Dict[str, float]] = {}
        self._all_points: List[MOPDUpdateWeights] = []

    def _random_individual(self) -> Dict[str, float]:
        keys = ['update_now', 'skip']
        w = {k: random.random() for k in keys}
        total = sum(w.values())
        if total > 0:
            w = {k: v / total for k, v in w.items()}
        return w

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

    def _dominates(self, a, b) -> bool:
        better_or_equal = all(a.objectives[k] >= b.objectives[k] for k in a.objectives)
        strictly_better = any(a.objectives[k] > b.objectives[k] for k in a.objectives)
        return better_or_equal and strictly_better

    def _fast_non_dominated_sort(self, points):
        fronts = []
        domination_count = {id(p): 0 for p in points}
        dominated_solutions = {id(p): [] for p in points}
        for i, p in enumerate(points):
            for j, q in enumerate(points):
                if i == j:
                    continue
                if self._dominates(p, q):
                    dominated_solutions[id(p)].append(q)
                elif self._dominates(q, p):
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
        obj_keys = list(front[0].objectives.keys())
        for obj in obj_keys:
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
        ind_to_point = {id(ind): point for ind, point in zip(population, self._all_points)}
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
            score = sum(weights[k] * (p.objectives[k] - min_vals[k]) / ranges[k] for k in obj_keys)
            p.scalarised_score = score
            if score > best_score:
                best_score = score
                best = p
        return best

    async def evolve(self):
        population = [self._random_individual() for _ in range(self.population_size)]
        eval_tasks = [self.evaluate_func(ind) for ind in population]
        eval_results = await asyncio.gather(*eval_tasks)
        points = []
        for ind, obj in zip(population, eval_results):
            point = MOPDUpdateWeights(vector_id=str(uuid.uuid4()), weights=ind, objectives=obj)
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
                child = self._crossover(parent1, parent2) if random.random() < self.crossover_rate else copy.deepcopy(parent1)
                child = self._mutate(child)
                offspring.append(child)
            child_tasks = [self.evaluate_func(ind) for ind in offspring]
            child_results = await asyncio.gather(*child_tasks)
            child_points = []
            for ind, obj in zip(offspring, child_results):
                point = MOPDUpdateWeights(vector_id=str(uuid.uuid4()), weights=ind, objectives=obj)
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
            new_pop = []
            new_points = []
            for front in fronts:
                if len(new_pop) + len(front) <= self.population_size:
                    for p in front:
                        for ind, p2 in zip(population, points):
                            if p2 is p:
                                new_pop.append(ind)
                                new_points.append(p)
                                break
                else:
                    crowding = self._crowding_distance(front)
                    sorted_front = sorted(front, key=lambda x: crowding.get(id(x), 0), reverse=True)
                    for p in sorted_front:
                        if len(new_pop) >= self.population_size:
                            break
                        for ind, p2 in zip(population, points):
                            if p2 is p:
                                new_pop.append(ind)
                                new_points.append(p)
                                break
            population = new_pop[:self.population_size]
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
# NEW: LIMIT Graph Manager
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
            if graph_id not in self.graphs:
                self.graphs[graph_id] = {'nodes': {}, 'edges': {}}
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


# ============================================================================
# NEW: MODP Optimizer
# ============================================================================
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
            self.states[problem_id].append({
                'state_id': state_id, 'state_attributes': state_attributes,
                'objective_values': objective_values, 'stage': stage
            })

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


# ============================================================================
# NEW: RLHF Trainer
# ============================================================================
class RLHFTrainer:
    def __init__(self, storage=None):
        self.storage = storage
        self.pairs = []

    def record_pair(self, pair_id, prompt, chosen, rejected, reward_diff, metadata=None):
        if self.storage and hasattr(self.storage, 'save_preference_pair'):
            self.storage.save_preference_pair(pair_id, prompt, chosen, rejected, reward_diff, metadata)
        else:
            self.pairs.append({'pair_id': pair_id, 'prompt': prompt, 'chosen': chosen,
                                'rejected': rejected, 'reward_diff': reward_diff, 'metadata': metadata})

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


# ============================================================================
# NEW: MoE Gating Network
# ============================================================================
class MoEGatingNetwork:
    def __init__(self, storage=None, config=None):
        self.storage = storage
        self.config = config or {}
        self.expert_names = self.config.get('expert_names', ['update_now', 'skip'])
        self.num_experts = len(self.expert_names)
        self.gating_weights = np.random.randn(self.num_experts, 12)

    def _encode_state(self, state):
        if isinstance(state, dict):
            features = [
                min(state.get('hours_since_last_update', 0) / 72.0, 1.0),
                state.get('hour_of_day', 0) / 24.0,
                state.get('day_of_week', 0) / 7.0,
                min(abs(state.get('carbon_trend', 0)) / 0.1, 1.0),
                min(state.get('material_version_age_days', 0) / 30.0, 1.0),
                min(state.get('helium_snapshot_age_days', 0) / 30.0, 1.0),
                min(state.get('current_carbon_intensity', 0) / 1000.0, 1.0),
                min(state.get('pending_updates_count', 0) / 10.0, 1.0),
                state.get('system_load', 0),
                state.get('last_update_success', 1.0),
                state.get('cache_freshness', 1.0),
                state.get('error_rate', 0.0),
            ]
        else:
            features = state.to_feature_vector()
        return np.array(features, dtype=np.float32)

    async def select_expert(self, state):
        x = self._encode_state(state)
        logits = self.gating_weights @ x
        probs = np.exp(logits - np.max(logits))
        probs /= probs.sum()
        expert_idx = np.argmax(probs)
        selected = self.expert_names[expert_idx]
        if self.storage and hasattr(self.storage, 'log_routing_decision'):
            sample_id = hashlib.sha256(str(state).encode()).hexdigest()[:16]
            self.storage.log_routing_decision(str(uuid.uuid4()), sample_id, selected, float(probs[expert_idx]))
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
# ADAPTIVE SCHEDULER
# ============================================================================
class AdaptiveScheduler:
    def __init__(self, config: Optional[Dict[str, Any]] = None, storage: Optional[Storage] = None):
        self.config = config or {}
        self.storage = storage
        self.scheduler_optimizer = DistillationSchedulerOptimizer({
            'distillation_epsilon': self.config.get('distillation_epsilon', 0.1),
            'distillation_train_every': self.config.get('distillation_train_every', 10),
            'distillation_replay_size': self.config.get('distillation_replay_size', 2000),
            'distillation_learning_rate': self.config.get('distillation_learning_rate', 0.01),
            'distill_weight': self.config.get('distill_weight', 0.7),
            'rl_weight': self.config.get('rl_weight', 0.3),
            'q_learning_rate': self.config.get('q_learning_rate', 0.1),
            'q_weights_path': self.config.get('q_weights_path', config.q_weights_path),
            'historical_model_path': self.config.get('historical_model_path', config.historical_model_path),
        })

        self.interaction_log: List[Dict] = []
        self.last_state_vec: Optional[np.ndarray] = None
        self.last_action_idx: Optional[int] = None
        self.last_teacher_probs: Optional[np.ndarray] = None

        self.last_carbon_update: Optional[datetime] = None
        self.last_material_update: Optional[datetime] = None
        self.last_helium_update: Optional[datetime] = None

        self.carbon_history = deque(maxlen=100)
        self.material_version = None
        self.helium_snapshot_mtime: Optional[float] = None

        self._lock = asyncio.Lock()

        self.message_queue = None
        if self.config.get('enable_message_queue', False) and AsyncMessageQueue is not None:
            self.message_queue = AsyncMessageQueue(queue_type="asyncio")

        self.moea_enabled = self.config.get('moea_enabled', True)
        self.moea_optimizer: Optional[NSGAIIUpdateOptimizer] = None
        self.global_best_weights: Optional[Dict[str, float]] = None
        self.pareto_front: List[MOPDUpdateWeights] = []
        self._moea_task: Optional[asyncio.Task] = None

        self.limit_graph_manager = LimitGraphManager(storage) if self.config.get('enable_limit_graph', True) else None
        self.modp_solver = MODPOptimizer(storage) if self.config.get('enable_modp', True) else None
        self.rlhf_trainer = RLHFTrainer(storage) if self.config.get('enable_rlhf', True) else None
        self.moe_gating = MoEGatingNetwork(storage, {'expert_names': self.scheduler_optimizer.ACTIONS}) if self.config.get('enable_moe', True) else None

        if self.limit_graph_manager:
            self._init_limit_graph()

        if self.moea_enabled:
            self._moea_task = asyncio.create_task(self._moea_loop())

        logger.info("AdaptiveScheduler initialized (v2.2.0) with MOEA, LIMIT Graph, MODP, RLHF, MoE")

    def _init_limit_graph(self):
        graph_id = "update_tasks"
        if not self.limit_graph_manager.get_metadata(graph_id):
            self.limit_graph_manager.create_graph(graph_id, "Update Task Relationships", {})
            for task in ['carbon', 'material', 'helium']:
                self.limit_graph_manager.add_node(graph_id, f"task_{task}", task, {})
            for i in range(2):
                src = ['carbon', 'material', 'helium'][i]
                dst = ['carbon', 'material', 'helium'][i+1]
                self.limit_graph_manager.add_edge(graph_id, f"edge_{src}_{dst}", f"task_{src}", f"task_{dst}", 1.0, {})

    def _build_state(self, task_type: str) -> UpdateState:
        now = datetime.utcnow()
        if task_type == 'carbon':
            last_update = self.last_carbon_update
            hours_since = (now - last_update).total_seconds() / 3600 if last_update else 72
            carbon_trend = 0.0
            if len(self.carbon_history) > 5:
                recent = list(self.carbon_history)[-24:]
                if len(recent) > 5:
                    slope = np.polyfit(range(len(recent)), recent, 1)[0]
                    carbon_trend = slope
            current_intensity = self.carbon_history[-1] if self.carbon_history else 400
            material_age = 0
            helium_age = 0
        elif task_type == 'material':
            last_update = self.last_material_update
            hours_since = (now - last_update).total_seconds() / 3600 if last_update else 72
            carbon_trend = 0.0
            current_intensity = 0.0
            material_age = 0.0 if self.material_version else 30.0
            helium_age = 0
        else:
            last_update = self.last_helium_update
            hours_since = (now - last_update).total_seconds() / 3600 if last_update else 72
            carbon_trend = 0.0
            current_intensity = 0.0
            material_age = 0
            helium_age = (now.timestamp() - self.helium_snapshot_mtime) / (3600 * 24) if self.helium_snapshot_mtime else 30.0

        return UpdateState(
            hours_since_last_update=hours_since,
            hour_of_day=now.hour,
            day_of_week=now.weekday(),
            carbon_trend=carbon_trend,
            material_version_age_days=material_age,
            helium_snapshot_age_days=helium_age,
            current_carbon_intensity=current_intensity,
            pending_updates_count=0,
            system_load=0.5,
            last_update_success=1.0,
            cache_freshness=1.0,
            error_rate=0.0,
        )

    async def decide_and_execute(self, task_type: str) -> bool:
        async with self._lock:
            state = self._build_state(task_type)

            if self.moe_gating:
                expert_name, _ = await self.moe_gating.select_expert(state)
                action = expert_name if expert_name in self.scheduler_optimizer.ACTIONS else 'skip'
                action_idx = self.scheduler_optimizer.ACTIONS.index(action)
                state_vec = state.to_feature_vector()
                teacher_probs = np.ones(2) / 2
                self._last_selected_expert = expert_name
            else:
                action, action_idx, state_vec, teacher_probs = await self.scheduler_optimizer.select_action(state, exploration=True)
            self.last_state_vec = state_vec
            self.last_action_idx = action_idx
            self.last_teacher_probs = teacher_probs

            if PROMETHEUS_AVAILABLE:
                task_metrics['update_action'].labels(action=action).inc()

            if self.global_best_weights is not None:
                one_hot = np.zeros(2)
                one_hot[action_idx] = 1.0
                moea_probs = np.array([self.global_best_weights[a] for a in self.scheduler_optimizer.ACTIONS])
                moea_probs = moea_probs / moea_probs.sum()
                blended = 0.7 * moea_probs + 0.3 * one_hot
                blended = blended / blended.sum()
                action_idx = np.argmax(blended)
                action = self.scheduler_optimizer.ACTIONS[action_idx]

            if action == 'skip':
                logger.info(f"Skipping {task_type} update")
                reward = 0.1
                await self._record_outcome(task_type, action, reward, state_vec, action_idx, teacher_probs)
                return False

            logger.info(f"Executing {task_type} update")
            if task_type == 'carbon':
                update_carbon_intensity.delay()
            elif task_type == 'material':
                update_material_catalog.delay()
            elif task_type == 'helium':
                update_helium_snapshot.delay()
            return True

    async def _record_outcome(self, task_type, action, reward, state_vec=None, action_idx=None, teacher_probs=None):
        if state_vec is None:
            state_vec = self.last_state_vec
            action_idx = self.last_action_idx
            teacher_probs = self.last_teacher_probs

        entry = {
            'timestamp': datetime.utcnow().isoformat(),
            'task_type': task_type,
            'action': action,
            'reward': reward,
        }
        if state_vec is not None:
            entry['state_vec'] = ','.join(map(str, state_vec))
        self.interaction_log.append(entry)

        log_path = Path(config.interaction_logs_path)
        df_log = pd.DataFrame([entry])
        if log_path.exists():
            df_log.to_csv(log_path, mode='a', header=False, index=False)
        else:
            df_log.to_csv(log_path, index=False)

        if state_vec is not None and action_idx is not None:
            await self.scheduler_optimizer.update(state_vec, action_idx, reward, state_vec, teacher_probs)

        if self.moe_gating and hasattr(self, '_last_selected_expert'):
            await self.moe_gating.add_training_sample(state_vec.reshape(1,-1), self._last_selected_expert, reward)

        if self.rlhf_trainer and random.random() < 0.05:
            chosen = action
            rejected = random.choice([a for a in self.scheduler_optimizer.ACTIONS if a != chosen])
            self.rlhf_trainer.record_pair(str(uuid.uuid4()), "Which update decision?", chosen, rejected, reward, {'task_type': task_type})

        if self.modp_solver:
            self.modp_solver.add_state(str(uuid.uuid4()), "update_scheduling", {'task_type': task_type}, {'reward': reward}, 0)

        if self.limit_graph_manager:
            self.limit_graph_manager.add_node("update_tasks", f"decision_{uuid.uuid4()}", "decision", {'task_type': task_type, 'action': action, 'reward': reward})

        if FeedbackEvent and self.message_queue:
            event = FeedbackEvent(
                source="adaptive_scheduler",
                feedback_type="routing",
                task_id=task_type,
                context={"action": action},
                action={"selected_action": action, "selected_rank": 1, "confidence_score": 0.5},
                performance={"quality_score": reward, "latency_ms": 0, "energy_joules": 0,
                             "carbon_g": 0, "helium_cost": 0, "duration_ms": 0},
                adaptive_cost_value=reward,
                tags=["scheduler", "outcome", task_type, action],
            )
            await self.message_queue.publish("scheduler_events", event.to_json())

    def get_scheduler_stats(self):
        stats = self.scheduler_optimizer.get_stats()
        if self.moea_enabled:
            stats['moea'] = {'pareto_front_size': len(self.pareto_front), 'best_weights': self.global_best_weights}
        if self.limit_graph_manager:
            stats['limit_graph'] = self.limit_graph_manager.get_metadata('update_tasks')
        return stats

    async def run_moea_update(self):
        if not self.moea_enabled or len(self.interaction_log) < 20:
            return []
        async def evaluate(weights):
            action_metrics = {a: [] for a in self.scheduler_optimizer.ACTIONS}
            for entry in self.interaction_log[-200:]:
                action = entry.get('action')
                if action in action_metrics:
                    action_metrics[action].append(entry.get('reward', 0))
            objectives = {}
            for metric in ['update_quality', 'resource_savings', 'timeliness']:
                weighted = []
                for a, w in weights.items():
                    if action_metrics.get(a):
                        avg = np.mean(action_metrics[a])
                        weighted.append(w * avg)
                    else:
                        weighted.append(w * 0.5)
                objectives[metric] = sum(weighted)
            return objectives
        self.moea_optimizer = NSGAIIUpdateOptimizer(
            evaluate_func=evaluate,
            population_size=self.config.get('moea_population_size', 20),
            generations=self.config.get('moea_generations', 10),
            mutation_rate=self.config.get('moea_mutation_rate', 0.2),
            crossover_rate=self.config.get('moea_crossover_rate', 0.8),
            tournament_size=self.config.get('moea_tournament_size', 3),
            objective_weights=self.config.get('moea_objective_weights'),
            dynamic_weights=self.config.get('moea_dynamic_weights', True),
        )
        pareto = await self.moea_optimizer.evolve()
        self.pareto_front = pareto
        if pareto:
            best = self.moea_optimizer._select_best_from_pareto(pareto, self.moea_optimizer._compute_dynamic_weights())
            if best:
                self.global_best_weights = best.weights
                logger.info(f"MOEA selected best weights: {best.weights}")
                if self.modp_solver:
                    self.modp_solver.add_state(f"moea_best_{time.time()}", "update_strategy_evolution", {'weights': best.weights}, best.objectives, 1)
                if self.limit_graph_manager:
                    self.limit_graph_manager.add_node("update_tasks", f"vector_{best.vector_id}", "best_weight_vector", {'weights': best.weights})
        return pareto

    async def _moea_loop(self):
        while True:
            try:
                await asyncio.sleep(self.config.get('moea_interval_seconds', 300))
                await self.run_moea_update()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"MOEA loop error: {e}")
                await asyncio.sleep(60)

    async def get_limit_graph(self, graph_id="update_tasks"):
        if self.limit_graph_manager:
            return {'metadata': self.limit_graph_manager.get_metadata(graph_id),
                    'nodes': self.limit_graph_manager.get_nodes(graph_id),
                    'edges': self.limit_graph_manager.get_edges(graph_id)}
        return {}

    async def get_moe_experts(self):
        return self.moe_gating.expert_names if self.moe_gating else []

    async def get_rlhf_pairs(self, limit=100):
        return self.rlhf_trainer.get_pairs(limit) if self.rlhf_trainer else []

    async def record_rlhf_pair(self, pair_id, prompt, chosen, rejected, reward_diff, metadata=None):
        if self.rlhf_trainer:
            self.rlhf_trainer.record_pair(pair_id, prompt, chosen, rejected, reward_diff, metadata)


# ============================================================================
# Celery tasks
# ============================================================================
@app.task(bind=True, name='src.enhancements.tasks.periodic_updater.update_carbon_intensity',
          autoretry_for=(Exception,), retry_backoff=True, retry_backoff_max=600, max_retries=3)
def update_carbon_intensity(self):
    start_time = time.time()
    logger.info("Starting carbon intensity update", regions=config.regions)
    try:
        cache = CacheManager()
        fetcher = CarbonIntensityFetcher(cache)
        async def fetch_all():
            tasks = [fetcher.get_intensity(region) for region in config.regions]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            return results
        results = asyncio.run(fetch_all())
        failures = sum(1 for r in results if isinstance(r, Exception))
        success_count = len(config.regions) - failures
        if PROMETHEUS_AVAILABLE:
            if failures == 0:
                task_metrics['carbon_success'].inc()
            else:
                task_metrics['carbon_failure'].inc()
            task_metrics['task_duration'].labels(task_name='update_carbon_intensity').observe(time.time() - start_time)
        reward = 0.5 * (success_count / len(config.regions)) + 0.5 * (1 if failures == 0 else 0)
        reward = max(0.0, min(1.0, reward))
        global _scheduler
        if _scheduler:
            asyncio.run(_scheduler._record_outcome('carbon', 'update_now', reward))
        return {"status": "success", "regions_updated": success_count, "total": len(config.regions), "reward": reward}
    except Exception as e:
        logger.error("Carbon intensity update failed", error=str(e), exc_info=True)
        if PROMETHEUS_AVAILABLE:
            task_metrics['carbon_failure'].inc()
        global _scheduler
        if _scheduler:
            asyncio.run(_scheduler._record_outcome('carbon', 'update_now', 0.0))
        raise self.retry(exc=e)


@app.task(bind=True, name='src.enhancements.tasks.periodic_updater.update_material_catalog',
          autoretry_for=(Exception,), retry_backoff=True, retry_backoff_max=600, max_retries=3)
def update_material_catalog(self):
    start_time = time.time()
    logger.info("Starting material catalog update")
    try:
        updater = MaterialFootprintUpdater()
        async def update():
            await updater.update_catalog()
        asyncio.run(update())
        if PROMETHEUS_AVAILABLE:
            task_metrics['material_success'].inc()
            task_metrics['task_duration'].labels(task_name='update_material_catalog').observe(time.time() - start_time)
        reward = 0.8
        global _scheduler
        if _scheduler:
            asyncio.run(_scheduler._record_outcome('material', 'update_now', reward))
        return {"status": "success", "reward": reward}
    except Exception as e:
        logger.error("Material catalog update failed", error=str(e), exc_info=True)
        if PROMETHEUS_AVAILABLE:
            task_metrics['material_failure'].inc()
        global _scheduler
        if _scheduler:
            asyncio.run(_scheduler._record_outcome('material', 'update_now', 0.0))
        raise self.retry(exc=e)


@app.task(bind=True, name='src.enhancements.tasks.periodic_updater.update_helium_snapshot',
          autoretry_for=(Exception,), retry_backoff=True, retry_backoff_max=600, max_retries=3)
def update_helium_snapshot(self):
    start_time = time.time()
    logger.info("Starting helium snapshot update", url=config.helium_snapshot_url)
    try:
        import aiohttp, aiofiles
        async def download():
            async with aiohttp.ClientSession() as session:
                async with session.get(config.helium_snapshot_url) as resp:
                    if resp.status != 200:
                        raise Exception(f"Download failed with status {resp.status}")
                    os.makedirs(os.path.dirname(config.helium_snapshot_path) or '.', exist_ok=True)
                    async with aiofiles.open(config.helium_snapshot_path, 'wb') as f:
                        async for chunk in resp.content.iter_chunked(8192):
                            await f.write(chunk)
        asyncio.run(download())
        if PROMETHEUS_AVAILABLE:
            task_metrics['helium_success'].inc()
            task_metrics['task_duration'].labels(task_name='update_helium_snapshot').observe(time.time() - start_time)
        reward = 0.7
        global _scheduler
        if _scheduler:
            asyncio.run(_scheduler._record_outcome('helium', 'update_now', reward))
        return {"status": "success", "path": config.helium_snapshot_path, "reward": reward}
    except Exception as e:
        logger.error("Helium snapshot update failed", error=str(e), exc_info=True)
        if PROMETHEUS_AVAILABLE:
            task_metrics['helium_failure'].inc()
        global _scheduler
        if _scheduler:
            asyncio.run(_scheduler._record_outcome('helium', 'update_now', 0.0))
        raise self.retry(exc=e)


@app.task(name='src.enhancements.tasks.periodic_updater.run_scheduler',
          autoretry_for=(Exception,), retry_backoff=True, retry_backoff_max=600, max_retries=3)
def run_scheduler():
    logger.info("Running adaptive scheduler")
    try:
        global _scheduler
        if _scheduler is None:
            _scheduler = AdaptiveScheduler(config.dict())
        for task_type in ['carbon', 'material', 'helium']:
            asyncio.run(_scheduler.decide_and_execute(task_type))
        return {"status": "success"}
    except Exception as e:
        logger.error("Scheduler run failed", error=str(e), exc_info=True)
        raise


@task_success.connect
def task_success_handler(sender, **kwargs):
    logger.info("Task succeeded", task=sender.name)

@task_failure.connect
def task_failure_handler(sender, **kwargs):
    logger.error("Task failed", task=sender.name, exc_info=kwargs.get('einfo'))

_scheduler: Optional[AdaptiveScheduler] = None


# ============================================================================
# Offline training
# ============================================================================
def train_historical_model(log_path: Path = Path(config.interaction_logs_path),
                           model_path: Path = Path(config.historical_model_path)):
    return UpdateHistoricalMLTeacher.train_from_logs([log_path], model_path)


# ============================================================================
# Example usage
# ============================================================================
if __name__ == "__main__":
    print("Start Celery worker: celery -A src.enhancements.tasks.periodic_updater.app worker --loglevel=info")
    print("Run scheduler task: celery -A src.enhancements.tasks.periodic_updater.app call src.enhancements.tasks.periodic_updater.run_scheduler")
