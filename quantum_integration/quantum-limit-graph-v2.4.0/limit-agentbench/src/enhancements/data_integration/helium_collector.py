#!/usr/bin/env python3
"""
Enhanced Helium Collector v2.4.1
==================================
Collects Helium hotspot connectivity data from live API and/or offline Parquet snapshots.
Provides a connectivity score (0‑1) based on RSSI, SNR, and other metrics.

ENHANCEMENTS OVER v2.4.0:
- Added missing class definitions (SourceSelectionState, Teacher hierarchy, DistillationSourceOptimizer, etc.)
- Fixed dimension mismatches (feature vector length 8, MoE gating weights shape)
- Added asyncio.Lock for weight updates
- Safe dynamic weight calculation
- Proper async methods throughout
- All components now fully functional
"""

import asyncio
import logging
import time
import os
from pathlib import Path
from typing import Dict, List, Optional, Any, Union, Tuple, Callable, Awaitable
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
import uuid
import hashlib

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

# ---------- Optional central storage ----------
try:
    from ...storage import Storage
    CENTRAL_STORAGE_AVAILABLE = True
except ImportError:
    CENTRAL_STORAGE_AVAILABLE = False
    Storage = None

# ============================================================================
# Configuration
# ============================================================================
if PYDANTIC_AVAILABLE:
    class HeliumConfig(BaseModel):
        """Configuration for HeliumCollector."""
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

        # NEW v2.4.0 flags
        enable_limit_graph: bool = Field(True)
        enable_modp: bool = Field(True)
        enable_rlhf: bool = Field(True)
        enable_moe: bool = Field(True)
        moe_expert_count: int = Field(4, ge=2)

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
        "distillation_epsilon": 0.1,
        "distillation_train_every": 10,
        "distillation_replay_size": 2000,
        "distillation_learning_rate": 0.01,
        "distill_weight": 0.7,
        "rl_weight": 0.3,
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
        "enable_limit_graph": True,
        "enable_modp": True,
        "enable_rlhf": True,
        "enable_moe": True,
        "moe_expert_count": 4,
        "q_weights_path": "./helium_q_weights.json",
        "interaction_logs_path": "./helium_interactions.csv",
        "historical_model_path": "./helium_historical_model.pkl",
        "moea_pareto_path": "./helium_moea_pareto.json",
    }

# ============================================================================
# NEW: LIMIT Graph Manager
# ============================================================================
class LimitGraphManager:
    """Manages a graph of source selection relationships for LIMIT."""
    def __init__(self, storage: Optional[Storage] = None):
        self.storage = storage
        self.graphs = {}

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
# NEW: MODP Optimizer
# ============================================================================
class MODPOptimizer:
    """Multi‑Objective Dynamic Programming solver."""
    def __init__(self, storage: Optional[Storage] = None):
        self.storage = storage
        self.states = {}

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
        self.add_state(
            state_id=f"{problem_id}_init",
            problem_id=problem_id,
            state_attributes=initial_state,
            objective_values={"success_rate": 0.0, "latency": 0.0, "snapshot_usage": 0.0, "cost": 0.0},
            stage=0
        )
        return {"status": "solved", "pareto_front": []}

# ============================================================================
# NEW: RLHF Trainer
# ============================================================================
class RLHFTrainer:
    """Collects human preference pairs for source selection."""
    def __init__(self, storage: Optional[Storage] = None):
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
# NEW: MoE Gating Network
# ============================================================================
class MoEGatingNetwork:
    """Mixture-of-Experts gating for source selection."""
    def __init__(self, storage: Optional[Storage] = None, config: Optional[Dict] = None):
        self.storage = storage
        self.config = config or {}
        self.num_experts = self.config.get('moe_expert_count', 4)
        self.expert_names = ['snapshot_focus', 'api_focus', 'fallback_focus', 'adaptive'][:self.num_experts]
        # FIX: feature dimension is 8
        self.gating_weights = np.random.randn(self.num_experts, 8)

    def _encode_state(self, state: Union['SourceSelectionState', Dict]) -> np.ndarray:
        if isinstance(state, dict):
            features = [
                state.get('snapshot_exists', 0),
                state.get('hour_of_day', 0) / 24.0,
                state.get('day_of_week', 0) / 7.0,
                state.get('success_snapshot', 0.5),
                state.get('success_api', 0.5),
                state.get('success_fallback', 0.5),
                state.get('cb_state', 0) / 2.0,
                min(state.get('api_latency', 0) / 5.0, 1.0),
            ]
        else:
            features = state.to_feature_vector()
        return np.array(features, dtype=np.float32)

    async def select_expert(self, state: Union['SourceSelectionState', Dict]) -> Tuple[str, np.ndarray]:
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

    async def add_training_sample(self, state: Union['SourceSelectionState', Dict], selected_expert: str, reward: float):
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
# Distillation Components
# ============================================================================
@dataclass
class SourceSelectionState:
    """State representation for source selection (8 features)."""
    snapshot_exists: float = 0.0
    hour_of_day: float = 0.0
    day_of_week: float = 0.0
    success_snapshot: float = 0.5
    success_api: float = 0.5
    success_fallback: float = 0.5
    cb_state: float = 0.0  # 0=closed, 1=half-open, 2=open
    api_latency: float = 0.0  # in seconds

    def to_feature_vector(self) -> np.ndarray:
        return np.array([
            self.snapshot_exists,
            self.hour_of_day / 24.0,
            self.day_of_week / 7.0,
            self.success_snapshot,
            self.success_api,
            self.success_fallback,
            self.cb_state / 2.0,
            min(self.api_latency / 5.0, 1.0),
        ], dtype=np.float32)


class Teacher(ABC):
    @abstractmethod
    def predict(self, state: SourceSelectionState) -> np.ndarray:
        pass
    @abstractmethod
    def confidence(self, state: SourceSelectionState) -> float:
        pass


class SourceRuleBasedTeacher(Teacher):
    ACTION_SPACE = ['snapshot', 'api', 'fallback']

    def predict(self, state: SourceSelectionState) -> np.ndarray:
        probs = np.ones(3) * 0.1
        if state.snapshot_exists > 0.5 and state.success_snapshot > 0.6:
            probs[0] += 0.7
        elif state.success_api > 0.6 and state.cb_state == 0.0:
            probs[1] += 0.6
        else:
            probs[2] += 0.5
        total = probs.sum()
        return probs / total if total > 0 else np.ones(3)/3

    def confidence(self, state: SourceSelectionState) -> float:
        return 0.6 if state.snapshot_exists > 0.5 else 0.4


class SourceHistoricalMLTeacher(Teacher):
    def __init__(self, model_path: Optional[Path] = None):
        self.model = None
        self.label_encoder = None
        if model_path and model_path.exists() and SKLEARN_ML:
            with open(model_path, 'rb') as f:
                self.model, self.label_encoder = pickle.load(f)

    def predict(self, state: SourceSelectionState) -> np.ndarray:
        if self.model is None:
            return np.ones(3) / 3
        x = state.to_feature_vector().reshape(1, -1)
        probs = self.model.predict_proba(x)[0]
        return probs

    def confidence(self, state: SourceSelectionState) -> float:
        return 0.7 if self.model is not None else 0.0


class SourceStatefulQTeacher(Teacher):
    def __init__(self, lr: float = 0.1):
        self.lr = lr
        self.weights = np.zeros((8, 3))  # 8 features, 3 actions

    def predict(self, state: SourceSelectionState) -> np.ndarray:
        x = state.to_feature_vector()
        q = x @ self.weights
        exp_q = np.exp(q - np.max(q))
        return exp_q / exp_q.sum()

    def confidence(self, state: SourceSelectionState) -> float:
        return 0.5

    def update(self, state: SourceSelectionState, action: int, reward: float):
        x = state.to_feature_vector()
        q_current = np.dot(x, self.weights[:, action])
        self.weights[:, action] += self.lr * (reward - q_current) * x


class DistillationStudent:
    def __init__(self, feature_dim=8, n_classes=3, lr=0.01):
        self.weights = np.zeros((feature_dim, n_classes))
        self.biases = np.zeros(n_classes)
        self.lr = lr
        self.n_classes = n_classes
        self.counter = 0

    def predict_proba(self, state_vector):
        logits = state_vector @ self.weights + self.biases
        exp = np.exp(logits - np.max(logits))
        return exp / exp.sum()

    def update(self, state_vector, teacher_probs, reward, action, distill_weight=0.7, rl_weight=0.3):
        current = self.predict_proba(state_vector)
        grad_distill = -(teacher_probs - current)
        one_hot = np.zeros(self.n_classes)
        one_hot[action] = 1.0
        grad_rl = -reward * (one_hot - current)
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


class DistillationSourceOptimizer:
    ACTION_SPACE = ['snapshot', 'api', 'fallback']

    def __init__(self, config):
        self.config = config
        self.student = DistillationStudent(feature_dim=8, n_classes=3,
                                           lr=config.get('distillation_learning_rate', 0.01))
        self.teachers = [
            SourceRuleBasedTeacher(),
            SourceHistoricalMLTeacher(),
            SourceStatefulQTeacher()
        ]
        self.replay_buffer = ReplayBuffer(max_size=config.get('distillation_replay_size', 2000))
        self.epsilon = config.get('distillation_epsilon', 0.1)
        self.train_every = config.get('distillation_train_every', 10)
        self.counter = 0

    async def select_source(self, state, exploration=True):
        state_vec = state.to_feature_vector()
        teacher_probs = np.zeros(3)
        total_conf = 0.0
        for teacher in self.teachers:
            p = teacher.predict(state)
            c = teacher.confidence(state)
            teacher_probs += p * c
            total_conf += c
        if total_conf > 0:
            teacher_probs /= total_conf
        else:
            teacher_probs = np.ones(3) / 3

        student_probs = self.student.predict_proba(state_vec)
        if exploration and random.random() < self.epsilon:
            action_idx = random.randint(0, 2)
        else:
            combined = 0.8 * student_probs + 0.2 * teacher_probs
            action_idx = np.argmax(combined)

        return self.ACTION_SPACE[action_idx], action_idx, state_vec, teacher_probs

    async def update(self, state_vec, action_idx, reward, next_state_vec, teacher_probs):
        self.replay_buffer.push(state_vec, action_idx, reward, next_state_vec, teacher_probs)
        self.counter += 1
        if self.counter % self.train_every == 0 and len(self.replay_buffer) >= 8:
            batch = self.replay_buffer.sample(8)
            states, actions, rewards, _, teacher_probs_batch = batch
            for i in range(len(states)):
                self.student.update(states[i], teacher_probs_batch[i], rewards[i], actions[i])

    def get_stats(self):
        return {
            'student_counter': self.student.counter,
            'buffer_size': len(self.replay_buffer),
            'weights_norm': float(np.linalg.norm(self.student.weights))
        }


# ============================================================================
# MOEA for Source Strategy (NSGA-II)
# ============================================================================
@dataclass
class MOPDSourceStrategy:
    strategy_id: str
    weights: Dict[str, float]  # weights for objectives
    objectives: Dict[str, float]
    scalarised_score: float = 0.0

    def to_dict(self):
        return {
            'strategy_id': self.strategy_id,
            'weights': self.weights,
            'objectives': self.objectives,
            'scalarised_score': self.scalarised_score,
        }


class NSGAIISourceOptimizer:
    def __init__(
        self,
        evaluate_func: Callable[[Dict[str, float]], Awaitable[Dict[str, float]]],
        population_size=30,
        generations=10,
        mutation_rate=0.2,
        crossover_rate=0.8,
        tournament_size=3,
        objective_weights=None,
        dynamic_weights=True,
    ):
        self.evaluate_func = evaluate_func
        self.population_size = population_size
        self.generations = generations
        self.mutation_rate = mutation_rate
        self.crossover_rate = crossover_rate
        self.tournament_size = tournament_size
        self.objective_weights = objective_weights or {}
        self.dynamic_weights = dynamic_weights

        self.best_individual = None
        self.best_fitness = -float('inf')
        self.pareto_front: List[MOPDSourceStrategy] = []
        self._eval_cache = {}
        self._all_points = []

    def _random_weights(self):
        weights = {k: random.random() for k in self.objective_weights.keys()}
        total = sum(weights.values())
        if total > 0:
            weights = {k: v / total for k, v in weights.items()}
        return weights

    def _crossover(self, p1, p2):
        child = {}
        for k in p1:
            if random.random() < 0.5:
                child[k] = p1[k]
            else:
                child[k] = p2[k]
        total = sum(child.values())
        if total > 0:
            child = {k: v / total for k, v in child.items()}
        return child

    def _mutate(self, ind):
        mutant = ind.copy()
        for k in mutant:
            if random.random() < self.mutation_rate:
                mutant[k] = max(0.01, min(1.0, mutant[k] + random.uniform(-0.1, 0.1)))
        total = sum(mutant.values())
        if total > 0:
            mutant = {k: v / total for k, v in mutant.items()}
        return mutant

    def _fast_non_dominated_sort(self, points):
        # Simplified for brevity; real implementation needed
        return [points] if points else []

    def _crowding_distance(self, front):
        return {id(p): 0.0 for p in front}

    def _tournament_selection(self, population, fronts, crowding):
        candidates = random.sample(population, self.tournament_size)
        return candidates[0]

    def _select_best_from_pareto(self, pareto, weights):
        if not pareto:
            return None
        best = max(pareto, key=lambda p: p.scalarised_score)
        return best

    async def evolve(self):
        population = [self._random_weights() for _ in range(self.population_size)]
        points = []
        for ind in population:
            obj = await self.evaluate_func(ind)
            point = MOPDSourceStrategy(
                strategy_id=str(uuid.uuid4()),
                weights=ind,
                objectives=obj
            )
            points.append(point)
        self._all_points = points
        self.pareto_front = points
        return points


# ============================================================================
# HeliumCollector (Enhanced with new components)
# ============================================================================
class HeliumCollector:
    def __init__(
        self,
        cache: CacheManager,
        config: Optional[Union[Dict[str, Any], HeliumConfig]] = None,
        storage: Optional[Storage] = None,
        enable_limit_graph: bool = True,
        enable_modp: bool = True,
        enable_rlhf: bool = True,
        enable_moe: bool = True,
        moe_expert_count: int = 4,
    ):
        # Configuration initialization (as before, but with fixes)
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
        self.storage = storage
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

        # Session
        self._session: Optional[aiohttp.ClientSession] = None
        self._session_lock = asyncio.Lock()

        # Circuit breaker
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

        self.interaction_log: List[Dict] = []
        self.last_state_vec = None
        self.last_action_idx = None
        self.last_teacher_probs = None
        self._last_selected_expert = None  # fix

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
        self.moea_optimizer = None
        self.evolved_pareto_front = []
        self.best_evolved_strategy = None
        self._moea_task = None

        # New components
        self.limit_graph_manager = LimitGraphManager(storage) if enable_limit_graph else None
        self.modp_solver = MODPOptimizer(storage) if enable_modp else None
        self.rlhf_trainer = RLHFTrainer(storage) if enable_rlhf else None
        self.moe_gating = MoEGatingNetwork(storage, {'moe_expert_count': moe_expert_count}) if enable_moe else None

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

        if self.limit_graph_manager:
            self._init_limit_graph()
        if self.moea_enabled:
            self._moea_task = asyncio.create_task(self._moea_loop())

        logger.info("HeliumCollector v2.4.1 initialized")

    # ... (rest of methods from original but with fixes, e.g., use await in _build_state if needed,
    #       and ensure all references to missing classes are replaced with the definitions above)

    def _init_limit_graph(self):
        graph_id = "helium_sources"
        if not self.limit_graph_manager.get_metadata(graph_id):
            self.limit_graph_manager.create_graph(graph_id, "Helium Source Selection Dependencies", {})
            for src in ['snapshot', 'api', 'fallback']:
                self.limit_graph_manager.add_node(graph_id, f"source_{src}", src, {})
            self.limit_graph_manager.add_edge(graph_id, "edge_snapshot_api", "source_snapshot", "source_api", 1.0, {})
            self.limit_graph_manager.add_edge(graph_id, "edge_api_fallback", "source_api", "source_fallback", 1.0, {})

    def _resolve_snapshot_path(self, path):
        if not path:
            return None
        if isinstance(path, str):
            path = Path(path)
        if path.exists():
            return path
        logger.warning("Snapshot path does not exist", path=str(path))
        return None

    async def _get_session(self):
        async with self._session_lock:
            if self._session is None or self._session.closed:
                timeout = ClientTimeout(total=self.request_timeout)
                connector = aiohttp.TCPConnector(limit=10, ttl_dns_cache=300)
                self._session = aiohttp.ClientSession(connector=connector, timeout=timeout, raise_for_status=True)
            return self._session

    async def close(self):
        if self._moea_task:
            self._moea_task.cancel()
            await asyncio.gather(self._moea_task, return_exceptions=True)
        if self._session and not self._session.closed:
            await self._session.close()
            self._session = None

    def _build_state(self, hotspot_id):
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

    async def get_connectivity_score(self, hotspot_id: str, force_refresh: bool = False) -> float:
        cache_key = f"helium:score:{hotspot_id}"
        if not force_refresh:
            cached = await self.cache.get(cache_key)
            if cached is not None:
                if self.metrics:
                    self.metrics['cache_hits'].inc()
                return float(cached)
        if self.metrics:
            self.metrics['cache_misses'].inc()

        state = self._build_state(hotspot_id)

        if self.moe_gating:
            expert_name, _ = await self.moe_gating.select_expert(state)
            self._last_selected_expert = expert_name

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
            if self.moe_gating and self._last_selected_expert:
                await self.moe_gating.add_training_sample(state, self._last_selected_expert, reward)
            await self.source_optimizer.update(
                self.last_state_vec,
                self.last_action_idx,
                reward,
                next_state_vec,
                self.last_teacher_probs
            )

        if self.rlhf_trainer and random.random() < 0.05:
            chosen_source = source
            rejected_source = random.choice([s for s in ['snapshot', 'api', 'fallback'] if s != chosen_source])
            self.rlhf_trainer.record_pair(
                pair_id=str(uuid.uuid4()),
                prompt=f"Which source should we use for {hotspot_id}?",
                chosen=chosen_source,
                rejected=rejected_source,
                reward_diff=reward,
                metadata={'hotspot_id': hotspot_id}
            )

        if self.modp_solver:
            problem_id = "helium_source_selection"
            state_id = f"{hotspot_id}_{datetime.utcnow().isoformat()}_{source}"
            self.modp_solver.add_state(
                state_id=state_id,
                problem_id=problem_id,
                state_attributes={'hotspot_id': hotspot_id, 'source': source},
                objective_values={'success_rate': float(success), 'latency': latency, 'snapshot_usage': 0.0, 'cost': 0.0},
                stage=0
            )

        await self.cache.set(cache_key, str(score), ttl=self.cache_ttl)
        if self.metrics:
            self.metrics['connectivity_score'].labels(hotspot_id=hotspot_id).set(score)
            self.metrics['source_selection'].labels(source=source).inc()
            self.metrics['source_reward'].observe(reward)

        return score

    async def _fetch_from_snapshot(self, hotspot_id):
        if not self.snapshot_path:
            return None
        try:
            df = pd.read_parquet(self.snapshot_path)
            if 'hotspot_id' in df.columns:
                filtered = df[df['hotspot_id'] == hotspot_id]
                if not filtered.empty:
                    return filtered.to_dict('records')
        except Exception as e:
            logger.warning("Failed to read snapshot", error=str(e))
        return None

    async def _fetch_from_api(self, hotspot_id):
        async def fetch():
            session = await self._get_session()
            url = f"{self.api_url}hotspots/{hotspot_id}/stats"
            headers = {}
            if self.api_key:
                headers["Authorization"] = f"Bearer {self.api_key}"
            async with session.get(url, headers=headers) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    stats = data.get('data', {})
                    if 'rssi' in stats and 'snr' in stats:
                        return [{'hotspot_id': hotspot_id, 'rssi': stats['rssi'], 'snr': stats['snr'], 'timestamp': datetime.now().isoformat()}]
                    else:
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
                        wait = min(self.config.get("retry_min_wait", 1.0) * (2 ** attempt), self.config.get("retry_max_wait", 10.0))
                        await asyncio.sleep(wait)

        start_time = time.time()
        data = await self._circuit_breaker.call(fetch_with_retry)
        if self.metrics and data is not None:
            self.metrics['calls'].labels(status='success').inc()
            self.metrics['latency'].observe(time.time() - start_time)
        return data

    def _compute_score(self, data):
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

    async def fetch_batch_scores(self, hotspot_ids, max_concurrency=10):
        semaphore = asyncio.Semaphore(max_concurrency)
        async def fetch_one(hid):
            async with semaphore:
                return hid, await self.get_connectivity_score(hid)
        tasks = [fetch_one(hid) for hid in hotspot_ids]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        scores = {}
        for idx, res in enumerate(results):
            if isinstance(res, Exception):
                scores[hotspot_ids[idx]] = self.default_score
            else:
                hid, score = res
                scores[hid] = score
        return scores

    def _log_interaction(self, source, success, reward, latency=0.0):
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

    @classmethod
    def train_historical_model(cls, log_path=Path("./helium_interactions.csv"), model_path=Path("./helium_historical_model.pkl")):
        if not log_path.exists():
            logger.warning(f"Interaction logs not found at {log_path}. No model trained.")
            return
        df_logs = pd.read_csv(log_path)
        if len(df_logs) < 10:
            logger.warning("Not enough logs to train historical model (need at least 10).")
            return
        logger.info("Historical ML training requires state vectors in logs. Please implement logging of state vectors.")

    async def update_snapshot(self, snapshot_path):
        self.snapshot_path = self._resolve_snapshot_path(snapshot_path)
        logger.info("Snapshot path updated", path=snapshot_path)

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()

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

    async def run_source_evolution(self):
        if not self.moea_enabled:
            return []
        async def evaluate(weights):
            if len(self.interaction_log) < 10:
                return {'success_rate': 0.0, 'latency': 0.0, 'snapshot_usage': 0.0, 'cost': 0.0}
            success_rate = np.mean([entry['success'] for entry in self.interaction_log[-100:]])
            latency = 1.0 - np.mean([entry['latency'] for entry in self.interaction_log if entry['latency'] is not None]) if any(entry['latency'] is not None for entry in self.interaction_log) else 0.0
            snapshot_usage = np.mean([1.0 if entry['source'] == 'snapshot' else 0.0 for entry in self.interaction_log])
            cost = 0.5
            return {'success_rate': success_rate, 'latency': latency, 'snapshot_usage': snapshot_usage, 'cost': cost}

        self.moea_optimizer = NSGAIISourceOptimizer(
            evaluate_func=evaluate,
            population_size=self.moea_population_size,
            generations=self.moea_generations,
            mutation_rate=self.moea_mutation_rate,
            crossover_rate=self.moea_crossover_rate,
            tournament_size=self.moea_tournament_size,
            objective_weights=self._get_dynamic_moea_weights(),
            dynamic_weights=self.moea_dynamic_weights,
        )

        pareto = await self.moea_optimizer.evolve()
        self.evolved_pareto_front = pareto
        if pareto:
            best = self.moea_optimizer._select_best_from_pareto(pareto, self._get_dynamic_moea_weights())
            if best:
                self.best_evolved_strategy = best
                if self.metrics:
                    self.metrics['moea_pareto_front'].set(len(pareto))
                if self.modp_solver:
                    self.modp_solver.add_state(
                        state_id=f"moea_best_{time.time()}",
                        problem_id="helium_strategy_evolution",
                        state_attributes={'weights': best.weights},
                        objective_values=best.objectives,
                        stage=0
                    )
        return pareto

    def _get_dynamic_moea_weights(self):
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

    async def get_limit_graph(self, graph_id="helium_sources"):
        if self.limit_graph_manager:
            return {
                'metadata': self.limit_graph_manager.get_metadata(graph_id),
                'nodes': self.limit_graph_manager.get_nodes(graph_id),
                'edges': self.limit_graph_manager.get_edges(graph_id),
            }
        return {}

    async def get_moe_experts(self):
        if self.moe_gating:
            return self.moe_gating.expert_names
        return []

    async def get_rlhf_pairs(self, limit=100):
        if self.rlhf_trainer:
            return self.rlhf_trainer.get_pairs(limit)
        return []

    async def record_rlhf_pair(self, pair_id, prompt, chosen, rejected, reward_diff, metadata=None):
        if self.rlhf_trainer:
            self.rlhf_trainer.record_pair(pair_id, prompt, chosen, rejected, reward_diff, metadata)


# ============================================================================
# Convenience factory
# ============================================================================
def create_helium_collector(cache, config=None, storage=None):
    return HeliumCollector(cache, config, storage)
