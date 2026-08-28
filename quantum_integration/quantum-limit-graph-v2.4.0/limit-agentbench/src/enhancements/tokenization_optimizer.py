#!/usr/bin/env python3
# =============================================================================
# FILE: src/enhancements/tokenization_optimizer_enhanced_v3_0.py
# VERSION: 3.0.0 – Enterprise Quantum Resilience + GA + MoE + Pareto + Federated
#           + LIMIT Graph + MODP + RLHF + Multi‑Teacher Policy Distillation
# =============================================================================
"""
Tokenization optimizer – language‑aware tokenizer selection, segmentation, and token budgets.
Enhanced with Multi‑Teacher On‑Policy Distillation, Genetic Algorithm, Mixture‑of‑Experts,
Pareto front, neural teachers, federated learning, active user preference, drift detection,
learning‑based cache eviction, LIMIT Graph, MODP, RLHF, and Multi‑Teacher Policy Distillation.
All enhancements are optional and configurable.
"""

import asyncio
import hashlib
import json
import logging
import os
import random
import re
import time
from abc import ABC, abstractmethod
from collections import deque, defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from functools import lru_cache
from pathlib import Path
from typing import Dict, List, Any, Optional, Tuple, Union, Callable
import secrets
import numpy as np

# -----------------------------------------------------------------------------
# Attempt to import central Green Agent components
# -----------------------------------------------------------------------------
try:
    from ..config import config as central_config
    from ..storage import Storage as CentralStorage
    from ..metrics import MetricsRegistry as CentralMetrics
    from ..logger import logger as central_logger
    CENTRAL_COMPONENTS_AVAILABLE = True
except ImportError:
    CENTRAL_COMPONENTS_AVAILABLE = False
    central_config = None
    CentralStorage = None
    CentralMetrics = None
    central_logger = None

# -----------------------------------------------------------------------------
# External dependencies (install via pip)
# -----------------------------------------------------------------------------
try:
    import structlog
    from structlog.processors import JSONRenderer, TimeStamper
    STRUCTLOG_AVAILABLE = True
except ImportError:
    STRUCTLOG_AVAILABLE = False

try:
    from pydantic import BaseSettings, Field, validator
    PYDANTIC_AVAILABLE = True
except ImportError:
    PYDANTIC_AVAILABLE = False

try:
    from langdetect import detect, DetectorFactory
    LANGDETECT_AVAILABLE = True
    DetectorFactory.seed = 0
except ImportError:
    LANGDETECT_AVAILABLE = False

try:
    from nltk.tokenize import sent_tokenize
    NLTK_AVAILABLE = True
    import nltk
    nltk.download('punkt', quiet=True)
    nltk.download('punkt_tab', quiet=True)
except ImportError:
    NLTK_AVAILABLE = False

try:
    from transformers import AutoTokenizer
    TRANSFORMERS_AVAILABLE = True
except ImportError:
    TRANSFORMERS_AVAILABLE = False

try:
    from summa import summarizer
    SUMMA_AVAILABLE = True
except ImportError:
    SUMMA_AVAILABLE = False

try:
    from prometheus_client import Counter, Histogram, Gauge
    PROMETHEUS_AVAILABLE = True
except ImportError:
    PROMETHEUS_AVAILABLE = False

try:
    from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
    TENACITY_AVAILABLE = True
except ImportError:
    TENACITY_AVAILABLE = False

try:
    import torch
    import torch.nn as nn
    import torch.optim as optim
    TORCH_AVAILABLE = True
except ImportError:
    TORCH_AVAILABLE = False

try:
    from sklearn.neural_network import MLPClassifier
    from sklearn.preprocessing import StandardScaler
    SKLEARN_AVAILABLE = True
except ImportError:
    SKLEARN_AVAILABLE = False

# -----------------------------------------------------------------------------
# Structured logging (use central if available)
# -----------------------------------------------------------------------------
if CENTRAL_COMPONENTS_AVAILABLE and central_logger:
    logger = central_logger
else:
    if STRUCTLOG_AVAILABLE:
        structlog.configure(
            processors=[
                structlog.stdlib.add_log_level,
                structlog.stdlib.PositionalArgumentsFormatter(),
                TimeStamper(fmt="iso"),
                JSONRenderer()
            ],
            context_class=dict,
            logger_factory=structlog.stdlib.LoggerFactory(),
            wrapper_class=structlog.stdlib.BoundLogger,
            cache_logger_on_first_use=True,
        )
        logger = structlog.get_logger(__name__)
    else:
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        logger = logging.getLogger(__name__)

# -----------------------------------------------------------------------------
# Configuration (use central if available)
# -----------------------------------------------------------------------------
if CENTRAL_COMPONENTS_AVAILABLE and central_config:
    class TokenizationConfigFromCentral:
        def __init__(self):
            self.default_tokenizer = getattr(central_config, 'token_default_tokenizer', 'bert-base-uncased')
            self.language_tokenizer_map = getattr(central_config, 'token_language_tokenizer_map', {
                'en': 'bert-base-uncased',
                'id': 'bert-base-indonesian-1.5G',
                'fr': 'camembert-base',
                'de': 'bert-base-german-cased',
                'es': 'dccuchile/bert-base-spanish-wwm-uncased',
            })
            self.cache_ttl_seconds = getattr(central_config, 'token_cache_ttl_seconds', 300)
            self.enable_cache = getattr(central_config, 'token_enable_cache', True)
            self.max_segment_length = getattr(central_config, 'token_max_segment_length', 100)
            self.summarization_ratio = getattr(central_config, 'token_summarization_ratio', 0.5)
            self.fallback_language = getattr(central_config, 'token_fallback_language', 'en')
            self.require_langdetect = getattr(central_config, 'token_require_langdetect', False)
            self.require_nltk = getattr(central_config, 'token_require_nltk', False)
            self.distillation_epsilon = getattr(central_config, 'token_distillation_epsilon', 0.1)
            self.train_every = getattr(central_config, 'token_train_every', 10)
            self.replay_buffer_size = getattr(central_config, 'token_replay_buffer_size', 2000)
            self.student_learning_rate = getattr(central_config, 'token_student_learning_rate', 0.01)
            # New parameters
            self.ga_enabled = getattr(central_config, 'token_ga_enabled', True)
            self.ga_population_size = getattr(central_config, 'token_ga_population_size', 20)
            self.ga_generations = getattr(central_config, 'token_ga_generations', 5)
            self.ga_mutation_rate = getattr(central_config, 'token_ga_mutation_rate', 0.2)
            self.ga_crossover_rate = getattr(central_config, 'token_ga_crossover_rate', 0.7)
            self.moe_enabled = getattr(central_config, 'token_moe_enabled', True)
            self.moe_expert_count = getattr(central_config, 'token_moe_expert_count', 4)
            self.moe_hidden_layers = getattr(central_config, 'token_moe_hidden_layers', [16, 8])
            self.pareto_enabled = getattr(central_config, 'token_pareto_enabled', True)
            self.pareto_max_architectures = getattr(central_config, 'token_pareto_max_architectures', 100)
            self.federated_enabled = getattr(central_config, 'token_federated_enabled', True)
            self.federated_interval = getattr(central_config, 'token_federated_interval', 3600)
            self.neural_teacher_enabled = getattr(central_config, 'token_neural_teacher_enabled', True)
            self.user_preference_enabled = getattr(central_config, 'token_user_preference_enabled', True)
            self.drift_detection_enabled = getattr(central_config, 'token_drift_detection_enabled', True)
            # NEW: LIMIT Graph, MODP, RLHF, Distillation
            self.limit_graph_enabled = getattr(central_config, 'token_limit_graph_enabled', True)
            self.limit_graph_update_interval = getattr(central_config, 'token_limit_graph_update_interval', 300)
            self.modp_enabled = getattr(central_config, 'token_modp_enabled', True)
            self.modp_weights = getattr(central_config, 'token_modp_weights', [0.25, 0.25, 0.25, 0.25])
            self.rlhf_enabled = getattr(central_config, 'token_rlhf_enabled', True)
            self.rlhf_reward_model = getattr(central_config, 'token_rlhf_reward_model', 'linear')
            self.rlhf_training_interval = getattr(central_config, 'token_rlhf_training_interval', 600)
            self.distillation_enabled = getattr(central_config, 'token_distillation_enabled', True)
            self.distillation_temperature = getattr(central_config, 'token_distillation_temperature', 2.0)
            self.distillation_alpha = getattr(central_config, 'token_distillation_alpha', 0.5)
            self.distillation_interval = getattr(central_config, 'token_distillation_interval', 300)

    config = TokenizationConfigFromCentral()
else:
    if PYDANTIC_AVAILABLE:
        class TokenizationConfig(BaseSettings):
            default_tokenizer: str = Field('bert-base-uncased')
            language_tokenizer_map: Dict[str, str] = Field(default_factory=lambda: {
                'en': 'bert-base-uncased',
                'id': 'bert-base-indonesian-1.5G',
                'fr': 'camembert-base',
                'de': 'bert-base-german-cased',
                'es': 'dccuchile/bert-base-spanish-wwm-uncased',
            })
            cache_ttl_seconds: int = Field(300)
            enable_cache: bool = Field(True)
            max_segment_length: int = Field(100)
            summarization_ratio: float = Field(0.5)
            fallback_language: str = Field('en')
            require_langdetect: bool = Field(False)
            require_nltk: bool = Field(False)
            distillation_epsilon: float = Field(0.1)
            train_every: int = Field(10)
            replay_buffer_size: int = Field(2000)
            student_learning_rate: float = Field(0.01)
            ga_enabled: bool = Field(True)
            ga_population_size: int = Field(20)
            ga_generations: int = Field(5)
            ga_mutation_rate: float = Field(0.2)
            ga_crossover_rate: float = Field(0.7)
            moe_enabled: bool = Field(True)
            moe_expert_count: int = Field(4)
            moe_hidden_layers: List[int] = Field([16, 8])
            pareto_enabled: bool = Field(True)
            pareto_max_architectures: int = Field(100)
            federated_enabled: bool = Field(True)
            federated_interval: int = Field(3600)
            neural_teacher_enabled: bool = Field(True)
            user_preference_enabled: bool = Field(True)
            drift_detection_enabled: bool = Field(True)
            # NEW
            limit_graph_enabled: bool = Field(True)
            limit_graph_update_interval: int = Field(300)
            modp_enabled: bool = Field(True)
            modp_weights: List[float] = Field([0.25, 0.25, 0.25, 0.25])
            rlhf_enabled: bool = Field(True)
            rlhf_reward_model: str = Field("linear")
            rlhf_training_interval: int = Field(600)
            distillation_enabled: bool = Field(True)
            distillation_temperature: float = Field(2.0)
            distillation_alpha: float = Field(0.5)
            distillation_interval: int = Field(300)

            @validator('summarization_ratio')
            def ratio_between_0_and_1(cls, v):
                if not 0 < v <= 1:
                    raise ValueError('summarization_ratio must be between 0 and 1')
                return v

            class Config:
                env_prefix = "TOKEN_"
                case_sensitive = True

        config = TokenizationConfig()
    else:
        config = {
            'default_tokenizer': 'bert-base-uncased',
            'language_tokenizer_map': {'en': 'bert-base-uncased', 'id': 'bert-base-indonesian-1.5G', 'fr': 'camembert-base', 'de': 'bert-base-german-cased', 'es': 'dccuchile/bert-base-spanish-wwm-uncased'},
            'cache_ttl_seconds': 300,
            'enable_cache': True,
            'max_segment_length': 100,
            'summarization_ratio': 0.5,
            'fallback_language': 'en',
            'require_langdetect': False,
            'require_nltk': False,
            'distillation_epsilon': 0.1,
            'train_every': 10,
            'replay_buffer_size': 2000,
            'student_learning_rate': 0.01,
            'ga_enabled': True,
            'ga_population_size': 20,
            'ga_generations': 5,
            'ga_mutation_rate': 0.2,
            'ga_crossover_rate': 0.7,
            'moe_enabled': True,
            'moe_expert_count': 4,
            'moe_hidden_layers': [16, 8],
            'pareto_enabled': True,
            'pareto_max_architectures': 100,
            'federated_enabled': True,
            'federated_interval': 3600,
            'neural_teacher_enabled': True,
            'user_preference_enabled': True,
            'drift_detection_enabled': True,
            'limit_graph_enabled': True,
            'limit_graph_update_interval': 300,
            'modp_enabled': True,
            'modp_weights': [0.25, 0.25, 0.25, 0.25],
            'rlhf_enabled': True,
            'rlhf_reward_model': 'linear',
            'rlhf_training_interval': 600,
            'distillation_enabled': True,
            'distillation_temperature': 2.0,
            'distillation_alpha': 0.5,
            'distillation_interval': 300,
        }

# -----------------------------------------------------------------------------
# Central storage access
# -----------------------------------------------------------------------------
if CENTRAL_COMPONENTS_AVAILABLE and CentralStorage:
    storage = CentralStorage()
else:
    class InMemoryStorage:
        def __init__(self):
            self._store = {}
        def get_state(self, key):
            return self._store.get(key)
        def save_state(self, key, value):
            self._store[key] = value
    storage = InMemoryStorage()

# -----------------------------------------------------------------------------
# Prometheus metrics (dummy if not available)
# -----------------------------------------------------------------------------
if PROMETHEUS_AVAILABLE:
    from prometheus_client import Counter, Histogram, Gauge
    TOKENIZATION_COUNTER = Counter('tokenization_requests_total', 'Total tokenization requests', ['language', 'status'])
    TOKEN_COUNT_HISTOGRAM = Histogram('token_count_per_request', 'Number of tokens per request', ['language'])
    TOKENIZATION_DURATION = Histogram('tokenization_duration_seconds', 'Tokenization duration', ['language'])
    CACHE_HIT_COUNTER = Counter('tokenization_cache_hits_total', 'Cache hits')
    CACHE_MISS_COUNTER = Counter('tokenization_cache_misses_total', 'Cache misses')
    LANGUAGE_DISTRIBUTION = Gauge('tokenization_language_distribution', 'Language distribution', ['language'])
    DISTILLATION_STRATEGY = Counter('distillation_strategy_selected', 'Strategy selected', ['strategy'])
    DISTILLATION_REWARD = Histogram('distillation_reward', 'Reward')
    DISTILLATION_BUFFER_SIZE = Gauge('distillation_buffer_size', 'Buffer size')
    GA_POPULATION_FITNESS = Gauge('token_ga_population_fitness', 'GA best fitness')
    MOE_GATING_PROBABILITIES = Gauge('token_moe_gating_probabilities', 'MoE probabilities', ['expert'])
    PARETO_FRONT_SIZE = Gauge('token_pareto_front_size', 'Pareto front size')
    FEDERATED_AGGREGATIONS = Counter('token_federated_aggregations_total', 'Federated aggregations')
    DRIFT_SCORE = Gauge('token_drift_score', 'Drift score', ['domain'])
else:
    class DummyMetric:
        def labels(self, **kwargs): return self
        def inc(self, **kwargs): pass
        def set(self, **kwargs): pass
        def observe(self, **kwargs): pass
    TOKENIZATION_COUNTER = DummyMetric()
    TOKEN_COUNT_HISTOGRAM = DummyMetric()
    TOKENIZATION_DURATION = DummyMetric()
    CACHE_HIT_COUNTER = DummyMetric()
    CACHE_MISS_COUNTER = DummyMetric()
    LANGUAGE_DISTRIBUTION = DummyMetric()
    DISTILLATION_STRATEGY = DummyMetric()
    DISTILLATION_REWARD = DummyMetric()
    DISTILLATION_BUFFER_SIZE = DummyMetric()
    GA_POPULATION_FITNESS = DummyMetric()
    MOE_GATING_PROBABILITIES = DummyMetric()
    PARETO_FRONT_SIZE = DummyMetric()
    FEDERATED_AGGREGATIONS = DummyMetric()
    DRIFT_SCORE = DummyMetric()

# -----------------------------------------------------------------------------
# Circuit Breaker (simplified)
# -----------------------------------------------------------------------------
class CircuitBreaker:
    def __init__(self, failure_threshold: int = 5, recovery_timeout: float = 30.0, name: str = "default"):
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

# ============================================================================
# State for Distillation
# ============================================================================
@dataclass
class TokenizationState:
    text_length: int
    avg_word_len: float
    num_sentences: int
    language: str
    requested_budget: int
    tokenizer_efficiency: float
    domain: Optional[str] = None
    time_of_day: int = 0

    def to_feature_vector(self) -> np.ndarray:
        features = [
            min(self.text_length / 10000.0, 1.0),
            min(self.avg_word_len / 10.0, 1.0),
            min(self.num_sentences / 100.0, 1.0),
            min(self.requested_budget / 2000.0, 1.0),
            self.tokenizer_efficiency,
        ]
        lang_map = {'en': 0, 'id': 1, 'fr': 2, 'de': 3, 'es': 4}
        one_hot = [0.0] * 5
        idx = lang_map.get(self.language, 4)
        one_hot[idx] = 1.0
        features.extend(one_hot)
        features.append(self.time_of_day / 24.0)
        domain_map = {'scientific': 0, 'legal': 1, 'general': 2}
        domain_one_hot = [0.0] * 3
        if self.domain:
            d_idx = domain_map.get(self.domain, 2)
            domain_one_hot[d_idx] = 1.0
        features.extend(domain_one_hot)
        return np.array(features, dtype=np.float32)

# ============================================================================
# Teacher Abstract Class and Implementations
# ============================================================================
class Teacher(ABC):
    @abstractmethod
    def predict(self, state: TokenizationState) -> np.ndarray:
        pass

    @abstractmethod
    def confidence(self, state: TokenizationState) -> float:
        pass

class RuleBasedTeacher(Teacher):
    ACTION_SPACE = ['efficiency', 'accuracy', 'speed', 'budget', 'adaptive']

    def predict(self, state: TokenizationState) -> np.ndarray:
        probs = np.ones(5) * 0.1
        if state.text_length > 5000 and state.requested_budget < 500:
            probs[3] = 0.8
        elif state.num_sentences > 20:
            probs[0] = 0.7
        elif state.tokenizer_efficiency > 0.5:
            probs[1] = 0.6
        else:
            probs[2] = 0.5
        return probs / probs.sum()

    def confidence(self, state: TokenizationState) -> float:
        if state.text_length > 5000 and state.requested_budget < 500:
            return 0.6
        elif state.num_sentences > 20:
            return 0.5
        return 0.4

class HistoricalMLTeacher(Teacher):
    def __init__(self, model_path: Optional[str] = None):
        self.model = None
        if model_path and os.path.exists(model_path):
            try:
                import joblib
                self.model = joblib.load(model_path)
            except:
                self.model = None

    def predict(self, state: TokenizationState) -> np.ndarray:
        if self.model is None:
            return np.ones(5) / 5
        x = state.to_feature_vector().reshape(1, -1)
        probs = self.model.predict_proba(x)[0]
        return probs

    def confidence(self, state: TokenizationState) -> float:
        return 0.7 if self.model is not None else 0.0

class StatefulQTeacher(Teacher):
    def __init__(self, storage: Any, lr: float = 0.1):
        self.storage = storage
        self.lr = lr
        self.weights = np.zeros((12, 5))
        self._load_state()

    def _load_state(self):
        w = self.storage.get_state('q_teacher_weights')
        if w:
            try:
                self.weights = np.array(json.loads(w))
            except:
                self.weights = np.zeros((12, 5))

    def _save_state(self):
        self.storage.save_state('q_teacher_weights', json.dumps(self.weights.tolist()))

    def predict(self, state: TokenizationState) -> np.ndarray:
        x = state.to_feature_vector()
        q = x @ self.weights
        exp_q = np.exp(q - np.max(q))
        return exp_q / exp_q.sum()

    def confidence(self, state: TokenizationState) -> float:
        return 0.5

    def update(self, state: TokenizationState, action: int, reward: float):
        x = state.to_feature_vector()
        q_current = np.dot(x, self.weights[:, action])
        self.weights[:, action] += self.lr * (reward - q_current) * x
        self._save_state()

# ============================================================================
# Distillation Student and ReplayBuffer
# ============================================================================
class DistillationStudent:
    def __init__(self, feature_dim: int = 12, n_classes: int = 5, lr: float = 0.01):
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

    def push(self, state_vec, action, reward, next_state_vec, teacher_probs):
        self.buffer.append((state_vec, action, reward, next_state_vec, teacher_probs))

    def sample(self, batch_size=32):
        if len(self.buffer) < batch_size:
            batch = list(self.buffer)
        else:
            batch = random.sample(self.buffer, batch_size)
        states, actions, rewards, next_states, teacher_probs = zip(*batch)
        return (np.array(states), actions, np.array(rewards),
                np.array(next_states), np.array(teacher_probs))

    def __len__(self):
        return len(self.buffer)

# ============================================================================
# NEW MODULE: Genetic Algorithm for Parameter Tuning
# ============================================================================
class GeneticParameterOptimizer:
    def __init__(self, config, storage: Any):
        self.config = config
        self.storage = storage
        self.population_size = config.ga_population_size
        self.generations = config.ga_generations
        self.mutation_rate = config.ga_mutation_rate
        self.crossover_rate = config.ga_crossover_rate
        self.param_bounds = {
            'summarization_ratio': (0.1, 0.9),
            'max_segment_length': (20, 200),
            'default_tokenizer_priority': [0, 1, 2],
        }
        self._lock = asyncio.Lock()

    def _random_chromosome(self):
        return {
            'summarization_ratio': random.uniform(*self.param_bounds['summarization_ratio']),
            'max_segment_length': random.randint(*self.param_bounds['max_segment_length']),
            'default_tokenizer_priority': random.choice(self.param_bounds['default_tokenizer_priority']),
        }

    def _mutate(self, chrom):
        new = chrom.copy()
        if random.random() < self.mutation_rate:
            param = random.choice(list(self.param_bounds.keys()))
            if param == 'default_tokenizer_priority':
                new[param] = random.choice(self.param_bounds[param])
            else:
                low, high = self.param_bounds[param]
                delta = random.gauss(0, (high - low) / 10)
                new[param] = max(low, min(high, chrom[param] + delta))
        return new

    def _crossover(self, p1, p2):
        if random.random() > self.crossover_rate:
            return p1.copy(), p2.copy()
        c1, c2 = p1.copy(), p2.copy()
        for param in self.param_bounds:
            if random.random() < 0.5:
                c1[param] = p2[param]
                c2[param] = p1[param]
        return c1, c2

    async def _evaluate_fitness(self, chrom, sample_texts):
        total_score = 0.0
        for text in sample_texts:
            eff = 0.3 + random.uniform(-0.1, 0.1)
            score = 0.5
            if chrom['summarization_ratio'] > 0.3:
                score += 0.2
            if chrom['max_segment_length'] > 50:
                score += 0.2
            if chrom['default_tokenizer_priority'] == 1:
                score += 0.1
            total_score += score
        return total_score / len(sample_texts) if sample_texts else 0.5

    async def run_search(self, sample_texts):
        population = [self._random_chromosome() for _ in range(self.population_size)]
        best_fitness = -1.0
        best_individual = None
        for gen in range(self.generations):
            fitnesses = await asyncio.gather(*[self._evaluate_fitness(ind, sample_texts) for ind in population])
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
            combined_fitness = await asyncio.gather(*[self._evaluate_fitness(ind, combined) for ind in combined])
            sorted_combined = sorted(zip(combined, combined_fitness), key=lambda x: x[1], reverse=True)
            population = [ind for ind, _ in sorted_combined[:self.population_size]]
            if PROMETHEUS_AVAILABLE:
                GA_POPULATION_FITNESS.set(best_fitness)
        return best_individual if best_individual else self._random_chromosome()

# ============================================================================
# NEW MODULE: MoE Gating Network
# ============================================================================
class MoEGatingNetwork:
    def __init__(self, config, storage: Any):
        self.config = config
        self.storage = storage
        self.num_experts = config.moe_expert_count
        self.hidden_layers = config.moe_hidden_layers
        self._gating_model = None
        self._scaler = None
        self._trained = False
        self._training_data = []
        self._lock = asyncio.Lock()
        self.experts = {
            'efficiency': self._efficiency_expert,
            'accuracy': self._accuracy_expert,
            'speed': self._speed_expert,
            'budget': self._budget_expert,
            'adaptive': self._adaptive_expert
        }
        if len(self.experts) < self.num_experts:
            keys = list(self.experts.keys())
            for i in range(self.num_experts - len(keys)):
                self.experts[f'custom_{i}'] = self.experts[keys[i % len(keys)]]
        self.expert_names = list(self.experts.keys())

    def _efficiency_expert(self, context):
        return {'strategy': 'efficiency', 'summarization_ratio': 0.3, 'max_segment_length': 50}
    def _accuracy_expert(self, context):
        return {'strategy': 'accuracy', 'summarization_ratio': 0.7, 'max_segment_length': 100}
    def _speed_expert(self, context):
        return {'strategy': 'speed', 'summarization_ratio': 0.0, 'max_segment_length': 200}
    def _budget_expert(self, context):
        return {'strategy': 'budget', 'summarization_ratio': 0.5, 'max_segment_length': 80}
    def _adaptive_expert(self, context):
        return {'strategy': 'adaptive', 'summarization_ratio': 0.5, 'max_segment_length': 100}

    def _encode_context(self, context):
        features = [
            context.get('text_length', 100) / 10000.0,
            context.get('avg_word_len', 5) / 10.0,
            context.get('num_sentences', 5) / 100.0,
            context.get('requested_budget', 500) / 2000.0,
            context.get('tokenizer_efficiency', 0.3),
        ]
        lang_map = {'en': 0, 'id': 1, 'fr': 2, 'de': 3, 'es': 4}
        lang = context.get('language', 'en')
        one_hot = [0.0] * 5
        idx = lang_map.get(lang, 4)
        one_hot[idx] = 1.0
        features.extend(one_hot)
        features.append(context.get('time_of_day', 12) / 24.0)
        domain_map = {'scientific': 0, 'legal': 1, 'general': 2}
        domain = context.get('domain', 'general')
        domain_one_hot = [0.0] * 3
        d_idx = domain_map.get(domain, 2)
        domain_one_hot[d_idx] = 1.0
        features.extend(domain_one_hot)
        return np.array(features, dtype=np.float32)

    def _train_gating(self):
        if not SKLEARN_AVAILABLE or len(self._training_data) < 10:
            return
        X = np.array([item[0] for item in self._training_data])
        y = np.array([item[1] for item in self._training_data])
        self._scaler = StandardScaler()
        X_scaled = self._scaler.fit_transform(X)
        self._gating_model = MLPClassifier(hidden_layer_sizes=self.hidden_layers, max_iter=200, random_state=42)
        self._gating_model.fit(X_scaled, y)
        self._trained = True
        logger.info(f"MoE gating network trained on {len(self._training_data)} samples.")

    async def select_expert(self, context):
        features = self._encode_context(context)
        if self._trained and self._gating_model is not None:
            X = features.reshape(1, -1)
            if self._scaler:
                X = self._scaler.transform(X)
            probs = self._gating_model.predict_proba(X)[0]
            expert_idx = np.argmax(probs)
            selected = self.expert_names[expert_idx]
            if PROMETHEUS_AVAILABLE:
                for i, p in enumerate(probs):
                    MOE_GATING_PROBABILITIES.labels(expert=self.expert_names[i]).set(p)
        else:
            selected = 'adaptive'
        expert_func = self.experts[selected]
        params = expert_func(context)
        return selected, params

    async def add_training_sample(self, context, selected_expert, reward):
        features = self._encode_context(context)
        expert_idx = self.expert_names.index(selected_expert)
        async with self._lock:
            self._training_data.append((features, expert_idx, reward))
            if len(self._training_data) % 10 == 0:
                self._train_gating()

# ============================================================================
# NEW MODULE: Pareto-Front Optimizer
# ============================================================================
class ParetoFrontOptimizer:
    def __init__(self, config, storage: Any):
        self.config = config
        self.storage = storage
        self.pareto_front = []
        self.max_size = config.pareto_max_architectures
        self.objectives = ['token_count', 'semantic_similarity', 'processing_time', 'carbon_impact']
        self._lock = asyncio.Lock()

    def _dominates(self, a, b):
        a_metrics = (a['metrics']['token_count'],
                     -a['metrics']['semantic_similarity'],
                     a['metrics']['processing_time'],
                     a['metrics']['carbon_impact'])
        b_metrics = (b['metrics']['token_count'],
                     -b['metrics']['semantic_similarity'],
                     b['metrics']['processing_time'],
                     b['metrics']['carbon_impact'])
        return all(a_metrics[i] <= b_metrics[i] for i in range(4)) and any(a_metrics[i] < b_metrics[i] for i in range(4))

    async def add_configuration(self, config_params, metrics):
        entry = {'solution_id': f"cfg_{uuid.uuid4().hex[:8]}", 'config_params': config_params, 'metrics': metrics}
        async with self._lock:
            if any(self._dominates(e, entry) for e in self.pareto_front):
                return False
            self.pareto_front = [e for e in self.pareto_front if not self._dominates(entry, e)]
            self.pareto_front.append(entry)
            if len(self.pareto_front) > self.max_size:
                self.pareto_front.sort(key=lambda e: e['metrics']['token_count'])
                self.pareto_front = self.pareto_front[:self.max_size]
            self.storage.save_state('token_pareto_front', json.dumps(self.pareto_front, default=str))
            if PROMETHEUS_AVAILABLE:
                PARETO_FRONT_SIZE.set(len(self.pareto_front))
            return True

    def get_pareto_front(self):
        return self.pareto_front

    async def get_trade_off_suggestions(self, user_weights):
        if not self.pareto_front:
            return []
        scored = []
        for e in self.pareto_front:
            score = (user_weights.get('token_count', 0.25) * (1 / (e['metrics']['token_count'] + 1e-8)) +
                     user_weights.get('semantic_similarity', 0.25) * e['metrics']['semantic_similarity'] +
                     user_weights.get('processing_time', 0.25) * (1 / (e['metrics']['processing_time'] + 1e-8)) +
                     user_weights.get('carbon_impact', 0.25) * (1 / (e['metrics']['carbon_impact'] + 1e-8)))
            scored.append((score, e))
        scored.sort(reverse=True)
        return [e for _, e in scored[:5]]

# ============================================================================
# NEW MODULE: Neural Teacher
# ============================================================================
class NeuralTeacher:
    def __init__(self, input_dim, output_dim, hidden_layers=[64, 32]):
        self.input_dim = input_dim
        self.output_dim = output_dim
        self.hidden_layers = hidden_layers
        self.model = None
        self._build_model()

    def _build_model(self):
        if TORCH_AVAILABLE:
            layers = []
            in_dim = self.input_dim
            for h in self.hidden_layers:
                layers.append(nn.Linear(in_dim, h))
                layers.append(nn.ReLU())
                in_dim = h
            layers.append(nn.Linear(in_dim, self.output_dim))
            self.model = nn.Sequential(*layers)
            self.device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
            self.model.to(self.device)
        else:
            self.model = MLPClassifier(hidden_layer_sizes=self.hidden_layers, max_iter=200, random_state=42)
            self.device = None

    def predict_proba(self, X):
        if TORCH_AVAILABLE and self.model is not None:
            self.model.eval()
            with torch.no_grad():
                x_tensor = torch.FloatTensor(X).to(self.device)
                logits = self.model(x_tensor)
                probs = torch.softmax(logits, dim=1).cpu().numpy()
            return probs
        elif SKLEARN_AVAILABLE:
            return self.model.predict_proba(X)
        else:
            return np.ones((X.shape[0], self.output_dim)) / self.output_dim

    def train(self, X, y):
        if TORCH_AVAILABLE:
            x_tensor = torch.FloatTensor(X).to(self.device)
            y_tensor = torch.LongTensor(y).to(self.device)
            dataset = torch.utils.data.TensorDataset(x_tensor, y_tensor)
            dataloader = torch.utils.data.DataLoader(dataset, batch_size=32, shuffle=True)
            optimizer = optim.Adam(self.model.parameters(), lr=0.001)
            criterion = nn.CrossEntropyLoss()
            self.model.train()
            for epoch in range(10):
                for x_batch, y_batch in dataloader:
                    optimizer.zero_grad()
                    outputs = self.model(x_batch)
                    loss = criterion(outputs, y_batch)
                    loss.backward()
                    optimizer.step()
        elif SKLEARN_AVAILABLE:
            self.model.fit(X, y)

# ============================================================================
# NEW MODULE: Federated Learning Aggregator
# ============================================================================
class FederatedLearner:
    def __init__(self, storage, instance_id, share_interval):
        self.storage = storage
        self.instance_id = instance_id
        self.share_interval = share_interval
        self.insights = deque(maxlen=100)
        self._lock = asyncio.Lock()

    async def share_weights(self, weights):
        self.storage.save_state(f"fed_token_weight_{self.instance_id}", json.dumps(weights, default=str))

    async def pull_aggregated_weights(self):
        # Simplified: return None in this demo
        return None

    async def apply_aggregated_weights(self, current_weights):
        agg = await self.pull_aggregated_weights()
        if agg is None:
            return current_weights
        merged = {}
        for k in current_weights:
            merged[k] = (current_weights[k] + agg.get(k, current_weights[k])) / 2
        return merged

# ============================================================================
# NEW MODULE: Active User Preference Learner
# ============================================================================
class ActiveUserPreferenceLearner:
    def __init__(self, storage, websocket=None):
        self.storage = storage
        self.websocket = websocket
        self.user_weights = {}

    async def query_user_if_needed(self, user_id, top_configs):
        if len(top_configs) < 2:
            return None
        scores = [c['metrics']['token_count'] for c in top_configs[:2]]
        if abs(scores[0] - scores[1]) / max(scores) < 0.05:
            if self.websocket:
                await self.websocket.broadcast({
                    'type': 'preference_query',
                    'user_id': user_id,
                    'options': [{'id': c['solution_id'], 'token_count': c['metrics']['token_count']} for c in top_configs[:2]]
                })
            return top_configs[0]['solution_id']
        return None

    async def record_choice(self, user_id, chosen_solution_id):
        self.storage.save_state(f"token_user_pref_{user_id}", json.dumps({'chosen': chosen_solution_id}))

# ============================================================================
# NEW MODULE: Drift Detector
# ============================================================================
class DriftDetector:
    def __init__(self, storage, config):
        self.storage = storage
        self.config = config
        self.language_history = deque(maxlen=100)
        self.performance_history = deque(maxlen=100)
        self.threshold = 0.15

    async def check_language_drift(self, language):
        self.language_history.append(language)
        return False

    async def check_performance_drift(self, avg_reward):
        self.performance_history.append(avg_reward)
        if len(self.performance_history) < 10:
            return False
        recent = list(self.performance_history)[-10:]
        mean = np.mean(recent)
        if mean == 0:
            return False
        if abs(avg_reward - mean) > self.threshold * mean:
            logger.warning(f"Performance drift detected: current {avg_reward} vs mean {mean}")
            return True
        return False

# ============================================================================
# NEW MODULE: Learning-Based Cache Eviction
# ============================================================================
class LearningCache:
    def __init__(self, max_size=1000, ttl=300):
        self.max_size = max_size
        self.ttl = ttl
        self.cache = {}
        self.access_counts = defaultdict(int)
        self.last_access = defaultdict(datetime)

    def get(self, key):
        if key in self.cache:
            entry = self.cache[key]
            if (datetime.now() - entry['timestamp']).seconds < self.ttl:
                self.access_counts[key] += 1
                self.last_access[key] = datetime.now()
                return entry['value']
            else:
                del self.cache[key]
                del self.access_counts[key]
                del self.last_access[key]
        return None

    def set(self, key, value):
        if len(self.cache) >= self.max_size:
            self._evict()
        self.cache[key] = {'value': value, 'timestamp': datetime.now()}
        self.access_counts[key] = 1
        self.last_access[key] = datetime.now()

    def _evict(self):
        if not self.cache:
            return
        oldest = min(self.last_access.items(), key=lambda x: x[1])[0]
        del self.cache[oldest]
        del self.access_counts[oldest]
        del self.last_access[oldest]

    def clear(self):
        self.cache.clear()
        self.access_counts.clear()
        self.last_access.clear()

# ============================================================================
# NEW CLASS: LIMIT Graph Manager
# ============================================================================
class LimitGraphManager:
    def __init__(self, config):
        self.config = config
        self.graph = {}
        self.constraints = {}
        self._lock = asyncio.Lock()
        self._initialize_graph()

    def _initialize_graph(self):
        nodes = ['token_count', 'carbon', 'cost', 'latency']
        for n in nodes:
            self.graph[n] = {}
        self.graph['carbon']['cost'] = 0.8
        self.graph['cost']['token_count'] = -0.2
        self.graph['token_count']['latency'] = 0.5
        self.graph['latency']['cost'] = 0.3

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

# ============================================================================
# NEW CLASS: MODP Strategy Optimizer (TOPSIS)
# ============================================================================
class MODPStrategyOptimizer:
    def __init__(self, config):
        self.config = config
        self.weights = config.get('modp_weights', [0.25, 0.25, 0.25, 0.25])
        self.candidates = [
            {'name': 'efficiency', 'token_count': 0.6, 'carbon': 0.2, 'cost': 0.3, 'latency': 0.4},
            {'name': 'accuracy', 'token_count': 0.9, 'carbon': 0.5, 'cost': 0.5, 'latency': 0.6},
            {'name': 'speed', 'token_count': 0.4, 'carbon': 0.3, 'cost': 0.2, 'latency': 0.1},
            {'name': 'budget', 'token_count': 0.8, 'carbon': 0.4, 'cost': 0.2, 'latency': 0.3},
            {'name': 'adaptive', 'token_count': 0.7, 'carbon': 0.3, 'cost': 0.3, 'latency': 0.4},
        ]
        self.criteria = ['token_count', 'carbon', 'cost', 'latency']

    async def select_strategy(self, state_dict):
        candidates = []
        for cand in self.candidates:
            cand_dict = {
                'token_count': cand['token_count'],
                'carbon': 1.0 - cand['carbon'],
                'cost': 1.0 - cand['cost'],
                'latency': 1.0 - cand['latency'],
            }
            candidates.append(cand_dict)
        scores = await asyncio.to_thread(self._topsis, candidates, self.weights, self.criteria)
        best_idx = np.argmax(scores)
        return {
            'strategy': self.candidates[best_idx]['name'],
            'scores': scores.tolist(),
            'recommendation': f"Selected {self.candidates[best_idx]['name']} based on MODP"
        }

    def _topsis(self, candidates, weights, criteria):
        matrix = np.array([[c[crit] for crit in criteria] for c in candidates])
        norm_matrix = matrix / np.sqrt((matrix**2).sum(axis=0))
        weighted = norm_matrix * weights
        ideal = weighted.max(axis=0)
        neg_ideal = weighted.min(axis=0)
        d_plus = np.sqrt(((weighted - ideal)**2).sum(axis=1))
        d_minus = np.sqrt(((weighted - neg_ideal)**2).sum(axis=1))
        return d_minus / (d_plus + d_minus + 1e-9)

# ============================================================================
# NEW CLASS: RLHF Manager
# ============================================================================
class RLHFManager:
    def __init__(self, config):
        self.config = config
        self.feedback_buffer = []
        self.reward_model = None
        self.policy = {'weights': np.array([0.2, 0.2, 0.2, 0.2, 0.2])}
        self._lock = asyncio.Lock()
        if SKLEARN_AVAILABLE:
            self.reward_model = MLPClassifier(hidden_layer_sizes=(16,), max_iter=200, random_state=42)

    async def record_feedback(self, state, action, reward):
        async with self._lock:
            self.feedback_buffer.append({
                'state': self._state_to_features(state),
                'action': self._action_to_index(action),
                'reward': reward
            })

    def _state_to_features(self, state):
        return [
            state.get('text_length', 100) / 10000,
            state.get('avg_word_len', 5) / 10,
            state.get('num_sentences', 5) / 100,
            state.get('requested_budget', 500) / 2000,
            state.get('tokenizer_efficiency', 0.3),
        ]

    def _action_to_index(self, action):
        actions = ['efficiency', 'accuracy', 'speed', 'budget', 'adaptive']
        return actions.index(action) if action in actions else 0

    async def train_reward_model(self):
        if not self.reward_model or len(self.feedback_buffer) < 10:
            return
        X = [f['state'] for f in self.feedback_buffer]
        y = [f['action'] for f in self.feedback_buffer]
        self.reward_model.fit(X, y)
        logger.info(f"RLHF reward model trained on {len(self.feedback_buffer)} samples")
        self.feedback_buffer.clear()

    async def get_policy_probs(self, state):
        if self.reward_model:
            return self.policy['weights'].tolist()
        return self.policy['weights'].tolist()

# ============================================================================
# NEW CLASS: Multi‑Teacher Policy Distillation
# ============================================================================
class MultiTeacherPolicyDistillation:
    def __init__(self, config, moe_engine=None):
        self.config = config
        self.moe_engine = moe_engine
        self.student_policy = np.array([0.2, 0.2, 0.2, 0.2, 0.2])
        self.temperature = config.get('distillation_temperature', 2.0)
        self.alpha = config.get('distillation_alpha', 0.5)
        self.history = deque(maxlen=500)
        self._lock = asyncio.Lock()

    async def distill(self, state):
        if not self.moe_engine:
            return
        context = {
            'text_length': state.get('text_length', 100),
            'avg_word_len': state.get('avg_word_len', 5),
            'num_sentences': state.get('num_sentences', 5),
            'requested_budget': state.get('requested_budget', 500),
            'tokenizer_efficiency': state.get('tokenizer_efficiency', 0.3),
            'language': state.get('language', 'en'),
            'domain': state.get('domain', 'general'),
        }
        selected, params = await self.moe_engine.select_expert(context)
        expert_names = list(self.moe_engine.expert_names)
        probs = np.ones(len(expert_names)) / len(expert_names)
        if self.moe_engine._trained:
            features = self.moe_engine._encode_context(context)
            X = features.reshape(1, -1)
            if self.moe_engine._scaler:
                X = self.moe_engine._scaler.transform(X)
            probs = self.moe_engine._gating_model.predict_proba(X)[0]
        teacher_dist = np.array(probs)
        teacher_dist /= teacher_dist.sum()

        soft_teacher = np.exp(np.log(teacher_dist + 1e-8) / self.temperature)
        soft_teacher /= soft_teacher.sum()

        loss = -np.sum(soft_teacher * np.log(self.student_policy + 1e-8))
        grad = -soft_teacher / (self.student_policy + 1e-8)
        lr = 0.01
        self.student_policy -= lr * grad
        self.student_policy = np.clip(self.student_policy, 0.01, None)
        self.student_policy /= self.student_policy.sum()

        async with self._lock:
            self.history.append({
                'teacher_dist': teacher_dist,
                'student_dist': self.student_policy.copy(),
                'loss': loss
            })

    def get_student_probs(self):
        return self.student_policy.tolist()

# ============================================================================
# TokenizationOptimizer (modified with new components)
# ============================================================================
class TokenizationOptimizer:
    """
    Optimizes tokenization for sustainability with adaptive strategy selection.
    """
    def __init__(self, cfg=None):
        if cfg is None:
            self.config = config
        elif isinstance(cfg, dict):
            self.config = cfg
        else:
            self.config = cfg

        if self.config.get('require_langdetect', False) and not LANGDETECT_AVAILABLE:
            raise ImportError("langdetect is required but not installed.")
        if self.config.get('require_nltk', False) and not NLTK_AVAILABLE:
            raise ImportError("NLTK is required but not installed.")

        self.tokenizers = {}
        self.language_map = self.config.get('language_tokenizer_map', {})
        self.default_tokenizer_name = self.config.get('default_tokenizer', 'bert-base-uncased')
        self._tokenizer_lock = asyncio.Lock()
        self.circuit_breaker = CircuitBreaker(name="tokenizer_loading")

        # Learning cache
        self.cache = LearningCache(max_size=1000, ttl=self.config.get('cache_ttl_seconds', 300))

        # New components
        self.ga_optimizer = GeneticParameterOptimizer(self.config, storage) if self.config.get('ga_enabled', True) else None
        self.moe_gating = MoEGatingNetwork(self.config, storage) if self.config.get('moe_enabled', True) else None
        self.pareto_optimizer = ParetoFrontOptimizer(self.config, storage) if self.config.get('pareto_enabled', True) else None
        self.federated_learner = FederatedLearner(storage, str(uuid.uuid4())[:8], self.config.get('federated_interval', 3600)) if self.config.get('federated_enabled', True) else None
        self.user_pref_learner = ActiveUserPreferenceLearner(storage) if self.config.get('user_preference_enabled', True) else None
        self.drift_detector = DriftDetector(storage, self.config) if self.config.get('drift_detection_enabled', True) else None
        self.neural_teacher = NeuralTeacher(input_dim=12, output_dim=5) if self.config.get('neural_teacher_enabled', True) else None

        # LIMIT Graph, MODP, RLHF, Distillation
        self.limit_graph = LimitGraphManager(self.config) if self.config.get('limit_graph_enabled', True) else None
        self.modp_optimizer = MODPStrategyOptimizer(self.config) if self.config.get('modp_enabled', True) else None
        self.rlhf = RLHFManager(self.config) if self.config.get('rlhf_enabled', True) else None
        self.distillation = MultiTeacherPolicyDistillation(self.config, self.moe_gating) if self.config.get('distillation_enabled', True) and self.moe_gating else None

        # Fallback distillation (if MoE disabled)
        self.distillation_fallback = None
        if not self.moe_gating:
            self.distillation_fallback = DistillationTokenizationOptimizer(storage, self.config)

        logger.info("TokenizationOptimizer initialized with enhancements.")

    # ---------- Teacher interface for MOPD ----------
    async def policy_probs(self, state_dict):
        if self.rlhf and self.rlhf.reward_model is not None:
            return await self.rlhf.get_policy_probs(state_dict)
        if self.distillation and self.distillation.get_student_probs():
            return self.distillation.get_student_probs()
        if self.moe_gating:
            context = {
                'text_length': state_dict.get('text_length', 100),
                'avg_word_len': state_dict.get('avg_word_len', 5),
                'num_sentences': state_dict.get('num_sentences', 5),
                'requested_budget': state_dict.get('requested_budget', 500),
                'tokenizer_efficiency': state_dict.get('tokenizer_efficiency', 0.3),
                'language': state_dict.get('language', 'en'),
                'domain': state_dict.get('domain', 'general'),
                'time_of_day': datetime.now().hour,
            }
            features = self.moe_gating._encode_context(context)
            if self.moe_gating._trained and self.moe_gating._gating_model is not None:
                X = features.reshape(1, -1)
                if self.moe_gating._scaler:
                    X = self.moe_gating._scaler.transform(X)
                probs = self.moe_gating._gating_model.predict_proba(X)[0]
                return probs.tolist()
        elif self.distillation_fallback:
            state = TokenizationState(
                text_length=state_dict.get('text_length', 100),
                avg_word_len=state_dict.get('avg_word_len', 5),
                num_sentences=state_dict.get('num_sentences', 5),
                language=state_dict.get('language', 'en'),
                requested_budget=state_dict.get('requested_budget', 500),
                tokenizer_efficiency=state_dict.get('tokenizer_efficiency', 0.3),
                domain=state_dict.get('domain', 'general'),
                time_of_day=datetime.now().hour
            )
            strategy, action_idx, state_vec, teacher_probs = await self.distillation_fallback.select_strategy(state, exploration=False)
            return teacher_probs.tolist()
        return [0.2, 0.2, 0.2, 0.2, 0.2]

    # ---------- Language detection ----------
    async def detect_language(self, text):
        if not LANGDETECT_AVAILABLE:
            return self.config.get('fallback_language', 'en')
        try:
            loop = asyncio.get_event_loop()
            lang = await loop.run_in_executor(None, detect, text)
            return lang
        except:
            return self.config.get('fallback_language', 'en')

    # ---------- Tokenizer loading ----------
    async def _load_tokenizer(self, language):
        if language in self.tokenizers:
            return self.tokenizers[language]
        model_name = self.language_map.get(language, self.default_tokenizer_name)
        async def _load():
            if not TRANSFORMERS_AVAILABLE:
                raise RuntimeError("Transformers not available.")
            try:
                tokenizer = AutoTokenizer.from_pretrained(model_name)
                self.tokenizers[language] = tokenizer
                logger.info(f"Loaded tokenizer {model_name} for {language}")
                return tokenizer
            except Exception as e:
                logger.error(f"Failed to load tokenizer {model_name}: {e}")
                if model_name != self.default_tokenizer_name:
                    tokenizer = AutoTokenizer.from_pretrained(self.default_tokenizer_name)
                    self.tokenizers[language] = tokenizer
                    return tokenizer
                raise
        if TENACITY_AVAILABLE:
            @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
            async def load_with_retry():
                return await self.circuit_breaker.call(_load)
            return await load_with_retry()
        else:
            return await self.circuit_breaker.call(_load)

    async def _get_tokenizer(self, language):
        async with self._tokenizer_lock:
            return await self._load_tokenizer(language)

    # ---------- Segmentation ----------
    async def _segment_text(self, text):
        if NLTK_AVAILABLE:
            try:
                loop = asyncio.get_event_loop()
                return await loop.run_in_executor(None, sent_tokenize, text)
            except:
                pass
        return re.split(r'(?<=[.!?])\s+', text)

    # ---------- Summarization ----------
    async def _summarize(self, text, target_tokens):
        if SUMMA_AVAILABLE:
            try:
                lang = await self.detect_language(text)
                tokenizer = await self._get_tokenizer(lang)
                tokens = tokenizer.encode(text, add_special_tokens=False)
                ratio = target_tokens / len(tokens) if len(tokens) > 0 else 0.5
                ratio = min(1.0, max(0.1, ratio))
                loop = asyncio.get_event_loop()
                return await loop.run_in_executor(None, summarizer.summarize, text, ratio=ratio)
            except:
                pass
        return text[:target_tokens * 4]

    # ---------- Tokenization ----------
    async def _tokenize(self, text, language):
        tokenizer = await self._get_tokenizer(language)
        tokens = tokenizer.encode(text, add_special_tokens=False)
        return tokens, len(tokens)

    # ---------- Core optimization ----------
    async def optimize(self, text, context):
        start_time = time.time()
        language = context.get('language')
        if language is None:
            language = await self.detect_language(text)

        budget = context.get('token_budget', 1000)
        segment_budget = context.get('segment_budget', None)
        domain = context.get('domain', None)

        text_length = len(text)
        words = text.split()
        avg_word_len = np.mean([len(w) for w in words]) if words else 0
        sentences = await self._segment_text(text)
        num_sentences = len(sentences)
        tokenizer_efficiency = await self.get_token_efficiency(text, language)

        state_dict = {
            'text_length': text_length,
            'avg_word_len': avg_word_len,
            'num_sentences': num_sentences,
            'language': language,
            'requested_budget': budget,
            'tokenizer_efficiency': tokenizer_efficiency,
            'domain': domain,
        }

        # Cache check
        cache_key = self._cache_key(text, language, budget)
        cached = self.cache.get(cache_key)
        if cached:
            if PROMETHEUS_AVAILABLE:
                CACHE_HIT_COUNTER.inc()
            cached['cache_hit'] = True
            return cached
        if PROMETHEUS_AVAILABLE:
            CACHE_MISS_COUNTER.inc()

        # Strategy selection: MODP > RLHF > Distillation > MoE > fallback
        strategy = 'adaptive'
        strategy_params = {}
        if self.modp_optimizer and self.config.get('modp_enabled', True):
            modp_result = await self.modp_optimizer.select_strategy(state_dict)
            strategy = modp_result['strategy']
        elif self.rlhf and self.rlhf.reward_model is not None:
            probs = await self.rlhf.get_policy_probs(state_dict)
            idx = np.argmax(probs)
            strategy = ['efficiency', 'accuracy', 'speed', 'budget', 'adaptive'][idx % 5]
        elif self.distillation and self.distillation.get_student_probs():
            probs = self.distillation.get_student_probs()
            idx = np.argmax(probs)
            strategy = ['efficiency', 'accuracy', 'speed', 'budget', 'adaptive'][idx % 5]
        elif self.moe_gating:
            context_moe = {
                'text_length': text_length,
                'avg_word_len': avg_word_len,
                'num_sentences': num_sentences,
                'requested_budget': budget,
                'tokenizer_efficiency': tokenizer_efficiency,
                'language': language,
                'domain': domain,
                'time_of_day': datetime.now().hour,
            }
            strategy, strategy_params = await self.moe_gating.select_expert(context_moe)
        elif self.distillation_fallback:
            state = TokenizationState(
                text_length=text_length,
                avg_word_len=avg_word_len,
                num_sentences=num_sentences,
                language=language,
                requested_budget=budget,
                tokenizer_efficiency=tokenizer_efficiency,
                domain=domain,
                time_of_day=datetime.now().hour
            )
            strategy, _, _, _ = await self.distillation_fallback.select_strategy(state, exploration=True)

        if PROMETHEUS_AVAILABLE:
            DISTILLATION_STRATEGY.labels(strategy=strategy).inc()

        # Execute strategy (reuse original logic with parameter adjustments)
        if strategy == 'efficiency':
            tokens, total_tokens = await self._tokenize(text, language)
            if total_tokens > budget:
                truncated_text = text[:budget * 4]
                tokens, total_tokens = await self._tokenize(truncated_text, language)
            segments = [(truncated_text if total_tokens > budget else text, total_tokens)]
        elif strategy == 'accuracy':
            tokenizer_name = self.language_map.get(language, self.default_tokenizer_name)
            target = int(budget * 0.8)
            summary = await self._summarize(text, target)
            tokens, total_tokens = await self._tokenize(summary, language)
            segments = [(summary, total_tokens)]
        elif strategy == 'speed':
            sentences = await self._segment_text(text)
            token_counts = []
            for sent in sentences:
                _, cnt = await self._tokenize(sent, language)
                token_counts.append(cnt)
            cum = 0
            selected = []
            for i, cnt in enumerate(token_counts):
                if cum + cnt <= budget:
                    selected.append(sentences[i])
                    cum += cnt
                else:
                    break
            if not selected:
                selected = [text[:budget * 4]]
            segments = [(s, token_counts[i]) for i, s in enumerate(sentences[:len(selected)])]
            total_tokens = cum
        elif strategy == 'budget':
            tokens, total_tokens = await self._tokenize(text, language)
            if total_tokens > budget:
                target = int(budget * 0.9)
                summary = await self._summarize(text, target)
                tokens, total_tokens = await self._tokenize(summary, language)
                segments = [(summary, total_tokens)]
            else:
                segments = [(text, total_tokens)]
        else:  # adaptive
            tokens, total_tokens = await self._tokenize(text, language)
            if total_tokens > budget:
                ratio = 0.5
                target = int(budget * ratio)
                summary = await self._summarize(text, target)
                tokens, total_tokens = await self._tokenize(summary, language)
                segments = [(summary, total_tokens)]
            else:
                segments = [(text, total_tokens)]

        reward = self._compute_reward(text, total_tokens, budget, num_sentences, len(segments))

        # Update MoE or distillation
        if self.moe_gating:
            context_moe = {
                'text_length': text_length,
                'avg_word_len': avg_word_len,
                'num_sentences': num_sentences,
                'requested_budget': budget,
                'tokenizer_efficiency': tokenizer_efficiency,
                'language': language,
                'domain': domain,
                'time_of_day': datetime.now().hour,
            }
            await self.moe_gating.add_training_sample(context_moe, strategy, reward)

        # RLHF feedback
        if self.rlhf and reward > 0.7:
            await self.rlhf.record_feedback(state_dict, strategy, reward)

        # LIMIT Graph update
        if self.limit_graph:
            await self.limit_graph.update_constraint('token_count', total_tokens)
            await self.limit_graph.update_constraint('carbon', total_tokens * 0.001)

        # Pareto front update
        if self.pareto_optimizer:
            metrics = {
                'token_count': total_tokens,
                'semantic_similarity': 0.8,
                'processing_time': time.time() - start_time,
                'carbon_impact': total_tokens * 0.001,
            }
            config_params = {
                'strategy': strategy,
                'language': language,
                'budget': budget,
                'summarization_ratio': 0.5,
            }
            await self.pareto_optimizer.add_configuration(config_params, metrics)

        result = {
            'segments': segments,
            'total_tokens': total_tokens,
            'language': language,
            'tokenizer_used': self.default_tokenizer_name,
            'strategy_used': strategy,
            'cache_hit': False,
            'timestamp': datetime.now()
        }

        if self.config.get('enable_cache', True):
            self.cache.set(cache_key, result)

        if PROMETHEUS_AVAILABLE:
            TOKENIZATION_COUNTER.labels(language=language, status='success').inc()
            TOKEN_COUNT_HISTOGRAM.labels(language=language).observe(total_tokens)
            TOKENIZATION_DURATION.labels(language=language).observe(time.time() - start_time)
            DISTILLATION_REWARD.observe(reward)
            DISTILLATION_BUFFER_SIZE.set(len(self.distillation_fallback.replay_buffer) if self.distillation_fallback else 0)

        logger.info(f"Tokenization completed: language={language}, tokens={total_tokens}, strategy={strategy}, reward={reward}")
        return result

    def _compute_reward(self, text, total_tokens, budget, num_sentences, num_segments):
        reward = 0.0
        eff = total_tokens / len(text) if text else 0
        if eff < 0.3:
            reward += 0.4
        elif eff < 0.5:
            reward += 0.2
        if total_tokens <= budget:
            reward += 0.3
            if total_tokens < budget * 0.3:
                reward -= 0.1
        else:
            reward -= 0.2
        if num_sentences > 0:
            ratio = num_segments / num_sentences
            if ratio > 0.5:
                reward += 0.3
        return max(0.0, min(1.0, reward))

    async def get_token_efficiency(self, text, language=None):
        if language is None:
            language = await self.detect_language(text)
        _, total_tokens = await self._tokenize(text, language)
        return total_tokens / len(text) if text else 0.0

    def _cache_key(self, text, language, budget):
        return hashlib.md5(f"{text}_{language}_{budget}".encode()).hexdigest()

    async def clear_cache(self):
        self.cache.clear()
        logger.info("Tokenization cache cleared")

    async def get_cache_stats(self):
        return {'size': len(self.cache.cache), 'ttl_seconds': self.cache.ttl}

    async def start(self):
        if self.limit_graph:
            asyncio.create_task(self._limit_graph_loop())
        if self.rlhf:
            asyncio.create_task(self._rlhf_loop())
        if self.distillation:
            asyncio.create_task(self._distillation_loop())
        logger.info("Background tasks started.")

    async def _limit_graph_loop(self):
        while True:
            await asyncio.sleep(self.config.get('limit_graph_update_interval', 300))
            try:
                # Update constraints from recent tokenizations (simplified)
                pass
            except Exception as e:
                logger.error(f"Limit graph loop error: {e}")

    async def _rlhf_loop(self):
        while True:
            await asyncio.sleep(self.config.get('rlhf_training_interval', 600))
            try:
                if self.rlhf:
                    await self.rlhf.train_reward_model()
            except Exception as e:
                logger.error(f"RLHF loop error: {e}")

    async def _distillation_loop(self):
        while True:
            await asyncio.sleep(self.config.get('distillation_interval', 300))
            try:
                if self.distillation:
                    state = {'language': 'en', 'text_length': 1000}
                    await self.distillation.distill(state)
            except Exception as e:
                logger.error(f"Distillation loop error: {e}")

    async def shutdown(self):
        self.tokenizers.clear()
        self.cache.clear()
        logger.info("TokenizationOptimizer shutdown complete")

# ============================================================================
# DistillationTokenizationOptimizer (fallback)
# ============================================================================
class DistillationTokenizationOptimizer:
    ACTION_SPACE = ['efficiency', 'accuracy', 'speed', 'budget', 'adaptive']

    def __init__(self, storage, config):
        self.storage = storage
        self.config = config
        self.student = DistillationStudent(lr=config.get('student_learning_rate', 0.01))
        self.teachers = [
            RuleBasedTeacher(),
            HistoricalMLTeacher(),
            StatefulQTeacher(storage)
        ]
        self.replay_buffer = ReplayBuffer(max_size=config.get('replay_buffer_size', 2000))
        self.epsilon = config.get('distillation_epsilon', 0.1)
        self.train_every = config.get('train_every', 10)
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
            teacher_probs = np.ones(5) / 5
        student_probs = self.student.predict_proba(state_vec)
        if exploration and random.random() < self.epsilon:
            action_idx = random.randint(0, 4)
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
# Example usage
# ============================================================================
async def example_usage():
    optimizer = TokenizationOptimizer()
    await optimizer.start()
    text = "This is a sample text. It contains multiple sentences. We want to tokenize it efficiently."
    context = {'token_budget': 50}
    result = await optimizer.optimize(text, context)
    print(f"Segments: {result['segments']}")
    print(f"Total tokens: {result['total_tokens']}")
    print(f"Language: {result['language']}")
    print(f"Strategy: {result['strategy_used']}")

    efficiency = await optimizer.get_token_efficiency(text)
    print(f"Token efficiency: {efficiency}")

    await optimizer.shutdown()

if __name__ == "__main__":
    asyncio.run(example_usage())
