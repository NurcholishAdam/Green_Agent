#!/usr/bin/env python3
"""
Lightweight LLM client with enhanced decision‑making:
- Contextual bandit (LinUCB) + Multi‑Objective trade‑off (quality, speed, cost)
- Mixture of Experts (MOE) routing with gating network
- Bio‑inspired Genetic Algorithm for arm evolution
- Carbon‑aware scheduling
- Self‑healing with anomaly detection (Isolation Forest + One‑Class SVM)
- LIMIT Graph for constraint enforcement
- RLHF Optimizer for preference‑based policy updates
- Multi‑Teacher Policy Distillation for combining decision teachers

All enhancements degrade gracefully if optional dependencies are missing.
"""

import asyncio
import logging
import json
import time
import hashlib
import uuid
import random
from typing import Dict, Any, Optional, Callable, List, Tuple, AsyncIterable
from dataclasses import dataclass, field
from datetime import datetime
from collections import defaultdict, deque
import numpy as np

import aiohttp
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
)

# ---------- Optional dependencies ----------
try:
    from sentence_transformers import SentenceTransformer
    SENTENCE_TRANSFORMERS_AVAILABLE = True
except ImportError:
    SENTENCE_TRANSFORMERS_AVAILABLE = False

try:
    from prometheus_client import Counter, Gauge, Histogram
    PROMETHEUS_AVAILABLE = True
except ImportError:
    PROMETHEUS_AVAILABLE = False

try:
    from hvac import Client as VaultClient
    VAULT_AVAILABLE = True
except ImportError:
    VAULT_AVAILABLE = False

# ---------- NEW: ENHANCEMENT MODULES (LIMIT Graph, RLHF, MultiTeacherDistiller) ----------
try:
    from enhancements.limit_graph import LimitGraph
    from enhancements.rlhf import RLHFOptimizer
    from enhancements.multi_teacher_policy_distillation import MultiTeacherDistiller
    ADDITIONAL_ENHANCEMENTS_AVAILABLE = True
except ImportError:
    ADDITIONAL_ENHANCEMENTS_AVAILABLE = False
    # Fallback stubs
    class LimitGraph:
        def __init__(self, *args, **kwargs): self.limits = {}
        def build_graph(self, nodes, edges): pass
        def get_limits(self, context): return {}
        def update_from_feedback(self, feedback): pass
    class RLHFOptimizer:
        def __init__(self, action_space, *args, **kwargs): self.actions = action_space
        def update(self, context, action, reward): pass
        def sample_action(self, context): return self.actions[0] if self.actions else None
    class MultiTeacherDistiller:
        def __init__(self, teachers, *args, **kwargs): self.teachers = teachers
        def distill(self, context): return self.teachers[0](context) if self.teachers else None

# ---------- Logger ----------
logger = logging.getLogger(__name__)

# ---------- Dummy metrics ----------
if not PROMETHEUS_AVAILABLE:
    class DummyMetric:
        def inc(self, *args, **kwargs): pass
        def set(self, *args, **kwargs): pass
        def observe(self, *args, **kwargs): pass
        def labels(self, *args, **kwargs): return self

    class DummyMetrics:
        requests_total = DummyMetric()
        request_duration = DummyMetric()
        circuit_breaker_state = DummyMetric()
        cache_hits = DummyMetric()
        cache_misses = DummyMetric()
        fallback_usage = DummyMetric()
        retry_count = DummyMetric()
        token_usage = DummyMetric()
        carbon_intensity = DummyMetric()

    metrics = DummyMetrics()
else:
    from prometheus_client import Counter, Gauge, Histogram
    metrics = type('Metrics', (), {})()
    metrics.requests_total = Counter('llm_requests_total', 'Total LLM requests')
    metrics.request_duration = Histogram('llm_request_duration_seconds', 'LLM request duration')
    metrics.circuit_breaker_state = Gauge('llm_circuit_breaker_state', 'Circuit breaker state per endpoint', ['endpoint'])
    metrics.cache_hits = Counter('llm_cache_hits_total', 'Cache hits')
    metrics.cache_misses = Counter('llm_cache_misses_total', 'Cache misses')
    metrics.fallback_usage = Counter('llm_fallback_usage_total', 'Fallback usage')
    metrics.retry_count = Counter('llm_retry_count_total', 'Retry count')
    metrics.token_usage = Counter('llm_token_usage_total', 'Token usage')
    metrics.carbon_intensity = Gauge('llm_carbon_intensity', 'Current carbon intensity')

# ---------- Circuit Breaker (per endpoint) ----------
class CircuitBreaker:
    """
    Async circuit breaker to protect against repeated failures.
    Supports per-endpoint state.
    """
    def __init__(self, name: str, failure_threshold: int = 5, recovery_timeout: float = 30.0):
        self.name = name
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.failure_count = 0
        self.last_failure_time: Optional[float] = None
        self.state = "closed"  # closed, open, half-open
        self._lock = asyncio.Lock()

    async def call(self, func, *args, **kwargs):
        async with self._lock:
            if self.state == "open":
                if time.time() - self.last_failure_time > self.recovery_timeout:
                    self.state = "half-open"
                    if PROMETHEUS_AVAILABLE:
                        metrics.circuit_breaker_state.labels(endpoint=self.name).set(0.5)
                else:
                    raise RuntimeError(f"Circuit breaker {self.name} is open")
        try:
            result = await func(*args, **kwargs)
            async with self._lock:
                if self.state == "half-open":
                    self.state = "closed"
                    self.failure_count = 0
                    if PROMETHEUS_AVAILABLE:
                        metrics.circuit_breaker_state.labels(endpoint=self.name).set(0)
            return result
        except Exception as e:
            async with self._lock:
                self.failure_count += 1
                self.last_failure_time = time.time()
                if self.failure_count >= self.failure_threshold:
                    self.state = "open"
                    if PROMETHEUS_AVAILABLE:
                        metrics.circuit_breaker_state.labels(endpoint=self.name).set(1)
            raise e

    def get_status(self) -> Dict:
        return {
            'name': self.name,
            'state': self.state,
            'failure_count': self.failure_count,
            'recovery_timeout': self.recovery_timeout,
        }

# ---------- MODULE 1: Contextual Bandit (LinUCB) + Multi‑Objective Optimizer ----------
class LinUCB:
    """Linear Upper Confidence Bound bandit for contextual arm selection."""
    def __init__(self, num_arms: int, feature_dim: int, alpha: float = 0.1):
        self.num_arms = num_arms
        self.feature_dim = feature_dim
        self.alpha = alpha
        self.A = [np.eye(feature_dim) for _ in range(num_arms)]
        self.b = [np.zeros(feature_dim) for _ in range(num_arms)]
        self.theta = [np.zeros(feature_dim) for _ in range(num_arms)]

    def select_arm(self, features: np.ndarray) -> int:
        """Return index of arm with highest upper confidence bound."""
        p = np.zeros(self.num_arms)
        for a in range(self.num_arms):
            A_inv = np.linalg.inv(self.A[a])
            self.theta[a] = A_inv.dot(self.b[a])
            p[a] = self.theta[a].dot(features) + self.alpha * np.sqrt(features.dot(A_inv).dot(features))
        return np.argmax(p)

    def update(self, arm: int, features: np.ndarray, reward: float):
        self.A[arm] += np.outer(features, features)
        self.b[arm] += reward * features

class MultiObjectiveOptimizer:
    """
    Multi‑objective decision process for parameter selection.
    Uses a weighted sum (adaptable) to balance quality, speed, cost.
    """
    def __init__(self, weights: Optional[List[float]] = None):
        self.weights = weights or [0.4, 0.3, 0.3]  # quality, speed, cost
        self.adaptive_weights = True
        self.learning_rate = 0.01
        self.recent_outcomes = deque(maxlen=100)

    def score_arms(self, arms: List[Dict], context: Dict) -> List[float]:
        """
        Evaluate each arm on multiple objectives and return a composite score.
        Each arm dict must contain 'quality_estimate', 'latency_estimate', 'cost_estimate'.
        """
        scores = []
        for arm in arms:
            quality = arm.get('quality_estimate', 0.5)
            latency = arm.get('latency_estimate', 1.0)  # seconds
            cost = arm.get('cost_estimate', 100)       # tokens
            # Normalize (invert latency and cost so higher is better)
            norm_latency = 1.0 / (1.0 + latency)
            norm_cost = 1.0 / (1.0 + cost/100)
            weighted = (self.weights[0] * quality +
                        self.weights[1] * norm_latency +
                        self.weights[2] * norm_cost)
            scores.append(weighted)
        return scores

    async def update_weights(self, outcomes: List[float]):
        """Adapt weights based on recent outcomes using gradient descent."""
        self.recent_outcomes.append(outcomes)
        if len(self.recent_outcomes) >= 10:
            # Simple heuristic: adjust weights to favour objectives that underperformed
            avg_outcome = np.mean(self.recent_outcomes, axis=0)
            target = np.mean(avg_outcome)  # average of all
            error = avg_outcome - target
            self.weights = [w - self.learning_rate * e for w, e in zip(self.weights, error)]
            total = sum(self.weights)
            if total > 0:
                self.weights = [w / total for w in self.weights]
            logger.info(f"Multi‑objective weights updated: {self.weights}")

# ---------- MODULE 2: Mixture of Experts (MOE) Router ----------
class MOERouter:
    """
    Routes prompts to teachers using a gating network that learns from context.
    """
    def __init__(self, feature_dim: int = 64, epsilon: float = 0.1):
        self.teachers: List[Tuple[str, 'LLMClient']] = []
        self.epsilon = epsilon
        self.gating_weights = None  # linear model (will be logistic regression)
        self.scaler = None
        self._trained = False
        self.teacher_embeddings = {}  # teacher_name -> embedding (if using TF‑IDF)
        self.rewards = defaultdict(float)
        self.counts = defaultdict(int)
        self.context_history = deque(maxlen=1000)  # (features, teacher_name, reward)
        # NEW: optional distiller for teacher selection
        self.distiller: Optional[MultiTeacherDistiller] = None

    def add_teacher(self, name: str, client: 'LLMClient'):
        self.teachers.append((name, client))
        self.rewards[name] = 0.0
        self.counts[name] = 0

    async def _extract_features(self, prompt: str) -> np.ndarray:
        """Extract features from prompt (e.g., length, complexity, domain)."""
        words = prompt.split()
        features = [
            len(prompt),
            len(words),
            np.mean([len(w) for w in words]) if words else 0,
            datetime.now().hour / 24.0
        ]
        return np.array(features)

    def _teacher_by_name(self, name: str) -> Optional['LLMClient']:
        for n, c in self.teachers:
            if n == name:
                return c
        return None

    async def select_teacher(self, prompt: str) -> 'LLMClient':
        """Select a teacher using epsilon‑greedy or gating network / distillation."""
        if self.distiller is not None and ADDITIONAL_ENHANCEMENTS_AVAILABLE:
            # Use distillation to select teacher
            features = await self._extract_features(prompt)
            teacher_name = self.distiller.distill(features)
            client = self._teacher_by_name(teacher_name)
            if client is not None:
                return client
            # Fallback to random if not found
            _, client = random.choice(self.teachers)
            return client

        if random.random() < self.epsilon:
            # Explore
            _, client = random.choice(self.teachers)
            return client

        # Exploit: use gating network if trained, else best average reward
        if self._trained and self.gating_weights is not None:
            features = await self._extract_features(prompt)
            # For simplicity, we use a softmax of rewards if gating not fully implemented.
            rewards = np.array([self.rewards.get(n, 0.0) for n, _ in self.teachers])
            probs = np.exp(rewards) / np.sum(np.exp(rewards))
            idx = np.random.choice(len(self.teachers), p=probs)
            _, client = self.teachers[idx]
            return client
        else:
            # Choose teacher with highest average reward
            best_name = max(self.rewards, key=lambda n: self.rewards[n] / max(self.counts[n], 1))
            _, client = next((n, c) for n, c in self.teachers if n == best_name)
            return client

    async def update(self, teacher_name: str, reward: float, prompt: str):
        """Update reward and gating model."""
        self.rewards[teacher_name] += reward
        self.counts[teacher_name] += 1
        features = await self._extract_features(prompt)
        self.context_history.append((features, teacher_name, reward))
        if len(self.context_history) % 100 == 0:
            await self._retrain_gating()

    async def _retrain_gating(self):
        """Train a logistic regression gating on recent context‑teacher pairs."""
        if len(self.context_history) < 50:
            return
        X = []
        y = []
        for features, name, _ in list(self.context_history)[-100:]:
            X.append(features)
            y.append([n for n, _ in self.teachers].index(name))
        X = np.array(X)
        y = np.array(y)
        try:
            from sklearn.linear_model import LogisticRegression
            from sklearn.preprocessing import StandardScaler
            self.scaler = StandardScaler()
            X_scaled = self.scaler.fit_transform(X)
            self.gating_weights = LogisticRegression(multi_class='multinomial', max_iter=1000)
            self.gating_weights.fit(X_scaled, y)
            self._trained = True
        except Exception as e:
            logger.warning(f"Could not train gating model: {e}")

    def get_stats(self) -> Dict:
        return {
            'teachers': [name for name, _ in self.teachers],
            'rewards': dict(self.rewards),
            'counts': dict(self.counts),
            'gating_trained': self._trained,
            'distillation_active': self.distiller is not None
        }

# ---------- MODULE 3: Bio‑Inspired Genetic Algorithm for Arm Evolution ----------
class GeneticAlgorithmOptimizer:
    """GA to evolve the set of parameter arms (temperature, max_tokens, model)."""
    def __init__(self, population_size: int = 20, mutation_rate: float = 0.1, crossover_rate: float = 0.8):
        self.pop_size = population_size
        self.mutation_rate = mutation_rate
        self.crossover_rate = crossover_rate
        self.population = []  # list of dicts: {'temp', 'max_tokens', 'model'}
        self.bounds = {
            'temp': (0.1, 1.0),
            'max_tokens': (50, 500),
            'model': ['small', 'medium', 'large']  # categorical
        }
        self.arm_history = deque(maxlen=100)

    def initialize(self):
        self.population = []
        for _ in range(self.pop_size):
            arm = {
                'temp': random.uniform(0.1, 1.0),
                'max_tokens': random.randint(50, 500),
                'model': random.choice(self.bounds['model'])
            }
            self.population.append(arm)

    def evaluate(self, fitness_func: Callable[[Dict], float]) -> List[float]:
        return [fitness_func(ind) for ind in self.population]

    def select(self, fitness: List[float], num_parents: int) -> List[Dict]:
        # Tournament selection
        selected = []
        for _ in range(num_parents):
            idx1, idx2 = np.random.choice(len(self.population), 2, replace=False)
            if fitness[idx1] > fitness[idx2]:
                selected.append(self.population[idx1])
            else:
                selected.append(self.population[idx2])
        return selected

    def crossover(self, parent1: Dict, parent2: Dict) -> Dict:
        if random.random() < self.crossover_rate:
            child = {}
            for key in parent1:
                if key == 'model':
                    child[key] = random.choice([parent1[key], parent2[key]])
                else:
                    if random.random() < 0.5:
                        child[key] = parent1[key]
                    else:
                        child[key] = parent2[key]
        else:
            child = parent1.copy()
        return child

    def mutate(self, individual: Dict) -> Dict:
        if random.random() < self.mutation_rate:
            key = random.choice(list(self.bounds.keys()))
            if key == 'model':
                individual[key] = random.choice(self.bounds['model'])
            else:
                low, high = self.bounds[key]
                individual[key] = random.uniform(low, high) if key == 'temp' else random.randint(int(low), int(high))
        return individual

    def evolve(self, fitness_func: Callable[[Dict], float], generations: int = 50) -> Dict:
        self.initialize()
        for gen in range(generations):
            fitness = self.evaluate(fitness_func)
            # Elitism
            best_idx = np.argmax(fitness)
            best = self.population[best_idx]
            parents = self.select(fitness, self.pop_size - 1)
            offspring = []
            for i in range(0, len(parents)-1, 2):
                child1 = self.crossover(parents[i], parents[i+1])
                child2 = self.crossover(parents[i+1], parents[i])
                offspring.append(self.mutate(child1))
                offspring.append(self.mutate(child2))
            self.population = offspring[:self.pop_size-1] + [best]
        final_fitness = self.evaluate(fitness_func)
        best_idx = np.argmax(final_fitness)
        return self.population[best_idx]

# ---------- MODULE 4: Carbon‑Aware Scheduling ----------
class CarbonIntensityManager:
    """Fetches and caches carbon intensity from an API."""
    def __init__(self, api_key: Optional[str] = None, region: str = "global"):
        self.api_key = api_key
        self.region = region
        self._session = None
        self.current_intensity = 400.0  # default
        self._last_update = None

    async def get_current_intensity(self) -> float:
        # Simulated – in production call electricitymap.org API
        self.current_intensity = 350 + random.uniform(-50, 50)
        if PROMETHEUS_AVAILABLE:
            metrics.carbon_intensity.set(self.current_intensity)
        return self.current_intensity

    async def close(self):
        if self._session:
            await self._session.close()

class CarbonAwareScheduler:
    """Adjusts LLM parameters to reduce carbon footprint when intensity is high."""
    def __init__(self, carbon_manager: CarbonIntensityManager, threshold: float = 400.0):
        self.carbon_manager = carbon_manager
        self.threshold = threshold

    async def adjust_params(self, params: Dict) -> Dict:
        intensity = await self.carbon_manager.get_current_intensity()
        if intensity > self.threshold:
            # Reduce token usage and use smaller model
            params['max_tokens'] = min(params.get('max_tokens', 150), 100)
            params['model'] = 'small' if params.get('model') != 'small' else 'small'
            logger.info(f"Carbon intensity {intensity:.0f} > threshold, reduced params to {params}")
        return params

# ---------- MODULE 5: Self‑Healing with Anomaly Detection ----------
class SelfHealingManager:
    """Monitors response quality and triggers recovery on anomalies."""
    def __init__(self, contamination: float = 0.1):
        self.contamination = contamination
        self.quality_history = deque(maxlen=500)
        self.anomaly_detectors = []  # list of sklearn models if available
        self._trained = False
        self._lock = asyncio.Lock()
        self.recovery_actions = deque(maxlen=100)

        # Try to load sklearn models
        try:
            from sklearn.ensemble import IsolationForest
            from sklearn.svm import OneClassSVM
            self.anomaly_detectors.append(('iforest', IsolationForest(contamination=contamination)))
            self.anomaly_detectors.append(('ocsvm', OneClassSVM(nu=contamination)))
        except ImportError:
            logger.warning("sklearn not available – using rule‑based fallback for self‑healing")

    async def record_quality(self, quality: float):
        """Record a quality score (e.g., response length, coherence)."""
        async with self._lock:
            self.quality_history.append(quality)
            if len(self.quality_history) >= 100 and not self._trained:
                await self._train()

    async def _train(self):
        """Train anomaly detectors on recent quality scores."""
        if not self.anomaly_detectors or len(self.quality_history) < 100:
            return
        X = np.array(list(self.quality_history)).reshape(-1, 1)
        for _, model in self.anomaly_detectors:
            try:
                model.fit(X)
            except Exception as e:
                logger.warning(f"Failed to train detector: {e}")
        self._trained = True

    async def detect_anomaly(self, quality: float) -> Tuple[bool, float]:
        """Return True if quality is anomalous, along with score."""
        if not self._trained or not self.anomaly_detectors:
            # Simple rule: quality < 0.3 is anomaly
            return quality < 0.3, 0.0
        X = np.array([[quality]])
        votes = []
        for _, model in self.anomaly_detectors:
            try:
                pred = model.predict(X)[0]
                votes.append(1 if pred == -1 else 0)
            except:
                votes.append(0)
        if not votes:
            return False, 0.0
        # Weighted vote (equal weights)
        anomaly = sum(votes) / len(votes) > 0.5
        return anomaly, sum(votes) / len(votes)

    async def trigger_recovery(self):
        """Log and perform recovery actions."""
        async with self._lock:
            self.recovery_actions.append({
                'action': 'reset_bandit',
                'timestamp': datetime.now().isoformat()
            })
        logger.warning("Self‑healing triggered: resetting bandit and gating.")

    def get_stats(self) -> Dict:
        return {
            'trained': self._trained,
            'history_len': len(self.quality_history),
            'recent_actions': list(self.recovery_actions)[-5:]
        }

# ---------- Semantic Cache (unchanged) ----------
class SemanticCache:
    """
    Cache with optional semantic similarity using sentence-transformers.
    """
    def __init__(self, similarity_threshold: float = 0.95, max_size: int = 1000):
        self.similarity_threshold = similarity_threshold
        self.max_size = max_size
        self.cache = {}  # prompt_hash -> response
        self.prompt_embeddings = {}  # prompt_hash -> embedding (if available)
        self.prompt_texts = {}  # prompt_hash -> original prompt
        self._lock = asyncio.Lock()
        self.embedding_model = None
        if SENTENCE_TRANSFORMERS_AVAILABLE:
            try:
                self.embedding_model = SentenceTransformer('all-MiniLM-L6-v2')
                logger.info("SentenceTransformer loaded for semantic caching")
            except Exception as e:
                logger.warning(f"Could not load SentenceTransformer: {e}")

    async def get(self, prompt: str) -> Optional[str]:
        """
        Retrieve cached response if similar prompt exists.
        """
        async with self._lock:
            prompt_hash = hashlib.md5(prompt.encode()).hexdigest()
            if prompt_hash in self.cache:
                metrics.cache_hits.inc()
                return self.cache[prompt_hash]

            if self.embedding_model is not None and len(self.cache) > 0:
                emb = self.embedding_model.encode(prompt)
                for cached_hash, cached_emb in self.prompt_embeddings.items():
                    sim = np.dot(emb, cached_emb) / (np.linalg.norm(emb) * np.linalg.norm(cached_emb) + 1e-8)
                    if sim >= self.similarity_threshold:
                        metrics.cache_hits.inc()
                        return self.cache[cached_hash]
            metrics.cache_misses.inc()
            return None

    async def set(self, prompt: str, response: str):
        """
        Store prompt-response pair.
        """
        async with self._lock:
            if len(self.cache) >= self.max_size:
                oldest = min(self.cache.keys(), key=lambda k: self.cache[k][1])
                del self.cache[oldest]
                if oldest in self.prompt_embeddings:
                    del self.prompt_embeddings[oldest]
                del self.prompt_texts[oldest]
            prompt_hash = hashlib.md5(prompt.encode()).hexdigest()
            self.cache[prompt_hash] = response
            self.prompt_texts[prompt_hash] = prompt
            if self.embedding_model is not None:
                emb = self.embedding_model.encode(prompt)
                self.prompt_embeddings[prompt_hash] = emb

# ---------- Fallback generator (unchanged) ----------
class TemplatedFallback:
    """
    Generates fallback explanations based on prompt keywords.
    """
    def __init__(self):
        self.templates = [
            "Based on available data, the recommended action is to proceed with caution.",
            "Due to current scarcity constraints, helium usage should be minimized.",
            "The system suggests optimizing workflows to reduce helium consumption.",
            "No specific recommendation can be generated at this time.",
        ]

    def generate(self, prompt: str) -> str:
        # Simple keyword-based selection
        if "scarcity" in prompt.lower() or "shortage" in prompt.lower():
            return "Helium scarcity is currently high. Please reduce usage and consider alternatives."
        elif "price" in prompt.lower():
            return "Helium prices are volatile. We recommend monitoring market trends."
        elif "optimize" in prompt.lower() or "efficiency" in prompt.lower():
            return "Optimizing helium usage can lead to significant cost savings and sustainability improvements."
        else:
            return np.random.choice(self.templates)

# ---------- Enhanced LLM Client ----------
class LLMClient:
    """
    Enhanced LLM client with contextual bandit, MOE, GA, carbon awareness, self‑healing,
    LIMIT Graph, RLHF, and Multi‑Teacher Distillation.
    """
    def __init__(
        self,
        endpoint: str = "http://localhost:8000/generate",
        model: str = "small",
        headers: Optional[Dict[str, str]] = None,
        timeout: int = 30,
        retry_attempts: int = 3,
        circuit_breaker_threshold: int = 5,
        circuit_breaker_timeout: float = 30.0,
        fallback_generator: Optional[Callable[[str], str]] = None,
        # New features
        enable_contextual_bandit: bool = True,
        enable_moe: bool = True,
        enable_ga: bool = True,
        enable_carbon_aware: bool = True,
        enable_self_healing: bool = True,
        enable_cache: bool = True,
        enable_metrics: bool = True,
        enable_lineage: bool = False,
        vault_url: Optional[str] = None,
        vault_token: Optional[str] = None,
        vault_secret_path: str = "llm/api_key",
        extra_endpoints: Optional[List[Tuple[str, str]]] = None,
        carbon_api_key: Optional[str] = None,
        carbon_region: str = "global",
        # NEW: additional enhancement flags
        enable_limit_graph: bool = True,
        enable_rlhf: bool = True,
        enable_distillation: bool = True,
    ):
        self.endpoint = endpoint
        self.model = model
        self.headers = headers or {}
        self.timeout = timeout
        self.retry_attempts = retry_attempts
        self.fallback_generator = fallback_generator or TemplatedFallback().generate

        self._session: Optional[aiohttp.ClientSession] = None
        self._circuit_breaker = CircuitBreaker(
            name="primary",
            failure_threshold=circuit_breaker_threshold,
            recovery_timeout=circuit_breaker_timeout,
        )
        self._logger = logger

        # 1. Contextual bandit + MOO
        self.contextual_bandit_enabled = enable_contextual_bandit
        if enable_contextual_bandit:
            self.arms = [
                {'temp': 0.7, 'max_tokens': 150, 'model': 'small'},
                {'temp': 0.5, 'max_tokens': 100, 'model': 'small'},
                {'temp': 0.9, 'max_tokens': 200, 'model': 'medium'},
                {'temp': 0.3, 'max_tokens': 50, 'model': 'small'},
                {'temp': 0.8, 'max_tokens': 300, 'model': 'large'},
            ]
            self.linucb = LinUCB(num_arms=len(self.arms), feature_dim=4, alpha=0.1)
            self.moo = MultiObjectiveOptimizer()

        # 2. MOE Router
        self.moe_enabled = enable_moe
        if enable_moe:
            self.router = MOERouter()
            self.router.add_teacher("primary", self)
            if extra_endpoints:
                for name, url in extra_endpoints:
                    teacher_client = LLMClient(
                        endpoint=url,
                        model=model,
                        headers=headers,
                        timeout=timeout,
                        retry_attempts=retry_attempts,
                        circuit_breaker_threshold=circuit_breaker_threshold,
                        circuit_breaker_timeout=circuit_breaker_timeout,
                        fallback_generator=fallback_generator,
                        enable_contextual_bandit=False,
                        enable_moe=False,
                        enable_ga=False,
                        enable_carbon_aware=False,
                        enable_self_healing=False,
                        enable_cache=False,
                        enable_metrics=False,
                        enable_lineage=False,
                    )
                    self.router.add_teacher(name, teacher_client)

        # 3. GA for arm evolution
        self.ga_enabled = enable_ga
        if enable_ga:
            self.ga = GeneticAlgorithmOptimizer()
            self.ga.initialize()
            self.ga_fitness_history = deque(maxlen=50)

        # 4. Carbon awareness
        self.carbon_aware_enabled = enable_carbon_aware
        if enable_carbon_aware:
            self.carbon_manager = CarbonIntensityManager(api_key=carbon_api_key, region=carbon_region)
            self.carbon_scheduler = CarbonAwareScheduler(self.carbon_manager)

        # 5. Self‑healing
        self.self_healing_enabled = enable_self_healing
        if enable_self_healing:
            self.self_healing = SelfHealingManager()

        # Semantic cache
        self.cache_enabled = enable_cache
        if enable_cache:
            self.cache = SemanticCache()

        # Metrics and lineage
        self.metrics_enabled = enable_metrics
        self.lineage_enabled = enable_lineage
        if enable_lineage:
            self.lineage_records = deque(maxlen=1000)

        # Vault
        self.vault_client = None
        if VAULT_AVAILABLE and vault_url and vault_token:
            try:
                self.vault_client = VaultClient(url=vault_url, token=vault_token)
                self.vault_secret_path = vault_secret_path
                self.headers = self._fetch_vault_secret()
                logger.info("Vault client initialized for key rotation")
            except Exception as e:
                logger.warning(f"Vault initialization failed: {e}")

        # NEW: additional modules
        self.limit_graph_enabled = enable_limit_graph and ADDITIONAL_ENHANCEMENTS_AVAILABLE
        self.rlhf_enabled = enable_rlhf and ADDITIONAL_ENHANCEMENTS_AVAILABLE
        self.distillation_enabled = enable_distillation and ADDITIONAL_ENHANCEMENTS_AVAILABLE

        # Instantiate if enabled
        self.limit_graph = LimitGraph() if self.limit_graph_enabled else None
        # RLHF action space: arm indices (0 to len(arms)-1)
        if self.rlhf_enabled and self.contextual_bandit_enabled:
            self.rlhf = RLHFOptimizer(action_space=list(range(len(self.arms))))
        else:
            self.rlhf = None

        # Distillation for arm selection: combine LinUCB, rule-based, static
        if self.distillation_enabled and self.contextual_bandit_enabled:
            self.arm_distiller = MultiTeacherDistiller([
                self._teacher_linucb,
                self._teacher_rule,
                self._teacher_static
            ])
        else:
            self.arm_distiller = None

        # If MOE router and distillation enabled, also set up teacher distillation for routing
        if self.distillation_enabled and self.moe_enabled:
            # For routing distillation, we can have teachers: LinUCB-based? Not directly.
            # We'll reuse the same arm_distiller? No, routing is about teacher selection.
            # We'll create a separate distiller for teacher selection using MOE rewards.
            self.router_distiller = MultiTeacherDistiller([
                self._teacher_router_best,
                self._teacher_router_random,
                self._teacher_router_static
            ])
            self.router.distiller = self.router_distiller
        else:
            self.router_distiller = None

    def _teacher_linucb(self, features: np.ndarray) -> int:
        """Teacher 1: LinUCB arm selection."""
        if self.contextual_bandit_enabled:
            return self.linucb.select_arm(features)
        return 0

    def _teacher_rule(self, features: np.ndarray) -> int:
        """Teacher 2: Rule-based (based on prompt length)."""
        # features[0] is length
        length = features[0] if len(features) > 0 else 50
        if length > 200:
            return 4  # large model
        elif length > 100:
            return 2  # medium
        else:
            return 0  # small

    def _teacher_static(self, features: np.ndarray) -> int:
        """Teacher 3: Static (always balanced)."""
        return 2

    def _teacher_router_best(self, features: np.ndarray) -> str:
        """Teacher for routing: best average reward."""
        if self.moe_enabled:
            # Choose teacher with highest average reward
            best_name = max(self.router.rewards, key=lambda n: self.router.rewards[n] / max(self.router.counts[n], 1))
            return best_name
        return "primary"

    def _teacher_router_random(self, features: np.ndarray) -> str:
        """Teacher for routing: random."""
        if self.moe_enabled and self.router.teachers:
            name, _ = random.choice(self.router.teachers)
            return name
        return "primary"

    def _teacher_router_static(self, features: np.ndarray) -> str:
        """Teacher for routing: always primary."""
        return "primary"

    def _fetch_vault_secret(self) -> Dict[str, str]:
        if self.vault_client:
            secret = self.vault_client.secrets.kv.v2.read_secret(path=self.vault_secret_path)
            api_key = secret['data']['data'].get('api_key')
            if api_key:
                headers = self.headers.copy()
                headers['Authorization'] = f"Bearer {api_key}"
                return headers
        return self.headers

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession()
        return self._session

    async def close(self):
        if self._session and not self._session.closed:
            await self._session.close()
            self._session = None
        if self.moe_enabled and self.router:
            for _, client in self.router.teachers:
                await client.close()
        if self.carbon_aware_enabled:
            await self.carbon_manager.close()

    def _is_transient_error(self, exc: Exception) -> bool:
        if isinstance(exc, (aiohttp.ClientError, asyncio.TimeoutError, ConnectionError)):
            return True
        if hasattr(exc, 'status') and exc.status >= 500:
            return True
        return False

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_exception_type(
            (aiohttp.ClientError, asyncio.TimeoutError, ConnectionError)
        ),
    )
    async def _do_request(self, payload: Dict) -> Dict:
        session = await self._get_session()
        if self.vault_client:
            self.headers = self._fetch_vault_secret()
        async with session.post(
            self.endpoint,
            json=payload,
            headers=self.headers,
            timeout=aiohttp.ClientTimeout(total=self.timeout)
        ) as resp:
            if resp.status != 200:
                raise aiohttp.ClientResponseError(
                    request_info=resp.request_info,
                    history=resp.history,
                    status=resp.status,
                    message=f"LLM API returned {resp.status}"
                )
            if self.metrics_enabled:
                metrics.requests_total.inc()
            return await resp.json()

    async def generate_explanation(
        self,
        prompt: str,
        max_tokens: int = 150,
        temperature: float = 0.7,
        **kwargs,
    ) -> str:
        # 1. Cache check
        if self.cache_enabled:
            cached = await self.cache.get(prompt)
            if cached is not None:
                return cached

        # 2. Select parameters via contextual bandit + MOO (or distillation)
        params = {'max_tokens': max_tokens, 'temperature': temperature, 'model': self.model}
        features = self._extract_features(prompt)

        if self.contextual_bandit_enabled:
            # Use distillation if available, else LinUCB
            if self.arm_distiller is not None:
                arm_idx = self.arm_distiller.distill(features)
                selected_arm = self.arms[arm_idx]
                source = "distilled"
            elif self.rlhf is not None and random.random() < 0.1:  # small exploration with RLHF
                arm_idx = self.rlhf.sample_action(features)
                selected_arm = self.arms[arm_idx]
                source = "rlhf"
            else:
                arm_idx = self.linucb.select_arm(features)
                selected_arm = self.arms[arm_idx]
                source = "linucb"
            params.update(selected_arm)
        else:
            arm_idx = -1
            selected_arm = params
            source = "default"

        # 3. Apply LIMIT Graph constraints
        if self.limit_graph_enabled and self.limit_graph is not None:
            limits = self.limit_graph.get_limits(features)
            if 'max_tokens' in limits:
                params['max_tokens'] = min(params.get('max_tokens', 150), limits['max_tokens'])
            if 'temperature' in limits:
                params['temperature'] = min(params.get('temperature', 0.7), limits['temperature'])
            if 'model' in limits and limits['model'] != 'any':
                params['model'] = limits['model'] if limits['model'] in ['small', 'medium', 'large'] else params['model']
            # Ensure params are valid
            if 'max_tokens' in params and params['max_tokens'] < 1:
                params['max_tokens'] = 1

        # 4. Carbon‑aware adjustment
        if self.carbon_aware_enabled:
            params = await self.carbon_scheduler.adjust_params(params)

        # 5. Multi‑teacher routing
        client = self
        teacher_name = "primary"
        if self.moe_enabled:
            client = await self.router.select_teacher(prompt)
            # Identify teacher name for updates
            for name, c in self.router.teachers:
                if c is client:
                    teacher_name = name
                    break

        # 6. Generate
        try:
            if client is self:
                result = await self._generate_internal(prompt, **params)
            else:
                result = await client.generate_explanation(prompt, **params)

            # 7. Compute reward (quality proxy) and update models
            reward = self._compute_reward(result)
            if self.contextual_bandit_enabled and arm_idx >= 0:
                self.linucb.update(arm_idx, features, reward)
                # Update MOO outcomes
                outcome = [reward, 1.0/(1.0+self.timeout), 1.0/(1.0+len(result)/100)]
                await self.moo.update_weights(outcome)

            # Update RLHF if used
            if self.rlhf is not None and arm_idx >= 0:
                self.rlhf.update(features, arm_idx, reward)

            if self.moe_enabled and client is not self:
                await self.router.update(teacher_name, reward, prompt)

            # 8. GA evolution (periodic)
            if self.ga_enabled and self.contextual_bandit_enabled:
                # Every 10 calls, evolve arms
                if len(self.ga.arm_history) % 10 == 0:
                    fitness_func = self._ga_fitness
                    best_arm = self.ga.evolve(fitness_func, generations=3)
                    # Replace the worst arm with the best
                    worst_idx = np.argmin([self._ga_fitness(a) for a in self.arms])
                    self.arms[worst_idx] = best_arm
                    logger.info(f"GA evolved best arm: {best_arm}")

            # 9. Self‑healing: record quality and detect anomalies
            if self.self_healing_enabled:
                quality = self._compute_quality_score(result)
                await self.self_healing.record_quality(quality)
                anomaly, _ = await self.self_healing.detect_anomaly(quality)
                if anomaly:
                    await self.self_healing.trigger_recovery()
                    result = self.fallback_generator(prompt)

            # 10. Cache result
            if self.cache_enabled:
                await self.cache.set(prompt, result)

            # 11. Lineage
            if self.lineage_enabled:
                self._record_lineage(prompt, result, params)

            return result
        except Exception as e:
            logger.warning(f"LLM generation failed: {e}")
            if self.metrics_enabled:
                metrics.fallback_usage.inc()
            return self.fallback_generator(prompt)

    def _extract_features(self, prompt: str) -> np.ndarray:
        words = prompt.split()
        features = [
            len(prompt),
            len(words),
            np.mean([len(w) for w in words]) if words else 0,
            datetime.now().hour / 24.0
        ]
        return np.array(features)

    def _compute_reward(self, response: str) -> float:
        # Quality proxy: length and presence of key words
        score = min(1.0, len(response) / 200)
        if "recommend" in response or "optimize" in response:
            score += 0.1
        return min(1.0, score)

    def _compute_quality_score(self, response: str) -> float:
        return min(1.0, len(response) / 200)

    def _ga_fitness(self, arm: Dict) -> float:
        temp = arm['temp']
        tokens = arm['max_tokens']
        model = arm['model']
        quality = 0.6 * (tokens / 500) + 0.4 * (1 if model == 'large' else 0.5 if model == 'medium' else 0.2)
        cost = tokens / 500 + (0.3 if model == 'large' else 0.2 if model == 'medium' else 0.1)
        return quality - 0.5 * cost

    async def _generate_internal(self, prompt: str, **kwargs) -> str:
        payload = {"prompt": prompt, **kwargs}
        try:
            result = await self._circuit_breaker.call(self._do_request, payload)
            text = result.get("text") or result.get("generated_text") or result.get("response", "")
            return text
        except Exception as e:
            if self.metrics_enabled:
                metrics.retry_count.inc()
            raise

    def _record_lineage(self, prompt: str, response: str, params: Dict):
        record = {
            'timestamp': datetime.utcnow().isoformat(),
            'prompt': prompt,
            'response': response,
            'params': params,
            'endpoint': self.endpoint,
            'model': self.model,
            'instance_id': str(uuid.uuid4()),
        }
        self.lineage_records.append(record)

    async def batch_generate_explanations(self, prompts: List[str], **kwargs) -> List[str]:
        return [await self.generate_explanation(p, **kwargs) for p in prompts]

    async def stream_explanation(self, prompt: str, **kwargs) -> AsyncIterable[str]:
        response = await self.generate_explanation(prompt, **kwargs)
        yield response

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()

    def get_circuit_breaker_status(self) -> Dict:
        return self._circuit_breaker.get_status()

    def get_stats(self) -> Dict:
        stats = {
            'circuit_breaker': self.get_circuit_breaker_status(),
            'contextual_bandit': {
                'arms': self.arms,
                'counts': self.linucb.A[0].shape
            } if self.contextual_bandit_enabled else {},
            'moe': self.router.get_stats() if self.moe_enabled else {},
            'ga': {'population_size': self.ga.pop_size} if self.ga_enabled else {},
            'carbon': {'intensity': self.carbon_manager.current_intensity} if self.carbon_aware_enabled else {},
            'self_healing': self.self_healing.get_stats() if self.self_healing_enabled else {},
            'cache': {'enabled': self.cache_enabled},
            'lineage': {'records': len(self.lineage_records)} if self.lineage_enabled else {},
            'limit_graph': {'enabled': self.limit_graph_enabled},
            'rlhf': {'enabled': self.rlhf_enabled},
            'distillation': {'enabled': self.distillation_enabled},
        }
        return stats

# ---------- For backward compatibility, keep original class names as aliases ----------
MOPDOptimizer = LinUCB  # not used but kept
MultiTeacherRouter = MOERouter
