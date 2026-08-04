# File: src/enhancements/tokenization_optimizer.py
"""
Tokenization optimizer – language‑aware tokenizer selection, segmentation, and token budgets.
Enhanced with Multi‑Teacher On‑Policy Distillation for adaptive strategy selection.
Version 2.0.0
"""

import asyncio
import hashlib
import logging
import os
import re
from abc import ABC, abstractmethod
from collections import deque
from dataclasses import dataclass
from datetime import datetime, timedelta
from functools import lru_cache
from typing import Dict, List, Any, Optional, Tuple, Union
import random
import numpy as np

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

# -----------------------------------------------------------------------------
# Structured logging
# -----------------------------------------------------------------------------
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
# Prometheus metrics (only if available)
# -----------------------------------------------------------------------------
if PROMETHEUS_AVAILABLE:
    TOKENIZATION_COUNTER = Counter('tokenization_requests_total', 'Total tokenization requests', ['language', 'status'])
    TOKEN_COUNT_HISTOGRAM = Histogram('token_count_per_request', 'Number of tokens per request', ['language'])
    TOKENIZATION_DURATION = Histogram('tokenization_duration_seconds', 'Tokenization duration', ['language'])
    CACHE_HIT_COUNTER = Counter('tokenization_cache_hits_total', 'Cache hits for tokenization')
    CACHE_MISS_COUNTER = Counter('tokenization_cache_misses_total', 'Cache misses for tokenization')
    LANGUAGE_DISTRIBUTION = Gauge('tokenization_language_distribution', 'Language distribution of requests', ['language'])
    # Distillation metrics
    DISTILLATION_STRATEGY = Counter('distillation_strategy_selected', 'Strategy selected by distillation', ['strategy'])
    DISTILLATION_REWARD = Histogram('distillation_reward', 'Reward received per request')
    DISTILLATION_BUFFER_SIZE = Gauge('distillation_buffer_size', 'Replay buffer size')

# -----------------------------------------------------------------------------
# Configuration with Pydantic (fallback if not installed)
# -----------------------------------------------------------------------------
if PYDANTIC_AVAILABLE:
    class TokenizationConfig(BaseSettings):
        """Configuration for the tokenization optimizer."""
        default_tokenizer: str = Field('bert-base-uncased', description="Default tokenizer model.")
        language_tokenizer_map: Dict[str, str] = Field(
            default_factory=lambda: {
                'en': 'bert-base-uncased',
                'id': 'bert-base-indonesian-1.5G',
                'fr': 'camembert-base',
                'de': 'bert-base-german-cased',
                'es': 'dccuchile/bert-base-spanish-wwm-uncased',
            },
            description="Mapping from language code to tokenizer model name."
        )
        cache_ttl_seconds: int = Field(300, description="TTL for tokenization cache (seconds).")
        enable_cache: bool = Field(True, description="Enable tokenization caching.")
        max_segment_length: int = Field(100, description="Maximum tokens per segment before split.")
        summarization_ratio: float = Field(0.5, description="Ratio of original length to summarize to.")
        fallback_language: str = Field('en', description="Fallback language if detection fails.")
        require_langdetect: bool = Field(False, description="Raise error if langdetect not available.")
        require_nltk: bool = Field(False, description="Raise error if NLTK not available.")
        # Distillation parameters
        distillation_epsilon: float = Field(0.1, description="Exploration rate for distillation.")
        train_every: int = Field(10, description="Update student every N steps.")
        replay_buffer_size: int = Field(2000, description="Size of replay buffer.")
        student_learning_rate: float = Field(0.01, description="Learning rate for student.")

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
    # Fallback config as dict
    config = {
        'default_tokenizer': 'bert-base-uncased',
        'language_tokenizer_map': {
            'en': 'bert-base-uncased',
            'id': 'bert-base-indonesian-1.5G',
            'fr': 'camembert-base',
            'de': 'bert-base-german-cased',
            'es': 'dccuchile/bert-base-spanish-wwm-uncased',
        },
        'cache_ttl_seconds': 300,
        'enable_cache': True,
        'max_segment_length': 100,
        'summarization_ratio': 0.5,
        'fallback_language': 'en',
        'require_langdetect': False,
        'require_nltk': False,
        # Distillation defaults
        'distillation_epsilon': 0.1,
        'train_every': 10,
        'replay_buffer_size': 2000,
        'student_learning_rate': 0.01,
    }

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
# NEW: Distillation Components
# ============================================================================
@dataclass
class TokenizationState:
    """Context for the distillation agent."""
    text_length: int
    avg_word_len: float
    num_sentences: int
    language: str
    requested_budget: int
    tokenizer_efficiency: float   # tokens/char for this language
    domain: Optional[str] = None
    time_of_day: int = 0          # 0-23

    def to_feature_vector(self) -> np.ndarray:
        """Convert to numeric feature vector (12 dims)."""
        features = [
            min(self.text_length / 10000.0, 1.0),
            min(self.avg_word_len / 10.0, 1.0),
            min(self.num_sentences / 100.0, 1.0),
            min(self.requested_budget / 2000.0, 1.0),
            self.tokenizer_efficiency,
        ]
        # One‑hot for language (top 4 + other)
        lang_map = {'en': 0, 'id': 1, 'fr': 2, 'de': 3, 'es': 4}
        one_hot = [0.0] * 5
        idx = lang_map.get(self.language, 4)
        one_hot[idx] = 1.0
        features.extend(one_hot)
        # Time and domain (if any)
        features.append(self.time_of_day / 24.0)
        # Domain one‑hot (simplified: 3 common domains)
        domain_map = {'scientific': 0, 'legal': 1, 'general': 2}
        domain_one_hot = [0.0] * 3
        if self.domain:
            d_idx = domain_map.get(self.domain, 2)
            domain_one_hot[d_idx] = 1.0
        features.extend(domain_one_hot)
        return np.array(features, dtype=np.float32)

# -----------------------------------------------------------------------------
# Teacher abstract class and implementations
# -----------------------------------------------------------------------------
class Teacher(ABC):
    @abstractmethod
    def predict(self, state: TokenizationState) -> np.ndarray:
        """Return probability vector over 5 strategies."""
        pass

    @abstractmethod
    def confidence(self, state: TokenizationState) -> float:
        """Return confidence in prediction [0,1]."""
        pass


class RuleBasedTeacher(Teacher):
    """Rule‑based expert: prefers summarization if text is long and budget small."""
    ACTION_SPACE = ['efficiency', 'accuracy', 'speed', 'budget', 'adaptive']

    def predict(self, state: TokenizationState) -> np.ndarray:
        probs = np.ones(5) * 0.1
        if state.text_length > 5000 and state.requested_budget < 500:
            probs[3] = 0.8   # budget strategy
        elif state.num_sentences > 20:
            probs[0] = 0.7   # efficiency (segmentation and truncation)
        elif state.tokenizer_efficiency > 0.5:  # high tokens/char → use larger model?
            probs[1] = 0.6   # accuracy
        else:
            probs[2] = 0.5   # speed
        return probs / probs.sum()

    def confidence(self, state: TokenizationState) -> float:
        if state.text_length > 5000 and state.requested_budget < 500:
            return 0.6
        elif state.num_sentences > 20:
            return 0.5
        return 0.4


class HistoricalMLTeacher(Teacher):
    """Offline trained classifier (placeholder)."""
    def __init__(self, model_path: Optional[str] = None):
        self.model = None
        if model_path and os.path.exists(model_path):
            import joblib
            self.model = joblib.load(model_path)

    def predict(self, state: TokenizationState) -> np.ndarray:
        if self.model is None:
            return np.ones(5) / 5
        x = state.to_feature_vector().reshape(1, -1)
        probs = self.model.predict_proba(x)[0]
        return probs

    def confidence(self, state: TokenizationState) -> float:
        return 0.7 if self.model is not None else 0.0


class StatefulQTeacher(Teacher):
    """Linear Q‑learning with state features."""
    def __init__(self, storage: Any, lr: float = 0.1):
        self.storage = storage
        self.lr = lr
        self.weights = np.zeros((12, 5))  # 12 features, 5 actions
        self._load_state()

    def _load_state(self):
        if hasattr(self.storage, 'get_state'):
            w = self.storage.get_state('q_teacher_weights')
            if w:
                self.weights = np.array(json.loads(w))

    def _save_state(self):
        if hasattr(self.storage, 'save_state'):
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


class DistillationStudent:
    """Linear softmax student updated via distillation + policy gradient."""
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


class DistillationTokenizationOptimizer:
    """
    Multi‑teacher on‑policy distillation agent for tokenization strategy selection.
    """
    ACTION_SPACE = ['efficiency', 'accuracy', 'speed', 'budget', 'adaptive']

    def __init__(self, storage: Any, config: Dict[str, Any]):
        self.storage = storage
        self.config = config
        self.student = DistillationStudent(lr=config.get('student_learning_rate', 0.01))
        self.teachers: List[Teacher] = [
            RuleBasedTeacher(),
            HistoricalMLTeacher(),
            StatefulQTeacher(storage)
        ]
        self.replay_buffer = ReplayBuffer(max_size=config.get('replay_buffer_size', 2000))
        self.epsilon = config.get('distillation_epsilon', 0.1)
        self.train_every = config.get('train_every', 10)
        self.counter = 0

    async def select_strategy(self, state: TokenizationState, exploration: bool = True) -> Tuple[str, int, np.ndarray, np.ndarray]:
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

        # Update StatefulQTeacher if we have the full state (we'll pass state separately)
        # For simplicity, we'll update it in the main loop with the actual state.

    def get_stats(self) -> Dict:
        return {
            'student_counter': self.student.counter,
            'buffer_size': len(self.replay_buffer),
            'weights_norm': float(np.linalg.norm(self.student.weights))
        }

# ============================================================================
# Tokenization Optimizer (Enhanced with Distillation)
# ============================================================================
class TokenizationOptimizer:
    """
    Optimizes tokenization for sustainability with adaptive strategy selection.
    """

    def __init__(self, cfg: Optional[Union[Dict[str, Any], TokenizationConfig]] = None):
        if cfg is None:
            if PYDANTIC_AVAILABLE:
                self.config = TokenizationConfig()
            else:
                self.config = config.copy()
        elif isinstance(cfg, dict):
            if PYDANTIC_AVAILABLE:
                self.config = TokenizationConfig(**cfg)
            else:
                self.config = cfg
        else:
            self.config = cfg

        # Validate required dependencies
        if self.config.get('require_langdetect', False) and not LANGDETECT_AVAILABLE:
            raise ImportError("langdetect is required but not installed.")
        if self.config.get('require_nltk', False) and not NLTK_AVAILABLE:
            raise ImportError("NLTK is required but not installed.")

        self.tokenizers: Dict[str, Any] = {}
        self.language_map = self.config.get('language_tokenizer_map', {})
        self.default_tokenizer_name = self.config.get('default_tokenizer', 'bert-base-uncased')
        self._tokenizer_lock = asyncio.Lock()
        self.circuit_breaker = CircuitBreaker(name="tokenizer_loading")

        self._cache: Dict[str, Dict] = {}
        self._cache_ttl = self.config.get('cache_ttl_seconds', 300)

        # Distillation agent (we use a simple in‑memory storage for Q‑teacher)
        self.distillation = DistillationTokenizationOptimizer(storage=self, config=self.config)

        logger.info("TokenizationOptimizer initialized", config=self.config)

    # ------------------------------------------------------------------
    # Storage interface for Q‑teacher (simplified)
    # ------------------------------------------------------------------
    def get_state(self, key: str) -> Optional[str]:
        # In a real system, this would read from a database.
        # For this example, we'll use a simple dict.
        if not hasattr(self, '_state_store'):
            self._state_store = {}
        return self._state_store.get(key)

    def save_state(self, key: str, value: str):
        if not hasattr(self, '_state_store'):
            self._state_store = {}
        self._state_store[key] = value

    # ------------------------------------------------------------------
    # Language detection
    # ------------------------------------------------------------------
    async def detect_language(self, text: str) -> str:
        if not LANGDETECT_AVAILABLE:
            logger.warning("langdetect not available; using fallback language: %s", self.config.get('fallback_language', 'en'))
            return self.config.get('fallback_language', 'en')
        try:
            loop = asyncio.get_event_loop()
            lang = await loop.run_in_executor(None, detect, text)
            return lang
        except Exception as e:
            logger.error("Language detection failed: %s", e, exc_info=True)
            return self.config.get('fallback_language', 'en')

    # ------------------------------------------------------------------
    # Tokenizer loading
    # ------------------------------------------------------------------
    async def _load_tokenizer(self, language: str) -> Any:
        if language in self.tokenizers:
            return self.tokenizers[language]

        model_name = self.language_map.get(language, self.default_tokenizer_name)

        async def _load():
            if not TRANSFORMERS_AVAILABLE:
                raise RuntimeError("Transformers not available.")
            try:
                tokenizer = AutoTokenizer.from_pretrained(model_name)
                self.tokenizers[language] = tokenizer
                logger.info("Loaded tokenizer", language=language, model=model_name)
                return tokenizer
            except Exception as e:
                logger.error("Failed to load tokenizer", language=language, model=model_name, error=str(e))
                if model_name != self.default_tokenizer_name:
                    logger.warning("Falling back to default tokenizer: %s", self.default_tokenizer_name)
                    tokenizer = AutoTokenizer.from_pretrained(self.default_tokenizer_name)
                    self.tokenizers[language] = tokenizer
                    return tokenizer
                else:
                    raise

        if TENACITY_AVAILABLE:
            @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
            async def load_with_retry():
                return await self.circuit_breaker.call(_load)
            return await load_with_retry()
        else:
            return await self.circuit_breaker.call(_load)

    async def _get_tokenizer(self, language: str) -> Any:
        async with self._tokenizer_lock:
            return await self._load_tokenizer(language)

    # ------------------------------------------------------------------
    # Segmentation
    # ------------------------------------------------------------------
    async def _segment_text(self, text: str) -> List[str]:
        if NLTK_AVAILABLE:
            try:
                loop = asyncio.get_event_loop()
                sentences = await loop.run_in_executor(None, sent_tokenize, text)
                return sentences
            except Exception as e:
                logger.error("NLTK segmentation failed: %s", e, exc_info=True)
        return re.split(r'(?<=[.!?])\s+', text)

    # ------------------------------------------------------------------
    # Summarization
    # ------------------------------------------------------------------
    async def _summarize(self, text: str, target_tokens: int) -> str:
        if SUMMA_AVAILABLE:
            try:
                lang = await self.detect_language(text)
                tokenizer = await self._get_tokenizer(lang)
                tokens = tokenizer.encode(text, add_special_tokens=False)
                ratio = target_tokens / len(tokens) if len(tokens) > 0 else 0.5
                ratio = min(1.0, max(0.1, ratio))
                loop = asyncio.get_event_loop()
                summary = await loop.run_in_executor(None, summarizer.summarize, text, ratio=ratio)
                return summary if summary else text[:target_tokens * 4]
            except Exception as e:
                logger.error("Summarization failed: %s", e, exc_info=True)
        return text[:target_tokens * 4]

    # ------------------------------------------------------------------
    # Tokenization
    # ------------------------------------------------------------------
    async def _tokenize(self, text: str, language: str) -> Tuple[List[int], int]:
        tokenizer = await self._get_tokenizer(language)
        tokens = tokenizer.encode(text, add_special_tokens=False)
        return tokens, len(tokens)

    def _cache_key(self, text: str, language: str, budget: int, strategy: str) -> str:
        # Include strategy in cache key because different strategies may produce different results
        key = f"{text}_{language}_{budget}_{strategy}"
        return hashlib.md5(key.encode()).hexdigest()

    # ------------------------------------------------------------------
    # Core optimization with distillation
    # ------------------------------------------------------------------
    async def optimize(self, text: str, context: Dict[str, Any]) -> Dict[str, Any]:
        start_time = time.time()
        language = context.get('language')
        if language is None:
            language = await self.detect_language(text)

        budget = context.get('token_budget', 1000)
        segment_budget = context.get('segment_budget', None)
        domain = context.get('domain', None)

        # Build state for distillation
        # Compute some features
        text_length = len(text)
        words = text.split()
        avg_word_len = np.mean([len(w) for w in words]) if words else 0
        sentences = await self._segment_text(text)
        num_sentences = len(sentences)
        tokenizer_efficiency = await self.get_token_efficiency(text, language)  # tokens/char

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

        # Select strategy via distillation
        strategy, action_idx, state_vec, teacher_probs = await self.distillation.select_strategy(state, exploration=True)

        # Check cache (including strategy)
        cache_key = self._cache_key(text, language, budget, strategy)
        if self.config.get('enable_cache', True) and cache_key in self._cache:
            cached = self._cache[cache_key]
            if (datetime.now() - cached['timestamp']).seconds < self._cache_ttl:
                if PROMETHEUS_AVAILABLE:
                    CACHE_HIT_COUNTER.inc()
                logger.debug("Cache hit", language=language, strategy=strategy)
                cached['cache_hit'] = True
                return cached

        if PROMETHEUS_AVAILABLE:
            CACHE_MISS_COUNTER.inc()
            DISTILLATION_STRATEGY.labels(strategy=strategy).inc()

        # Apply the chosen strategy
        # For simplicity, we map strategies to different behaviours:
        if strategy == 'efficiency':
            # Use smallest tokenizer (default) and truncate aggressively
            tokenizer_name = self.default_tokenizer_name
            # No summarization, just truncate to budget
            tokens, total_tokens = await self._tokenize(text, language)
            if total_tokens > budget:
                # Truncate tokens directly (not ideal but simple)
                truncated_text = text[:budget * 4]  # rough heuristic
                tokens, total_tokens = await self._tokenize(truncated_text, language)
            segments = [(truncated_text if total_tokens > budget else text, total_tokens)]
        elif strategy == 'accuracy':
            # Use best tokenizer for language (if available) and summarize to preserve meaning
            tokenizer_name = self.language_map.get(language, self.default_tokenizer_name)
            # Summarize to ~80% of budget
            target = int(budget * 0.8)
            summary = await self._summarize(text, target)
            tokens, total_tokens = await self._tokenize(summary, language)
            segments = [(summary, total_tokens)]
        elif strategy == 'speed':
            # Use regex segmentation, no summarization, no expensive tokenizer
            tokenizer_name = 'bert-base-uncased'  # fastest
            # Simply split by sentences and take first N sentences to fit budget
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
            # Try to meet budget exactly with summarization if needed
            tokenizer_name = self.default_tokenizer_name
            tokens, total_tokens = await self._tokenize(text, language)
            if total_tokens > budget:
                target = int(budget * 0.9)
                summary = await self._summarize(text, target)
                tokens, total_tokens = await self._tokenize(summary, language)
                segments = [(summary, total_tokens)]
            else:
                segments = [(text, total_tokens)]
        else:  # 'adaptive'
            # Mix: use the default strategy but with adaptive threshold
            tokenizer_name = self.default_tokenizer_name
            tokens, total_tokens = await self._tokenize(text, language)
            if total_tokens > budget:
                # Use summarization with ratio based on historical performance
                ratio = 0.5  # could be learned
                target = int(budget * ratio)
                summary = await self._summarize(text, target)
                tokens, total_tokens = await self._tokenize(summary, language)
                segments = [(summary, total_tokens)]
            else:
                segments = [(text, total_tokens)]

        # Compute reward
        # Criteria: token efficiency (lower is better), budget adherence, semantic preservation (proxy: ratio of retained sentences)
        reward = 0.0
        # Efficiency: tokens per character - we want lower than average (0.3 is a baseline)
        eff = total_tokens / len(text) if text else 0
        if eff < 0.3:
            reward += 0.4
        elif eff < 0.5:
            reward += 0.2
        # Budget adherence: if total_tokens <= budget, reward; if too low, penalize (wasteful)
        if total_tokens <= budget:
            reward += 0.3
            if total_tokens < budget * 0.3:
                reward -= 0.1  # too short
        else:
            reward -= 0.2
        # Semantic preservation: we'll use the ratio of segments retained vs original sentences
        if num_sentences > 0:
            retained = len(segments)
            ratio = retained / num_sentences
            if ratio > 0.5:
                reward += 0.3
        # Normalise reward to [0,1]
        reward = max(0.0, min(1.0, reward))

        # Update distillation agent
        next_state = state  # we could compute a new state, but for simplicity we reuse
        await self.distillation.update(state_vec, action_idx, reward, next_state.to_feature_vector(), teacher_probs)

        # Prepare result
        result = {
            'segments': segments,
            'total_tokens': total_tokens,
            'language': language,
            'tokenizer_used': tokenizer_name,
            'strategy_used': strategy,
            'cache_hit': False,
            'timestamp': datetime.now()
        }

        # Cache
        if self.config.get('enable_cache', True):
            self._cache[cache_key] = result

        # Update metrics
        if PROMETHEUS_AVAILABLE:
            TOKENIZATION_COUNTER.labels(language=language, status='success').inc()
            TOKEN_COUNT_HISTOGRAM.labels(language=language).observe(total_tokens)
            TOKENIZATION_DURATION.labels(language=language).observe(time.time() - start_time)
            LANGUAGE_DISTRIBUTION.labels(language=language).set(1)
            DISTILLATION_REWARD.observe(reward)
            DISTILLATION_BUFFER_SIZE.set(len(self.distillation.replay_buffer))

        logger.info("Tokenization completed", language=language, total_tokens=total_tokens,
                    segments=len(segments), strategy=strategy, reward=reward)
        return result

    # ------------------------------------------------------------------
    # Utility: get token efficiency
    # ------------------------------------------------------------------
    async def get_token_efficiency(self, text: str, language: Optional[str] = None) -> float:
        if language is None:
            language = await self.detect_language(text)
        _, total_tokens = await self._tokenize(text, language)
        return total_tokens / len(text) if text else 0.0

    # ------------------------------------------------------------------
    # Cache management
    # ------------------------------------------------------------------
    async def clear_cache(self):
        self._cache.clear()
        logger.info("Tokenization cache cleared")

    async def get_cache_stats(self) -> Dict:
        return {'size': len(self._cache), 'ttl_seconds': self._cache_ttl}

    # ------------------------------------------------------------------
    # Shutdown
    # ------------------------------------------------------------------
    async def shutdown(self):
        self.tokenizers.clear()
        self._cache.clear()
        logger.info("TokenizationOptimizer shutdown complete")

# -----------------------------------------------------------------------------
# Example usage
# -----------------------------------------------------------------------------
async def example_usage():
    optimizer = TokenizationOptimizer()
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
