# cache_manager.py (Enhanced v2.0.0)
"""
Enhanced Cache Manager for Green Agent with Adaptive Caching Policy
====================================================================

Uses Multi‑Teacher On‑Policy Distillation to select caching strategies
(Redis vs. memory, TTL, no‑cache) based on context and learn from outcomes.

All existing features (Redis backend, memory LRU fallback, TTL, metrics,
background cleanup) are retained.
"""

import asyncio
import json
import logging
from typing import Optional, Any, Dict, Callable, Tuple
from datetime import datetime, timedelta
from collections import OrderedDict
import time
import random
from abc import ABC, abstractmethod
import hashlib
import numpy as np

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
        # We'll persist in the cache_manager's memory (or a separate key)
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

        # Update Q-teacher (if we have the original state)
        # We'll do that separately in the main loop.

    def get_stats(self) -> Dict:
        return {
            'student_counter': self.student.counter,
            'buffer_size': len(self.replay_buffer),
            'weights_norm': float(np.linalg.norm(self.student.weights))
        }


# ============================================================================
# MAIN CACHE MANAGER (Enhanced)
# ============================================================================

class CacheManager:
    """
    Asynchronous cache manager with adaptive caching policy via distillation.

    If Redis is available and reachable, it will be used for caching. Otherwise,
    it falls back to an in‑memory LRU cache with TTL support. The policy selection
    (which backend, TTL, or skip) is learned via multi‑teacher distillation.

    All existing features are retained; the policy decisions are now adaptive.
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
        # NEW: distillation parameters
        distillation_epsilon: float = 0.1,
        distillation_train_every: int = 10,
        distillation_replay_size: int = 2000,
        distillation_learning_rate: float = 0.01,
        distill_weight: float = 0.7,
        rl_weight: float = 0.3,
    ):
        """
        Initialize the cache manager with adaptive policy.

        Args:
            redis_url: Redis connection URL.
            serializer: Optional callable to serialize values to a string.
            deserializer: Optional callable to deserialize strings to Python objects.
            max_memory_entries: Maximum number of entries in the memory LRU cache.
            cleanup_interval_seconds: How often (seconds) to clean expired memory entries.
            retry_attempts: Number of retries for Redis operations.
            retry_delay_ms: Base delay (ms) for exponential backoff.
            distillation_*: Parameters for the distillation agent.
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

        # Memory LRU cache: dict with OrderedDict for LRU ordering
        self._memory_cache: OrderedDict[str, Tuple[Any, datetime]] = OrderedDict()
        self._memory_lock = asyncio.Lock()

        # Background tasks
        self._cleanup_task: Optional[asyncio.Task] = None
        self._health_task: Optional[asyncio.Task] = None
        self._running = True

        # Prometheus metrics (if enabled)
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
            }

        # NEW: Distillation optimizer
        self.distillation_config = {
            'distillation_epsilon': distillation_epsilon,
            'distillation_train_every': distillation_train_every,
            'distillation_replay_size': distillation_replay_size,
            'distillation_learning_rate': distillation_learning_rate,
            'distill_weight': distill_weight,
            'rl_weight': rl_weight,
        }
        self.policy_optimizer = DistillationCachePolicyOptimizer(self, self.distillation_config)

        # Track key metrics for state building
        self.key_access_count: Dict[str, int] = {}
        self.key_last_access: Dict[str, datetime] = {}
        self.key_size_estimate: Dict[str, float] = {}

        # Start background tasks
        self._start_background_tasks()

        # Initialize Redis (async)
        asyncio.create_task(self._init_redis())

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
                # Test connection with ping
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
                await asyncio.sleep(30)  # Check every 30 seconds
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
        # Key characteristics
        key_length = len(key)
        # Estimate size of serialized value (if possible)
        try:
            serialized = self._serialize(value)
            size_bytes = len(serialized.encode('utf-8'))
        except:
            size_bytes = 1024  # default
        # Access frequency (times accessed in last hour)
        access_count = self.key_access_count.get(key, 0)
        if key in self.key_last_access:
            last = self.key_last_access[key]
            if (now - last).total_seconds() > 3600:
                access_count = 0
        freq = access_count  # per hour

        # Context
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

        # Historical performance for this key
        # We'll use a simple heuristic: hit rate from our internal tracking
        # For simplicity, we'll derive from metrics if available, else default.
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

    # ----- Update tracking stats -----
    def _update_key_stats(self, key: str, hit: bool, latency: float):
        """Update access statistics for a key."""
        now = datetime.now()
        self.key_access_count[key] = self.key_access_count.get(key, 0) + 1
        self.key_last_access[key] = now

    # ----- New: Apply selected policy -----
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
        """
        Execute the selected policy.
        Returns (success, hit, latency, result)
        """
        start = time.time()
        success = False
        hit = False
        result = None

        if policy == 'no_cache':
            # Skip caching entirely; just return the value (if set) or None (if get)
            # For get, we don't have the value, so we'll just return None.
            # For set, we do nothing.
            if value is not None:
                # This is a set operation; we skip storing
                success = True
            else:
                # Get operation; return None (miss)
                hit = False
            latency = (time.time() - start) * 1000
            return success, hit, latency, None

        # Determine backend and TTL
        if policy == 'redis_ttl_short':
            backend = 'redis'
            effective_ttl = 60
        elif policy == 'redis_ttl_long':
            backend = 'redis'
            effective_ttl = 600
        elif policy == 'memory_only':
            backend = 'memory'
            effective_ttl = ttl  # use provided TTL
        elif policy == 'adaptive_ttl':
            # Use a dynamic TTL based on hit rate (e.g., longer for high hit)
            if self._redis_available:
                backend = 'redis'
            else:
                backend = 'memory'
            # For simplicity, use a fixed adaptive TTL (e.g., 300s)
            effective_ttl = 300
        else:
            backend = 'redis'
            effective_ttl = ttl

        # Execute operation
        if value is not None:
            # SET operation
            if backend == 'redis':
                try:
                    serialized = self._serialize(value)
                    await self._redis_operation('setex', key, effective_ttl, serialized)
                    success = True
                except Exception as e:
                    logger.error(f"Redis set failed for key {key}: {e}")
                    self._redis_available = False
            else:
                # memory
                async with self._memory_lock:
                    expiry = datetime.now() + timedelta(seconds=effective_ttl)
                    self._memory_cache[key] = (value, expiry)
                    if len(self._memory_cache) > self.max_memory_entries:
                        self._memory_cache.popitem(last=False)
                success = True
            latency = (time.time() - start) * 1000
            return success, False, latency, None
        else:
            # GET operation
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
    # PUBLIC METHODS (Enhanced with distillation)
    # ========================================================================

    async def get(self, key: str) -> Optional[Any]:
        """
        Retrieve a value from cache. Policy is selected adaptively.
        """
        start = time.time()
        value = None

        # Build state
        # For get, we don't have a value to estimate size, so we use previous size or default.
        state = await self._get_policy_state(key, None)
        # Select policy
        policy, action_idx, state_vec, teacher_probs = await self.policy_optimizer.select_policy(state, exploration=True)

        # Execute policy
        success, hit, latency, result = await self._apply_policy(policy, key, None, 0, action_idx, state_vec, teacher_probs)

        # Update stats
        self._update_key_stats(key, hit, latency)

        # Compute reward
        reward = 0.0
        if hit:
            reward += 0.5
        # Latency: lower is better; we want reward if latency is low
        if latency < 10:
            reward += 0.3
        elif latency < 50:
            reward += 0.15
        # Resource usage: prefer Redis over memory if redis available (less memory pressure)
        if policy.startswith('redis') and self._redis_available:
            reward += 0.2
        elif policy == 'memory_only' and len(self._memory_cache) < self.max_memory_entries * 0.5:
            reward += 0.1
        reward = max(0.0, min(1.0, reward))

        # Update agent (async, non‑blocking)
        next_state = await self._get_policy_state(key, None)
        asyncio.create_task(self.policy_optimizer.update(state_vec, action_idx, reward, next_state.to_feature_vector(), teacher_probs))

        # Update metrics
        if self.metrics:
            if hit:
                self.metrics['hits'].inc()
            else:
                self.metrics['misses'].inc()
            self.metrics['latency'].labels('get').observe(time.time() - start)

        logger.debug(f"Cache {('hit' if hit else 'miss')} (policy={policy}): {key}")
        return result

    async def set(self, key: str, value: Any, ttl: int = 300) -> None:
        """
        Store a value in cache. Policy is selected adaptively.
        """
        start = time.time()
        # Build state with value size
        state = await self._get_policy_state(key, value)
        # Select policy
        policy, action_idx, state_vec, teacher_probs = await self.policy_optimizer.select_policy(state, exploration=True)

        # Execute policy
        success, _, latency, _ = await self._apply_policy(policy, key, value, ttl, action_idx, state_vec, teacher_probs)

        # Compute reward
        reward = 0.0
        if success:
            reward += 0.6
        # Latency reward
        if latency < 5:
            reward += 0.2
        elif latency < 20:
            reward += 0.1
        # Resource efficiency: prefer Redis for large values
        if policy.startswith('redis') and state.estimated_size_bytes > 100_000:
            reward += 0.2
        reward = max(0.0, min(1.0, reward))

        # Update agent (async)
        next_state = await self._get_policy_state(key, value)
        asyncio.create_task(self.policy_optimizer.update(state_vec, action_idx, reward, next_state.to_feature_vector(), teacher_probs))

        # Update metrics
        if self.metrics:
            self.metrics['latency'].labels('set').observe(time.time() - start)
            self.metrics['size'].set(
                (await self._redis.dbsize() if self._redis_available else 0) + len(self._memory_cache)
            )
        logger.debug(f"Cache set (policy={policy}): {key} (TTL={ttl}s)")

    async def delete(self, key: str) -> bool:
        """
        Delete a key from cache.
        """
        # We can still use the same distillation logic for deletion, but it's simpler to just delete.
        # For consistency, we could also learn whether to delete from Redis or memory, but we'll keep it simple.
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
        if self._cleanup_task:
            self._cleanup_task.cancel()
        if self._health_task:
            self._health_task.cancel()
        await asyncio.gather(self._cleanup_task, self._health_task, return_exceptions=True)
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

        # Create cache manager with adaptive policy
        cache = CacheManager(max_memory_entries=5, cleanup_interval_seconds=10,
                             distillation_epsilon=0.2, distillation_train_every=2)

        # Simulate some accesses to let the agent learn
        for i in range(20):
            key = f"key{i%5}"
            if i % 3 == 0:
                await cache.set(key, {"data": i}, ttl=5)
            else:
                val = await cache.get(key)
                print(f"get {key}: {val}")
            await asyncio.sleep(0.1)

        stats = await cache.get_stats()
        print(f"Stats: {stats}")

        await cache.close()

    asyncio.run(demo())
