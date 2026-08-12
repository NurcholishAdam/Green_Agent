"""
Lightweight LLM client for generating natural language explanations.
Enhanced with retries, circuit breaker, persistent session, fallback,
MOPD-based adaptive parameter selection, multi-teacher routing,
semantic caching, metrics, and lineage tracking.
"""

import asyncio
import logging
import json
import time
import hashlib
import uuid
from typing import Dict, Any, Optional, Callable, List, Tuple
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
    RetryError,
    retry_if_exception,
)

# ---------- Optional dependencies ----------
# For semantic caching
try:
    from sentence_transformers import SentenceTransformer
    SENTENCE_TRANSFORMERS_AVAILABLE = True
except ImportError:
    SENTENCE_TRANSFORMERS_AVAILABLE = False

# For metrics
try:
    from prometheus_client import Counter, Gauge, Histogram
    PROMETHEUS_AVAILABLE = True
except ImportError:
    PROMETHEUS_AVAILABLE = False

# For Vault integration
try:
    from hvac import Client as VaultClient
    VAULT_AVAILABLE = True
except ImportError:
    VAULT_AVAILABLE = False

# ---------- Logger ----------
logger = logging.getLogger(__name__)

# ---------- Dummy metrics if Prometheus not available ----------
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

# ---------- MOPD Optimizer for adaptive parameter selection ----------
class MOPDOptimizer:
    """
    Simple epsilon-greedy bandit for selecting generation parameters
    based on multiple objectives (quality, speed, cost).
    """
    def __init__(self, epsilon: float = 0.1):
        self.epsilon = epsilon
        # Each arm is a tuple of (temperature, max_tokens, model)
        self.arms = [
            (0.7, 150, "small"),
            (0.5, 100, "small"),
            (0.9, 200, "medium"),
            (0.3, 50, "small"),
        ]
        self.rewards = defaultdict(float)
        self.counts = defaultdict(int)
        self._lock = asyncio.Lock()

    async def select_params(self, context: Optional[Dict] = None) -> Dict:
        """
        Select parameters based on current context (e.g., task type, budget).
        """
        async with self._lock:
            if random.random() < self.epsilon:
                arm_idx = random.randrange(len(self.arms))
            else:
                # Choose arm with highest average reward
                avg_rewards = [self.rewards[i] / max(self.counts[i], 1) for i in range(len(self.arms))]
                arm_idx = max(range(len(self.arms)), key=lambda i: avg_rewards[i])
            self.counts[arm_idx] += 1
            return self._arm_to_params(self.arms[arm_idx])

    async def update(self, params: Dict, reward: float):
        """
        Update the bandit with the reward from using the parameters.
        """
        arm_idx = self._params_to_arm(params)
        if arm_idx is not None:
            async with self._lock:
                self.rewards[arm_idx] += reward
                self.counts[arm_idx] += 1

    def _arm_to_params(self, arm: Tuple) -> Dict:
        temperature, max_tokens, model = arm
        return {'temperature': temperature, 'max_tokens': max_tokens, 'model': model}

    def _params_to_arm(self, params: Dict) -> Optional[int]:
        for i, arm in enumerate(self.arms):
            if (arm[0] == params.get('temperature') and
                arm[1] == params.get('max_tokens') and
                arm[2] == params.get('model')):
                return i
        return None

# ---------- Multi-Teacher routing ----------
class MultiTeacherRouter:
    """
    Maintains a pool of teacher endpoints and selects one based on a student policy.
    """
    def __init__(self, epsilon: float = 0.1):
        self.teachers: List[Tuple[str, 'LLMClient']] = []  # (name, client)
        self.student_weights = None  # linear model or bandit
        self.epsilon = epsilon
        self.teacher_rewards = defaultdict(float)
        self.teacher_counts = defaultdict(int)
        self._lock = asyncio.Lock()

    def add_teacher(self, name: str, client: 'LLMClient'):
        self.teachers.append((name, client))
        self.teacher_rewards[name] = 0.0
        self.teacher_counts[name] = 0

    async def select_teacher(self, prompt: str) -> 'LLMClient':
        """
        Select a teacher based on epsilon-greedy or a simple bandit.
        """
        async with self._lock:
            if not self.teachers:
                raise RuntimeError("No teachers registered")
            if random.random() < self.epsilon:
                # Explore: pick random
                _, client = random.choice(self.teachers)
            else:
                # Exploit: pick teacher with highest average reward
                best_name = max(self.teacher_rewards, key=lambda n: self.teacher_rewards[n] / max(self.teacher_counts[n], 1))
                _, client = next((n, c) for n, c in self.teachers if n == best_name)
            return client

    async def update(self, teacher_name: str, reward: float):
        """
        Update reward for a teacher.
        """
        async with self._lock:
            self.teacher_rewards[teacher_name] += reward
            self.teacher_counts[teacher_name] += 1

# ---------- Semantic Cache ----------
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
            # Exact match first
            prompt_hash = hashlib.md5(prompt.encode()).hexdigest()
            if prompt_hash in self.cache:
                metrics.cache_hits.inc()
                return self.cache[prompt_hash]

            # Semantic match if embedding model available
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
                # Evict oldest
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

# ---------- Fallback generator with templated responses ----------
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
    Lightweight LLM client with retry, circuit breaker, persistent session, fallback,
    MOPD-based adaptive parameter selection, multi-teacher routing, semantic caching,
    metrics, and lineage tracking.
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
        enable_mopd: bool = True,
        enable_multi_teacher: bool = False,
        enable_cache: bool = True,
        enable_metrics: bool = True,
        enable_lineage: bool = False,
        vault_url: Optional[str] = None,
        vault_token: Optional[str] = None,
        vault_secret_path: str = "llm/api_key",
        extra_endpoints: Optional[List[Tuple[str, str]]] = None,  # (name, endpoint)
    ):
        """
        Args:
            endpoint: Primary LLM API endpoint.
            model: Model identifier.
            headers: Optional HTTP headers (e.g., for authentication).
            timeout: Request timeout in seconds.
            retry_attempts: Number of retry attempts on failure.
            circuit_breaker_threshold: Failures before opening circuit.
            circuit_breaker_timeout: Seconds to wait before trying half-open.
            fallback_generator: Function to generate a fallback explanation if LLM fails.
            enable_mopd: Whether to use MOPD for adaptive parameter selection.
            enable_multi_teacher: Whether to maintain multiple endpoint teachers.
            enable_cache: Whether to use semantic caching.
            enable_metrics: Whether to record Prometheus metrics.
            enable_lineage: Whether to track prompt-response lineage.
            vault_url: Vault URL for secret rotation.
            vault_token: Vault token.
            vault_secret_path: Vault secret path for API key.
            extra_endpoints: Additional teacher endpoints as (name, url) pairs.
        """
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

        # MOPD optimizer
        self.mopd_enabled = enable_mopd
        if enable_mopd:
            self.mopd_optimizer = MOPDOptimizer()

        # Multi-teacher routing
        self.multi_teacher_enabled = enable_multi_teacher
        if enable_multi_teacher:
            self.router = MultiTeacherRouter()
            # Add primary teacher
            self.router.add_teacher("primary", self)
            # Add extra endpoints if provided
            if extra_endpoints:
                for name, url in extra_endpoints:
                    # Create a separate client instance for each teacher
                    teacher_client = LLMClient(
                        endpoint=url,
                        model=model,
                        headers=headers,
                        timeout=timeout,
                        retry_attempts=retry_attempts,
                        circuit_breaker_threshold=circuit_breaker_threshold,
                        circuit_breaker_timeout=circuit_breaker_timeout,
                        fallback_generator=fallback_generator,
                        enable_mopd=False,  # avoid recursion
                        enable_multi_teacher=False,
                        enable_cache=False,
                        enable_metrics=False,
                        enable_lineage=False,
                    )
                    self.router.add_teacher(name, teacher_client)

        # Semantic cache
        self.cache_enabled = enable_cache
        if enable_cache:
            self.cache = SemanticCache()

        # Metrics
        self.metrics_enabled = enable_metrics

        # Lineage
        self.lineage_enabled = enable_lineage
        if enable_lineage:
            self.lineage_records = deque(maxlen=1000)

        # Vault for key rotation
        self.vault_client = None
        if VAULT_AVAILABLE and vault_url and vault_token:
            try:
                self.vault_client = VaultClient(url=vault_url, token=vault_token)
                self.vault_secret_path = vault_secret_path
                # Fetch initial key
                self.headers = self._fetch_vault_secret()
                logger.info("Vault client initialized for key rotation")
            except Exception as e:
                logger.warning(f"Vault initialization failed: {e}")

    def _fetch_vault_secret(self) -> Dict[str, str]:
        """Fetch API key from Vault and update headers."""
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
        """Close the persistent session and any teacher sessions."""
        if self._session and not self._session.closed:
            await self._session.close()
            self._session = None
        if self.multi_teacher_enabled and self.router:
            for _, client in self.router.teachers:
                await client.close()

    def _is_transient_error(self, exc: Exception) -> bool:
        """Determine if an error is transient and should be retried."""
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
        """Perform the HTTP request with retry."""
        session = await self._get_session()
        # Check if we need to refresh Vault token
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
        """
        Send prompt to LLM and return generated explanation.
        Supports adaptive parameters via MOPD, multi-teacher routing,
        semantic caching, and fallback.
        """
        # 1. Check cache
        if self.cache_enabled:
            cached = await self.cache.get(prompt)
            if cached is not None:
                logger.debug("Cache hit for prompt")
                return cached

        # 2. Select parameters via MOPD (if enabled)
        params = {'max_tokens': max_tokens, 'temperature': temperature, 'model': self.model}
        if self.mopd_enabled:
            mopd_params = await self.mopd_optimizer.select_params({'prompt': prompt})
            params.update(mopd_params)

        # 3. Multi-teacher routing (if enabled)
        client = self
        if self.multi_teacher_enabled:
            # Select teacher
            client = await self.router.select_teacher(prompt)

        # 4. Generate
        try:
            if client is self:
                # Use this client's method
                result = await self._generate_internal(prompt, **params)
            else:
                # Delegate to teacher client
                result = await client.generate_explanation(prompt, **params)

            # 5. Update MOPD with reward (if enabled)
            if self.mopd_enabled:
                # Simple reward based on response length (quality proxy)
                reward = min(1.0, len(result) / 200)
                await self.mopd_optimizer.update(params, reward)

            # 6. Update multi-teacher reward (if enabled)
            if self.multi_teacher_enabled and client is not self:
                # Reward based on response quality
                reward = min(1.0, len(result) / 200)
                # Find teacher name
                for name, c in self.router.teachers:
                    if c is client:
                        await self.router.update(name, reward)
                        break

            # 7. Cache result
            if self.cache_enabled:
                await self.cache.set(prompt, result)

            # 8. Lineage tracking
            if self.lineage_enabled:
                self._record_lineage(prompt, result, params)

            return result
        except Exception as e:
            logger.warning(f"LLM generation failed: {e}")
            if self.metrics_enabled:
                metrics.fallback_usage.inc()
            return self.fallback_generator(prompt)

    async def _generate_internal(self, prompt: str, **kwargs) -> str:
        """Internal generation using the primary endpoint."""
        payload = {
            "prompt": prompt,
            **kwargs,
        }
        self._logger.debug(f"LLM request: {payload}")

        try:
            # Use circuit breaker
            result = await self._circuit_breaker.call(self._do_request, payload)
            self._logger.debug(f"LLM response: {result}")
            # Extract text
            text = result.get("text")
            if text is None:
                text = result.get("generated_text", result.get("response", ""))
            return text
        except Exception as e:
            if self.metrics_enabled:
                metrics.retry_count.inc()
            raise

    def _record_lineage(self, prompt: str, response: str, params: Dict):
        """Record prompt-response pair for lineage."""
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
        # Optionally persist to DB or log
        logger.debug(f"Lineage record: {record}")

    async def batch_generate_explanations(self, prompts: List[str], **kwargs) -> List[str]:
        """Generate explanations for multiple prompts in batch (if API supports)."""
        # If batch endpoint supported, implement; else sequential
        results = []
        for prompt in prompts:
            results.append(await self.generate_explanation(prompt, **kwargs))
        return results

    async def stream_explanation(self, prompt: str, **kwargs) -> AsyncIterable[str]:
        """Stream tokens from LLM (if streaming endpoint supported)."""
        # Implementation would depend on API; placeholder
        response = await self.generate_explanation(prompt, **kwargs)
        yield response

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()

    def get_circuit_breaker_status(self) -> Dict:
        return self._circuit_breaker.get_status()

    def get_mopd_stats(self) -> Dict:
        if self.mopd_enabled:
            return {
                'arms': self.mopd_optimizer.arms,
                'counts': dict(self.mopd_optimizer.counts),
                'rewards': dict(self.mopd_optimizer.rewards),
            }
        return {}

    def get_teacher_stats(self) -> Dict:
        if self.multi_teacher_enabled:
            return {
                'teachers': [name for name, _ in self.router.teachers],
                'rewards': dict(self.router.teacher_rewards),
                'counts': dict(self.router.teacher_counts),
            }
        return {}
