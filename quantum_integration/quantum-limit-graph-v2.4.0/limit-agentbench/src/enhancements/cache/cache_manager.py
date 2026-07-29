# cache_manager.py
"""
Enhanced Cache Manager for Green Agent
======================================

Provides an asynchronous caching layer with Redis backend and in‑memory fallback.
Features:
- Async Redis client (redis.asyncio) with connection pooling and automatic reconnection.
- Graceful fallback to in‑memory LRU cache with TTL.
- Robust error handling with retries and circuit breaker.
- Consistent JSON serialization/deserialization.
- Background TTL cleanup for memory cache.
- Configurable size limit for memory cache.
- Prometheus metrics integration (optional).
- Comprehensive docstrings.
"""

import asyncio
import json
import logging
from typing import Optional, Any, Dict, Callable, Tuple
from datetime import datetime, timedelta
from collections import OrderedDict
import time

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

logger = logging.getLogger(__name__)


class CacheManager:
    """
    Asynchronous cache manager with Redis backend and in‑memory LRU fallback.

    If Redis is available and reachable, it will be used for caching. Otherwise,
    it falls back to an in‑memory LRU cache with TTL support. All operations are
    async and thread‑safe.

    Usage:
        cache = CacheManager("redis://localhost:6379/0")
        await cache.set("key", {"value": 42}, ttl=60)
        value = await cache.get("key")  # returns Python dict
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
    ):
        """
        Initialize the cache manager.

        Args:
            redis_url: Redis connection URL (default: "redis://localhost:6379/0").
            serializer: Optional callable to serialize values to a string.
                       Default is JSON serialization.
            deserializer: Optional callable to deserialize strings to Python objects.
                         Default is JSON deserialization.
            max_memory_entries: Maximum number of entries in the memory LRU cache.
            cleanup_interval_seconds: How often (seconds) to clean expired memory entries.
            retry_attempts: Number of retries for Redis operations before failing.
            retry_delay_ms: Base delay (ms) for exponential backoff.
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
                        # If we were previously unavailable, mark as available
                        if not self._redis_available:
                            async with self._redis_lock:
                                self._redis_available = True
                                if self.metrics:
                                    self.metrics['redis_available'].set(1)
                            logger.info("Redis reconnected.")
                    except Exception:
                        # Redis became unavailable
                        if self._redis_available:
                            async with self._redis_lock:
                                self._redis_available = False
                                if self.metrics:
                                    self.metrics['redis_available'].set(0)
                            logger.warning("Redis connection lost.")
                else:
                    # Try to reinitialize
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
            # Fallback to str
            return str(value)

    def _deserialize(self, value_str: str) -> Any:
        """Deserialize a string to a Python object."""
        try:
            return self.deserializer(value_str)
        except Exception as e:
            logger.error(f"Deserialization failed for value '{value_str[:50]}...': {e}")
            return value_str  # fallback

    async def _redis_operation(self, operation: str, *args, **kwargs) -> Any:
        """
        Execute a Redis operation with retries and error handling.
        Raises Exception if all retries fail.
        """
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
                # Optionally re-check availability
                if not self._redis_available:
                    raise RuntimeError("Redis became unavailable")

        raise last_exception

    async def get(self, key: str) -> Optional[Any]:
        """
        Retrieve a value from cache.

        Args:
            key: Cache key.

        Returns:
            Deserialized Python object, or None if not found.
        """
        start = time.time()
        value = None

        # Try Redis first
        if self._redis_available:
            try:
                value_str = await self._redis_operation('get', key)
                if value_str is not None:
                    value = self._deserialize(value_str)
                    if self.metrics:
                        self.metrics['hits'].inc()
                        self.metrics['latency'].labels('get').observe(time.time() - start)
                    logger.debug(f"Cache hit (Redis): {key}")
                    return value
            except Exception as e:
                logger.error(f"Redis get failed for key {key}: {e}")
                # Mark Redis unavailable
                async with self._redis_lock:
                    self._redis_available = False
                    if self.metrics:
                        self.metrics['redis_available'].set(0)
                if self.metrics:
                    self.metrics['errors'].labels('get').inc()

        # Fallback to memory LRU
        async with self._memory_lock:
            if key in self._memory_cache:
                stored_value, expiry = self._memory_cache[key]
                if expiry and datetime.now() > expiry:
                    del self._memory_cache[key]
                    if self.metrics:
                        self.metrics['misses'].inc()
                        self.metrics['latency'].labels('get').observe(time.time() - start)
                    logger.debug(f"Cache expired (memory): {key}")
                    return None
                # Move to end to mark as recently used (LRU)
                self._memory_cache.move_to_end(key)
                value = stored_value
                if self.metrics:
                    self.metrics['hits'].inc()
                    self.metrics['latency'].labels('get').observe(time.time() - start)
                logger.debug(f"Cache hit (memory): {key}")
                return value

        if self.metrics:
            self.metrics['misses'].inc()
            self.metrics['latency'].labels('get').observe(time.time() - start)
        logger.debug(f"Cache miss: {key}")
        return None

    async def set(self, key: str, value: Any, ttl: int = 300) -> None:
        """
        Store a value in cache with a TTL.

        Args:
            key: Cache key.
            value: Value to store (will be serialized).
            ttl: Time‑to‑live in seconds (default: 300).
        """
        start = time.time()
        serialized = self._serialize(value)

        # Try Redis first
        if self._redis_available:
            try:
                await self._redis_operation('setex', key, ttl, serialized)
                if self.metrics:
                    self.metrics['latency'].labels('set').observe(time.time() - start)
                logger.debug(f"Cache set (Redis): {key} (TTL={ttl}s)")
                return
            except Exception as e:
                logger.error(f"Redis set failed for key {key}: {e}")
                async with self._redis_lock:
                    self._redis_available = False
                    if self.metrics:
                        self.metrics['redis_available'].set(0)
                if self.metrics:
                    self.metrics['errors'].labels('set').inc()

        # Fallback to memory LRU
        async with self._memory_lock:
            expiry = datetime.now() + timedelta(seconds=ttl) if ttl > 0 else None
            # If key exists, remove it first to update order
            if key in self._memory_cache:
                del self._memory_cache[key]
            self._memory_cache[key] = (value, expiry)
            # Enforce size limit: remove oldest (first) if over limit
            if len(self._memory_cache) > self.max_memory_entries:
                self._memory_cache.popitem(last=False)
            if self.metrics:
                self.metrics['latency'].labels('set').observe(time.time() - start)
                self.metrics['memory_size'].set(len(self._memory_cache))
                self.metrics['size'].set(
                    (await self._redis.dbsize() if self._redis_available else 0) + len(self._memory_cache)
                )
        logger.debug(f"Cache set (memory): {key} (TTL={ttl}s)")

    async def delete(self, key: str) -> bool:
        """
        Delete a key from cache.

        Args:
            key: Cache key.

        Returns:
            True if the key existed and was deleted, False otherwise.
        """
        deleted = False

        # Try Redis first
        if self._redis_available:
            try:
                deleted = await self._redis_operation('delete', key) > 0
                if deleted:
                    logger.debug(f"Cache delete (Redis): {key}")
                    return deleted
            except Exception as e:
                logger.error(f"Redis delete failed for key {key}: {e}")
                async with self._redis_lock:
                    self._redis_available = False
                    if self.metrics:
                        self.metrics['redis_available'].set(0)

        # Fallback to memory
        async with self._memory_lock:
            if key in self._memory_cache:
                del self._memory_cache[key]
                deleted = True
                if self.metrics:
                    self.metrics['memory_size'].set(len(self._memory_cache))
                logger.debug(f"Cache delete (memory): {key}")
        return deleted

    async def clear(self) -> None:
        """Clear all cache entries (both Redis and memory)."""
        # Clear Redis
        if self._redis_available:
            try:
                await self._redis_operation('flushdb')
                logger.info("Redis cache cleared.")
            except Exception as e:
                logger.error(f"Redis clear failed: {e}")
                async with self._redis_lock:
                    self._redis_available = False
                    if self.metrics:
                        self.metrics['redis_available'].set(0)

        # Clear memory
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
        """
        Get a value; if missing, set it to the default and return it.

        Args:
            key: Cache key.
            default: Value to set if key doesn't exist.
            ttl: TTL for the new entry.

        Returns:
            The cached value (deserialized).
        """
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
        }
        if self.metrics:
            # Retrieve metric values from Prometheus (via registry)
            # This requires the registry to be accessible; we'll compute from our internal counters.
            # Since we don't have a registry reference, we'll use internal counters from the metrics objects.
            # Not ideal, but we can provide a simplified summary.
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

        # Create cache manager
        cache = CacheManager(max_memory_entries=5, cleanup_interval_seconds=10)
        await cache.set("key1", {"foo": "bar"}, ttl=5)
        await cache.set("key2", "hello", ttl=10)
        await cache.set("key3", 42, ttl=20)

        # Retrieve
        val = await cache.get("key1")
        print(f"key1: {val}")  # {'foo': 'bar'}

        # Wait for expiry
        await asyncio.sleep(6)
        val = await cache.get("key1")
        print(f"key1 after expiry: {val}")  # None

        # Test LRU eviction
        for i in range(10):
            await cache.set(f"key{i}", i, ttl=60)
        # The oldest should be evicted
        val = await cache.get("key0")
        print(f"key0 (should be None): {val}")

        await cache.close()

    asyncio.run(demo())
