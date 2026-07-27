# cache_manager.py
"""
Enhanced Cache Manager for Green Agent
======================================

Provides an asynchronous caching layer with Redis backend and in‑memory fallback.
Features:
- Async Redis client (redis.asyncio) with connection pooling.
- Graceful fallback to in‑memory dictionary with TTL.
- Robust error handling with logging.
- Cache invalidation methods (delete, clear).
- Optional serialization via JSON.
- Prometheus metrics integration (optional).
- Comprehensive docstrings.
"""

import asyncio
import json
import logging
from typing import Optional, Any, Dict, Callable
from datetime import datetime, timedelta

# ---------- Redis async client ----------
try:
    from redis.asyncio import Redis, ConnectionPool
    REDIS_AVAILABLE = True
except ImportError:
    REDIS_AVAILABLE = False

# ---------- Prometheus metrics (optional) ----------
try:
    from prometheus_client import Counter, Gauge, Histogram
    PROMETHEUS_AVAILABLE = True
except ImportError:
    PROMETHEUS_AVAILABLE = False

logger = logging.getLogger(__name__)


class CacheManager:
    """
    Asynchronous cache manager with Redis backend and in‑memory fallback.

    If Redis is available and reachable, it will be used for caching. Otherwise,
    it falls back to an in‑memory dictionary with TTL support. All operations are
    async and thread‑safe.

    Usage:
        cache = CacheManager("redis://localhost:6379/0")
        await cache.set("key", "value", ttl=60)
        value = await cache.get("key")
    """

    def __init__(self, redis_url: str = "redis://localhost:6379/0", serializer: Optional[Callable] = None):
        """
        Initialize the cache manager.

        Args:
            redis_url: Redis connection URL (default: "redis://localhost:6379/0").
            serializer: Optional callable to serialize values before storing.
                        Default is JSON serialization.
        """
        self.redis_url = redis_url
        self.serializer = serializer or (lambda v: json.dumps(v) if not isinstance(v, str) else v)
        self.deserializer = lambda v: json.loads(v) if isinstance(v, str) and v.startswith('{') else v
        self._redis: Optional[Redis] = None
        self._memory_cache: Dict[str, Any] = {}
        self._memory_ttl: Dict[str, datetime] = {}
        self._available = False
        self._lock = asyncio.Lock()

        # Prometheus metrics (if enabled)
        if PROMETHEUS_AVAILABLE:
            self.metrics = {
                'hits': Counter('cache_hits_total', 'Cache hits'),
                'misses': Counter('cache_misses_total', 'Cache misses'),
                'errors': Counter('cache_errors_total', 'Cache errors', ['operation']),
                'latency': Histogram('cache_operation_seconds', 'Cache operation latency', ['operation']),
                'size': Gauge('cache_size', 'Cache entries')
            }
        else:
            self.metrics = None

        self._init_redis()

    def _init_redis(self):
        """Initialize Redis connection pool if available."""
        if not REDIS_AVAILABLE:
            logger.warning("redis.asyncio not installed; falling back to in‑memory cache.")
            return

        try:
            pool = ConnectionPool.from_url(self.redis_url, decode_responses=True)
            self._redis = Redis(connection_pool=pool)
            # Test connection asynchronously
            asyncio.create_task(self._test_connection())
        except Exception as e:
            logger.error(f"Redis initialization failed: {e}")
            self._redis = None
            self._available = False

    async def _test_connection(self):
        """Test Redis connectivity and set availability flag."""
        try:
            await self._redis.ping()
            self._available = True
            logger.info("Redis connection established.")
        except Exception as e:
            logger.warning(f"Redis ping failed: {e}")
            self._available = False
            self._redis = None

    async def _ensure_redis_available(self) -> bool:
        """Check if Redis is available; attempt reconnect if not."""
        if self._available and self._redis:
            return True
        # Attempt to reconnect once
        self._init_redis()
        # Wait for test to complete (simplified)
        await asyncio.sleep(0.1)
        return self._available

    async def get(self, key: str) -> Optional[str]:
        """
        Retrieve a value from cache.

        Args:
            key: Cache key.

        Returns:
            Cached value as string, or None if not found.
        """
        start = datetime.now()
        value = None

        # Try Redis first
        if await self._ensure_redis_available():
            try:
                value = await self._redis.get(key)
                if value is not None:
                    if self.metrics:
                        self.metrics['hits'].inc()
                        self.metrics['latency'].labels('get').observe((datetime.now() - start).total_seconds())
                    logger.debug(f"Cache hit (Redis): {key}")
                    return value
            except Exception as e:
                logger.error(f"Redis get failed for key {key}: {e}")
                self._available = False
                self._redis = None
                if self.metrics:
                    self.metrics['errors'].labels('get').inc()

        # Fallback to memory
        async with self._lock:
            if key in self._memory_cache:
                expiry = self._memory_ttl.get(key)
                if expiry and datetime.now() > expiry:
                    del self._memory_cache[key]
                    if key in self._memory_ttl:
                        del self._memory_ttl[key]
                    if self.metrics:
                        self.metrics['misses'].inc()
                        self.metrics['latency'].labels('get').observe((datetime.now() - start).total_seconds())
                    logger.debug(f"Cache expired (memory): {key}")
                    return None
                value = self._memory_cache[key]
                if self.metrics:
                    self.metrics['hits'].inc()
                    self.metrics['latency'].labels('get').observe((datetime.now() - start).total_seconds())
                logger.debug(f"Cache hit (memory): {key}")
                return value

        if self.metrics:
            self.metrics['misses'].inc()
            self.metrics['latency'].labels('get').observe((datetime.now() - start).total_seconds())
        logger.debug(f"Cache miss: {key}")
        return None

    async def set(self, key: str, value: Any, ttl: int = 300) -> None:
        """
        Store a value in cache with a TTL.

        Args:
            key: Cache key.
            value: Value to store (will be serialized if not a string).
            ttl: Time‑to‑live in seconds (default: 300).
        """
        start = datetime.now()
        serialized = self.serializer(value) if callable(self.serializer) else str(value)

        # Try Redis first
        if await self._ensure_redis_available():
            try:
                await self._redis.setex(key, ttl, serialized)
                if self.metrics:
                    self.metrics['latency'].labels('set').observe((datetime.now() - start).total_seconds())
                logger.debug(f"Cache set (Redis): {key} (TTL={ttl}s)")
                return
            except Exception as e:
                logger.error(f"Redis set failed for key {key}: {e}")
                self._available = False
                self._redis = None
                if self.metrics:
                    self.metrics['errors'].labels('set').inc()

        # Fallback to memory
        async with self._lock:
            expiry = datetime.now() + timedelta(seconds=ttl)
            self._memory_cache[key] = serialized
            self._memory_ttl[key] = expiry
            if self.metrics:
                self.metrics['latency'].labels('set').observe((datetime.now() - start).total_seconds())
                self.metrics['size'].set(len(self._memory_cache))
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
        if await self._ensure_redis_available():
            try:
                deleted = await self._redis.delete(key) > 0
                if deleted:
                    logger.debug(f"Cache delete (Redis): {key}")
                return deleted
            except Exception as e:
                logger.error(f"Redis delete failed for key {key}: {e}")
                self._available = False
                self._redis = None

        # Fallback to memory
        async with self._lock:
            if key in self._memory_cache:
                del self._memory_cache[key]
                if key in self._memory_ttl:
                    del self._memory_ttl[key]
                deleted = True
                if self.metrics:
                    self.metrics['size'].set(len(self._memory_cache))
                logger.debug(f"Cache delete (memory): {key}")
        return deleted

    async def clear(self) -> None:
        """Clear all cache entries (both Redis and memory)."""
        # Clear Redis
        if await self._ensure_redis_available():
            try:
                await self._redis.flushdb()
                logger.info("Redis cache cleared.")
            except Exception as e:
                logger.error(f"Redis clear failed: {e}")
                self._available = False
                self._redis = None

        # Clear memory
        async with self._lock:
            self._memory_cache.clear()
            self._memory_ttl.clear()
            if self.metrics:
                self.metrics['size'].set(0)
        logger.info("Memory cache cleared.")

    async def close(self) -> None:
        """Close Redis connection pool gracefully."""
        if self._redis:
            await self._redis.close()
            await self._redis.connection_pool.disconnect()
            logger.info("Redis connection closed.")

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()

    # ---------- Convenience methods ----------
    async def get_or_set(self, key: str, default: Any, ttl: int = 300) -> str:
        """
        Get a value; if missing, set it to the default and return it.

        Args:
            key: Cache key.
            default: Value to set if key doesn't exist.
            ttl: TTL for the new entry.

        Returns:
            The cached value (either existing or newly set).
        """
        value = await self.get(key)
        if value is None:
            value = self.serializer(default) if callable(self.serializer) else str(default)
            await self.set(key, value, ttl)
        return value

    # ---------- Statistics ----------
    async def get_stats(self) -> Dict[str, Any]:
        """Return current cache statistics."""
        stats = {
            'backend': 'redis' if self._available and self._redis else 'memory',
            'memory_entries': len(self._memory_cache),
            'redis_available': self._available,
        }
        if self.metrics:
            stats['metrics'] = {
                'hits': self.metrics['hits']._value.get(),
                'misses': self.metrics['misses']._value.get(),
                'errors': self.metrics['errors']._value.get(),
            }
        return stats


# ============================================================================
# Example usage (if run directly)
# ============================================================================
if __name__ == "__main__":
    import asyncio

    async def demo():
        logging.basicConfig(level=logging.INFO)

        # Create cache manager (Redis URL optional)
        cache = CacheManager()
        await cache.set("test_key", "test_value", ttl=10)
        value = await cache.get("test_key")
        print(f"Value: {value}")
        await asyncio.sleep(11)
        value = await cache.get("test_key")
        print(f"After expiry: {value}")
        await cache.close()

    asyncio.run(demo())
