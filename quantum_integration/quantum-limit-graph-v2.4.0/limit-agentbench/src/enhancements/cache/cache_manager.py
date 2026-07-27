import redis
from typing import Optional

class CacheManager:
    """Simple Redis‑based cache (falls back to in‑memory if Redis unavailable)."""
    def __init__(self, redis_url: str = "redis://localhost:6379/0"):
        try:
            self.client = redis.from_url(redis_url)
            self.client.ping()
            self._available = True
        except Exception:
            self._available = False
            self._memory_cache = {}

    async def get(self, key: str) -> Optional[str]:
        if self._available:
            return self.client.get(key)
        else:
            return self._memory_cache.get(key)

    async def set(self, key: str, value: str, ttl: int = 300):
        if self._available:
            self.client.setex(key, ttl, value)
        else:
            self._memory_cache[key] = value
