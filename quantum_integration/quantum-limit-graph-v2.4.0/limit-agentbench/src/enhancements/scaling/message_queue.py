"""
Asynchronous message queue abstraction.
Supports 'asyncio' (in-memory) and 'redis' backends.
"""
import asyncio
from typing import Optional, Callable, Awaitable, Any
from ..config import config
from ..logger import logger

class AsyncMessageQueue:
    """Generic async queue for decoupled feedback processing."""

    def __init__(self, queue_type: str = "asyncio", redis_url: Optional[str] = None):
        self.type = queue_type
        self.redis_url = redis_url or config.REDIS_URL
        self._queue = None
        self._is_redis = False

        if self.type == "redis" and self.redis_url:
            try:
                import aioredis
                self._queue = aioredis.from_url(self.redis_url, decode_responses=True)
                self._is_redis = True
                logger.info("Using Redis message queue")
            except ImportError:
                logger.warning("aioredis not installed, falling back to asyncio.Queue")
                self._queue = asyncio.Queue()
        else:
            self._queue = asyncio.Queue()
            logger.info("Using in-memory asyncio.Queue")

    async def publish(self, channel: str, message: Any):
        """Publish a message (event) to the queue."""
        if self._is_redis:
            await self._queue.publish(channel, message)
        else:
            # For asyncio.Queue, we use a simple tuple (channel, message)
            await self._queue.put((channel, message))

    async def subscribe(self, channel: str, callback: Callable[[Any], Awaitable[None]]):
        """Subscribe to a channel and process messages with callback."""
        if self._is_redis:
            pubsub = self._queue.pubsub()
            await pubsub.subscribe(channel)
            async for message in pubsub.listen():
                if message['type'] == 'message':
                    await callback(message['data'])
        else:
            while True:
                chan, msg = await self._queue.get()
                if chan == channel:
                    await callback(msg)

    async def close(self):
        if self._is_redis:
            await self._queue.close()
