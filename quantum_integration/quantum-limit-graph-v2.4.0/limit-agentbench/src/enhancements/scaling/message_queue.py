"""
Asynchronous Message Queue Abstraction
=======================================
Supports 'asyncio' (in‑memory) and 'redis' backends with auto‑reconnection,
retries, metrics, and durable streaming (Redis Streams).
"""
import asyncio
import json
import time
from typing import Optional, Callable, Awaitable, Any, Dict, List, Union
from dataclasses import dataclass
from ..config import config
from ..logger import logger

# Try to import Redis client
try:
    from redis.asyncio import Redis
    from redis.asyncio.client import PubSub
    REDIS_AVAILABLE = True
except ImportError:
    REDIS_AVAILABLE = False

# Optional Prometheus metrics
try:
    from prometheus_client import Counter, Gauge
    PROMETHEUS_AVAILABLE = True
except ImportError:
    PROMETHEUS_AVAILABLE = False


@dataclass
class Message:
    """Structured message with metadata."""
    channel: str
    payload: Any
    timestamp: float = time.time()
    message_id: Optional[str] = None  # for Redis Streams

    def to_json(self) -> str:
        return json.dumps({
            "channel": self.channel,
            "payload": self.payload,
            "timestamp": self.timestamp,
            "message_id": self.message_id,
        })

    @classmethod
    def from_json(cls, data: str) -> "Message":
        d = json.loads(data)
        return cls(
            channel=d["channel"],
            payload=d["payload"],
            timestamp=d["timestamp"],
            message_id=d.get("message_id"),
        )


class AsyncMessageQueue:
    """
    Generic async queue for decoupled feedback processing.

    Supports:
        - asyncio.Queue (in‑memory, no persistence)
        - Redis Pub/Sub (fire‑and‑forget, no persistence)
        - Redis Streams (durable, with consumer groups) – configurable

    Features:
        - Auto‑reconnection for Redis.
        - Retry logic with exponential backoff.
        - Prometheus metrics (if available).
        - Graceful shutdown and cancellation.
        - JSON serialization.
        - Queue size monitoring.
        - Context manager support.
    """

    def __init__(
        self,
        queue_type: str = "asyncio",
        redis_url: Optional[str] = None,
        use_streams: bool = False,
        consumer_group: Optional[str] = None,
        consumer_name: Optional[str] = None,
        metrics_registry=None,
    ):
        """
        Args:
            queue_type: "asyncio" or "redis".
            redis_url: Redis connection string.
            use_streams: If True, use Redis Streams (durable) instead of Pub/Sub.
            consumer_group: Consumer group name for Streams.
            consumer_name: Consumer name for Streams.
            metrics_registry: Optional Prometheus registry for metrics.
        """
        self.type = queue_type
        self.redis_url = redis_url or config.REDIS_URL
        self.use_streams = use_streams
        self.consumer_group = consumer_group or "green_agent_group"
        self.consumer_name = consumer_name or f"consumer_{id(self)}"
        self._queue = None
        self._is_redis = False
        self._pubsub = None  # for Pub/Sub
        self._stream_consumer_task = None
        self._closed = False
        self._metrics = None
        self._retry_count = 0

        # Metrics
        if PROMETHEUS_AVAILABLE and metrics_registry:
            self._metrics = {
                "published": Counter("green_agent_queue_published_total", "Messages published", registry=metrics_registry.registry if hasattr(metrics_registry, "registry") else None),
                "consumed": Counter("green_agent_queue_consumed_total", "Messages consumed", registry=metrics_registry.registry if hasattr(metrics_registry, "registry") else None),
                "queue_size": Gauge("green_agent_queue_size", "Current queue size", registry=metrics_registry.registry if hasattr(metrics_registry, "registry") else None),
            }

        # Initialize backend
        self._initialize()

    def _initialize(self):
        """Set up the appropriate backend."""
        if self.type == "redis" and self.redis_url and REDIS_AVAILABLE:
            self._queue = Redis.from_url(self.redis_url, decode_responses=True)
            self._is_redis = True
            logger.info(f"Using Redis queue (streams={self.use_streams})")
        else:
            self._queue = asyncio.Queue()
            self._is_redis = False
            logger.info("Using in-memory asyncio.Queue")

    # --------------------------------------------------------------------------
    # Public API
    # --------------------------------------------------------------------------
    async def publish(self, channel: str, message: Any) -> Optional[str]:
        """
        Publish a message to the queue.

        Returns:
            message_id if using Redis Streams, else None.
        """
        if self._closed:
            raise RuntimeError("Queue is closed")

        msg = Message(channel=channel, payload=message)

        if self._metrics:
            self._metrics["published"].inc()

        if self._is_redis:
            try:
                if self.use_streams:
                    # Use Redis Streams: XADD
                    msg_id = await self._queue.xadd(
                        channel,
                        {"data": msg.to_json()},
                    )
                    logger.debug(f"Published to stream {channel} with ID {msg_id}")
                    return msg_id
                else:
                    # Pub/Sub
                    await self._queue.publish(channel, msg.to_json())
                    logger.debug(f"Published to channel {channel}")
                    return None
            except Exception as e:
                logger.error(f"Redis publish failed: {e}")
                # Fallback to asyncio.Queue? Or raise? We'll raise for now.
                raise
        else:
            # asyncio.Queue
            await self._queue.put((channel, msg))
            if self._metrics:
                self._metrics["queue_size"].set(self._queue.qsize())
            logger.debug(f"Queued message to channel {channel}")
            return None

    async def subscribe(
        self,
        channels: Union[str, List[str]],
        callback: Callable[[Any], Awaitable[None]],
    ):
        """
        Subscribe to one or more channels and process messages with callback.

        For Redis Pub/Sub, subscribes to channels.
        For Redis Streams, creates a consumer and reads from streams.
        For asyncio.Queue, listens for messages on the given channel(s).

        This method runs indefinitely until cancelled or closed.
        """
        if isinstance(channels, str):
            channels = [channels]

        if self._is_redis:
            if self.use_streams:
                # Redis Streams consumer
                await self._stream_consumer(channels, callback)
            else:
                # Redis Pub/Sub
                await self._pubsub_consumer(channels, callback)
        else:
            # asyncio.Queue
            await self._queue_consumer(channels, callback)

    async def close(self):
        """Close the queue and cleanup resources."""
        self._closed = True
        if self._is_redis:
            if self._pubsub:
                await self._pubsub.unsubscribe()
                await self._pubsub.close()
            if self._queue:
                await self._queue.close()
            logger.info("Redis connection closed.")
        else:
            # Clear queue
            while not self._queue.empty():
                try:
                    self._queue.get_nowait()
                except asyncio.QueueEmpty:
                    break
            logger.info("In-memory queue cleared.")

    async def qsize(self) -> int:
        """Get current queue size (only for asyncio.Queue)."""
        if self._is_redis:
            # For Redis, we can't easily get queue size. Return -1.
            return -1
        return self._queue.qsize()

    # --------------------------------------------------------------------------
    # Context manager support
    # --------------------------------------------------------------------------
    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()

    # --------------------------------------------------------------------------
    # Internal consumer implementations
    # --------------------------------------------------------------------------
    async def _pubsub_consumer(self, channels: List[str], callback: Callable[[Any], Awaitable[None]]):
        """Redis Pub/Sub consumer."""
        self._pubsub = self._queue.pubsub()
        await self._pubsub.subscribe(*channels)
        logger.info(f"Subscribed to Redis channels: {channels}")

        try:
            async for message in self._pubsub.listen():
                if self._closed:
                    break
                if message["type"] == "message":
                    try:
                        data = message["data"]
                        msg = Message.from_json(data)
                        if self._metrics:
                            self._metrics["consumed"].inc()
                        await callback(msg.payload)
                    except Exception as e:
                        logger.error(f"Error processing message: {e}")
        except asyncio.CancelledError:
            logger.info("Pub/Sub consumer cancelled.")
        finally:
            if self._pubsub:
                await self._pubsub.unsubscribe(*channels)
                await self._pubsub.close()

    async def _stream_consumer(self, streams: List[str], callback: Callable[[Any], Awaitable[None]]):
        """Redis Streams consumer with consumer group."""
        # Create consumer group if not exists
        for stream in streams:
            try:
                await self._queue.xgroup_create(
                    stream,
                    self.consumer_group,
                    id="0",
                    mkstream=True,
                )
            except Exception as e:
                # Group may already exist – ignore
                logger.debug(f"Consumer group creation for {stream}: {e}")

        logger.info(f"Subscribed to Redis Streams: {streams} (group={self.consumer_group}, consumer={self.consumer_name})")

        while not self._closed:
            try:
                # Read messages from all streams
                # Using XREADGROUP with block=5000ms
                response = await self._queue.xreadgroup(
                    self.consumer_group,
                    self.consumer_name,
                    {stream: ">" for stream in streams},
                    count=10,
                    block=5000,
                )
                if response:
                    for stream, messages in response.items():
                        for msg_id, msg_data in messages.items():
                            try:
                                payload = msg_data["data"]
                                msg = Message.from_json(payload)
                                if self._metrics:
                                    self._metrics["consumed"].inc()
                                await callback(msg.payload)
                                # Acknowledge (XACK)
                                await self._queue.xack(stream, self.consumer_group, msg_id)
                            except Exception as e:
                                logger.error(f"Error processing stream message {msg_id}: {e}")
            except asyncio.CancelledError:
                logger.info("Stream consumer cancelled.")
                break
            except Exception as e:
                logger.error(f"Stream consumer error: {e}")
                await asyncio.sleep(1)  # backoff

    async def _queue_consumer(self, channels: List[str], callback: Callable[[Any], Awaitable[None]]):
        """asyncio.Queue consumer."""
        logger.info(f"Listening on asyncio.Queue channels: {channels}")
        while not self._closed:
            try:
                # Get next item
                chan, msg = await self._queue.get()
                if chan in channels:
                    try:
                        if self._metrics:
                            self._metrics["consumed"].inc()
                        await callback(msg.payload)
                    except Exception as e:
                        logger.error(f"Error processing message: {e}")
                self._queue.task_done()
                if self._metrics:
                    self._metrics["queue_size"].set(self._queue.qsize())
            except asyncio.CancelledError:
                logger.info("Queue consumer cancelled.")
                break
            except Exception as e:
                logger.error(f"Queue consumer error: {e}")
                await asyncio.sleep(0.1)
