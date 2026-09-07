"""
Async Message Queue for cross‑module communication.

Supports publish/subscribe and federated aggregation hooks.
"""

import asyncio
from collections import defaultdict, deque
from typing import Any, Dict, List, Optional

class AsyncMessageQueue:
    def __init__(self, queue_type: str = "asyncio", max_queue_size: int = 10000):
        self.queue_type = queue_type
        self.max_queue_size = max_queue_size
        self.subscribers = defaultdict(list)
        self.message_buffer = deque(maxlen=max_queue_size)

    async def publish(self, topic: str, message: Any):
        """Publish a message to a topic."""
        self.message_buffer.append((topic, message))
        for callback in self.subscribers.get(topic, []):
            await callback(message)

    async def subscribe(self, topic: str, callback):
        """Subscribe to a topic with a callback."""
        self.subscribers[topic].append(callback)

    async def receive(self, topic: str, timeout: Optional[float] = None) -> Any:
        """Wait for a message on a topic (for simple request/reply)."""
        # Simplified: return most recent message for topic
        for t, msg in reversed(self.message_buffer):
            if t == topic:
                return msg
        return None

    # Federated aggregation hooks
    async def publish_student_weights(self, weights: Dict):
        """Publish student weights for federated aggregation."""
        await self.publish("federated_weights", weights)

    async def receive_student_weights(self) -> Any:
        """Receive aggregated student weights."""
        return await self.receive("federated_weights")

    async def close(self):
        """Close queue and clear subscribers."""
        self.subscribers.clear()
        self.message_buffer.clear()
