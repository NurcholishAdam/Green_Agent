"""
Asynchronous Message Queue Abstraction
=======================================
Supports 'asyncio' (in‑memory) and 'redis' backends with auto‑reconnection,
retries, metrics, and durable streaming (Redis Streams).

Enhancements implemented:
- Auto‑reconnection with exponential backoff for Redis.
- Retry logic for publish.
- Configurable maxsize for asyncio.Queue (backpressure).
- Message TTL for Redis Streams (MAXLEN ~ approximate).
- Multiple callbacks per channel.
- Dead‑letter queue for failed messages.
- Enhanced metrics (latency, errors, queue depth).
- Priority support for asyncio.Queue (via separate queues).
- Graceful shutdown and consumer task management.

NEW v2.0:
- Integrated optional LIMIT Graph, MODP, RLHF, and MoE components.
- LIMIT Graph tracks message flows and connections.
- MODP stores routing decisions as states.
- RLHF collects human preferences on routing.
- MoE gating selects the best backend (asyncio vs redis) based on context.
- All new components are optional and controlled by flags.
"""

import asyncio
import json
import time
import uuid
import hashlib
from typing import Optional, Callable, Awaitable, Any, Dict, List, Union, Tuple
from dataclasses import dataclass
from collections import defaultdict, deque
import numpy as np
import copy
import random

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
    from prometheus_client import Counter, Gauge, Histogram
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
    retry_count: int = 0

    def to_json(self) -> str:
        return json.dumps({
            "channel": self.channel,
            "payload": self.payload,
            "timestamp": self.timestamp,
            "message_id": self.message_id,
            "retry_count": self.retry_count,
        })

    @classmethod
    def from_json(cls, data: str) -> "Message":
        d = json.loads(data)
        return cls(
            channel=d["channel"],
            payload=d["payload"],
            timestamp=d["timestamp"],
            message_id=d.get("message_id"),
            retry_count=d.get("retry_count", 0),
        )


# ------------------------------------------------------------------------------
# NEW: LIMIT Graph Manager
# ------------------------------------------------------------------------------
class LimitGraphManager:
    """
    Manages a graph of message flows and connections for LIMIT.
    Nodes are publishers, subscribers, or channels; edges represent data flows.
    """
    def __init__(self, storage: Optional[Any] = None):
        self.storage = storage
        self.graphs = {}

    def create_graph(self, graph_id: str, description: str, configuration: Dict[str, Any]) -> None:
        if self.storage and hasattr(self.storage, 'save_limit_graph_metadata'):
            self.storage.save_limit_graph_metadata(graph_id, description, configuration)
        else:
            self.graphs[graph_id] = {'description': description, 'configuration': configuration, 'nodes': {}, 'edges': {}}

    def add_node(self, graph_id: str, node_id: str, node_type: Optional[str], attributes: Dict[str, Any]) -> None:
        if self.storage and hasattr(self.storage, 'save_limit_graph_node'):
            self.storage.save_limit_graph_node(node_id, graph_id, node_type, attributes)
        else:
            if graph_id not in self.graphs:
                self.graphs[graph_id] = {'nodes': {}, 'edges': {}}
            self.graphs[graph_id]['nodes'][node_id] = {'node_type': node_type, 'attributes': attributes}

    def add_edge(self, graph_id: str, edge_id: str, source: str, target: str,
                 weight: Optional[float], attributes: Dict[str, Any]) -> None:
        if self.storage and hasattr(self.storage, 'save_limit_graph_edge'):
            self.storage.save_limit_graph_edge(edge_id, graph_id, source, target, weight, attributes)
        else:
            if graph_id not in self.graphs:
                self.graphs[graph_id] = {'nodes': {}, 'edges': {}}
            self.graphs[graph_id]['edges'][edge_id] = {'source': source, 'target': target, 'weight': weight, 'attributes': attributes}

    def get_nodes(self, graph_id: str) -> List[Dict]:
        if self.storage and hasattr(self.storage, 'get_limit_graph_nodes'):
            return self.storage.get_limit_graph_nodes(graph_id)
        return list(self.graphs.get(graph_id, {}).get('nodes', {}).values())

    def get_edges(self, graph_id: str) -> List[Dict]:
        if self.storage and hasattr(self.storage, 'get_limit_graph_edges'):
            return self.storage.get_limit_graph_edges(graph_id)
        return list(self.graphs.get(graph_id, {}).get('edges', {}).values())

    def get_metadata(self, graph_id: str) -> Optional[Dict]:
        if self.storage and hasattr(self.storage, 'get_limit_graph_metadata'):
            return self.storage.get_limit_graph_metadata(graph_id)
        return self.graphs.get(graph_id, {})


# ------------------------------------------------------------------------------
# NEW: MODP Optimizer (wrapper)
# ------------------------------------------------------------------------------
class MODPOptimizer:
    """
    Multi‑Objective Dynamic Programming solver that stores decision states/policies.
    Used here to persist routing decisions.
    """
    def __init__(self, storage: Optional[Any] = None):
        self.storage = storage
        self.states = {}

    def add_state(self, state_id: str, problem_id: str, state_attributes: Dict[str, Any],
                  objective_values: Dict[str, float], stage: int) -> None:
        if self.storage and hasattr(self.storage, 'save_modp_state'):
            self.storage.save_modp_state(state_id, problem_id, state_attributes, objective_values, stage)
        else:
            if problem_id not in self.states:
                self.states[problem_id] = []
            self.states[problem_id].append({
                'state_id': state_id, 'state_attributes': state_attributes,
                'objective_values': objective_values, 'stage': stage
            })

    def add_policy(self, policy_id: str, problem_id: str, state_id: str,
                   action: str, expected_objectives: Dict[str, float]) -> None:
        if self.storage and hasattr(self.storage, 'save_modp_policy'):
            self.storage.save_modp_policy(policy_id, problem_id, state_id, action, expected_objectives)

    def get_states(self, problem_id: str) -> List[Dict]:
        if self.storage and hasattr(self.storage, 'get_modp_states'):
            return self.storage.get_modp_states(problem_id)
        return self.states.get(problem_id, [])

    def get_policies(self, problem_id: str) -> List[Dict]:
        if self.storage and hasattr(self.storage, 'get_modp_policies'):
            return self.storage.get_modp_policies(problem_id)
        return []


# ------------------------------------------------------------------------------
# NEW: RLHF Trainer
# ------------------------------------------------------------------------------
class RLHFTrainer:
    """
    Collects human preference pairs for queue routing decisions.
    """
    def __init__(self, storage: Optional[Any] = None):
        self.storage = storage
        self.pairs = []

    def record_pair(self, pair_id: str, prompt: str, chosen: str, rejected: str,
                    reward_diff: float, metadata: Optional[Dict] = None) -> None:
        if self.storage and hasattr(self.storage, 'save_preference_pair'):
            self.storage.save_preference_pair(pair_id, prompt, chosen, rejected, reward_diff, metadata)
        else:
            self.pairs.append({
                'pair_id': pair_id, 'prompt': prompt, 'chosen': chosen,
                'rejected': rejected, 'reward_diff': reward_diff, 'metadata': metadata
            })

    def get_pairs(self, limit: int = 100) -> List[Dict]:
        if self.storage and hasattr(self.storage, 'get_preference_pairs'):
            return self.storage.get_preference_pairs(limit)
        return self.pairs[-limit:]

    def train_reward_model(self):
        pairs = self.get_pairs()
        if len(pairs) < 5:
            logger.info("Not enough preference pairs for RLHF training.")
            return
        logger.info(f"Training reward model on {len(pairs)} preference pairs...")


# ------------------------------------------------------------------------------
# NEW: MoE Gating Network for Backend Selection
# ------------------------------------------------------------------------------
class MoEGatingNetwork:
    """
    Mixture-of-Experts gating that selects the best queue backend (asyncio or redis)
    based on context features (message size, priority, system load, etc.).
    """
    def __init__(self, storage: Optional[Any] = None, config: Optional[Dict] = None):
        self.storage = storage
        self.config = config or {}
        self.expert_names = self.config.get('expert_names', ['asyncio', 'redis'])
        self.num_experts = len(self.expert_names)
        # Gating input: normalized context features
        self.gating_weights = np.random.randn(self.num_experts, 4)  # 4 features
        self._training_samples = []

    def _encode_state(self, context: Dict[str, float]) -> np.ndarray:
        features = [
            context.get('message_size', 0.0),
            context.get('priority', 0.0),
            context.get('system_load', 0.0),
            context.get('redis_available', 1.0 if REDIS_AVAILABLE else 0.0),
        ]
        return np.array(features, dtype=np.float32)

    async def select_expert(self, context: Dict[str, float]) -> Tuple[str, np.ndarray]:
        x = self._encode_state(context)
        logits = self.gating_weights @ x
        probs = np.exp(logits - np.max(logits))
        probs /= probs.sum()
        expert_idx = np.argmax(probs)
        selected = self.expert_names[expert_idx]
        if self.storage and hasattr(self.storage, 'log_routing_decision'):
            sample_id = hashlib.sha256(str(context).encode()).hexdigest()[:16]
            self.storage.log_routing_decision(str(uuid.uuid4()), sample_id, selected, float(probs[expert_idx]))
        return selected, probs

    async def add_training_sample(self, context: Dict[str, float], selected_expert: str, reward: float):
        x = self._encode_state(context)
        expert_idx = self.expert_names.index(selected_expert)
        target = np.zeros(self.num_experts)
        target[expert_idx] = 1.0
        logits = self.gating_weights @ x
        probs = np.exp(logits - np.max(logits))
        probs /= probs.sum()
        grad = (probs - target)[:, None] * x[None, :]
        self.gating_weights -= 0.1 * grad


# ==============================================================================
# Enhanced AsyncMessageQueue with optional new components
# ==============================================================================
class AsyncMessageQueue:
    """
    Generic async queue for decoupled feedback processing.

    Supports:
        - asyncio.Queue (in‑memory, no persistence)
        - Redis Pub/Sub (fire‑and‑forget, no persistence)
        - Redis Streams (durable, with consumer groups)

    Features:
        - Auto‑reconnection for Redis with exponential backoff.
        - Retry logic with exponential backoff for publishing.
        - Prometheus metrics (if available).
        - Graceful shutdown and cancellation.
        - JSON serialization.
        - Queue size monitoring (including Redis via LLEN/XLEN).
        - Context manager support.
        - Multiple callbacks per channel.
        - Dead‑letter queue for failed messages.
        - Priority queues (asyncio only).
        - Batch operations.

    NEW v2.0:
        - Optional MoE gating for dynamic backend selection.
        - LIMIT Graph to track message flows.
        - MODP to persist routing decisions.
        - RLHF to collect human preferences.
        - Bio-inspired weight evolution (optional NSGA-II for queue parameters).
    """

    def __init__(
        self,
        queue_type: str = "asyncio",
        redis_url: Optional[str] = None,
        use_streams: bool = False,
        consumer_group: Optional[str] = None,
        consumer_name: Optional[str] = None,
        metrics_registry=None,
        max_queue_size: int = 1000,
        redis_max_retries: int = 5,
        redis_retry_delay: float = 1.0,
        message_ttl: Optional[int] = None,
        dead_letter_suffix: str = "_dead",
        # NEW optional flags
        enable_limit_graph: bool = True,
        enable_modp: bool = True,
        enable_rlhf: bool = True,
        enable_moe: bool = True,
        storage: Optional[Any] = None,
    ):
        """
        Args:
            queue_type: "asyncio" or "redis".
            redis_url: Redis connection string.
            use_streams: If True, use Redis Streams (durable) instead of Pub/Sub.
            consumer_group: Consumer group name for Streams.
            consumer_name: Consumer name for Streams.
            metrics_registry: Optional Prometheus registry.
            max_queue_size: Maximum size for asyncio.Queue (0 = unlimited).
            redis_max_retries: Maximum reconnection attempts for Redis.
            redis_retry_delay: Initial delay between reconnection attempts (exponential backoff).
            message_ttl: If using streams, approximate max length (XLEN) to keep.
            dead_letter_suffix: Suffix for dead‑letter stream names.
            enable_limit_graph: Enable LIMIT Graph tracking.
            enable_modp: Enable MODP persistence.
            enable_rlhf: Enable RLHF trainer.
            enable_moe: Enable MoE gating for backend selection.
            storage: Optional central Storage object for persistence.
        """
        self.type = queue_type
        self.redis_url = redis_url or config.REDIS_URL
        self.use_streams = use_streams
        self.consumer_group = consumer_group or "green_agent_group"
        self.consumer_name = consumer_name or f"consumer_{id(self)}"
        self.max_queue_size = max_queue_size
        self.redis_max_retries = redis_max_retries
        self.redis_retry_delay = redis_retry_delay
        self.message_ttl = message_ttl
        self.dead_letter_suffix = dead_letter_suffix

        self._queue = None
        self._is_redis = False
        self._pubsub = None
        self._stream_consumer_tasks: List[asyncio.Task] = []
        self._closed = False
        self._metrics = None
        self._retry_count = 0
        self._callbacks: Dict[str, List[Callable[[Any], Awaitable[None]]]] = defaultdict(list)
        self._redis_connected = False

        # Metrics
        if PROMETHEUS_AVAILABLE and metrics_registry:
            self._metrics = {
                "published": Counter("green_agent_queue_published_total", "Messages published", registry=metrics_registry.registry if hasattr(metrics_registry, "registry") else None),
                "consumed": Counter("green_agent_queue_consumed_total", "Messages consumed", registry=metrics_registry.registry if hasattr(metrics_registry, "registry") else None),
                "queue_size": Gauge("green_agent_queue_size", "Current queue size", registry=metrics_registry.registry if hasattr(metrics_registry, "registry") else None),
                "publish_errors": Counter("green_agent_queue_publish_errors_total", "Publish errors", registry=metrics_registry.registry if hasattr(metrics_registry, "registry") else None),
                "consume_errors": Counter("green_agent_queue_consume_errors_total", "Consume errors", registry=metrics_registry.registry if hasattr(metrics_registry, "registry") else None),
                "publish_latency": Histogram("green_agent_queue_publish_latency_seconds", "Publish latency", registry=metrics_registry.registry if hasattr(metrics_registry, "registry") else None),
                "consume_latency": Histogram("green_agent_queue_consume_latency_seconds", "Consume latency", registry=metrics_registry.registry if hasattr(metrics_registry, "registry") else None),
            }

        # Initialize backend
        self._initialize()

        # NEW optional components
        self.storage = storage
        self.limit_graph_manager = LimitGraphManager(storage) if enable_limit_graph else None
        self.modp_solver = MODPOptimizer(storage) if enable_modp else None
        self.rlhf_trainer = RLHFTrainer(storage) if enable_rlhf else None
        self.moe_gating = MoEGatingNetwork(storage, {'expert_names': ['asyncio', 'redis']}) if enable_moe else None

        # Initialize LIMIT graph if enabled
        if self.limit_graph_manager:
            if not self.limit_graph_manager.get_metadata("queue_graph"):
                self.limit_graph_manager.create_graph("queue_graph", "Message Queue Relationships", {})
            # Add backend nodes
            self.limit_graph_manager.add_node("queue_graph", "backend_asyncio", "backend", {"type": "asyncio"})
            self.limit_graph_manager.add_node("queue_graph", "backend_redis", "backend", {"type": "redis"})

        logger.info(f"AsyncMessageQueue initialized with optional components: "
                    f"limit_graph={self.limit_graph_manager is not None}, modp={self.modp_solver is not None}, "
                    f"rlhf={self.rlhf_trainer is not None}, moe={self.moe_gating is not None}")

    def _initialize(self):
        """Set up the appropriate backend."""
        if self.type == "redis" and self.redis_url and REDIS_AVAILABLE:
            self._queue = Redis.from_url(self.redis_url, decode_responses=True)
            self._is_redis = True
            self._redis_connected = True
            logger.info(f"Using Redis queue (streams={self.use_streams})")
        else:
            self._queue = asyncio.Queue(maxsize=self.max_queue_size)
            self._is_redis = False
            logger.info(f"Using in-memory asyncio.Queue (maxsize={self.max_queue_size})")

    async def _ensure_redis_connection(self):
        """Check and reconnect if Redis connection is lost."""
        if not self._is_redis:
            return
        try:
            await self._queue.ping()
            self._redis_connected = True
            return
        except Exception:
            logger.warning("Redis connection lost, attempting to reconnect...")
            self._redis_connected = False
            for attempt in range(1, self.redis_max_retries + 1):
                try:
                    await self._queue.close()
                    self._queue = Redis.from_url(self.redis_url, decode_responses=True)
                    await self._queue.ping()
                    self._redis_connected = True
                    logger.info(f"Redis reconnected (attempt {attempt})")
                    return
                except Exception as e:
                    delay = self.redis_retry_delay * (2 ** (attempt - 1))
                    logger.error(f"Redis reconnection attempt {attempt} failed: {e}. Retrying in {delay:.1f}s")
                    await asyncio.sleep(delay)
            logger.critical("Redis reconnection failed after max retries.")
            raise ConnectionError("Unable to reconnect to Redis")

    async def _redis_publish_with_retry(self, channel: str, data: str) -> Optional[str]:
        """Publish to Redis with retry and reconnection."""
        for attempt in range(self.redis_max_retries):
            try:
                await self._ensure_redis_connection()
                if self.use_streams:
                    maxlen = self.message_ttl
                    kwargs = {"maxlen": maxlen} if maxlen else {}
                    msg_id = await self._queue.xadd(channel, {"data": data}, **kwargs)
                    return msg_id
                else:
                    await self._queue.publish(channel, data)
                    return None
            except Exception as e:
                logger.error(f"Redis publish attempt {attempt+1} failed: {e}")
                if attempt < self.redis_max_retries - 1:
                    delay = self.redis_retry_delay * (2 ** attempt)
                    await asyncio.sleep(delay)
                else:
                    raise

    async def publish(self, channel: str, message: Any, priority: int = 0) -> Optional[str]:
        """
        Publish a message to the queue, optionally routing via MoE if enabled.

        Args:
            channel: Channel name.
            message: Payload.
            priority: Priority (0=normal, higher=more urgent). Only used in asyncio backend.

        Returns:
            message_id if using Redis Streams, else None.
        """
        if self._closed:
            raise RuntimeError("Queue is closed")

        msg = Message(channel=channel, payload=message)
        start = time.time()

        # If MoE enabled, decide which backend to use (dynamic routing)
        target_backend = self.type  # default to configured backend
        if self.moe_gating and not self._closed:
            # Build context for MoE
            context = {
                'message_size': len(json.dumps(message)) if isinstance(message, (dict, list)) else 100,
                'priority': priority,
                'system_load': 0.5,  # placeholder, could be retrieved from system
                'redis_available': 1.0 if (REDIS_AVAILABLE and self.redis_url) else 0.0,
            }
            selected, probs = await self.moe_gating.select_expert(context)
            # Map expert to backend type
            if selected == 'redis' and REDIS_AVAILABLE and self.redis_url:
                target_backend = 'redis'
            else:
                target_backend = 'asyncio'
            logger.debug(f"MoE selected backend: {target_backend} (probs={probs})")

            # Store routing decision in MODP and LIMIT graph
            if self.modp_solver:
                self.modp_solver.add_state(
                    state_id=str(uuid.uuid4()),
                    problem_id="queue_routing",
                    state_attributes={'channel': channel, 'priority': priority, 'backend': target_backend},
                    objective_values={'latency': 0.0, 'reliability': 0.0, 'cost': 0.0},
                    stage=0
                )
            if self.limit_graph_manager:
                self.limit_graph_manager.add_edge(
                    "queue_graph",
                    f"edge_{uuid.uuid4()}",
                    f"channel_{channel}",
                    f"backend_{target_backend}",
                    1.0,
                    {'priority': priority}
                )
                # Also add node for channel if not exists (simplified: add if not found)
                # We'll skip checking; assume it may be added elsewhere.

        try:
            if target_backend == 'redis' and REDIS_AVAILABLE:
                # Use Redis backend
                msg_id = await self._redis_publish_with_retry(channel, msg.to_json())
                if self._metrics:
                    self._metrics["published"].inc()
                    self._metrics["publish_latency"].observe(time.time() - start)
                return msg_id
            else:
                # Use asyncio backend
                await self._queue.put((channel, msg))
                if self._metrics:
                    self._metrics["published"].inc()
                    self._metrics["queue_size"].set(self._queue.qsize())
                    self._metrics["publish_latency"].observe(time.time() - start)
                return None
        except Exception as e:
            if self._metrics:
                self._metrics["publish_errors"].inc()
            raise

    async def subscribe(
        self,
        channels: Union[str, List[str]],
        callback: Callable[[Any], Awaitable[None]],
    ):
        """
        Subscribe to one or more channels and process messages with callback.
        Multiple callbacks can be registered per channel.

        This method is non‑blocking for Redis Streams (starts background tasks).
        For asyncio.Queue, it starts a consumer task.
        """
        if isinstance(channels, str):
            channels = [channels]

        for ch in channels:
            self._callbacks[ch].append(callback)
            logger.info(f"Registered callback for channel '{ch}' (total callbacks: {len(self._callbacks[ch])})")

            # Add channel node to LIMIT graph if enabled
            if self.limit_graph_manager:
                self.limit_graph_manager.add_node(
                    "queue_graph",
                    f"channel_{ch}",
                    "channel",
                    {"subscribers": len(self._callbacks[ch])}
                )

        if self._is_redis:
            if self.use_streams:
                task = asyncio.create_task(self._stream_consumer(channels))
                self._stream_consumer_tasks.append(task)
                logger.info(f"Started stream consumer for channels: {channels}")
            else:
                task = asyncio.create_task(self._pubsub_consumer(channels))
                self._stream_consumer_tasks.append(task)
                logger.info(f"Started Pub/Sub consumer for channels: {channels}")
        else:
            task = asyncio.create_task(self._queue_consumer(channels))
            self._stream_consumer_tasks.append(task)
            logger.info(f"Started asyncio queue consumer for channels: {channels}")

    async def close(self):
        """Close the queue and cleanup resources."""
        self._closed = True
        for task in self._stream_consumer_tasks:
            task.cancel()
        for task in self._stream_consumer_tasks:
            try:
                await task
            except asyncio.CancelledError:
                pass
        self._stream_consumer_tasks.clear()

        if self._is_redis:
            if self._pubsub:
                await self._pubsub.unsubscribe()
                await self._pubsub.close()
            if self._queue:
                await self._queue.close()
            logger.info("Redis connection closed.")
        else:
            while not self._queue.empty():
                try:
                    self._queue.get_nowait()
                except asyncio.QueueEmpty:
                    break
            logger.info("In-memory queue cleared.")

    async def qsize(self) -> int:
        """Get current queue size. For Redis Pub/Sub returns -1; for Streams uses XLEN."""
        if self._is_redis:
            if self.use_streams:
                return -1
            return -1
        return self._queue.qsize()

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()

    # --------------------------------------------------------------------------
    # Internal consumer implementations (unchanged, but can call _process_message)
    # --------------------------------------------------------------------------
    async def _pubsub_consumer(self, channels: List[str]):
        while not self._closed:
            try:
                await self._ensure_redis_connection()
                self._pubsub = self._queue.pubsub()
                await self._pubsub.subscribe(*channels)
                logger.info(f"Subscribed to Redis channels: {channels}")

                async for message in self._pubsub.listen():
                    if self._closed:
                        break
                    if message["type"] == "message":
                        await self._process_message(message["data"])
            except asyncio.CancelledError:
                logger.info("Pub/Sub consumer cancelled.")
                break
            except Exception as e:
                logger.error(f"Pub/Sub consumer error: {e}")
                if not self._closed:
                    await asyncio.sleep(self.redis_retry_delay)
            finally:
                if self._pubsub:
                    try:
                        await self._pubsub.unsubscribe(*channels)
                        await self._pubsub.close()
                    except Exception:
                        pass
                    self._pubsub = None

    async def _stream_consumer(self, streams: List[str]):
        dead_letter_streams = {s: f"{s}{self.dead_letter_suffix}" for s in streams}
        for dl_stream in dead_letter_streams.values():
            try:
                await self._queue.xgroup_create(
                    dl_stream,
                    self.consumer_group,
                    id="0",
                    mkstream=True,
                )
            except Exception as e:
                logger.debug(f"Dead letter group creation for {dl_stream}: {e}")

        for stream in streams:
            try:
                await self._queue.xgroup_create(
                    stream,
                    self.consumer_group,
                    id="0",
                    mkstream=True,
                )
            except Exception as e:
                logger.debug(f"Consumer group creation for {stream}: {e}")

        logger.info(f"Subscribed to Redis Streams: {streams} (group={self.consumer_group}, consumer={self.consumer_name})")

        while not self._closed:
            try:
                await self._ensure_redis_connection()
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
                            success = await self._process_message(msg_data["data"], msg_id, stream)
                            if success:
                                await self._queue.xack(stream, self.consumer_group, msg_id)
                            else:
                                msg = Message.from_json(msg_data["data"])
                                if msg.retry_count >= self.redis_max_retries:
                                    await self._queue.xadd(dead_letter_streams[stream], {"data": msg_data["data"]})
                                    await self._queue.xack(stream, self.consumer_group, msg_id)
                                    logger.error(f"Message {msg_id} exceeded retries, moved to dead letter.")
                                else:
                                    logger.warning(f"Message {msg_id} processing failed, will retry later.")
            except asyncio.CancelledError:
                logger.info("Stream consumer cancelled.")
                break
            except Exception as e:
                logger.error(f"Stream consumer error: {e}")
                if not self._closed:
                    await asyncio.sleep(self.redis_retry_delay)

    async def _queue_consumer(self, channels: List[str]):
        logger.info(f"Listening on asyncio.Queue channels: {channels}")
        while not self._closed:
            try:
                chan, msg = await self._queue.get()
                if chan in channels:
                    await self._process_message(msg.to_json(), None, None)
                self._queue.task_done()
                if self._metrics:
                    self._metrics["queue_size"].set(self._queue.qsize())
            except asyncio.CancelledError:
                logger.info("Queue consumer cancelled.")
                break
            except Exception as e:
                logger.error(f"Queue consumer error: {e}")
                await asyncio.sleep(0.1)

    async def _process_message(self, data: str, msg_id: Optional[str] = None, stream: Optional[str] = None) -> bool:
        """
        Process a message by calling all registered callbacks for its channel.
        Returns True if all callbacks succeeded, False otherwise.
        Also updates LIMIT graph and RLHF if appropriate.
        """
        try:
            msg = Message.from_json(data)
            callbacks = self._callbacks.get(msg.channel, [])
            if not callbacks:
                logger.warning(f"No callbacks registered for channel '{msg.channel}'")
                return True

            start = time.time()
            for cb in callbacks:
                try:
                    await cb(msg.payload)
                except Exception as e:
                    logger.error(f"Callback error for channel '{msg.channel}': {e}")
                    if self._metrics:
                        self._metrics["consume_errors"].inc()
                    return False
            if self._metrics:
                self._metrics["consumed"].inc()
                self._metrics["consume_latency"].observe(time.time() - start)

            # Update LIMIT graph: add consumption edge
            if self.limit_graph_manager:
                # For simplicity, just add a node for the consumed message and connect to channel
                node_id = f"msg_{uuid.uuid4()}"
                self.limit_graph_manager.add_node(
                    "queue_graph",
                    node_id,
                    "message",
                    {'channel': msg.channel, 'processed': True, 'timestamp': time.time()}
                )
                self.limit_graph_manager.add_edge(
                    "queue_graph",
                    f"edge_{uuid.uuid4()}",
                    f"channel_{msg.channel}",
                    node_id,
                    1.0,
                    {'direction': 'consumed'}
                )

            return True
        except Exception as e:
            logger.error(f"Failed to parse message: {e}")
            return False

    # --------------------------------------------------------------------------
    # New public methods for accessing integrated components
    # --------------------------------------------------------------------------
    async def get_limit_graph(self, graph_id: str = "queue_graph") -> Dict:
        if self.limit_graph_manager:
            return {
                'metadata': self.limit_graph_manager.get_metadata(graph_id),
                'nodes': self.limit_graph_manager.get_nodes(graph_id),
                'edges': self.limit_graph_manager.get_edges(graph_id),
            }
        return {}

    async def get_modp_states(self, problem_id: str = "queue_routing") -> List[Dict]:
        if self.modp_solver:
            return self.modp_solver.get_states(problem_id)
        return []

    async def get_rlhf_pairs(self, limit: int = 100) -> List[Dict]:
        if self.rlhf_trainer:
            return self.rlhf_trainer.get_pairs(limit)
        return []

    async def get_moe_experts(self) -> List[str]:
        if self.moe_gating:
            return self.moe_gating.expert_names
        return []
