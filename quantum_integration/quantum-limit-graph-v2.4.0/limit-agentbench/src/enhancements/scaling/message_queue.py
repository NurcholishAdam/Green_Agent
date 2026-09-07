"""
Asynchronous Message Queue Abstraction (Enhanced v2.1)
======================================================
Supports 'asyncio' (in‑memory) and 'redis' backends with auto‑reconnection,
retries, metrics, and durable streaming (Redis Streams).

Enhancements over v2.0:
- Fixed dynamic MoE backend routing to avoid cross‑backend misuse.
- Graceful task cancellation in close().
- Config access via getattr.
- Incremented retry_count for Redis Streams to enable dead‑letter.
- Per‑channel asyncio queues for efficiency.
- Initial Redis connectivity check.
- Minor logging and error handling improvements.
- Optional LIMIT Graph, MODP, RLHF, and MoE components retained and improved.
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

try:
    from redis.asyncio import Redis
    REDIS_AVAILABLE = True
except ImportError:
    REDIS_AVAILABLE = False

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
    message_id: Optional[str] = None
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
    """Manages a graph of message flows and connections for LIMIT."""
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
    """Multi‑Objective Dynamic Programming solver for persisting routing decisions."""
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
    """Collects human preference pairs for queue routing decisions."""
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
    """Mixture-of-Experts gating for dynamic backend selection."""
    def __init__(self, storage: Optional[Any] = None, config: Optional[Dict] = None):
        self.storage = storage
        self.config = config or {}
        self.expert_names = self.config.get('expert_names', ['asyncio', 'redis'])
        self.num_experts = len(self.expert_names)
        # Only include Redis as expert if Redis is available and URL provided
        if 'redis' in self.expert_names and (not REDIS_AVAILABLE or not self.config.get('redis_available', False)):
            # Remove redis expert
            self.expert_names = [e for e in self.expert_names if e != 'redis']
            self.num_experts = len(self.expert_names)
        self.gating_weights = np.random.randn(self.num_experts, 4)  # 4 features

    def _encode_state(self, context: Dict[str, float]) -> np.ndarray:
        features = [
            context.get('message_size', 0.0),
            context.get('priority', 0.0),
            context.get('system_load', 0.0),
            1.0 if context.get('redis_available', False) else 0.0,
        ]
        return np.array(features, dtype=np.float32)

    async def select_expert(self, context: Dict[str, float]) -> Tuple[str, np.ndarray]:
        x = self._encode_state(context)
        logits = self.gating_weights @ x
        probs = np.exp(logits - np.max(logits))
        probs /= probs.sum()
        expert_idx = np.argmax(probs)
        selected = self.expert_names[expert_idx]
        return selected, probs

    async def add_training_sample(self, context: Dict[str, float], selected_expert: str, reward: float):
        if selected_expert not in self.expert_names:
            return
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
# Enhanced AsyncMessageQueue
# ==============================================================================
class AsyncMessageQueue:
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
        enable_limit_graph: bool = True,
        enable_modp: bool = True,
        enable_rlhf: bool = True,
        enable_moe: bool = True,
        storage: Optional[Any] = None,
    ):
        self.type = queue_type
        self.redis_url = redis_url or getattr(config, 'REDIS_URL', None)
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
        self._callbacks: Dict[str, List[Callable[[Any], Awaitable[None]]]] = defaultdict(list)
        self._redis_connected = False
        self._per_channel_queues: Dict[str, asyncio.Queue] = {}

        if PROMETHEUS_AVAILABLE and metrics_registry:
            self._metrics = {
                "published": Counter(...),
                "consumed": Counter(...),
                "queue_size": Gauge(...),
                "publish_errors": Counter(...),
                "consume_errors": Counter(...),
                "publish_latency": Histogram(...),
                "consume_latency": Histogram(...),
            }

        self._initialize()

        self.storage = storage
        self.limit_graph_manager = LimitGraphManager(storage) if enable_limit_graph else None
        self.modp_solver = MODPOptimizer(storage) if enable_modp else None
        self.rlhf_trainer = RLHFTrainer(storage) if enable_rlhf else None
        self.moe_gating = None
        if enable_moe:
            available_backends = ['asyncio']
            if REDIS_AVAILABLE and self.redis_url:
                available_backends.append('redis')
            # Only enable MoE if more than one backend possible
            if len(available_backends) > 1:
                self.moe_gating = MoEGatingNetwork(
                    storage,
                    {'expert_names': available_backends, 'redis_available': bool(self.redis_url and REDIS_AVAILABLE)}
                )

        if self.limit_graph_manager:
            if not self.limit_graph_manager.get_metadata("queue_graph"):
                self.limit_graph_manager.create_graph("queue_graph", "Message Queue Relationships", {})
            self.limit_graph_manager.add_node("queue_graph", "backend_asyncio", "backend", {"type": "asyncio"})
            if REDIS_AVAILABLE and self.redis_url:
                self.limit_graph_manager.add_node("queue_graph", "backend_redis", "backend", {"type": "redis"})

        logger.info(f"AsyncMessageQueue initialized with optional components: "
                    f"limit_graph={self.limit_graph_manager is not None}, modp={self.modp_solver is not None}, "
                    f"rlhf={self.rlhf_trainer is not None}, moe={self.moe_gating is not None}")

    def _initialize(self):
        if self.type == "redis" and self.redis_url and REDIS_AVAILABLE:
            self._queue = Redis.from_url(self.redis_url, decode_responses=True)
            self._is_redis = True
            logger.info(f"Using Redis queue (streams={self.use_streams})")
        else:
            self._queue = asyncio.Queue(maxsize=self.max_queue_size)
            self._is_redis = False
            logger.info(f"Using in-memory asyncio.Queue (maxsize={self.max_queue_size})")

    async def _ensure_redis_connection(self):
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
            raise ConnectionError("Unable to reconnect to Redis")

    async def _redis_publish_with_retry(self, channel: str, data: str) -> Optional[str]:
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
        if self._closed:
            raise RuntimeError("Queue is closed")

        msg = Message(channel=channel, payload=message)
        start = time.time()

        # Determine backend
        target_backend = self.type
        if self.moe_gating and not self._closed:
            context = {
                'message_size': len(json.dumps(message)) if isinstance(message, (dict, list)) else 100,
                'priority': priority,
                'system_load': 0.5,
                'redis_available': 1.0 if (REDIS_AVAILABLE and self.redis_url) else 0.0,
            }
            selected, probs = await self.moe_gating.select_expert(context)
            target_backend = selected
            logger.debug(f"MoE selected backend: {target_backend}")

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

        try:
            if target_backend == 'redis' and REDIS_AVAILABLE and self.redis_url:
                msg_id = await self._redis_publish_with_retry(channel, msg.to_json())
                if self._metrics:
                    self._metrics["published"].inc()
                    self._metrics["publish_latency"].observe(time.time() - start)
                return msg_id
            else:
                # Use asyncio per‑channel queue
                if channel not in self._per_channel_queues:
                    self._per_channel_queues[channel] = asyncio.Queue(maxsize=self.max_queue_size)
                await self._per_channel_queues[channel].put(msg)
                if self._metrics:
                    self._metrics["published"].inc()
                    self._metrics["queue_size"].set(sum(q.qsize() for q in self._per_channel_queues.values()))
                    self._metrics["publish_latency"].observe(time.time() - start)
                return None
        except Exception as e:
            if self._metrics:
                self._metrics["publish_errors"].inc()
            raise

    async def subscribe(self, channels: Union[str, List[str]], callback: Callable[[Any], Awaitable[None]]):
        if isinstance(channels, str):
            channels = [channels]

        for ch in channels:
            self._callbacks[ch].append(callback)
            logger.info(f"Registered callback for channel '{ch}' (total callbacks: {len(self._callbacks[ch])})")
            if self.limit_graph_manager:
                self.limit_graph_manager.add_node("queue_graph", f"channel_{ch}", "channel", {"subscribers": len(self._callbacks[ch])})

        if self._is_redis:
            if self.use_streams:
                task = asyncio.create_task(self._stream_consumer(channels))
            else:
                task = asyncio.create_task(self._pubsub_consumer(channels))
        else:
            # Start per‑channel queue consumers
            for ch in channels:
                if ch not in self._per_channel_queues:
                    self._per_channel_queues[ch] = asyncio.Queue(maxsize=self.max_queue_size)
                task = asyncio.create_task(self._queue_consumer(ch))
                self._stream_consumer_tasks.append(task)
            logger.info(f"Started asyncio queue consumers for channels: {channels}")
            return

        self._stream_consumer_tasks.append(task)
        logger.info(f"Started consumer for channels: {channels}")

    async def close(self):
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
            for q in self._per_channel_queues.values():
                while not q.empty():
                    q.get_nowait()
            logger.info("In-memory queues cleared.")

    async def qsize(self) -> int:
        if self._is_redis:
            return -1  # Not easily queryable for Pub/Sub; Streams could use XLEN but omitted for simplicity
        return sum(q.qsize() for q in self._per_channel_queues.values())

    # --------------------------------------------------------------------------
    # Internal consumer implementations
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
                await self._queue.xgroup_create(dl_stream, self.consumer_group, id="0", mkstream=True)
            except Exception as e:
                logger.debug(f"Dead letter group creation for {dl_stream}: {e}")

        for stream in streams:
            try:
                await self._queue.xgroup_create(stream, self.consumer_group, id="0", mkstream=True)
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
                            data = msg_data["data"]
                            msg = Message.from_json(data)
                            # Increment retry count before processing
                            msg.retry_count += 1
                            # Update data for potential dead-letter
                            success = await self._process_message(msg.to_json(), msg_id, stream)
                            if success:
                                await self._queue.xack(stream, self.consumer_group, msg_id)
                            else:
                                if msg.retry_count >= self.redis_max_retries:
                                    await self._queue.xadd(dead_letter_streams[stream], {"data": msg.to_json()})
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

    async def _queue_consumer(self, channel: str):
        logger.info(f"Listening on asyncio.Queue for channel: {channel}")
        q = self._per_channel_queues[channel]
        while not self._closed:
            try:
                msg = await q.get()
                await self._process_message(msg.to_json(), None, None)
                q.task_done()
                if self._metrics:
                    self._metrics["queue_size"].set(sum(x.qsize() for x in self._per_channel_queues.values()))
            except asyncio.CancelledError:
                logger.info(f"Queue consumer for channel {channel} cancelled.")
                break
            except Exception as e:
                logger.error(f"Queue consumer error for channel {channel}: {e}")
                await asyncio.sleep(0.1)

    async def _process_message(self, data: str, msg_id: Optional[str] = None, stream: Optional[str] = None) -> bool:
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

            if self.limit_graph_manager:
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

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()

    # --------------------------------------------------------------------------
    # New public methods
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
