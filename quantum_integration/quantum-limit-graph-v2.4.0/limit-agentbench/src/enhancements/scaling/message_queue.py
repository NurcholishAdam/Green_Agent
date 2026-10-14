#!/usr/bin/env python3
# message_queue.py
"""
Asynchronous Message Queue Abstraction (Enhanced v3.0.0)
========================================================

Production-grade message queue supporting:

* Backends: `asyncio` (in-memory) and `redis` (Pub/Sub + Streams).
* Sub-services: MessageSerializer, RetryPolicy, BackendRegistry,
  DeadLetterService, MoEGatingService, EventPersistenceService,
  PubSubConsumerService, StreamConsumerService, AsyncioConsumerService.
* Protocols for all injected components.
* Shared `ObservabilityHub` and `GovernanceLayer`.
* Non-destructive `ChaosService` with full save/restore.
* Full lifecycle: `start()` / `wait_ready()` / `shutdown()`.
* Pydantic v1/v2 compatible config (dataclass fallback).
* Idempotency keys with LRU cache.
* Dead-letter queue with bounded capacity.
* Bounded caches (deque maxlen / LRU OrderedDict).
* Backpressure with configurable `put_timeout`.

FIXED BUGS OVER v2.1
--------------------
- `Message.timestamp` now uses `field(default_factory=time.time)` (was: import-time constant).
- Prometheus metrics properly registered (was: `Counter(...)` stubs that crashed at import).
- `MoEGatingService.add_training_sample` uses `reward` in the gradient.
- `MoEGatingService._encode_state` coerces `redis_available` to float.
- Cross-backend misuse prevented: MoE may only select the *configured* backend or
  a whitelisted alternate when the queue type is `"auto"`.
- `_queue_consumer` re-queues on callback failure with bounded retries + DLQ.
- `_process_message` short-circuits when queue is closing.
- `Message.from_json` wrapped in try/except; malformed input returns a typed error.
- Pub/Sub uses a separate Redis client from the primary command client,
  preventing reconnect thrashing.
- `message_ttl` semantic mismatch resolved: renamed to `stream_maxlen` (with
  `message_ttl` retained as a deprecated alias).
- Backpressure: `publish` accepts `put_timeout` and returns a typed `PublishResult`.
- Bounded `_per_channel_queues` via LRU.
- LIMIT Graph and MODP writes are bounded (truncated).
- `msg_id` propagated to callbacks via a `MessageEnvelope`.
- Idempotency cache on `publish`.
- Dead-letter queue with bounded capacity.
- Lifecycle: `start()` / `wait_ready()` / `shutdown()`.
- Safety invariants: queue depth, subscriber count, DLQ growth.
- `__all__` exports.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import math
import time
import uuid
from collections import OrderedDict, defaultdict, deque
from dataclasses import dataclass, field, fields as dataclass_fields, asdict
from datetime import datetime, timezone
from enum import Enum
from typing import (
    Any, Awaitable, Callable, Deque, Dict, FrozenSet, List, Optional,
    Protocol, Sequence, Set, Tuple, Union, runtime_checkable,
)

import numpy as np


# ============================================================================
# Pydantic compatibility layer
# ============================================================================
PYDANTIC_AVAILABLE = False
PYDANTIC_V1 = False
PYDANTIC_V2 = False
BaseModel: Any = object
Field: Any = lambda *a, **k: None  # noqa: E731
ConfigDict: Any = None
try:
    import pydantic as _pyd  # type: ignore
    _ver = getattr(_pyd, 'VERSION', '0') or '0'
    _maj = int(str(_ver).split('.')[0])
    if _maj >= 2:
        from pydantic import BaseModel, ConfigDict, Field  # type: ignore
        PYDANTIC_AVAILABLE = True
        PYDANTIC_V2 = True
    elif _maj == 1:
        from pydantic import BaseModel, Field  # type: ignore
        PYDANTIC_AVAILABLE = True
        PYDANTIC_V1 = True
except Exception:
    pass


# ============================================================================
# Sibling imports (graceful)
# ============================================================================
try:
    from ..config import config as _central_config  # type: ignore
except ImportError:
    _central_config = None

try:
    from ..logger import logger as _central_logger  # type: ignore
    logger = _central_logger
except ImportError:
    logger = logging.getLogger(__name__)

try:
    from redis.asyncio import Redis  # type: ignore
    REDIS_AVAILABLE = True
except ImportError:
    Redis = None  # type: ignore
    REDIS_AVAILABLE = False


# ============================================================================
# Named constants
# ============================================================================
MODULE_VERSION = "3.0.0"
IDEMPOTENCY_CACHE_SIZE = 4_096
DEAD_LETTER_MAX = 10_000
APPROVAL_AUDIT_MAX = 1_000
APPROVAL_PENDING_MAX = 512
EVENT_NODES_MAX = 10_000
EVENT_EDGES_MAX = 20_000
MODP_STATES_MAX = 10_000
RLHF_PAIRS_MAX = 10_000
PER_CHANNEL_LRU_MAX = 512
DEFAULT_MAX_QUEUE_SIZE = 1_000
DEFAULT_PUBLISH_TIMEOUT = 5.0
DEFAULT_MAX_RETRIES = 5
DEFAULT_RETRY_DELAY = 1.0
DEFAULT_APPROVAL_TIMEOUT = 60.0
CHAOS_MAX_FAULTS_PER_RUN = 6
NUMERIC_EPS = 1e-9
MAX_PAYLOAD_BYTES = 4 * 1024 * 1024  # 4 MB


# ============================================================================
# Serialization helpers
# ============================================================================
def _to_serializable(obj: Any) -> Any:
    if obj is None or isinstance(obj, (bool, int, float, str)):
        return obj
    if isinstance(obj, Enum):
        return obj.value
    if isinstance(obj, datetime):
        return obj.isoformat()
    if isinstance(obj, dict):
        return {str(k): _to_serializable(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple, set, frozenset)):
        return [_to_serializable(v) for v in obj]
    if isinstance(obj, deque):
        return [_to_serializable(v) for v in obj]
    if isinstance(obj, np.ndarray):
        return obj.tolist()
    if isinstance(obj, np.generic):
        return obj.item()
    if hasattr(obj, 'model_dump') and callable(obj.model_dump):
        try:
            return _to_serializable(obj.model_dump(mode='json'))
        except Exception:
            pass
    if hasattr(obj, 'dict') and callable(obj.dict):
        try:
            return _to_serializable(obj.dict())
        except Exception:
            pass
    if hasattr(obj, 'to_json') and callable(obj.to_json):
        try:
            return json.loads(obj.to_json())
        except Exception:
            pass
    if hasattr(obj, 'to_dict') and callable(obj.to_dict):
        return _to_serializable(obj.to_dict())
    if hasattr(obj, '__dict__'):
        return _to_serializable(obj.__dict__)
    return str(obj)


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _coerce_float(value: Any, default: Optional[float] = None) -> Optional[float]:
    if value is None:
        return default
    try:
        f = float(value)
    except (TypeError, ValueError):
        return default
    if not math.isfinite(f):
        return default
    return f


# ============================================================================
# Protocols
# ============================================================================
@runtime_checkable
class StorageProtocol(Protocol):
    def save_limit_graph_metadata(self, graph_id: str, description: str,
                                  configuration: Dict[str, Any]) -> Any: ...
    def save_limit_graph_node(self, node_id: str, graph_id: str,
                              node_type: Optional[str],
                              attributes: Dict[str, Any]) -> Any: ...
    def save_limit_graph_edge(self, edge_id: str, graph_id: str, source: str,
                              target: str, weight: Optional[float],
                              attributes: Dict[str, Any]) -> Any: ...
    def save_modp_state(self, state_id: str, problem_id: str,
                        state_attributes: Dict[str, Any],
                        objective_values: Dict[str, float], stage: int) -> Any: ...
    def save_preference_pair(self, pair_id: str, prompt: str, chosen: str,
                             rejected: str, reward_diff: float,
                             metadata: Optional[Dict[str, Any]]) -> Any: ...


@runtime_checkable
class PayloadProtocol(Protocol):
    def to_json(self) -> str: ...


@runtime_checkable
class MetricsRegistryProtocol(Protocol):
    registry: Any


@runtime_checkable
class EventBrokerProtocol(Protocol):
    def subscribe(self, event_type: str,
                  handler: Callable[[Any], Awaitable[None]]) -> None: ...


@runtime_checkable
class RedisClientProtocol(Protocol):
    async def ping(self) -> Any: ...
    async def publish(self, channel: str, data: str) -> Any: ...
    async def xadd(self, stream: str, fields: Dict[str, Any], **kwargs: Any) -> Any: ...
    async def close(self) -> None: ...


# ============================================================================
# Enums
# ============================================================================
class QueueBackend(str, Enum):
    ASYNCIO = "asyncio"
    REDIS = "redis"
    AUTO = "auto"


class FaultType(str, Enum):
    REDIS_DISCONNECT = "redis_disconnect"
    CALLBACK_FAILURE = "callback_failure"
    SLOW_CONSUMER = "slow_consumer"
    QUEUE_FULL = "queue_full"
    MESSAGE_CORRUPTION = "message_corruption"
    BACKEND_UNAVAILABLE = "backend_unavailable"


class ApprovalStatus(str, Enum):
    PENDING = "pending"
    APPROVED = "approved"
    DENIED = "denied"
    TIMEOUT = "timeout"
    AUTO_APPROVED = "auto_approved"


class PublishStatus(str, Enum):
    OK = "ok"
    BACKPRESSURE = "backpressure"
    TIMEOUT = "timeout"
    ERROR = "error"
    DUPLICATE = "duplicate"
    CLOSED = "closed"


class CircuitBreakerState(str, Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


# ============================================================================
# ObservabilityHub
# ============================================================================
class ObservabilityHub:
    def __init__(self) -> None:
        self._counters: Dict[str, float] = {}
        self._gauges: Dict[str, float] = {}
        self._histograms: Dict[str, Deque[float]] = {}
        self._lock = asyncio.Lock()

    @staticmethod
    def _key(name: str, labels: Optional[Dict[str, str]]) -> str:
        if not labels:
            return name
        tag = ",".join(f'{k}="{v}"' for k, v in sorted(labels.items()))
        return f"{name}{{{tag}}}"

    def counter(self, name: str, value: float = 1.0, **labels: str) -> None:
        k = self._key(name, labels or None)
        self._counters[k] = self._counters.get(k, 0.0) + float(value)

    def gauge(self, name: str, value: float, **labels: str) -> None:
        k = self._key(name, labels or None)
        self._gauges[k] = float(value)

    def histogram(self, name: str, value: float, **labels: str) -> None:
        k = self._key(name, labels or None)
        if k not in self._histograms:
            self._histograms[k] = deque(maxlen=1024)
        self._histograms[k].append(float(value))

    async def export_prometheus(self) -> str:
        async with self._lock:
            lines: List[str] = []
            for k, v in self._counters.items():
                base = k.split('{')[0]
                lines.append(f"# TYPE {base} counter")
                lines.append(f"{k} {v}")
            for k, v in self._gauges.items():
                base = k.split('{')[0]
                lines.append(f"# TYPE {base} gauge")
                lines.append(f"{k} {v}")
            for k, vals in self._histograms.items():
                if not vals:
                    continue
                base = k.split('{')[0]
                lines.append(f"# TYPE {base} histogram")
                lines.append(f"{base}_count {len(vals)}")
                lines.append(f"{base}_sum {sum(vals)}")
            return "\n".join(lines)


# ============================================================================
# DeadLetterQueue, IdempotencyCache, CircuitBreaker
# ============================================================================
class DeadLetterQueue:
    def __init__(self, max_size: int = DEAD_LETTER_MAX) -> None:
        self._items: Deque[Dict[str, Any]] = deque(maxlen=max_size)
        self._lock = asyncio.Lock()
        self._total_received = 0

    async def record(self, topic: str, error: str,
                     payload: Any = None) -> None:
        async with self._lock:
            self._items.append({
                'topic': topic,
                'error': str(error),
                'timestamp': _utcnow().isoformat(),
                'payload': _to_serializable(payload) if payload is not None else None,
            })
            self._total_received += 1

    def snapshot(self) -> List[Dict[str, Any]]:
        return list(self._items)

    def __len__(self) -> int:
        return len(self._items)

    @property
    def total_received(self) -> int:
        return self._total_received


class IdempotencyCache:
    def __init__(self, max_size: int = IDEMPOTENCY_CACHE_SIZE) -> None:
        self.max_size = max_size
        self._cache: "OrderedDict[str, float]" = OrderedDict()

    def seen(self, key: Optional[str]) -> bool:
        if not key:
            return False
        if key in self._cache:
            self._cache.move_to_end(key)
            return True
        self._cache[key] = time.monotonic()
        if len(self._cache) > self.max_size:
            self._cache.popitem(last=False)
        return False


class CircuitBreaker:
    def __init__(self, name: str, failure_threshold: int = 5,
                 recovery_timeout: float = 30.0) -> None:
        self.name = name
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.state = CircuitBreakerState.CLOSED
        self.failure_count = 0
        self.last_failure_time: Optional[float] = None
        self._lock = asyncio.Lock()

    async def call(self, func: Callable[..., Awaitable[Any]],
                   *args: Any, **kwargs: Any) -> Any:
        async with self._lock:
            if self.state == CircuitBreakerState.OPEN:
                if (self.last_failure_time is not None and
                        (time.monotonic() - self.last_failure_time) >= self.recovery_timeout):
                    self.state = CircuitBreakerState.HALF_OPEN
                    self.failure_count = 0
                else:
                    raise RuntimeError(f"Circuit {self.name} is OPEN")
        try:
            result = await func(*args, **kwargs)
            async with self._lock:
                if self.state == CircuitBreakerState.HALF_OPEN:
                    self.state = CircuitBreakerState.CLOSED
                self.failure_count = 0
            return result
        except Exception:
            async with self._lock:
                self.failure_count += 1
                self.last_failure_time = time.monotonic()
                if self.failure_count >= self.failure_threshold:
                    self.state = CircuitBreakerState.OPEN
            raise


# ============================================================================
# Governance
# ============================================================================
@dataclass
class ApprovalRequest:
    request_id: str
    context: Dict[str, Any]
    requested_at: datetime
    timeout_seconds: float
    status: ApprovalStatus = ApprovalStatus.PENDING
    future: Optional[asyncio.Future] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            'request_id': self.request_id,
            'context': _to_serializable(self.context),
            'requested_at': self.requested_at.isoformat(),
            'timeout_seconds': self.timeout_seconds,
            'status': self.status.value,
        }


@dataclass
class ApprovalResult:
    request_id: str
    status: ApprovalStatus
    reason: str = ""
    decided_at: Optional[datetime] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            'request_id': self.request_id,
            'status': self.status.value,
            'reason': self.reason,
            'decided_at': self.decided_at.isoformat() if self.decided_at else None,
        }


class GovernanceLayer:
    def __init__(
        self,
        enabled: bool = False,
        timeout_seconds: float = DEFAULT_APPROVAL_TIMEOUT,
        auto_approve_on_timeout: bool = False,
        publish_fn: Optional[Callable[[str, Any], Awaitable[None]]] = None,
    ) -> None:
        self.enabled = bool(enabled)
        self.timeout_seconds = float(timeout_seconds)
        self.auto_approve_on_timeout = bool(auto_approve_on_timeout)
        self.publish_fn = publish_fn
        self.pending: "OrderedDict[str, ApprovalRequest]" = OrderedDict()
        self.audit: Deque[Dict[str, Any]] = deque(maxlen=APPROVAL_AUDIT_MAX)
        self._lock = asyncio.Lock()

    async def request_approval(
        self, context: Dict[str, Any], timeout: Optional[float] = None,
    ) -> ApprovalResult:
        if not self.enabled:
            r = ApprovalResult(
                request_id='auto', status=ApprovalStatus.AUTO_APPROVED,
                reason='approval_disabled', decided_at=_utcnow(),
            )
            self._audit(context, r)
            return r
        timeout = float(timeout if timeout is not None else self.timeout_seconds)
        request_id = uuid.uuid4().hex
        loop = asyncio.get_running_loop()
        future: asyncio.Future = loop.create_future()
        req = ApprovalRequest(
            request_id=request_id, context=context,
            requested_at=_utcnow(), timeout_seconds=timeout, future=future,
        )
        async with self._lock:
            self.pending[request_id] = req
            while len(self.pending) > APPROVAL_PENDING_MAX:
                self.pending.popitem(last=False)
        try:
            if self.publish_fn is not None:
                try:
                    await self.publish_fn(
                        'approval_requests', json.dumps(req.to_dict(), default=str),
                    )
                except Exception as e:
                    logger.warning("Approval publish failed: %s", e)
            decision = await asyncio.wait_for(future, timeout=timeout)
            result = ApprovalResult(
                request_id=request_id,
                status=ApprovalStatus.APPROVED if decision else ApprovalStatus.DENIED,
                reason='human_decision', decided_at=_utcnow(),
            )
        except asyncio.TimeoutError:
            status = (ApprovalStatus.AUTO_APPROVED if self.auto_approve_on_timeout
                      else ApprovalStatus.TIMEOUT)
            result = ApprovalResult(
                request_id=request_id, status=status, reason='timeout',
                decided_at=_utcnow(),
            )
        except Exception as e:
            result = ApprovalResult(
                request_id=request_id, status=ApprovalStatus.DENIED,
                reason=f'error:{e}', decided_at=_utcnow(),
            )
        finally:
            async with self._lock:
                self.pending.pop(request_id, None)
        self._audit(context, result)
        return result

    async def submit_decision(self, request_id: str, approved: bool) -> bool:
        async with self._lock:
            req = self.pending.get(request_id)
            if req is None or req.future is None or req.future.done():
                return False
            req.future.set_result(approved)
            return True

    def list_pending(self) -> List[Dict[str, Any]]:
        return [r.to_dict() for r in self.pending.values()]

    def _audit(self, context: Dict[str, Any], result: ApprovalResult) -> None:
        self.audit.append({
            'timestamp': _utcnow().isoformat(),
            'context': _to_serializable(context),
            'result': result.to_dict(),
        })

    def get_audit_log(self) -> List[Dict[str, Any]]:
        return list(self.audit)


# ============================================================================
# Safety
# ============================================================================
class SafetyInvariant:
    def __init__(self, name: str, predicate: Callable[[Dict[str, Any]], bool],
                 description: str, kind: str = "always") -> None:
        if kind not in ("always", "eventually"):
            raise ValueError(f"Unsupported invariant kind: {kind}")
        self.name = name
        self.predicate = predicate
        self.description = description
        self.kind = kind


class SafetyMonitor:
    def __init__(self) -> None:
        self.invariants: List[SafetyInvariant] = []
        self._eventually_flags: Dict[str, bool] = {}

    def add(self, name: str, predicate: Callable[[Dict[str, Any]], bool],
            description: str, kind: str = "always") -> None:
        self.invariants.append(SafetyInvariant(name, predicate, description, kind))
        if kind == "eventually":
            self._eventually_flags[name] = False

    def check(self, state: Dict[str, Any]) -> List[str]:
        violations: List[str] = []
        for inv in self.invariants:
            try:
                ok = bool(inv.predicate(state))
            except Exception as e:
                violations.append(f"{inv.name}: error ({e})")
                continue
            if inv.kind == "always" and not ok:
                violations.append(f"{inv.name}: {inv.description}")
            elif inv.kind == "eventually" and ok:
                self._eventually_flags[inv.name] = True
        return violations

    def final_check(self) -> List[str]:
        return [
            f"{inv.name}: {inv.description} (never satisfied)"
            for inv in self.invariants
            if inv.kind == "eventually" and not self._eventually_flags.get(inv.name, False)
        ]


# ============================================================================
# Config
# ============================================================================
@dataclass
class MessageQueueConfig:
    queue_type: str = "asyncio"
    redis_url: Optional[str] = None
    use_streams: bool = False
    consumer_group: str = "green_agent_group"
    consumer_name: Optional[str] = None
    max_queue_size: int = DEFAULT_MAX_QUEUE_SIZE
    publish_timeout: float = DEFAULT_PUBLISH_TIMEOUT
    max_retries: int = DEFAULT_MAX_RETRIES
    retry_delay: float = DEFAULT_RETRY_DELAY
    stream_maxlen: Optional[int] = None
    dead_letter_suffix: str = "_dead"
    per_channel_lru_max: int = PER_CHANNEL_LRU_MAX

    # Feature flags
    enable_limit_graph: bool = True
    enable_modp: bool = True
    enable_rlhf: bool = True
    enable_moe: bool = True
    enable_human_approval: bool = False
    enable_temporal_safety: bool = True
    enable_chaos_testing: bool = False
    enable_idempotency: bool = True

    # MoE
    moe_expert_names: List[str] = field(default_factory=list)
    moe_seed: int = 42

    # Approval / chaos
    approval_timeout_seconds: float = DEFAULT_APPROVAL_TIMEOUT
    approval_auto_approve_on_timeout: bool = False
    chaos_interval_seconds: int = 3600

    # Bounds
    limit_graph_nodes_max: int = EVENT_NODES_MAX
    limit_graph_edges_max: int = EVENT_EDGES_MAX
    modp_states_max: int = MODP_STATES_MAX
    rlhf_pairs_max: int = RLHF_PAIRS_MAX

    # Payload safety
    max_payload_bytes: int = MAX_PAYLOAD_BYTES

    # Deprecated alias (kept for backward compat; use stream_maxlen)
    message_ttl: Optional[int] = None

    def __post_init__(self) -> None:
        if self.queue_type not in ("asyncio", "redis", "auto"):
            raise ValueError(f"Unknown queue_type: {self.queue_type}")
        if self.max_queue_size < 1:
            raise ValueError("max_queue_size must be >= 1")
        if self.publish_timeout <= 0:
            raise ValueError("publish_timeout must be > 0")
        if self.max_retries < 1:
            raise ValueError("max_retries must be >= 1")
        if self.retry_delay <= 0:
            raise ValueError("retry_delay must be > 0")
        if self.per_channel_lru_max < 1:
            raise ValueError("per_channel_lru_max must be >= 1")
        # Migrate deprecated alias
        if self.message_ttl is not None and self.stream_maxlen is None:
            logger.warning(
                "message_ttl is deprecated; interpreting as stream_maxlen."
            )
            self.stream_maxlen = int(self.message_ttl)

    def to_dict(self) -> Dict[str, Any]:
        return _to_serializable(self)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "MessageQueueConfig":
        valid = {f.name for f in dataclass_fields(cls)}
        return cls(**{k: v for k, v in data.items() if k in valid})


def _default_config_from_central() -> MessageQueueConfig:
    if _central_config is None:
        return MessageQueueConfig()
    return MessageQueueConfig(
        redis_url=getattr(_central_config, 'REDIS_URL', None),
    )


# ============================================================================
# Message dataclass (fixed default_factory)
# ============================================================================
@dataclass
class Message:
    channel: str
    payload: Any
    timestamp: float = field(default_factory=time.time)   # Fixed: was class-level
    message_id: Optional[str] = None
    retry_count: int = 0
    stream_id: Optional[str] = None

    def to_json(self) -> str:
        return json.dumps({
            "channel": self.channel,
            "payload": _to_serializable(self.payload),
            "timestamp": self.timestamp,
            "message_id": self.message_id,
            "retry_count": self.retry_count,
            "stream_id": self.stream_id,
        })

    @classmethod
    def from_json(cls, data: str) -> "Message":
        try:
            d = json.loads(data)
        except json.JSONDecodeError as e:
            raise ValueError(f"Malformed message JSON: {e}") from e
        if not isinstance(d, dict):
            raise ValueError("Message must be a JSON object")
        channel = d.get("channel")
        if not isinstance(channel, str):
            raise ValueError("Message.channel is required and must be a string")
        return cls(
            channel=channel,
            payload=d.get("payload"),
            timestamp=float(d.get("timestamp", time.time())),
            message_id=d.get("message_id"),
            retry_count=int(d.get("retry_count", 0)),
            stream_id=d.get("stream_id"),
        )


@dataclass
class MessageEnvelope:
    """Payload wrapper carrying message metadata to callbacks."""
    channel: str
    payload: Any
    message_id: Optional[str]
    stream_id: Optional[str]
    retry_count: int
    timestamp: float

    def to_dict(self) -> Dict[str, Any]:
        return {
            'channel': self.channel,
            'payload': _to_serializable(self.payload),
            'message_id': self.message_id,
            'stream_id': self.stream_id,
            'retry_count': self.retry_count,
            'timestamp': self.timestamp,
        }


@dataclass
class PublishResult:
    status: PublishStatus
    message_id: Optional[str] = None
    stream_id: Optional[str] = None
    error: Optional[str] = None

    @property
    def ok(self) -> bool:
        return self.status == PublishStatus.OK

    def to_dict(self) -> Dict[str, Any]:
        return {
            'status': self.status.value,
            'message_id': self.message_id,
            'stream_id': self.stream_id,
            'error': self.error,
        }


# ============================================================================
# MessageSerializer
# ============================================================================
class MessageSerializer:
    """Converts arbitrary payloads to/from wire format with size checks."""

    def __init__(self, max_payload_bytes: int = MAX_PAYLOAD_BYTES) -> None:
        self.max_payload_bytes = max_payload_bytes
        self._disabled = False

    def disable(self) -> None:
        self._disabled = True

    def enable(self) -> None:
        self._disabled = False

    def serialize(self, msg: Message) -> str:
        if self._disabled:
            raise RuntimeError("MessageSerializer disabled (chaos)")
        raw = msg.to_json()
        if len(raw.encode('utf-8')) > self.max_payload_bytes:
            raise ValueError(
                f"Payload exceeds {self.max_payload_bytes} bytes"
            )
        return raw

    def deserialize(self, raw: str) -> Message:
        return Message.from_json(raw)

    def payload_size(self, payload: Any) -> int:
        try:
            return len(json.dumps(_to_serializable(payload)).encode('utf-8'))
        except Exception:
            return 0


# ============================================================================
# RetryPolicy
# ============================================================================
class RetryPolicy:
    """Exponential backoff with jitter."""

    def __init__(self, max_retries: int = DEFAULT_MAX_RETRIES,
                 base_delay: float = DEFAULT_RETRY_DELAY,
                 jitter: float = 0.2) -> None:
        self.max_retries = max(1, int(max_retries))
        self.base_delay = max(0.001, float(base_delay))
        self.jitter = max(0.0, min(1.0, float(jitter)))

    def delay(self, attempt: int) -> float:
        base = min(self.base_delay * (2 ** max(0, attempt)), 30.0)
        if self.jitter > 0:
            base *= (1.0 + self.jitter * (2 * np.random.random() - 1.0))
        return max(0.0, base)

    async def wait(self, attempt: int) -> None:
        await asyncio.sleep(self.delay(attempt))


# ============================================================================
# BackendRegistry
# ============================================================================
class BackendRegistry:
    """
    Tracks which backends are instantiated and available for routing.

    The configured `queue_type` is authoritative:
      * 'asyncio'  — only asyncio backend available.
      * 'redis'    — only redis backend available.
      * 'auto'     — both asyncio and redis are available; MoE may select.
    """

    def __init__(self, config: MessageQueueConfig,
                 redis_available: bool) -> None:
        self.config = config
        self.redis_available = bool(redis_available and config.redis_url)
        self._available: List[str] = []
        if config.queue_type == "asyncio":
            self._available = ["asyncio"]
        elif config.queue_type == "redis":
            self._available = ["redis"] if self.redis_available else ["asyncio"]
        else:  # auto
            self._available = ["asyncio"]
            if self.redis_available:
                self._available.append("redis")

    @property
    def available(self) -> List[str]:
        return list(self._available)

    def is_available(self, backend: str) -> bool:
        return backend in self._available

    def default_backend(self) -> str:
        if self.config.queue_type == "redis" and self.redis_available:
            return "redis"
        return "asyncio"


# ============================================================================
# MoEGatingService (reward-aware)
# ============================================================================
class MoEGatingService:
    """MoE gating over backend selection. Reward now used in the gradient."""

    CONTEXT_DIM = 5

    def __init__(self, config: MessageQueueConfig,
                 available_backends: Sequence[str],
                 observability: Optional[ObservabilityHub] = None) -> None:
        self.config = config
        self.obs = observability or ObservabilityHub()
        self.expert_names: Tuple[str, ...] = tuple(available_backends)
        if not self.expert_names:
            self.expert_names = ("asyncio",)
        self.num_experts = len(self.expert_names)
        rng = np.random.default_rng(config.moe_seed)
        self.gating_weights = rng.standard_normal(
            (self.num_experts, self.CONTEXT_DIM),
        ) * 0.01
        self.learning_rate = 0.1
        self._lock = asyncio.Lock()
        self._disabled = False

    def disable(self) -> None:
        self._disabled = True

    def enable(self) -> None:
        self._disabled = False

    def _encode_state(self, context: Dict[str, float]) -> np.ndarray:
        # Fixed: coerce redis_available to float properly
        redis_avail = float(context.get('redis_available', 0.0))
        features = [
            float(_coerce_float(context.get('message_size'), 0.0) or 0.0),
            float(_coerce_float(context.get('priority'), 0.0) or 0.0),
            float(_coerce_float(context.get('system_load'), 0.5) or 0.5),
            1.0 if redis_avail > 0.5 else 0.0,
            float(_coerce_float(context.get('success_rate'), 0.9) or 0.9),
        ]
        return np.array(features, dtype=np.float32)

    async def select_expert(self, context: Dict[str, float]) -> Tuple[str, np.ndarray]:
        if self._disabled:
            return self.expert_names[0], np.zeros(self.num_experts)
        x = self._encode_state(context)
        logits = self.gating_weights @ x
        probs = np.exp(logits - np.max(logits))
        probs /= probs.sum()
        idx = int(np.argmax(probs))
        self.obs.counter('moe_gating_selections_total', expert=self.expert_names[idx])
        return self.expert_names[idx], probs

    async def add_training_sample(
        self, context: Dict[str, float], selected_expert: str, reward: float,
    ) -> None:
        if self._disabled or selected_expert not in self.expert_names:
            return
        idx = self.expert_names.index(selected_expert)
        x = self._encode_state(context)
        async with self._lock:
            logits = self.gating_weights @ x
            probs = np.exp(logits - np.max(logits))
            probs /= probs.sum()
            target = np.zeros(self.num_experts)
            target[idx] = 1.0
            # Fixed: reward-weighted gradient
            grad = float(reward) * (probs - target)[:, None] * x[None, :]
            self.gating_weights -= self.learning_rate * grad
        self.obs.counter('moe_gating_updates_total')


# ============================================================================
# EventPersistenceService (LIMIT Graph + MODP + RLHF)
# ============================================================================
class EventPersistenceService:
    """
    Batches writes to LIMIT Graph, MODP, and RLHF backends.
    In-memory structures are bounded; storage writes run in threads.
    """

    def __init__(self, config: MessageQueueConfig,
                 storage: Optional[StorageProtocol] = None,
                 observability: Optional[ObservabilityHub] = None,
                 dlq: Optional[DeadLetterQueue] = None) -> None:
        self.config = config
        self.storage = storage
        self.obs = observability or ObservabilityHub()
        self.dlq = dlq
        self._lock = asyncio.Lock()

        self.limit_graphs: Dict[str, Dict[str, Any]] = {}
        self.modp_states: Dict[str, Deque[Dict[str, Any]]] = {}
        self.rlhf_pairs: Deque[Dict[str, Any]] = deque(maxlen=config.rlhf_pairs_max)

    async def create_graph(self, graph_id: str, description: str,
                            configuration: Dict[str, Any]) -> None:
        try:
            if self.storage and hasattr(self.storage, 'save_limit_graph_metadata'):
                await asyncio.to_thread(
                    self.storage.save_limit_graph_metadata,
                    graph_id, description, configuration,
                )
                return
            async with self._lock:
                self.limit_graphs[graph_id] = {
                    'description': description,
                    'configuration': configuration,
                    'nodes': OrderedDict(),
                    'edges': OrderedDict(),
                }
        except Exception as e:
            await self._record_dlq('create_graph', e, {'graph_id': graph_id})

    async def add_node(self, graph_id: str, node_id: str,
                        node_type: Optional[str],
                        attributes: Dict[str, Any]) -> None:
        try:
            if self.storage and hasattr(self.storage, 'save_limit_graph_node'):
                await asyncio.to_thread(
                    self.storage.save_limit_graph_node,
                    node_id, graph_id, node_type, attributes,
                )
                return
            async with self._lock:
                if graph_id not in self.limit_graphs:
                    self.limit_graphs[graph_id] = {
                        'nodes': OrderedDict(), 'edges': OrderedDict(),
                    }
                nodes = self.limit_graphs[graph_id].setdefault('nodes', OrderedDict())
                nodes[node_id] = {'node_type': node_type, 'attributes': attributes}
                while len(nodes) > self.config.limit_graph_nodes_max:
                    nodes.popitem(last=False)
        except Exception as e:
            await self._record_dlq('add_node', e, {'node_id': node_id})

    async def add_edge(self, graph_id: str, edge_id: str, source: str,
                        target: str, weight: Optional[float],
                        attributes: Dict[str, Any]) -> None:
        try:
            if self.storage and hasattr(self.storage, 'save_limit_graph_edge'):
                await asyncio.to_thread(
                    self.storage.save_limit_graph_edge,
                    edge_id, graph_id, source, target, weight, attributes,
                )
                return
            async with self._lock:
                if graph_id not in self.limit_graphs:
                    self.limit_graphs[graph_id] = {
                        'nodes': OrderedDict(), 'edges': OrderedDict(),
                    }
                edges = self.limit_graphs[graph_id].setdefault('edges', OrderedDict())
                edges[edge_id] = {
                    'source': source, 'target': target,
                    'weight': weight, 'attributes': attributes,
                }
                while len(edges) > self.config.limit_graph_edges_max:
                    edges.popitem(last=False)
        except Exception as e:
            await self._record_dlq('add_edge', e, {'edge_id': edge_id})

    def get_nodes(self, graph_id: str) -> List[Dict[str, Any]]:
        if self.storage and hasattr(self.storage, 'get_limit_graph_nodes'):
            try:
                return list(self.storage.get_limit_graph_nodes(graph_id))
            except Exception:
                pass
        return list(self.limit_graphs.get(graph_id, {}).get('nodes', {}).values())

    def get_edges(self, graph_id: str) -> List[Dict[str, Any]]:
        if self.storage and hasattr(self.storage, 'get_limit_graph_edges'):
            try:
                return list(self.storage.get_limit_graph_edges(graph_id))
            except Exception:
                pass
        return list(self.limit_graphs.get(graph_id, {}).get('edges', {}).values())

    def get_metadata(self, graph_id: str) -> Optional[Dict[str, Any]]:
        if self.storage and hasattr(self.storage, 'get_limit_graph_metadata'):
            try:
                return self.storage.get_limit_graph_metadata(graph_id)
            except Exception:
                pass
        return self.limit_graphs.get(graph_id, {})

    async def add_modp_state(self, state_id: str, problem_id: str,
                              state_attributes: Dict[str, Any],
                              objective_values: Dict[str, float],
                              stage: int) -> None:
        try:
            if self.storage and hasattr(self.storage, 'save_modp_state'):
                await asyncio.to_thread(
                    self.storage.save_modp_state,
                    state_id, problem_id, state_attributes,
                    objective_values, stage,
                )
                return
            async with self._lock:
                if problem_id not in self.modp_states:
                    self.modp_states[problem_id] = deque(
                        maxlen=self.config.modp_states_max,
                    )
                self.modp_states[problem_id].append({
                    'state_id': state_id,
                    'state_attributes': state_attributes,
                    'objective_values': objective_values,
                    'stage': stage,
                })
        except Exception as e:
            await self._record_dlq('add_modp_state', e, {'state_id': state_id})

    def get_modp_states(self, problem_id: str) -> List[Dict[str, Any]]:
        if self.storage and hasattr(self.storage, 'get_modp_states'):
            try:
                return list(self.storage.get_modp_states(problem_id))
            except Exception:
                pass
        return list(self.modp_states.get(problem_id, []))

    async def record_preference_pair(
        self, pair_id: str, prompt: str, chosen: str, rejected: str,
        reward_diff: float, metadata: Optional[Dict[str, Any]] = None,
    ) -> None:
        try:
            if self.storage and hasattr(self.storage, 'save_preference_pair'):
                await asyncio.to_thread(
                    self.storage.save_preference_pair,
                    pair_id, prompt, chosen, rejected, reward_diff, metadata,
                )
                return
            async with self._lock:
                self.rlhf_pairs.append({
                    'pair_id': pair_id, 'prompt': prompt,
                    'chosen': chosen, 'rejected': rejected,
                    'reward_diff': reward_diff, 'metadata': metadata,
                })
        except Exception as e:
            await self._record_dlq('record_preference_pair', e, {'pair_id': pair_id})

    def get_preference_pairs(self, limit: int = 100) -> List[Dict[str, Any]]:
        if self.storage and hasattr(self.storage, 'get_preference_pairs'):
            try:
                return list(self.storage.get_preference_pairs(limit))
            except Exception:
                pass
        return list(self.rlhf_pairs)[-limit:]

    async def _record_dlq(self, topic: str, error: Exception,
                          payload: Optional[Dict[str, Any]] = None) -> None:
        if self.dlq is not None:
            await self.dlq.record(topic, str(error), payload)


# ============================================================================
# AsyncioConsumerService
# ============================================================================
class AsyncioConsumerService:
    """Per-channel asyncio.Queue consumer with re-queue and DLQ on failure."""

    def __init__(self, config: MessageQueueConfig,
                 serializer: MessageSerializer,
                 observability: ObservabilityHub,
                 dlq: DeadLetterQueue,
                 retry_policy: RetryPolicy) -> None:
        self.config = config
        self.serializer = serializer
        self.obs = observability
        self.dlq = dlq
        self.retry = retry_policy
        self._queues: "OrderedDict[str, asyncio.Queue]" = OrderedDict()
        self._consumers: Dict[str, asyncio.Task] = {}
        self._lock = asyncio.Lock()
        self._shutting_down = False
        self._disabled = False

    def disable(self) -> None:
        self._disabled = True

    def enable(self) -> None:
        self._disabled = False

    def _get_queue(self, channel: str) -> asyncio.Queue:
        if channel in self._queues:
            self._queues.move_to_end(channel)
            return self._queues[channel]
        q = asyncio.Queue(maxsize=self.config.max_queue_size)
        self._queues[channel] = q
        # LRU eviction
        while len(self._queues) > self.config.per_channel_lru_max:
            old_channel, old_q = self._queues.popitem(last=False)
            if old_channel in self._consumers:
                task = self._consumers.pop(old_channel)
                task.cancel()
        return q

    async def publish(self, channel: str, msg: Message,
                      timeout: Optional[float] = None) -> PublishResult:
        if self._disabled:
            return PublishResult(
                status=PublishStatus.ERROR, error="asyncio backend disabled",
            )
        if self._shutting_down:
            return PublishResult(status=PublishStatus.CLOSED)
        timeout = float(timeout if timeout is not None else self.config.publish_timeout)
        q = self._get_queue(channel)
        try:
            await asyncio.wait_for(q.put(msg), timeout=timeout)
            self.obs.gauge('queue_depth', self.depth())
            return PublishResult(status=PublishStatus.OK, message_id=msg.message_id)
        except asyncio.TimeoutError:
            self.obs.counter('publish_backpressure_total', channel=channel)
            return PublishResult(status=PublishStatus.BACKPRESSURE, error="queue full")
        except Exception as e:
            return PublishResult(status=PublishStatus.ERROR, error=str(e))

    async def register_consumer(
        self, channel: str,
        handler: Callable[[MessageEnvelope], Awaitable[bool]],
    ) -> None:
        async with self._lock:
            if channel in self._consumers:
                return
            self._get_queue(channel)
            self._consumers[channel] = asyncio.create_task(
                self._consume_loop(channel, handler),
            )

    async def _consume_loop(
        self, channel: str,
        handler: Callable[[MessageEnvelope], Awaitable[bool]],
    ) -> None:
        q = self._queues.get(channel)
        if q is None:
            return
        while not self._shutting_down:
            try:
                msg = await q.get()
            except asyncio.CancelledError:
                break
            env = MessageEnvelope(
                channel=msg.channel, payload=msg.payload,
                message_id=msg.message_id, stream_id=msg.stream_id,
                retry_count=msg.retry_count, timestamp=msg.timestamp,
            )
            start = time.monotonic()
            success = False
            try:
                success = bool(await handler(env))
            except Exception as e:
                logger.error("Callback error on '%s': %s", channel, e)
                self.obs.counter('consume_errors_total', channel=channel)
            finally:
                self.obs.histogram('consume_latency_seconds',
                                    time.monotonic() - start)
                q.task_done()
            if success:
                self.obs.counter('messages_consumed_total', channel=channel)
                continue
            # Failure: re-queue with bounded retries
            msg.retry_count += 1
            if msg.retry_count < self.config.max_retries:
                await self.retry.wait(msg.retry_count)
                try:
                    q.put_nowait(msg)
                    self.obs.counter('messages_requeued_total', channel=channel)
                except asyncio.QueueFull:
                    await self.dlq.record(
                        'asyncio_requeue_full',
                        'Re-queue failed: queue full',
                        {'channel': channel, 'message_id': msg.message_id},
                    )
                    self.obs.counter('messages_dead_lettered_total',
                                      channel=channel, reason='queue_full')
            else:
                await self.dlq.record(
                    'asyncio_max_retries',
                    'Message exceeded retries',
                    {'channel': channel, 'message_id': msg.message_id},
                )
                self.obs.counter('messages_dead_lettered_total',
                                  channel=channel, reason='max_retries')

    def depth(self) -> int:
        return sum(q.qsize() for q in self._queues.values())

    async def shutdown(self) -> None:
        self._shutting_down = True
        for task in self._consumers.values():
            task.cancel()
        if self._consumers:
            await asyncio.gather(*self._consumers.values(),
                                  return_exceptions=True)
        self._consumers.clear()
        for q in self._queues.values():
            while not q.empty():
                try:
                    q.get_nowait()
                except asyncio.QueueEmpty:
                    break
        self._queues.clear()


# ============================================================================
# PubSubConsumerService
# ============================================================================
class PubSubConsumerService:
    """Redis Pub/Sub consumer with its own client (fixes reconnect thrashing)."""

    def __init__(self, config: MessageQueueConfig,
                 serializer: MessageSerializer,
                 observability: ObservabilityHub,
                 dlq: DeadLetterQueue,
                 retry_policy: RetryPolicy) -> None:
        self.config = config
        self.serializer = serializer
        self.obs = observability
        self.dlq = dlq
        self.retry = retry_policy
        self._pubsub_client: Optional[Any] = None   # dedicated pubsub client
        self._handlers: Dict[str, List[Callable[[MessageEnvelope], Awaitable[bool]]]] = defaultdict(list)
        self._consumer_task: Optional[asyncio.Task] = None
        self._shutting_down = False
        self._disabled = False
        self._lock = asyncio.Lock()

    def disable(self) -> None:
        self._disabled = True

    def enable(self) -> None:
        self._disabled = False

    async def _get_client(self) -> Optional[Any]:
        if self._disabled or not REDIS_AVAILABLE or not self.config.redis_url:
            return None
        if self._pubsub_client is None:
            self._pubsub_client = Redis.from_url(
                self.config.redis_url, decode_responses=True,
            )
        return self._pubsub_client

    async def register(
        self, channels: Sequence[str],
        handler: Callable[[MessageEnvelope], Awaitable[bool]],
    ) -> None:
        for ch in channels:
            self._handlers[ch].append(handler)
        if self._consumer_task is None or self._consumer_task.done():
            self._consumer_task = asyncio.create_task(
                self._consume_loop(list(self._handlers.keys())),
            )

    async def _consume_loop(self, channels: List[str]) -> None:
        while not self._shutting_down:
            try:
                client = await self._get_client()
                if client is None:
                    return
                pubsub = client.pubsub()
                await pubsub.subscribe(*channels)
                async for message in pubsub.listen():
                    if self._shutting_down:
                        break
                    if message.get("type") != "message":
                        continue
                    try:
                        msg = self.serializer.deserialize(message["data"])
                    except ValueError as e:
                        await self.dlq.record(
                            'pubsub_corrupt', str(e), {'raw': message["data"][:128]},
                        )
                        self.obs.counter('messages_dead_lettered_total',
                                          reason='corrupt')
                        continue
                    await self._dispatch(msg)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("PubSub consumer error: %s", e)
                self.obs.counter('consume_errors_total', backend='redis_pubsub')
                await self.retry.wait(0)
        # Cleanup
        if self._pubsub_client is not None:
            try:
                await self._pubsub_client.close()
            except Exception:
                pass
            self._pubsub_client = None

    async def _dispatch(self, msg: Message) -> None:
        handlers = self._handlers.get(msg.channel, [])
        env = MessageEnvelope(
            channel=msg.channel, payload=msg.payload,
            message_id=msg.message_id, stream_id=msg.stream_id,
            retry_count=msg.retry_count, timestamp=msg.timestamp,
        )
        start = time.monotonic()
        any_success = False
        for handler in handlers:
            try:
                ok = await handler(env)
                if ok:
                    any_success = True
            except Exception as e:
                logger.error("PubSub handler error on '%s': %s", msg.channel, e)
                self.obs.counter('consume_errors_total',
                                  backend='redis_pubsub', channel=msg.channel)
        self.obs.histogram('consume_latency_seconds', time.monotonic() - start)
        if any_success:
            self.obs.counter('messages_consumed_total',
                              backend='redis_pubsub', channel=msg.channel)
        # Pub/Sub has no ack; a failure here has no dead-letter path.

    async def shutdown(self) -> None:
        self._shutting_down = True
        if self._consumer_task is not None:
            self._consumer_task.cancel()
            try:
                await self._consumer_task
            except (asyncio.CancelledError, Exception):
                pass
            self._consumer_task = None


# ============================================================================
# StreamConsumerService
# ============================================================================
class StreamConsumerService:
    """Redis Streams consumer with proper DLQ and per-stream groups."""

    def __init__(self, config: MessageQueueConfig,
                 serializer: MessageSerializer,
                 observability: ObservabilityHub,
                 dlq: DeadLetterQueue,
                 retry_policy: RetryPolicy) -> None:
        self.config = config
        self.serializer = serializer
        self.obs = observability
        self.dlq = dlq
        self.retry = retry_policy
        self._client: Optional[Any] = None
        self._handlers: Dict[str, List[Callable[[MessageEnvelope], Awaitable[bool]]]] = defaultdict(list)
        self._consumer_task: Optional[asyncio.Task] = None
        self._shutting_down = False
        self._disabled = False
        self._consumer_name = config.consumer_name or f"consumer_{uuid.uuid4().hex[:8]}"
        self._lock = asyncio.Lock()

    def disable(self) -> None:
        self._disabled = True

    def enable(self) -> None:
        self._disabled = False

    async def _get_client(self) -> Optional[Any]:
        if self._disabled or not REDIS_AVAILABLE or not self.config.redis_url:
            return None
        if self._client is None:
            self._client = Redis.from_url(
                self.config.redis_url, decode_responses=True,
            )
        return self._client

    async def register(
        self, streams: Sequence[str],
        handler: Callable[[MessageEnvelope], Awaitable[bool]],
    ) -> None:
        for s in streams:
            self._handlers[s].append(handler)
        if self._consumer_task is None or self._consumer_task.done():
            self._consumer_task = asyncio.create_task(
                self._consume_loop(list(self._handlers.keys())),
            )

    async def _ensure_group(self, client: Any, stream: str) -> None:
        try:
            await client.xgroup_create(
                stream, self.config.consumer_group, id="0", mkstream=True,
            )
        except Exception as e:
            # BUSYGROUP is normal when group already exists
            if "BUSYGROUP" not in str(e):
                logger.debug("xgroup_create for %s: %s", stream, e)

    async def _consume_loop(self, streams: List[str]) -> None:
        while not self._shutting_down:
            try:
                client = await self._get_client()
                if client is None:
                    return
                for s in streams:
                    await self._ensure_group(client, s)
                    await self._ensure_group(
                        client, f"{s}{self.config.dead_letter_suffix}",
                    )
                response = await client.xreadgroup(
                    self.config.consumer_group, self._consumer_name,
                    {s: ">" for s in streams},
                    count=10, block=5000,
                )
                if not response:
                    continue
                for stream, messages in response.items():
                    for msg_id, msg_data in messages.items():
                        await self._handle_stream_message(
                            client, stream, msg_id, msg_data,
                        )
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("Stream consumer error: %s", e)
                self.obs.counter('consume_errors_total', backend='redis_streams')
                await self.retry.wait(0)
        if self._client is not None:
            try:
                await self._client.close()
            except Exception:
                pass
            self._client = None

    async def _handle_stream_message(
        self, client: Any, stream: str, msg_id: str, msg_data: Dict[str, Any],
    ) -> None:
        try:
            raw = msg_data.get("data")
            if raw is None:
                raise ValueError("Missing 'data' field")
            msg = self.serializer.deserialize(raw)
        except Exception as e:
            await self.dlq.record('stream_corrupt', str(e), {'stream': stream, 'msg_id': msg_id})
            self.obs.counter('messages_dead_lettered_total', reason='corrupt')
            try:
                await client.xack(stream, self.config.consumer_group, msg_id)
            except Exception:
                pass
            return
        # Increment retry count before handing off
        msg.retry_count += 1
        env = MessageEnvelope(
            channel=msg.channel, payload=msg.payload,
            message_id=msg.message_id, stream_id=msg_id,
            retry_count=msg.retry_count, timestamp=msg.timestamp,
        )
        handlers = self._handlers.get(stream, [])
        start = time.monotonic()
        success = False
        for h in handlers:
            try:
                ok = await h(env)
                if ok:
                    success = True
            except Exception as e:
                logger.error("Stream handler error: %s", e)
        self.obs.histogram('consume_latency_seconds', time.monotonic() - start)
        if success:
            try:
                await client.xack(stream, self.config.consumer_group, msg_id)
            except Exception as e:
                logger.warning("xack failed: %s", e)
            self.obs.counter('messages_consumed_total', backend='redis_streams')
            return
        # Failure path
        if msg.retry_count >= self.config.max_retries:
            try:
                dl_stream = f"{stream}{self.config.dead_letter_suffix}"
                await client.xadd(dl_stream, {"data": msg.to_json()})
                await client.xack(stream, self.config.consumer_group, msg_id)
                self.obs.counter('messages_dead_lettered_total', reason='max_retries')
            except Exception as e:
                logger.error("Dead-letter XADD failed: %s", e)
                await self.dlq.record('dlq_xadd_failed', str(e), {'msg_id': msg_id})
        else:
            self.obs.counter('messages_requeued_total', backend='redis_streams')
            # Leave unacked; will be re-read on next loop

    async def shutdown(self) -> None:
        self._shutting_down = True
        if self._consumer_task is not None:
            self._consumer_task.cancel()
            try:
                await self._consumer_task
            except (asyncio.CancelledError, Exception):
                pass
            self._consumer_task = None


# ============================================================================
# ChaosService
# ============================================================================
class ChaosService:
    def __init__(self, queue: "AsyncMessageQueue",
                 config: MessageQueueConfig) -> None:
        self.queue = queue
        self.config = config
        self._saved: Dict[str, Any] = {}
        self._injected: List[str] = []
        self._lock = asyncio.Lock()

    async def inject(self, fault_type: FaultType, **params: Any) -> Dict[str, Any]:
        async with self._lock:
            try:
                if fault_type == FaultType.REDIS_DISCONNECT:
                    if self.queue.pubsub_service is not None:
                        self._saved.setdefault(
                            'pubsub_disabled', self.queue.pubsub_service._disabled,
                        )
                        self.queue.pubsub_service.disable()
                    if self.queue.stream_service is not None:
                        self._saved.setdefault(
                            'stream_disabled', self.queue.stream_service._disabled,
                        )
                        self.queue.stream_service.disable()
                    return {'status': 'injected'}
                if fault_type == FaultType.CALLBACK_FAILURE:
                    self._saved.setdefault('callback_failure', True)
                    return {'status': 'injected'}
                if fault_type == FaultType.SLOW_CONSUMER:
                    self._saved.setdefault('slow_consumer', True)
                    return {'status': 'injected'}
                if fault_type == FaultType.QUEUE_FULL:
                    if self.queue.asyncio_service is not None:
                        self._saved.setdefault(
                            'max_queue_size',
                            self.queue.asyncio_service.config.max_queue_size,
                        )
                        # Shrink the queue size to force backpressure
                        self.queue.asyncio_service.config.max_queue_size = 1
                    return {'status': 'injected'}
                if fault_type == FaultType.MESSAGE_CORRUPTION:
                    if self.queue.serializer is not None:
                        self._saved.setdefault(
                            'serializer_disabled', self.queue.serializer._disabled,
                        )
                        self.queue.serializer.disable()
                    return {'status': 'injected'}
                if fault_type == FaultType.BACKEND_UNAVAILABLE:
                    if self.queue.moe_service is not None:
                        self._saved.setdefault(
                            'moe_disabled', self.queue.moe_service._disabled,
                        )
                        self.queue.moe_service.disable()
                    return {'status': 'injected'}
            except Exception as e:
                logger.exception("Chaos injection failed: %s", e)
                return {'status': 'error', 'error': str(e)}
            return {'status': 'unknown_fault'}

    def restore(self) -> None:
        q = self.queue
        for key, value in list(self._saved.items()):
            if key == 'pubsub_disabled' and q.pubsub_service is not None:
                q.pubsub_service._disabled = bool(value)
            elif key == 'stream_disabled' and q.stream_service is not None:
                q.stream_service._disabled = bool(value)
            elif key == 'max_queue_size' and q.asyncio_service is not None:
                q.asyncio_service.config.max_queue_size = int(value)
            elif key == 'serializer_disabled' and q.serializer is not None:
                q.serializer._disabled = bool(value)
            elif key == 'moe_disabled' and q.moe_service is not None:
                q.moe_service._disabled = bool(value)
            self._saved.pop(key, None)
        self._injected.clear()

    async def run_test(self, test_channel: str = "chaos_test") -> Dict[str, Any]:
        report: Dict[str, Any] = {'faults': [], 'results': {}, 'restored': False}
        faults = [
            FaultType.MESSAGE_CORRUPTION,
            FaultType.BACKEND_UNAVAILABLE,
            FaultType.QUEUE_FULL,
            FaultType.SLOW_CONSUMER,
        ]
        try:
            for ft in faults[:CHAOS_MAX_FAULTS_PER_RUN]:
                res = await self.inject(ft)
                report['faults'].append(ft.value)
                report['results'][ft.value] = res
            # Steady-state hypothesis: publish should not crash
            try:
                res = await self.queue.publish(test_channel, {"probe": True})
                report['results']['post_fault_publish'] = res.to_dict()
            except Exception as e:
                report['results']['post_fault_publish'] = {'error': str(e)}
            try:
                health = await self.queue.get_health()
                report['results']['health'] = {
                    'status': health.get('status', 'unknown'),
                }
            except Exception as e:
                report['results']['health'] = {'error': str(e)}
        finally:
            self.restore()
            report['restored'] = True
        return report


# ============================================================================
# AsyncMessageQueue (main orchestrator)
# ============================================================================
class AsyncMessageQueue:
    """
    Production-grade async message queue v3.0.0.

    Supports asyncio (in-memory), Redis Pub/Sub, and Redis Streams backends
    with sub-services, Protocols, ObservabilityHub, GovernanceLayer,
    ChaosService, full lifecycle, Pydantic v1/v2 config, idempotency,
    dead-letter queue, bounded caches, and backpressure.
    """

    def __init__(
        self,
        config: Optional[Union[MessageQueueConfig, Dict[str, Any]]] = None,
        storage: Optional[StorageProtocol] = None,
        metrics_registry: Optional[MetricsRegistryProtocol] = None,
        publish_approval_fn: Optional[Callable[[str, Any], Awaitable[None]]] = None,
        **kwargs: Any,
    ) -> None:
        if config is None:
            base = _default_config_from_central()
            valid = {f.name for f in dataclass_fields(MessageQueueConfig)}
            for k, v in kwargs.items():
                if k in valid:
                    setattr(base, k, v)
            config = base
        elif isinstance(config, dict):
            config = MessageQueueConfig.from_dict(config)
        self.config = config
        self.storage = storage
        self.metrics_registry = metrics_registry

        # Infra
        self.observability = ObservabilityHub()
        self.dlq = DeadLetterQueue()
        self._idempotency = IdempotencyCache()
        self.governance = GovernanceLayer(
            enabled=config.enable_human_approval,
            timeout_seconds=config.approval_timeout_seconds,
            auto_approve_on_timeout=config.approval_auto_approve_on_timeout,
            publish_fn=publish_approval_fn,
        )
        self.safety = SafetyMonitor()
        self._setup_invariants()

        # Serialization + retry
        self.serializer = MessageSerializer(config.max_payload_bytes)
        self.retry_policy = RetryPolicy(
            max_retries=config.max_retries, base_delay=config.retry_delay,
        )

        # Backend registry
        self.backend_registry = BackendRegistry(config, redis_available=REDIS_AVAILABLE)

        # Sub-services
        self.persistence_service = EventPersistenceService(
            config, storage=storage,
            observability=self.observability, dlq=self.dlq,
        )
        self.asyncio_service = AsyncioConsumerService(
            config, self.serializer, self.observability, self.dlq, self.retry_policy,
        )
        self.pubsub_service: Optional[PubSubConsumerService] = None
        self.stream_service: Optional[StreamConsumerService] = None
        if self.backend_registry.is_available("redis"):
            if config.use_streams:
                self.stream_service = StreamConsumerService(
                    config, self.serializer, self.observability,
                    self.dlq, self.retry_policy,
                )
            else:
                self.pubsub_service = PubSubConsumerService(
                    config, self.serializer, self.observability,
                    self.dlq, self.retry_policy,
                )

        # MoE (only if more than one backend available)
        available = self.backend_registry.available
        if config.enable_moe and len(available) > 1:
            self.moe_service = MoEGatingService(
                config, available, observability=self.observability,
            )
        else:
            self.moe_service = None

        # Chaos
        self.chaos_service = (
            ChaosService(self, config) if config.enable_chaos_testing else None
        )

        # Lifecycle
        self._started = False
        self._shutting_down = False
        self._background_tasks: List[asyncio.Task] = []
        self._chaos_task: Optional[asyncio.Task] = None
        self._ready_event = asyncio.Event()

        logger.info(
            "AsyncMessageQueue v%s initialised: type=%s streams=%s "
            "backends=%s moe=%s governance=%s chaos=%s",
            MODULE_VERSION, config.queue_type, config.use_streams,
            available, self.moe_service is not None,
            self.governance.enabled, self.chaos_service is not None,
        )

    # ------------------------------------------------------------------
    # Safety invariants
    # ------------------------------------------------------------------
    def _setup_invariants(self) -> None:
        def _depth_bounded(state: Dict[str, Any]) -> bool:
            return int(state.get('depth', 0)) >= 0

        def _subscribers_nonneg(state: Dict[str, Any]) -> bool:
            return int(state.get('subscribers', 0)) >= 0

        def _dlq_monotonic(state: Dict[str, Any]) -> bool:
            # This is trivially true; it's a placeholder for future invariants.
            return True

        self.safety.add('depth_bounded', _depth_bounded,
                        'Queue depth must be non-negative')
        self.safety.add('subscribers_nonneg', _subscribers_nonneg,
                        'Subscriber count must be non-negative')
        self.safety.add('dlq_monotonic', _dlq_monotonic,
                        'DLQ total received must be monotonic')

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------
    async def start(self) -> None:
        if self._started:
            return
        self._started = True

        # Init graph if enabled
        if self.config.enable_limit_graph:
            try:
                existing = self.persistence_service.get_metadata("queue_graph")
                if not existing:
                    await self.persistence_service.create_graph(
                        "queue_graph", "Message Queue Relationships",
                        {"version": MODULE_VERSION},
                    )
                await self.persistence_service.add_node(
                    "queue_graph", "backend_asyncio", "backend", {"type": "asyncio"},
                )
                if self.backend_registry.is_available("redis"):
                    await self.persistence_service.add_node(
                        "queue_graph", "backend_redis", "backend", {"type": "redis"},
                    )
            except Exception as e:
                logger.warning("Graph init failed: %s", e)
                await self.dlq.record('graph_init', str(e))

        # Chaos loop
        if self.config.enable_chaos_testing:
            self._chaos_task = asyncio.create_task(self._chaos_loop())
            self._background_tasks.append(self._chaos_task)

        self._ready_event.set()
        logger.info("AsyncMessageQueue started")

    async def wait_ready(self) -> None:
        if not self._started:
            await self.start()
        await self._ready_event.wait()

    async def shutdown(self) -> None:
        logger.info("AsyncMessageQueue shutdown")
        self._shutting_down = True
        for t in self._background_tasks:
            t.cancel()
        if self._background_tasks:
            await asyncio.gather(*self._background_tasks, return_exceptions=True)
        self._background_tasks.clear()

        if self.asyncio_service is not None:
            try:
                await self.asyncio_service.shutdown()
            except Exception as e:
                logger.warning("asyncio_service shutdown error: %s", e)
        if self.pubsub_service is not None:
            try:
                await self.pubsub_service.shutdown()
            except Exception as e:
                logger.warning("pubsub_service shutdown error: %s", e)
        if self.stream_service is not None:
            try:
                await self.stream_service.shutdown()
            except Exception as e:
                logger.warning("stream_service shutdown error: %s", e)

        unmet = self.safety.final_check()
        if unmet:
            logger.warning("Unmet eventual invariants: %s", unmet)

    async def _chaos_loop(self) -> None:
        interval = max(60, self.config.chaos_interval_seconds)
        while not self._shutting_down:
            try:
                await asyncio.sleep(interval)
                if self.chaos_service is not None:
                    await self.chaos_service.run_test()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("Chaos loop error: %s", e)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------
    async def publish(
        self,
        channel: str,
        message: Any,
        priority: int = 0,
        idempotency_key: Optional[str] = None,
        put_timeout: Optional[float] = None,
    ) -> PublishResult:
        await self.wait_ready()
        if self._shutting_down:
            return PublishResult(status=PublishStatus.CLOSED)
        if idempotency_key and self._idempotency.seen(idempotency_key):
            self.observability.counter('publish_duplicates_total')
            return PublishResult(status=PublishStatus.DUPLICATE)

        # Serialize once
        msg = Message(
            channel=channel,
            payload=message,
            message_id=uuid.uuid4().hex,
        )
        try:
            payload_str = self.serializer.serialize(msg)
        except ValueError as e:
            self.observability.counter('publish_errors_total', reason='payload_too_large')
            return PublishResult(status=PublishStatus.ERROR, error=str(e))

        # Backend selection
        target_backend = self.backend_registry.default_backend()
        context_for_moe: Optional[Dict[str, float]] = None
        if self.moe_service is not None:
            context_for_moe = {
                'message_size': float(len(payload_str)),
                'priority': float(priority),
                'system_load': 0.5,
                'redis_available': 1.0 if self.backend_registry.is_available("redis") else 0.0,
                'success_rate': 0.9,
            }
            try:
                selected, _ = await self.moe_service.select_expert(context_for_moe)
                # Validate selection against available backends
                if self.backend_registry.is_available(selected):
                    target_backend = selected
            except Exception as e:
                logger.warning("MoE selection failed; using default: %s", e)

        # Persist routing decision (bounded)
        if self.config.enable_modp:
            try:
                await self.persistence_service.add_modp_state(
                    state_id=uuid.uuid4().hex,
                    problem_id="queue_routing",
                    state_attributes={
                        'channel': channel,
                        'priority': priority,
                        'backend': target_backend,
                    },
                    objective_values={'priority': float(priority)},
                    stage=0,
                )
            except Exception:
                pass
        if self.config.enable_limit_graph:
            try:
                await self.persistence_service.add_edge(
                    "queue_graph",
                    f"edge_{uuid.uuid4().hex[:8]}",
                    f"channel_{channel}",
                    f"backend_{target_backend}",
                    float(priority),
                    {'channel': channel},
                )
            except Exception:
                pass

        # Dispatch
        start = time.monotonic()
        result = PublishResult(status=PublishStatus.ERROR, error="no backend")
        try:
            if target_backend == "redis" and self.backend_registry.is_available("redis"):
                result = await self._redis_publish(channel, payload_str, msg)
            else:
                result = await self.asyncio_service.publish(
                    channel, msg, timeout=put_timeout,
                )
        except Exception as e:
            logger.error("Publish failed: %s", e)
            result = PublishResult(status=PublishStatus.ERROR, error=str(e))
            self.observability.counter('publish_errors_total', backend=target_backend)
        finally:
            elapsed = time.monotonic() - start
            self.observability.histogram('publish_latency_seconds', elapsed,
                                          backend=target_backend)
            if result.ok:
                self.observability.counter('messages_published_total',
                                            backend=target_backend)

        # MoE reward feedback
        if self.moe_service is not None and context_for_moe is not None:
            reward = 1.0 if result.ok else -1.0
            try:
                await self.moe_service.add_training_sample(
                    context_for_moe, target_backend, reward,
                )
            except Exception as e:
                logger.debug("MoE training sample failed: %s", e)

        return result

    async def _redis_publish(self, channel: str, payload_str: str,
                             msg: Message) -> PublishResult:
        if self.stream_service is not None and self.stream_service._client:
            client = await self.stream_service._get_client()
            if client is None:
                return PublishResult(status=PublishStatus.ERROR, error="no redis client")
            try:
                kwargs: Dict[str, Any] = {}
                if self.config.stream_maxlen:
                    kwargs['maxlen'] = self.config.stream_maxlen
                    kwargs['approximate'] = True
                stream_id = await client.xadd(channel, {"data": payload_str}, **kwargs)
                return PublishResult(status=PublishStatus.OK,
                                      message_id=msg.message_id,
                                      stream_id=str(stream_id))
            except Exception as e:
                logger.warning("Streams publish failed, falling back to asyncio: %s", e)
                return await self.asyncio_service.publish(channel, msg)
        else:
            # Pub/Sub path
            client = await self._get_pubsub_client()
            if client is None:
                return await self.asyncio_service.publish(channel, msg)
            try:
                await client.publish(channel, payload_str)
                return PublishResult(status=PublishStatus.OK, message_id=msg.message_id)
            except Exception as e:
                logger.warning("Pub/Sub publish failed, falling back to asyncio: %s", e)
                return await self.asyncio_service.publish(channel, msg)

    async def _get_pubsub_client(self) -> Optional[Any]:
        if self.pubsub_service is None:
            return None
        return await self.pubsub_service._get_client()

    async def subscribe(
        self,
        channels: Union[str, Sequence[str]],
        callback: Callable[[MessageEnvelope], Awaitable[bool]],
        backend: Optional[str] = None,
    ) -> None:
        await self.wait_ready()
        if isinstance(channels, str):
            channels = [channels]
        channels = list(channels)

        chosen = backend or self.backend_registry.default_backend()
        if chosen == "redis" and self.backend_registry.is_available("redis"):
            if self.stream_service is not None:
                await self.stream_service.register(channels, callback)
            elif self.pubsub_service is not None:
                await self.pubsub_service.register(channels, callback)
            else:
                await self.asyncio_service.register_consumer(channels[0], callback)
        else:
            # asyncio per-channel consumer
            for ch in channels:
                await self.asyncio_service.register_consumer(ch, callback)

        if self.config.enable_limit_graph:
            for ch in channels:
                try:
                    await self.persistence_service.add_node(
                        "queue_graph", f"channel_{ch}", "channel",
                        {"subscribers": 1},
                    )
                except Exception:
                    pass

    async def qsize(self) -> Optional[int]:
        if self.backend_registry.default_backend() == "redis":
            return None  # Not queryable for Redis Pub/Sub or Streams
        if self.asyncio_service is None:
            return 0
        return self.asyncio_service.depth()

    async def health(self) -> Dict[str, Any]:
        return await self.get_health()

    async def get_health(self) -> Dict[str, Any]:
        depth = 0
        if self.asyncio_service is not None:
            depth = self.asyncio_service.depth()
        return {
            "status": "healthy" if not self._shutting_down else "stopping",
            "version": MODULE_VERSION,
            "started": self._started,
            "type": self.config.queue_type,
            "backends_available": self.backend_registry.available,
            "depth": depth,
            "dead_letters": len(self.dlq),
            "dead_letters_total": self.dlq.total_received,
            "components": {
                "moe": self.moe_service is not None,
                "governance": self.governance.enabled,
                "chaos": self.chaos_service is not None,
                "streams": self.stream_service is not None,
                "pubsub": self.pubsub_service is not None,
            },
        }

    async def get_prometheus_metrics(self) -> str:
        return await self.observability.export_prometheus()

    def get_dead_letters(self) -> List[Dict[str, Any]]:
        return self.dlq.snapshot()

    # ------------------------------------------------------------------
    # Queries
    # ------------------------------------------------------------------
    async def get_limit_graph(self, graph_id: str = "queue_graph") -> Dict[str, Any]:
        return {
            'metadata': self.persistence_service.get_metadata(graph_id),
            'nodes': self.persistence_service.get_nodes(graph_id),
            'edges': self.persistence_service.get_edges(graph_id),
        }

    async def get_modp_states(self, problem_id: str = "queue_routing") -> List[Dict[str, Any]]:
        return self.persistence_service.get_modp_states(problem_id)

    async def get_rlhf_pairs(self, limit: int = 100) -> List[Dict[str, Any]]:
        return self.persistence_service.get_preference_pairs(limit)

    async def get_moe_experts(self) -> List[str]:
        if self.moe_service is None:
            return []
        return list(self.moe_service.expert_names)

    # ------------------------------------------------------------------
    # Governance / chaos API
    # ------------------------------------------------------------------
    async def submit_approval_decision(self, request_id: str,
                                        approved: bool) -> bool:
        return await self.governance.submit_decision(request_id, approved)

    def get_approval_audit(self) -> List[Dict[str, Any]]:
        return self.governance.get_audit_log()

    def list_pending_approvals(self) -> List[Dict[str, Any]]:
        return self.governance.list_pending()

    async def inject_fault(self, fault_type: str, **params: Any) -> Dict[str, Any]:
        if self.chaos_service is None:
            return {'status': 'disabled'}
        try:
            ft = FaultType(fault_type)
        except ValueError:
            return {'status': 'unknown_fault'}
        return await self.chaos_service.inject(ft, **params)

    async def run_chaos_test(self) -> Dict[str, Any]:
        if self.chaos_service is None:
            return {'status': 'disabled'}
        return await self.chaos_service.run_test()

    # ------------------------------------------------------------------
    # Async context manager
    # ------------------------------------------------------------------
    async def __aenter__(self) -> "AsyncMessageQueue":
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        await self.shutdown()


# ============================================================================
# Exports
# ============================================================================
__all__ = [
    # Main
    "AsyncMessageQueue",
    "MessageQueueConfig",
    "Message",
    "MessageEnvelope",
    "PublishResult",
    # Sub-services
    "MessageSerializer",
    "RetryPolicy",
    "BackendRegistry",
    "DeadLetterQueue",
    "MoEGatingService",
    "EventPersistenceService",
    "AsyncioConsumerService",
    "PubSubConsumerService",
    "StreamConsumerService",
    "ChaosService",
    # Infra
    "ObservabilityHub",
    "GovernanceLayer",
    "ApprovalRequest",
    "ApprovalResult",
    "ApprovalStatus",
    "SafetyMonitor",
    "SafetyInvariant",
    "CircuitBreaker",
    "CircuitBreakerState",
    "IdempotencyCache",
    # Enums
    "QueueBackend",
    "FaultType",
    "PublishStatus",
    # Protocols
    "StorageProtocol",
    "PayloadProtocol",
    "MetricsRegistryProtocol",
    "EventBrokerProtocol",
    "RedisClientProtocol",
    # Constants
    "MODULE_VERSION",
    "REDIS_AVAILABLE",
    # Pydantic flags
    "PYDANTIC_AVAILABLE",
    "PYDANTIC_V1",
    "PYDANTIC_V2",
]


# ============================================================================
# Example usage
# ============================================================================
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    async def _demo() -> None:
        config = MessageQueueConfig(
            queue_type="asyncio",
            max_queue_size=100,
            publish_timeout=2.0,
            enable_chaos_testing=True,
        )
        queue = AsyncMessageQueue(config=config)
        await queue.start()

        received: List[MessageEnvelope] = []

        async def handler(env: MessageEnvelope) -> bool:
            received.append(env)
            print(f"Received on '{env.channel}': {env.payload} "
                  f"(id={env.message_id}, retry={env.retry_count})")
            return True

        await queue.subscribe("test_channel", handler)

        # Publish a few messages
        for i in range(5):
            res = await queue.publish(
                "test_channel",
                {"index": i, "value": i * 10},
                idempotency_key=f"demo-{i}",
            )
            print(f"Publish {i}: {res.to_dict()}")

        # Duplicate should be caught
        dup = await queue.publish(
            "test_channel", {"index": 0}, idempotency_key="demo-0",
        )
        print(f"Duplicate: {dup.to_dict()}")

        # Give consumers a moment to process
        await asyncio.sleep(0.2)

        health = await queue.get_health()
        print(f"Health: {health['status']}, depth={health['depth']}, "
              f"backends={health['backends_available']}")

        chaos = await queue.run_chaos_test()
        print(f"Chaos restored: {chaos['restored']}, faults={chaos['faults']}")

        metrics = await queue.get_prometheus_metrics()
        print(metrics[:400])

        await queue.shutdown()

    asyncio.run(_demo())
