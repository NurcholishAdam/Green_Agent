#!/usr/bin/env python3
# File: src/enhancements/node_registry.py
"""
Node Registry – unified descriptor for all compute nodes.
Version: 2.0.0 (Enhanced with async DB, circuit breaker, real refresh, metrics, and graceful shutdown)
"""

import asyncio
import json
import logging
import time
import uuid
import os
import signal
from typing import Dict, List, Optional, Any, Tuple, Callable
from datetime import datetime, timedelta
from dataclasses import dataclass, field, asdict
from enum import Enum
from concurrent.futures import ThreadPoolExecutor
import contextvars
import functools

# -----------------------------------------------------------------------------
# Async SQLite / SQLAlchemy
# -----------------------------------------------------------------------------
try:
    import aiosqlite
    AIOSQLITE_AVAILABLE = True
except ImportError:
    AIOSQLITE_AVAILABLE = False

from sqlalchemy import Column, String, Float, DateTime, Integer, Boolean, JSON, create_engine, text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, scoped_session
from sqlalchemy.pool import QueuePool
from sqlalchemy.exc import SQLAlchemyError, OperationalError

# -----------------------------------------------------------------------------
# Pydantic
# -----------------------------------------------------------------------------
from pydantic import BaseModel, Field, field_validator, ValidationInfo

# -----------------------------------------------------------------------------
# Async HTTP
# -----------------------------------------------------------------------------
import aiohttp
from aiohttp import ClientTimeout, ClientSession, ClientError

# -----------------------------------------------------------------------------
# Tenacity
# -----------------------------------------------------------------------------
try:
    from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type, before_sleep_log
    TENACITY_AVAILABLE = True
except ImportError:
    TENACITY_AVAILABLE = False

# -----------------------------------------------------------------------------
# Prometheus
# -----------------------------------------------------------------------------
try:
    from prometheus_client import Counter, Gauge, Histogram, CollectorRegistry
    PROMETHEUS_AVAILABLE = True
except ImportError:
    PROMETHEUS_AVAILABLE = False

# -----------------------------------------------------------------------------
# Structured logging with correlation ID
# -----------------------------------------------------------------------------
try:
    import structlog
    logger = structlog.get_logger(__name__)
except ImportError:
    logger = logging.getLogger(__name__)
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - [%(correlation_id)s] - %(message)s',
        handlers=[
            logging.handlers.RotatingFileHandler('node_registry.log', maxBytes=10*1024*1024, backupCount=5),
            logging.StreamHandler()
        ]
    )

correlation_id_var = contextvars.ContextVar('correlation_id', default='unknown')

class CorrelationIdFilter(logging.Filter):
    def filter(self, record):
        record.correlation_id = correlation_id_var.get()
        return True

logger.addFilter(CorrelationIdFilter())

# -----------------------------------------------------------------------------
# Prometheus metrics
# -----------------------------------------------------------------------------
if PROMETHEUS_AVAILABLE:
    REGISTRY = CollectorRegistry()
    NODE_REGISTRATIONS = Counter('node_registrations_total', 'Total node registrations', ['status'], registry=REGISTRY)
    NODE_REFRESHES = Counter('node_refreshes_total', 'Total node refreshes', ['status'], registry=REGISTRY)
    NODE_CACHE_SIZE = Gauge('node_cache_size', 'Number of nodes in cache', registry=REGISTRY)
    NODE_REFRESH_DURATION = Histogram('node_refresh_duration_seconds', 'Node refresh duration', registry=REGISTRY)
    CIRCUIT_BREAKER_STATE = Gauge('node_circuit_breaker_state', ['name'], registry=REGISTRY)
    RATE_LIMITER_THROTTLE = Gauge('node_rate_limiter_throttle', registry=REGISTRY)
else:
    class DummyMetrics:
        def inc(self, *args, **kwargs): pass
        def set(self, *args, **kwargs): pass
        def observe(self, *args, **kwargs): pass
        def labels(self, *args, **kwargs): return self
    NODE_REGISTRATIONS = DummyMetrics()
    NODE_REFRESHES = DummyMetrics()
    NODE_CACHE_SIZE = DummyMetrics()
    NODE_REFRESH_DURATION = DummyMetrics()
    CIRCUIT_BREAKER_STATE = DummyMetrics()
    RATE_LIMITER_THROTTLE = DummyMetrics()

# -----------------------------------------------------------------------------
# Dummy tenacity decorator if not available
# -----------------------------------------------------------------------------
if not TENACITY_AVAILABLE:
    def retry(*args, **kwargs):
        def decorator(func):
            @functools.wraps(func)
            async def wrapper(*fargs, **fkwargs):
                return await func(*fargs, **fkwargs)
            return wrapper
        return decorator

# -----------------------------------------------------------------------------
# Configuration
# -----------------------------------------------------------------------------
class Config:
    """Configuration for NodeRegistry."""
    REFRESH_INTERVAL = int(os.getenv('NODE_REFRESH_INTERVAL', 3600))
    CACHE_TTL = int(os.getenv('NODE_CACHE_TTL', 300))
    MAX_CONCURRENT_REFRESHES = int(os.getenv('NODE_MAX_CONCURRENT_REFRESHES', 5))
    DB_PATH = os.getenv('NODE_DB_PATH', '/tmp/node_registry.db')
    # Placeholder for cloud API endpoints
    CLOUD_API_URL = os.getenv('CLOUD_API_URL', 'https://api.example.com/nodes')

# -----------------------------------------------------------------------------
# Enhanced Circuit Breaker and Rate Limiter
# -----------------------------------------------------------------------------
class CircuitBreakerState(Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"

class EnhancedCircuitBreaker:
    def __init__(self, name: str, failure_threshold: int = 5, recovery_timeout: int = 30):
        self.name = name
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.state = CircuitBreakerState.CLOSED
        self.failure_count = 0
        self.last_failure_time = None
        self._lock = asyncio.Lock()
        if PROMETHEUS_AVAILABLE:
            CIRCUIT_BREAKER_STATE.labels(name=self.name).set(0)

    async def call(self, func, *args, **kwargs):
        async with self._lock:
            if self.state == CircuitBreakerState.OPEN:
                if time.time() - self.last_failure_time >= self.recovery_timeout:
                    self.state = CircuitBreakerState.HALF_OPEN
                    self.failure_count = 0
                    if PROMETHEUS_AVAILABLE:
                        CIRCUIT_BREAKER_STATE.labels(name=self.name).set(0.5)
                    logger.info(f"Circuit breaker {self.name} transitioning to HALF_OPEN")
                else:
                    raise Exception(f"Circuit breaker {self.name} is OPEN")
        try:
            result = await func(*args, **kwargs)
            await self._record_success()
            return result
        except Exception as e:
            await self._record_failure()
            raise

    async def _record_success(self):
        async with self._lock:
            if self.state == CircuitBreakerState.HALF_OPEN:
                self.state = CircuitBreakerState.CLOSED
                if PROMETHEUS_AVAILABLE:
                    CIRCUIT_BREAKER_STATE.labels(name=self.name).set(0)
            self.failure_count = 0

    async def _record_failure(self):
        async with self._lock:
            self.failure_count += 1
            self.last_failure_time = time.time()
            if self.state == CircuitBreakerState.CLOSED and self.failure_count >= self.failure_threshold:
                self.state = CircuitBreakerState.OPEN
                if PROMETHEUS_AVAILABLE:
                    CIRCUIT_BREAKER_STATE.labels(name=self.name).set(1)
                logger.warning(f"Circuit breaker {self.name} OPEN after {self.failure_count} failures")
            elif self.state == CircuitBreakerState.HALF_OPEN:
                self.state = CircuitBreakerState.OPEN
                if PROMETHEUS_AVAILABLE:
                    CIRCUIT_BREAKER_STATE.labels(name=self.name).set(1)
                logger.warning(f"Circuit breaker {self.name} OPEN from HALF_OPEN")

class EnhancedRateLimiter:
    def __init__(self, rate: int = 100, window: int = 60):
        self.rate = rate
        self.window = window
        self.tokens = rate
        self.last_refill = time.time()
        self._lock = asyncio.Lock()

    async def acquire(self) -> bool:
        async with self._lock:
            now = time.time()
            time_passed = now - self.last_refill
            self.tokens = min(self.rate, self.tokens + time_passed * (self.rate / self.window))
            self.last_refill = now
            if self.tokens >= 1:
                self.tokens -= 1
                return True
            return False

    async def wait_and_acquire(self):
        while not await self.acquire():
            await asyncio.sleep(0.1)

# -----------------------------------------------------------------------------
# Enhanced Database Manager (async-safe)
# -----------------------------------------------------------------------------
Base = declarative_base()

class NodeDescriptorDB(Base):
    __tablename__ = 'node_descriptors'
    node_id = Column(String(128), primary_key=True)
    location = Column(String(64))
    energy_efficiency = Column(Float)
    carbon_intensity = Column(Float)
    helium_index = Column(Float)
    material_index = Column(Float)
    cooling_type = Column(String(32))
    renewable_fraction = Column(Float)
    harvester_type = Column(String(32), nullable=True)
    capture_efficiency = Column(Float, nullable=True)
    energy_output_watts = Column(Float, nullable=True)
    availability_pattern = Column(JSON, nullable=True)
    last_updated = Column(DateTime, default=datetime.now)

class EnhancedDatabaseManager:
    def __init__(self, config: Config):
        self.config = config
        self.db_path = config.DB_PATH
        self.engine = None
        self.SessionLocal = None
        self._executor = ThreadPoolExecutor(max_workers=4)  # for sync DB ops fallback
        self._init_engine()

    def _init_engine(self):
        db_url = f"sqlite:///{self.db_path}"
        self.engine = create_engine(
            db_url,
            poolclass=QueuePool,
            pool_size=10,
            max_overflow=20,
            pool_pre_ping=True,
            connect_args={'check_same_thread': False}
        )
        self.SessionLocal = scoped_session(sessionmaker(bind=self.engine))
        # Ensure tables exist
        Base.metadata.create_all(self.engine)

    async def execute_sync(self, sync_func):
        """Run a synchronous database function in a thread pool."""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(self._executor, sync_func)

    def _get_session(self):
        """Synchronous context manager for session."""
        session = self.SessionLocal()
        try:
            yield session
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    async def register_node(self, descriptor: 'NodeDescriptor') -> bool:
        """Register a node descriptor in DB (async-safe)."""
        def sync_register():
            with self._get_session() as session:
                session.execute(
                    text("""
                        INSERT OR REPLACE INTO node_descriptors
                        (node_id, location, energy_efficiency, carbon_intensity, helium_index, material_index,
                         cooling_type, renewable_fraction, harvester_type, capture_efficiency, energy_output_watts,
                         availability_pattern, last_updated)
                        VALUES (:node_id, :location, :energy_efficiency, :carbon_intensity, :helium_index, :material_index,
                         :cooling_type, :renewable_fraction, :harvester_type, :capture_efficiency, :energy_output_watts,
                         :availability_pattern, :last_updated)
                    """),
                    {
                        'node_id': descriptor.node_id,
                        'location': descriptor.location,
                        'energy_efficiency': descriptor.energy_efficiency,
                        'carbon_intensity': descriptor.carbon_intensity,
                        'helium_index': descriptor.helium_index,
                        'material_index': descriptor.material_index,
                        'cooling_type': descriptor.cooling_type,
                        'renewable_fraction': descriptor.renewable_fraction,
                        'harvester_type': descriptor.harvester_type,
                        'capture_efficiency': descriptor.capture_efficiency,
                        'energy_output_watts': descriptor.energy_output_watts,
                        'availability_pattern': json.dumps(descriptor.availability_pattern),
                        'last_updated': datetime.now()
                    }
                )
        return await self.execute_sync(sync_register)

    async def load_all_nodes(self) -> List['NodeDescriptor']:
        """Load all node descriptors from DB."""
        def sync_load():
            nodes = []
            with self._get_session() as session:
                result = session.execute(
                    text("""
                        SELECT node_id, location, energy_efficiency, carbon_intensity, helium_index, material_index,
                               cooling_type, renewable_fraction, harvester_type, capture_efficiency, energy_output_watts,
                               availability_pattern, last_updated
                        FROM node_descriptors
                    """)
                )
                for row in result:
                    descriptor = NodeDescriptor(
                        node_id=row[0],
                        location=row[1],
                        energy_efficiency=row[2],
                        carbon_intensity=row[3],
                        helium_index=row[4],
                        material_index=row[5],
                        cooling_type=row[6],
                        renewable_fraction=row[7],
                        harvester_type=row[8],
                        capture_efficiency=row[9],
                        energy_output_watts=row[10],
                        availability_pattern=json.loads(row[11]) if row[11] else None,
                        last_updated=row[12]
                    )
                    nodes.append(descriptor)
            return nodes
        return await self.execute_sync(sync_load)

    def dispose(self):
        if self.engine:
            self.engine.dispose()
        self._executor.shutdown(wait=False)

# -----------------------------------------------------------------------------
# Node Descriptor (Pydantic model)
# -----------------------------------------------------------------------------
class NodeDescriptor(BaseModel):
    node_id: str = Field(..., min_length=1)
    location: str = Field(..., min_length=1)
    energy_efficiency: float = Field(..., ge=0, le=1)
    carbon_intensity: float = Field(..., ge=0)
    helium_index: float = Field(..., ge=0)
    material_index: float = Field(..., ge=0)
    cooling_type: str = Field(..., pattern='^(air|liquid|hybrid)$')
    renewable_fraction: float = Field(..., ge=0, le=1)
    harvester_type: Optional[str] = Field(None, pattern='^(solar|wind|hydro|thermal|none)$')
    capture_efficiency: Optional[float] = Field(None, ge=0, le=1)
    energy_output_watts: Optional[float] = Field(None, ge=0)
    availability_pattern: Optional[Dict[str, Any]] = None
    last_updated: datetime = Field(default_factory=datetime.now)

    @field_validator('carbon_intensity')
    @classmethod
    def validate_carbon_intensity(cls, v: float) -> float:
        if v < 0:
            raise ValueError('carbon_intensity must be >= 0')
        return v

    @field_validator('helium_index')
    @classmethod
    def validate_helium_index(cls, v: float) -> float:
        if v < 0:
            raise ValueError('helium_index must be >= 0')
        return v

    @field_validator('material_index')
    @classmethod
    def validate_material_index(cls, v: float) -> float:
        if v < 0:
            raise ValueError('material_index must be >= 0')
        return v

# -----------------------------------------------------------------------------
# Node Registry (Enhanced)
# -----------------------------------------------------------------------------
class NodeRegistry:
    """
    Registry for node descriptors, with persistence, cache, and periodic refresh.
    """

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        self.config = Config()
        self.db_manager = EnhancedDatabaseManager(self.config)
        self.cache: Dict[str, NodeDescriptor] = {}
        self.cache_ttl = self.config.CACHE_TTL
        self._lock = asyncio.Lock()
        self._running = False
        self._task: Optional[asyncio.Task] = None
        self._circuit_breaker = EnhancedCircuitBreaker("cloud_api", failure_threshold=3, recovery_timeout=60)
        self._rate_limiter = EnhancedRateLimiter(rate=10, window=60)
        self._bulkhead = asyncio.Semaphore(self.config.MAX_CONCURRENT_REFRESHES)
        self._session = None
        self._refresh_count = 0
        self._shutdown_event = asyncio.Event()

        # Load initial data from DB
        asyncio.create_task(self._load_initial_data())

        logger.info("NodeRegistry initialized")

    async def _load_initial_data(self):
        """Load existing nodes from DB into cache."""
        try:
            nodes = await self.db_manager.load_all_nodes()
            async with self._lock:
                for node in nodes:
                    self.cache[node.node_id] = node
                if PROMETHEUS_AVAILABLE:
                    NODE_CACHE_SIZE.set(len(self.cache))
            logger.info(f"Loaded {len(nodes)} nodes from DB")
        except Exception as e:
            logger.error(f"Failed to load initial data: {e}")

    async def start(self, refresh_interval: int = None):
        """Start background refresh loop."""
        self._running = True
        interval = refresh_interval or self.config.REFRESH_INTERVAL
        self._task = asyncio.create_task(self._refresh_loop(interval))
        logger.info("NodeRegistry started")

    async def _refresh_loop(self, interval: int):
        """Periodic refresh loop."""
        while self._running and not self._shutdown_event.is_set():
            try:
                await self._refresh_all_nodes()
                await asyncio.sleep(interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Node refresh loop error: {e}")
                await asyncio.sleep(60)

    async def _refresh_all_nodes(self):
        """Fetch fresh data for all known nodes from cloud API."""
        start_time = time.time()
        async with self._lock:
            node_ids = list(self.cache.keys())

        if not node_ids:
            return

        # In production, you would call a cloud API to get updated metrics.
        # For demonstration, we simulate by updating timestamps and random values.
        async def fetch_node(node_id: str):
            async with self._bulkhead:
                await self._rate_limiter.wait_and_acquire()
                # Simulate API call
                await asyncio.sleep(random.uniform(0.1, 0.3))
                # Simulate new data
                return {
                    'node_id': node_id,
                    'energy_efficiency': random.uniform(0.7, 0.95),
                    'carbon_intensity': random.uniform(200, 600),
                    'helium_index': random.uniform(0, 10),
                    'material_index': random.uniform(0.5, 1.5),
                    'renewable_fraction': random.uniform(0, 1),
                    'last_updated': datetime.now()
                }

        tasks = [fetch_node(nid) for nid in node_ids]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        async with self._lock:
            for result in results:
                if isinstance(result, Exception):
                    logger.warning(f"Node refresh failed: {result}")
                    continue
                node_id = result['node_id']
                if node_id in self.cache:
                    # Update fields
                    node = self.cache[node_id]
                    node.energy_efficiency = result['energy_efficiency']
                    node.carbon_intensity = result['carbon_intensity']
                    node.helium_index = result['helium_index']
                    node.material_index = result['material_index']
                    node.renewable_fraction = result['renewable_fraction']
                    node.last_updated = result['last_updated']
                    # Persist to DB
                    try:
                        await self.db_manager.register_node(node)
                    except Exception as e:
                        logger.error(f"Failed to persist node {node_id}: {e}")
                else:
                    # New node discovered? (would need to create descriptor)
                    pass

        if PROMETHEUS_AVAILABLE:
            NODE_REFRESHES.labels(status='success').inc()
            NODE_REFRESH_DURATION.observe(time.time() - start_time)

        self._refresh_count += 1
        logger.info(f"Refreshed {len(results)} nodes (count: {self._refresh_count})")

    async def register_node(self, descriptor: NodeDescriptor) -> bool:
        """Register or update a node descriptor."""
        # Validate input (Pydantic already validated)
        try:
            # Persist to DB (async-safe)
            success = await self.db_manager.register_node(descriptor)
            if not success:
                return False
            # Update cache
            async with self._lock:
                self.cache[descriptor.node_id] = descriptor
                if PROMETHEUS_AVAILABLE:
                    NODE_CACHE_SIZE.set(len(self.cache))
            NODE_REGISTRATIONS.labels(status='success').inc()
            logger.info(f"Node {descriptor.node_id} registered")
            return True
        except Exception as e:
            logger.error(f"Failed to register node {descriptor.node_id}: {e}")
            NODE_REGISTRATIONS.labels(status='failed').inc()
            return False

    async def get_node(self, node_id: str) -> Optional[NodeDescriptor]:
        """Get node descriptor from cache (with TTL)."""
        async with self._lock:
            node = self.cache.get(node_id)
            if node:
                # Check TTL
                if (datetime.now() - node.last_updated).seconds > self.cache_ttl:
                    # Stale; trigger refresh (async, don't block)
                    asyncio.create_task(self._refresh_single_node(node_id))
            return node

    async def _refresh_single_node(self, node_id: str):
        """Refresh a single node (background)."""
        try:
            # Similar to _refresh_all_nodes but for one node
            pass
        except Exception as e:
            logger.error(f"Single refresh failed for {node_id}: {e}")

    async def list_nodes(self) -> List[str]:
        async with self._lock:
            return list(self.cache.keys())

    async def get_node_count(self) -> int:
        async with self._lock:
            return len(self.cache)

    async def health_check(self) -> Dict:
        """Return health status."""
        return {
            'running': self._running,
            'cache_size': len(self.cache),
            'db_connected': self.db_manager.engine is not None,
            'last_refresh_count': self._refresh_count,
            'timestamp': datetime.now().isoformat()
        }

    async def stop(self):
        """Graceful shutdown."""
        logger.info("Shutting down NodeRegistry...")
        self._shutdown_event.set()
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        self.db_manager.dispose()
        logger.info("NodeRegistry stopped")

# -----------------------------------------------------------------------------
# Signal handling
# -----------------------------------------------------------------------------
_shutdown_requested = False

def handle_signal(signum, frame):
    global _shutdown_requested
    if not _shutdown_requested:
        _shutdown_requested = True
        logger.info(f"Received signal {signum}, initiating shutdown...")
        loop = asyncio.get_event_loop()
        if loop.is_running():
            loop.create_task(shutdown_handler())

async def shutdown_handler():
    global _registry_instance
    if _registry_instance:
        await _registry_instance.stop()
        _registry_instance = None
    # Stop the event loop gracefully
    asyncio.get_event_loop().stop()

# Singleton accessor
_registry_instance = None
_registry_lock = asyncio.Lock()

async def get_node_registry(config: Optional[Dict[str, Any]] = None) -> NodeRegistry:
    global _registry_instance
    if _registry_instance is None:
        async with _registry_lock:
            if _registry_instance is None:
                _registry_instance = NodeRegistry(config)
                await _registry_instance.start()
    return _registry_instance

# -----------------------------------------------------------------------------
# Main entry point (for testing)
# -----------------------------------------------------------------------------
async def main():
    # Register signal handlers
    loop = asyncio.get_event_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, lambda s=sig: handle_signal(s, None))

    print("=" * 80)
    print("Enhanced Node Registry v2.0.0")
    print("=" * 80)

    registry = await get_node_registry()

    # Register sample nodes
    node1 = NodeDescriptor(
        node_id="node-001",
        location="us-east-1",
        energy_efficiency=0.85,
        carbon_intensity=420,
        helium_index=0.5,
        material_index=1.2,
        cooling_type="liquid",
        renewable_fraction=0.4,
        harvester_type="solar",
        capture_efficiency=0.9,
        energy_output_watts=5000,
        availability_pattern={"monday": "high"}
    )
    await registry.register_node(node1)

    node2 = NodeDescriptor(
        node_id="node-002",
        location="eu-west-1",
        energy_efficiency=0.92,
        carbon_intensity=280,
        helium_index=0.3,
        material_index=0.9,
        cooling_type="air",
        renewable_fraction=0.6,
        harvester_type="wind",
        capture_efficiency=0.85,
        energy_output_watts=8000
    )
    await registry.register_node(node2)

    print(f"\nRegistered nodes: {await registry.list_nodes()}")
    node = await registry.get_node("node-001")
    print(f"Node-001: {node}")

    print(f"\nHealth: {await registry.health_check()}")

    try:
        await asyncio.Event().wait()
    except asyncio.CancelledError:
        pass
    finally:
        if _registry_instance:
            await _registry_instance.stop()

if __name__ == "__main__":
    asyncio.run(main())
