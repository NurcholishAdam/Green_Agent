# File: src/enhancements/feedback_collector.py
"""
Enhanced Feedback Collector v2.0.0
- Collects post‑routing metrics, enriches context, batches them,
  and feeds the AdaptiveCostFunction.
- Implements retry, circuit breaker, Prometheus metrics, sampling,
  anomaly detection integration, and raw feedback persistence.
"""

import asyncio
import logging
import json
import uuid
import time
from typing import Dict, Any, Optional, List, Tuple
from dataclasses import dataclass, field
from datetime import datetime
from collections import deque
import numpy as np

# ---------- Pydantic ----------
from pydantic import BaseModel, Field

# ---------- SQLAlchemy ----------
try:
    from sqlalchemy import Column, String, Float, DateTime, Integer, JSON, text, create_engine
    from sqlalchemy.ext.declarative import declarative_base
    from sqlalchemy.orm import sessionmaker, scoped_session
    SQLALCHEMY_AVAILABLE = True
except ImportError:
    SQLALCHEMY_AVAILABLE = False

# ---------- Prometheus ----------
try:
    from prometheus_client import Counter, Gauge, Histogram, CollectorRegistry
    PROMETHEUS_AVAILABLE = True
except ImportError:
    PROMETHEUS_AVAILABLE = False

# ---------- Tenacity ----------
try:
    from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type, before_sleep_log
    TENACITY_AVAILABLE = True
except ImportError:
    TENACITY_AVAILABLE = False

# ---------- Structlog ----------
try:
    import structlog
    logger = structlog.get_logger(__name__)
except ImportError:
    logger = logging.getLogger(__name__)
    logging.basicConfig(level=logging.INFO)

# ---------- Local imports (stubs) ----------
# These would normally be imported from your project.
# For self‑containment, we define dummy classes.
class AdaptiveCostFunction:
    async def record_feedback(self, context: Dict, metrics: Dict) -> None:
        pass
    @property
    def prediction_errors(self):
        return deque(maxlen=1000)
    @property
    def weights(self):
        return {}
    @property
    def learning_rate(self):
        return 0.01

class ExpertRegistry:
    async def get_expert(self, expert_id: str) -> Optional[Any]:
        return None

class NodeRegistry:
    async def get_node(self, node_id: str) -> Optional[Dict]:
        return None

class CarbonIntensityManager:
    async def get_intensity(self, region: str = None) -> float:
        return 400.0

class AnomalyDetector:
    async def ingest(self, node_id: str, metrics: Dict) -> Optional[Any]:
        return None

# ---------- Configuration ----------
class FeedbackCollectorConfig(BaseModel):
    """Configuration for FeedbackCollector."""
    batch_size: int = Field(10, ge=1)
    flush_interval_seconds: float = Field(5.0, ge=0.1)
    sampling_rate: float = Field(1.0, ge=0.0, le=1.0)
    max_retry_attempts: int = Field(3, ge=0)
    circuit_breaker_threshold: int = Field(5, ge=1)
    circuit_breaker_timeout: int = Field(30, ge=1)
    enable_anomaly_detection: bool = True
    enable_persistence: bool = True
    db_path: str = "feedback_collector.db"

# ---------- Circuit Breaker ----------
class CircuitBreakerState(Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"

class EnhancedCircuitBreaker:
    def __init__(self, name: str, threshold: int = 5, timeout: int = 30):
        self.name = name
        self.threshold = threshold
        self.timeout = timeout
        self.state = CircuitBreakerState.CLOSED
        self.failure_count = 0
        self.last_failure_time = None
        self._lock = asyncio.Lock()
        self.metrics = {"total_calls": 0, "failed_calls": 0, "successful_calls": 0}

    async def call(self, func, *args, **kwargs):
        async with self._lock:
            if self.state == CircuitBreakerState.OPEN:
                if time.time() - self.last_failure_time >= self.timeout:
                    self.state = CircuitBreakerState.HALF_OPEN
                    self.failure_count = 0
                else:
                    raise Exception(f"Circuit breaker {self.name} is OPEN")
        self.metrics["total_calls"] += 1
        try:
            result = await func(*args, **kwargs)
            await self._record_success()
            return result
        except Exception as e:
            await self._record_failure()
            raise

    async def _record_success(self):
        async with self._lock:
            self.metrics["successful_calls"] += 1
            if self.state == CircuitBreakerState.HALF_OPEN:
                self.state = CircuitBreakerState.CLOSED
            self.failure_count = 0

    async def _record_failure(self):
        async with self._lock:
            self.metrics["failed_calls"] += 1
            self.failure_count += 1
            self.last_failure_time = time.time()
            if self.state == CircuitBreakerState.CLOSED and self.failure_count >= self.threshold:
                self.state = CircuitBreakerState.OPEN
            elif self.state == CircuitBreakerState.HALF_OPEN:
                self.state = CircuitBreakerState.OPEN

# ---------- Retry decorator ----------
def retry_decorator(attempts: int = 3, min_wait: int = 2, max_wait: int = 10):
    if TENACITY_AVAILABLE:
        return retry(
            stop=stop_after_attempt(attempts),
            wait=wait_exponential(multiplier=1, min=min_wait, max=max_wait),
            retry=retry_if_exception_type(Exception),
            before_sleep=before_sleep_log(logger, logging.WARNING)
        )
    else:
        def decorator(func):
            async def wrapper(*args, **kwargs):
                for attempt in range(attempts):
                    try:
                        return await func(*args, **kwargs)
                    except Exception as e:
                        if attempt == attempts - 1:
                            raise
                        await asyncio.sleep(2 ** attempt)
                return None
            return wrapper
        return decorator

# ---------- Database Models ----------
Base = declarative_base() if SQLALCHEMY_AVAILABLE else None

class FeedbackRecordDB(Base):
    __tablename__ = 'feedback_records'
    id = Column(Integer, primary_key=True)
    request_id = Column(String(128))
    expert_id = Column(String(128))
    node_id = Column(String(128))
    energy_joules = Column(Float)
    carbon_kg = Column(Float)
    helium_units = Column(Float)
    latency_ms = Column(Float)
    accuracy = Column(Float)
    material_index = Column(Float, default=1.0)
    region = Column(String(64), nullable=True)
    carbon_intensity = Column(Float, nullable=True)
    timestamp = Column(DateTime, default=datetime.now)

# ---------- Prometheus Metrics ----------
if PROMETHEUS_AVAILABLE:
    REGISTRY = CollectorRegistry()
    FEEDBACK_RECORDS_TOTAL = Counter('feedback_records_total', 'Total feedback records processed', ['status'], registry=REGISTRY)
    FEEDBACK_ERRORS_TOTAL = Counter('feedback_errors_total', 'Total feedback processing errors', registry=REGISTRY)
    FEEDBACK_PROCESSING_DURATION = Histogram('feedback_processing_duration_seconds', 'Feedback processing latency', registry=REGISTRY)
else:
    class DummyMetric:
        def labels(self, **kwargs): return self
        def inc(self, **kwargs): pass
        def observe(self, **kwargs): pass
    FEEDBACK_RECORDS_TOTAL = DummyMetric()
    FEEDBACK_ERRORS_TOTAL = DummyMetric()
    FEEDBACK_PROCESSING_DURATION = DummyMetric()

# ---------- Main Feedback Collector ----------
class FeedbackCollector:
    """
    Enhanced Feedback Collector v2.0.0
    """

    def __init__(
        self,
        cost_function: AdaptiveCostFunction,
        registry: ExpertRegistry,
        node_registry: Optional[NodeRegistry] = None,
        carbon_manager: Optional[CarbonIntensityManager] = None,
        anomaly_detector: Optional[AnomalyDetector] = None,
        config: Optional[FeedbackCollectorConfig] = None,
    ):
        self.cost_function = cost_function
        self.registry = registry
        self.node_registry = node_registry
        self.carbon_manager = carbon_manager
        self.anomaly_detector = anomaly_detector
        self.config = config or FeedbackCollectorConfig()

        # Feedback queue and batch processing
        self._queue: asyncio.Queue = asyncio.Queue(maxsize=10000)
        self._batch = []
        self._flush_task: Optional[asyncio.Task] = None
        self._running = False
        self._shutdown_event = asyncio.Event()

        # Circuit breaker for DB/API calls
        self._circuit_breaker = EnhancedCircuitBreaker(
            "feedback_collector",
            threshold=self.config.circuit_breaker_threshold,
            timeout=self.config.circuit_breaker_timeout
        )

        # Persistence
        self._db_session = None
        if SQLALCHEMY_AVAILABLE and self.config.enable_persistence:
            self._init_db()

        logger.info("FeedbackCollector initialized", batch_size=self.config.batch_size, sampling_rate=self.config.sampling_rate)

    def _init_db(self):
        """Initialize SQLAlchemy engine and session."""
        engine = create_engine(f"sqlite:///{self.config.db_path}")
        Base.metadata.create_all(engine)
        self._db_session = scoped_session(sessionmaker(bind=engine))

    async def start(self):
        """Start background batch processor."""
        self._running = True
        self._flush_task = asyncio.create_task(self._flush_loop())
        logger.info("FeedbackCollector started")

    async def stop(self):
        """Gracefully shut down."""
        self._running = False
        self._shutdown_event.set()
        if self._flush_task:
            self._flush_task.cancel()
            try:
                await self._flush_task
            except asyncio.CancelledError:
                pass
        # Flush any remaining records
        await self._flush_batch(force=True)
        if self._db_session:
            self._db_session.remove()
        logger.info("FeedbackCollector stopped")

    async def record(
        self,
        request_id: str,
        expert_id: str,
        node_id: str,
        actual_energy_joules: float,
        actual_carbon_kg: float,
        actual_helium_units: float,
        actual_latency_ms: float,
        actual_accuracy: float,
    ) -> None:
        """
        Record actual metrics after a routing decision.
        This method is called by the router after task execution.
        It enqueues the feedback for batch processing.
        """
        # Apply sampling
        if random.random() > self.config.sampling_rate:
            logger.debug("Feedback sampled out", request_id=request_id)
            return

        # Enrich context
        context = {
            'request_id': request_id,
            'expert_id': expert_id,
            'node_id': node_id,
        }

        # Get material index from node registry
        material_index = 1.0
        if self.node_registry:
            try:
                node = await self.node_registry.get_node(node_id)
                if node and 'material_index' in node:
                    material_index = node['material_index']
            except Exception as e:
                logger.warning("Failed to get material index", error=str(e))

        # Get carbon intensity if available
        carbon_intensity = None
        region = None
        if self.carbon_manager:
            try:
                # Assume node_registry provides region; otherwise use default
                region = 'global'
                intensity = await self.carbon_manager.get_intensity(region)
                carbon_intensity = intensity
            except Exception as e:
                logger.warning("Failed to get carbon intensity", error=str(e))

        metrics = {
            'energy_joules': actual_energy_joules,
            'carbon_kg': actual_carbon_kg,
            'helium_units': actual_helium_units,
            'latency_ms': actual_latency_ms,
            'accuracy': actual_accuracy,
            'material_index': material_index,
        }

        # Anomaly detection integration
        if self.anomaly_detector and self.config.enable_anomaly_detection:
            try:
                event = await self.anomaly_detector.ingest(node_id, metrics)
                if event:
                    logger.info("Anomaly detected", node_id=node_id, event=event.description)
            except Exception as e:
                logger.warning("Anomaly detection failed", error=str(e))

        # Enqueue feedback
        await self._queue.put((context, metrics, region, carbon_intensity))
        FEEDBACK_RECORDS_TOTAL.labels(status='queued').inc()

    async def _flush_loop(self):
        """Background task that periodically flushes the queue."""
        while self._running:
            try:
                # Wait for either flush interval or queue size threshold
                await asyncio.sleep(self.config.flush_interval_seconds)
                await self._flush_batch()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("Flush loop error", error=str(e))
                await asyncio.sleep(5)

    async def _flush_batch(self, force: bool = False):
        """Collect items from the queue and process in batches."""
        # Drain queue into batch until batch size or queue empty
        while len(self._batch) < self.config.batch_size and not self._queue.empty():
            try:
                item = self._queue.get_nowait()
                self._batch.append(item)
            except asyncio.QueueEmpty:
                break

        # If batch is not full and not forced, wait for more
        if not self._batch:
            return
        if len(self._batch) < self.config.batch_size and not force:
            return

        # Process the batch
        await self._process_batch(self._batch)
        self._batch.clear()

    async def _process_batch(self, batch: List[Tuple[Dict, Dict, Optional[str], Optional[float]]]):
        """
        Process a batch of feedback records.
        This includes:
        - Persisting raw feedback (if enabled)
        - Feeding records to the cost function (with retry)
        """
        start_time = time.time()
        errors = 0
        for context, metrics, region, carbon_intensity in batch:
            try:
                # Persist raw feedback
                if self.config.enable_persistence and self._db_session:
                    await self._persist_feedback(context, metrics, region, carbon_intensity)

                # Feed to cost function with retry and circuit breaker
                @retry_decorator(attempts=self.config.max_retry_attempts)
                async def feed():
                    await self.cost_function.record_feedback(context, metrics)

                await self._circuit_breaker.call(feed)
                FEEDBACK_RECORDS_TOTAL.labels(status='processed').inc()
            except Exception as e:
                errors += 1
                logger.error("Feedback processing failed", error=str(e), request_id=context.get('request_id'))
                FEEDBACK_ERRORS_TOTAL.inc()

        duration = time.time() - start_time
        FEEDBACK_PROCESSING_DURATION.observe(duration)
        logger.info("Batch processed", size=len(batch), errors=errors, duration_seconds=duration)

    async def _persist_feedback(self, context: Dict, metrics: Dict, region: Optional[str], carbon_intensity: Optional[float]):
        """
        Save raw feedback to database.
        """
        if not self._db_session:
            return
        try:
            session = self._db_session()
            session.execute(
                text("""
                    INSERT INTO feedback_records
                    (request_id, expert_id, node_id, energy_joules, carbon_kg, helium_units,
                     latency_ms, accuracy, material_index, region, carbon_intensity)
                    VALUES (:request_id, :expert_id, :node_id, :energy_joules, :carbon_kg,
                     :helium_units, :latency_ms, :accuracy, :material_index, :region, :carbon_intensity)
                """),
                {
                    'request_id': context.get('request_id'),
                    'expert_id': context.get('expert_id'),
                    'node_id': context.get('node_id'),
                    'energy_joules': metrics.get('energy_joules', 0),
                    'carbon_kg': metrics.get('carbon_kg', 0),
                    'helium_units': metrics.get('helium_units', 0),
                    'latency_ms': metrics.get('latency_ms', 0),
                    'accuracy': metrics.get('accuracy', 0),
                    'material_index': metrics.get('material_index', 1.0),
                    'region': region,
                    'carbon_intensity': carbon_intensity,
                }
            )
            session.commit()
        except Exception as e:
            logger.warning("Failed to persist feedback", error=str(e))
            session.rollback()
        finally:
            session.close()

    async def get_adaptation_status(self) -> Dict[str, Any]:
        """Return current weight values and recent MAE."""
        errors = list(self.cost_function.prediction_errors)
        mae = np.mean(np.abs(errors)) if errors else 0.0
        return {
            'weights': self.cost_function.weights,
            'mae': mae,
            'samples': len(errors),
            'learning_rate': self.cost_function.learning_rate,
            'queue_size': self._queue.qsize(),
            'batch_size': self.config.batch_size,
            'sampling_rate': self.config.sampling_rate,
        }

# ---------- Example usage ----------
if __name__ == "__main__":
    import asyncio
    import random

    # Mock dependencies
    class MockCostFunction(AdaptiveCostFunction):
        async def record_feedback(self, context, metrics):
            print(f"Recorded feedback: {context}, {metrics}")
        @property
        def prediction_errors(self):
            return deque([0.1, -0.2, 0.05], maxlen=100)
        @property
        def weights(self):
            return {'alpha': 0.8, 'beta': 0.2}
        @property
        def learning_rate(self):
            return 0.01

    class MockNodeRegistry:
        async def get_node(self, node_id):
            return {'material_index': 1.5}

    class MockCarbonManager:
        async def get_intensity(self, region):
            return 350.0

    async def main():
        collector = FeedbackCollector(
            cost_function=MockCostFunction(),
            registry=ExpertRegistry(),
            node_registry=MockNodeRegistry(),
            carbon_manager=MockCarbonManager(),
            config=FeedbackCollectorConfig(batch_size=3, flush_interval_seconds=1.0, sampling_rate=0.5)
        )
        await collector.start()

        # Simulate incoming feedback
        for i in range(20):
            await collector.record(
                request_id=f"req_{i}",
                expert_id="expert_1",
                node_id="node_1",
                actual_energy_joules=random.uniform(10, 50),
                actual_carbon_kg=random.uniform(0.1, 0.5),
                actual_helium_units=random.uniform(0, 5),
                actual_latency_ms=random.uniform(50, 200),
                actual_accuracy=random.uniform(0.8, 1.0)
            )
            await asyncio.sleep(0.05)

        await asyncio.sleep(3)
        status = await collector.get_adaptation_status()
        print("Adaptation status:", status)

        await collector.stop()

    asyncio.run(main())
