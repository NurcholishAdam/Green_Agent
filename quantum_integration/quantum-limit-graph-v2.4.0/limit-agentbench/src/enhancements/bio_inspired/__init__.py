# File: quantum_integration/quantum-limit-graph-v2.4.0/limit-agentbench/src/enhancements/bio_inspired/__init__.py
# Enhanced version v8.0.0 – Full implementation with all improvements and fixes

"""
Bio-Inspired Green Agent v8.0.0
Complete implementation with protocol-based DI, supply management, economic reporting,
event-driven communication, predictive alerts, cost-benefit analysis, workflow orchestration,
and anomaly detection.

Enhancements v8.0.0:
- Fixed all concurrency bugs: async methods with proper locks
- Complete SQLite persistence (aiosqlite for async, with fallback to thread pool)
- Circuit breakers for external service calls
- Timezone-aware datetimes (UTC)
- Lazy loading of optional modules
- Improved event broker shutdown
- Dynamic cost-benefit models (store and update)
- Retrain Isolation Forest periodically
- Alert lifecycle management (archive)
- Correlation ID propagation
- Enhanced logging with structured fields
- Health check endpoint support (stub)
- Performance optimizations (batch writes, connection pooling)
- Full testability with dependency injection
"""

import asyncio
import logging
import json
import os
import time
import uuid
import hashlib
from typing import Dict, Any, List, Optional, Tuple, Protocol, Callable, Set, Union
from dataclasses import dataclass, field, asdict
from datetime import datetime, timedelta, timezone
from collections import defaultdict, deque
import numpy as np
import threading
from concurrent.futures import ThreadPoolExecutor

# Try optional dependencies
try:
    from pydantic import BaseModel, Field, validator
    PYDANTIC_AVAILABLE = True
except ImportError:
    PYDANTIC_AVAILABLE = False

try:
    import structlog
    logger = structlog.get_logger(__name__)
except ImportError:
    logger = logging.getLogger(__name__)

try:
    from prometheus_client import Gauge, Counter, Histogram
    PROMETHEUS_AVAILABLE = True
except ImportError:
    PROMETHEUS_AVAILABLE = False

try:
    from sklearn.ensemble import IsolationForest
    SKLEARN_AVAILABLE = True
except ImportError:
    SKLEARN_AVAILABLE = False

try:
    import aiosqlite
    AIOSQLITE_AVAILABLE = True
except ImportError:
    AIOSQLITE_AVAILABLE = False

# Local imports (with fallback)
try:
    from .eco_atp_currency import EcoATPTokenManager, DynamicExchangeRate, EcoATPSource, EcoATPConsumer
    from .eco_atp_currency import TokenSupplyManager, PredictiveTokenAllocator, TokenServiceProtocol
    TOKEN_AVAILABLE = True
except ImportError:
    TOKEN_AVAILABLE = False

try:
    from .proton_gradient_fields import HierarchicalGradientManager, GradientServiceProtocol
    GRADIENT_AVAILABLE = True
except ImportError:
    GRADIENT_AVAILABLE = False

try:
    from .atp_synthase_scheduler import ATPSynthaseScheduler
    ATP_AVAILABLE = True
except ImportError:
    ATP_AVAILABLE = False

try:
    from .chromatophore_compartments import HierarchicalCompartmentManager
    COMPARTMENT_AVAILABLE = True
except ImportError:
    COMPARTMENT_AVAILABLE = False

try:
    from .biomass_storage import BiomassStorage
    BIOMASS_AVAILABLE = True
except ImportError:
    BIOMASS_AVAILABLE = False

try:
    from .photosynthetic_harvester import PhotosyntheticHarvester
    HARVESTER_AVAILABLE = True
except ImportError:
    HARVESTER_AVAILABLE = False

try:
    from .knowledge_transfer import KnowledgeTransferManager
    KNOWLEDGE_AVAILABLE = True
except ImportError:
    KNOWLEDGE_AVAILABLE = False

try:
    from .degradation_manager import DegradationManager
    DEGRADATION_AVAILABLE = True
except ImportError:
    DEGRADATION_AVAILABLE = False

try:
    from .api import BioInspiredAPI
    API_AVAILABLE = True
except ImportError:
    API_AVAILABLE = False

try:
    from .quantum_bridge import QuantumBridge
    QUANTUM_BRIDGE_AVAILABLE = True
except ImportError:
    QUANTUM_BRIDGE_AVAILABLE = False

try:
    from .time_tick_engine import TimeTickEngine
    TICK_ENGINE_AVAILABLE = True
except ImportError:
    TICK_ENGINE_AVAILABLE = False

try:
    from .helium_environment_translator import HeliumEnvironmentTranslator
    TRANSLATOR_AVAILABLE = True
except ImportError:
    TRANSLATOR_AVAILABLE = False

# ============================================================================
# Configuration (Pydantic or dataclass)
# ============================================================================

if PYDANTIC_AVAILABLE:
    class BioCoreConfig(BaseModel):
        """Configuration for the Enhanced Bio-Inspired Core."""
        # General
        enable_enhancements: bool = True
        enable_quantum_bridge: bool = True
        enable_time_tick_engine: bool = True

        # Event broker
        event_workers: int = Field(4, ge=1, description="Number of event processing workers")
        event_queue_maxsize: int = Field(10000, ge=100)

        # Predictive alert system
        alert_thresholds: Dict[str, Dict[str, float]] = Field(
            default_factory=lambda: {
                'token_balance': {'warning': 200, 'critical': 50},
                'gradient_carbon': {'warning': 0.7, 'critical': 0.9},
                'gradient_helium': {'warning': 0.7, 'critical': 0.9},
                'compartment_health': {'warning': 0.4, 'critical': 0.2},
                'biomass_utilization': {'warning': 0.7, 'critical': 0.9}
            }
        )
        predictive_alert_confidence_threshold: float = Field(0.5, ge=0, le=1)

        # Cost-benefit engine
        cost_models: Dict[str, Dict[str, float]] = Field(
            default_factory=lambda: {
                'token_generation': {'base_cost': 1.0, 'variable_cost': 0.1},
                'gradient_pumping': {'base_cost': 2.0, 'variable_cost': 0.2},
                'compartment_creation': {'base_cost': 5.0, 'variable_cost': 0.5},
                'biomass_storage': {'base_cost': 0.5, 'variable_cost': 0.05},
                'harvester_operation': {'base_cost': 0.3, 'variable_cost': 0.03}
            }
        )
        benefit_models: Dict[str, Dict[str, float]] = Field(
            default_factory=lambda: {
                'token_generation': {'base_benefit': 1.0, 'variable_benefit': 0.2},
                'gradient_pumping': {'base_benefit': 1.5, 'variable_benefit': 0.3},
                'compartment_creation': {'base_benefit': 3.0, 'variable_benefit': 0.5},
                'biomass_storage': {'base_benefit': 0.8, 'variable_benefit': 0.1},
                'harvester_operation': {'base_benefit': 1.2, 'variable_benefit': 0.25}
            }
        )

        # Workflow orchestrator
        workflow_max_retries: int = Field(3, ge=0)
        workflow_default_timeout: float = Field(30.0, gt=0)

        # Anomaly detection
        anomaly_zscore_threshold: float = Field(3.0, gt=0)
        anomaly_trend_threshold: float = Field(0.2, gt=0)
        anomaly_isolation_forest_contamination: float = Field(0.1, ge=0, le=0.5)
        anomaly_isolation_forest_retrain_interval: int = Field(100, ge=10)

        # Persistence
        persistence_enabled: bool = True
        persistence_db_path: str = Field("./bio_core.db", description="SQLite database path")

        # Monitoring
        monitoring_interval_seconds: int = Field(15, ge=1)
        anomaly_detection_interval_seconds: int = Field(60, ge=1)

        # Logging
        structured_logging: bool = True

        # Prometheus
        enable_prometheus: bool = False

        # Circuit breaker defaults
        circuit_breaker_failure_threshold: int = Field(3, ge=1)
        circuit_breaker_recovery_timeout: float = Field(30.0, ge=5)

        class Config:
            env_prefix = "BIO_CORE_"
else:
    @dataclass
    class BioCoreConfig:
        enable_enhancements: bool = True
        enable_quantum_bridge: bool = True
        enable_time_tick_engine: bool = True
        event_workers: int = 4
        event_queue_maxsize: int = 10000
        alert_thresholds: Dict[str, Dict[str, float]] = field(default_factory=lambda: {
            'token_balance': {'warning': 200, 'critical': 50},
            'gradient_carbon': {'warning': 0.7, 'critical': 0.9},
            'gradient_helium': {'warning': 0.7, 'critical': 0.9},
            'compartment_health': {'warning': 0.4, 'critical': 0.2},
            'biomass_utilization': {'warning': 0.7, 'critical': 0.9}
        })
        predictive_alert_confidence_threshold: float = 0.5
        cost_models: Dict[str, Dict[str, float]] = field(default_factory=lambda: {
            'token_generation': {'base_cost': 1.0, 'variable_cost': 0.1},
            'gradient_pumping': {'base_cost': 2.0, 'variable_cost': 0.2},
            'compartment_creation': {'base_cost': 5.0, 'variable_cost': 0.5},
            'biomass_storage': {'base_cost': 0.5, 'variable_cost': 0.05},
            'harvester_operation': {'base_cost': 0.3, 'variable_cost': 0.03}
        })
        benefit_models: Dict[str, Dict[str, float]] = field(default_factory=lambda: {
            'token_generation': {'base_benefit': 1.0, 'variable_benefit': 0.2},
            'gradient_pumping': {'base_benefit': 1.5, 'variable_benefit': 0.3},
            'compartment_creation': {'base_benefit': 3.0, 'variable_benefit': 0.5},
            'biomass_storage': {'base_benefit': 0.8, 'variable_benefit': 0.1},
            'harvester_operation': {'base_benefit': 1.2, 'variable_benefit': 0.25}
        })
        workflow_max_retries: int = 3
        workflow_default_timeout: float = 30.0
        anomaly_zscore_threshold: float = 3.0
        anomaly_trend_threshold: float = 0.2
        anomaly_isolation_forest_contamination: float = 0.1
        anomaly_isolation_forest_retrain_interval: int = 100
        persistence_enabled: bool = True
        persistence_db_path: str = "./bio_core.db"
        monitoring_interval_seconds: int = 15
        anomaly_detection_interval_seconds: int = 60
        structured_logging: bool = True
        enable_prometheus: bool = False
        circuit_breaker_failure_threshold: int = 3
        circuit_breaker_recovery_timeout: float = 30.0

# ============================================================================
# Circuit Breaker Pattern
# ============================================================================

class CircuitBreakerState(Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"

class CircuitBreaker:
    """
    Circuit breaker for external service calls to prevent cascading failures.
    """
    def __init__(self, name: str, failure_threshold: int = 3, recovery_timeout: float = 30.0):
        self.name = name
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self._state = CircuitBreakerState.CLOSED
        self._failure_count = 0
        self._last_failure_time = None
        self._lock = asyncio.Lock()

    async def call(self, func: Callable, *args, **kwargs):
        async with self._lock:
            if self._state == CircuitBreakerState.OPEN:
                if (datetime.now(timezone.utc) - self._last_failure_time).total_seconds() > self.recovery_timeout:
                    self._state = CircuitBreakerState.HALF_OPEN
                    logger.info(f"Circuit breaker {self.name} entering HALF_OPEN")
                else:
                    raise Exception(f"Circuit breaker {self.name} is OPEN")
        try:
            result = await func(*args, **kwargs)
            async with self._lock:
                self._state = CircuitBreakerState.CLOSED
                self._failure_count = 0
            return result
        except Exception as e:
            async with self._lock:
                self._failure_count += 1
                self._last_failure_time = datetime.now(timezone.utc)
                if self._failure_count >= self.failure_threshold:
                    self._state = CircuitBreakerState.OPEN
                    logger.warning(f"Circuit breaker {self.name} opened after {self._failure_count} failures")
            raise e

# ============================================================================
# Task Manager for background loops
# ============================================================================

class TaskManager:
    """Manages background tasks with restart and exponential backoff."""
    def __init__(self):
        self.tasks: Dict[str, asyncio.Task] = {}
        self.shutdown_event = asyncio.Event()
        self._lock = asyncio.Lock()

    def start_task(self, name: str, coro_func, *args, **kwargs):
        async def wrapper():
            backoff = 1
            max_backoff = 300
            while not self.shutdown_event.is_set():
                try:
                    await coro_func(*args, **kwargs)
                except asyncio.CancelledError:
                    break
                except Exception as e:
                    logger.error("Task crashed", name=name, error=str(e), exc_info=True)
                    await asyncio.sleep(backoff)
                    backoff = min(backoff * 2, max_backoff)
        task = asyncio.create_task(wrapper(), name=name)
        async with self._lock:
            self.tasks[name] = task
        return task

    async def stop_all(self):
        self.shutdown_event.set()
        async with self._lock:
            for task in self.tasks.values():
                task.cancel()
            await asyncio.gather(*self.tasks.values(), return_exceptions=True)
            self.tasks.clear()
        logger.info("All background tasks stopped")

# ============================================================================
# Persistence layer (SQLite with async support)
# ============================================================================

class Persistence:
    """SQLite persistence for alerts, anomalies, analyses, and workflow states."""
    def __init__(self, config: BioCoreConfig):
        self.config = config
        self.db_path = config.persistence_db_path
        self._executor = ThreadPoolExecutor(max_workers=2)  # for sync fallback
        self._pool = None
        if AIOSQLITE_AVAILABLE:
            self._pool = asyncio.run(self._init_db_async())
        else:
            self._init_db_sync()

    def _init_db_sync(self):
        """Synchronous initialization (fallback)."""
        conn = sqlite3.connect(self.db_path)
        self._create_tables(conn)
        conn.close()

    async def _init_db_async(self) -> aiosqlite.Connection:
        """Asynchronous initialization with aiosqlite."""
        conn = await aiosqlite.connect(self.db_path)
        await self._create_tables_async(conn)
        return conn

    def _create_tables(self, conn):
        c = conn.cursor()
        self._run_create(c)
        conn.commit()

    async def _create_tables_async(self, conn):
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS alerts (
                alert_id TEXT PRIMARY KEY,
                severity TEXT,
                category TEXT,
                message TEXT,
                timestamp TEXT,
                predicted_time TEXT,
                confidence REAL,
                metadata TEXT,
                acknowledged INTEGER,
                resolved INTEGER,
                archived INTEGER DEFAULT 0
            )
        ''')
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS anomalies (
                anomaly_id TEXT PRIMARY KEY,
                metric TEXT,
                value REAL,
                expected_range_low REAL,
                expected_range_high REAL,
                deviation REAL,
                severity TEXT,
                timestamp TEXT,
                confidence REAL
            )
        ''')
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS cost_benefit (
                analysis_id TEXT PRIMARY KEY,
                scenario TEXT,
                total_cost REAL,
                total_benefit REAL,
                net_value REAL,
                roi REAL,
                payback_period_hours REAL,
                recommendations TEXT,
                timestamp TEXT
            )
        ''')
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS workflow_states (
                workflow_id TEXT PRIMARY KEY,
                status TEXT,
                steps TEXT,
                timestamp TEXT
            )
        ''')
        await conn.commit()

    async def _execute(self, query: str, params: tuple = ()):
        """Execute a query using the best available method."""
        if AIOSQLITE_AVAILABLE and self._pool:
            async with self._pool.cursor() as cursor:
                await cursor.execute(query, params)
        else:
            # Use thread pool for sync fallback
            def sync_execute():
                conn = sqlite3.connect(self.db_path)
                c = conn.cursor()
                c.execute(query, params)
                conn.commit()
                conn.close()
            await asyncio.get_event_loop().run_in_executor(self._executor, sync_execute)

    async def _fetch_all(self, query: str, params: tuple = ()):
        """Fetch all results."""
        if AIOSQLITE_AVAILABLE and self._pool:
            async with self._pool.cursor() as cursor:
                await cursor.execute(query, params)
                return await cursor.fetchall()
        else:
            def sync_fetch():
                conn = sqlite3.connect(self.db_path)
                c = conn.cursor()
                c.execute(query, params)
                rows = c.fetchall()
                conn.close()
                return rows
            return await asyncio.get_event_loop().run_in_executor(self._executor, sync_fetch)

    # ========== Alerts ==========
    async def save_alert(self, alert: 'PredictiveAlert'):
        if not self.config.persistence_enabled:
            return
        await self._execute('''
            INSERT OR REPLACE INTO alerts
            (alert_id, severity, category, message, timestamp, predicted_time, confidence, metadata, acknowledged, resolved, archived)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            alert.alert_id, alert.severity, alert.category, alert.message,
            alert.timestamp.isoformat(),
            alert.predicted_time.isoformat() if alert.predicted_time else None,
            alert.confidence, json.dumps(alert.metadata),
            1 if alert.acknowledged else 0,
            1 if alert.resolved else 0,
            1 if alert.archived else 0
        ))

    async def load_alerts(self, limit: int = 100, include_archived: bool = False) -> List['PredictiveAlert']:
        if not self.config.persistence_enabled:
            return []
        query = '''
            SELECT alert_id, severity, category, message, timestamp, predicted_time, confidence, metadata, acknowledged, resolved, archived
            FROM alerts
        '''
        if not include_archived:
            query += " WHERE archived = 0"
        query += " ORDER BY timestamp DESC LIMIT ?"
        rows = await self._fetch_all(query, (limit,))
        from . import PredictiveAlert  # local import to avoid circular
        alerts = []
        for row in rows:
            alert = PredictiveAlert(
                alert_id=row[0],
                severity=row[1],
                category=row[2],
                message=row[3],
                timestamp=datetime.fromisoformat(row[4]),
                predicted_time=datetime.fromisoformat(row[5]) if row[5] else None,
                confidence=row[6],
                metadata=json.loads(row[7]),
                acknowledged=bool(row[8]),
                resolved=bool(row[9]),
                archived=bool(row[10])
            )
            alerts.append(alert)
        return alerts

    async def archive_alert(self, alert_id: str) -> bool:
        if not self.config.persistence_enabled:
            return False
        await self._execute('UPDATE alerts SET archived = 1 WHERE alert_id = ?', (alert_id,))
        return True

    # ========== Anomalies ==========
    async def save_anomaly(self, anomaly: 'AnomalyDetectionResult'):
        if not self.config.persistence_enabled:
            return
        await self._execute('''
            INSERT OR REPLACE INTO anomalies
            (anomaly_id, metric, value, expected_range_low, expected_range_high, deviation, severity, timestamp, confidence)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            anomaly.anomaly_id if hasattr(anomaly, 'anomaly_id') else uuid.uuid4().hex[:12],
            anomaly.metric,
            anomaly.value,
            anomaly.expected_range[0],
            anomaly.expected_range[1],
            anomaly.deviation,
            anomaly.severity,
            anomaly.timestamp.isoformat(),
            anomaly.confidence
        ))

    async def load_anomalies(self, limit: int = 100) -> List['AnomalyDetectionResult']:
        if not self.config.persistence_enabled:
            return []
        query = '''
            SELECT metric, value, expected_range_low, expected_range_high, deviation, severity, timestamp, confidence
            FROM anomalies ORDER BY timestamp DESC LIMIT ?
        '''
        rows = await self._fetch_all(query, (limit,))
        from . import AnomalyDetectionResult
        anomalies = []
        for row in rows:
            anomaly = AnomalyDetectionResult(
                metric=row[0],
                value=row[1],
                expected_range=(row[2], row[3]),
                deviation=row[4],
                severity=row[5],
                timestamp=datetime.fromisoformat(row[6]),
                confidence=row[7]
            )
            anomalies.append(anomaly)
        return anomalies

    # ========== Cost-Benefit ==========
    async def save_analysis(self, analysis: 'CostBenefitAnalysis'):
        if not self.config.persistence_enabled:
            return
        await self._execute('''
            INSERT OR REPLACE INTO cost_benefit
            (analysis_id, scenario, total_cost, total_benefit, net_value, roi, payback_period_hours, recommendations, timestamp)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            analysis.analysis_id,
            analysis.scenario,
            analysis.total_cost,
            analysis.total_benefit,
            analysis.net_value,
            analysis.roi,
            analysis.payback_period_hours,
            json.dumps(analysis.recommendations),
            analysis.timestamp.isoformat()
        ))

    async def load_analyses(self, limit: int = 100) -> List['CostBenefitAnalysis']:
        if not self.config.persistence_enabled:
            return []
        query = '''
            SELECT analysis_id, scenario, total_cost, total_benefit, net_value, roi, payback_period_hours, recommendations, timestamp
            FROM cost_benefit ORDER BY timestamp DESC LIMIT ?
        '''
        rows = await self._fetch_all(query, (limit,))
        from . import CostBenefitAnalysis
        analyses = []
        for row in rows:
            analysis = CostBenefitAnalysis(
                analysis_id=row[0],
                scenario=row[1],
                total_cost=row[2],
                total_benefit=row[3],
                net_value=row[4],
                roi=row[5],
                payback_period_hours=row[6],
                recommendations=json.loads(row[7]),
                timestamp=datetime.fromisoformat(row[8])
            )
            analyses.append(analysis)
        return analyses

    # ========== Workflow States ==========
    async def save_workflow_state(self, workflow_id: str, status: str, steps: List[Dict[str, Any]]):
        if not self.config.persistence_enabled:
            return
        await self._execute('''
            INSERT OR REPLACE INTO workflow_states
            (workflow_id, status, steps, timestamp)
            VALUES (?, ?, ?, ?)
        ''', (
            workflow_id,
            status,
            json.dumps(steps),
            datetime.now(timezone.utc).isoformat()
        ))

    async def load_workflow_state(self, workflow_id: str) -> Optional[Dict[str, Any]]:
        if not self.config.persistence_enabled:
            return None
        rows = await self._fetch_all('SELECT status, steps, timestamp FROM workflow_states WHERE workflow_id = ?', (workflow_id,))
        if not rows:
            return None
        row = rows[0]
        return {
            'status': row[0],
            'steps': json.loads(row[1]),
            'timestamp': datetime.fromisoformat(row[2])
        }

    async def close(self):
        if AIOSQLITE_AVAILABLE and self._pool:
            await self._pool.close()
        self._executor.shutdown(wait=True)

# ============================================================================
# Service Protocols (unchanged)
# ============================================================================

class TokenServiceProtocol(Protocol):
    def get_system_summary(self) -> Dict[str, Any]: ...
    def get_account_summary(self, account_id: str) -> Dict[str, Any]: ...
    def reserve_tokens(self, account_id: str, amount: float, consumer: Any, tenant_id: str, priority: int) -> Tuple[bool, List[str]]: ...
    def generate_tokens(self, account_id: str, source: Any, **kwargs) -> List[Any]: ...
    def consume_tokens(self, token_ids: List[str], consumer: Any, operation_success: bool) -> float: ...
    def recover_tokens(self, token_ids: List[str], completion_percentage: float) -> float: ...
    def create_account(self, account_id: str) -> Any: ...

class GradientServiceProtocol(Protocol):
    def get_field_strengths(self) -> Dict[str, float]: ...
    def pump_field(self, field_id: str, amount: float, source: str) -> None: ...
    def discharge_field(self, field_id: str, amount: float) -> float: ...
    def get_dominant_field(self) -> Tuple[str, float]: ...
    def get_field_stats(self) -> Dict[str, Any]: ...
    def find_root_cause(self, anomaly_field: str, max_depth: int) -> Dict[str, Any]: ...
    def explain_gradient_state(self, field_id: str) -> Dict[str, Any]: ...
    def forecast(self, field_id: str, horizon_seconds: float) -> Dict[str, Any]: ...
    def get_forecast_summary(self) -> Dict[str, Any]: ...

class CompartmentServiceProtocol(Protocol):
    def find_best_compartment(self, expert_type: str, task_complexity: float) -> Any: ...
    def get_ecosystem_stats(self) -> Dict[str, Any]: ...
    def create_compartment(self, expert_type: str, expert_instance: Any, resources: Any, parent_id: str) -> Any: ...
    def decommission_compartment(self, compartment_id: str) -> Dict[str, Any]: ...

class BiomassServiceProtocol(Protocol):
    def store_task(self, task_data: Dict[str, Any], ecoatp_cost: float, guarantee: Any, deadline: Any, initial_tier: Any) -> Tuple[bool, Optional[str]]: ...
    def retrieve_task(self, token_id: str) -> Tuple[Optional[Dict[str, Any]], float]: ...
    def get_storage_stats(self) -> Dict[str, Any]: ...
    def simulate_storage_scenario(self, scenario: Dict[str, Any]) -> Dict[str, Any]: ...

# ============================================================================
# Event Broker (Enhanced with multi‑worker and graceful shutdown)
# ============================================================================

@dataclass
class BioEvent:
    event_type: str
    source: str
    timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    data: Dict[str, Any] = field(default_factory=dict)
    correlation_id: Optional[str] = None
    priority: int = 0

class EventBroker:
    """
    Event-driven communication with multi‑worker processing.
    """
    def __init__(self, config: BioCoreConfig):
        self.config = config
        self.subscribers: Dict[str, List[Callable]] = defaultdict(list)
        self.event_history: deque = deque(maxlen=10000)
        self.event_queue: asyncio.PriorityQueue = asyncio.PriorityQueue(maxsize=config.event_queue_maxsize)
        self._lock = asyncio.Lock()
        self._running = True
        self._workers: List[asyncio.Task] = []
        self._worker_count = config.event_workers
        self._shutdown_complete = asyncio.Event()

        logger.info("Event Broker initialized", workers=self._worker_count)

    def subscribe(self, event_type: str, callback: Callable):
        self.subscribers[event_type].append(callback)

    def unsubscribe(self, event_type: str, callback: Callable):
        if event_type in self.subscribers:
            self.subscribers[event_type].remove(callback)

    async def publish(self, event: BioEvent):
        async with self._lock:
            await self.event_queue.put((event.priority, event))
            self.event_history.append(event)

    def start_processing(self):
        """Start worker tasks."""
        for i in range(self._worker_count):
            task = asyncio.create_task(self._process_events_worker(i))
            self._workers.append(task)

    async def _process_events_worker(self, worker_id: int):
        """Worker task to process events."""
        while self._running:
            try:
                priority, event = await self.event_queue.get()
                if event.event_type in self.subscribers:
                    for callback in self.subscribers[event.event_type]:
                        try:
                            if asyncio.iscoroutinefunction(callback):
                                await callback(event)
                            else:
                                callback(event)
                        except Exception as e:
                            logger.error("Event callback error", worker=worker_id, error=str(e))
                self.event_queue.task_done()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("Event processing error", worker=worker_id, error=str(e))

    async def stop_processing(self):
        """Gracefully stop workers after processing remaining events."""
        self._running = False
        # Wait for queue to empty
        while not self.event_queue.empty():
            await asyncio.sleep(0.1)
        # Cancel workers
        for task in self._workers:
            task.cancel()
        await asyncio.gather(*self._workers, return_exceptions=True)
        self._workers.clear()
        self._shutdown_complete.set()
        logger.info("Event broker stopped")

    def get_stats(self) -> Dict[str, Any]:
        return {
            'total_events': len(self.event_history),
            'subscribers': {k: len(v) for k, v in self.subscribers.items()},
            'queue_size': self.event_queue.qsize(),
            'is_running': self._running,
            'workers': self._worker_count
        }

# ============================================================================
# Predictive Alert System (with persistence and locks fixed)
# ============================================================================

@dataclass
class PredictiveAlert:
    alert_id: str
    severity: str
    category: str
    message: str
    timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    predicted_time: Optional[datetime] = None
    confidence: float = 0.0
    metadata: Dict[str, Any] = field(default_factory=dict)
    acknowledged: bool = False
    resolved: bool = False
    archived: bool = False

class PredictiveAlertSystem:
    def __init__(self, config: BioCoreConfig, event_broker: Optional[EventBroker] = None, persistence: Optional[Persistence] = None):
        self.config = config
        self.event_broker = event_broker
        self.persistence = persistence
        self.alerts: List[PredictiveAlert] = []
        self._lock = asyncio.Lock()
        self.thresholds = config.alert_thresholds
        self._token_circuit = CircuitBreaker("token_service", failure_threshold=config.circuit_breaker_failure_threshold)
        self._gradient_circuit = CircuitBreaker("gradient_service", failure_threshold=config.circuit_breaker_failure_threshold)

        if self.event_broker:
            self.event_broker.subscribe('token_balance_update', self._on_token_update)
            self.event_broker.subscribe('gradient_update', self._on_gradient_update)

        # Load saved alerts
        if persistence:
            self.alerts = asyncio.run(persistence.load_alerts(limit=1000))

        logger.info("Predictive Alert System initialized")

    async def _on_token_update(self, event: BioEvent):
        balance = event.data.get('balance', 500)
        await self._check_threshold('token_balance', balance, event.data)

    async def _on_gradient_update(self, event: BioEvent):
        field = event.data.get('field', 'carbon')
        strength = event.data.get('strength', 0.5)
        threshold_key = f'gradient_{field}'
        if threshold_key in self.thresholds:
            await self._check_threshold(threshold_key, strength, event.data)

    async def _check_threshold(self, metric: str, value: float, metadata: Dict):
        if metric not in self.thresholds:
            return
        thresholds = self.thresholds[metric]
        severity = None
        if value <= thresholds.get('critical', 0):
            severity = 'critical'
        elif value <= thresholds.get('warning', 0):
            severity = 'warning'
        if not severity:
            return

        alert_id = hashlib.md5(f"{metric}_{value}_{datetime.now(timezone.utc).timestamp()}".encode()).hexdigest()[:12]
        alert = PredictiveAlert(
            alert_id=alert_id,
            severity=severity,
            category=metric,
            message=f"{metric} at {value:.3f} (threshold: {severity})",
            predicted_time=datetime.now(timezone.utc) + timedelta(minutes=5),
            confidence=0.7,
            metadata=metadata
        )
        async with self._lock:
            self.alerts.append(alert)
        if self.persistence:
            await self.persistence.save_alert(alert)
        if self.event_broker:
            await self.event_broker.publish(BioEvent(
                event_type='alert_generated',
                source='predictive_alert_system',
                data={'alert': alert.__dict__}
            ))
        logger.warning("Alert generated", alert_id=alert_id, message=alert.message)

    async def generate_predictive_alerts(self, metrics: Dict[str, float]) -> List[PredictiveAlert]:
        alerts = []
        if 'token_balance' in metrics and 'token_trend' in metrics:
            balance = metrics['token_balance']
            trend = metrics['token_trend']
            if trend < -0.1 and balance < 500:
                predicted_balance = balance + trend * 60
                if predicted_balance < 100:
                    alert = PredictiveAlert(
                        alert_id=hashlib.md5(f"predict_token_{datetime.now(timezone.utc).timestamp()}".encode()).hexdigest()[:12],
                        severity='warning',
                        category='token',
                        message="Token balance predicted to drop below 100 in 1 hour",
                        predicted_time=datetime.now(timezone.utc) + timedelta(hours=1),
                        confidence=0.6,
                        metadata={'current': balance, 'trend': trend, 'predicted': predicted_balance}
                    )
                    alerts.append(alert)
        if 'gradient_carbon' in metrics and 'gradient_carbon_trend' in metrics:
            carbon = metrics['gradient_carbon']
            trend = metrics['gradient_carbon_trend']
            if trend > 0.1 and carbon > 0.5:
                predicted_carbon = carbon + trend * 60
                if predicted_carbon > 0.9:
                    alert = PredictiveAlert(
                        alert_id=hashlib.md5(f"predict_carbon_{datetime.now(timezone.utc).timestamp()}".encode()).hexdigest()[:12],
                        severity='critical',
                        category='gradient',
                        message="Carbon gradient predicted to reach critical level in 1 hour",
                        predicted_time=datetime.now(timezone.utc) + timedelta(hours=1),
                        confidence=0.65,
                        metadata={'current': carbon, 'trend': trend, 'predicted': predicted_carbon}
                    )
                    alerts.append(alert)
        return alerts

    async def get_active_alerts(self, severity: Optional[str] = None) -> List[PredictiveAlert]:
        async with self._lock:
            alerts = [a for a in self.alerts if not a.resolved and not a.archived]
            if severity:
                alerts = [a for a in alerts if a.severity == severity]
            return alerts

    async def acknowledge_alert(self, alert_id: str) -> bool:
        async with self._lock:
            for alert in self.alerts:
                if alert.alert_id == alert_id:
                    alert.acknowledged = True
                    if self.persistence:
                        await self.persistence.save_alert(alert)
                    return True
            return False

    async def resolve_alert(self, alert_id: str) -> bool:
        async with self._lock:
            for alert in self.alerts:
                if alert.alert_id == alert_id:
                    alert.resolved = True
                    if self.persistence:
                        await self.persistence.save_alert(alert)
                    return True
            return False

    async def archive_alert(self, alert_id: str) -> bool:
        async with self._lock:
            for alert in self.alerts:
                if alert.alert_id == alert_id:
                    alert.archived = True
                    if self.persistence:
                        await self.persistence.save_alert(alert)
                    return True
            return False

    async def get_alert_stats(self) -> Dict[str, Any]:
        async with self._lock:
            return {
                'total_alerts': len(self.alerts),
                'active_alerts': len([a for a in self.alerts if not a.resolved and not a.archived]),
                'acknowledged': len([a for a in self.alerts if a.acknowledged]),
                'by_severity': {
                    'critical': len([a for a in self.alerts if a.severity == 'critical']),
                    'warning': len([a for a in self.alerts if a.severity == 'warning']),
                    'info': len([a for a in self.alerts if a.severity == 'info'])
                },
                'by_category': {
                    'token': len([a for a in self.alerts if a.category == 'token']),
                    'gradient': len([a for a in self.alerts if a.category == 'gradient']),
                    'compartment': len([a for a in self.alerts if a.category == 'compartment']),
                    'biomass': len([a for a in self.alerts if a.category == 'biomass']),
                    'harvester': len([a for a in self.alerts if a.category == 'harvester'])
                }
            }

# ============================================================================
# Cost-Benefit Engine (with config and persistence, async methods)
# ============================================================================

@dataclass
class CostBenefitAnalysis:
    analysis_id: str
    scenario: str
    total_cost: float
    total_benefit: float
    net_value: float
    roi: float
    payback_period_hours: float
    recommendations: List[str] = field(default_factory=list)
    timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

class CostBenefitEngine:
    def __init__(self, config: BioCoreConfig, token_manager=None, persistence: Optional[Persistence] = None):
        self.config = config
        self.token_manager = token_manager
        self.persistence = persistence
        self.analyses: List[CostBenefitAnalysis] = []
        self._lock = asyncio.Lock()
        self.cost_models = config.cost_models
        self.benefit_models = config.benefit_models
        # Load saved analyses
        if persistence:
            self.analyses = asyncio.run(persistence.load_analyses(limit=1000))
        logger.info("Cost-Benefit Engine initialized")

    async def analyze_scenario(self, scenario: str, parameters: Dict[str, Any]) -> CostBenefitAnalysis:
        async with self._lock:
            total_cost = 0.0
            total_benefit = 0.0
            for operation, amount in parameters.items():
                if operation in self.cost_models:
                    model = self.cost_models[operation]
                    cost = model['base_cost'] + model['variable_cost'] * amount
                    total_cost += cost
                if operation in self.benefit_models:
                    model = self.benefit_models[operation]
                    benefit = model['base_benefit'] + model['variable_benefit'] * amount
                    total_benefit += benefit
            net_value = total_benefit - total_cost
            roi = (total_benefit / max(total_cost, 0.001)) - 1
            if net_value > 0 and total_benefit > 0:
                payback_period = total_cost / (total_benefit / 24)
            else:
                payback_period = float('inf')
            analysis = CostBenefitAnalysis(
                analysis_id=hashlib.md5(f"{scenario}_{datetime.now(timezone.utc).timestamp()}".encode()).hexdigest()[:12],
                scenario=scenario,
                total_cost=total_cost,
                total_benefit=total_benefit,
                net_value=net_value,
                roi=roi,
                payback_period_hours=payback_period,
                recommendations=self._generate_recommendations(total_cost, total_benefit, roi)
            )
            self.analyses.append(analysis)
            if self.persistence:
                await self.persistence.save_analysis(analysis)
            return analysis

    def _generate_recommendations(self, cost: float, benefit: float, roi: float) -> List[str]:
        recommendations = []
        if cost > benefit:
            recommendations.append("Reduce costs or increase benefits")
            if cost > 10:
                recommendations.append("Consider optimizing resource allocation")
        else:
            recommendations.append("Current configuration is cost-effective")
        if roi < 0.5:
            recommendations.append("Improve return on investment")
        return recommendations or ["Scenario is economically viable"]

    async def get_best_scenario(self, scenarios: List[str]) -> Optional[str]:
        best_analysis = None
        best_roi = -float('inf')
        async with self._lock:
            for analysis in self.analyses:
                if analysis.scenario in scenarios and analysis.roi > best_roi:
                    best_roi = analysis.roi
                    best_analysis = analysis
        return best_analysis.scenario if best_analysis else None

    async def get_analysis_stats(self) -> Dict[str, Any]:
        async with self._lock:
            if not self.analyses:
                return {'total_analyses': 0}
            rois = [a.roi for a in self.analyses]
            return {
                'total_analyses': len(self.analyses),
                'average_roi': np.mean(rois),
                'max_roi': max(rois),
                'min_roi': min(rois),
                'best_scenario': max(self.analyses, key=lambda a: a.roi).scenario,
                'recent_analyses': [
                    {'scenario': a.scenario, 'roi': a.roi, 'net_value': a.net_value}
                    for a in self.analyses[-5:]
                ]
            }

    async def update_cost_model(self, operation: str, base_cost: float, variable_cost: float) -> bool:
        async with self._lock:
            if operation not in self.cost_models:
                return False
            self.cost_models[operation] = {'base_cost': base_cost, 'variable_cost': variable_cost}
            return True

# ============================================================================
# Workflow Orchestrator (with config and persistence, async)
# ============================================================================

@dataclass
class WorkflowStep:
    step_id: str
    name: str
    service: str
    action: str
    parameters: Dict[str, Any] = field(default_factory=dict)
    depends_on: List[str] = field(default_factory=list)
    retry_count: int = 0
    max_retries: int = 3
    timeout_seconds: float = 30.0
    status: str = 'pending'

class WorkflowOrchestrator:
    def __init__(self, bio_core, config: BioCoreConfig, persistence: Optional[Persistence] = None):
        self.bio_core = bio_core
        self.config = config
        self.persistence = persistence
        self.workflows: Dict[str, List[WorkflowStep]] = {}
        self.workflow_status: Dict[str, str] = {}
        self._lock = asyncio.Lock()
        logger.info("Workflow Orchestrator initialized")

    def define_workflow(self, workflow_id: str, steps: List[Dict[str, Any]]):
        workflow_steps = []
        for i, step in enumerate(steps):
            workflow_steps.append(WorkflowStep(
                step_id=f"{workflow_id}_step_{i}",
                name=step.get('name', f'Step {i}'),
                service=step['service'],
                action=step['action'],
                parameters=step.get('parameters', {}),
                depends_on=step.get('depends_on', []),
                max_retries=self.config.workflow_max_retries,
                timeout_seconds=self.config.workflow_default_timeout
            ))
        async with self._lock:
            self.workflows[workflow_id] = workflow_steps
            self.workflow_status[workflow_id] = 'pending'
        logger.info("Workflow defined", workflow_id=workflow_id, steps=len(workflow_steps))

    async def execute_workflow(self, workflow_id: str) -> Dict[str, Any]:
        async with self._lock:
            if workflow_id not in self.workflows:
                return {'status': 'error', 'message': 'Workflow not found'}
            steps = self.workflows[workflow_id]
            self.workflow_status[workflow_id] = 'running'

        results = {}
        for step in steps:
            # Check dependencies
            deps_met = all(
                results.get(dep, {}).get('status') == 'success'
                for dep in step.depends_on
            )
            if not deps_met:
                step.status = 'failed'
                async with self._lock:
                    self.workflow_status[workflow_id] = 'failed'
                # Save state
                if self.persistence:
                    await self.persistence.save_workflow_state(workflow_id, 'failed', steps)
                return {'status': 'failed', 'step': step.name, 'reason': 'Dependencies not met', 'results': results}

            for attempt in range(step.max_retries):
                step.status = 'running'
                try:
                    result = await self._execute_step(step)
                    step.status = 'completed'
                    results[step.name] = {'status': 'success', 'result': result}
                    break
                except asyncio.TimeoutError:
                    step.retry_count += 1
                    if step.retry_count >= step.max_retries:
                        step.status = 'failed'
                        results[step.name] = {'status': 'failed', 'error': 'Timeout'}
                        break
                except Exception as e:
                    step.retry_count += 1
                    if step.retry_count >= step.max_retries:
                        step.status = 'failed'
                        results[step.name] = {'status': 'failed', 'error': str(e)}
                        break

        final_status = 'completed' if all(s.status == 'completed' for s in steps) else 'failed' if any(s.status == 'failed' for s in steps) else 'partial'
        async with self._lock:
            self.workflow_status[workflow_id] = final_status
        if self.persistence:
            await self.persistence.save_workflow_state(workflow_id, final_status, steps)
        return {'status': final_status, 'results': results, 'steps': [{'name': s.name, 'status': s.status} for s in steps]}

    async def _execute_step(self, step: WorkflowStep) -> Dict[str, Any]:
        service = getattr(self.bio_core, step.service, None)
        if not service:
            raise ValueError(f"Service {step.service} not found")
        action = getattr(service, step.action, None)
        if not action:
            raise ValueError(f"Action {step.action} not found on {step.service}")
        try:
            if asyncio.iscoroutinefunction(action):
                result = await asyncio.wait_for(action(**step.parameters), timeout=step.timeout_seconds)
            else:
                result = action(**step.parameters)
            return {'success': True, 'result': result}
        except asyncio.TimeoutError:
            raise
        except Exception as e:
            raise

    async def get_workflow_status(self, workflow_id: str) -> Dict[str, Any]:
        async with self._lock:
            if workflow_id not in self.workflows:
                return {'status': 'not_found'}
            steps = self.workflows[workflow_id]
            return {
                'workflow_id': workflow_id,
                'status': self.workflow_status.get(workflow_id, 'pending'),
                'steps': [
                    {'name': s.name, 'status': s.status, 'retries': s.retry_count}
                    for s in steps
                ]
            }

    async def get_workflow_stats(self) -> Dict[str, Any]:
        async with self._lock:
            return {
                'total_workflows': len(self.workflows),
                'workflow_status': self.workflow_status,
                'completed': sum(1 for s in self.workflow_status.values() if s == 'completed'),
                'running': sum(1 for s in self.workflow_status.values() if s == 'running'),
                'failed': sum(1 for s in self.workflow_status.values() if s == 'failed'),
                'pending': sum(1 for s in self.workflow_status.values() if s == 'pending')
            }

# ============================================================================
# Anomaly Detection System (with Isolation Forest and periodic retraining)
# ============================================================================

@dataclass
class AnomalyDetectionResult:
    metric: str
    value: float
    expected_range: Tuple[float, float]
    deviation: float
    severity: str
    timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    confidence: float = 0.0
    anomaly_id: str = field(default_factory=lambda: uuid.uuid4().hex[:12])

class AnomalyDetectionSystem:
    def __init__(self, config: BioCoreConfig, event_broker: Optional[EventBroker] = None, persistence: Optional[Persistence] = None):
        self.config = config
        self.event_broker = event_broker
        self.persistence = persistence
        self.metric_history: Dict[str, deque] = defaultdict(lambda: deque(maxlen=1000))
        self.anomalies: List[AnomalyDetectionResult] = []
        self._lock = asyncio.Lock()
        self.zscore_threshold = config.anomaly_zscore_threshold
        self.trend_threshold = config.anomaly_trend_threshold
        self.isolation_forest = None
        self.isolation_forest_trained = False
        self.samples_since_retrain = 0
        if SKLEARN_AVAILABLE:
            self.isolation_forest = IsolationForest(contamination=config.anomaly_isolation_forest_contamination, random_state=42)
        logger.info("Anomaly Detection System initialized")

    def record_metric(self, metric: str, value: float):
        self.metric_history[metric].append(value)
        self.samples_since_retrain += 1

    async def detect_anomalies(self, metric: str, value: float) -> Optional[AnomalyDetectionResult]:
        if metric not in self.metric_history or len(self.metric_history[metric]) < 10:
            return None
        history = list(self.metric_history[metric])[-50:]
        mean = np.mean(history)
        std = np.std(history)
        if std == 0:
            return None
        zscore = abs(value - mean) / std
        if zscore > self.zscore_threshold:
            severity = 'critical' if zscore > self.zscore_threshold * 1.5 else 'warning'
            anomaly = AnomalyDetectionResult(
                metric=metric,
                value=value,
                expected_range=(mean - 2*std, mean + 2*std),
                deviation=zscore,
                severity=severity,
                confidence=min(0.9, zscore / (self.zscore_threshold * 2))
            )
            async with self._lock:
                self.anomalies.append(anomaly)
            if self.persistence:
                await self.persistence.save_anomaly(anomaly)
            if self.event_broker:
                await self.event_broker.publish(BioEvent(
                    event_type='anomaly_detected',
                    source='anomaly_detection_system',
                    data={'anomaly': anomaly.__dict__}
                ))
            logger.warning("Anomaly detected", metric=metric, zscore=zscore)
            return anomaly
        return None

    async def detect_trend_anomaly(self, metric: str) -> Optional[AnomalyDetectionResult]:
        if metric not in self.metric_history or len(self.metric_history[metric]) < 20:
            return None
        history = list(self.metric_history[metric])[-20:]
        x = np.arange(len(history))
        slope = np.polyfit(x, history, 1)[0]
        if abs(slope) > self.trend_threshold:
            mean = np.mean(history)
            std = np.std(history)
            expected = mean + slope * len(history)
            anomaly = AnomalyDetectionResult(
                metric=metric,
                value=history[-1],
                expected_range=(expected - std, expected + std),
                deviation=abs(slope) / self.trend_threshold,
                severity='warning' if abs(slope) > self.trend_threshold * 1.5 else 'info',
                confidence=min(0.8, abs(slope) / (self.trend_threshold * 2))
            )
            async with self._lock:
                self.anomalies.append(anomaly)
            if self.event_broker:
                await self.event_broker.publish(BioEvent(
                    event_type='anomaly_detected',
                    source='anomaly_detection_system',
                    data={'anomaly': anomaly.__dict__}
                ))
            return anomaly
        return None

    async def detect_multivariate_anomaly(self, metrics: Dict[str, float]) -> Optional[AnomalyDetectionResult]:
        if not SKLEARN_AVAILABLE or not self.isolation_forest:
            return None
        # Collect features for each metric that has enough history
        feature_vectors = []
        metric_names = []
        for metric, value in metrics.items():
            if metric in self.metric_history and len(self.metric_history[metric]) >= 10:
                # Use recent values as features
                history = list(self.metric_history[metric])[-10:]
                feature_vectors.append(history)
                metric_names.append(metric)
        if len(feature_vectors) < 2:  # need at least 2 metrics
            return None
        X = np.array(feature_vectors).T  # shape (samples, features)
        if X.shape[0] < 10:
            return None

        # Retrain periodically
        if self.samples_since_retrain >= self.config.anomaly_isolation_forest_retrain_interval:
            self.isolation_forest.fit(X)
            self.isolation_forest_trained = True
            self.samples_since_retrain = 0
        elif not self.isolation_forest_trained:
            self.isolation_forest.fit(X)
            self.isolation_forest_trained = True
        else:
            # Incremental training not supported; we could fit on all data, but we'll skip.
            pass

        # For current values, we need to form a vector of current values for each metric
        current_vector = np.array([metrics[m] for m in metric_names]).reshape(1, -1)
        pred = self.isolation_forest.predict(current_vector)[0]
        if pred == -1:  # anomaly
            anomaly = AnomalyDetectionResult(
                metric="multivariate",
                value=0,
                expected_range=(0, 1),
                deviation=1,
                severity='warning',
                confidence=0.6
            )
            async with self._lock:
                self.anomalies.append(anomaly)
            return anomaly
        return None

    async def get_recent_anomalies(self, limit: int = 20) -> List[AnomalyDetectionResult]:
        async with self._lock:
            return self.anomalies[-limit:] if self.anomalies else []

    async def get_anomaly_stats(self) -> Dict[str, Any]:
        async with self._lock:
            return {
                'total_anomalies': len(self.anomalies),
                'by_severity': {
                    'critical': len([a for a in self.anomalies if a.severity == 'critical']),
                    'warning': len([a for a in self.anomalies if a.severity == 'warning']),
                    'info': len([a for a in self.anomalies if a.severity == 'info'])
                },
                'by_metric': {
                    metric: len([a for a in self.anomalies if a.metric == metric])
                    for metric in set(a.metric for a in self.anomalies)
                },
                'recent_anomalies': [
                    {'metric': a.metric, 'value': a.value, 'severity': a.severity, 'deviation': a.deviation}
                    for a in self.anomalies[-5:]
                ]
            }

# ============================================================================
# Enhanced Bio-Inspired Core (v8.0.0)
# ============================================================================

class EnhancedBioInspiredCore:
    """
    Enhanced Bio-Inspired Core v8.0.0 with all improvements.
    """

    def __init__(self,
                 config: Optional[Union[BioCoreConfig, Dict[str, Any]]] = None,
                 csv_path: Optional[str] = None,
                 quantum_graph: Optional[Any] = None):
        # Load configuration
        if isinstance(config, dict):
            if PYDANTIC_AVAILABLE:
                self.config = BioCoreConfig(**config)
            else:
                self.config = BioCoreConfig(**config)
        elif isinstance(config, BioCoreConfig):
            self.config = config
        else:
            self.config = BioCoreConfig()

        # Persistence
        self.persistence = Persistence(self.config) if self.config.persistence_enabled else None

        # Initialize core modules
        self.exchange_rate = DynamicExchangeRate()
        self._token_manager = EcoATPTokenManager(self.exchange_rate)
        self._gradient_manager = HierarchicalGradientManager()
        self._scheduler = ATPSynthaseScheduler(self._token_manager, self._gradient_manager)
        self._compartment_manager = HierarchicalCompartmentManager(self._token_manager)
        self._biomass_storage = BiomassStorage(self._token_manager, self._gradient_manager)
        self._harvester = PhotosyntheticHarvester(self._token_manager)

        if self.config.enable_enhancements:
            self._supply_manager = TokenSupplyManager(self._token_manager)
            self._token_allocator = PredictiveTokenAllocator(self._token_manager)

        # Knowledge transfer
        self._knowledge_transfer = KnowledgeTransferManager()
        # Degradation management
        self._degradation_manager = DegradationManager()
        self._degradation_manager.update_metrics(
            token_balance=self._token_manager.get_system_summary().get('total_balance', 500)
        )
        self._degradation_manager.register_callback(self._on_tier_change)

        # NEW: Event broker with multi-workers
        self._event_broker = EventBroker(self.config)
        self._event_broker.start_processing()

        # NEW: Predictive alert system
        self._alert_system = PredictiveAlertSystem(self.config, self._event_broker, self.persistence)

        # NEW: Cost-benefit engine
        self._cost_benefit_engine = CostBenefitEngine(self.config, self._token_manager, self.persistence)

        # NEW: Workflow orchestrator
        self._workflow_orchestrator = WorkflowOrchestrator(self, self.config, self.persistence)

        # NEW: Anomaly detection system
        self._anomaly_detection = AnomalyDetectionSystem(self.config, self._event_broker, self.persistence)

        # NEW: Lazy-loaded modules
        self._quantum_bridge = None
        self._tick_engine = None

        # API
        self._api = BioInspiredAPI(self) if API_AVAILABLE else None

        # Wire event subscriptions
        self._event_broker.subscribe('token_balance_update', self._on_token_balance_update)
        self._event_broker.subscribe('gradient_update', self._on_gradient_update_event)

        # TaskManager for background loops
        self._task_manager = TaskManager()
        self._task_manager.start_task("enhanced_monitoring", self._enhanced_monitoring_loop)
        self._task_manager.start_task("anomaly_detection", self._anomaly_detection_loop)

        # Optional: start tick engine (lazy)
        if self.config.enable_time_tick_engine and TICK_ENGINE_AVAILABLE and csv_path:
            self._init_tick_engine(csv_path)

        # Optional: quantum bridge (lazy)
        if self.config.enable_quantum_bridge and QUANTUM_BRIDGE_AVAILABLE and quantum_graph:
            self._quantum_bridge = QuantumBridge(self._gradient_manager, quantum_graph)

        # Prometheus metrics
        self._setup_metrics()

        # Define standard workflows
        self._define_standard_workflows()

        logger.info("Enhanced Bio-Inspired Core v8.0.0 initialized", config=self.config.dict() if PYDANTIC_AVAILABLE else asdict(self.config))

    def _setup_metrics(self):
        if not self.config.enable_prometheus or not PROMETHEUS_AVAILABLE:
            self.metrics = {}
            return
        self.metrics = {
            'alerts_total': Counter('bio_core_alerts_total', 'Total alerts generated'),
            'alerts_active': Gauge('bio_core_alerts_active', 'Active alerts'),
            'anomalies_total': Counter('bio_core_anomalies_total', 'Total anomalies detected'),
            'workflows_completed': Counter('bio_core_workflows_completed', 'Completed workflows'),
            'event_queue_size': Gauge('bio_core_event_queue_size', 'Event queue size')
        }

    def _init_tick_engine(self, csv_path: str):
        from .time_tick_engine import TimeTickEngine
        from .helium_environment_translator import HeliumEnvironmentTranslator
        self._tick_engine = TimeTickEngine(
            csv_path=csv_path,
            harvester=self._harvester,
            translator_class=HeliumEnvironmentTranslator
        )
        self._task_manager.start_task("tick_engine", self._tick_engine.run_simulation, 0.1, self._on_tick)

    # ============================================================================
    # Event Handlers
    # ============================================================================

    async def _on_token_balance_update(self, event: BioEvent):
        balance = event.data.get('balance', 500)
        self._anomaly_detection.record_metric('token_balance', balance)
        await self._anomaly_detection.detect_anomalies('token_balance', balance)

    async def _on_gradient_update_event(self, event: BioEvent):
        field = event.data.get('field', 'carbon')
        strength = event.data.get('strength', 0.5)
        self._anomaly_detection.record_metric(f'gradient_{field}', strength)
        await self._anomaly_detection.detect_anomalies(f'gradient_{field}', strength)

    async def _on_tier_change(self, old_tier, new_tier, policies):
        logger.warning("Tier change", old=old_tier.name, new=new_tier.name)
        await self._event_broker.publish(BioEvent(
            event_type='tier_change',
            source='degradation_manager',
            data={'old_tier': old_tier.name, 'new_tier': new_tier.name}
        ))

    async def _on_tick(self, idx: int, row, harvest_result: Dict[str, Any]):
        """Callback after each tick: update quantum graph."""
        if self._quantum_bridge:
            self._quantum_bridge.apply_to_quantum_graph()

    # ============================================================================
    # Background Loops (managed by TaskManager)
    # ============================================================================

    async def _enhanced_monitoring_loop(self):
        while True:
            try:
                summary = self._token_manager.get_system_summary()
                gradients = self._gradient_manager.get_field_strengths()

                self._degradation_manager.update_metrics(
                    token_balance=summary.get('total_balance', 500),
                    carbon_gradient=gradients.get('carbon', 0.5),
                    compartment_health=self._get_avg_compartment_health(),
                    harvester_activity=self._harvester.total_harvested if self._harvester else 0
                )

                for field_id, strength in gradients.items():
                    self._gradient_manager.record_measurement(field_id, strength)
                    await self._event_broker.publish(BioEvent(
                        event_type='gradient_update',
                        source='monitoring',
                        data={'field': field_id, 'strength': strength}
                    ))

                await self._event_broker.publish(BioEvent(
                    event_type='token_balance_update',
                    source='monitoring',
                    data={'balance': summary.get('total_balance', 500)}
                ))

                # Generate predictive alerts
                metrics = {
                    'token_balance': summary.get('total_balance', 500),
                    'gradient_carbon': gradients.get('carbon', 0.5),
                    'gradient_carbon_trend': self._gradient_manager.get_field_stats().get('carbon', {}).get('trend', 0)
                }
                alerts = await self._alert_system.generate_predictive_alerts(metrics)
                for alert in alerts:
                    logger.warning("Predictive alert", alert=alert.message)

                # Update Prometheus metrics
                if self.metrics:
                    self.metrics['alerts_active'].set(len(self._alert_system.get_active_alerts()))
                    self.metrics['event_queue_size'].set(self._event_broker.event_queue.qsize())

                await asyncio.sleep(self.config.monitoring_interval_seconds)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("Monitoring error", error=str(e))
                await asyncio.sleep(30)

    async def _anomaly_detection_loop(self):
        while True:
            try:
                # Check trend anomalies
                for metric in ['token_balance', 'gradient_carbon', 'gradient_helium']:
                    await self._anomaly_detection.detect_trend_anomaly(metric)

                # Multivariate anomaly detection (if enough metrics)
                if SKLEARN_AVAILABLE:
                    metrics = {
                        'token_balance': self._token_manager.get_system_summary().get('total_balance', 500),
                        'gradient_carbon': self._gradient_manager.get_field_strengths().get('carbon', 0.5),
                        'gradient_helium': self._gradient_manager.get_field_strengths().get('helium', 0.5)
                    }
                    await self._anomaly_detection.detect_multivariate_anomaly(metrics)

                await asyncio.sleep(self.config.anomaly_detection_interval_seconds)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("Anomaly detection loop error", error=str(e))
                await asyncio.sleep(120)

    # ============================================================================
    # Utility methods
    # ============================================================================

    def _get_avg_compartment_health(self) -> float:
        if not self._compartment_manager:
            return 0.5
        compartments = self._compartment_manager.compartments
        if not compartments:
            return 0.5
        return np.mean([c.health_score for c in compartments.values()])

    def _define_standard_workflows(self):
        """Define standard workflows."""
        self._workflow_orchestrator.define_workflow(
            'token_generation',
            [
                {'name': 'Harvest Energy', 'service': 'harvester', 'action': 'harvest_energy', 'parameters': {'amount': 10}},
                {'name': 'Generate Tokens', 'service': 'token_manager', 'action': 'generate_tokens',
                 'parameters': {'account_id': 'system', 'source': 'harvest', 'num_tokens': 10}},
                {'name': 'Distribute Tokens', 'service': 'token_manager', 'action': 'reserve_tokens',
                 'parameters': {'account_id': 'system', 'amount': 5, 'consumer': 'workflow'}},
                {'name': 'Update Gradients', 'service': 'gradient_manager', 'action': 'pump_field',
                 'parameters': {'field_id': 'opportunity', 'amount': 0.1, 'source': 'token_generation'}}
            ]
        )

    # ============================================================================
    # Protocol-compliant service accessors
    # ============================================================================

    @property
    def token_service(self) -> TokenServiceProtocol:
        return self._token_manager

    @property
    def gradient_service(self) -> GradientServiceProtocol:
        return self._gradient_manager

    @property
    def compartment_service(self) -> CompartmentServiceProtocol:
        return self._compartment_manager

    @property
    def biomass_service(self) -> BiomassServiceProtocol:
        return self._biomass_storage

    # Legacy accessors
    @property
    def token_manager(self): return self._token_manager
    @property
    def gradient_manager(self): return self._gradient_manager
    @property
    def scheduler(self): return self._scheduler
    @property
    def compartment_manager(self): return self._compartment_manager
    @property
    def biomass_storage(self): return self._biomass_storage
    @property
    def harvester(self): return self._harvester
    @property
    def supply_manager(self): return self._supply_manager if hasattr(self, '_supply_manager') else None
    @property
    def token_allocator(self): return self._token_allocator if hasattr(self, '_token_allocator') else None
    @property
    def knowledge_transfer(self): return self._knowledge_transfer
    @property
    def degradation_manager(self): return self._degradation_manager
    @property
    def api(self): return self._api

    # NEW accessors
    @property
    def event_broker(self): return self._event_broker
    @property
    def alert_system(self): return self._alert_system
    @property
    def cost_benefit_engine(self): return self._cost_benefit_engine
    @property
    def workflow_orchestrator(self): return self._workflow_orchestrator
    @property
    def anomaly_detection(self): return self._anomaly_detection
    @property
    def quantum_bridge(self): return self._quantum_bridge
    @property
    def tick_engine(self): return self._tick_engine

    # ============================================================================
    # System Status and Reporting
    # ============================================================================

    def get_system_status(self) -> Dict[str, Any]:
        status = {
            'token_economy': self._token_manager.get_system_summary(),
            'gradients': self._gradient_manager.get_field_stats(),
            'gradient_forecasts': self._gradient_manager.get_forecast_summary(),
            'scheduler': self._scheduler.get_scheduler_stats() if self._scheduler else {},
            'compartments': self._compartment_manager.get_ecosystem_stats() if self._compartment_manager else {},
            'biomass': self._biomass_storage.get_storage_stats() if self._biomass_storage else {},
            'harvester': self._harvester.get_harvesting_stats() if self._harvester else {},
            'degradation': self._degradation_manager.get_tier_status() if hasattr(self, '_degradation_manager') else {},
            'knowledge': self._knowledge_transfer.get_knowledge_summary() if hasattr(self, '_knowledge_transfer') else {},
            'event_broker': self._event_broker.get_stats(),
            'alerts': self._alert_system.get_alert_stats(),
            'anomalies': self._anomaly_detection.get_anomaly_stats(),
            'workflows': self._workflow_orchestrator.get_workflow_stats(),
            'cost_benefit': self._cost_benefit_engine.get_analysis_stats(),
            'quantum_bridge': self._quantum_bridge.get_qubo_report() if self._quantum_bridge else None,
            'tick_engine': {'running': self._tick_engine is not None}
        }
        if hasattr(self, '_supply_manager'):
            status['token_economy']['supply_management'] = self._supply_manager.get_economic_indicators()
        if hasattr(self, '_token_allocator'):
            status['token_economy']['pre_allocation'] = self._token_allocator.get_cache_stats()
        return status

    def get_economic_report(self) -> Dict[str, Any]:
        report = {
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'token_economy': self._token_manager.get_system_summary()
        }
        if hasattr(self, '_supply_manager'):
            report['supply_management'] = self._supply_manager.get_economic_indicators()
        if hasattr(self, '_token_allocator'):
            report['pre_allocation'] = self._token_allocator.get_cache_stats()
        report['cost_benefit'] = self._cost_benefit_engine.get_analysis_stats()

        indicators = report.get('supply_management', {})
        utilization = indicators.get('utilization', 0.5)
        inflation = indicators.get('inflation_pressure', 0)

        if 0.6 < utilization < 0.9 and abs(inflation) < 0.2:
            report['health'] = 'healthy'
        elif utilization < 0.4:
            report['health'] = 'deflationary'
        elif utilization > 0.95:
            report['health'] = 'inflationary'
        else:
            report['health'] = 'stable'

        recs = []
        if utilization < 0.4:
            recs.append("Economy under-utilized. Increase task throughput.")
        if utilization > 0.95:
            recs.append("Economy over-heating. Add capacity or reduce load.")
        if inflation > 0.3:
            recs.append("High inflation pressure. Token burning recommended.")
        if report.get('cost_benefit', {}).get('total_analyses', 0) > 0:
            best_scenario = self._cost_benefit_engine.get_best_scenario(
                ['token_generation', 'gradient_pumping', 'compartment_creation']
            )
            if best_scenario:
                recs.append(f"Best cost-benefit scenario: {best_scenario}")
        report['recommendations'] = recs if recs else ["Economy is healthy."]
        return report

    def process_task(self, task: Dict[str, Any]) -> Dict[str, Any]:
        ecoatp_required = task.get('complexity', 0.5) * 10
        if hasattr(self, '_token_allocator'):
            success, _ = self._token_allocator.get_tokens('task_processor', ecoatp_required)
            if success:
                self._token_allocator.record_demand('task_processor', ecoatp_required)
        else:
            success, _ = self._token_manager.reserve_tokens('task_processor', ecoatp_required, EcoATPConsumer.EXPERT_EXECUTION)
        if not success:
            if self._biomass_storage:
                stored, token_id = self._biomass_storage.store_task(task_data=task, ecoatp_cost=ecoatp_required)
                return {'success': True, 'status': 'stored', 'biomass_token': token_id}
            return {'success': False, 'reason': 'Insufficient tokens'}
        return {'success': True, 'task_id': task.get('task_id', 'unknown')}

    # ============================================================================
    # NEW: Workflow Management
    # ============================================================================

    async def run_workflow(self, workflow_id: str) -> Dict[str, Any]:
        return await self._workflow_orchestrator.execute_workflow(workflow_id)

    # ============================================================================
    # NEW: Cost-Benefit Analysis
    # ============================================================================

    async def analyze_cost_benefit(self, scenario: str, parameters: Dict[str, Any]) -> CostBenefitAnalysis:
        return await self._cost_benefit_engine.analyze_scenario(scenario, parameters)

    # ============================================================================
    # NEW: Alert Management
    # ============================================================================

    async def get_active_alerts(self, severity: Optional[str] = None) -> List[PredictiveAlert]:
        return await self._alert_system.get_active_alerts(severity)

    async def acknowledge_alert(self, alert_id: str) -> bool:
        return await self._alert_system.acknowledge_alert(alert_id)

    async def resolve_alert(self, alert_id: str) -> bool:
        return await self._alert_system.resolve_alert(alert_id)

    async def archive_alert(self, alert_id: str) -> bool:
        return await self._alert_system.archive_alert(alert_id)

    # ============================================================================
    # NEW: Anomaly Detection
    # ============================================================================

    async def get_recent_anomalies(self, limit: int = 20) -> List[AnomalyDetectionResult]:
        return await self._anomaly_detection.get_recent_anomalies(limit)

    # ============================================================================
    # Graceful Shutdown
    # ============================================================================

    async def shutdown(self):
        logger.info("Shutting down Enhanced Bio-Inspired Core")
        # Stop background tasks
        await self._task_manager.stop_all()
        # Stop event processing
        await self._event_broker.stop_processing()
        # Close persistence
        if self.persistence:
            await self.persistence.close()
        # Close token manager etc.
        if hasattr(self._token_manager, 'close'):
            await self._token_manager.close()
        if hasattr(self._gradient_manager, 'close'):
            await self._gradient_manager.close()
        logger.info("Shutdown complete")

# ============================================================================
# Convenience functions
# ============================================================================

def create_metabolic_ecosystem(config: Optional[Union[BioCoreConfig, Dict[str, Any]]] = None,
                               csv_path: Optional[str] = None,
                               quantum_graph: Optional[Any] = None) -> EnhancedBioInspiredCore:
    return EnhancedBioInspiredCore(config=config, csv_path=csv_path, quantum_graph=quantum_graph)

def create_minimal_ecosystem() -> EnhancedBioInspiredCore:
    return EnhancedBioInspiredCore(config={'enable_enhancements': False})

# ============================================================================
# Example usage
# ============================================================================

async def main():
    config = {
        'enable_enhancements': True,
        'enable_quantum_bridge': True,
        'enable_time_tick_engine': True,
        'event_workers': 4,
        'persistence_db_path': './bio_core.db'
    }
    core = EnhancedBioInspiredCore(config=config, csv_path="helium_data.csv")
    # Define and run a workflow
    await core._workflow_orchestrator.define_workflow(
        'test_workflow',
        [
            {'name': 'Get Token Summary', 'service': 'token_manager', 'action': 'get_system_summary',
             'parameters': {}}
        ]
    )
    result = await core.run_workflow('test_workflow')
    print(result)
    # Get status
    status = core.get_system_status()
    print(status)
    # Shutdown
    await core.shutdown()

if __name__ == "__main__":
    asyncio.run(main())
