# File: src/enhancements/control_system.py

"""
Complete Control System for Green Agent - Enhanced Version 8.0 (PLATINUM)

CRITICAL ENHANCEMENTS OVER v7.0:
1. ADDED: State persistence with Redis/SQLite for workflows and circuit breakers
2. ADDED: Distributed circuit breaker coordination across instances
3. ADDED: Configuration validation with Pydantic schemas
4. ADDED: WebSocket authentication with JWT
5. ADDED: Distributed task queue with Celery/RQ
6. ADDED: Configuration hot-reload with file watcher
7. ADDED: Graceful shutdown with task draining
8. ADDED: Detailed health check endpoint with component breakdown
9. ADDED: Metrics export to Prometheus Pushgateway
10. ADDED: Rate limiting for WebSocket connections
11. ADDED: API versioning support
12. ADDED: Request/response logging middleware
13. ADDED: Bulkhead pattern for task isolation
14. ADDED: Leader election for multi-instance deployments
15. ADDED: Audit log streaming to external systems
"""

import asyncio
import hashlib
import json
import logging
import os
import signal
import sys
import time
import uuid
import threading
import importlib
import inspect
import contextvars
from abc import ABC, abstractmethod
from collections import defaultdict, deque
from contextlib import asynccontextmanager
from dataclasses import dataclass, field, asdict
from datetime import datetime, timedelta
from enum import Enum
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Set, Tuple, Type, Union, Protocol
from typing import runtime_checkable
import yaml
import numpy as np
import copy
import random
import base64
from functools import wraps
import hashlib

# Security & Production dependencies
from cryptography.fernet import Fernet
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2
from jose import JWTError, jwt
from passlib.context import CryptContext
from opentelemetry import trace, context as otel_context
from opentelemetry.trace import Status, StatusCode
from opentelemetry.propagate import inject, extract
from opentelemetry.propagators.composite import CompositePropagator
from tenacity import retry, stop_after_attempt, wait_exponential, before_sleep_log, retry_if_exception_type
from prometheus_client import Counter, Gauge, Histogram, generate_latest, CollectorRegistry, Summary
from prometheus_client import push_to_gateway
import websockets
from websockets.server import serve
from websockets.exceptions import ConnectionClosed

# State persistence
try:
    import redis.asyncio as redis
    REDIS_AVAILABLE = True
except ImportError:
    REDIS_AVAILABLE = False

try:
    import aiosqlite
    SQLITE_AVAILABLE = True
except ImportError:
    SQLITE_AVAILABLE = False

# Distributed task queue
try:
    from celery import Celery
    CELERY_AVAILABLE = True
except ImportError:
    CELERY_AVAILABLE = False

# Configuration validation
from pydantic import BaseModel, Field, validator, ValidationError

# Configure structured logging with contextvars
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - [%(correlation_id)s] - [%(trace_id)s] - %(message)s',
    handlers=[
        logging.FileHandler('green_agent_v8.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Context variables for distributed tracing
_correlation_id_var: contextvars.ContextVar[str] = contextvars.ContextVar('correlation_id', default='')
_trace_id_var: contextvars.ContextVar[str] = contextvars.ContextVar('trace_id', default='')

def get_correlation_id() -> str:
    """Get or create correlation ID for request tracing"""
    try:
        cid = _correlation_id_var.get()
        if not cid:
            cid = str(uuid.uuid4())[:8]
            _correlation_id_var.set(cid)
        return cid
    except LookupError:
        cid = str(uuid.uuid4())[:8]
        _correlation_id_var.set(cid)
        return cid

def set_correlation_id(cid: str):
    """Set correlation ID for current context"""
    _correlation_id_var.set(cid)

def get_trace_id() -> str:
    """Get OpenTelemetry trace ID"""
    try:
        return _trace_id_var.get()
    except LookupError:
        return ""

def set_trace_id(tid: str):
    """Set trace ID for current context"""
    _trace_id_var.set(tid)

# Audit logging
audit_logger = logging.getLogger("audit")
audit_handler = logging.FileHandler('audit.log')
audit_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
audit_logger.addHandler(audit_handler)
audit_logger.setLevel(logging.INFO)

# Enhanced Prometheus metrics
REGISTRY = CollectorRegistry()
TASKS_EXECUTED = Counter('green_agent_tasks_total', 'Total tasks executed',
                        ['task_type', 'status'], registry=REGISTRY)
TASK_DURATION = Histogram('green_agent_task_duration_seconds', 'Task execution duration',
                         ['task_type'], registry=REGISTRY)
COMPONENT_HEALTH = Gauge('green_agent_component_health', 'Component health status',
                        ['component_name'], registry=REGISTRY)
ACTIVE_TASKS = Gauge('green_agent_active_tasks', 'Number of active tasks', registry=REGISTRY)
SYSTEM_UPTIME = Gauge('green_agent_uptime_seconds', 'System uptime', registry=REGISTRY)
DEAD_LETTER_COUNT = Gauge('green_agent_dead_letter_count', 'Dead letter queue size', registry=REGISTRY)
MODULE_INTEGRATION = Gauge('green_agent_module_integration', 'Module integration status',
                          ['module_name'], registry=REGISTRY)
HELIUM_AWARE_TASKS = Counter('green_agent_helium_aware_tasks_total', 'Helium-aware task decisions',
                            ['decision'], registry=REGISTRY)
CIRCUIT_BREAKER_STATE = Gauge('green_agent_circuit_breaker_state', 'Circuit breaker state',
                             ['component', 'state', 'instance'], registry=REGISTRY)
AUTH_FAILURES = Counter('green_agent_auth_failures_total', 'Authentication failures',
                       ['reason'], registry=REGISTRY)
QUEUE_SIZE = Gauge('green_agent_queue_size', 'Task queue size', registry=REGISTRY)
LEADER_ELECTION = Gauge('green_agent_leader_election', 'Leader election status', registry=REGISTRY)

# ============================================================
# ENHANCED DATA MODELS
# ============================================================

@runtime_checkable
class HealthCheckable(Protocol):
    """Protocol for health-checkable components"""
    def health_check(self) -> Dict[str, Any]:
        """Return health status dictionary"""
        ...
    
    def get_statistics(self) -> Dict[str, Any]:
        """Return component statistics"""
        ...

@runtime_checkable
class HeliumAware(Protocol):
    """Protocol for helium-aware components"""
    def update_helium_status(self, scarcity_index: float) -> None:
        """Update helium scarcity status"""
        ...
    
    def get_helium_impact(self) -> float:
        """Get current helium impact factor"""
        ...

class ComponentStatus(str, Enum):
    """Component status states"""
    UNINITIALIZED = "uninitialized"
    INITIALIZING = "initializing"
    HEALTHY = "healthy"
    DEGRADED = "degraded"
    FAILED = "failed"
    STOPPED = "stopped"

class EventType(str, Enum):
    """System event types"""
    COMPONENT_STARTED = "component_started"
    COMPONENT_STOPPED = "component_stopped"
    COMPONENT_FAILED = "component_failed"
    TASK_COMPLETED = "task_completed"
    TASK_FAILED = "task_failed"
    ALERT_TRIGGERED = "alert_triggered"
    CONFIG_CHANGED = "config_changed"
    SCALING_EVENT = "scaling_event"
    HELIUM_SCARCITY = "helium_scarcity"
    CARBON_THRESHOLD = "carbon_threshold"
    THERMAL_ALERT = "thermal_alert"
    BLOCKCHAIN_VERIFIED = "blockchain_verified"
    QUANTUM_OPTIMIZED = "quantum_optimized"

@dataclass
class SystemEvent:
    """System event data structure"""
    event_id: str = field(default_factory=lambda: str(uuid.uuid4())[:8])
    event_type: EventType = EventType.COMPONENT_STARTED
    source: str = "control_system"
    data: Dict = field(default_factory=dict)
    timestamp: datetime = field(default_factory=datetime.now)
    correlation_id: str = field(default_factory=get_correlation_id)
    trace_id: str = field(default_factory=get_trace_id)
    
    def to_json(self) -> str:
        return json.dumps(asdict(self), default=str)

@dataclass
class ComponentInfo:
    """Component registration information"""
    name: str
    instance: Any
    status: ComponentStatus = ComponentStatus.UNINITIALIZED
    registered_at: datetime = field(default_factory=datetime.now)
    last_health_check: Optional[datetime] = None
    health_score: float = 0.0
    dependencies: List[str] = field(default_factory=list)
    metrics: Dict = field(default_factory=dict)
    circuit_breaker_state: str = "closed"
    failure_count: int = 0
    last_failure: Optional[datetime] = None

# ============================================================
# CONFIGURATION VALIDATION WITH PYDANTIC
# ============================================================

class HeliumConfig(BaseModel):
    scheduling_enabled: bool = True
    scarcity_threshold: float = Field(0.7, ge=0.0, le=1.0)

class SecurityConfig(BaseModel):
    jwt_secret: str = Field(..., min_length=32)
    jwt_expiry_seconds: int = Field(3600, ge=60, le=86400)
    rate_limit: int = Field(100, ge=1, le=10000)
    
    @validator('jwt_secret')
    def validate_jwt_secret(cls, v):
        if len(v) < 32:
            raise ValueError('JWT secret must be at least 32 characters')
        return v

class MonitoringConfig(BaseModel):
    health_check_interval: int = Field(30, ge=5, le=300)
    metrics_enabled: bool = True
    pushgateway_url: Optional[str] = None

class WebSocketConfig(BaseModel):
    enabled: bool = True
    host: str = "localhost"
    port: int = Field(8765, ge=1024, le=65535)
    rate_limit: int = Field(60, ge=1, le=600)  # messages per minute

class TracingConfig(BaseModel):
    enabled: bool = True
    sampling_rate: float = Field(0.1, ge=0.0, le=1.0)

class ControlSystemConfig(BaseModel):
    system: Dict = Field(..., description="System configuration")
    helium: HeliumConfig
    security: SecurityConfig
    monitoring: MonitoringConfig
    websocket: WebSocketConfig
    tracing: TracingConfig
    
    class Config:
        extra = "forbid"

# ============================================================
# STATE PERSISTENCE LAYER
# ============================================================

class StatePersistence:
    """State persistence with Redis or SQLite"""
    
    def __init__(self, backend: str = 'redis', redis_url: str = None):
        self.backend = backend
        self.redis_client = None
        self.sqlite_conn = None
        
        if backend == 'redis' and REDIS_AVAILABLE and redis_url:
            self.redis_client = redis.from_url(redis_url)
            logger.info("Redis state persistence initialized")
        elif backend == 'sqlite' and SQLITE_AVAILABLE:
            asyncio.create_task(self._init_sqlite())
            logger.info("SQLite state persistence initialized")
        else:
            logger.warning(f"State persistence backend {backend} not available, using in-memory")
    
    async def _init_sqlite(self):
        """Initialize SQLite database"""
        self.sqlite_conn = await aiosqlite.connect('green_agent_state.db')
        await self.sqlite_conn.execute('''
            CREATE TABLE IF NOT EXISTS workflows (
                workflow_id TEXT PRIMARY KEY,
                state TEXT,
                created_at TEXT,
                updated_at TEXT
            )
        ''')
        await self.sqlite_conn.execute('''
            CREATE TABLE IF NOT EXISTS circuit_breakers (
                name TEXT PRIMARY KEY,
                state TEXT,
                failure_count INTEGER,
                last_failure TEXT,
                updated_at TEXT,
                instance_id TEXT
            )
        ''')
        await self.sqlite_conn.commit()
    
    async def save_workflow_state(self, workflow_id: str, state: Dict):
        """Save workflow state"""
        state_json = json.dumps(state, default=str)
        
        if self.redis_client:
            await self.redis_client.hset(f"workflow:{workflow_id}", "state", state_json)
            await self.redis_client.expire(f"workflow:{workflow_id}", 86400)
        elif self.sqlite_conn:
            await self.sqlite_conn.execute('''
                INSERT OR REPLACE INTO workflows (workflow_id, state, updated_at)
                VALUES (?, ?, ?)
            ''', (workflow_id, state_json, datetime.now().isoformat()))
            await self.sqlite_conn.commit()
    
    async def load_workflow_state(self, workflow_id: str) -> Optional[Dict]:
        """Load workflow state"""
        if self.redis_client:
            data = await self.redis_client.hget(f"workflow:{workflow_id}", "state")
            if data:
                return json.loads(data)
        elif self.sqlite_conn:
            cursor = await self.sqlite_conn.execute(
                'SELECT state FROM workflows WHERE workflow_id = ?', (workflow_id,)
            )
            row = await cursor.fetchone()
            if row:
                return json.loads(row[0])
        
        return None
    
    async def save_circuit_breaker(self, name: str, state: Dict, instance_id: str):
        """Save circuit breaker state"""
        state_json = json.dumps(state, default=str)
        
        if self.redis_client:
            await self.redis_client.hset(f"cb:{name}", mapping={
                'state': state_json,
                'instance_id': instance_id,
                'updated_at': datetime.now().isoformat()
            })
            await self.redis_client.expire(f"cb:{name}", 300)
        elif self.sqlite_conn:
            await self.sqlite_conn.execute('''
                INSERT OR REPLACE INTO circuit_breakers (name, state, updated_at, instance_id)
                VALUES (?, ?, ?, ?)
            ''', (name, state_json, datetime.now().isoformat(), instance_id))
            await self.sqlite_conn.commit()
    
    async def load_circuit_breaker(self, name: str) -> Optional[Dict]:
        """Load circuit breaker state"""
        if self.redis_client:
            data = await self.redis_client.hgetall(f"cb:{name}")
            if data and b'state' in data:
                return json.loads(data[b'state'])
        elif self.sqlite_conn:
            cursor = await self.sqlite_conn.execute(
                'SELECT state FROM circuit_breakers WHERE name = ?', (name,)
            )
            row = await cursor.fetchone()
            if row:
                return json.loads(row[0])
        
        return None
    
    async def close(self):
        """Close persistence connections"""
        if self.redis_client:
            await self.redis_client.close()
        if self.sqlite_conn:
            await self.sqlite_conn.close()

# ============================================================
# DISTRIBUTED CIRCUIT BREAKER
# ============================================================

class DistributedCircuitBreaker:
    """Circuit breaker with distributed coordination"""
    
    def __init__(self, name: str, persistence: StatePersistence, instance_id: str,
                 failure_threshold: int = 5, recovery_timeout: int = 60):
        self.name = name
        self.persistence = persistence
        self.instance_id = instance_id
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        
        self.state = "closed"
        self.failure_count = 0
        self.last_failure_time = None
        self._lock = asyncio.Lock()
        
        # Load persisted state
        asyncio.create_task(self._load_state())
    
    async def _load_state(self):
        """Load persisted circuit breaker state"""
        state = await self.persistence.load_circuit_breaker(self.name)
        if state:
            self.state = state.get('state', 'closed')
            self.failure_count = state.get('failure_count', 0)
            self.last_failure_time = state.get('last_failure_time')
    
    async def _save_state(self):
        """Save circuit breaker state"""
        await self.persistence.save_circuit_breaker(self.name, {
            'state': self.state,
            'failure_count': self.failure_count,
            'last_failure_time': self.last_failure_time
        }, self.instance_id)
    
    async def call(self, func: Callable, *args, **kwargs):
        """Execute function with circuit breaker protection"""
        async with self._lock:
            if self.state == "open":
                if self.last_failure_time and time.time() - self.last_failure_time >= self.recovery_timeout:
                    self.state = "half_open"
                    logger.info(f"Circuit breaker {self.name} transitioning to half-open")
                    CIRCUIT_BREAKER_STATE.labels(
                        component=self.name, state='half_open', instance=self.instance_id
                    ).set(1)
                else:
                    raise Exception(f"Circuit breaker {self.name} is open")
        
        try:
            result = await func(*args, **kwargs) if asyncio.iscoroutinefunction(func) else func(*args, **kwargs)
            
            async with self._lock:
                if self.state == "half_open":
                    self.state = "closed"
                    self.failure_count = 0
                    logger.info(f"Circuit breaker {self.name} closed")
                    CIRCUIT_BREAKER_STATE.labels(
                        component=self.name, state='closed', instance=self.instance_id
                    ).set(1)
                    await self._save_state()
            
            return result
            
        except Exception as e:
            async with self._lock:
                self.failure_count += 1
                self.last_failure_time = time.time()
                
                if self.state == "half_open":
                    self.state = "open"
                    CIRCUIT_BREAKER_STATE.labels(
                        component=self.name, state='open', instance=self.instance_id
                    ).set(1)
                elif self.failure_count >= self.failure_threshold and self.state == "closed":
                    self.state = "open"
                    CIRCUIT_BREAKER_STATE.labels(
                        component=self.name, state='open', instance=self.instance_id
                    ).set(1)
                
                await self._save_state()
            
            logger.warning(f"Circuit breaker {self.name} opened after {self.failure_count} failures")
            raise e

# ============================================================
# ENHANCED EVENT BUS WITH PERSISTENCE
# ============================================================

class EnhancedEventBus:
    """Event bus with persistence and dead-letter queue"""
    
    def __init__(self, persistence: StatePersistence):
        self.subscribers: Dict[str, List[Tuple[Callable, DistributedCircuitBreaker]]] = defaultdict(list)
        self.event_store: deque = deque(maxlen=10000)
        self.dead_letter_events: deque = deque(maxlen=1000)
        self._lock = asyncio.Lock()
        self.persistence = persistence
        self._retry_policy = {
            'max_attempts': 3,
            'min_wait': 1,
            'max_wait': 10
        }
    
    def subscribe(self, event_type: str, callback: Callable, circuit_breaker_name: str = None):
        """Subscribe to events with circuit breaker"""
        cb = DistributedCircuitBreaker(
            circuit_breaker_name or f"event_{event_type}",
            self.persistence,
            instance_id=str(uuid.uuid4())[:8]
        )
        self.subscribers[event_type].append((callback, cb))
        logger.info(f"Subscribed to {event_type} events with circuit breaker")
    
    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=1, max=10))
    async def publish(self, event: SystemEvent):
        """Publish event with persistence"""
        async with self._lock:
            self.event_store.append(event)
            
            # Persist event
            await self.persistence.save_workflow_state(
                f"event_{event.event_id}",
                asdict(event)
            )
            
            subscribers = self.subscribers.get(event.event_type.value, [])
            
            tasks = []
            for callback, cb in subscribers:
                tasks.append(self._notify_subscriber_with_circuit_breaker(callback, cb, event))
            
            if tasks:
                results = await asyncio.gather(*tasks, return_exceptions=True)
                for i, result in enumerate(results):
                    if isinstance(result, Exception):
                        self.dead_letter_events.append({
                            'event': event,
                            'error': str(result),
                            'timestamp': datetime.now().isoformat()
                        })
                        DEAD_LETTER_COUNT.set(len(self.dead_letter_events))
                        logger.error(f"Event delivery failed: {result}")
    
    async def _notify_subscriber_with_circuit_breaker(self, callback: Callable, 
                                                      circuit_breaker: DistributedCircuitBreaker, 
                                                      event: SystemEvent):
        """Notify subscriber with circuit breaker protection"""
        try:
            await circuit_breaker.call(self._notify_subscriber, callback, event)
        except Exception as e:
            logger.error(f"Circuit breaker prevented notification: {e}")
            raise
    
    async def _notify_subscriber(self, callback: Callable, event: SystemEvent):
        """Notify a single subscriber"""
        token = _correlation_id_var.set(event.correlation_id)
        trace_token = _trace_id_var.set(event.trace_id)
        
        try:
            if asyncio.iscoroutinefunction(callback):
                await callback(event)
            else:
                callback(event)
        finally:
            _correlation_id_var.reset(token)
            _trace_id_var.reset(trace_token)
    
    def get_statistics(self) -> Dict:
        """Get event bus statistics"""
        return {
            'total_events': len(self.event_store),
            'dead_letter_events': len(self.dead_letter_events),
            'subscriber_count': sum(len(v) for v in self.subscribers.values()),
            'event_types': list(self.subscribers.keys()),
            'retry_policy': self._retry_policy
        }

# ============================================================
# ENHANCED SAGA ORCHESTRATOR WITH PERSISTENCE
# ============================================================

@dataclass
class SagaStep:
    """Saga workflow step"""
    step_id: str
    action: Callable
    compensation: Optional[Callable] = None
    timeout: int = 30
    required: bool = True
    retry_count: int = 0
    max_retries: int = 3

class SagaOrchestrator:
    """Enhanced saga orchestrator with persistence"""
    
    def __init__(self, persistence: StatePersistence):
        self.persistence = persistence
        self.active_workflows: Dict[str, Dict] = {}
        self.workflow_history: deque = deque(maxlen=1000)
        self._lock = asyncio.Lock()
    
    async def execute_workflow(self, workflow_id: str, steps: List[SagaStep],
                             context: Dict = None, metadata: Dict = None) -> Dict:
        """Execute workflow with persistence"""
        workflow = {
            'workflow_id': workflow_id,
            'steps': [asdict(step) for step in steps],
            'state': 'running',
            'current_step': 0,
            'completed_steps': [],
            'compensated_steps': [],
            'context': context or {},
            'metadata': metadata or {},
            'started_at': datetime.now().isoformat(),
            'retry_counts': {}
        }
        
        async with self._lock:
            self.active_workflows[workflow_id] = workflow
        
        # Persist initial state
        await self.persistence.save_workflow_state(workflow_id, workflow)
        
        try:
            for i, step in enumerate(steps):
                workflow['current_step'] = i
                await self.persistence.save_workflow_state(workflow_id, workflow)
                
                for attempt in range(step.max_retries + 1):
                    try:
                        result = await asyncio.wait_for(
                            self._execute_action(step.action, workflow['context']),
                            timeout=step.timeout
                        )
                        workflow['context'].update(result or {})
                        workflow['completed_steps'].append(step.step_id)
                        workflow['retry_counts'][step.step_id] = attempt
                        break
                    except asyncio.TimeoutError as e:
                        if attempt >= step.max_retries:
                            raise Exception(f"Step {step.step_id} timed out after {attempt} retries")
                        logger.warning(f"Step {step.step_id} timed out, retrying {attempt+1}/{step.max_retries}")
                        await asyncio.sleep(2 ** attempt)
                    except Exception as e:
                        if attempt >= step.max_retries:
                            if step.required:
                                raise
                            else:
                                logger.warning(f"Optional step {step.step_id} failed, continuing")
                                break
                        logger.warning(f"Step {step.step_id} failed, retrying: {e}")
                        await asyncio.sleep(2 ** attempt)
            
            if workflow['state'] == 'running':
                workflow['state'] = 'completed'
                workflow['completed_at'] = datetime.now().isoformat()
                await self.persistence.save_workflow_state(workflow_id, workflow)
                
                audit_logger.info(f"Workflow {workflow_id} completed", extra={
                    'workflow_id': workflow_id,
                    'steps_completed': len(workflow['completed_steps']),
                    'duration': (datetime.now() - datetime.fromisoformat(workflow['started_at'])).total_seconds()
                })
                
        except Exception as e:
            workflow['state'] = 'failed'
            workflow['error'] = str(e)
            workflow['failed_at'] = datetime.now().isoformat()
            await self.persistence.save_workflow_state(workflow_id, workflow)
            logger.error(f"Workflow {workflow_id} failed: {e}")
            
            await self._compensate(workflow)
        
        finally:
            self.workflow_history.append(workflow)
            async with self._lock:
                if workflow_id in self.active_workflows:
                    del self.active_workflows[workflow_id]
        
        return workflow
    
    async def _execute_action(self, action: Callable, context: Dict) -> Dict:
        """Execute action with context injection"""
        if asyncio.iscoroutinefunction(action):
            return await action(context)
        return action(context)
    
    async def _compensate(self, workflow: Dict):
        """Execute compensation for completed steps"""
        steps = [SagaStep(**step_dict) for step_dict in workflow['steps']]
        
        for step in reversed(steps):
            if step.step_id in workflow['completed_steps'] and step.compensation:
                try:
                    await self._execute_action(step.compensation, workflow['context'])
                    workflow['compensated_steps'].append(step.step_id)
                    await self.persistence.save_workflow_state(workflow['workflow_id'], workflow)
                    logger.info(f"Compensated step {step.step_id}")
                except Exception as e:
                    logger.error(f"Compensation failed for step {step.step_id}: {e}")
    
    async def get_workflow_status(self, workflow_id: str) -> Optional[Dict]:
        """Get workflow status from persistence"""
        if workflow_id in self.active_workflows:
            return self.active_workflows[workflow_id]
        
        return await self.persistence.load_workflow_state(workflow_id)

# ============================================================
# CONFIGURATION WATCHER FOR HOT-RELOAD
# ============================================================

class ConfigWatcher:
    """Watch configuration file for changes and hot-reload"""
    
    def __init__(self, config_path: str, reload_callback: Callable):
        self.config_path = Path(config_path)
        self.reload_callback = reload_callback
        self.last_mtime = self.config_path.stat().st_mtime if self.config_path.exists() else 0
        self.running = False
    
    async def start(self):
        """Start watching for configuration changes"""
        self.running = True
        while self.running:
            await asyncio.sleep(30)  # Check every 30 seconds
            if self.config_path.exists():
                current_mtime = self.config_path.stat().st_mtime
                if current_mtime > self.last_mtime:
                    logger.info("Configuration file changed, reloading...")
                    try:
                        await self.reload_callback()
                        self.last_mtime = current_mtime
                    except Exception as e:
                        logger.error(f"Configuration reload failed: {e}")
    
    async def stop(self):
        """Stop watching"""
        self.running = False

# ============================================================
# BULKHEAD PATTERN FOR TASK ISOLATION
# ============================================================

class Bulkhead:
    """Bulkhead pattern for resource isolation"""
    
    def __init__(self, name: str, max_concurrent: int = 10, max_queue_size: int = 100):
        self.name = name
        self.max_concurrent = max_concurrent
        self.max_queue_size = max_queue_size
        self.active_count = 0
        self.queue = asyncio.Queue(maxsize=max_queue_size)
        self._lock = asyncio.Lock()
    
    async def execute(self, func: Callable, *args, **kwargs):
        """Execute function with bulkhead protection"""
        async with self._lock:
            if self.active_count >= self.max_concurrent:
                # Queue task
                try:
                    await asyncio.wait_for(self.queue.put((func, args, kwargs)), timeout=1.0)
                except asyncio.TimeoutError:
                    raise Exception(f"Bulkhead {self.name} queue full")
                return
            
            self.active_count += 1
        
        try:
            result = await func(*args, **kwargs) if asyncio.iscoroutinefunction(func) else func(*args, **kwargs)
            
            # Process queued tasks
            while not self.queue.empty():
                try:
                    queued_func, queued_args, queued_kwargs = self.queue.get_nowait()
                    await queued_func(*queued_args, **queued_kwargs)
                except asyncio.QueueEmpty:
                    break
            
            return result
            
        finally:
            async with self._lock:
                self.active_count -= 1

# ============================================================
# LEADER ELECTION FOR MULTI-INSTANCE DEPLOYMENTS
# ============================================================

class LeaderElection:
    """Leader election using Redis distributed lock"""
    
    def __init__(self, redis_client, lock_key: str = "green_agent:leader", ttl: int = 30):
        self.redis = redis_client
        self.lock_key = lock_key
        self.ttl = ttl
        self.is_leader = False
        self.renewal_task = None
    
    async def acquire_leadership(self) -> bool:
        """Attempt to acquire leadership"""
        if not self.redis:
            # No Redis, assume leader (single instance)
            self.is_leader = True
            LEADER_ELECTION.set(1 if self.is_leader else 0)
            return True
        
        # Try to set lock with NX (only if not exists)
        result = await self.redis.set(self.lock_key, str(uuid.uuid4()), nx=True, ex=self.ttl)
        
        if result:
            self.is_leader = True
            LEADER_ELECTION.set(1)
            # Start renewal task
            self.renewal_task = asyncio.create_task(self._renew_lock())
            logger.info("Instance acquired leadership")
            return True
        
        self.is_leader = False
        LEADER_ELECTION.set(0)
        return False
    
    async def _renew_lock(self):
        """Renew leadership lock periodically"""
        while self.is_leader:
            await asyncio.sleep(self.ttl / 2)
            if self.redis:
                await self.redis.expire(self.lock_key, self.ttl)
    
    async def release_leadership(self):
        """Release leadership"""
        if self.is_leader and self.redis:
            await self.redis.delete(self.lock_key)
            if self.renewal_task:
                self.renewal_task.cancel()
        self.is_leader = False
        LEADER_ELECTION.set(0)
        logger.info("Instance released leadership")

# ============================================================
# ENHANCED API GATEWAY WITH VERSIONING
# ============================================================

class APIGateway:
    """API gateway with versioning, authentication, and rate limiting"""
    
    def __init__(self, jwt_secret: str = None, rate_limit: int = 100, persistence: StatePersistence = None):
        self.routes: Dict[str, Dict] = {}
        self.versioned_routes: Dict[str, Dict] = defaultdict(dict)
        self.rate_limiters: Dict[str, Dict] = defaultdict(lambda: {'tokens': rate_limit, 'last_refill': time.time()})
        self.request_history: deque = deque(maxlen=10000)
        self.rate_limit = rate_limit
        self.jwt_secret = jwt_secret or os.getenv('JWT_SECRET', 'default-secret-change-me')
        self.pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")
        self.persistence = persistence
        
        # User store (in production, use database)
        self.users = {
            'admin': {
                'password': self.pwd_context.hash('admin123'),
                'roles': ['admin', 'operator', 'viewer'],
                'api_keys': [hashlib.sha256(f"key_{user}".encode()).hexdigest()[:16]]
            }
        }
    
    def register_route(self, path: str, handler: Callable, methods: List[str] = None, 
                      auth_required: bool = True, roles: List[str] = None, version: int = 1):
        """Register API route with versioning"""
        versioned_path = f"/v{version}{path}"
        self.versioned_routes[version][versioned_path] = {
            'handler': handler,
            'methods': methods or ['GET'],
            'auth_required': auth_required,
            'roles': roles or []
        }
        self.routes[versioned_path] = self.versioned_routes[version][versioned_path]
        logger.info(f"Registered route: {versioned_path} (v{version})")
    
    async def authenticate(self, request: Dict) -> Dict:
        """Authenticate request using JWT or API key"""
        api_key = request.get('headers', {}).get('X-API-Key')
        if api_key:
            for username, user_data in self.users.items():
                if api_key in user_data.get('api_keys', []):
                    audit_logger.info(f"API key authentication successful for {username}")
                    return {'authenticated': True, 'user': username, 'roles': user_data['roles']}
        
        auth_header = request.get('headers', {}).get('Authorization', '')
        if auth_header.startswith('Bearer '):
            token = auth_header[7:]
            try:
                payload = jwt.decode(token, self.jwt_secret, algorithms=['HS256'])
                return {
                    'authenticated': True,
                    'user': payload.get('sub'),
                    'roles': payload.get('roles', [])
                }
            except JWTError as e:
                AUTH_FAILURES.labels(reason='invalid_jwt').inc()
                audit_logger.warning(f"JWT authentication failed: {e}")
                return {'authenticated': False, 'error': str(e)}
        
        return {'authenticated': False, 'error': 'No authentication provided'}
    
    def generate_token(self, username: str, roles: List[str] = None, expires_in: int = 3600) -> str:
        """Generate JWT token for user"""
        payload = {
            'sub': username,
            'roles': roles or ['viewer'],
            'exp': datetime.utcnow() + timedelta(seconds=expires_in),
            'iat': datetime.utcnow(),
            'jti': str(uuid.uuid4())
        }
        token = jwt.encode(payload, self.jwt_secret, algorithm='HS256')
        audit_logger.info(f"Token generated for user: {username}")
        return token
    
    def check_rate_limit(self, client_id: str, path: str) -> bool:
        """Check rate limit using sliding window algorithm"""
        key = f"{client_id}_{path}"
        limiter = self.rate_limiters[key]
        now = time.time()
        
        elapsed = now - limiter['last_refill']
        limiter['tokens'] = min(self.rate_limit, limiter['tokens'] + elapsed * (self.rate_limit / 60))
        limiter['last_refill'] = now
        
        if limiter['tokens'] >= 1:
            limiter['tokens'] -= 1
            return True
        return False
    
    async def handle_request(self, request: Dict) -> Dict:
        """Handle incoming request with version detection"""
        path = request.get('path', '/')
        method = request.get('method', 'GET')
        client_id = request.get('client_id', 'anonymous')
        
        # Extract version from path
        version = 1
        if path.startswith('/v'):
            parts = path.split('/')
            if len(parts) > 1 and parts[1].startswith('v'):
                try:
                    version = int(parts[1][1:])
                    path = '/' + '/'.join(parts[2:])
                except ValueError:
                    pass
        
        # Find route
        route = None
        if version in self.versioned_routes:
            route = self.versioned_routes[version].get(path)
        
        if not route:
            route = self.routes.get(path)
        
        if not route:
            return {'error': 'Not found', 'status': 404}
        
        if method not in route['methods']:
            return {'error': 'Method not allowed', 'status': 405}
        
        # Authentication
        if route.get('auth_required', True):
            auth_result = await self.authenticate(request)
            if not auth_result.get('authenticated'):
                AUTH_FAILURES.labels(reason='unauthorized').inc()
                return {'error': 'Unauthorized', 'status': 401}
            
            required_roles = route.get('roles', [])
            if required_roles and not any(role in auth_result.get('roles', []) for role in required_roles):
                AUTH_FAILURES.labels(reason='insufficient_permissions').inc()
                return {'error': 'Insufficient permissions', 'status': 403'}
            
            request['user'] = auth_result
        
        # Rate limiting
        if not self.check_rate_limit(client_id, path):
            return {'error': 'Rate limit exceeded', 'status': 429, 'retry_after': 60}
        
        # Log request
        audit_logger.info(f"API request: {method} {path} from {client_id}")
        
        try:
            handler = route['handler']
            response = await handler(request) if asyncio.iscoroutinefunction(handler) else handler(request)
            self.request_history.append({
                'path': path, 'method': method, 'status': response.get('status', 200),
                'timestamp': datetime.now().isoformat()
            })
            return response
        except Exception as e:
            logger.error(f"Handler error: {e}")
            return {'error': str(e), 'status': 500}
    
    def get_statistics(self) -> Dict:
        """Get gateway statistics"""
        return {
            'routes': len(self.routes),
            'versions': list(self.versioned_routes.keys()),
            'total_requests': len(self.request_history),
            'rate_limit': self.rate_limit,
            'recent_requests': list(self.request_history)[-10:]
        }

# ============================================================
# MAIN CONTROL SYSTEM (ENHANCED)
# ============================================================

class GreenAgentControlSystem:
    """
    ENHANCED Green Agent Control System v8.0
    
    Central orchestration with:
    - State persistence (Redis/SQLite)
    - Distributed circuit breakers
    - Configuration validation and hot-reload
    - WebSocket authentication
    - Leader election
    - Bulkhead pattern
    - Distributed tracing
    - Comprehensive health checks
    """
    
    def __init__(self, config_path: str = None):
        self.config_path = config_path
        self.config = self._load_and_validate_config()
        
        # Instance ID for distributed coordination
        self.instance_id = str(uuid.uuid4())[:8]
        logger.info(f"Instance ID: {self.instance_id}")
        
        # State persistence
        self.persistence = StatePersistence(
            backend=self.config.get('persistence_backend', 'redis'),
            redis_url=self.config.get('redis_url')
        )
        
        # Component registry
        self.components: Dict[str, ComponentInfo] = {}
        
        # Core infrastructure (enhanced)
        self.event_bus = EnhancedEventBus(self.persistence)
        self.saga_orchestrator = SagaOrchestrator(self.persistence)
        self.api_gateway = APIGateway(
            jwt_secret=self.config.get('security', {}).get('jwt_secret'),
            rate_limit=self.config.get('security', {}).get('rate_limit', 100),
            persistence=self.persistence
        )
        self.secrets_manager = SecretsManager()
        
        # Distributed components
        self.circuit_breakers: Dict[str, DistributedCircuitBreaker] = {}
        self.bulkheads: Dict[str, Bulkhead] = {}
        self.leader_election = LeaderElection(
            self.persistence.redis_client if self.persistence.redis_client else None
        )
        
        # Tracking
        self.start_time = datetime.now()
        self.task_history: deque = deque(maxlen=10000)
        self.alert_history: deque = deque(maxlen=500)
        
        # Helium-aware scheduling
        self.helium_scarcity_level = 0.0
        self.throttled_tasks: Set[str] = set()
        
        # WebSocket connections
        self.websocket_connections: Set = set()
        self.ws_rate_limiters: Dict[str, deque] = defaultdict(lambda: deque(maxlen=60))
        
        # Task queues
        self.task_queue = asyncio.Queue()
        self.accepting_tasks = True
        self._tasks_completed = asyncio.Event()
        
        # Configuration watcher
        if self.config_path:
            self.config_watcher = ConfigWatcher(self.config_path, self._reload_config)
        else:
            self.config_watcher = None
        
        # Metrics exporter
        self.metrics_exporter_task = None
        
        # Distributed tracing
        self.tracer = trace.get_tracer(__name__)
        
        # Initialize
        self._register_core_routes()
        self._register_event_handlers()
        self._init_bulkheads()
        self._discover_enhancement_modules()
        
        # Start background tasks
        self.background_tasks = [
            asyncio.create_task(self._health_monitor_loop()),
            asyncio.create_task(self._helium_update_loop()),
            asyncio.create_task(self._gradual_cycle_loop()),
            asyncio.create_task(self._task_processor()),
            asyncio.create_task(self._metrics_exporter_loop())
        ]
        
        if self.config_watcher:
            self.background_tasks.append(asyncio.create_task(self.config_watcher.start()))
        
        # Start WebSocket server
        asyncio.create_task(self._start_websocket_server())
        
        # Acquire leadership
        asyncio.create_task(self.leader_election.acquire_leadership())
        
        # Update metrics
        SYSTEM_UPTIME.set(0)
        
        logger.info(f"GreenAgentControlSystem v8.0 initialized with {len(self.components)} components")
    
    def _load_and_validate_config(self) -> Dict:
        """Load and validate configuration with Pydantic"""
        config_file = self.config_path or 'control_system_config.yaml'
        
        default_config = {
            'system': {'name': 'Green Agent', 'version': '8.0'},
            'helium': {'scheduling_enabled': True, 'scarcity_threshold': 0.7},
            'security': {'jwt_secret': os.getenv('JWT_SECRET', 'default-secret-change-me'), 
                        'jwt_expiry_seconds': 3600, 'rate_limit': 100},
            'monitoring': {'health_check_interval': 30, 'metrics_enabled': True,
                          'pushgateway_url': os.getenv('PUSHGATEWAY_URL')},
            'websocket': {'enabled': True, 'host': 'localhost', 'port': 8765, 'rate_limit': 60},
            'tracing': {'enabled': True, 'sampling_rate': 0.1},
            'persistence_backend': os.getenv('PERSISTENCE_BACKEND', 'redis'),
            'redis_url': os.getenv('REDIS_URL', 'redis://localhost:6379')
        }
        
        if Path(config_file).exists():
            try:
                with open(config_file, 'r') as f:
                    user_config = yaml.safe_load(f)
                    default_config.update(user_config)
            except Exception as e:
                logger.warning(f"Failed to load config: {e}")
        
        # Validate with Pydantic
        try:
            validated = ControlSystemConfig(**default_config)
            logger.info("Configuration validated successfully")
            return default_config
        except ValidationError as e:
            logger.error(f"Configuration validation failed: {e}")
            raise
    
    async def _reload_config(self):
        """Hot-reload configuration"""
        try:
            new_config = self._load_and_validate_config()
            old_config = self.config
            self.config = new_config
            
            # Update components that need config changes
            if new_config['security']['rate_limit'] != old_config['security']['rate_limit']:
                self.api_gateway.rate_limit = new_config['security']['rate_limit']
            
            # Publish config change event
            await self.event_bus.publish(SystemEvent(
                event_type=EventType.CONFIG_CHANGED,
                source='config_watcher',
                data={'old_config': old_config, 'new_config': new_config}
            ))
            
            audit_logger.info("Configuration hot-reloaded")
            logger.info("Configuration reloaded successfully")
            
        except Exception as e:
            logger.error(f"Configuration reload failed: {e}")
    
    def _init_bulkheads(self):
        """Initialize bulkheads for task isolation"""
        self.bulkheads = {
            'helium_tasks': Bulkhead('helium_tasks', max_concurrent=10, max_queue_size=50),
            'carbon_tasks': Bulkhead('carbon_tasks', max_concurrent=20, max_queue_size=100),
            'quantum_tasks': Bulkhead('quantum_tasks', max_concurrent=5, max_queue_size=25),
            'general_tasks': Bulkhead('general_tasks', max_concurrent=50, max_queue_size=200)
        }
    
    def _register_core_routes(self):
        """Register core API routes with versioning"""
        self.api_gateway.register_route('/health', self._health_handler, ['GET'], auth_required=False, version=1)
        self.api_gateway.register_route('/health/detailed', self._detailed_health_handler, ['GET'], 
                                        auth_required=True, roles=['admin'], version=1)
        self.api_gateway.register_route('/status', self._status_handler, ['GET'], 
                                        auth_required=True, roles=['viewer', 'operator', 'admin'], version=1)
        self.api_gateway.register_route('/components', self._components_handler, ['GET'], 
                                        auth_required=True, roles=['viewer'], version=1)
        self.api_gateway.register_route('/events', self._events_handler, ['GET'], 
                                        auth_required=True, roles=['operator'], version=1)
        self.api_gateway.register_route('/helium/status', self._helium_status_handler, ['GET'], 
                                        auth_required=True, roles=['viewer'], version=1)
        self.api_gateway.register_route('/workflows', self._workflows_handler, ['GET'], 
                                        auth_required=True, roles=['operator'], version=1)
        self.api_gateway.register_route('/metrics', self._metrics_handler, ['GET'], 
                                        auth_required=True, roles=['admin'], version=1)
        self.api_gateway.register_route('/token', self._token_handler, ['POST'], auth_required=False, version=1)
        
        # API v2 example
        self.api_gateway.register_route('/status', self._status_handler_v2, ['GET'], 
                                        auth_required=True, roles=['viewer'], version=2)
    
    async def _health_handler(self, request: Dict) -> Dict:
        """Basic health check endpoint"""
        return {
            'status': 'healthy',
            'timestamp': datetime.now().isoformat(),
            'version': '8.0',
            'instance_id': self.instance_id,
            'components_healthy': sum(1 for c in self.components.values() if c.status == ComponentStatus.HEALTHY)
        }
    
    async def _detailed_health_handler(self, request: Dict) -> Dict:
        """Detailed health check with component breakdown"""
        health = {
            'status': 'healthy',
            'version': '8.0',
            'instance_id': self.instance_id,
            'is_leader': self.leader_election.is_leader,
            'timestamp': datetime.now().isoformat(),
            'components': {},
            'circuit_breakers': {},
            'bulkheads': {},
            'queue_status': {
                'event_bus_size': len(self.event_bus.event_store),
                'dead_letter_size': len(self.event_bus.dead_letter_events),
                'active_tasks': ACTIVE_TASKS._value.get(),
                'task_queue_size': self.task_queue.qsize()
            }
        }
        
        # Component health
        for name, info in self.components.items():
            component_health = {
                'status': info.status.value,
                'health_score': info.health_score,
                'uptime_seconds': (datetime.now() - info.registered_at).total_seconds(),
                'failure_count': info.failure_count,
                'last_failure': info.last_failure.isoformat() if info.last_failure else None
            }
            health['components'][name] = component_health
        
        # Circuit breakers
        for name, cb in self.circuit_breakers.items():
            health['circuit_breakers'][name] = {
                'state': cb.state,
                'failure_count': cb.failure_count,
                'last_failure_time': cb.last_failure_time
            }
        
        # Bulkheads
        for name, bh in self.bulkheads.items():
            health['bulkheads'][name] = {
                'active_count': bh.active_count,
                'queue_size': bh.queue.qsize(),
                'max_concurrent': bh.max_concurrent,
                'max_queue_size': bh.max_queue_size
            }
        
        # Check dependencies
        for name, info in self.components.items():
            for dep in info.dependencies:
                if dep not in self.components:
                    health['components'][name]['missing_dependency'] = dep
                    health['status'] = 'degraded'
        
        return health
    
    async def _status_handler_v2(self, request: Dict) -> Dict:
        """Status handler for API v2"""
        status = self.get_system_status()
        status['api_version'] = 2
        status['instance_id'] = self.instance_id
        return status
    
    async def _status_handler(self, request: Dict) -> Dict:
        """Status handler for API v1"""
        return self.get_system_status()
    
    async def _components_handler(self, request: Dict) -> Dict:
        """Components list endpoint"""
        return {
            'components': [
                {
                    'name': name,
                    'status': info.status.value,
                    'health_score': info.health_score,
                    'registered_at': info.registered_at.isoformat()
                }
                for name, info in self.components.items()
            ],
            'count': len(self.components),
            'instance_id': self.instance_id
        }
    
    async def _events_handler(self, request: Dict) -> Dict:
        """Events statistics endpoint"""
        return self.event_bus.get_statistics()
    
    async def _helium_status_handler(self, request: Dict) -> Dict:
        """Helium status endpoint"""
        return {
            'scarcity_level': self.helium_scarcity_level,
            'throttled_tasks': list(self.throttled_tasks),
            'helium_components': [n for n in self.components if 'helium' in n],
            'is_throttling': len(self.throttled_tasks) > 0
        }
    
    async def _workflows_handler(self, request: Dict) -> Dict:
        """Workflow status endpoint"""
        return {
            'active_workflows': len(self.saga_orchestrator.active_workflows),
            'completed_workflows': len(self.saga_orchestrator.workflow_history),
            'recent_workflows': [
                {
                    'workflow_id': w['workflow_id'],
                    'state': w['state'],
                    'started_at': w.get('started_at'),
                    'completed_at': w.get('completed_at')
                }
                for w in list(self.saga_orchestrator.workflow_history)[-5:]
            ]
        }
    
    async def _metrics_handler(self, request: Dict) -> Dict:
        """Prometheus metrics endpoint"""
        return {'metrics': generate_latest(REGISTRY).decode()}
    
    async def _token_handler(self, request: Dict) -> Dict:
        """Token generation endpoint"""
        data = request.get('data', {})
        username = data.get('username')
        password = data.get('password')
        
        if username == 'admin' and password == 'admin123':
            token = self.api_gateway.generate_token(username, ['admin', 'operator', 'viewer'])
            return {'token': token, 'expires_in': 3600, 'token_type': 'Bearer'}
        
        AUTH_FAILURES.labels(reason='invalid_credentials').inc()
        return {'error': 'Invalid credentials', 'status': 401}
    
    def _register_event_handlers(self):
        """Register system event handlers"""
        self.event_bus.subscribe(EventType.COMPONENT_FAILED.value, self._handle_component_failure)
        self.event_bus.subscribe(EventType.HELIUM_SCARCITY.value, self._handle_helium_scarcity)
        self.event_bus.subscribe(EventType.CARBON_THRESHOLD.value, self._handle_carbon_threshold)
        self.event_bus.subscribe(EventType.THERMAL_ALERT.value, self._handle_thermal_alert)
        self.event_bus.subscribe(EventType.TASK_COMPLETED.value, self._handle_task_completed)
    
    async def _handle_component_failure(self, event: SystemEvent):
        """Handle component failure event"""
        component_name = event.data.get('component_name', 'unknown')
        logger.warning(f"Component failure detected: {component_name}")
        
        with self.tracer.start_as_current_span("handle_component_failure") as span:
            span.set_attribute("component.name", component_name)
            
            if component_name in self.components:
                self.components[component_name].status = ComponentStatus.FAILED
                COMPONENT_HEALTH.labels(component_name=component_name).set(0)
                
                # Try to restart component if configured
                if self.config.get('system', {}).get('auto_restart', True):
                    asyncio.create_task(self._restart_component(component_name))
    
    async def _handle_helium_scarcity(self, event: SystemEvent):
        """Handle helium scarcity event"""
        self.helium_scarcity_level = event.data.get('scarcity_index', 0.0)
        
        if self.helium_scarcity_level > self.config['helium']['scarcity_threshold']:
            await self._throttle_non_critical_tasks()
            HELIUM_AWARE_TASKS.labels(decision='throttle').inc()
            audit_logger.warning(f"Helium scarcity threshold exceeded: {self.helium_scarcity_level:.2f}")
        elif self.helium_scarcity_level < 0.3 and self.throttled_tasks:
            await self._restore_throttled_tasks()
            HELIUM_AWARE_TASKS.labels(decision='restore').inc()
        
        logger.info(f"Helium scarcity updated: {self.helium_scarcity_level:.2f}")
    
    async def _handle_carbon_threshold(self, event: SystemEvent):
        """Handle carbon threshold event"""
        carbon_kg = event.data.get('carbon_kg', 0)
        audit_logger.warning(f"Carbon threshold exceeded: {carbon_kg}kg", extra={
            'carbon_kg': carbon_kg,
            'threshold': event.data.get('threshold', 1000)
        })
    
    async def _handle_thermal_alert(self, event: SystemEvent):
        """Handle thermal alert event"""
        temp_c = event.data.get('temperature_c', 0)
        audit_logger.warning(f"Thermal alert: {temp_c}°C", extra={
            'temperature_c': temp_c,
            'region': event.data.get('region', 'unknown')
        })
    
    async def _handle_task_completed(self, event: SystemEvent):
        """Handle task completion event"""
        task_type = event.data.get('task_type', 'unknown')
        duration = event.data.get('duration', 0)
        logger.info(f"Task completed: {task_type} in {duration:.2f}s")
    
    async def _restart_component(self, component_name: str):
        """Attempt to restart a failed component with backoff"""
        for attempt in range(3):
            logger.info(f"Attempting to restart component {component_name}, attempt {attempt + 1}/3")
            await asyncio.sleep(5 * (2 ** attempt))  # Exponential backoff
            
            try:
                # Re-initialize component
                component_class = self.components[component_name].instance.__class__
                new_instance = component_class()
                self.components[component_name].instance = new_instance
                self.components[component_name].status = ComponentStatus.HEALTHY
                self.components[component_name].failure_count = 0
                COMPONENT_HEALTH.labels(component_name=component_name).set(1)
                logger.info(f"Component {component_name} restarted successfully")
                return
            except Exception as e:
                logger.error(f"Component restart failed: {e}")
        
        logger.error(f"Failed to restart component {component_name} after 3 attempts")
    
    async def _task_processor(self):
        """Background task processor with bulkhead isolation"""
        while self.accepting_tasks:
            try:
                task_type, task_data, future = await self.task_queue.get()
                QUEUE_SIZE.set(self.task_queue.qsize())
                
                # Determine bulkhead based on task type
                if 'helium' in task_type:
                    bulkhead = self.bulkheads['helium_tasks']
                elif 'carbon' in task_type:
                    bulkhead = self.bulkheads['carbon_tasks']
                elif 'quantum' in task_type:
                    bulkhead = self.bulkheads['quantum_tasks']
                else:
                    bulkhead = self.bulkheads['general_tasks']
                
                # Execute with bulkhead protection
                result = await bulkhead.execute(self._execute_task, task_type, task_data)
                future.set_result(result)
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Task processor error: {e}")
    
    async def _execute_task(self, task_type: str, task_data: Dict) -> Dict:
        """Execute task with retry and circuit breaker"""
        if task_type in self.throttled_tasks:
            return {'status': 'throttled', 'reason': 'helium_scarcity', 'retry_after': 300}
        
        # Get or create circuit breaker
        if task_type not in self.circuit_breakers:
            self.circuit_breakers[task_type] = DistributedCircuitBreaker(
                task_type, self.persistence, self.instance_id
            )
        
        return await self.circuit_breakers[task_type].call(
            self._route_task, task_type, task_data
        )
    
    async def _route_task(self, task_type: str, task_data: Dict) -> Dict:
        """Route task to appropriate component"""
        routing_map = {
            'helium_collect': ('helium_data_collector', 'collect_all_data'),
            'regret_optimize': ('regret_optimizer', 'calculate_regret'),
            'thermal_optimize': ('thermal_optimizer', 'run_optimization'),
        }
        
        if task_type in routing_map:
            component_name, method_name = routing_map[task_type]
            component = self.get_component(component_name)
            
            if component and hasattr(component, method_name):
                method = getattr(component, method_name)
                if asyncio.iscoroutinefunction(method):
                    return await method(**task_data)
                else:
                    return method(**task_data)
            else:
                return {'error': f'Component or method not found: {component_name}.{method_name}'}
        
        return {'error': f'Unknown task type: {task_type}'}
    
    async def _metrics_exporter_loop(self):
        """Background metrics export to Prometheus Pushgateway"""
        pushgateway_url = self.config['monitoring'].get('pushgateway_url')
        if not pushgateway_url:
            return
        
        while True:
            try:
                push_to_gateway(
                    pushgateway_url,
                    job='green_agent',
                    registry=REGISTRY
                )
                await asyncio.sleep(60)  # Export every minute
            except Exception as e:
                logger.error(f"Metrics export failed: {e}")
                await asyncio.sleep(300)
    
    async def _start_websocket_server(self):
        """Start WebSocket server with authentication"""
        if not self.config['websocket']['enabled']:
            return
        
        host = self.config['websocket']['host']
        port = self.config['websocket']['port']
        ws_rate_limit = self.config['websocket']['rate_limit']
        
        async def handler(websocket, path):
            # Extract token from query string or headers
            token = None
            if 'token' in websocket.request.query_string.decode():
                params = dict(p.split('=') for p in websocket.request.query_string.decode().split('&'))
                token = params.get('token')
            
            # Authenticate
            if not token:
                await websocket.close(code=1008, reason="Missing authentication token")
                return
            
            try:
                payload = jwt.decode(token, self.config['security']['jwt_secret'], algorithms=['HS256'])
                user = payload.get('sub')
                if not user:
                    await websocket.close(code=1008, reason="Invalid token")
                    return
            except JWTError:
                await websocket.close(code=1008, reason="Invalid token")
                return
            
            # Rate limiting for WebSocket
            client_id = user
            rate_limiter = self.ws_rate_limiters[client_id]
            now = time.time()
            
            # Clean old entries
            while rate_limiter and rate_limiter[0] < now - 60:
                rate_limiter.popleft()
            
            if len(rate_limiter) >= ws_rate_limit:
                await websocket.close(code=1009, reason="Rate limit exceeded")
                return
            
            rate_limiter.append(now)
            self.websocket_connections.add(websocket)
            
            logger.info(f"WebSocket client connected: {user}, total: {len(self.websocket_connections)}")
            
            try:
                async for message in websocket:
                    data = json.loads(message)
                    if data.get('type') == 'ping':
                        await websocket.send(json.dumps({'type': 'pong', 'timestamp': datetime.now().isoformat()}))
                    elif data.get('type') == 'subscribe':
                        await self._send_status_update(websocket)
            except ConnectionClosed:
                pass
            finally:
                self.websocket_connections.remove(websocket)
                logger.info(f"WebSocket client disconnected: {user}")
        
        try:
            async with serve(handler, host, port):
                logger.info(f"WebSocket server started on ws://{host}:{port}")
                await asyncio.Future()
        except Exception as e:
            logger.error(f"WebSocket server failed to start: {e}")
    
    async def _send_status_update(self, websocket):
        """Send status update to WebSocket client"""
        status = self.get_system_status()
        await websocket.send(json.dumps({
            'type': 'status_update',
            'data': status,
            'timestamp': datetime.now().isoformat()
        }))
    
    def _discover_enhancement_modules(self):
        """Auto-discover enhancement modules"""
        discovery_map = self._get_enhanced_discovery_map()
        
        discovered_count = 0
        for component_name, config in discovery_map.items():
            try:
                instance = self._try_import_component(config)
                if instance:
                    self.register_component(component_name, instance, config.get('category', 'general'))
                    discovered_count += 1
                    MODULE_INTEGRATION.labels(module_name=component_name).set(1)
                else:
                    MODULE_INTEGRATION.labels(module_name=component_name).set(0)
            except Exception as e:
                logger.debug(f"Module {component_name} not available: {e}")
                MODULE_INTEGRATION.labels(module_name=component_name).set(0)
        
        logger.info(f"Discovered {discovered_count}/{len(discovery_map)} enhancement modules")
    
    def _get_enhanced_discovery_map(self) -> Dict:
        """Get enhanced discovery map with all module configurations"""
        return {
            'helium_data_collector': {
                'module': 'control_system',
                'class': 'HeliumDataCollector',
                'factory': None,
                'category': 'helium',
                'version': '2.0'
            },
            'regret_optimizer': {
                'module': 'control_system',
                'class': 'RegretOptimizer',
                'factory': None,
                'category': 'optimization',
                'version': '1.0'
            },
            'thermal_optimizer': {
                'module': 'control_system',
                'class': 'ThermalOptimizer',
                'factory': None,
                'category': 'optimization',
                'version': '1.0'
            }
        }
    
    def _try_import_component(self, config: Dict) -> Optional[Any]:
        """Try to import and instantiate a component"""
        module_name = config.get('module')
        class_name = config.get('class')
        
        if module_name == 'control_system':
            if class_name in globals():
                try:
                    return globals()[class_name]()
                except Exception as e:
                    logger.debug(f"Failed to instantiate {class_name}: {e}")
        
        return None
    
    def register_component(self, name: str, instance: Any, category: str = "general"):
        """Register a component with the control system"""
        if isinstance(instance, HealthCheckable):
            health_status = instance.health_check()
            initial_status = ComponentStatus.HEALTHY if health_status.get('healthy', True) else ComponentStatus.DEGRADED
        else:
            initial_status = ComponentStatus.HEALTHY
        
        self.components[name] = ComponentInfo(
            name=name,
            instance=instance,
            status=initial_status,
            registered_at=datetime.now(),
            health_score=1.0 if initial_status == ComponentStatus.HEALTHY else 0.5
        )
        
        COMPONENT_HEALTH.labels(component_name=name).set(1 if initial_status == ComponentStatus.HEALTHY else 0)
        audit_logger.info(f"Component registered: {name}", extra={
            'component_name': name,
            'category': category,
            'status': initial_status.value
        })
        logger.info(f"Component registered: {name} (category: {category})")
    
    def get_component(self, name: str) -> Optional[Any]:
        """Get a registered component instance"""
        info = self.components.get(name)
        return info.instance if info else None
    
    def check_component_health(self, name: str) -> bool:
        """Check health of a specific component"""
        if name not in self.components:
            return False
        
        info = self.components[name]
        
        with self.tracer.start_as_current_span(f"health_check_{name}") as span:
            try:
                instance = info.instance
                
                if isinstance(instance, HealthCheckable):
                    result = instance.health_check()
                    healthy = result.get('healthy', True)
                    info.health_score = result.get('score', 1.0 if healthy else 0.5)
                elif hasattr(instance, 'is_data_fresh'):
                    healthy = instance.is_data_fresh()
                    info.health_score = 1.0 if healthy else 0.5
                elif hasattr(instance, 'get_statistics'):
                    stats = instance.get_statistics()
                    healthy = len(stats) > 0
                    info.health_score = 0.9 if healthy else 0.3
                else:
                    healthy = True
                    info.health_score = 0.8
                
                info.status = ComponentStatus.HEALTHY if healthy else ComponentStatus.DEGRADED
                info.last_health_check = datetime.now()
                
                span.set_attribute("component.healthy", healthy)
                span.set_attribute("component.health_score", info.health_score)
                
                COMPONENT_HEALTH.labels(component_name=name).set(1 if healthy else 0)
                
                if not healthy:
                    audit_logger.warning(f"Component unhealthy: {name}", extra={
                        'component_name': name,
                        'health_score': info.health_score
                    })
                
                return healthy
                
            except Exception as e:
                info.status = ComponentStatus.FAILED
                info.health_score = 0.0
                info.last_failure = datetime.now()
                info.failure_count += 1
                
                COMPONENT_HEALTH.labels(component_name=name).set(0)
                
                span.set_status(Status(StatusCode.ERROR, str(e)))
                logger.error(f"Health check failed for {name}: {e}")
                
                asyncio.create_task(self.event_bus.publish(SystemEvent(
                    event_type=EventType.COMPONENT_FAILED,
                    source='health_monitor',
                    data={'component_name': name, 'error': str(e)}
                )))
                
                return False
    
    def check_all_components_health(self) -> Dict:
        """Check health of all registered components"""
        results = {}
        for name in self.components:
            results[name] = self.check_component_health(name)
        
        healthy_count = sum(1 for v in results.values() if v)
        total_count = len(results)
        
        return {
            'total': total_count,
            'healthy': healthy_count,
            'degraded': total_count - healthy_count,
            'health_pct': (healthy_count / max(total_count, 1)) * 100,
            'components': results
        }
    
    async def _throttle_non_critical_tasks(self):
        """Throttle non-critical tasks during helium scarcity"""
        non_critical = {'synthetic_data_generation', 'model_training', 'batch_processing', 'report_generation'}
        self.throttled_tasks.update(non_critical)
        audit_logger.warning(f"Throttled tasks due to helium scarcity: {non_critical}")
    
    async def _restore_throttled_tasks(self):
        """Restore throttled tasks when helium scarcity eases"""
        self.throttled_tasks.clear()
        audit_logger.info("Restored all throttled tasks")
    
    async def update_helium_status(self):
        """Update helium scarcity status from collector"""
        helium_collector = self.get_component('helium_data_collector')
        
        if helium_collector:
            with self.tracer.start_as_current_span("update_helium_status") as span:
                try:
                    latest = helium_collector.get_latest()
                    if latest:
                        new_scarcity = getattr(latest, 'scarcity_index', 0.5)
                        old_scarcity = self.helium_scarcity_level
                        self.helium_scarcity_level = new_scarcity
                        
                        span.set_attribute("old_scarcity", old_scarcity)
                        span.set_attribute("new_scarcity", new_scarcity)
                        
                        if new_scarcity > self.config['helium']['scarcity_threshold'] and old_scarcity <= self.config['helium']['scarcity_threshold']:
                            await self.event_bus.publish(SystemEvent(
                                event_type=EventType.HELIUM_SCARCITY,
                                source='control_system',
                                data={'scarcity_index': new_scarcity, 'old_scarcity': old_scarcity}
                            ))
                        
                except Exception as e:
                    logger.warning(f"Helium status update failed: {e}")
                    span.set_status(Status(StatusCode.ERROR, str(e)))
    
    async def execute_task(self, task_type: str, task_data: Dict = None) -> Dict:
        """Execute a task asynchronously with queue"""
        future = asyncio.Future()
        await self.task_queue.put((task_type, task_data or {}, future))
        QUEUE_SIZE.set(self.task_queue.qsize())
        return await future
    
    async def run_gradual_cycle(self):
        """Run enhanced gradual cyclic orchestration"""
        logger.info("Starting enhanced gradual cyclic orchestration...")
        cycle_results = {'phases': {}, 'trace_id': get_trace_id(), 'timestamp': datetime.now().isoformat()}
        
        with self.tracer.start_as_current_span("gradual_cycle") as span:
            try:
                # Phase 1: Update helium status
                await self.update_helium_status()
                cycle_results['phases']['helium_update'] = {
                    'scarcity_level': self.helium_scarcity_level,
                    'status': 'completed'
                }
                
                # Phase 2: Run regret optimization
                regret_optimizer = self.get_component('regret_optimizer')
                if regret_optimizer:
                    try:
                        decisions = [{'id': 'sample1', 'alternatives': []}]
                        outcomes = [{'metrics': {'latency': 50, 'carbon': 300, 'cost': 100}}]
                        regret_result = regret_optimizer.calculate_regret(decisions, outcomes)
                        cycle_results['phases']['regret_optimization'] = {
                            'avg_regret': regret_result.get('average_regret'),
                            'status': 'completed'
                        }
                    except Exception as e:
                        logger.warning(f"Regret optimization skipped: {e}")
                        cycle_results['phases']['regret_optimization'] = {'error': str(e)}
                
                # Phase 3: Run thermal optimization
                thermal_optimizer = self.get_component('thermal_optimizer')
                if thermal_optimizer:
                    try:
                        thermal_result = thermal_optimizer.run_optimization(
                            'liquid_cooled', 75, self.helium_scarcity_level
                        )
                        cycle_results['phases']['thermal_optimization'] = {
                            'efficiency': thermal_result.get('efficiency'),
                            'target_temp': thermal_result.get('target_temperature_c'),
                            'status': 'completed'
                        }
                    except Exception as e:
                        logger.warning(f"Thermal optimization skipped: {e}")
                        cycle_results['phases']['thermal_optimization'] = {'error': str(e)}
                
                # Phase 4: Calculate system health
                health = self.check_all_components_health()
                cycle_results['phases']['health_check'] = {
                    'healthy_components': health['healthy'],
                    'total_components': health['total'],
                    'health_pct': health['health_pct']
                }
                
                span.set_attribute("cycle.phases_completed", len(cycle_results['phases']))
                span.set_attribute("system.health_pct", health['health_pct'])
                
                audit_logger.info("Gradual cycle completed", extra={
                    'phases': len(cycle_results['phases']),
                    'health_pct': health['health_pct'],
                    'helium_scarcity': self.helium_scarcity_level
                })
                
                logger.info("Gradual cyclic orchestration completed")
                
            except Exception as e:
                logger.error(f"Gradual cycle failed: {e}")
                cycle_results['error'] = str(e)
                span.set_status(Status(StatusCode.ERROR, str(e)))
        
        return cycle_results
    
    async def _health_monitor_loop(self):
        """Background health monitoring loop"""
        interval = self.config['monitoring']['health_check_interval']
        while True:
            try:
                self.check_all_components_health()
                SYSTEM_UPTIME.set((datetime.now() - self.start_time).total_seconds())
                DEAD_LETTER_COUNT.set(len(self.event_bus.dead_letter_events))
                await asyncio.sleep(interval)
            except Exception as e:
                logger.error(f"Health monitor error: {e}")
                await asyncio.sleep(60)
    
    async def _helium_update_loop(self):
        """Background helium status update loop"""
        while True:
            try:
                await self.update_helium_status()
                await asyncio.sleep(300)  # Every 5 minutes
            except Exception as e:
                logger.error(f"Helium update error: {e}")
                await asyncio.sleep(600)
    
    async def _gradual_cycle_loop(self):
        """Background gradual cyclic orchestration loop"""
        await asyncio.sleep(60)  # Wait for system to stabilize
        while True:
            try:
                await self.run_gradual_cycle()
                await asyncio.sleep(3600)  # Every hour
            except Exception as e:
                logger.error(f"Gradual cycle error: {e}")
                await asyncio.sleep(7200)
    
    def get_system_status(self) -> Dict:
        """Get comprehensive system status"""
        health = self.check_all_components_health()
        
        return {
            'system': {
                'name': 'Green Agent Control System',
                'version': '8.0',
                'instance_id': self.instance_id,
                'is_leader': self.leader_election.is_leader,
                'uptime_seconds': (datetime.now() - self.start_time).total_seconds(),
                'started_at': self.start_time.isoformat()
            },
            'components': {
                'total': len(self.components),
                'healthy': health['healthy'],
                'degraded': health['degraded'],
                'health_pct': health['health_pct'],
                'details': {
                    name: {
                        'status': info.status.value,
                        'health_score': info.health_score,
                        'registered_at': info.registered_at.isoformat(),
                        'failure_count': info.failure_count
                    }
                    for name, info in self.components.items()
                }
            },
            'events': self.event_bus.get_statistics(),
            'tasks': {
                'total_executed': len(self.task_history),
                'active': ACTIVE_TASKS._value.get(),
                'queue_size': self.task_queue.qsize(),
                'throttled': list(self.throttled_tasks),
                'recent_success_rate': self._calculate_success_rate()
            },
            'helium': {
                'scarcity_level': self.helium_scarcity_level,
                'throttled_tasks': len(self.throttled_tasks),
                'is_throttling': len(self.throttled_tasks) > 0
            },
            'api': {
                'routes': len(self.api_gateway.routes),
                'versions': list(self.api_gateway.versioned_routes.keys()),
                'requests_processed': len(self.api_gateway.request_history)
            },
            'secrets': {
                'stored': len(self.secrets_manager.secrets_store),
                'needing_rotation': len(self.secrets_manager.check_rotation_needed())
            },
            'websocket': {
                'connections': len(self.websocket_connections),
                'enabled': self.config['websocket']['enabled'],
                'rate_limit': self.config['websocket']['rate_limit']
            },
            'bulkheads': {
                name: {
                    'active': bh.active_count,
                    'queued': bh.queue.qsize(),
                    'max_concurrent': bh.max_concurrent
                }
                for name, bh in self.bulkheads.items()
            }
        }
    
    def _calculate_success_rate(self) -> float:
        """Calculate recent task success rate"""
        recent_tasks = list(self.task_history)[-100:]
        if not recent_tasks:
            return 1.0
        
        successes = sum(1 for t in recent_tasks if t.get('status') == 'success')
        return successes / len(recent_tasks)
    
    def get_integration_report(self) -> Dict:
        """Get report on module integration status"""
        modules_found = list(self.components.keys())
        
        expected_modules = [
            'helium_data_collector', 'regret_optimizer', 'thermal_optimizer'
        ]
        
        missing = [m for m in expected_modules if m not in modules_found]
        
        return {
            'total_expected': len(expected_modules),
            'total_integrated': len(modules_found),
            'integration_pct': (len(modules_found) / max(len(expected_modules), 1)) * 100,
            'integrated_modules': modules_found,
            'missing_modules': missing,
            'integration_ready': len(missing) == 0,
            'version': '8.0',
            'instance_id': self.instance_id
        }
    
    async def shutdown(self):
        """Graceful shutdown with task draining"""
        logger.info("Initiating graceful shutdown...")
        
        # 1. Stop accepting new tasks
        self.accepting_tasks = False
        
        # 2. Wait for active tasks to complete (with timeout)
        active = ACTIVE_TASKS._value.get()
        if active > 0:
            logger.info(f"Waiting for {active} active tasks to complete...")
            try:
                await asyncio.wait_for(self._tasks_completed.wait(), timeout=30.0)
            except asyncio.TimeoutError:
                logger.warning("Timeout waiting for tasks to complete")
        
        # 3. Drain task queue
        queue_size = self.task_queue.qsize()
        if queue_size > 0:
            logger.info(f"Draining {queue_size} pending tasks...")
            while not self.task_queue.empty():
                try:
                    self.task_queue.get_nowait()
                except asyncio.QueueEmpty:
                    break
        
        # 4. Release leadership
        await self.leader_election.release_leadership()
        
        # 5. Close connections
        await self.persistence.close()
        await self.secrets_manager.close()
        
        # 6. Cancel background tasks
        for task in self.background_tasks:
            task.cancel()
        
        # 7. Save final state
        await self._save_state()
        
        audit_logger.info("System shutdown complete", extra={
            'uptime_seconds': (datetime.now() - self.start_time).total_seconds(),
            'final_components': len(self.components)
        })
        
        logger.info("Graceful shutdown complete")
    
    async def _save_state(self):
        """Save system state to disk"""
        state = {
            'helium_scarcity_level': self.helium_scarcity_level,
            'throttled_tasks': list(self.throttled_tasks),
            'components': {
                name: {
                    'status': info.status.value,
                    'health_score': info.health_score,
                    'registered_at': info.registered_at.isoformat(),
                    'failure_count': info.failure_count
                }
                for name, info in self.components.items()
            },
            'statistics': self.get_system_status()
        }
        
        state_file = Path('control_system_state.json')
        with open(state_file, 'w') as f:
            json.dump(state, f, indent=2, default=str)
        
        logger.info(f"System state saved to {state_file}")

# ============================================================
# MAIN DEMONSTRATION
# ============================================================

async def main_v8():
    """Enhanced V8.0 demonstration"""
    print("=" * 80)
    print("Green Agent Control System v8.0 - Platinum Enhanced Demo")
    print("=" * 80)
    
    # Create example configuration
    example_config = {
        'system': {'name': 'Green Agent', 'version': '8.0'},
        'helium': {'scheduling_enabled': True, 'scarcity_threshold': 0.7},
        'security': {'jwt_secret': 'a-very-secure-secret-key-that-is-32-chars-long', 
                    'jwt_expiry_seconds': 3600, 'rate_limit': 100},
        'monitoring': {'health_check_interval': 30, 'metrics_enabled': True},
        'websocket': {'enabled': True, 'host': 'localhost', 'port': 8765, 'rate_limit': 60},
        'tracing': {'enabled': True, 'sampling_rate': 0.1},
        'persistence_backend': 'sqlite'
    }
    
    if not Path('control_system_config.yaml').exists():
        with open('control_system_config.yaml', 'w') as f:
            yaml.dump(example_config, f)
        print("✅ Created example configuration file: control_system_config.yaml")
    
    # Initialize control system
    control = GreenAgentControlSystem('control_system_config.yaml')
    
    print(f"\n✅ V8.0 Platinum Enhancements Active:")
    print(f"   ✅ State Persistence (SQLite)")
    print(f"   ✅ Distributed Circuit Breakers")
    print(f"   ✅ Configuration Validation (Pydantic)")
    print(f"   ✅ WebSocket Authentication (JWT)")
    print(f"   ✅ Leader Election (Distributed)")
    print(f"   ✅ Bulkhead Pattern (Task Isolation)")
    print(f"   ✅ Configuration Hot-Reload")
    print(f"   ✅ API Versioning (v1, v2)")
    print(f"   ✅ Metrics Export to Pushgateway")
    
    print(f"\n🔧 Configuration:")
    print(f"   Instance ID: {control.instance_id}")
    print(f"   Leader: {'✅' if control.leader_election.is_leader else '❌'}")
    print(f"   Persistence: {control.config.get('persistence_backend', 'memory')}")
    print(f"   Bulkheads: {len(control.bulkheads)}")
    
    # Show discovered modules
    print(f"\n📦 Discovered Enhancement Modules:")
    for name, info in control.components.items():
        print(f"   ✅ {name}: {info.status.value}")
    
    # Integration report
    report = control.get_integration_report()
    print(f"\n🔗 Integration Report:")
    print(f"   Integrated: {report['total_integrated']}/{report['total_expected']} modules")
    print(f"   Integration: {report['integration_pct']:.0f}%")
    print(f"   Version: {report['version']}")
    
    if report['missing_modules']:
        print(f"   Missing: {', '.join(report['missing_modules'])}")
    
    # Test API endpoints
    print(f"\n🌐 API Gateway Test:")
    
    # Get JWT token
    token_response = await control.api_gateway.handle_request({
        'path': '/v1/token',
        'method': 'POST',
        'data': {'username': 'admin', 'password': 'admin123'},
        'client_id': 'demo'
    })
    
    if 'token' in token_response:
        token = token_response['token']
        print(f"   ✅ JWT Token Generated")
        
        # Test versioned endpoints
        endpoints = [
            ('/v1/health', 'GET'),
            ('/v1/status', 'GET'),
            ('/v2/status', 'GET'),
            ('/v1/components', 'GET'),
            ('/v1/helium/status', 'GET'),
            ('/v1/health/detailed', 'GET')
        ]
        
        for path, method in endpoints:
            response = await control.api_gateway.handle_request({
                'path': path,
                'method': method,
                'client_id': 'demo',
                'headers': {'Authorization': f'Bearer {token}'}
            })
            status = response.get('status', response.get('error', 'unknown'))
            print(f"   {path}: {status}")
    
    # Test task execution
    print(f"\n⚙️ Task Execution with Bulkhead Isolation:")
    
    # Execute tasks with different bulkheads
    tasks = [
        ('helium_collect', {}, 'helium_tasks'),
        ('regret_optimize', {}, 'general_tasks'),
        ('thermal_optimize', {'cooling_type': 'liquid_cooled', 'load_pct': 75, 'helium_scarcity': 0.5}, 'general_tasks')
    ]
    
    for task_type, task_data, bulkhead in tasks:
        bulkhead_info = control.bulkheads.get(bulkhead)
        if bulkhead_info:
            result = await control.execute_task(task_type, task_data)
            status = result.get('status', 'unknown')
            print(f"   {task_type} ({bulkhead}): {status}")
    
    # Run gradual cycle
    print(f"\n🔄 Gradual Cyclic Orchestration:")
    cycle = await control.run_gradual_cycle()
    phases_completed = len(cycle.get('phases', {}))
    print(f"   Phases completed: {phases_completed}")
    for phase_name, phase_data in cycle.get('phases', {}).items():
        if 'error' not in phase_data:
            print(f"      ✅ {phase_name}: completed")
        else:
            print(f"      ⚠️ {phase_name}: {phase_data['error'][:50]}")
    
    # System status
    status = control.get_system_status()
    print(f"\n📊 System Status:")
    print(f"   Components: {status['components']['total']}")
    print(f"   Healthy: {status['components']['healthy']}")
    print(f"   Health %: {status['components']['health_pct']:.1f}%")
    print(f"   Uptime: {status['system']['uptime_seconds']:.0f}s")
    print(f"   Helium Scarcity: {status['helium']['scarcity_level']:.2f}")
    print(f"   Task Success Rate: {status['tasks']['recent_success_rate']:.1%}")
    print(f"   Queue Size: {status['tasks']['queue_size']}")
    
    # Bulkhead status
    print(f"\n🔒 Bulkhead Status:")
    for name, bh in control.bulkheads.items():
        print(f"   {name}: active={bh.active_count}, queued={bh.queue.qsize()}")
    
    # Health check
    health = await control.api_gateway.handle_request({
        'path': '/v1/health/detailed',
        'method': 'GET',
        'client_id': 'demo',
        'headers': {'Authorization': f'Bearer {token}'}
    })
    print(f"\n🏥 Detailed Health Check:")
    print(f"   Status: {health.get('status', 'unknown')}")
    print(f"   Instance: {health.get('instance_id', 'unknown')}")
    print(f"   Leader: {'✅' if health.get('is_leader') else '❌'}")
    
    # Shutdown gracefully
    await control.shutdown()
    
    print("\n" + "=" * 80)
    print("✅ Green Agent Control System v8.0 - Demo Complete")
    print(f"   {report['total_integrated']} modules integrated")
    print(f"   Audit trail saved to audit.log")
    print(f"   System state saved to control_system_state.json")
    print("=" * 80)
    
    return control

if __name__ == "__main__":
    print("Running V8.0 enhanced version with all critical fixes and improvements...")
    asyncio.run(main_v8())
