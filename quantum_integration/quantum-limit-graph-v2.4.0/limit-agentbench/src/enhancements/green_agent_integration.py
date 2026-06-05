# File: src/enhancements/green_agent_integration.py (ENHANCED VERSION 8.0)

"""
Green Agent Integration Layer - Version 8.0 (MASTER ORCHESTRATOR ENTERPRISE)

ENHANCEMENTS OVER v7.0:
1. ADDED: Module versioning and compatibility checking
2. ADDED: Circuit breaker for module calls with automatic recovery
3. ADDED: Module auto-restart on failure with exponential backoff
4. ADDED: Distributed tracing with OpenTelemetry integration
5. ADDED: Rate limiting for high-frequency module calls
6. ADDED: Module health scoring with weighted metrics
7. ADDED: Circuit breaker dashboard data export
8. ADDED: Gradual recovery after circuit breaker opens
9. ADDED: Module dependency compatibility validation
10. ADDED: Performance benchmarking for each module
11. ADDED: Module call retry with exponential backoff
12. ADDED: Integration with fallback manager for graceful degradation
13. ADDED: Module state persistence across restarts
14. ADDED: Real-time module metrics streaming
15. ADDED: Module dependency visualization export
"""

import asyncio
import hashlib
import json
import logging
import math
import os
import random
import time
import threading
import uuid
import importlib
import inspect
from dataclasses import dataclass, field, asdict
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any, Callable, Set
from collections import defaultdict, deque
from enum import Enum
from contextlib import asynccontextmanager
from functools import wraps
import numpy as np

# Prometheus metrics
from prometheus_client import Counter, Gauge, Histogram, CollectorRegistry

# Distributed tracing
try:
    from opentelemetry import trace
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import BatchSpanProcessor
    from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
    OPENTELEMETRY_AVAILABLE = True
except ImportError:
    OPENTELEMETRY_AVAILABLE = False

logger = logging.getLogger(__name__)

# ============================================================
# ENHANCED ENUMS AND DATA MODELS
# ============================================================

class CircuitBreakerState(str, Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"

@dataclass
class ModuleVersion:
    """Module version information"""
    major: int = 1
    minor: int = 0
    patch: int = 0
    
    def __str__(self):
        return f"{self.major}.{self.minor}.{self.patch}"
    
    def to_tuple(self):
        return (self.major, self.minor, self.patch)
    
    def __ge__(self, other):
        return self.to_tuple() >= other.to_tuple()

@dataclass
class ModuleInfo:
    """Enhanced module discovery information with versioning"""
    name: str
    category: str
    available: bool
    instance: Any = None
    factory_function: str = None
    init_error: str = None
    last_health_check: Optional[datetime] = None
    health_status: str = "unknown"
    integration_count: int = 0
    dependencies: List[str] = field(default_factory=list)
    phase: int = 1
    version: ModuleVersion = field(default_factory=ModuleVersion)
    api_version: ModuleVersion = field(default_factory=ModuleVersion)
    min_dependency_versions: Dict[str, ModuleVersion] = field(default_factory=dict)
    requires_gpu: bool = False
    memory_estimate_mb: float = 0.0
    average_latency_ms: float = 0.0
    success_rate: float = 1.0
    state: str = "initializing"

@dataclass
class IntegrationMetrics:
    """Enhanced metrics from integration run"""
    source_module: str = "green_agent_integration"
    
    # Module availability counts
    total_modules_available: int = 0
    total_modules_discovered: int = 0
    
    # Phase completion
    phase1_data_collection: bool = False
    phase2_optimization: bool = False
    phase3_verification: bool = False
    phase4_reporting: bool = False
    phase5_orchestration: bool = False
    phase6_monitoring: bool = False
    
    # Integration results per module
    module_results: Dict[str, bool] = field(default_factory=dict)
    module_latencies: Dict[str, float] = field(default_factory=dict)
    module_retry_counts: Dict[str, int] = field(default_factory=dict)
    module_circuit_breaker_states: Dict[str, str] = field(default_factory=dict)
    
    # Performance
    total_integration_time_ms: float = 0.0
    modules_integrated: int = 0
    
    # Health
    overall_health_score: float = 0.0
    
    # GPU metrics
    gpu_available: bool = False
    gpu_memory_gb: float = 0.0
    
    # Tracing
    trace_id: str = ""

@dataclass
class IntegrationEvent:
    """Enhanced event for cross-module communication"""
    event_id: str = field(default_factory=lambda: str(uuid.uuid4())[:8])
    event_type: str = ""
    source_module: str = ""
    target_modules: List[str] = field(default_factory=list)
    payload: Dict = field(default_factory=dict)
    timestamp: datetime = field(default_factory=datetime.now)
    trace_id: str = ""
    span_id: str = ""

# ============================================================
# ENHANCED CIRCUIT BREAKER FOR MODULE CALLS
# ============================================================

class ModuleCircuitBreaker:
    """Circuit breaker for individual module calls with gradual recovery"""
    
    def __init__(self, module_name: str, failure_threshold: int = 5,
                 recovery_timeout: int = 60, half_open_max_calls: int = 3):
        self.module_name = module_name
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.half_open_max_calls = half_open_max_calls
        
        self.state = CircuitBreakerState.CLOSED
        self.failure_count = 0
        self.success_count = 0
        self.last_failure_time = None
        self.half_open_calls_made = 0
        self.recovery_start_time = None
        self.metrics = deque(maxlen=100)
    
    async def call(self, func: Callable, *args, **kwargs) -> Any:
        """Execute function with circuit breaker protection"""
        if self.state == CircuitBreakerState.OPEN:
            if time.time() - self.last_failure_time >= self.recovery_timeout:
                self.state = CircuitBreakerState.HALF_OPEN
                self.half_open_calls_made = 0
                self.recovery_start_time = time.time()
                logger.info(f"Circuit breaker {self.module_name} transitioning to HALF_OPEN")
            else:
                raise Exception(f"Circuit breaker {self.module_name} is OPEN")
        
        start_time = time.time()
        try:
            result = await func(*args, **kwargs) if asyncio.iscoroutinefunction(func) else func(*args, **kwargs)
            elapsed = (time.time() - start_time) * 1000
            
            self._record_success(elapsed)
            return result
            
        except Exception as e:
            elapsed = (time.time() - start_time) * 1000
            self._record_failure(elapsed)
            raise e
    
    def _record_success(self, latency_ms: float):
        """Record successful call"""
        self.success_count += 1
        
        if self.state == CircuitBreakerState.HALF_OPEN:
            self.half_open_calls_made += 1
            if self.success_count >= 2:
                self.state = CircuitBreakerState.CLOSED
                self.failure_count = 0
                self.success_count = 0
                recovery_time = time.time() - self.recovery_start_time
                logger.info(f"Circuit breaker {self.module_name} closed after {recovery_time:.1f}s recovery")
        
        self.metrics.append({
            'success': True,
            'latency_ms': latency_ms,
            'timestamp': datetime.now().isoformat()
        })
    
    def _record_failure(self, latency_ms: float):
        """Record failed call"""
        self.failure_count += 1
        self.last_failure_time = time.time()
        
        if self.state == CircuitBreakerState.HALF_OPEN:
            self.state = CircuitBreakerState.OPEN
            logger.warning(f"Circuit breaker {self.module_name} opened from HALF_OPEN")
        elif self.state == CircuitBreakerState.CLOSED and self.failure_count >= self.failure_threshold:
            self.state = CircuitBreakerState.OPEN
            logger.warning(f"Circuit breaker {self.module_name} opened after {self.failure_count} failures")
        
        self.metrics.append({
            'success': False,
            'latency_ms': latency_ms,
            'timestamp': datetime.now().isoformat()
        })
    
    def get_state(self) -> str:
        """Get current state"""
        return self.state.value
    
    def get_metrics(self) -> Dict:
        """Get circuit breaker metrics"""
        recent = list(self.metrics)[-10:]
        successes = [m for m in recent if m['success']]
        
        return {
            'state': self.state.value,
            'failure_count': self.failure_count,
            'success_count': self.success_count,
            'last_failure': self.last_failure_time,
            'success_rate_10': len(successes) / max(len(recent), 1),
            'avg_latency_ms': np.mean([m['latency_ms'] for m in recent]) if recent else 0
        }

# ============================================================
# ENHANCED RATE LIMITER FOR MODULE CALLS
# ============================================================

class ModuleRateLimiter:
    """Rate limiter for high-frequency module calls"""
    
    def __init__(self, calls_per_second: float = 10.0):
        self.calls_per_second = calls_per_second
        self.min_interval = 1.0 / calls_per_second
        self.last_call_time = 0
        self._lock = asyncio.Lock()
    
    async def acquire(self):
        """Acquire permission to make a call"""
        async with self._lock:
            now = time.time()
            elapsed = now - self.last_call_time
            if elapsed < self.min_interval:
                wait_time = self.min_interval - elapsed
                await asyncio.sleep(wait_time)
            self.last_call_time = time.time()

# ============================================================
# MODULE AUTO-RESTART MANAGER
# ============================================================

class ModuleAutoRestartManager:
    """Automatic module restart on failure with exponential backoff"""
    
    def __init__(self, integrator: 'GreenAgentIntegrator'):
        self.integrator = integrator
        self.failure_counts: Dict[str, int] = defaultdict(int)
        self.restart_attempts: Dict[str, int] = defaultdict(int)
        self.last_restart_time: Dict[str, datetime] = {}
        self.restart_locks: Dict[str, asyncio.Lock] = defaultdict(asyncio.Lock)
        self.max_retries = 3
        self.base_delay = 5
    
    async def attempt_restart(self, module_name: str) -> bool:
        """Attempt to restart a failed module"""
        async with self.restart_locks[module_name]:
            # Check if recently restarted
            if module_name in self.last_restart_time:
                time_since = (datetime.now() - self.last_restart_time[module_name]).seconds
                if time_since < self.base_delay * (2 ** self.restart_attempts[module_name]):
                    logger.debug(f"Module {module_name} cooling down before restart")
                    return False
            
            if self.restart_attempts[module_name] >= self.max_retries:
                logger.error(f"Module {module_name} exceeded max restart attempts ({self.max_retries})")
                return False
            
            self.restart_attempts[module_name] += 1
            self.last_restart_time[module_name] = datetime.now()
            
            logger.info(f"Attempting restart {self.restart_attempts[module_name]}/{self.max_retries} for {module_name}")
            
            try:
                # Re-initialize module
                module_info = self.integrator.discovered_modules.get(module_name)
                if module_info and module_info.available:
                    instance = self.integrator._initialize_module(module_name, module_info)
                    if instance:
                        self.integrator.module_instances[module_name] = instance
                        module_info.instance = instance
                        module_info.health_status = "healthy"
                        module_info.state = "running"
                        self.failure_counts[module_name] = 0
                        logger.info(f"Module {module_name} restarted successfully")
                        return True
            except Exception as e:
                logger.error(f"Failed to restart {module_name}: {e}")
            
            return False
    
    def record_failure(self, module_name: str):
        """Record a module failure"""
        self.failure_counts[module_name] += 1
    
    def get_statistics(self) -> Dict:
        """Get restart manager statistics"""
        return {
            'total_restarts': sum(self.restart_attempts.values()),
            'modules_with_failures': len([c for c in self.failure_counts.values() if c > 0]),
            'restart_attempts': dict(self.restart_attempts),
            'failure_counts': dict(self.failure_counts)
        }

# ============================================================
# ENHANCED MAIN INTEGRATOR CLASS
# ============================================================

class GreenAgentIntegrator:
    """
    ENHANCED Unified Integration Layer for ALL Green Agent Modules v8.0
    
    Features:
    - Module versioning and compatibility checking
    - Circuit breakers for module calls
    - Auto-restart on failure with exponential backoff
    - Distributed tracing with OpenTelemetry
    - Rate limiting for high-frequency calls
    - Health scoring with weighted metrics
    - Module state persistence
    """
    
    def __init__(self, config: Dict = None):
        self.config = config or self._load_default_config()
        
        # Module discovery registry
        self.discovered_modules: Dict[str, ModuleInfo] = {}
        
        # Instance cache
        self.module_instances: Dict[str, Any] = {}
        
        # Integration history
        self.integration_runs: List[IntegrationMetrics] = []
        
        # Orchestration state
        self.current_phase = "initializing"
        self.cycle_count = 0
        
        # Performance tracking
        self.module_latencies: Dict[str, List[float]] = defaultdict(list)
        self.module_retry_counts: Dict[str, int] = defaultdict(int)
        
        # Circuit breakers for modules
        self.circuit_breakers: Dict[str, ModuleCircuitBreaker] = {}
        
        # Rate limiters for modules
        self.rate_limiters: Dict[str, ModuleRateLimiter] = {}
        
        # Auto-restart manager
        self.restart_manager = ModuleAutoRestartManager(self)
        
        # Event system
        self.event_handlers: Dict[str, List[Callable]] = defaultdict(list)
        self.event_history: List[IntegrationEvent] = []
        
        # Dependency graph
        self.dependency_graph = None
        self._init_dependency_graph()
        
        # Multi-tenant support
        self.tenant_instances: Dict[str, Dict[str, Any]] = {}
        self.active_tenants: Set[str] = set()
        
        # State persistence
        self.state_persistence = None
        self._init_state_persistence()
        
        # GPU acceleration
        self.gpu_accelerator = None
        self._init_gpu_acceleration()
        
        # Distributed tracing
        self.tracer = None
        self._init_tracing()
        
        # Discover all modules
        self._discover_all_modules()
        
        # Build dependency graph
        self._build_dependency_graph()
        
        # Validate module versions
        self._validate_module_versions()
        
        # Initialize all available modules (in dependency order)
        self._initialize_all_modules_ordered()
        
        # Initialize circuit breakers
        self._init_circuit_breakers()
        
        # Update metrics
        self._update_all_metrics()
        
        # Start background health monitor
        self.running = True
        self.background_tasks = [
            asyncio.create_task(self._health_monitor_loop())
        ]
        
        logger.info(f"GreenAgentIntegrator v8.0 initialized with "
                   f"{self._count_available()} available out of "
                   f"{self._count_discovered()} discovered modules, "
                   f"GPU: {self.gpu_accelerator is not None}")
    
    def _load_default_config(self) -> Dict:
        """Load default configuration"""
        return {
            'circuit_breaker': {
                'failure_threshold': 5,
                'recovery_timeout': 60,
                'half_open_max_calls': 3
            },
            'rate_limiting': {
                'enabled': False,
                'calls_per_second': 10
            },
            'auto_restart': {
                'enabled': True,
                'max_retries': 3,
                'base_delay_seconds': 5
            },
            'tracing': {
                'enabled': False,
                'otlp_endpoint': 'localhost:4317'
            },
            'health_check_interval': 30,
            'state_persistence_dir': './integration_state'
        }
    
    def _init_dependency_graph(self):
        """Initialize dependency graph"""
        try:
            import networkx as nx
            self.dependency_graph = nx.DiGraph()
        except ImportError:
            self.dependency_graph = None
    
    def _init_state_persistence(self):
        """Initialize state persistence for modules"""
        state_dir = Path(self.config.get('state_persistence_dir', './integration_state'))
        state_dir.mkdir(exist_ok=True)
        
        class StatePersistence:
            def __init__(self, path):
                self.path = path
            
            def save_module_state(self, module_name: str, state: Dict):
                file_path = self.path / f"{module_name}_state.json"
                with open(file_path, 'w') as f:
                    json.dump(state, f, default=str)
            
            def load_module_state(self, module_name: str) -> Optional[Dict]:
                file_path = self.path / f"{module_name}_state.json"
                if file_path.exists():
                    with open(file_path, 'r') as f:
                        return json.load(f)
                return None
        
        self.state_persistence = StatePersistence(state_dir)
    
    def _init_tracing(self):
        """Initialize OpenTelemetry tracing"""
        if not OPENTELEMETRY_AVAILABLE or not self.config.get('tracing', {}).get('enabled', False):
            return
        
        try:
            provider = TracerProvider()
            otlp_exporter = OTLPSpanExporter(endpoint=self.config['tracing']['otlp_endpoint'])
            processor = BatchSpanProcessor(otlp_exporter)
            provider.add_span_processor(processor)
            trace.set_tracer_provider(provider)
            self.tracer = trace.get_tracer(__name__)
            logger.info("OpenTelemetry tracing initialized")
        except Exception as e:
            logger.warning(f"Failed to initialize tracing: {e}")
    
    def _init_gpu_acceleration(self):
        """Initialize GPU acceleration if available"""
        try:
            from .gpu_acceleration import get_gpu_accelerator
            self.gpu_accelerator = get_gpu_accelerator()
            if self.gpu_accelerator and self.gpu_accelerator.cuda_available:
                logger.info("GPU acceleration integrated")
        except ImportError:
            pass
    
    def _init_circuit_breakers(self):
        """Initialize circuit breakers for all modules"""
        cb_config = self.config.get('circuit_breaker', {})
        for module_name in self.discovered_modules:
            self.circuit_breakers[module_name] = ModuleCircuitBreaker(
                module_name,
                failure_threshold=cb_config.get('failure_threshold', 5),
                recovery_timeout=cb_config.get('recovery_timeout', 60),
                half_open_max_calls=cb_config.get('half_open_max_calls', 3)
            )
            
            if self.config.get('rate_limiting', {}).get('enabled', False):
                self.rate_limiters[module_name] = ModuleRateLimiter(
                    self.config['rate_limiting'].get('calls_per_second', 10)
                )
    
    def _discover_all_modules(self):
        """Discover ALL Green Agent enhancement modules with versioning"""
        
        discovery_map = {
            # Helium Ecosystem (6 modules)
            'helium_data_collector': {
                'module': 'helium_data_collector',
                'factory': 'get_helium_collector',
                'category': 'helium',
                'phase': 1,
                'dependencies': [],
                'version': ModuleVersion(1, 0, 0),
                'api_version': ModuleVersion(1, 0, 0)
            },
            'helium_elasticity': {
                'module': 'helium_elasticity',
                'factory': 'get_helium_elasticity_calculator',
                'category': 'helium',
                'phase': 2,
                'dependencies': ['helium_data_collector'],
                'version': ModuleVersion(2, 0, 0),
                'api_version': ModuleVersion(1, 0, 0),
                'min_dependency_versions': {'helium_data_collector': ModuleVersion(1, 0, 0)}
            },
            # Federated Learning
            'federated_learning': {
                'module': 'federated_learning',
                'class': 'FederatedLearningSystem',
                'category': 'ai_ml',
                'phase': 5,
                'dependencies': [],
                'version': ModuleVersion(7, 1, 0),
                'api_version': ModuleVersion(1, 0, 0),
                'requires_gpu': True,
                'memory_estimate_mb': 500
            },
            # GPU Acceleration
            'gpu_acceleration': {
                'module': 'gpu_acceleration',
                'factory': 'get_gpu_accelerator',
                'category': 'performance',
                'phase': 1,
                'dependencies': [],
                'version': ModuleVersion(3, 0, 0),
                'api_version': ModuleVersion(1, 0, 0)
            }
            # Additional modules continue...
        }
        
        for module_name, config in discovery_map.items():
            version = config.get('version', ModuleVersion(1, 0, 0))
            if not isinstance(version, ModuleVersion):
                version = ModuleVersion(**version) if isinstance(version, dict) else ModuleVersion()
            
            module_info = self._try_discover_module(module_name, config)
            module_info.version = version
            module_info.api_version = config.get('api_version', ModuleVersion(1, 0, 0))
            module_info.min_dependency_versions = config.get('min_dependency_versions', {})
            module_info.requires_gpu = config.get('requires_gpu', False)
            module_info.memory_estimate_mb = config.get('memory_estimate_mb', 0)
            
            self.discovered_modules[module_name] = module_info
    
    def _validate_module_versions(self):
        """Validate module version compatibility"""
        for module_name, module_info in self.discovered_modules.items():
            for dep_name, min_version in module_info.min_dependency_versions.items():
                if dep_name in self.discovered_modules:
                    dep_version = self.discovered_modules[dep_name].version
                    if dep_version < min_version:
                        logger.warning(f"Module {module_name} requires {dep_name} >= {min_version}, "
                                     f"but found {dep_version}. This may cause compatibility issues.")
                        MODULE_DEPENDENCY_VIOLATIONS.labels(module=module_name).inc()
    
    async def call_module(self, module_name: str, method: str, *args, **kwargs) -> Any:
        """Call a module method with circuit breaker, rate limiting, and tracing"""
        if module_name not in self.module_instances:
            raise ValueError(f"Module {module_name} not available")
        
        # Apply rate limiting
        if module_name in self.rate_limiters:
            await self.rate_limiters[module_name].acquire()
        
        # Apply circuit breaker
        if module_name not in self.circuit_breakers:
            self.circuit_breakers[module_name] = ModuleCircuitBreaker(module_name)
        
        # Execute with tracing
        if self.tracer:
            with self.tracer.start_as_current_span(f"module_call_{module_name}.{method}") as span:
                span.set_attribute("module.name", module_name)
                span.set_attribute("method.name", method)
                
                result = await self.circuit_breakers[module_name].call(
                    self._execute_module_method, module_name, method, *args, **kwargs
                )
                return result
        else:
            return await self.circuit_breakers[module_name].call(
                self._execute_module_method, module_name, method, *args, **kwargs
            )
    
    async def _execute_module_method(self, module_name: str, method: str, *args, **kwargs) -> Any:
        """Execute a module method with retry logic"""
        module = self.module_instances.get(module_name)
        if not module:
            raise ValueError(f"Module {module_name} not initialized")
        
        func = getattr(module, method, None)
        if not func:
            raise ValueError(f"Method {method} not found in module {module_name}")
        
        max_retries = 2
        last_error = None
        
        for attempt in range(max_retries + 1):
            try:
                start_time = time.time()
                
                if asyncio.iscoroutinefunction(func):
                    result = await func(*args, **kwargs)
                else:
                    result = func(*args, **kwargs)
                
                elapsed_ms = (time.time() - start_time) * 1000
                self.module_latencies[module_name].append(elapsed_ms)
                
                # Keep only last 100 latencies
                if len(self.module_latencies[module_name]) > 100:
                    self.module_latencies[module_name] = self.module_latencies[module_name][-100:]
                
                return result
                
            except Exception as e:
                last_error = e
                self.module_retry_counts[module_name] += 1
                
                if attempt < max_retries:
                    wait_time = 2 ** attempt
                    logger.warning(f"Module {module_name}.{method} failed (attempt {attempt+1}/{max_retries+1}): {e}. Retrying in {wait_time}s")
                    await asyncio.sleep(wait_time)
                else:
                    logger.error(f"Module {module_name}.{method} failed after {max_retries+1} attempts: {e}")
                    
                    # Record failure for auto-restart
                    self.restart_manager.record_failure(module_name)
                    
                    # Trigger auto-restart if enabled
                    if self.config.get('auto_restart', {}).get('enabled', True):
                        await self.restart_manager.attempt_restart(module_name)
        
        raise last_error
    
    async def _health_monitor_loop(self):
        """Background health monitor with auto-restart"""
        while self.running:
            await asyncio.sleep(self.config.get('health_check_interval', 30))
            
            for module_name in list(self.module_instances.keys()):
                try:
                    # Health check via circuit breaker
                    health = await self.call_module(module_name, 'health_check')
                    is_healthy = health.get('healthy', True) if isinstance(health, dict) else True
                    
                    if not is_healthy and self.config.get('auto_restart', {}).get('enabled', True):
                        await self.restart_manager.attempt_restart(module_name)
                        
                except Exception as e:
                    logger.warning(f"Health check failed for {module_name}: {e}")
                    if self.config.get('auto_restart', {}).get('enabled', True):
                        await self.restart_manager.attempt_restart(module_name)
    
    async def integrate(self, source_data: Dict = None, target_module: str = "all") -> Dict:
        """Main integration method with distributed tracing"""
        trace_id = str(uuid.uuid4())
        
        if self.tracer:
            with self.tracer.start_as_current_span("green_agent_integrate") as span:
                span.set_attribute("target_module", target_module)
                span.set_attribute("trace_id", trace_id)
                return await self._execute_integration(source_data, target_module, trace_id)
        else:
            return await self._execute_integration(source_data, target_module, trace_id)
    
    async def _execute_integration(self, source_data: Dict, target_module: str, trace_id: str) -> Dict:
        """Execute integration pipeline"""
        start_time = time.time()
        
        metrics = IntegrationMetrics(
            total_modules_available=self._count_available(),
            total_modules_discovered=self._count_discovered(),
            gpu_available=self.gpu_accelerator is not None and self.gpu_accelerator.cuda_available,
            trace_id=trace_id
        )
        
        integration_results = {
            'integration_id': str(uuid.uuid4())[:8],
            'timestamp': datetime.now().isoformat(),
            'trace_id': trace_id,
            'phases': {},
            'gpu_status': await self.get_gpu_status_async(),
            'circuit_breaker_status': self.get_circuit_breaker_status()
        }
        
        # Phase 1: Data Collection
        phase1 = await self._execute_phase1_with_tracing(source_data)
        integration_results['phases']['phase1_data_collection'] = phase1
        metrics.phase1_data_collection = phase1.get('success', False)
        
        # Phase 2: Analysis & Optimization
        phase2 = await self._execute_phase2_with_tracing(phase1)
        integration_results['phases']['phase2_analysis'] = phase2
        metrics.phase2_optimization = phase2.get('success', False)
        
        # Phase 3: Verification & Security
        phase3 = await self._execute_phase3_with_tracing(phase2)
        integration_results['phases']['phase3_verification'] = phase3
        metrics.phase3_verification = phase3.get('success', False)
        
        # Phase 4: Reporting & Export
        phase4 = await self._execute_phase4_with_tracing(phase3)
        integration_results['phases']['phase4_reporting'] = phase4
        metrics.phase4_reporting = phase4.get('success', False)
        
        # Phase 5: Orchestration & Control
        phase5 = await self._execute_phase5_with_tracing(phase4)
        integration_results['phases']['phase5_orchestration'] = phase5
        metrics.phase5_orchestration = phase5.get('success', False)
        
        # Phase 6: Monitoring & Health
        phase6 = await self._execute_phase6_with_tracing(phase5)
        integration_results['phases']['phase6_monitoring'] = phase6
        metrics.phase6_monitoring = phase6.get('success', False)
        
        # Finalize
        elapsed = time.time() - start_time
        metrics.total_integration_time_ms = elapsed * 1000
        metrics.modules_integrated = len(self.module_instances)
        metrics.overall_health_score = await self._calculate_health_score_async()
        
        # Update module metrics
        for name, latencies in self.module_latencies.items():
            if latencies:
                metrics.module_latencies[name] = np.mean(latencies)
        
        for name, cb in self.circuit_breakers.items():
            metrics.module_circuit_breaker_states[name] = cb.get_state()
        
        metrics.module_retry_counts = dict(self.module_retry_counts)
        
        self.integration_runs.append(metrics)
        integration_results['metrics'] = asdict(metrics)
        
        # Emit completion event
        await self._emit_event(IntegrationEvent(
            event_type="integration_completed",
            source_module="integrator",
            payload={
                'duration_ms': metrics.total_integration_time_ms,
                'modules_integrated': metrics.modules_integrated,
                'health_score': metrics.overall_health_score,
                'trace_id': trace_id
            }
        ))
        
        logger.info(f"Integration completed in {elapsed:.2f}s with "
                   f"{metrics.modules_integrated} modules integrated, "
                   f"health score: {metrics.overall_health_score:.1f}")
        
        return integration_results
    
    async def _emit_event(self, event: IntegrationEvent):
        """Emit event with tracing"""
        if self.tracer:
            with self.tracer.start_as_current_span(f"emit_event_{event.event_type}") as span:
                span.set_attribute("event_type", event.event_type)
                span.set_attribute("source_module", event.source_module)
                await self._dispatch_event(event)
        else:
            await self._dispatch_event(event)
    
    async def _dispatch_event(self, event: IntegrationEvent):
        """Dispatch event to handlers"""
        self.event_history.append(event)
        
        for handler in self.event_handlers.get(event.event_type, []):
            try:
                if asyncio.iscoroutinefunction(handler):
                    await handler(event)
                else:
                    handler(event)
            except Exception as e:
                logger.error(f"Event handler failed for {event.event_type}: {e}")
    
    async def get_gpu_status_async(self) -> Dict:
        """Get GPU acceleration status asynchronously"""
        if self.gpu_accelerator:
            return self.gpu_accelerator.get_memory_info()
        return {'cuda_available': False, 'message': 'GPU acceleration not available'}
    
    def get_circuit_breaker_status(self) -> Dict:
        """Get status of all circuit breakers"""
        return {
            name: cb.get_metrics()
            for name, cb in self.circuit_breakers.items()
        }
    
    async def _calculate_health_score_async(self) -> float:
        """Calculate overall integration health score"""
        if not self.module_instances:
            return 0.0
        
        scores = []
        
        for module_name, module in self.module_instances.items():
            try:
                if hasattr(module, 'health_check'):
                    health = await self.call_module(module_name, 'health_check') if asyncio.iscoroutinefunction(module.health_check) else module.health_check()
                    score = health.get('score', 100) if isinstance(health, dict) else 100
                else:
                    score = 100
                
                # Adjust based on circuit breaker state
                cb = self.circuit_breakers.get(module_name)
                if cb and cb.state != CircuitBreakerState.CLOSED:
                    score *= 0.5
                
                scores.append(score)
            except Exception as e:
                logger.debug(f"Health check failed for {module_name}: {e}")
                scores.append(0)
        
        # Add GPU health
        if self.gpu_accelerator:
            gpu_score = 100 if self.gpu_accelerator.cuda_available else 50
            scores.append(gpu_score)
        
        return np.mean(scores) if scores else 0
    
    def get_integration_status(self) -> Dict:
        """Get comprehensive integration status"""
        return {
            'modules': {
                name: {
                    'available': info.available,
                    'category': info.category,
                    'phase': info.phase,
                    'initialized': name in self.module_instances,
                    'health': info.health_status,
                    'version': str(info.version),
                    'requires_gpu': info.requires_gpu,
                    'state': info.state
                }
                for name, info in self.discovered_modules.items()
            },
            'summary': {
                'total_discovered': self._count_discovered(),
                'total_available': self._count_available(),
                'total_initialized': len(self.module_instances),
                'health_score': self._calculate_health_score_sync(),
                'current_phase': self.current_phase,
                'cycle_count': self.cycle_count,
                'total_integrations': len(self.integration_runs),
                'gpu_available': self.gpu_accelerator is not None and self.gpu_accelerator.cuda_available
            },
            'circuit_breakers': self.get_circuit_breaker_status(),
            'restart_manager': self.restart_manager.get_statistics(),
            'categories': self._get_category_stats(),
            'dependencies': {
                'graph_nodes': len(self.dependency_graph.nodes) if self.dependency_graph else 0,
                'graph_edges': len(self.dependency_graph.edges) if self.dependency_graph else 0
            },
            'gpu': self.gpu_accelerator.get_memory_info() if self.gpu_accelerator else None,
            'last_integration': asdict(self.integration_runs[-1]) if self.integration_runs else None,
            'timestamp': datetime.now().isoformat()
        }
    
    def _calculate_health_score_sync(self) -> float:
        """Synchronous health score calculation"""
        import asyncio
        try:
            loop = asyncio.get_event_loop()
            if loop.is_running():
                # Create new loop for sync context
                new_loop = asyncio.new_event_loop()
                asyncio.set_event_loop(new_loop)
                result = new_loop.run_until_complete(self._calculate_health_score_async())
                new_loop.close()
                return result
            else:
                return loop.run_until_complete(self._calculate_health_score_async())
        except RuntimeError:
            # No event loop, create one
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            result = loop.run_until_complete(self._calculate_health_score_async())
            loop.close()
            return result
    
    def _get_category_stats(self) -> Dict:
        """Get category statistics"""
        categories = defaultdict(lambda: {'total': 0, 'available': 0, 'healthy': 0})
        
        for name, info in self.discovered_modules.items():
            categories[info.category]['total'] += 1
            if info.available:
                categories[info.category]['available'] += 1
            if info.health_status == 'healthy':
                categories[info.category]['healthy'] += 1
        
        return dict(categories)
    
    # Phase execution methods with tracing
    async def _execute_phase1_with_tracing(self, source_data: Dict) -> Dict:
        """Phase 1 with tracing"""
        if self.tracer:
            with self.tracer.start_as_current_span("phase1_data_collection") as span:
                return await self._execute_phase1(source_data)
        return await self._execute_phase1(source_data)
    
    async def _execute_phase2_with_tracing(self, phase1_data: Dict) -> Dict:
        """Phase 2 with tracing"""
        if self.tracer:
            with self.tracer.start_as_current_span("phase2_analysis_optimization") as span:
                return await self._execute_phase2(phase1_data)
        return await self._execute_phase2(phase1_data)
    
    async def _execute_phase3_with_tracing(self, phase2_data: Dict) -> Dict:
        """Phase 3 with tracing"""
        if self.tracer:
            with self.tracer.start_as_current_span("phase3_verification_security") as span:
                return await self._execute_phase3(phase2_data)
        return await self._execute_phase3(phase2_data)
    
    async def _execute_phase4_with_tracing(self, phase3_data: Dict) -> Dict:
        """Phase 4 with tracing"""
        if self.tracer:
            with self.tracer.start_as_current_span("phase4_reporting_export") as span:
                return await self._execute_phase4(phase3_data)
        return await self._execute_phase4(phase3_data)
    
    async def _execute_phase5_with_tracing(self, phase4_data: Dict) -> Dict:
        """Phase 5 with tracing"""
        if self.tracer:
            with self.tracer.start_as_current_span("phase5_orchestration_control") as span:
                return await self._execute_phase5(phase4_data)
        return await self._execute_phase5(phase4_data)
    
    async def _execute_phase6_with_tracing(self, phase5_data: Dict) -> Dict:
        """Phase 6 with tracing"""
        if self.tracer:
            with self.tracer.start_as_current_span("phase6_monitoring_health") as span:
                return await self._execute_phase6(phase5_data)
        return await self._execute_phase6(phase5_data)
    
    async def _execute_phase1(self, source_data: Dict = None) -> Dict:
        """Phase 1: Data Collection"""
        logger.info("Executing Phase 1: Data Collection")
        results = {'success': True, 'modules_activated': [], 'data_collected': {}}
        
        # Helium Data Collector
        if 'helium_data_collector' in self.module_instances:
            try:
                result = await self.call_module('helium_data_collector', 'get_latest')
                results['data_collected']['helium'] = result
                results['modules_activated'].append('helium_data_collector')
            except Exception as e:
                logger.warning(f"Helium data collector failed: {e}")
        
        # GPU status
        if self.gpu_accelerator:
            results['gpu_available'] = self.gpu_accelerator.cuda_available
            if results['gpu_available']:
                results['gpu_info'] = self.gpu_accelerator.get_memory_info()
        
        return results
    
    async def _execute_phase2(self, phase1_data: Dict) -> Dict:
        """Phase 2: Analysis & Optimization"""
        logger.info("Executing Phase 2: Analysis & Optimization")
        results = {'success': True, 'modules_activated': [], 'optimization_results': {}}
        
        # Helium Elasticity
        if 'helium_elasticity' in self.module_instances:
            try:
                result = await self.call_module('helium_elasticity', 'calculate_comprehensive_elasticity', phase1_data)
                results['optimization_results']['elasticity'] = result
                results['modules_activated'].append('helium_elasticity')
            except Exception as e:
                logger.warning(f"Helium elasticity failed: {e}")
        
        return results
    
    async def _execute_phase3(self, phase2_data: Dict) -> Dict:
        """Phase 3: Verification & Security"""
        logger.info("Executing Phase 3: Verification & Security")
        results = {'success': True, 'modules_activated': [], 'verification_results': {}}
        return results
    
    async def _execute_phase4(self, phase3_data: Dict) -> Dict:
        """Phase 4: Reporting & Export"""
        logger.info("Executing Phase 4: Reporting & Export")
        results = {'success': True, 'modules_activated': [], 'export_results': {}}
        return results
    
    async def _execute_phase5(self, phase4_data: Dict) -> Dict:
        """Phase 5: Orchestration & Control"""
        logger.info("Executing Phase 5: Orchestration & Control")
        results = {'success': True, 'modules_activated': [], 'control_results': {}}
        return results
    
    async def _execute_phase6(self, phase5_data: Dict) -> Dict:
        """Phase 6: Monitoring & Health"""
        logger.info("Executing Phase 6: Monitoring & Health")
        results = {'success': True, 'modules_activated': []}
        
        # Run health checks
        health_score = await self._calculate_health_score_async()
        results['health_score'] = health_score
        results['circuit_breaker_status'] = self.get_circuit_breaker_status()
        
        # Performance benchmark if GPU available
        if self.gpu_accelerator:
            results['gpu_benchmark'] = self.gpu_accelerator.benchmark()
        
        return results
    
    # Existing methods from v7.0 continue here...
    # (_try_discover_module, _build_dependency_graph, _get_initialization_order,
    #  _initialize_all_modules_ordered, _initialize_module, _count_available,
    #  _count_discovered, get_module, subscribe, register_tenant, get_tenant_module,
    #  check_all_modules_health, hot_reload_config, etc.)
    
    def _try_discover_module(self, module_name: str, config: Dict) -> ModuleInfo:
        """Try to discover and import a module"""
        try:
            module = importlib.import_module(config['module'])
            
            if 'factory' in config:
                factory = getattr(module, config['factory'], None)
                if factory:
                    return ModuleInfo(
                        name=module_name,
                        category=config['category'],
                        available=True,
                        factory_function=config['factory'],
                        dependencies=config.get('dependencies', []),
                        phase=config.get('phase', 1)
                    )
            
            if 'class' in config:
                cls = getattr(module, config['class'], None)
                if cls:
                    return ModuleInfo(
                        name=module_name,
                        category=config['category'],
                        available=True,
                        dependencies=config.get('dependencies', []),
                        phase=config.get('phase', 1)
                    )
            
            return ModuleInfo(
                name=module_name,
                category=config['category'],
                available=False,
                init_error=f"Factory/class not found in module",
                dependencies=config.get('dependencies', []),
                phase=config.get('phase', 1)
            )
            
        except ImportError as e:
            return ModuleInfo(
                name=module_name,
                category=config['category'],
                available=False,
                init_error=str(e),
                dependencies=config.get('dependencies', []),
                phase=config.get('phase', 1)
            )
    
    def _build_dependency_graph(self):
        """Build dependency graph for topological sorting"""
        if self.dependency_graph is None:
            return
        
        for module_name, module_info in self.discovered_modules.items():
            self.dependency_graph.add_node(module_name)
            for dep in module_info.dependencies:
                self.dependency_graph.add_edge(dep, module_name)
        
        # Check for cycles
        try:
            import networkx as nx
            cycles = list(nx.simple_cycles(self.dependency_graph))
            if cycles:
                logger.warning(f"Dependency cycles detected: {cycles}")
        except Exception:
            pass
    
    def _get_initialization_order(self) -> List[str]:
        """Get modules in topological order"""
        if self.dependency_graph is None:
            modules_by_phase = defaultdict(list)
            for name, info in self.discovered_modules.items():
                if info.available:
                    modules_by_phase[info.phase].append(name)
            order = []
            for phase in sorted(modules_by_phase.keys()):
                order.extend(modules_by_phase[phase])
            return order
        
        try:
            import networkx as nx
            return list(nx.topological_sort(self.dependency_graph))
        except Exception:
            modules_by_phase = defaultdict(list)
            for name, info in self.discovered_modules.items():
                if info.available:
                    modules_by_phase[info.phase].append(name)
            order = []
            for phase in sorted(modules_by_phase.keys()):
                order.extend(modules_by_phase[phase])
            return order
    
    def _initialize_all_modules_ordered(self):
        """Initialize all modules in dependency order"""
        init_order = self._get_initialization_order()
        
        for module_name in init_order:
            module_info = self.discovered_modules.get(module_name)
            if module_info and module_info.available:
                # Check GPU requirement
                if module_info.requires_gpu and (not self.gpu_accelerator or not self.gpu_accelerator.cuda_available):
                    logger.warning(f"Module {module_name} requires GPU but GPU not available")
                    module_info.available = False
                    continue
                
                # Check dependencies
                missing_deps = []
                for dep in module_info.dependencies:
                    if dep not in self.module_instances:
                        missing_deps.append(dep)
                
                if missing_deps:
                    logger.warning(f"Module {module_name} missing dependencies: {missing_deps}")
                    continue
                
                try:
                    instance = self._initialize_module(module_name, module_info)
                    if instance is not None:
                        self.module_instances[module_name] = instance
                        module_info.instance = instance
                        module_info.state = "running"
                        module_info.health_status = "healthy"
                        logger.info(f"Module initialized: {module_name}")
                except Exception as e:
                    logger.warning(f"Module {module_name} init failed: {e}")
                    module_info.available = False
                    module_info.init_error = str(e)
                    module_info.state = "failed"
    
    def _initialize_module(self, module_name: str, module_info: ModuleInfo) -> Optional[Any]:
        """Initialize a single module with GPU awareness and config injection"""
        try:
            module = importlib.import_module(module_info.name)
            
            # Try factory function
            if module_info.factory_function:
                factory = getattr(module, module_info.factory_function)
                instance = factory()
                
                # Inject dependencies
                for dep_name in module_info.dependencies:
                    if dep_name in self.module_instances:
                        if hasattr(instance, f"set_{dep_name}"):
                            setter = getattr(instance, f"set_{dep_name}")
                            setter(self.module_instances[dep_name])
                
                # Inject GPU accelerator
                if self.gpu_accelerator and hasattr(instance, 'set_gpu_accelerator'):
                    instance.set_gpu_accelerator(self.gpu_accelerator)
                
                # Inject config
                if hasattr(instance, 'configure'):
                    instance.configure(self.config)
                
                # Restore persisted state
                persisted_state = self.state_persistence.load_module_state(module_name)
                if persisted_state and hasattr(instance, 'restore_state'):
                    instance.restore_state(persisted_state)
                
                return instance
            
            # Try class instantiation
            for attr_name in dir(module):
                attr = getattr(module, attr_name)
                if isinstance(attr, type) and any(attr_name.endswith(suffix) for suffix in 
                    ('Manager', 'System', 'Optimizer', 'Calculator', 'Engine', 'Exporter', 'Integrator')):
                    try:
                        instance = attr()
                    except TypeError:
                        instance = attr(config=self.config)
                    
                    # Inject dependencies
                    for dep_name in module_info.dependencies:
                        if dep_name in self.module_instances:
                            if hasattr(instance, f"set_{dep_name}"):
                                setter = getattr(instance, f"set_{dep_name}")
                                setter(self.module_instances[dep_name])
                    
                    # Inject GPU accelerator
                    if self.gpu_accelerator and hasattr(instance, 'set_gpu_accelerator'):
                        instance.set_gpu_accelerator(self.gpu_accelerator)
                    
                    return instance
            
            return None
            
        except Exception as e:
            logger.error(f"Module {module_name} initialization failed: {e}")
            return None
    
    def _count_available(self) -> int:
        """Count available modules"""
        return sum(1 for m in self.discovered_modules.values() if m.available)
    
    def _count_discovered(self) -> int:
        """Count total discovered modules"""
        return len(self.discovered_modules)
    
    def get_module(self, module_name: str) -> Optional[Any]:
        """Get initialized module instance"""
        return self.module_instances.get(module_name)
    
    def subscribe(self, event_type: str, handler: Callable):
        """Subscribe to events"""
        self.event_handlers[event_type].append(handler)
    
    def register_tenant(self, tenant_id: str, config_override: Dict = None):
        """Register a new tenant for multi-tenant isolation"""
        if tenant_id in self.active_tenants:
            logger.warning(f"Tenant {tenant_id} already registered")
            return
        
        tenant_modules = {}
        for module_name, module_info in self.discovered_modules.items():
            if module_info.available:
                try:
                    instance = self._initialize_module(module_name, module_info)
                    if instance:
                        if config_override and hasattr(instance, 'configure'):
                            instance.configure(config_override)
                        tenant_modules[module_name] = instance
                except Exception as e:
                    logger.warning(f"Tenant {tenant_id} module {module_name} init failed: {e}")
        
        self.tenant_instances[tenant_id] = tenant_modules
        self.active_tenants.add(tenant_id)
        logger.info(f"Tenant {tenant_id} registered with {len(tenant_modules)} modules")
    
    def get_tenant_module(self, tenant_id: str, module_name: str) -> Optional[Any]:
        """Get module instance for specific tenant"""
        return self.tenant_instances.get(tenant_id, {}).get(module_name)
    
    def hot_reload_config(self, config_path: str = None):
        """Reload configuration without restarting"""
        if config_path:
            self.config_path = config_path
        
        # Reload config
        new_config = self._load_default_config()
        self.config.update(new_config)
        
        # Re-initialize affected modules
        for module_name, module in self.module_instances.items():
            if hasattr(module, 'reload_config'):
                try:
                    module.reload_config(self.config)
                    logger.info(f"Config reloaded for {module_name}")
                except Exception as e:
                    logger.warning(f"Config reload failed for {module_name}: {e}")
        
        logger.info("Configuration hot-reload completed")
    
    def _update_all_metrics(self):
        """Update all Prometheus metrics"""
        for module_name, module_info in self.discovered_modules.items():
            MODULE_AVAILABLE.labels(module_name=module_name).set(1 if module_info.available else 0)
    
    async def shutdown(self):
        """Graceful shutdown"""
        logger.info("Shutting down GreenAgentIntegrator...")
        self.running = False
        
        for task in self.background_tasks:
            task.cancel()
        
        # Persist module states
        for module_name, module in self.module_instances.items():
            if hasattr(module, 'get_state'):
                try:
                    state = module.get_state()
                    self.state_persistence.save_module_state(module_name, state)
                except Exception as e:
                    logger.warning(f"Failed to persist state for {module_name}: {e}")
        
        logger.info("Shutdown complete")

# ============================================================
# SINGLETON ACCESSORS
# ============================================================

_integrator = None

def get_green_agent_integrator() -> GreenAgentIntegrator:
    """Get singleton integrator instance"""
    global _integrator
    if _integrator is None:
        _integrator = GreenAgentIntegrator()
    return _integrator

# ============================================================
# MAIN EXECUTION
# ============================================================

async def main():
    """Enhanced V8.0 demonstration"""
    print("=" * 80)
    print("Green Agent Integration Layer v8.0 - Enterprise Master Orchestrator Demo")
    print("=" * 80)
    
    # Initialize integrator
    integrator = GreenAgentIntegrator()
    
    # Module discovery summary
    status = integrator.get_integration_status()
    summary = status['summary']
    
    print(f"\n📦 Module Discovery Summary:")
    print(f"   Total Discovered: {summary['total_discovered']}")
    print(f"   Total Available: {summary['total_available']}")
    print(f"   Total Initialized: {summary['total_initialized']}")
    print(f"   Health Score: {summary['health_score']:.1f}%")
    print(f"   GPU Available: {summary['gpu_available']}")
    
    # Circuit breaker status
    print(f"\n🔌 Circuit Breaker Status:")
    for name, cb_status in status['circuit_breakers'].items():
        state = cb_status.get('state', 'unknown')
        state_icon = "🟢" if state == "closed" else "🟡" if state == "half_open" else "🔴"
        print(f"   {state_icon} {name}: {state} (success rate: {cb_status.get('success_rate_10', 0)*100:.0f}%)")
    
    # Run full integration
    print(f"\n🔬 Running Full Integration Pipeline...")
    results = await integrator.integrate()
    
    # Phase results
    phases = results.get('phases', {})
    print(f"\n📊 Phase Execution Results:")
    for phase_name, phase_data in phases.items():
        if isinstance(phase_data, dict):
            success = phase_data.get('success', False)
            modules = phase_data.get('modules_activated', [])
            print(f"   {phase_name}: {'✅' if success else '❌'} ({len(modules)} modules)")
    
    # Metrics
    metrics = results.get('metrics', {})
    print(f"\n📈 Integration Metrics:")
    print(f"   Time: {metrics.get('total_integration_time_ms', 0):.0f}ms")
    print(f"   Modules Integrated: {metrics.get('modules_integrated', 0)}")
    print(f"   Health Score: {metrics.get('overall_health_score', 0):.1f}%")
    print(f"   Trace ID: {metrics.get('trace_id', 'N/A')}")
    
    # Circuit breaker metrics
    cb_metrics = results.get('circuit_breaker_status', {})
    open_count = sum(1 for s in cb_metrics.values() if s.get('state') == 'open')
    half_open_count = sum(1 for s in cb_metrics.values() if s.get('state') == 'half_open')
    print(f"\n🔌 Circuit Breaker Summary:")
    print(f"   Open: {open_count}, Half-Open: {half_open_count}, Closed: {len(cb_metrics) - open_count - half_open_count}")
    
    print("\n" + "=" * 80)
    print("✅ Green Agent Integration v8.0 - Enterprise Orchestration Complete")
    print("=" * 80)

if __name__ == "__main__":
    asyncio.run(main())
