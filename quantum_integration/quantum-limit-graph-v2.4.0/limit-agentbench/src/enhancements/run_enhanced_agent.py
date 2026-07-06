# File: quantum_integration/quantum-limit-graph-v2.4.0/limit-agentbench/src/enhancements/run_enhanced_agent.py
# Enhanced with dynamic pipeline selection, degradation awareness, predictive integration,
# circuit breakers, reinforcement learning, observability dashboard, and task prioritization

"""
Enhanced Green Agent Runner v6.0.0
Complete integration with dynamic pipeline selection, degradation awareness,
predictive homeostasis, bio-inspired orchestration, and advanced intelligence.

NEW FEATURES v6.0.0:
1. ADDED: Circuit Breaker Pattern - Automatic fallback for failed pipelines
2. ADDED: Reinforcement Learning Pipeline Selection - Self-optimizing pipeline choice
3. ADDED: Real-time Observability Dashboard - WebSocket-based monitoring
4. ADDED: Task Priority Queue - Intelligent task scheduling and prioritization
5. ADDED: Robust Configuration Management - Pydantic-based config with validation
"""

import asyncio
import logging
from typing import Dict, Any, List, Optional, Callable, Tuple, Set
from datetime import datetime
import numpy as np
import json
import os
import signal
import heapq
import threading
import time
from collections import defaultdict, deque
from dataclasses import dataclass, field
from enum import Enum

# Pydantic for configuration
from pydantic import BaseModel, Field, field_validator, ValidationError, ConfigDict

# WebSocket for dashboard
import websockets
from websockets.server import serve
from websockets.exceptions import ConnectionClosed

# Environment variables
from dotenv import load_dotenv

# Prometheus for metrics
try:
    from prometheus_client import Counter, Gauge, Histogram, CollectorRegistry, start_http_server
    PROMETHEUS_AVAILABLE = True
except ImportError:
    PROMETHEUS_AVAILABLE = False

logger = logging.getLogger(__name__)

# ============================================================================
# Try importing modules
# ============================================================================
try:
    from enhancements.moe_expert_system import UnifiedMetabolicEcosystem
    from enhancements.bio_inspired import EnhancedBioInspiredCore
    from enhancements.bio_inspired.eco_atp_currency import EcoATPSource, EcoATPConsumer
    from enhancements.bio_inspired.degradation_manager import OperationalTier
    BIO_AVAILABLE = True
except ImportError:
    BIO_AVAILABLE = False

# ============================================================================
# Configuration Management (NEW v6.0.0)
# ============================================================================

class RunnerConfig(BaseModel):
    """Enhanced configuration model with validation"""
    model_config = ConfigDict(arbitrary_types_allowed=True)
    
    # Core features
    enable_dynamic_pipeline: bool = Field(default=True, description="Enable dynamic pipeline selection")
    enable_degradation_aware: bool = Field(default=True, description="Enable degradation-aware processing")
    enable_predictive_informed: bool = Field(default=True, description="Enable predictive-informed scheduling")
    enable_reinforcement_learning: bool = Field(default=True, description="Enable RL-based pipeline selection")
    enable_circuit_breakers: bool = Field(default=True, description="Enable circuit breaker pattern")
    enable_dashboard: bool = Field(default=True, description="Enable WebSocket dashboard")
    enable_prometheus: bool = Field(default=False, description="Enable Prometheus metrics")
    
    # Performance
    max_concurrent_tasks: int = Field(default=10, ge=1, le=100, description="Maximum concurrent tasks")
    task_timeout_seconds: int = Field(default=300, ge=10, le=3600, description="Task timeout in seconds")
    queue_max_size: int = Field(default=1000, ge=10, le=10000, description="Maximum queue size")
    
    # Circuit breaker
    circuit_breaker_failure_threshold: int = Field(default=3, ge=1, le=10, description="Failures before circuit opens")
    circuit_breaker_timeout_seconds: int = Field(default=60, ge=10, le=600, description="Circuit open timeout")
    
    # RL parameters
    rl_learning_rate: float = Field(default=0.1, ge=0.01, le=1.0, description="RL learning rate")
    rl_discount_factor: float = Field(default=0.9, ge=0.5, le=1.0, description="RL discount factor")
    rl_exploration_rate: float = Field(default=0.1, ge=0.0, le=1.0, description="RL exploration rate")
    
    # Dashboard
    dashboard_port: int = Field(default=8777, ge=1024, le=65535, description="Dashboard WebSocket port")
    dashboard_update_interval: int = Field(default=5, ge=1, le=60, description="Dashboard update interval (seconds)")
    
    # Fallback pipelines
    fallback_pipelines: List[str] = Field(
        default=['standard', 'energy_efficient'],
        description="Pipeline fallback order"
    )
    
    @field_validator('fallback_pipelines')
    @classmethod
    def validate_fallback_pipelines(cls, v: List[str]) -> List[str]:
        """Validate fallback pipelines exist"""
        valid_pipelines = ['standard', 'quantum_enhanced', 'helium_optimized', 
                          'energy_efficient', 'bio_optimized']
        for pipeline in v:
            if pipeline not in valid_pipelines:
                raise ValueError(f"Invalid fallback pipeline: {pipeline}")
        return v
    
    @classmethod
    def from_env(cls) -> 'RunnerConfig':
        """Load configuration from environment variables"""
        load_dotenv()
        
        config_dict = {}
        
        # Map environment variables to config fields
        env_mapping = {
            'ENABLE_DYNAMIC_PIPELINE': 'enable_dynamic_pipeline',
            'ENABLE_DEGRADATION_AWARE': 'enable_degradation_aware',
            'ENABLE_PREDICTIVE_INFORMED': 'enable_predictive_informed',
            'ENABLE_REINFORCEMENT_LEARNING': 'enable_reinforcement_learning',
            'ENABLE_CIRCUIT_BREAKERS': 'enable_circuit_breakers',
            'ENABLE_DASHBOARD': 'enable_dashboard',
            'ENABLE_PROMETHEUS': 'enable_prometheus',
            'MAX_CONCURRENT_TASKS': 'max_concurrent_tasks',
            'TASK_TIMEOUT_SECONDS': 'task_timeout_seconds',
            'QUEUE_MAX_SIZE': 'queue_max_size',
            'CIRCUIT_BREAKER_FAILURE_THRESHOLD': 'circuit_breaker_failure_threshold',
            'CIRCUIT_BREAKER_TIMEOUT_SECONDS': 'circuit_breaker_timeout_seconds',
            'RL_LEARNING_RATE': 'rl_learning_rate',
            'RL_DISCOUNT_FACTOR': 'rl_discount_factor',
            'RL_EXPLORATION_RATE': 'rl_exploration_rate',
            'DASHBOARD_PORT': 'dashboard_port',
            'DASHBOARD_UPDATE_INTERVAL': 'dashboard_update_interval'
        }
        
        for env_var, config_key in env_mapping.items():
            value = os.getenv(env_var)
            if value is not None:
                # Convert to appropriate type
                if config_key in ['max_concurrent_tasks', 'task_timeout_seconds', 
                                 'queue_max_size', 'circuit_breaker_failure_threshold',
                                 'circuit_breaker_timeout_seconds', 'dashboard_port',
                                 'dashboard_update_interval']:
                    try:
                        config_dict[config_key] = int(value)
                    except ValueError:
                        logger.warning(f"Invalid integer value for {env_var}: {value}")
                elif config_key in ['enable_dynamic_pipeline', 'enable_degradation_aware',
                                   'enable_predictive_informed', 'enable_reinforcement_learning',
                                   'enable_circuit_breakers', 'enable_dashboard', 
                                   'enable_prometheus']:
                    config_dict[config_key] = value.lower() in ['true', '1', 'yes', 'on']
                elif config_key in ['rl_learning_rate', 'rl_discount_factor', 'rl_exploration_rate']:
                    try:
                        config_dict[config_key] = float(value)
                    except ValueError:
                        logger.warning(f"Invalid float value for {env_var}: {value}")
        
        return cls(**config_dict)

# ============================================================================
# Circuit Breaker Pattern (NEW v6.0.0)
# ============================================================================

class CircuitState(Enum):
    """Circuit breaker states"""
    CLOSED = "closed"
    HALF_OPEN = "half_open"
    OPEN = "open"

@dataclass
class CircuitBreakerMetrics:
    """Metrics for a circuit breaker"""
    failures: int = 0
    successes: int = 0
    total_calls: int = 0
    last_failure_time: Optional[datetime] = None
    last_success_time: Optional[datetime] = None
    average_latency_ms: float = 0.0

class PipelineCircuitBreaker:
    """
    Circuit breaker for pipeline failure management.
    
    Prevents repeated failures by temporarily disabling failing pipelines.
    """
    
    def __init__(self, config: RunnerConfig):
        self.config = config
        self.failure_counts: Dict[str, int] = defaultdict(int)
        self.success_counts: Dict[str, int] = defaultdict(int)
        self.states: Dict[str, CircuitState] = defaultdict(lambda: CircuitState.CLOSED)
        self.state_timestamps: Dict[str, datetime] = {}
        self.metrics: Dict[str, CircuitBreakerMetrics] = defaultdict(CircuitBreakerMetrics)
        self._lock = asyncio.Lock()
        
        logger.info("PipelineCircuitBreaker initialized")
    
    async def record_failure(self, pipeline: str, latency_ms: float = 0):
        """Record a pipeline failure"""
        async with self._lock:
            self.failure_counts[pipeline] += 1
            self.metrics[pipeline].failures += 1
            self.metrics[pipeline].total_calls += 1
            self.metrics[pipeline].last_failure_time = datetime.now()
            self.metrics[pipeline].average_latency_ms = (
                self.metrics[pipeline].average_latency_ms * 0.9 + latency_ms * 0.1
            )
            
            # Check if circuit should open
            if self.failure_counts[pipeline] >= self.config.circuit_breaker_failure_threshold:
                self.states[pipeline] = CircuitState.OPEN
                self.state_timestamps[pipeline] = datetime.now()
                logger.warning(f"Circuit breaker OPEN for pipeline: {pipeline}")
                
                # Update Prometheus metric
                if PROMETHEUS_AVAILABLE:
                    from prometheus_client import Gauge
                    gauge = Gauge('circuit_breaker_state', 'Circuit breaker state', ['pipeline'])
                    gauge.labels(pipeline=pipeline).set(2)  # 2 = OPEN
    
    async def record_success(self, pipeline: str, latency_ms: float = 0):
        """Record a pipeline success"""
        async with self._lock:
            self.success_counts[pipeline] += 1
            self.metrics[pipeline].successes += 1
            self.metrics[pipeline].total_calls += 1
            self.metrics[pipeline].last_success_time = datetime.now()
            self.metrics[pipeline].average_latency_ms = (
                self.metrics[pipeline].average_latency_ms * 0.9 + latency_ms * 0.1
            )
            
            # Reset failure count on success
            self.failure_counts[pipeline] = 0
            
            # Close circuit if it was half-open
            if self.states[pipeline] == CircuitState.HALF_OPEN:
                self.states[pipeline] = CircuitState.CLOSED
                logger.info(f"Circuit breaker CLOSED for pipeline: {pipeline}")
                
                if PROMETHEUS_AVAILABLE:
                    from prometheus_client import Gauge
                    gauge = Gauge('circuit_breaker_state', 'Circuit breaker state', ['pipeline'])
                    gauge.labels(pipeline=pipeline).set(0)  # 0 = CLOSED
    
    async def is_available(self, pipeline: str) -> Tuple[bool, str]:
        """Check if pipeline is available for use"""
        async with self._lock:
            state = self.states[pipeline]
            
            if state == CircuitState.OPEN:
                # Check if timeout has elapsed
                if pipeline in self.state_timestamps:
                    elapsed = (datetime.now() - self.state_timestamps[pipeline]).total_seconds()
                    if elapsed >= self.config.circuit_breaker_timeout_seconds:
                        # Move to half-open
                        self.states[pipeline] = CircuitState.HALF_OPEN
                        logger.info(f"Circuit breaker HALF-OPEN for pipeline: {pipeline}")
                        
                        if PROMETHEUS_AVAILABLE:
                            from prometheus_client import Gauge
                            gauge = Gauge('circuit_breaker_state', 'Circuit breaker state', ['pipeline'])
                            gauge.labels(pipeline=pipeline).set(1)  # 1 = HALF_OPEN
                        
                        return True, "half_open"
                return False, "circuit_open"
            
            return True, state.value
    
    def get_state(self, pipeline: str) -> str:
        """Get circuit breaker state"""
        return self.states[pipeline].value
    
    def get_metrics(self, pipeline: str) -> Dict[str, Any]:
        """Get metrics for a pipeline"""
        metrics = self.metrics[pipeline]
        return {
            'failures': metrics.failures,
            'successes': metrics.successes,
            'total_calls': metrics.total_calls,
            'state': self.states[pipeline].value,
            'failure_rate': metrics.failures / max(metrics.total_calls, 1),
            'success_rate': metrics.successes / max(metrics.total_calls, 1),
            'average_latency_ms': metrics.average_latency_ms,
            'last_failure': metrics.last_failure_time.isoformat() if metrics.last_failure_time else None,
            'last_success': metrics.last_success_time.isoformat() if metrics.last_success_time else None
        }
    
    def reset(self, pipeline: str):
        """Reset circuit breaker for a pipeline"""
        self.failure_counts[pipeline] = 0
        self.states[pipeline] = CircuitState.CLOSED
        self.metrics[pipeline] = CircuitBreakerMetrics()
        logger.info(f"Circuit breaker RESET for pipeline: {pipeline}")

# ============================================================================
# Reinforcement Learning Pipeline Selector (NEW v6.0.0)
# ============================================================================

class RLPipelineLearner:
    """
    Reinforcement Learning for adaptive pipeline selection.
    
    Uses Q-learning to learn optimal pipeline selection based on system state.
    """
    
    def __init__(self, config: RunnerConfig):
        self.config = config
        self.q_table: Dict[str, Dict[str, float]] = defaultdict(lambda: defaultdict(float))
        self.state_action_counts: Dict[str, Dict[str, int]] = defaultdict(lambda: defaultdict(int))
        self.learning_rate = config.rl_learning_rate
        self.discount_factor = config.rl_discount_factor
        self.exploration_rate = config.rl_exploration_rate
        
        self._lock = asyncio.Lock()
        self.total_updates = 0
        self.last_state: Optional[str] = None
        self.last_action: Optional[str] = None
        
        logger.info(f"RLPipelineLearner initialized (α={self.learning_rate}, γ={self.discount_factor}, ε={self.exploration_rate})")
    
    def _state_to_key(self, state: Dict[str, Any]) -> str:
        """Convert state dictionary to string key"""
        # Use key features for state representation
        tier = state.get('degradation_tier', 5)
        token_balance = state.get('token_balance', 1000)
        carbon_gradient = state.get('carbon_gradient', 0.5)
        
        # Discretize continuous values
        token_level = 'high' if token_balance > 500 else 'low'
        carbon_level = 'high' if carbon_gradient > 0.5 else 'low'
        tier_level = f'tier_{tier}'
        
        return f"{tier_level}_{token_level}_{carbon_level}"
    
    def get_best_pipeline(self, state: Dict[str, Any], available_pipelines: List[str]) -> str:
        """Get best pipeline using epsilon-greedy policy"""
        state_key = self._state_to_key(state)
        
        # Exploration
        if np.random.random() < self.exploration_rate:
            # Decay exploration rate
            self.exploration_rate *= 0.999
            return np.random.choice(available_pipelines)
        
        # Exploitation
        q_values = {p: self.q_table[state_key].get(p, 0.0) for p in available_pipelines}
        best_pipeline = max(q_values, key=q_values.get)
        
        self.last_state = state_key
        self.last_action = best_pipeline
        
        return best_pipeline
    
    async def update(self, state: Dict[str, Any], pipeline: str, reward: float, 
                     next_state: Dict[str, Any]):
        """Update Q-table using Q-learning"""
        async with self._lock:
            state_key = self._state_to_key(state)
            next_state_key = self._state_to_key(next_state)
            
            current_q = self.q_table[state_key][pipeline]
            max_next_q = max(self.q_table[next_state_key].values()) if self.q_table[next_state_key] else 0
            
            # Q-learning update
            new_q = current_q + self.learning_rate * (
                reward + self.discount_factor * max_next_q - current_q
            )
            
            self.q_table[state_key][pipeline] = new_q
            self.state_action_counts[state_key][pipeline] += 1
            self.total_updates += 1
    
    def get_q_values(self, state: Dict[str, Any]) -> Dict[str, float]:
        """Get Q-values for a state"""
        state_key = self._state_to_key(state)
        return dict(self.q_table[state_key])
    
    def get_statistics(self) -> Dict[str, Any]:
        """Get RL statistics"""
        total_states = len(self.q_table)
        total_actions = sum(len(actions) for actions in self.q_table.values())
        
        return {
            'total_updates': self.total_updates,
            'total_states': total_states,
            'total_actions': total_actions,
            'exploration_rate': self.exploration_rate,
            'learning_rate': self.learning_rate,
            'discount_factor': self.discount_factor
        }
    
    def export_q_table(self) -> Dict[str, Dict[str, float]]:
        """Export Q-table for analysis"""
        return {k: dict(v) for k, v in self.q_table.items()}
    
    def import_q_table(self, q_table: Dict[str, Dict[str, float]]):
        """Import Q-table from external source"""
        for state, actions in q_table.items():
            for action, value in actions.items():
                self.q_table[state][action] = value

# ============================================================================
# Task Priority Queue (NEW v6.0.0)
# ============================================================================

@dataclass(order=True)
class PrioritizedTask:
    """Task with priority for queue ordering"""
    priority: float
    sequence: int
    task: Dict[str, Any] = field(compare=False)
    timestamp: datetime = field(compare=False, default_factory=datetime.now)

class TaskPriorityQueue:
    """
    Priority queue for intelligent task scheduling.
    
    Tasks are ordered by dynamic priority calculated from system state.
    """
    
    def __init__(self, max_size: int = 1000):
        self.heap: List[PrioritizedTask] = []
        self.sequence = 0
        self.max_size = max_size
        self._lock = asyncio.Lock()
        
        logger.info(f"TaskPriorityQueue initialized with max_size={max_size}")
    
    async def push(self, task: Dict[str, Any], priority: float):
        """Push task with priority (higher priority = lower value)"""
        async with self._lock:
            if len(self.heap) >= self.max_size:
                logger.warning(f"Task queue full ({self.max_size}), dropping lowest priority task")
                # Remove lowest priority task
                heapq.heappop(self.heap)
            
            # Negate priority for max-heap behavior
            heapq.heappush(self.heap, PrioritizedTask(
                priority=-priority,
                sequence=self.sequence,
                task=task
            ))
            self.sequence += 1
    
    async def pop(self) -> Optional[Dict[str, Any]]:
        """Pop highest priority task"""
        async with self._lock:
            if not self.heap:
                return None
            
            prioritized = heapq.heappop(self.heap)
            return prioritized.task
    
    async def peek(self) -> Optional[Dict[str, Any]]:
        """Peek at highest priority task without removing"""
        async with self._lock:
            if not self.heap:
                return None
            return self.heap[0].task
    
    async def size(self) -> int:
        """Get current queue size"""
        return len(self.heap)
    
    async def clear(self):
        """Clear all tasks"""
        async with self._lock:
            self.heap.clear()
            self.sequence = 0
    
    def calculate_priority(self, task: Dict[str, Any], state: Dict[str, Any]) -> float:
        """
        Calculate dynamic priority for a task.
        
        Priority factors:
        - Task base priority (1-3, default 2)
        - System degradation tier
        - Task carbon impact
        - Task urgency flags
        """
        base_priority = task.get('priority', 2)
        tier = state.get('degradation_tier', 5)
        carbon_impact = task.get('carbon_impact', 0.5)
        is_critical = task.get('is_critical', False)
        
        # Base weight
        priority = float(base_priority)
        
        # Degradation awareness
        if tier <= 2:
            # In degraded state, prioritize critical and high-priority tasks
            if is_critical or base_priority >= 2:
                priority += 2.0
            else:
                priority -= 1.0
        
        # Carbon awareness - prioritize low-carbon tasks when carbon gradient is high
        if state.get('carbon_gradient', 0.5) > 0.7:
            if carbon_impact < 0.3:
                priority += 0.5
            elif carbon_impact > 0.7:
                priority -= 0.5
        
        # Urgency boost
        if task.get('urgency', 'normal') == 'critical':
            priority += 3.0
        elif task.get('urgency') == 'high':
            priority += 1.0
        
        # Ensure priority is positive
        return max(0.1, priority)

# ============================================================================
# Observability Dashboard (NEW v6.0.0)
# ============================================================================

class AgentDashboardServer:
    """
    Real-time WebSocket dashboard for agent observability.
    
    Provides live updates on system state, pipeline performance, and task status.
    """
    
    def __init__(self, config: RunnerConfig):
        self.config = config
        self.port = config.dashboard_port
        self.clients: Set[websockets.WebSocketServerProtocol] = set()
        self._server = None
        self._running = False
        self._lock = asyncio.Lock()
        self._last_broadcast = {}
        
        logger.info(f"AgentDashboardServer initialized on port {self.port}")
    
    async def start(self):
        """Start WebSocket server"""
        if not self.config.enable_dashboard:
            logger.info("Dashboard disabled by configuration")
            return
        
        self._running = True
        self._server = await serve(
            self._handle_client,
            "0.0.0.0",
            self.port,
            ping_interval=30,
            ping_timeout=60
        )
        logger.info(f"Dashboard WebSocket server started on port {self.port}")
        
        # Start broadcast loop
        asyncio.create_task(self._broadcast_loop())
    
    async def stop(self):
        """Stop WebSocket server"""
        self._running = False
        if self._server:
            self._server.close()
            await self._server.wait_closed()
            logger.info("Dashboard WebSocket server stopped")
    
    async def _handle_client(self, websocket: websockets.WebSocketServerProtocol, path: str):
        """Handle WebSocket client connection"""
        async with self._lock:
            self.clients.add(websocket)
            logger.info(f"Dashboard client connected ({len(self.clients)} total)")
        
        try:
            # Send initial status
            await websocket.send(json.dumps({
                'type': 'connected',
                'timestamp': datetime.now().isoformat(),
                'message': 'Connected to Green Agent Dashboard'
            }))
            
            # Handle client messages
            async for message in websocket:
                try:
                    data = json.loads(message)
                    await self._handle_client_message(websocket, data)
                except json.JSONDecodeError:
                    await websocket.send(json.dumps({
                        'type': 'error',
                        'message': 'Invalid JSON received'
                    }))
                    
        except ConnectionClosed:
            pass
        finally:
            async with self._lock:
                self.clients.discard(websocket)
                logger.info(f"Dashboard client disconnected ({len(self.clients)} total)")
    
    async def _handle_client_message(self, websocket: websockets.WebSocketServerProtocol, data: Dict):
        """Handle client messages (commands, queries)"""
        msg_type = data.get('type')
        
        if msg_type == 'get_status':
            # Request status update - handled by broadcast
            pass
        elif msg_type == 'get_pipeline_stats':
            # Request pipeline statistics
            pass
        elif msg_type == 'reset_circuit_breaker':
            pipeline = data.get('pipeline')
            if pipeline:
                # This would be handled by the runner
                pass
        elif msg_type == 'force_pipeline':
            pipeline = data.get('pipeline')
            if pipeline:
                # This would be handled by the runner
                pass
    
    async def broadcast_status(self, status: Dict[str, Any]):
        """Broadcast system status to all clients"""
        self._last_broadcast = status
        message = json.dumps({
            'type': 'status_update',
            'timestamp': datetime.now().isoformat(),
            'data': status
        })
        
        if not self.clients:
            return
        
        async with self._lock:
            disconnected = set()
            for client in self.clients:
                try:
                    await client.send(message)
                except (ConnectionClosed, websockets.WebSocketException):
                    disconnected.add(client)
            
            # Remove disconnected clients
            for client in disconnected:
                self.clients.discard(client)
    
    async def _broadcast_loop(self):
        """Background loop for periodic broadcasts"""
        while self._running:
            try:
                await asyncio.sleep(self.config.dashboard_update_interval)
                # The runner will call broadcast_status with latest data
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Broadcast loop error: {e}")

# ============================================================================
# Pipeline Selection Engine (Enhanced v6.0.0)
# ============================================================================

class DynamicPipelineSelector:
    """
    Dynamically selects processing pipeline based on system state.
    
    Enhanced with RL-based learning and circuit breaker integration.
    """
    
    def __init__(self, config: RunnerConfig):
        self.config = config
        self.pipeline_performance: Dict[str, List[float]] = defaultdict(list)
        self.pipeline_history: deque = deque(maxlen=1000)
        self.config = config
        
        # Initialize RL learner
        self.rl_learner = RLPipelineLearner(config) if config.enable_reinforcement_learning else None
        
        # Initialize circuit breaker
        self.circuit_breaker = PipelineCircuitBreaker(config) if config.enable_circuit_breakers else None
        
        # Pipeline suitability matrix (used as fallback or initial Q-values)
        self.pipeline_suitability = {
            'standard': {
                'tier_5': 1.0, 'tier_4': 0.8, 'tier_3': 0.6, 'tier_2': 0.4, 'tier_1': 0.2,
                'tokens_abundant': 0.8, 'tokens_scarce': 0.6,
                'carbon_low': 0.9, 'carbon_high': 0.5
            },
            'quantum_enhanced': {
                'tier_5': 0.9, 'tier_4': 0.7, 'tier_3': 0.4, 'tier_2': 0.2, 'tier_1': 0.0,
                'tokens_abundant': 0.9, 'tokens_scarce': 0.2,
                'carbon_low': 0.8, 'carbon_high': 0.3
            },
            'helium_optimized': {
                'tier_5': 0.9, 'tier_4': 0.9, 'tier_3': 0.7, 'tier_2': 0.5, 'tier_1': 0.3,
                'tokens_abundant': 0.7, 'tokens_scarce': 0.8,
                'carbon_low': 0.8, 'carbon_high': 0.7
            },
            'energy_efficient': {
                'tier_5': 0.8, 'tier_4': 0.9, 'tier_3': 0.9, 'tier_2': 0.7, 'tier_1': 0.5,
                'tokens_abundant': 0.6, 'tokens_scarce': 0.9,
                'carbon_low': 0.7, 'carbon_high': 0.9
            },
            'bio_optimized': {
                'tier_5': 1.0, 'tier_4': 0.9, 'tier_3': 0.8, 'tier_2': 0.6, 'tier_1': 0.3,
                'tokens_abundant': 0.9, 'tokens_scarce': 0.5,
                'carbon_low': 0.9, 'carbon_high': 0.4
            }
        }
        
        logger.info("DynamicPipelineSelector initialized")
    
    def select_pipeline(
        self, task: Dict[str, Any], system_state: Dict[str, Any]
    ) -> Tuple[str, Dict[str, float]]:
        """
        Select optimal pipeline based on system state.
        
        Uses RL if available, otherwise falls back to heuristic scoring.
        """
        available_pipelines = list(self.pipelines.keys())
        
        # Filter unavailable pipelines (circuit breaker)
        if self.circuit_breaker:
            available_pipelines = [
                p for p in available_pipelines 
                if asyncio.run(self.circuit_breaker.is_available(p))[0]
            ]
        
        if not available_pipelines:
            # Fallback to all pipelines
            available_pipelines = list(self.pipelines.keys())
            logger.warning("No pipelines available, using all pipelines")
        
        # Use RL if enabled and trained
        if self.rl_learner and self.config.enable_reinforcement_learning:
            best_pipeline = self.rl_learner.get_best_pipeline(system_state, available_pipelines)
            scores = self.rl_learner.get_q_values(system_state)
            return best_pipeline, scores
        
        # Fallback to heuristic scoring
        scores = self._calculate_scores(system_state)
        
        # Filter scores for available pipelines
        filtered_scores = {p: scores.get(p, 0.0) for p in available_pipelines}
        
        # Select best pipeline
        best_pipeline = max(filtered_scores, key=filtered_scores.get)
        
        # Record selection
        self.pipeline_history.append({
            'timestamp': datetime.utcnow().isoformat(),
            'selected': best_pipeline,
            'scores': filtered_scores,
            'conditions': {
                'tier': system_state.get('degradation_tier', 5),
                'token_balance': system_state.get('token_balance', 1000),
                'carbon_gradient': system_state.get('carbon_gradient', 0.5)
            }
        })
        
        return best_pipeline, filtered_scores
    
    def _calculate_scores(self, system_state: Dict[str, Any]) -> Dict[str, float]:
        """Calculate heuristic scores for each pipeline"""
        scores = {}
        
        # Extract system conditions
        tier = system_state.get('degradation_tier', 5)
        token_balance = system_state.get('token_balance', 1000)
        carbon_gradient = system_state.get('carbon_gradient', 0.5)
        predicted_carbon = system_state.get('predicted_carbon', carbon_gradient)
        
        # Determine conditions
        tier_key = f'tier_{tier}'
        token_condition = 'tokens_abundant' if token_balance > 500 else 'tokens_scarce'
        carbon_condition = 'carbon_low' if carbon_gradient < 0.5 else 'carbon_high'
        
        for pipeline, suitability in self.pipeline_suitability.items():
            score = 0.0
            
            # Tier suitability
            score += suitability.get(tier_key, 0.5) * 0.3
            
            # Token suitability
            score += suitability.get(token_condition, 0.5) * 0.25
            
            # Carbon suitability
            score += suitability.get(carbon_condition, 0.5) * 0.25
            
            # Predictive adjustment
            if predicted_carbon > carbon_gradient:
                # Carbon worsening - prefer energy efficient
                if pipeline == 'energy_efficient':
                    score += 0.1
            elif predicted_carbon < carbon_gradient:
                # Carbon improving - can use more resources
                if pipeline in ['quantum_enhanced', 'bio_optimized']:
                    score += 0.1
            
            # Historical performance
            if pipeline in self.pipeline_performance:
                recent = self.pipeline_performance[pipeline][-20:]
                if recent:
                    score += np.mean(recent) * 0.2
            
            scores[pipeline] = score
        
        return scores
    
    def record_performance(self, pipeline: str, success: bool, latency_ms: float, 
                          reward: Optional[float] = None):
        """Record pipeline performance for learning"""
        # Update performance history
        if pipeline not in self.pipeline_performance:
            self.pipeline_performance[pipeline] = []
        
        score = (1.0 if success else 0.0) * 0.7 + (1.0 / (1.0 + latency_ms / 100)) * 0.3
        self.pipeline_performance[pipeline].append(score)
        
        # Update RL if enabled
        if self.rl_learner and self.config.enable_reinforcement_learning:
            # Use provided reward or calculate from success/latency
            if reward is None:
                reward = score
            # RL update would require state tracking
            # This is handled in the main runner
    
    def get_pipeline_stats(self) -> Dict[str, Any]:
        """Get pipeline selection statistics"""
        recent = list(self.pipeline_history)[-50:]
        
        pipeline_counts = defaultdict(int)
        for entry in recent:
            pipeline_counts[entry['selected']] += 1
        
        stats = {
            'recent_selections': dict(pipeline_counts),
            'pipeline_performance': {
                p: {
                    'avg_score': np.mean(scores[-20:]) if len(scores) >= 5 else 0.5,
                    'total_runs': len(scores)
                }
                for p, scores in self.pipeline_performance.items()
            },
            'last_selection': self.pipeline_history[-1] if self.pipeline_history else None
        }
        
        # Add RL stats
        if self.rl_learner:
            stats['rl_statistics'] = self.rl_learner.get_statistics()
        
        # Add circuit breaker stats
        if self.circuit_breaker:
            stats['circuit_breakers'] = {
                p: self.circuit_breaker.get_metrics(p)
                for p in self.pipeline_performance.keys()
            }
        
        return stats

# ============================================================================
# Enhanced Agent Runner
# ============================================================================

class EnhancedGreenAgentRunner:
    """
    Enhanced Green Agent Runner v6.0.0
    
    Features:
    - Dynamic pipeline selection with RL
    - Circuit breaker pattern for pipeline resilience
    - Task priority queue for intelligent scheduling
    - Real-time observability dashboard
    - Degradation-aware task processing
    - Predictive-informed scheduling
    - Bio-inspired orchestration
    - Graceful shutdown
    """
    
    def __init__(self, config: Optional[RunnerConfig] = None):
        # Load configuration
        self.config = config or RunnerConfig.from_env()
        logger.info(f"Loaded configuration: {self.config.model_dump()}")
        
        # Initialize bio-inspired core
        self.bio_core = None
        if BIO_AVAILABLE:
            try:
                self.bio_core = EnhancedBioInspiredCore()
            except Exception as e:
                logger.warning(f"Bio-inspired core not available: {str(e)}")
        
        # Initialize MoE ecosystem
        self.moe_ecosystem = None
        if BIO_AVAILABLE:
            try:
                self.moe_ecosystem = UnifiedMetabolicEcosystem()
            except Exception as e:
                logger.warning(f"MoE ecosystem not available: {str(e)}")
        
        # Pipeline selector with RL and circuit breakers
        self.pipeline_selector = DynamicPipelineSelector(self.config)
        
        # Available pipelines
        self.pipelines = {
            'standard': self._standard_pipeline,
            'quantum_enhanced': self._quantum_pipeline,
            'helium_optimized': self._helium_pipeline,
            'energy_efficient': self._energy_efficient_pipeline,
            'bio_optimized': self._bio_optimized_pipeline
        }
        
        # Task priority queue
        self.task_queue = TaskPriorityQueue(max_size=self.config.queue_max_size)
        
        # Dashboard server
        self.dashboard = AgentDashboardServer(self.config)
        
        # Task tracking
        self.total_tasks = 0
        self.successful_tasks = 0
        self.failed_tasks = 0
        self.task_history = deque(maxlen=1000)
        
        # State
        self.running = True
        self._worker_tasks: List[asyncio.Task] = []
        self._shutdown_event = asyncio.Event()
        
        # Register signal handlers
        self._register_signal_handlers()
        
        logger.info("Enhanced Green Agent Runner v6.0.0 initialized")
    
    def _load_config(self, config_path: Optional[str]) -> Dict[str, Any]:
        """Legacy config loader - kept for compatibility"""
        return self.config.model_dump()
    
    def _register_signal_handlers(self):
        """Register signal handlers for graceful shutdown"""
        try:
            loop = asyncio.get_event_loop()
            for sig in [signal.SIGINT, signal.SIGTERM]:
                loop.add_signal_handler(sig, lambda: asyncio.create_task(self.shutdown()))
        except NotImplementedError:
            pass
    
    # ========================================================================
    # System State Collection
    # ========================================================================
    
    def _get_system_state(self) -> Dict[str, Any]:
        """Collect current system state for pipeline selection"""
        state = {
            'degradation_tier': 5,
            'token_balance': 1000,
            'carbon_gradient': 0.5,
            'predicted_carbon': 0.5
        }
        
        if self.bio_core:
            # Get degradation tier
            if hasattr(self.bio_core, 'degradation_manager'):
                state['degradation_tier'] = self.bio_core.degradation_manager.current_tier.value
            
            # Get token balance
            if hasattr(self.bio_core, 'token_manager'):
                summary = self.bio_core.token_manager.get_system_summary()
                state['token_balance'] = summary.get('total_balance', 1000)
            
            # Get carbon gradient
            if hasattr(self.bio_core, 'gradient_manager'):
                strengths = self.bio_core.gradient_manager.get_field_strengths()
                state['carbon_gradient'] = strengths.get('carbon', 0.5)
            
            # Get predicted carbon
            if hasattr(self.bio_core, 'gradient_manager'):
                if hasattr(self.bio_core.gradient_manager, 'forecast'):
                    forecast = self.bio_core.gradient_manager.forecast('carbon', 300)
                    state['predicted_carbon'] = forecast.get('predicted', state['carbon_gradient'])
        
        return state
    
    # ========================================================================
    # Enhanced Task Processing with Queue and Fallback
    # ========================================================================
    
    async def submit_task(self, task: Dict[str, Any]) -> str:
        """Submit a task to the priority queue"""
        # Calculate priority
        state = self._get_system_state()
        priority = self.task_queue.calculate_priority(task, state)
        
        # Add task ID if not present
        if 'task_id' not in task:
            task['task_id'] = f"task_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{self.total_tasks}"
        
        # Push to queue
        await self.task_queue.push(task, priority)
        logger.debug(f"Task {task['task_id']} queued with priority {priority:.2f}")
        
        return task['task_id']
    
    async def process_task(self, task: Dict[str, Any]) -> Dict[str, Any]:
        """
        Process a task with dynamic pipeline selection and circuit breaker fallback.
        """
        start_time = datetime.utcnow()
        self.total_tasks += 1
        task_id = task.get('task_id', 'unknown')
        
        # Get system state
        system_state = self._get_system_state()
        
        # Check degradation tier
        if self.config.enable_degradation_aware:
            tier = system_state['degradation_tier']
            if tier <= 1:
                return {
                    'success': False,
                    'reason': f'System in survival mode (tier {tier})',
                    'task_id': task_id
                }
            
            # Adjust task priority based on tier
            if tier <= 2 and task.get('priority', 2) > 1:
                return {
                    'success': False,
                    'reason': f'Non-critical tasks deferred in tier {tier}',
                    'task_id': task_id
                }
        
        # Select pipeline using RL
        if self.config.enable_dynamic_pipeline:
            pipeline_name, scores = self.pipeline_selector.select_pipeline(task, system_state)
        else:
            pipeline_name = task.get('pipeline', 'standard')
        
        # Execute pipeline with circuit breaker fallback
        result = await self._execute_with_fallback(
            task, pipeline_name, system_state
        )
        
        # Record performance
        success = result.get('success', False)
        latency = (datetime.utcnow() - start_time).total_seconds() * 1000
        
        # Calculate reward for RL
        reward = 0.0
        if success:
            reward = 1.0 - min(1.0, latency / 1000)  # Normalize
        else:
            reward = -1.0
        
        # Record performance with RL
        self.pipeline_selector.record_performance(
            pipeline_name, success, latency, reward
        )
        
        # Update RL if enabled
        if self.config.enable_reinforcement_learning and self.pipeline_selector.rl_learner:
            next_state = self._get_system_state()
            await self.pipeline_selector.rl_learner.update(
                system_state, pipeline_name, reward, next_state
            )
        
        # Update task tracking
        if success:
            self.successful_tasks += 1
        else:
            self.failed_tasks += 1
        
        # Record history
        self.task_history.append({
            'task_id': task_id,
            'pipeline': pipeline_name,
            'success': success,
            'latency_ms': latency,
            'timestamp': datetime.utcnow().isoformat()
        })
        
        # Add metadata
        result['pipeline_used'] = pipeline_name
        result['pipeline_scores'] = scores
        result['system_state'] = {
            'tier': system_state['degradation_tier'],
            'token_balance': system_state['token_balance'],
            'carbon_gradient': system_state['carbon_gradient']
        }
        
        # Broadcast status to dashboard
        if self.config.enable_dashboard:
            await self.dashboard.broadcast_status(self.get_status())
        
        return result
    
    async def _execute_with_fallback(self, task: Dict[str, Any], 
                                     initial_pipeline: str,
                                     system_state: Dict[str, Any]) -> Dict[str, Any]:
        """Execute pipeline with circuit breaker fallback"""
        # Build fallback chain
        fallback_chain = [initial_pipeline] + self.config.fallback_pipelines
        # Remove duplicates while preserving order
        seen = set()
        fallback_chain = [p for p in fallback_chain if not (p in seen or seen.add(p))]
        
        # Try each pipeline in order
        for pipeline_name in fallback_chain:
            try:
                # Check circuit breaker
                if self.config.enable_circuit_breakers:
                    available, state = await self.pipeline_selector.circuit_breaker.is_available(pipeline_name)
                    if not available:
                        logger.warning(f"Pipeline {pipeline_name} unavailable (state: {state})")
                        continue
                
                # Execute pipeline
                pipeline_func = self.pipelines.get(pipeline_name)
                if not pipeline_func:
                    logger.warning(f"Pipeline {pipeline_name} not found")
                    continue
                
                # Apply timeout
                try:
                    result = await asyncio.wait_for(
                        pipeline_func(task),
                        timeout=self.config.task_timeout_seconds
                    )
                    
                    # Record success in circuit breaker
                    if self.config.enable_circuit_breakers:
                        await self.pipeline_selector.circuit_breaker.record_success(pipeline_name)
                    
                    return result
                    
                except asyncio.TimeoutError:
                    logger.error(f"Pipeline {pipeline_name} timed out after {self.config.task_timeout_seconds}s")
                    if self.config.enable_circuit_breakers:
                        await self.pipeline_selector.circuit_breaker.record_failure(pipeline_name)
                    continue
                    
            except Exception as e:
                logger.error(f"Pipeline {pipeline_name} failed: {str(e)}")
                if self.config.enable_circuit_breakers:
                    await self.pipeline_selector.circuit_breaker.record_failure(pipeline_name)
                continue
        
        # All pipelines failed
        return {
            'success': False,
            'error': 'All pipelines failed',
            'task_id': task.get('task_id', 'unknown'),
            'tried_pipelines': fallback_chain
        }
    
    # ========================================================================
    # Worker Management
    # ========================================================================
    
    async def _worker_loop(self, worker_id: int):
        """Worker loop for processing queued tasks"""
        logger.info(f"Worker {worker_id} started")
        
        while self.running:
            try:
                # Get next task
                task = await self.task_queue.pop()
                if task is None:
                    await asyncio.sleep(0.1)
                    continue
                
                # Process task
                result = await self.process_task(task)
                
                # Update task status if callback provided
                if 'callback' in task:
                    try:
                        if asyncio.iscoroutinefunction(task['callback']):
                            await task['callback'](result)
                        else:
                            task['callback'](result)
                    except Exception as e:
                        logger.error(f"Callback error for task {task.get('task_id')}: {e}")
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Worker {worker_id} error: {e}")
                await asyncio.sleep(0.5)
        
        logger.info(f"Worker {worker_id} stopped")
    
    async def start_workers(self, num_workers: int = None):
        """Start worker pool"""
        if num_workers is None:
            num_workers = self.config.max_concurrent_tasks
        
        for i in range(num_workers):
            worker = asyncio.create_task(self._worker_loop(i))
            self._worker_tasks.append(worker)
        
        logger.info(f"Started {num_workers} workers")
    
    # ========================================================================
    # Batch Processing
    # ========================================================================
    
    async def batch_process(self, tasks: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Process batch of tasks by submitting to queue and collecting results"""
        # Submit all tasks
        task_ids = []
        for task in tasks:
            task_id = await self.submit_task(task)
            task_ids.append(task_id)
        
        # Wait for tasks to complete (simplified - in production use proper tracking)
        results = []
        for i, task_id in enumerate(task_ids):
            # This is a placeholder - in real implementation, track task completion
            # For now, process directly
            result = await self.process_task(tasks[i])
            results.append(result)
        
        return results
    
    # ========================================================================
    # Pipeline Implementations
    # ========================================================================
    
    async def _standard_pipeline(self, task: Dict[str, Any]) -> Dict[str, Any]:
        """Standard processing pipeline"""
        # Simulate processing
        await asyncio.sleep(0.01)
        return {'success': True, 'pipeline': 'standard', 'task_id': task.get('task_id')}
    
    async def _quantum_pipeline(self, task: Dict[str, Any]) -> Dict[str, Any]:
        """Quantum-enhanced pipeline"""
        if not task.get('quantum_capable', False):
            # Simulate fallback to standard for non-capable tasks
            return await self._standard_pipeline(task)
        
        # Simulate quantum processing
        await asyncio.sleep(0.02)
        return {'success': True, 'pipeline': 'quantum', 'task_id': task.get('task_id')}
    
    async def _helium_pipeline(self, task: Dict[str, Any]) -> Dict[str, Any]:
        """Helium-optimized pipeline"""
        # Simulate helium processing
        await asyncio.sleep(0.015)
        return {'success': True, 'pipeline': 'helium', 'task_id': task.get('task_id')}
    
    async def _energy_efficient_pipeline(self, task: Dict[str, Any]) -> Dict[str, Any]:
        """Energy-efficient pipeline"""
        # Simulate energy-efficient processing
        await asyncio.sleep(0.005)
        return {'success': True, 'pipeline': 'energy_efficient', 'task_id': task.get('task_id')}
    
    async def _bio_optimized_pipeline(self, task: Dict[str, Any]) -> Dict[str, Any]:
        """Bio-optimized pipeline"""
        if self.moe_ecosystem:
            # Route through MoE ecosystem
            try:
                result = self.moe_ecosystem.process_task(task)
                result['pipeline'] = 'bio_optimized'
                return result
            except Exception as e:
                logger.error(f"MoE ecosystem error: {e}")
                return await self._standard_pipeline(task)
        
        return await self._standard_pipeline(task)
    
    # ========================================================================
    # Status and Shutdown
    # ========================================================================
    
    def get_status(self) -> Dict[str, Any]:
        """Get comprehensive agent status"""
        system_state = self._get_system_state()
        
        return {
            'version': '6.0.0',
            'total_tasks': self.total_tasks,
            'successful_tasks': self.successful_tasks,
            'failed_tasks': self.failed_tasks,
            'success_rate': self.successful_tasks / max(self.total_tasks, 1),
            'queue_size': len(self.task_queue.heap),
            'pipeline_stats': self.pipeline_selector.get_pipeline_stats(),
            'system_state': system_state,
            'running': self.running,
            'config': self.config.model_dump(),
            'timestamp': datetime.utcnow().isoformat()
        }
    
    async def start(self):
        """Start the runner"""
        logger.info("Starting Enhanced Green Agent Runner v6.0.0...")
        
        # Start dashboard
        await self.dashboard.start()
        
        # Start workers
        await self.start_workers()
        
        # Start Prometheus metrics server
        if self.config.enable_prometheus and PROMETHEUS_AVAILABLE:
            try:
                start_http_server(9090)
                logger.info("Prometheus metrics server started on port 9090")
            except Exception as e:
                logger.warning(f"Failed to start Prometheus server: {e}")
        
        logger.info("Enhanced Green Agent Runner started successfully")
    
    async def shutdown(self):
        """Graceful shutdown"""
        if not self.running:
            return
        
        logger.info("Shutting down Enhanced Green Agent Runner...")
        self.running = False
        self._shutdown_event.set()
        
        # Cancel workers
        for worker in self._worker_tasks:
            worker.cancel()
        
        # Wait for workers to finish
        if self._worker_tasks:
            await asyncio.gather(*self._worker_tasks, return_exceptions=True)
        
        # Stop dashboard
        await self.dashboard.stop()
        
        # Shutdown bio core
        if self.bio_core:
            await self.bio_core.shutdown()
        
        logger.info("Enhanced Green Agent Runner shutdown complete")
    
    async def __aenter__(self):
        await self.start()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.shutdown()

# ============================================================================
# CLI Entry Point
# ============================================================================

async def main():
    """Main entry point"""
    # Load configuration
    config = RunnerConfig.from_env()
    
    # Create and start runner
    async with EnhancedGreenAgentRunner(config) as runner:
        logger.info("Agent running. Press Ctrl+C to stop.")
        
        # Keep running until interrupted
        try:
            while runner.running:
                await asyncio.sleep(1)
                
                # Print status every 30 seconds
                if int(time.time()) % 30 == 0:
                    status = runner.get_status()
                    logger.info(f"Status: {status['total_tasks']} tasks, "
                              f"{status['success_rate']*100:.1f}% success rate, "
                              f"queue: {status['queue_size']}")
        except KeyboardInterrupt:
            logger.info("Received interrupt signal")
        except Exception as e:
            logger.error(f"Runtime error: {e}")
        
        # Let context manager handle shutdown

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Graceful shutdown complete")
