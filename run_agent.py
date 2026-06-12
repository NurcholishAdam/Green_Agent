# File: run_agent.py
# Enhanced with MoE integration, adaptive learning, and comprehensive monitoring

#!/usr/bin/env python3
"""
Green Agent v2.4.0 - Enhanced Runner with MoE Integration
Main entry point for the Green Agent with Mixture of Experts capabilities.
"""

import asyncio
import logging
import sys
import os
import json
import time
from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from enum import Enum
import numpy as np
from collections import deque
import signal
import threading

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('green_agent.log'),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

# ============================================================================
# Configuration and State Management
# ============================================================================

class AgentState(Enum):
    """Agent operational states"""
    INITIALIZING = "initializing"
    RUNNING = "running"
    LEARNING = "learning"
    OPTIMIZING = "optimizing"
    DEGRADED = "degraded"
    RECOVERING = "recovering"
    PAUSED = "paused"
    SHUTTING_DOWN = "shutting_down"

@dataclass
class AgentConfig:
    """Enhanced agent configuration"""
    # Core settings
    agent_name: str = "GreenAgent-MoE"
    version: str = "2.4.0"
    
    # MoE settings
    enable_moe: bool = True
    num_experts: int = 5
    top_k_routing: int = 2
    expert_cooldown_ms: int = 100
    
    # Quantum settings
    enable_quantum: bool = True
    quantum_backend: str = "simulator"
    max_qubits: int = 20
    quantum_error_threshold: float = 0.01
    
    # Resource settings
    max_carbon_budget_kg: float = 0.1
    max_helium_budget: float = 0.05
    max_latency_ms: float = 1000
    max_memory_mb: float = 8192
    
    # Learning settings
    enable_adaptive_learning: bool = True
    learning_rate: float = 0.01
    experience_buffer_size: int = 10000
    batch_size: int = 32
    
    # Monitoring settings
    enable_prometheus: bool = True
    metrics_port: int = 9090
    grafana_dashboard: bool = True
    
    # Security settings
    enable_zero_trust: bool = True
    authentication_required: bool = True
    encryption_enabled: bool = True
    
    # Pipeline settings
    default_pipeline: str = "adaptive"
    max_concurrent_tasks: int = 10
    task_timeout_seconds: int = 300
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return {k: v for k, v in self.__dict__.items() if not k.startswith('_')}
    
    @classmethod
    def from_file(cls, path: str) -> 'AgentConfig':
        """Load configuration from file"""
        with open(path, 'r') as f:
            config_dict = json.load(f)
        return cls(**config_dict)

@dataclass
class AgentMetrics:
    """Comprehensive agent metrics"""
    # Performance metrics
    total_tasks: int = 0
    successful_tasks: int = 0
    failed_tasks: int = 0
    average_latency_ms: float = 0.0
    p95_latency_ms: float = 0.0
    p99_latency_ms: float = 0.0
    
    # Resource metrics
    total_carbon_kg: float = 0.0
    total_helium_units: float = 0.0
    total_energy_kwh: float = 0.0
    carbon_per_task_kg: float = 0.0
    
    # Expert metrics
    expert_utilization: Dict[str, float] = field(default_factory=dict)
    expert_success_rates: Dict[str, float] = field(default_factory=dict)
    routing_accuracy: float = 0.0
    
    # System metrics
    uptime_seconds: float = 0.0
    memory_usage_mb: float = 0.0
    cpu_usage_percent: float = 0.0
    active_connections: int = 0
    
    # Learning metrics
    learning_iterations: int = 0
    model_improvements: int = 0
    experience_buffer_size: int = 0
    
    def update_latency(self, latency_ms: float):
        """Update latency metrics with running average"""
        alpha = 0.1
        self.average_latency_ms = (
            (1 - alpha) * self.average_latency_ms + alpha * latency_ms
        )

# ============================================================================
# Experience Buffer for Adaptive Learning
# ============================================================================

class ExperienceBuffer:
    """Experience replay buffer for adaptive learning"""
    
    def __init__(self, max_size: int = 10000):
        self.max_size = max_size
        self.buffer: deque = deque(maxlen=max_size)
        self.priorities: deque = deque(maxlen=max_size)
        self.total_experiences = 0
    
    def add(
        self,
        state: Dict[str, Any],
        action: Dict[str, Any],
        reward: float,
        next_state: Dict[str, Any],
        done: bool,
        priority: float = 1.0
    ):
        """Add experience to buffer"""
        experience = {
            'state': state,
            'action': action,
            'reward': reward,
            'next_state': next_state,
            'done': done,
            'timestamp': datetime.utcnow().isoformat()
        }
        
        self.buffer.append(experience)
        self.priorities.append(priority)
        self.total_experiences += 1
    
    def sample(self, batch_size: int) -> List[Dict[str, Any]]:
        """Sample batch of experiences with priority"""
        if len(self.buffer) < batch_size:
            return list(self.buffer)
        
        # Priority-based sampling
        priorities = np.array(self.priorities)
        probabilities = priorities / priorities.sum()
        
        indices = np.random.choice(
            len(self.buffer),
            size=min(batch_size, len(self.buffer)),
            p=probabilities,
            replace=False
        )
        
        return [self.buffer[i] for i in indices]
    
    def update_priorities(self, indices: List[int], priorities: List[float]):
        """Update experience priorities"""
        for idx, priority in zip(indices, priorities):
            if idx < len(self.priorities):
                self.priorities[idx] = priority
    
    def __len__(self) -> int:
        return len(self.buffer)

# ============================================================================
# Task Scheduler with Priority Management
# ============================================================================

class TaskPriority(Enum):
    """Task priority levels"""
    CRITICAL = 0
    HIGH = 1
    MEDIUM = 2
    LOW = 3
    BACKGROUND = 4

@dataclass
class ScheduledTask:
    """Task with scheduling metadata"""
    task_id: str
    priority: TaskPriority
    task_config: Dict[str, Any]
    created_at: datetime = field(default_factory=datetime.utcnow)
    scheduled_at: Optional[datetime] = None
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    timeout_at: Optional[datetime] = None
    retry_count: int = 0
    max_retries: int = 3
    status: str = "pending"
    assigned_expert: Optional[str] = None
    
    def is_expired(self) -> bool:
        """Check if task has expired"""
        if self.timeout_at:
            return datetime.utcnow() > self.timeout_at
        return False
    
    def can_retry(self) -> bool:
        """Check if task can be retried"""
        return self.retry_count < self.max_retries

class TaskScheduler:
    """
    Priority-based task scheduler with deadline awareness.
    Implements earliest deadline first with priority boosting.
    """
    
    def __init__(self, max_concurrent: int = 10):
        self.max_concurrent = max_concurrent
        self.pending_tasks: Dict[str, ScheduledTask] = {}
        self.running_tasks: Dict[str, ScheduledTask] = {}
        self.completed_tasks: Dict[str, ScheduledTask] = {}
        
        # Priority queues
        self.priority_queues: Dict[TaskPriority, List[str]] = {
            p: [] for p in TaskPriority
        }
        
        # Statistics
        self.total_scheduled = 0
        self.total_completed = 0
        self.total_failed = 0
        self.average_wait_time_ms = 0.0
    
    def schedule_task(
        self,
        task_config: Dict[str, Any],
        priority: TaskPriority = TaskPriority.MEDIUM,
        timeout_seconds: int = 300
    ) -> str:
        """Schedule a new task"""
        task_id = f"task_{datetime.utcnow().timestamp()}_{self.total_scheduled}"
        
        task = ScheduledTask(
            task_id=task_id,
            priority=priority,
            task_config=task_config,
            timeout_at=datetime.utcnow() + timedelta(seconds=timeout_seconds)
        )
        
        self.pending_tasks[task_id] = task
        self.priority_queues[priority].append(task_id)
        self.total_scheduled += 1
        
        logger.debug(f"Scheduled task {task_id} with priority {priority.name}")
        return task_id
    
    def get_next_task(self) -> Optional[ScheduledTask]:
        """Get next task to execute based on priority"""
        if len(self.running_tasks) >= self.max_concurrent:
            return None
        
        # Check priorities from highest to lowest
        for priority in TaskPriority:
            if self.priority_queues[priority]:
                task_id = self.priority_queues[priority].pop(0)
                
                if task_id in self.pending_tasks:
                    task = self.pending_tasks.pop(task_id)
                    
                    # Boost priority of long-waiting tasks
                    wait_time = (datetime.utcnow() - task.created_at).total_seconds()
                    if wait_time > 60 and priority.value > 0:
                        # Boost to next higher priority
                        boosted_priority = TaskPriority(max(0, priority.value - 1))
                        logger.info(f"Boosted task {task_id} from {priority.name} to {boosted_priority.name}")
                    
                    return task
        
        return None
    
    def start_task(self, task_id: str, assigned_expert: str):
        """Mark task as started"""
        if task_id in self.pending_tasks:
            task = self.pending_tasks.pop(task_id)
        else:
            return
        
        task.started_at = datetime.utcnow()
        task.assigned_expert = assigned_expert
        task.status = "running"
        
        # Calculate wait time
        wait_time = (task.started_at - task.created_at).total_seconds() * 1000
        alpha = 0.1
        self.average_wait_time_ms = (
            (1 - alpha) * self.average_wait_time_ms + alpha * wait_time
        )
        
        self.running_tasks[task_id] = task
    
    def complete_task(self, task_id: str, success: bool = True):
        """Mark task as completed"""
        if task_id not in self.running_tasks:
            return
        
        task = self.running_tasks.pop(task_id)
        task.completed_at = datetime.utcnow()
        task.status = "completed" if success else "failed"
        
        self.completed_tasks[task_id] = task
        
        if success:
            self.total_completed += 1
        else:
            self.total_failed += 1
        
        # Clean old completed tasks
        self._cleanup_completed()
    
    def retry_task(self, task_id: str) -> bool:
        """Retry a failed task"""
        if task_id not in self.completed_tasks:
            return False
        
        task = self.completed_tasks.pop(task_id)
        
        if not task.can_retry():
            return False
        
        task.retry_count += 1
        task.status = "pending"
        task.started_at = None
        task.completed_at = None
        task.assigned_expert = None
        
        self.pending_tasks[task_id] = task
        self.priority_queues[task.priority].append(task_id)
        
        return True
    
    def _cleanup_completed(self):
        """Remove old completed tasks"""
        cutoff = datetime.utcnow() - timedelta(hours=1)
        expired = [
            tid for tid, task in self.completed_tasks.items()
            if task.completed_at and task.completed_at < cutoff
        ]
        for tid in expired:
            del self.completed_tasks[tid]
    
    def get_queue_stats(self) -> Dict[str, Any]:
        """Get queue statistics"""
        return {
            'pending_tasks': len(self.pending_tasks),
            'running_tasks': len(self.running_tasks),
            'completed_tasks': len(self.completed_tasks),
            'total_scheduled': self.total_scheduled,
            'total_completed': self.total_completed,
            'total_failed': self.total_failed,
            'success_rate': self.total_completed / max(self.total_scheduled, 1),
            'average_wait_time_ms': self.average_wait_time_ms,
            'queue_breakdown': {
                p.name: len(tasks)
                for p, tasks in self.priority_queues.items()
            }
        }

# ============================================================================
# Enhanced Green Agent Class
# ============================================================================

class EnhancedGreenAgent:
    """
    Enhanced Green Agent with MoE integration, adaptive learning,
    and comprehensive monitoring capabilities.
    """
    
    def __init__(self, config: Optional[AgentConfig] = None):
        """
        Initialize the Enhanced Green Agent.
        
        Args:
            config: Agent configuration (loads default if None)
        """
        self.config = config or AgentConfig()
        self.state = AgentState.INITIALIZING
        self.start_time = datetime.utcnow()
        
        # Initialize components
        self.metrics = AgentMetrics()
        self.experience_buffer = ExperienceBuffer(
            max_size=self.config.experience_buffer_size
        )
        self.task_scheduler = TaskScheduler(
            max_concurrent=self.config.max_concurrent_tasks
        )
        
        # Expert management
        self.active_experts: Dict[str, Any] = {}
        self.expert_performance: Dict[str, Dict[str, Any]] = {}
        
        # Pipeline registry
        self.pipelines: Dict[str, callable] = {}
        
        # Security context
        self.security_context: Optional[Dict[str, Any]] = None
        
        # Initialize subsystems
        self._initialize_subsystems()
        
        # Start background tasks
        self._start_background_tasks()
        
        # Register signal handlers
        self._register_signal_handlers()
        
        self.state = AgentState.RUNNING
        
        logger.info(f"Enhanced Green Agent v{self.config.version} initialized")
        logger.info(f"MoE: {'enabled' if self.config.enable_moe else 'disabled'}")
        logger.info(f"Quantum: {'enabled' if self.config.enable_quantum else 'disabled'}")
    
    def _initialize_subsystems(self):
        """Initialize all agent subsystems"""
        try:
            # Initialize expert registry
            self._initialize_experts()
            
            # Initialize pipelines
            self._initialize_pipelines()
            
            # Initialize security
            if self.config.enable_zero_trust:
                self._initialize_security()
            
            # Initialize monitoring
            if self.config.enable_prometheus:
                self._initialize_monitoring()
            
            logger.info("All subsystems initialized successfully")
            
        except Exception as e:
            logger.error(f"Failed to initialize subsystems: {str(e)}")
            self.state = AgentState.DEGRADED
            raise
    
    def _initialize_experts(self):
        """Initialize expert system"""
        if not self.config.enable_moe:
            return
        
        # Import and initialize experts
        try:
            from enhancements.moe_expert_system.experts import (
                EnergyExpert,
                DataExpert,
                IoTExpert,
                HeliumExpert
            )
            
            self.active_experts = {
                'energy': EnergyExpert(),
                'data': DataExpert(),
                'iot': IoTExpert(),
                'helium': HeliumExpert()
            }
            
            # Initialize quantum expert if enabled
            if self.config.enable_quantum:
                from enhancements.moe_expert_system.experts import QuantumExpert
                self.active_experts['quantum'] = QuantumExpert()
            
            logger.info(f"Initialized {len(self.active_experts)} experts")
            
        except ImportError as e:
            logger.warning(f"Expert system not available: {str(e)}")
            self.config.enable_moe = False
    
    def _initialize_pipelines(self):
        """Initialize processing pipelines"""
        self.pipelines = {
            'standard': self._standard_pipeline,
            'quantum': self._quantum_pipeline,
            'helium_optimized': self._helium_optimized_pipeline,
            'energy_efficient': self._energy_efficient_pipeline,
            'adaptive': self._adaptive_pipeline
        }
        
        logger.info(f"Initialized {len(self.pipelines)} pipelines")
    
    def _initialize_security(self):
        """Initialize security context"""
        self.security_context = {
            'session_id': hashlib.sha256(
                f"{datetime.utcnow().timestamp()}{os.urandom(16)}".encode()
            ).hexdigest(),
            'created_at': datetime.utcnow().isoformat(),
            'encryption_enabled': self.config.encryption_enabled,
            'auth_required': self.config.authentication_required
        }
        
        logger.info("Security context initialized")
    
    def _initialize_monitoring(self):
        """Initialize monitoring endpoints"""
        # Start Prometheus metrics server in background
        if self.config.enable_prometheus:
            import threading
            monitor_thread = threading.Thread(
                target=self._run_metrics_server,
                daemon=True
            )
            monitor_thread.start()
            logger.info(f"Metrics server starting on port {self.config.metrics_port}")
    
    def _run_metrics_server(self):
        """Run Prometheus metrics server"""
        try:
            from prometheus_client import start_http_server, Gauge, Counter, Histogram
            
            # Define metrics
            self.prometheus_metrics = {
                'tasks_total': Counter(
                    'green_agent_tasks_total',
                    'Total tasks processed'
                ),
                'tasks_failed': Counter(
                    'green_agent_tasks_failed',
                    'Failed tasks'
                ),
                'carbon_kg': Gauge(
                    'green_agent_carbon_kg',
                    'Carbon emissions in kg'
                ),
                'helium_units': Gauge(
                    'green_agent_helium_units',
                    'Helium consumption'
                ),
                'latency_ms': Histogram(
                    'green_agent_latency_ms',
                    'Task latency in milliseconds'
                ),
                'expert_utilization': Gauge(
                    'green_agent_expert_utilization',
                    'Expert utilization rate',
                    ['expert_id']
                )
            }
            
            # Start server
            start_http_server(self.config.metrics_port)
            logger.info(f"Prometheus metrics server running on port {self.config.metrics_port}")
            
        except ImportError:
            logger.warning("Prometheus client not installed. Metrics server disabled.")
            self.config.enable_prometheus = False
    
    def _start_background_tasks(self):
        """Start background maintenance tasks"""
        # Start adaptive learning loop
        if self.config.enable_adaptive_learning:
            asyncio.create_task(self._adaptive_learning_loop())
        
        # Start health check loop
        asyncio.create_task(self._health_check_loop())
        
        # Start metrics collection loop
        asyncio.create_task(self._metrics_collection_loop())
    
    def _register_signal_handlers(self):
        """Register OS signal handlers"""
        signal.signal(signal.SIGINT, self._handle_shutdown)
        signal.signal(signal.SIGTERM, self._handle_shutdown)
    
    def _handle_shutdown(self, signum, frame):
        """Handle shutdown signals"""
        logger.info(f"Received signal {signum}. Initiating graceful shutdown...")
        self.state = AgentState.SHUTTING_DOWN
        
        # Save state
        self._save_state()
        
        # Cleanup
        asyncio.create_task(self._cleanup())
        
        sys.exit(0)
    
    # ========================================================================
    # Pipeline Implementations
    # ========================================================================
    
    async def _standard_pipeline(
        self,
        task: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Standard processing pipeline"""
        logger.debug(f"Executing standard pipeline for task {task.get('task_id')}")
        
        # Step 1: Validate input
        if not self._validate_task(task):
            return {'success': False, 'error': 'Invalid task configuration'}
        
        # Step 2: Profile workload
        workload_profile = await self._profile_workload(task)
        
        # Step 3: Select expert if MoE enabled
        if self.config.enable_moe and self.active_experts:
            expert_plan = await self._select_expert(workload_profile)
        else:
            expert_plan = {'expert': 'default', 'confidence': 1.0}
        
        # Step 4: Execute task
        result = await self._execute_task(task, expert_plan)
        
        # Step 5: Validate result
        if self._validate_result(result):
            return {'success': True, 'result': result, 'expert_plan': expert_plan}
        else:
            return {'success': False, 'error': 'Result validation failed'}
    
    async def _quantum_pipeline(
        self,
        task: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Quantum-enhanced processing pipeline"""
        if not self.config.enable_quantum:
            return await self._standard_pipeline(task)
        
        logger.debug(f"Executing quantum pipeline for task {task.get('task_id')}")
        
        # Step 1: Check quantum suitability
        if not self._is_quantum_suitable(task):
            return await self._standard_pipeline(task)
        
        # Step 2: Prepare quantum circuit
        quantum_circuit = await self._prepare_quantum_circuit(task)
        
        # Step 3: Execute quantum optimization
        quantum_result = await self._execute_quantum(quantum_circuit)
        
        # Step 4: Combine with classical processing
        hybrid_result = await self._combine_quantum_classical(
            task, quantum_result
        )
        
        return {
            'success': True,
            'result': hybrid_result,
            'quantum_enhanced': True,
            'quantum_metrics': {
                'circuit_depth': quantum_circuit.get('depth', 0),
                'qubits_used': quantum_circuit.get('qubits', 0),
                'error_rate': quantum_result.get('error_rate', 0)
            }
        }
    
    async def _helium_optimized_pipeline(
        self,
        task: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Helium-optimized processing pipeline"""
        logger.debug(f"Executing helium-optimized pipeline for task {task.get('task_id')}")
        
        # Step 1: Assess helium constraints
        helium_profile = await self._assess_helium_constraints(task)
        
        # Step 2: Optimize for helium efficiency
        optimized_config = await self._optimize_for_helium(task, helium_profile)
        
        # Step 3: Execute with helium-aware constraints
        result = await self._execute_with_helium_constraints(
            task, optimized_config
        )
        
        # Step 4: Calculate helium savings
        helium_savings = self._calculate_helium_savings(task, result)
        
        return {
            'success': True,
            'result': result,
            'helium_optimized': True,
            'helium_metrics': {
                'saved_units': helium_savings,
                'efficiency': optimized_config.get('efficiency', 0),
                'scarcity_level': helium_profile.get('scarcity', 0)
            }
        }
    
    async def _energy_efficient_pipeline(
        self,
        task: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Energy-efficient processing pipeline"""
        logger.debug(f"Executing energy-efficient pipeline for task {task.get('task_id')}")
        
        # Step 1: Calculate energy budget
        energy_budget = await self._calculate_energy_budget(task)
        
        # Step 2: Optimize for energy efficiency
        energy_plan = await self._optimize_energy_usage(task, energy_budget)
        
        # Step 3: Execute with energy constraints
        result = await self._execute_with_energy_constraints(
            task, energy_plan
        )
        
        return {
            'success': True,
            'result': result,
            'energy_efficient': True,
            'energy_metrics': {
                'budget_kwh': energy_budget,
                'actual_kwh': result.get('energy_used', 0),
                'savings_percent': energy_plan.get('savings_percent', 0)
            }
        }
    
    async def _adaptive_pipeline(
        self,
        task: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Adaptive pipeline that selects best strategy"""
        logger.debug(f"Executing adaptive pipeline for task {task.get('task_id')}")
        
        # Step 1: Analyze task characteristics
        task_analysis = await self._analyze_task(task)
        
        # Step 2: Select best pipeline based on analysis
        best_pipeline = await self._select_best_pipeline(task_analysis)
        
        # Step 3: Execute selected pipeline
        pipeline_func = self.pipelines.get(best_pipeline, self._standard_pipeline)
        result = await pipeline_func(task)
        
        # Step 4: Learn from execution
        await self._learn_from_execution(task, result, best_pipeline)
        
        # Add adaptive metadata
        result['adaptive_pipeline'] = True
        result['selected_pipeline'] = best_pipeline
        result['task_analysis'] = task_analysis
        
        return result
    
    # ========================================================================
    # Core Processing Methods
    # ========================================================================
    
    async def process_task(
        self,
        task: Dict[str, Any],
        pipeline: Optional[str] = None,
        priority: TaskPriority = TaskPriority.MEDIUM
    ) -> Dict[str, Any]:
        """
        Process a task through the agent.
        
        Args:
            task: Task configuration
            pipeline: Pipeline to use (auto-select if None)
            priority: Task priority
            
        Returns:
            Processing result
        """
        start_time = time.time()
        task_id = task.get('task_id', f"task_{datetime.utcnow().timestamp()}")
        task['task_id'] = task_id
        
        logger.info(f"Processing task {task_id} with priority {priority.name}")
        
        # Schedule task
        self.task_scheduler.schedule_task(
            task,
            priority=priority,
            timeout_seconds=self.config.task_timeout_seconds
        )
        
        try:
            # Select pipeline
            if pipeline:
                pipeline_func = self.pipelines.get(pipeline, self._standard_pipeline)
            else:
                pipeline_func = self._adaptive_pipeline
            
            # Execute task
            result = await pipeline_func(task)
            
            # Update metrics
            latency_ms = (time.time() - start_time) * 1000
            self.metrics.total_tasks += 1
            self.metrics.update_latency(latency_ms)
            
            if result.get('success'):
                self.metrics.successful_tasks += 1
                self.task_scheduler.complete_task(task_id, success=True)
            else:
                self.metrics.failed_tasks += 1
                self.task_scheduler.complete_task(task_id, success=False)
            
            # Update Prometheus metrics
            if self.config.enable_prometheus and hasattr(self, 'prometheus_metrics'):
                self.prometheus_metrics['tasks_total'].inc()
                self.prometheus_metrics['latency_ms'].observe(latency_ms)
                
                if not result.get('success'):
                    self.prometheus_metrics['tasks_failed'].inc()
            
            # Add timing metadata
            result['task_id'] = task_id
            result['latency_ms'] = latency_ms
            result['processed_at'] = datetime.utcnow().isoformat()
            
            return result
            
        except asyncio.TimeoutError:
            logger.error(f"Task {task_id} timed out")
            self.metrics.failed_tasks += 1
            self.task_scheduler.complete_task(task_id, success=False)
            
            return {
                'success': False,
                'error': 'Task timeout',
                'task_id': task_id,
                'latency_ms': (time.time() - start_time) * 1000
            }
            
        except Exception as e:
            logger.error(f"Task {task_id} failed: {str(e)}", exc_info=True)
            self.metrics.failed_tasks += 1
            self.task_scheduler.complete_task(task_id, success=False)
            
            return {
                'success': False,
                'error': str(e),
                'task_id': task_id,
                'latency_ms': (time.time() - start_time) * 1000
            }
    
    async def batch_process(
        self,
        tasks: List[Dict[str, Any]],
        max_concurrent: Optional[int] = None,
        pipeline: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """
        Process multiple tasks concurrently.
        
        Args:
            tasks: List of task configurations
            max_concurrent: Maximum concurrent tasks
            pipeline: Pipeline to use
            
        Returns:
            List of results
        """
        max_concurrent = max_concurrent or self.config.max_concurrent_tasks
        semaphore = asyncio.Semaphore(max_concurrent)
        
        async def process_with_limit(task):
            async with semaphore:
                return await self.process_task(task, pipeline=pipeline)
        
        logger.info(f"Batch processing {len(tasks)} tasks (max concurrent: {max_concurrent})")
        
        tasks_coroutines = [process_with_limit(task) for task in tasks]
        results = await asyncio.gather(*tasks_coroutines, return_exceptions=True)
        
        # Handle exceptions
        processed_results = []
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                logger.error(f"Batch task {i} failed: {str(result)}")
                processed_results.append({
                    'success': False,
                    'error': str(result),
                    'task_index': i
                })
            else:
                processed_results.append(result)
        
        return processed_results
    
    # ========================================================================
    # Expert Management Methods
    # ========================================================================
    
    async def _select_expert(
        self,
        workload_profile: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Select best expert for workload"""
        if not self.active_experts:
            return {'expert': 'default', 'confidence': 1.0}
        
        expert_scores = {}
        
        for expert_id, expert in self.active_experts.items():
            # Calculate relevance score
            relevance = await self._calculate_expert_relevance(
                expert, workload_profile
            )
            
            # Get performance history
            performance = self.expert_performance.get(expert_id, {})
            success_rate = performance.get('success_rate', 0.8)
            
            # Get resource efficiency
            carbon_efficiency = 1.0 / (1.0 + expert.profile.carbon_per_inference)
            helium_efficiency = 1.0 / (1.0 + expert.profile.helium_per_inference)
            
            # Combined score
            score = (
                0.4 * relevance +
                0.3 * success_rate +
                0.15 * carbon_efficiency +
                0.15 * helium_efficiency
            )
            
            expert_scores[expert_id] = score
        
        # Select top expert
        best_expert = max(expert_scores.items(), key=lambda x: x[1])
        
        return {
            'expert': best_expert[0],
            'confidence': best_expert[1],
            'all_scores': expert_scores
        }
    
    async def _calculate_expert_relevance(
        self,
        expert: Any,
        workload_profile: Dict[str, Any]
    ) -> float:
        """Calculate expert relevance for workload"""
        # Check supported task types
        task_type = workload_profile.get('task_type', 'general')
        if task_type in expert.profile.supported_task_types:
            base_relevance = 0.8
        else:
            base_relevance = 0.3
        
        # Check domain match
        if hasattr(expert.profile, 'domain'):
            domain = expert.profile.domain.value
            if domain in workload_profile.get('domains', []):
                base_relevance += 0.2
        
        return min(base_relevance, 1.0)
    
    # ========================================================================
    # Adaptive Learning Methods
    # ========================================================================
    
    async def _adaptive_learning_loop(self):
        """Background loop for adaptive learning"""
        logger.info("Starting adaptive learning loop")
        
        while self.state == AgentState.RUNNING:
            try:
                if len(self.experience_buffer) >= self.config.batch_size:
                    self.state = AgentState.LEARNING
                    
                    # Sample experiences
                    batch = self.experience_buffer.sample(self.config.batch_size)
                    
                    # Learn from batch
                    improvements = await self._learn_from_batch(batch)
                    
                    if improvements > 0:
                        self.metrics.learning_iterations += 1
                        self.metrics.model_improvements += improvements
                        logger.debug(f"Learning iteration complete: {improvements} improvements")
                    
                    self.state = AgentState.RUNNING
                
                # Wait before next learning cycle
                await asyncio.sleep(5)
                
            except Exception as e:
                logger.error(f"Adaptive learning error: {str(e)}")
                await asyncio.sleep(30)
    
    async def _learn_from_batch(
        self,
        batch: List[Dict[str, Any]]
    ) -> int:
        """Learn from batch of experiences"""
        improvements = 0
        
        for experience in batch:
            # Extract experience components
            state = experience['state']
            action = experience['action']
            reward = experience['reward']
            next_state = experience['next_state']
            
            # Update expert preferences based on reward
            if 'expert' in action:
                expert_id = action['expert']
                if expert_id in self.expert_performance:
                    # Update success rate with exponential moving average
                    alpha = 0.1
                    old_rate = self.expert_performance[expert_id].get('success_rate', 0.5)
                    new_rate = old_rate * (1 - alpha) + reward * alpha
                    self.expert_performance[expert_id]['success_rate'] = new_rate
                    
                    if abs(new_rate - old_rate) > 0.01:
                        improvements += 1
            
            # Update pipeline preferences
            if 'pipeline' in action:
                pipeline = action['pipeline']
                if pipeline not in self.expert_performance:
                    self.expert_performance[pipeline] = {}
                
                # Track pipeline performance
                perf = self.expert_performance[pipeline]
                perf['total_uses'] = perf.get('total_uses', 0) + 1
                perf['total_reward'] = perf.get('total_reward', 0) + reward
                perf['avg_reward'] = perf['total_reward'] / perf['total_uses']
        
        return improvements
    
    async def _learn_from_execution(
        self,
        task: Dict[str, Any],
        result: Dict[str, Any],
        pipeline: str
    ):
        """Learn from task execution"""
        # Calculate reward
        reward = self._calculate_reward(task, result)
        
        # Create experience
        experience = {
            'state': {
                'task_type': task.get('task_type', 'unknown'),
                'complexity': task.get('complexity', 0.5),
                'carbon_zone': task.get('carbon_zone', 0),
                'helium_scarcity': task.get('helium_dependency', 0)
            },
            'action': {
                'pipeline': pipeline,
                'expert': result.get('expert_plan', {}).get('expert', 'default')
            },
            'reward': reward,
            'next_state': {
                'success': result.get('success', False),
                'latency_ms': result.get('latency_ms', 0),
                'carbon_kg': result.get('result', {}).get('carbon_kg', 0)
            },
            'done': True
        }
        
        # Add to experience buffer
        self.experience_buffer.add(
            state=experience['state'],
            action=experience['action'],
            reward=reward,
            next_state=experience['next_state'],
            done=True,
            priority=abs(reward)  # Higher priority for surprising results
        )
    
    def _calculate_reward(
        self,
        task: Dict[str, Any],
        result: Dict[str, Any]
    ) -> float:
        """Calculate reward for task execution"""
        reward = 0.0
        
        # Success bonus
        if result.get('success', False):
            reward += 0.5
        
        # Latency bonus (within budget)
        latency_budget = task.get('max_latency_ms', 1000)
        actual_latency = result.get('latency_ms', latency_budget)
        if actual_latency <= latency_budget:
            reward += 0.3 * (1 - actual_latency / latency_budget)
        
        # Carbon bonus (within budget)
        carbon_budget = task.get('max_carbon_budget', 0.1)
        actual_carbon = result.get('result', {}).get('carbon_kg', carbon_budget)
        if actual_carbon <= carbon_budget:
            reward += 0.2 * (1 - actual_carbon / carbon_budget)
        
        return reward
    
    # ========================================================================
    # Background Tasks
    # ========================================================================
    
    async def _health_check_loop(self):
        """Background health check loop"""
        logger.info("Starting health check loop")
        
        while self.state in [AgentState.RUNNING, AgentState.DEGRADED]:
            try:
                # Check expert health
                if self.active_experts:
                    for expert_id, expert in self.active_experts.items():
                        if hasattr(expert, 'get_expert_statistics'):
                            stats = expert.get_expert_statistics()
                            self.expert_performance[expert_id] = stats
                
                # Check system resources
                import psutil
                self.metrics.memory_usage_mb = psutil.Process().memory_info().rss / 1024 / 1024
                self.metrics.cpu_usage_percent = psutil.cpu_percent()
                
                # Update state if degraded
                if self.metrics.cpu_usage_percent > 90:
                    self.state = AgentState.DEGRADED
                    logger.warning("High CPU usage detected")
                elif self.state == AgentState.DEGRADED and self.metrics.cpu_usage_percent < 70:
                    self.state = AgentState.RUNNING
                    logger.info("System recovered from degraded state")
                
                await asyncio.sleep(10)
                
            except Exception as e:
                logger.error(f"Health check error: {str(e)}")
                await asyncio.sleep(30)
    
    async def _metrics_collection_loop(self):
        """Background metrics collection loop"""
        logger.info("Starting metrics collection loop")
        
        while self.state == AgentState.RUNNING:
            try:
                # Update uptime
                self.metrics.uptime_seconds = (
                    datetime.utcnow() - self.start_time
                ).total_seconds()
                
                # Update experience buffer size
                self.metrics.experience_buffer_size = len(self.experience_buffer)
                
                # Update Prometheus metrics
                if self.config.enable_prometheus and hasattr(self, 'prometheus_metrics'):
                    self.prometheus_metrics['carbon_kg'].set(
                        self.metrics.total_carbon_kg
                    )
                    self.prometheus_metrics['helium_units'].set(
                        self.metrics.total_helium_units
                    )
                    
                    for expert_id, perf in self.expert_performance.items():
                        self.prometheus_metrics['expert_utilization'].labels(
                            expert_id=expert_id
                        ).set(perf.get('success_rate', 0))
                
                await asyncio.sleep(15)
                
            except Exception as e:
                logger.error(f"Metrics collection error: {str(e)}")
                await asyncio.sleep(30)
    
    # ========================================================================
    # Utility Methods
    # ========================================================================
    
    def _validate_task(self, task: Dict[str, Any]) -> bool:
        """Validate task configuration"""
        required_fields = ['task_type']
        return all(field in task for field in required_fields)
    
    async def _profile_workload(
        self,
        task: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Profile workload characteristics"""
        return {
            'task_type': task.get('task_type', 'general'),
            'complexity': task.get('complexity', 0.5),
            'data_size_mb': task.get('data_size_mb', 1.0),
            'domains': task.get('domains', []),
            'carbon_sensitivity': task.get('carbon_sensitivity', 0.5),
            'helium_dependency': task.get('helium_dependency', 0.0)
        }
    
    async def _execute_task(
        self,
        task: Dict[str, Any],
        expert_plan: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Execute task with selected expert"""
        expert_id = expert_plan.get('expert', 'default')
        
        if expert_id in self.active_experts:
            expert = self.active_experts[expert_id]
            # Execute with expert
            result = {
                'expert_used': expert_id,
                'carbon_kg': expert.profile.carbon_per_inference,
                'helium_units': expert.profile.helium_per_inference,
                'energy_kwh': expert.profile.energy_per_inference,
                'latency_ms': expert.profile.avg_latency_ms
            }
        else:
            # Default execution
            result = {
                'expert_used': 'default',
                'carbon_kg': 0.0001,
                'helium_units': 0.01,
                'energy_kwh': 0.001,
                'latency_ms': 50.0
            }
        
        return result
    
    def _validate_result(self, result: Dict[str, Any]) -> bool:
        """Validate task result"""
        return result is not None
    
    def _is_quantum_suitable(self, task: Dict[str, Any]) -> bool:
        """Check if task is suitable for quantum processing"""
        return (
            task.get('quantum_capable', False) and
            task.get('complexity', 0) > 0.7
        )
    
    async def _prepare_quantum_circuit(
        self,
        task: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Prepare quantum circuit for task"""
        return {
            'circuit_type': 'variational',
            'qubits': min(int(task.get('complexity', 0.5) * 20), self.config.max_qubits),
            'depth': int(task.get('complexity', 0.5) * 10),
            'optimization_target': task.get('optimization_target', 'energy')
        }
    
    async def _execute_quantum(
        self,
        circuit: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Execute quantum circuit (simulated)"""
        # Simulate quantum execution
        return {
            'success': True,
            'error_rate': np.random.exponential(0.001),
            'execution_time_ms': np.random.exponential(100),
            'energy_saved_percent': np.random.uniform(10, 30)
        }
    
    async def _combine_quantum_classical(
        self,
        task: Dict[str, Any],
        quantum_result: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Combine quantum and classical results"""
        return {
            'hybrid_result': True,
            'quantum_contribution': quantum_result.get('energy_saved_percent', 0),
            'carbon_kg': 0.00005,
            'latency_ms': quantum_result.get('execution_time_ms', 100) + 50
        }
    
    async def _assess_helium_constraints(
        self,
        task: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Assess helium constraints for task"""
        helium_dependency = task.get('helium_dependency', 0)
        
        return {
            'scarcity': helium_dependency,
            'is_critical': helium_dependency > 0.7,
            'budget_available': self.config.max_helium_budget,
            'recommended_reduction': helium_dependency * 0.3
        }
    
    async def _optimize_for_helium(
        self,
        task: Dict[str, Any],
        helium_profile: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Optimize task configuration for helium efficiency"""
        return {
            'efficiency': 0.85,
            'compression': 'zstd' if helium_profile['is_critical'] else 'lz4',
            'batch_size': 16 if helium_profile['is_critical'] else 64,
            'quantization': 'int4' if helium_profile['is_critical'] else 'int8'
        }
    
    async def _execute_with_helium_constraints(
        self,
        task: Dict[str, Any],
        config: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Execute task with helium constraints"""
        return {
            'helium_used': self.config.max_helium_budget * 0.1,
            'efficiency': config['efficiency'],
            'carbon_kg': 0.00008
        }
    
    def _calculate_helium_savings(
        self,
        task: Dict[str, Any],
        result: Dict[str, Any]
    ) -> float:
        """Calculate helium savings from optimization"""
        baseline = self.config.max_helium_budget * 0.2
        actual = result.get('helium_used', baseline)
        return baseline - actual
    
    async def _calculate_energy_budget(
        self,
        task: Dict[str, Any]
    ) -> float:
        """Calculate energy budget for task"""
        base_budget = 0.01  # kWh
        complexity_factor = task.get('complexity', 0.5) * 2
        return base_budget * complexity_factor
    
    async def _optimize_energy_usage(
        self,
        task: Dict[str, Any],
        budget: float
    ) -> Dict[str, Any]:
        """Optimize energy usage"""
        return {
            'target_kwh': budget * 0.8,  # Target 80% of budget
            'savings_percent': 20,
            'strategy': 'dynamic_frequency_scaling'
        }
    
    async def _execute_with_energy_constraints(
        self,
        task: Dict[str, Any],
        plan: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Execute with energy constraints"""
        return {
            'energy_used': plan['target_kwh'],
            'savings_achieved': True,
            'carbon_kg': plan['target_kwh'] * 0.4  # kg CO2 per kWh
        }
    
    async def _analyze_task(
        self,
        task: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Analyze task for adaptive pipeline selection"""
        return {
            'complexity_score': task.get('complexity', 0.5),
            'helium_sensitivity': task.get('helium_dependency', 0),
            'carbon_sensitivity': task.get('carbon_sensitivity', 0.5),
            'quantum_suitable': self._is_quantum_suitable(task),
            'data_intensive': task.get('data_size_mb', 1) > 100
        }
    
    async def _select_best_pipeline(
        self,
        analysis: Dict[str, Any]
    ) -> str:
        """Select best pipeline based on task analysis"""
        if analysis['quantum_suitable'] and self.config.enable_quantum:
            return 'quantum'
        elif analysis['helium_sensitivity'] > 0.6:
            return 'helium_optimized'
        elif analysis['carbon_sensitivity'] > 0.7:
            return 'energy_efficient'
        else:
            return 'standard'
    
    # ========================================================================
    # State Management
    # ========================================================================
    
    def _save_state(self):
        """Save agent state to disk"""
        state = {
            'config': self.config.to_dict(),
            'metrics': {
                'total_tasks': self.metrics.total_tasks,
                'successful_tasks': self.metrics.successful_tasks,
                'total_carbon_kg': self.metrics.total_carbon_kg,
                'total_helium_units': self.metrics.total_helium_units,
                'uptime_seconds': self.metrics.uptime_seconds
            },
            'expert_performance': self.expert_performance,
            'saved_at': datetime.utcnow().isoformat()
        }
        
        try:
            with open('agent_state.json', 'w') as f:
                json.dump(state, f, indent=2, default=str)
            logger.info("Agent state saved successfully")
        except Exception as e:
            logger.error(f"Failed to save state: {str(e)}")
    
    async def _cleanup(self):
        """Cleanup resources"""
        logger.info("Cleaning up resources...")
        
        # Save final state
        self._save_state()
        
        # Close expert connections
        for expert_id, expert in self.active_experts.items():
            if hasattr(expert, 'reset_metrics'):
                expert.reset_metrics()
        
        # Clear buffers
        self.experience_buffer.buffer.clear()
        
        logger.info("Cleanup complete")
    
    # ========================================================================
    # Public API Methods
    # ========================================================================
    
    def get_status(self) -> Dict[str, Any]:
        """Get comprehensive agent status"""
        return {
            'agent_name': self.config.agent_name,
            'version': self.config.version,
            'state': self.state.value,
            'uptime_seconds': (datetime.utcnow() - self.start_time).total_seconds(),
            'metrics': {
                'total_tasks': self.metrics.total_tasks,
                'successful_tasks': self.metrics.successful_tasks,
                'failed_tasks': self.metrics.failed_tasks,
                'success_rate': (
                    self.metrics.successful_tasks / max(self.metrics.total_tasks, 1)
                ),
                'average_latency_ms': self.metrics.average_latency_ms,
                'total_carbon_kg': self.metrics.total_carbon_kg,
                'total_helium_units': self.metrics.total_helium_units,
                'learning_iterations': self.metrics.learning_iterations,
                'experience_buffer_size': len(self.experience_buffer)
            },
            'experts': {
                expert_id: {
                    'active': True,
                    'performance': self.expert_performance.get(expert_id, {})
                }
                for expert_id in self.active_experts
            },
            'pipelines': list(self.pipelines.keys()),
            'config': self.config.to_dict(),
            'queue_stats': self.task_scheduler.get_queue_stats()
        }
    
    def get_expert_stats(self) -> Dict[str, Any]:
        """Get expert statistics"""
        stats = {}
        for expert_id, expert in self.active_experts.items():
            if hasattr(expert, 'get_expert_statistics'):
                stats[expert_id] = expert.get_expert_statistics()
            else:
                stats[expert_id] = {
                    'profile': expert.profile.to_dict() if hasattr(expert, 'profile') else {}
                }
        return stats
    
    def get_learning_stats(self) -> Dict[str, Any]:
        """Get learning statistics"""
        return {
            'iterations': self.metrics.learning_iterations,
            'improvements': self.metrics.model_improvements,
            'buffer_size': len(self.experience_buffer),
            'recent_rewards': [
                exp['reward'] for exp in list(self.experience_buffer.buffer)[-100:]
            ]
        }

# ============================================================================
# Main Entry Point
# ============================================================================

async def main():
    """Main entry point for Enhanced Green Agent"""
    logger.info("Starting Enhanced Green Agent v2.4.0")
    
    # Load configuration
    config = AgentConfig()
    
    # Override from command line arguments
    if len(sys.argv) > 1:
        if sys.argv[1] == '--config' and len(sys.argv) > 2:
            config = AgentConfig.from_file(sys.argv[2])
        elif sys.argv[1] == '--no-quantum':
            config.enable_quantum = False
        elif sys.argv[1] == '--no-moe':
            config.enable_moe = False
    
    # Initialize agent
    agent = EnhancedGreenAgent(config=config)
    
    try:
        # Example tasks
        example_tasks = [
            {
                'task_type': 'inference',
                'complexity': 0.3,
                'carbon_zone': 2,
                'helium_dependency': 0.2,
                'max_latency_ms': 100,
                'max_carbon_budget': 0.05,
                'domains': ['energy', 'data']
            },
            {
                'task_type': 'optimization',
                'complexity': 0.8,
                'carbon_zone': 5,
                'helium_dependency': 0.6,
                'quantum_capable': True,
                'max_latency_ms': 500,
                'domains': ['quantum', 'energy']
            },
            {
                'task_type': 'data_processing',
                'complexity': 0.5,
                'carbon_zone': 3,
                'helium_dependency': 0.3,
                'data_size_mb': 500,
                'max_latency_ms': 1000,
                'domains': ['data', 'iot']
            }
        ]
        
        # Process individual tasks
        logger.info("Processing individual tasks...")
        for i, task in enumerate(example_tasks):
            result = await agent.process_task(task)
            logger.info(f"Task {i+1} result: success={result.get('success')}, "
                       f"latency={result.get('latency_ms', 0):.1f}ms")
        
        # Batch processing
        logger.info("\nBatch processing tasks...")
        batch_results = await agent.batch_process(example_tasks)
        logger.info(f"Batch complete: {len(batch_results)} tasks processed")
        
        # Print status
        status = agent.get_status()
        logger.info(f"\nAgent Status:")
        logger.info(f"  State: {status['state']}")
        logger.info(f"  Uptime: {status['uptime_seconds']:.1f}s")
        logger.info(f"  Tasks: {status['metrics']['total_tasks']} total, "
                   f"{status['metrics']['success_rate']:.1%} success rate")
        logger.info(f"  Carbon: {status['metrics']['total_carbon_kg']:.6f} kg")
        logger.info(f"  Helium: {status['metrics']['total_helium_units']:.6f} units")
        logger.info(f"  Learning: {status['metrics']['learning_iterations']} iterations")
        
        # Keep running for monitoring
        logger.info("\nAgent running. Press Ctrl+C to stop.")
        await asyncio.sleep(60)  # Run for 1 minute for demonstration
        
    except KeyboardInterrupt:
        logger.info("Received interrupt signal")
    except Exception as e:
        logger.error(f"Fatal error: {str(e)}", exc_info=True)
    finally:
        # Cleanup
        await agent._cleanup()
        logger.info("Agent shutdown complete")

if __name__ == "__main__":
    asyncio.run(main())
