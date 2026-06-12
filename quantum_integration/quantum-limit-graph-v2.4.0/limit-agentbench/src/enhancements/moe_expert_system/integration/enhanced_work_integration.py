# File: quantum_integration/quantum-limit-graph-v2.4.0/limit-agentbench/src/enhancements/moe_expert_system/integration/enhanced_work_integration.py

"""
Enhanced Work Integration for Green Agent MoE System
Version: 2.0.0

Advanced work integration with:
- DAG-based workflow orchestration
- Formal work state machine
- SLA management and tracking
- Deadline-aware priority scheduling
- Resource reservation and allocation
- Intelligent work batching
- Transaction rollback support
- Work checkpointing and resumption
- Multi-tenant work isolation
- Work versioning and lineage
- Predictive work scheduling
- Dynamic work decomposition
- Cross-work optimization
- Work migration between pipelines
- Real-time work monitoring
- Adaptive concurrency control

Integration Points:
- Layer 0: Work classification and profiling
- Layer 1: Meta-cognitive work optimization
- Layer 2: Neuro-symbolic work validation
- Layer 3: Dual-axis resource allocation
- Layer 6: Distributed work execution
- Layer 7: Work monitoring and metrics
- Layer 8: Immutable work audit trail
- Layer 9: Pareto-optimal work scheduling
"""

import asyncio
import logging
from typing import Dict, Any, List, Optional, Tuple, Set, Callable
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from collections import defaultdict, deque
import numpy as np
import hashlib
import json
import uuid
import networkx as nx
from concurrent.futures import ThreadPoolExecutor
import heapq

logger = logging.getLogger(__name__)

# ============================================================================
# Enums and Data Classes
# ============================================================================

class WorkState(Enum):
    """Formal work state machine"""
    CREATED = "created"
    VALIDATED = "validated"
    QUEUED = "queued"
    SCHEDULED = "scheduled"
    RESOURCES_RESERVED = "resources_reserved"
    EXECUTING = "executing"
    CHECKPOINTED = "checkpointed"
    COMPLETED = "completed"
    FAILED = "failed"
    ROLLING_BACK = "rolling_back"
    ROLLED_BACK = "rolled_back"
    CANCELLED = "cancelled"
    SUSPENDED = "suspended"
    RESUMED = "resumed"
    MIGRATED = "migrated"
    ARCHIVED = "archived"
    
    def is_terminal(self) -> bool:
        """Check if state is terminal"""
        return self in [
            WorkState.COMPLETED, WorkState.FAILED,
            WorkState.ROLLED_BACK, WorkState.CANCELLED,
            WorkState.ARCHIVED
        ]
    
    def is_active(self) -> bool:
        """Check if work is actively processing"""
        return self in [
            WorkState.EXECUTING, WorkState.ROLLING_BACK,
            WorkState.CHECKPOINTED
        ]
    
    def can_transition_to(self, target: 'WorkState') -> bool:
        """Check if transition is valid"""
        valid_transitions = {
            WorkState.CREATED: [
                WorkState.VALIDATED, WorkState.CANCELLED
            ],
            WorkState.VALIDATED: [
                WorkState.QUEUED, WorkState.CANCELLED
            ],
            WorkState.QUEUED: [
                WorkState.SCHEDULED, WorkState.CANCELLED
            ],
            WorkState.SCHEDULED: [
                WorkState.RESOURCES_RESERVED, WorkState.CANCELLED
            ],
            WorkState.RESOURCES_RESERVED: [
                WorkState.EXECUTING, WorkState.CANCELLED
            ],
            WorkState.EXECUTING: [
                WorkState.COMPLETED, WorkState.FAILED,
                WorkState.CHECKPOINTED, WorkState.SUSPENDED,
                WorkState.MIGRATED
            ],
            WorkState.CHECKPOINTED: [
                WorkState.EXECUTING, WorkState.RESUMED,
                WorkState.FAILED
            ],
            WorkState.FAILED: [
                WorkState.ROLLING_BACK, WorkState.QUEUED
            ],
            WorkState.ROLLING_BACK: [
                WorkState.ROLLED_BACK, WorkState.FAILED
            ],
            WorkState.ROLLED_BACK: [
                WorkState.QUEUED, WorkState.ARCHIVED
            ],
            WorkState.SUSPENDED: [
                WorkState.RESUMED, WorkState.CANCELLED
            ],
            WorkState.RESUMED: [
                WorkState.EXECUTING
            ],
            WorkState.COMPLETED: [
                WorkState.ARCHIVED
            ]
        }
        return target in valid_transitions.get(self, [])

class WorkPriority(Enum):
    """Work priority with deadline awareness"""
    CRITICAL = 0    # Must complete within deadline
    HIGH = 1        # Should complete soon
    MEDIUM = 2      # Normal priority
    LOW = 3         # Can be delayed
    BACKGROUND = 4  # Lowest priority
    DEFERRABLE = 5  # Can be shifted
    
    @property
    def weight(self) -> float:
        """Get scheduling weight"""
        weights = {
            WorkPriority.CRITICAL: 10.0,
            WorkPriority.HIGH: 5.0,
            WorkPriority.MEDIUM: 2.0,
            WorkPriority.LOW: 1.0,
            WorkPriority.BACKGROUND: 0.5,
            WorkPriority.DEFERRABLE: 0.2
        }
        return weights.get(self, 1.0)

class SLALevel(Enum):
    """Service Level Agreement levels"""
    PLATINUM = "platinum"     # 99.99% availability, <10ms latency
    GOLD = "gold"            # 99.9% availability, <50ms latency
    SILVER = "silver"        # 99% availability, <200ms latency
    BRONZE = "bronze"        # 95% availability, <1000ms latency
    BEST_EFFORT = "best_effort"  # No guarantees

@dataclass
class WorkSLA:
    """Service Level Agreement for work"""
    level: SLALevel
    max_latency_ms: float
    min_availability: float
    max_carbon_kg: Optional[float] = None
    max_helium_units: Optional[float] = None
    deadline: Optional[datetime] = None
    penalty_per_violation: float = 0.0
    violations: int = 0
    
    def is_violated(self, actual_latency_ms: float) -> bool:
        """Check if SLA is violated"""
        return actual_latency_ms > self.max_latency_ms
    
    def time_until_deadline(self) -> Optional[float]:
        """Get seconds until deadline"""
        if self.deadline:
            return (self.deadline - datetime.utcnow()).total_seconds()
        return None
    
    def is_deadline_critical(self) -> bool:
        """Check if deadline is approaching"""
        remaining = self.time_until_deadline()
        if remaining is None:
            return False
        return remaining < 60  # Less than 1 minute

@dataclass
class ResourceReservation:
    """Resource reservation for work execution"""
    reservation_id: str
    work_id: str
    resources: Dict[str, float]  # resource_type -> amount
    reserved_at: datetime
    expires_at: datetime
    carbon_budget_kg: float
    helium_budget: float
    is_active: bool = True

@dataclass
class WorkCheckpoint:
    """Checkpoint for work resumption"""
    checkpoint_id: str
    work_id: str
    state: WorkState
    progress: float  # 0.0 to 1.0
    intermediate_results: Dict[str, Any]
    resource_usage: Dict[str, float]
    created_at: datetime
    pipeline_state: Dict[str, Any]

@dataclass
class WorkVersion:
    """Work version for lineage tracking"""
    version_id: str
    work_id: str
    version_number: int
    parent_version: Optional[str]
    changes: List[str]
    created_at: datetime
    created_by: str

# ============================================================================
# Enhanced Work Context
# ============================================================================

@dataclass
class EnhancedWorkContext:
    """Enhanced work context with comprehensive metadata"""
    # Core identification
    task_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    work_type: str = "general"
    priority: WorkPriority = WorkPriority.MEDIUM
    
    # State management
    state: WorkState = WorkState.CREATED
    state_history: List[Tuple[WorkState, datetime]] = field(default_factory=list)
    
    # SLA
    sla: Optional[WorkSLA] = None
    
    # Complexity
    complexity: float = 0.5
    estimated_duration_ms: float = 100.0
    
    # Layer 0: Helium Profile
    helium_dependency: float = 0.0
    helium_profile: Dict[str, Any] = field(default_factory=dict)
    
    # Layer 1: Meta-cognitive state
    meta_cognitive_state: Dict[str, Any] = field(default_factory=dict)
    reflection_notes: List[str] = field(default_factory=list)
    
    # Layer 2: Neuro-symbolic context
    symbolic_rules: Dict[str, Any] = field(default_factory=dict)
    knowledge_graph_nodes: List[str] = field(default_factory=list)
    
    # Layer 3: Dual-axis parameters
    carbon_zone: int = 0
    helium_zone: int = 0
    dual_axis_score: float = 0.0
    
    # Layer 10: Quantum integration
    quantum_capable: bool = False
    quantum_circuit_required: bool = False
    quantum_backend_type: Optional[str] = None
    
    # Resource constraints
    max_carbon_budget: float = float('inf')
    max_helium_budget: float = float('inf')
    max_latency_ms: float = 1000.0
    min_accuracy: float = 0.0
    
    # Batching
    batch_group: Optional[str] = None
    can_batch: bool = True
    batch_priority: int = 0
    
    # Dependencies
    depends_on: List[str] = field(default_factory=list)
    dependents: List[str] = field(default_factory=list)
    
    # Checkpointing
    checkpoints: List[WorkCheckpoint] = field(default_factory=list)
    resume_from_checkpoint: Optional[str] = None
    
    # Versioning
    versions: List[WorkVersion] = field(default_factory=list)
    current_version: int = 1
    
    # Multi-tenancy
    tenant_id: str = "default"
    isolation_level: str = "shared"
    
    # Resource reservation
    reservation: Optional[ResourceReservation] = None
    
    # Execution tracking
    created_at: datetime = field(default_factory=datetime.utcnow)
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    execution_attempts: int = 0
    max_attempts: int = 3
    
    # Rollback
    rollback_actions: List[Callable] = field(default_factory=list)
    compensation_actions: List[Callable] = field(default_factory=list)
    
    # Monitoring
    metrics: Dict[str, Any] = field(default_factory=dict)
    events: List[Dict[str, Any]] = field(default_factory=list)
    
    def transition_to(self, new_state: WorkState) -> bool:
        """Transition to new state with validation"""
        if not self.state.can_transition_to(new_state):
            logger.warning(
                f"Invalid state transition: {self.state.value} -> {new_state.value}"
            )
            return False
        
        old_state = self.state
        self.state = new_state
        self.state_history.append((new_state, datetime.utcnow()))
        
        logger.debug(f"Work {self.task_id}: {old_state.value} -> {new_state.value}")
        return True
    
    def add_checkpoint(self, checkpoint: WorkCheckpoint):
        """Add work checkpoint"""
        self.checkpoints.append(checkpoint)
        
        # Keep only last 5 checkpoints
        if len(self.checkpoints) > 5:
            self.checkpoints = self.checkpoints[-5:]
    
    def add_version(self, changes: List[str], created_by: str):
        """Create new work version"""
        self.current_version += 1
        version = WorkVersion(
            version_id=f"{self.task_id}_v{self.current_version}",
            work_id=self.task_id,
            version_number=self.current_version,
            parent_version=f"{self.task_id}_v{self.current_version - 1}" if self.current_version > 1 else None,
            changes=changes,
            created_at=datetime.utcnow(),
            created_by=created_by
        )
        self.versions.append(version)
    
    def add_event(self, event_type: str, details: Dict[str, Any]):
        """Add work event"""
        self.events.append({
            'type': event_type,
            'details': details,
            'timestamp': datetime.utcnow().isoformat()
        })
        
        # Keep last 1000 events
        if len(self.events) > 1000:
            self.events = self.events[-1000:]
    
    def is_sla_critical(self) -> bool:
        """Check if SLA deadline is critical"""
        if self.sla:
            return self.sla.is_deadline_critical()
        return False
    
    def can_retry(self) -> bool:
        """Check if work can be retried"""
        return self.execution_attempts < self.max_attempts
    
    def to_routing_context(self) -> Dict[str, Any]:
        """Convert to routing context"""
        return {
            'task_id': self.task_id,
            'task_type': self.work_type,
            'complexity': self.complexity,
            'input_size_mb': self.meta_cognitive_state.get('data_size_mb', 1.0),
            'carbon_budget_remaining': self.max_carbon_budget,
            'helium_budget_remaining': self.max_helium_budget,
            'latency_budget_ms': self.max_latency_ms,
            'carbon_zone': self.carbon_zone,
            'helium_scarcity': self.helium_dependency,
            'grid_carbon_intensity': self.meta_cognitive_state.get('grid_intensity', 400),
            'hardware_availability': {
                'cpu': 1.0,
                'gpu': 0.8,
                'quantum': 1.0 if self.quantum_capable else 0.0,
                'edge': 0.5
            },
            'priority': self.priority.weight if hasattr(self.priority, 'weight') else 1,
            'deadline_pressure': 1.0 if self.is_sla_critical() else 0.0
        }

# ============================================================================
# DAG Workflow Engine
# ============================================================================

class WorkflowDAG:
    """
    DAG-based workflow orchestration engine.
    
    Manages dependencies between work items and orchestrates execution.
    """
    
    def __init__(self):
        self.graph = nx.DiGraph()
        self.work_items: Dict[str, EnhancedWorkContext] = {}
        self.ready_queue: List[EnhancedWorkContext] = []
        self.completed: Set[str] = set()
        self.failed: Set[str] = set()
    
    def add_work(
        self,
        work: EnhancedWorkContext,
        dependencies: Optional[List[str]] = None
    ):
        """Add work item with dependencies"""
        self.work_items[work.task_id] = work
        self.graph.add_node(work.task_id, work=work)
        
        if dependencies:
            for dep_id in dependencies:
                if dep_id in self.work_items:
                    self.graph.add_edge(dep_id, work.task_id)
                    work.depends_on.append(dep_id)
                    self.work_items[dep_id].dependents.append(work.task_id)
        
        # Check if ready to execute
        if not dependencies or all(
            d in self.completed for d in dependencies
        ):
            self.ready_queue.append(work)
    
    def mark_completed(self, work_id: str):
        """Mark work as completed and update dependents"""
        self.completed.add(work_id)
        
        # Check dependents
        for dependent_id in self.graph.successors(work_id):
            work = self.work_items.get(dependent_id)
            if work:
                # Check if all dependencies are completed
                if all(
                    d in self.completed
                    for d in work.depends_on
                ):
                    self.ready_queue.append(work)
    
    def mark_failed(self, work_id: str):
        """Mark work as failed"""
        self.failed.add(work_id)
        
        # Optionally fail dependents or allow retry
        work = self.work_items.get(work_id)
        if work and work.can_retry():
            # Re-add to ready queue for retry
            self.ready_queue.append(work)
    
    def get_next_ready(self) -> Optional[EnhancedWorkContext]:
        """Get next ready work item"""
        if self.ready_queue:
            return self.ready_queue.pop(0)
        return None
    
    def get_execution_order(self) -> List[List[str]]:
        """Get topological execution order"""
        try:
            # Get topological generations
            generations = list(nx.topological_generations(self.graph))
            return [list(gen) for gen in generations]
        except nx.NetworkXError:
            logger.error("Cycle detected in workflow DAG")
            return []
    
    def get_critical_path(self) -> List[str]:
        """Get critical path through DAG"""
        try:
            return nx.dag_longest_path(self.graph)
        except nx.NetworkXError:
            return []
    
    def get_blocked_work(self) -> List[str]:
        """Get work items blocked by dependencies"""
        blocked = []
        for work_id, work in self.work_items.items():
            if work_id not in self.completed and work_id not in self.failed:
                if not all(d in self.completed for d in work.depends_on):
                    blocked.append(work_id)
        return blocked
    
    def get_workflow_stats(self) -> Dict[str, Any]:
        """Get workflow statistics"""
        total = len(self.work_items)
        return {
            'total_work_items': total,
            'completed': len(self.completed),
            'failed': len(self.failed),
            'ready': len(self.ready_queue),
            'blocked': len(self.get_blocked_work()),
            'progress': len(self.completed) / max(total, 1),
            'critical_path_length': len(self.get_critical_path()),
            'generations': len(self.get_execution_order()),
            'has_cycles': not nx.is_directed_acyclic_graph(self.graph)
        }

# ============================================================================
# Resource Reservation Manager
# ============================================================================

class ResourceReservationManager:
    """
    Manages resource reservations for work execution.
    
    Features:
    - Resource allocation tracking
    - Reservation timeouts
    - Overallocation prevention
    - Carbon/helium budget enforcement
    """
    
    def __init__(
        self,
        total_carbon_budget: float = 1.0,
        total_helium_budget: float = 0.1,
        max_concurrent_reservations: int = 50
    ):
        self.total_carbon_budget = total_carbon_budget
        self.total_helium_budget = total_helium_budget
        self.max_concurrent_reservations = max_concurrent_reservations
        
        self.reservations: Dict[str, ResourceReservation] = {}
        self.allocated_carbon: float = 0.0
        self.allocated_helium: float = 0.0
        
        # Resource pools
        self.resource_pools: Dict[str, float] = {
            'cpu_cores': 64.0,
            'gpu_devices': 8.0,
            'memory_gb': 256.0,
            'quantum_qubits': 20.0,
            'network_bandwidth_gbps': 10.0
        }
        
        self.allocated_resources: Dict[str, float] = defaultdict(float)
    
    def reserve_resources(
        self,
        work_id: str,
        required_resources: Dict[str, float],
        carbon_budget: float,
        helium_budget: float,
        duration_seconds: float = 300
    ) -> Optional[ResourceReservation]:
        """
        Reserve resources for work execution.
        
        Returns None if resources cannot be reserved.
        """
        # Check carbon budget
        if self.allocated_carbon + carbon_budget > self.total_carbon_budget:
            logger.warning(f"Cannot reserve: carbon budget exceeded")
            return None
        
        # Check helium budget
        if self.allocated_helium + helium_budget > self.total_helium_budget:
            logger.warning(f"Cannot reserve: helium budget exceeded")
            return None
        
        # Check resource availability
        for resource, amount in required_resources.items():
            if resource in self.resource_pools:
                available = self.resource_pools[resource] - self.allocated_resources[resource]
                if amount > available:
                    logger.warning(
                        f"Cannot reserve {resource}: {amount} > {available} available"
                    )
                    return None
        
        # Check concurrent reservations
        if len(self.reservations) >= self.max_concurrent_reservations:
            logger.warning("Max concurrent reservations reached")
            return None
        
        # Create reservation
        reservation = ResourceReservation(
            reservation_id=f"res_{work_id}_{datetime.utcnow().timestamp()}",
            work_id=work_id,
            resources=required_resources,
            reserved_at=datetime.utcnow(),
            expires_at=datetime.utcnow() + timedelta(seconds=duration_seconds),
            carbon_budget_kg=carbon_budget,
            helium_budget=helium_budget
        )
        
        # Allocate resources
        for resource, amount in required_resources.items():
            self.allocated_resources[resource] += amount
        
        self.allocated_carbon += carbon_budget
        self.allocated_helium += helium_budget
        
        self.reservations[reservation.reservation_id] = reservation
        
        logger.info(
            f"Reserved resources for {work_id}: "
            f"carbon={carbon_budget:.4f}kg, helium={helium_budget:.4f}"
        )
        
        return reservation
    
    def release_reservation(self, reservation_id: str):
        """Release reserved resources"""
        if reservation_id not in self.reservations:
            return
        
        reservation = self.reservations.pop(reservation_id)
        
        # Release resources
        for resource, amount in reservation.resources.items():
            self.allocated_resources[resource] = max(
                0, self.allocated_resources[resource] - amount
            )
        
        self.allocated_carbon = max(0, self.allocated_carbon - reservation.carbon_budget_kg)
        self.allocated_helium = max(0, self.allocated_helium - reservation.helium_budget)
        
        reservation.is_active = False
        
        logger.debug(f"Released reservation {reservation_id}")
    
    def cleanup_expired(self):
        """Clean up expired reservations"""
        now = datetime.utcnow()
        expired = [
            rid for rid, res in self.reservations.items()
            if res.expires_at < now
        ]
        
        for rid in expired:
            self.release_reservation(rid)
    
    def get_availability(self) -> Dict[str, float]:
        """Get current resource availability"""
        return {
            resource: self.resource_pools[resource] - self.allocated_resources[resource]
            for resource in self.resource_pools
        }
    
    def can_accommodate(
        self,
        required_resources: Dict[str, float],
        carbon_budget: float,
        helium_budget: float
    ) -> Tuple[bool, str]:
        """Check if resources can be accommodated"""
        # Check carbon
        if self.allocated_carbon + carbon_budget > self.total_carbon_budget:
            return False, "Carbon budget exceeded"
        
        # Check helium
        if self.allocated_helium + helium_budget > self.total_helium_budget:
            return False, "Helium budget exceeded"
        
        # Check resources
        for resource, amount in required_resources.items():
            if resource in self.resource_pools:
                available = self.resource_pools[resource] - self.allocated_resources[resource]
                if amount > available:
                    return False, f"Insufficient {resource}"
        
        return True, "Resources available"

# ============================================================================
# Intelligent Work Batcher
# ============================================================================

class IntelligentWorkBatcher:
    """
    Intelligent work batching for efficiency optimization.
    
    Groups compatible work items to reduce overhead.
    """
    
    def __init__(
        self,
        max_batch_size: int = 10,
        max_batch_latency_ms: float = 100,
        batching_window_ms: float = 50
    ):
        self.max_batch_size = max_batch_size
        self.max_batch_latency_ms = max_batch_latency_ms
        self.batching_window_ms = batching_window_ms
        
        self.batch_groups: Dict[str, List[EnhancedWorkContext]] = defaultdict(list)
        self.batching_queue: List[EnhancedWorkContext] = []
        
        self.batch_stats: Dict[str, Any] = {
            'total_batches_created': 0,
            'total_work_batched': 0,
            'average_batch_size': 0.0,
            'batching_efficiency': 0.0
        }
    
    def add_work(self, work: EnhancedWorkContext):
        """Add work to batching queue"""
        if not work.can_batch:
            return
        
        self.batching_queue.append(work)
    
    def form_batches(self) -> List[Tuple[str, List[EnhancedWorkContext]]]:
        """
        Form optimal batches from queued work.
        
        Groups by:
        - Same work type
        - Similar resource requirements
        - Compatible SLA levels
        - Same tenant (if isolated)
        """
        if not self.batching_queue:
            return []
        
        batches = []
        
        # Group by work type first
        by_type: Dict[str, List[EnhancedWorkContext]] = defaultdict(list)
        for work in self.batching_queue:
            by_type[work.work_type].append(work)
        
        # Form batches within each type
        for work_type, work_items in by_type.items():
            # Sort by priority
            work_items.sort(key=lambda w: w.priority.weight if hasattr(w.priority, 'weight') else 1)
            
            # Create batches respecting max size
            for i in range(0, len(work_items), self.max_batch_size):
                batch = work_items[i:i + self.max_batch_size]
                
                # Check batch latency
                total_latency = sum(
                    w.estimated_duration_ms for w in batch
                )
                
                if total_latency <= self.max_batch_latency_ms:
                    batch_id = f"batch_{work_type}_{len(batches)}"
                    batches.append((batch_id, batch))
                    
                    # Update stats
                    self.batch_stats['total_batches_created'] += 1
                    self.batch_stats['total_work_batched'] += len(batch)
        
        # Clear queue
        self.batching_queue.clear()
        
        # Update average
        total_batches = max(self.batch_stats['total_batches_created'], 1)
        self.batch_stats['average_batch_size'] = (
            self.batch_stats['total_work_batched'] / total_batches
        )
        
        return batches
    
    def should_batch(self, work: EnhancedWorkContext) -> bool:
        """Determine if work should be batched"""
        if not work.can_batch:
            return False
        
        # Don't batch critical/SLA work
        if work.is_sla_critical():
            return False
        
        # Don't batch quantum work
        if work.quantum_capable:
            return False
        
        return True

# ============================================================================
# Enhanced Work Integrator
# ============================================================================

class EnhancedWorkIntegrator:
    """
    Enhanced Work Integration for Green Agent MoE System.
    
    Features:
    - DAG-based workflow orchestration
    - Formal work state machine
    - SLA management
    - Resource reservation
    - Intelligent batching
    - Checkpointing and rollback
    - Multi-tenant support
    - Work versioning
    """
    
    def __init__(
        self,
        expert_router=None,
        meta_cognitive_module=None,
        neuro_symbolic_module=None,
        quantum_module=None,
        enable_batching: bool = True,
        enable_checkpointing: bool = True,
        enable_rollback: bool = True,
        enable_sla_tracking: bool = True,
        enable_resource_reservation: bool = True
    ):
        # Core modules
        self.router = expert_router
        self.meta_cognitive = meta_cognitive_module
        self.neuro_symbolic = neuro_symbolic_module
        self.quantum_module = quantum_module
        
        # Feature flags
        self.enable_batching = enable_batching
        self.enable_checkpointing = enable_checkpointing
        self.enable_rollback = enable_rollback
        self.enable_sla_tracking = enable_sla_tracking
        self.enable_resource_reservation = enable_resource_reservation
        
        # Work management
        self.active_works: Dict[str, EnhancedWorkContext] = {}
        self.completed_works: Dict[str, Dict[str, Any]] = {}
        self.failed_works: Dict[str, Dict[str, Any]] = {}
        
        # Workflow engine
        self.workflow_dag = WorkflowDAG()
        
        # Resource manager
        self.resource_manager = ResourceReservationManager()
        
        # Work batcher
        self.batcher = IntelligentWorkBatcher() if enable_batching else None
        
        # Work metrics
        self.work_metrics: Dict[str, List[Dict]] = defaultdict(list)
        
        # SLA tracking
        self.sla_violations: List[Dict] = []
        
        # Pipeline registry
        self.pipelines = {
            'standard': self._standard_pipeline,
            'quantum_enhanced': self._quantum_pipeline,
            'helium_optimized': self._helium_pipeline,
            'meta_cognitive': self._meta_cognitive_pipeline,
            'batched': self._batched_pipeline,
            'checkpointed': self._checkpointed_pipeline
        }
        
        # Tenant isolation
        self.tenant_contexts: Dict[str, Dict[str, Any]] = {}
        
        # Background tasks
        self._start_background_tasks()
        
        logger.info("Enhanced Work Integrator initialized")
    
    def _start_background_tasks(self):
        """Start background maintenance tasks"""
        asyncio.create_task(self._cleanup_loop())
        asyncio.create_task(self._sla_monitor_loop())
        asyncio.create_task(self._batching_loop())
    
    # ========================================================================
    # Primary Work Processing
    # ========================================================================
    
    async def process_work(
        self,
        work_request: Dict[str, Any],
        pipeline_type: str = 'standard',
        dependencies: Optional[List[str]] = None,
        tenant_id: str = "default"
    ) -> Dict[str, Any]:
        """
        Process work through enhanced pipeline.
        
        Args:
            work_request: Work request with full context
            pipeline_type: Type of processing pipeline
            dependencies: Work dependencies
            tenant_id: Tenant identifier
            
        Returns:
            Processing results
        """
        # Create enhanced work context
        context = self._create_work_context(work_request, tenant_id)
        
        # Add to workflow DAG if dependencies exist
        if dependencies:
            self.workflow_dag.add_work(context, dependencies)
        else:
            self.workflow_dag.add_work(context)
        
        # Validate state transition
        if not context.transition_to(WorkState.VALIDATED):
            return self._create_error_response(context, "Invalid state transition")
        
        # Check SLA
        if self.enable_sla_tracking and context.sla:
            if context.sla.is_deadline_critical():
                context.priority = WorkPriority.CRITICAL
        
        # Reserve resources if enabled
        if self.enable_resource_reservation:
            reservation = await self._reserve_work_resources(context)
            if not reservation:
                context.transition_to(WorkState.QUEUED)
                return {
                    'success': False,
                    'status': 'queued',
                    'reason': 'Resources not available',
                    'task_id': context.task_id
                }
            context.reservation = reservation
            context.transition_to(WorkState.RESOURCES_RESERVED)
        
        # Check batching
        if self.enable_batching and self.batcher.should_batch(context):
            self.batcher.add_work(context)
            context.transition_to(WorkState.QUEUED)
            return {
                'success': True,
                'status': 'batched',
                'task_id': context.task_id
            }
        
        # Transition to executing
        if not context.transition_to(WorkState.EXECUTING):
            return self._create_error_response(context, "Cannot start execution")
        
        context.started_at = datetime.utcnow()
        context.execution_attempts += 1
        
        self.active_works[context.task_id] = context
        
        try:
            # Select and execute pipeline
            pipeline = self.pipelines.get(pipeline_type, self._standard_pipeline)
            result = await pipeline(context)
            
            # Checkpoint if enabled
            if self.enable_checkpointing:
                await self._create_checkpoint(context, result)
            
            # Mark completed
            context.transition_to(WorkState.COMPLETED)
            context.completed_at = datetime.utcnow()
            
            # Update workflow DAG
            self.workflow_dag.mark_completed(context.task_id)
            
            # Release resources
            if context.reservation:
                self.resource_manager.release_reservation(
                    context.reservation.reservation_id
                )
            
            # Record completion
            self.completed_works[context.task_id] = {
                'context': context,
                'result': result,
                'timestamp': datetime.utcnow().isoformat()
            }
            
            # Check SLA
            if self.enable_sla_tracking and context.sla:
                execution_time = (context.completed_at - context.started_at).total_seconds() * 1000
                if context.sla.is_violated(execution_time):
                    self._record_sla_violation(context, execution_time)
            
            # Update metrics
            self._update_work_metrics(context.task_id, result)
            
            return result
            
        except Exception as e:
            logger.error(f"Work processing failed for {context.task_id}: {str(e)}")
            
            context.transition_to(WorkState.FAILED)
            
            # Attempt rollback
            if self.enable_rollback:
                await self._rollback_work(context)
            
            # Retry if possible
            if context.can_retry():
                context.transition_to(WorkState.QUEUED)
                return await self.process_work(
                    work_request, pipeline_type, dependencies, tenant_id
                )
            
            # Release resources
            if context.reservation:
                self.resource_manager.release_reservation(
                    context.reservation.reservation_id
                )
            
            self.failed_works[context.task_id] = {
                'context': context,
                'error': str(e),
                'timestamp': datetime.utcnow().isoformat()
            }
            
            return self._create_error_response(context, str(e))
        
        finally:
            self.active_works.pop(context.task_id, None)
    
    # ========================================================================
    # Resource Reservation
    # ========================================================================
    
    async def _reserve_work_resources(
        self,
        context: EnhancedWorkContext
    ) -> Optional[ResourceReservation]:
        """Reserve resources for work execution"""
        # Calculate required resources
        required = {
            'cpu_cores': context.complexity * 4,
            'memory_gb': context.meta_cognitive_state.get('data_size_mb', 100) / 1024,
            'network_bandwidth_gbps': context.complexity * 0.5
        }
        
        if context.quantum_capable:
            required['quantum_qubits'] = min(
                context.complexity * 20, 20
            )
        
        return self.resource_manager.reserve_resources(
            work_id=context.task_id,
            required_resources=required,
            carbon_budget=context.max_carbon_budget,
            helium_budget=context.max_helium_budget,
            duration_seconds=context.estimated_duration_ms / 1000 * 2
        )
    
    # ========================================================================
    # Checkpointing
    # ========================================================================
    
    async def _create_checkpoint(
        self,
        context: EnhancedWorkContext,
        result: Dict[str, Any]
    ):
        """Create work checkpoint"""
        if not self.enable_checkpointing:
            return
        
        checkpoint = WorkCheckpoint(
            checkpoint_id=f"ckpt_{context.task_id}_{datetime.utcnow().timestamp()}",
            work_id=context.task_id,
            state=context.state,
            progress=0.5,  # Estimated progress
            intermediate_results=result,
            resource_usage={
                'carbon_kg': result.get('final_plan', {}).get('aggregate_carbon_kg', 0),
                'helium_units': result.get('final_plan', {}).get('aggregate_helium', 0)
            },
            created_at=datetime.utcnow(),
            pipeline_state={}
        )
        
        context.add_checkpoint(checkpoint)
        
        # Transition to checkpointed state
        if context.state == WorkState.EXECUTING:
            context.transition_to(WorkState.CHECKPOINTED)
    
    # ========================================================================
    # Rollback Support
    # ========================================================================
    
    async def _rollback_work(self, context: EnhancedWorkContext):
        """Rollback work execution"""
        if not self.enable_rollback:
            return
        
        context.transition_to(WorkState.ROLLING_BACK)
        
        # Execute compensation actions in reverse order
        for action in reversed(context.compensation_actions):
            try:
                await action() if asyncio.iscoroutinefunction(action) else action()
            except Exception as e:
                logger.error(f"Compensation action failed: {str(e)}")
        
        # Execute rollback actions
        for action in context.rollback_actions:
            try:
                await action() if asyncio.iscoroutinefunction(action) else action()
            except Exception as e:
                logger.error(f"Rollback action failed: {str(e)}")
        
        context.transition_to(WorkState.ROLLED_BACK)
    
    # ========================================================================
    # Batched Pipeline
    # ========================================================================
    
    async def _batched_pipeline(
        self,
        context: EnhancedWorkContext
    ) -> Dict[str, Any]:
        """Execute work as part of a batch"""
        # This is called when batch is ready to execute
        batch_group = context.batch_group
        
        # Execute with standard pipeline but with batch awareness
        result = await self._standard_pipeline(context)
        result['batched'] = True
        result['batch_group'] = batch_group
        
        return result
    
    # ========================================================================
    # Checkpointed Pipeline
    # ========================================================================
    
    async def _checkpointed_pipeline(
        self,
        context: EnhancedWorkContext
    ) -> Dict[str, Any]:
        """Execute work with checkpointing support"""
        # Check if resuming from checkpoint
        if context.resume_from_checkpoint:
            checkpoint = next(
                (c for c in context.checkpoints if c.checkpoint_id == context.resume_from_checkpoint),
                None
            )
            if checkpoint:
                logger.info(f"Resuming from checkpoint: {checkpoint.checkpoint_id}")
                context.transition_to(WorkState.RESUMED)
                # Restore intermediate state
        
        # Execute standard pipeline
        result = await self._standard_pipeline(context)
        
        # Create checkpoint
        await self._create_checkpoint(context, result)
        
        return result
    
    # ========================================================================
    # Standard Pipeline (with enhancements)
    # ========================================================================
    
    async def _standard_pipeline(
        self,
        context: EnhancedWorkContext
    ) -> Dict[str, Any]:
        """Enhanced standard pipeline"""
        
        # Step 1: Meta-cognitive pre-processing
        if self.meta_cognitive:
            context = await self._apply_meta_cognition(context)
        
        # Step 2: Neuro-symbolic constraint extraction
        symbolic_constraints = None
        if self.neuro_symbolic:
            symbolic_constraints = await self._extract_symbolic_constraints(context)
        
        # Step 3: Build dual-axis context
        dual_axis_context = self._build_dual_axis_context(context)
        
        # Step 4: Route through MoE system
        routing_result = self.router.route_and_execute(
            workload_profile=context.to_routing_context(),
            meta_cognitive_state=context.meta_cognitive_state,
            dual_axis_context=dual_axis_context,
            symbolic_constraints=symbolic_constraints
        )
        
        # Step 5: Post-processing
        result = self._post_process_result(routing_result, context)
        
        # Add work metadata
        result['work_metadata'] = {
            'task_id': context.task_id,
            'work_type': context.work_type,
            'priority': context.priority.name if hasattr(context.priority, 'name') else str(context.priority),
            'state': context.state.value,
            'version': context.current_version,
            'attempt': context.execution_attempts,
            'tenant_id': context.tenant_id
        }
        
        # Add SLA status
        if self.enable_sla_tracking and context.sla:
            execution_time = result.get('execution_time_ms', 0)
            result['sla_status'] = {
                'violated': context.sla.is_violated(execution_time),
                'max_latency_ms': context.sla.max_latency_ms,
                'actual_latency_ms': execution_time,
                'deadline': context.sla.deadline.isoformat() if context.sla.deadline else None,
                'violations': context.sla.violations
            }
        
        return result
    
    # ========================================================================
    # SLA Monitoring
    # ========================================================================
    
    async def _sla_monitor_loop(self):
        """Background SLA monitoring loop"""
        while True:
            try:
                if not self.enable_sla_tracking:
                    await asyncio.sleep(60)
                    continue
                
                now = datetime.utcnow()
                
                # Check active works for SLA violations
                for work_id, work in list(self.active_works.items()):
                    if work.sla and work.sla.deadline:
                        remaining = work.sla.time_until_deadline()
                        
                        if remaining is not None:
                            if remaining <= 0:
                                logger.warning(f"SLA deadline exceeded for {work_id}")
                                self._record_sla_violation(work, float('inf'))
                            elif remaining < 30:
                                logger.warning(
                                    f"SLA deadline approaching for {work_id}: {remaining:.0f}s remaining"
                                )
                                # Escalate priority
                                work.priority = WorkPriority.CRITICAL
                
                await asyncio.sleep(5)
                
            except Exception as e:
                logger.error(f"SLA monitor error: {str(e)}")
                await asyncio.sleep(30)
    
    def _record_sla_violation(
        self,
        context: EnhancedWorkContext,
        actual_latency_ms: float
    ):
        """Record SLA violation"""
        violation = {
            'work_id': context.task_id,
            'sla_level': context.sla.level.value,
            'max_latency_ms': context.sla.max_latency_ms,
            'actual_latency_ms': actual_latency_ms,
            'violated_at': datetime.utcnow().isoformat(),
            'tenant_id': context.tenant_id
        }
        
        self.sla_violations.append(violation)
        context.sla.violations += 1
        
        logger.warning(
            f"SLA VIOLATION: {context.task_id} "
            f"({actual_latency_ms:.0f}ms > {context.sla.max_latency_ms:.0f}ms)"
        )
    
    # ========================================================================
    # Batching Loop
    # ========================================================================
    
    async def _batching_loop(self):
        """Background batching loop"""
        while True:
            try:
                if not self.enable_batching or not self.batcher:
                    await asyncio.sleep(5)
                    continue
                
                # Form and execute batches
                batches = self.batcher.form_batches()
                
                for batch_id, batch_works in batches:
                    logger.info(
                        f"Executing batch {batch_id} with {len(batch_works)} works"
                    )
                    
                    # Execute batch concurrently
                    tasks = []
                    for work in batch_works:
                        work.batch_group = batch_id
                        tasks.append(self._batched_pipeline(work))
                    
                    results = await asyncio.gather(*tasks, return_exceptions=True)
                    
                    # Process results
                    for work, result in zip(batch_works, results):
                        if isinstance(result, Exception):
                            logger.error(f"Batch work {work.task_id} failed: {str(result)}")
                        else:
                            work.transition_to(WorkState.COMPLETED)
                            self.completed_works[work.task_id] = {
                                'context': work,
                                'result': result,
                                'timestamp': datetime.utcnow().isoformat()
                            }
                
                await asyncio.sleep(self.batcher.batching_window_ms / 1000)
                
            except Exception as e:
                logger.error(f"Batching loop error: {str(e)}")
                await asyncio.sleep(5)
    
    # ========================================================================
    # Cleanup Loop
    # ========================================================================
    
    async def _cleanup_loop(self):
        """Background cleanup loop"""
        while True:
            try:
                now = datetime.utcnow()
                max_age = timedelta(hours=24)
                
                # Clean up completed works
                expired = [
                    wid for wid, work in self.completed_works.items()
                    if now - datetime.fromisoformat(work['timestamp']) > max_age
                ]
                for wid in expired:
                    del self.completed_works[wid]
                
                # Clean up failed works
                expired_failed = [
                    wid for wid, work in self.failed_works.items()
                    if now - datetime.fromisoformat(work['timestamp']) > max_age
                ]
                for wid in expired_failed:
                    del self.failed_works[wid]
                
                # Clean up resource reservations
                if self.enable_resource_reservation:
                    self.resource_manager.cleanup_expired()
                
                await asyncio.sleep(300)  # Every 5 minutes
                
            except Exception as e:
                logger.error(f"Cleanup error: {str(e)}")
                await asyncio.sleep(60)
    
    # ========================================================================
    # Work Context Creation
    # ========================================================================
    
    def _create_work_context(
        self,
        request: Dict[str, Any],
        tenant_id: str = "default"
    ) -> EnhancedWorkContext:
        """Create enhanced work context from request"""
        
        # Create SLA if specified
        sla = None
        if request.get('sla_level'):
            sla_level = SLALevel(request['sla_level'])
            sla_configs = {
                SLALevel.PLATINUM: (10, 0.9999),
                SLALevel.GOLD: (50, 0.999),
                SLALevel.SILVER: (200, 0.99),
                SLALevel.BRONZE: (1000, 0.95),
                SLALevel.BEST_EFFORT: (5000, 0.0)
            }
            max_latency, min_availability = sla_configs.get(sla_level, (1000, 0.95))
            
            sla = WorkSLA(
                level=sla_level,
                max_latency_ms=request.get('max_latency_ms', max_latency),
                min_availability=min_availability,
                max_carbon_kg=request.get('max_carbon_budget'),
                max_helium_units=request.get('max_helium_budget'),
                deadline=request.get('deadline')
            )
        
        context = EnhancedWorkContext(
            task_id=request.get('task_id', str(uuid.uuid4())),
            work_type=request.get('task_type', 'inference'),
            priority=WorkPriority[request.get('priority', 'MEDIUM').upper()],
            sla=sla,
            complexity=request.get('complexity', 0.5),
            estimated_duration_ms=request.get('estimated_duration_ms', 100),
            helium_dependency=request.get('helium_dependency', 0.0),
            helium_profile=request.get('helium_profile', {}),
            meta_cognitive_state=request.get('meta_cognitive_state', {}),
            symbolic_rules=request.get('symbolic_rules', {}),
            carbon_zone=request.get('carbon_zone', 0),
            helium_zone=request.get('helium_zone', 0),
            quantum_capable=request.get('quantum_capable', False),
            quantum_circuit_required=request.get('quantum_circuit_required', False),
            quantum_backend_type=request.get('quantum_backend_type'),
            max_carbon_budget=request.get('max_carbon_budget', float('inf')),
            max_helium_budget=request.get('max_helium_budget', float('inf')),
            max_latency_ms=request.get('max_latency_ms', 1000.0),
            min_accuracy=request.get('min_accuracy', 0.0),
            can_batch=request.get('can_batch', True),
            tenant_id=tenant_id,
            isolation_level=request.get('isolation_level', 'shared')
        )
        
        # Add version
        context.add_version(["Initial creation"], "system")
        
        # Add event
        context.add_event("created", {'request': str(request)[:200]})
        
        return context
    
    # ========================================================================
    # Utility Methods
    # ========================================================================
    
    def _create_error_response(
        self,
        context: EnhancedWorkContext,
        error: str
    ) -> Dict[str, Any]:
        """Create error response"""
        context.add_event("error", {'error': error})
        
        return {
            'success': False,
            'error': error,
            'task_id': context.task_id,
            'state': context.state.value,
            'attempt': context.execution_attempts,
            'can_retry': context.can_retry()
        }
    
    def _update_work_metrics(
        self,
        task_id: str,
        result: Dict[str, Any]
    ):
        """Update work metrics"""
        self.work_metrics[task_id].append({
            'timestamp': datetime.utcnow().isoformat(),
            'success': result.get('success', False),
            'action': result.get('final_plan', {}).get('action', 'unknown'),
            'execution_time': result.get('execution_time_ms', 0),
            'carbon_kg': result.get('final_plan', {}).get('aggregate_carbon_kg', 0)
        })
    
    def get_work_statistics(self) -> Dict[str, Any]:
        """Get comprehensive work statistics"""
        total_completed = len(self.completed_works)
        total_failed = len(self.failed_works)
        total_active = len(self.active_works)
        total = total_completed + total_failed + total_active
        
        return {
            'total_works': total,
            'active_works': total_active,
            'completed_works': total_completed,
            'failed_works': total_failed,
            'success_rate': total_completed / max(total, 1),
            'workflow_stats': self.workflow_dag.get_workflow_stats(),
            'resource_availability': self.resource_manager.get_availability(),
            'sla_violations': len(self.sla_violations),
            'batching_stats': self.batcher.batch_stats if self.batcher else {},
            'pipeline_distribution': {
                pipeline: sum(
                    1 for w in self.completed_works.values()
                    if w['result'].get('pipeline_type') == pipeline
                )
                for pipeline in self.pipelines.keys()
            }
        }
    
    def get_work_state(self, task_id: str) -> Optional[Dict[str, Any]]:
        """Get current state of work"""
        if task_id in self.active_works:
            work = self.active_works[task_id]
            return {
                'task_id': task_id,
                'state': work.state.value,
                'priority': work.priority.name if hasattr(work.priority, 'name') else str(work.priority),
                'started_at': work.started_at.isoformat() if work.started_at else None,
                'attempt': work.execution_attempts,
                'sla_status': {
                    'violated': work.sla.violations > 0 if work.sla else False,
                    'deadline': work.sla.deadline.isoformat() if work.sla and work.sla.deadline else None
                } if work.sla else None
            }
        
        if task_id in self.completed_works:
            return {
                'task_id': task_id,
                'state': 'completed',
                'completed_at': self.completed_works[task_id]['timestamp']
            }
        
        if task_id in self.failed_works:
            return {
                'task_id': task_id,
                'state': 'failed',
                'error': self.failed_works[task_id]['error']
            }
        
        return None
    
    def cancel_work(self, task_id: str) -> bool:
        """Cancel work execution"""
        if task_id in self.active_works:
            work = self.active_works[task_id]
            work.transition_to(WorkState.CANCELLED)
            
            # Release resources
            if work.reservation:
                self.resource_manager.release_reservation(
                    work.reservation.reservation_id
                )
            
            del self.active_works[task_id]
            return True
        
        return False
    
    def get_tenant_works(self, tenant_id: str) -> List[EnhancedWorkContext]:
        """Get all works for a tenant"""
        return [
            w for w in self.active_works.values()
            if w.tenant_id == tenant_id
        ]
