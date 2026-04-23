# src/distributed/ray_cluster_manager.py (EXTENDED)

from typing import Dict, List, Optional, Any
from enum import Enum
from dataclasses import dataclass
import asyncio
import logging

logger = logging.getLogger(__name__)

class WorkerType(Enum):
    """Worker pool types with helium footprints"""
    STANDARD_CPU = "standard_cpu"
    GPU_SINGLE = "gpu_single"
    GPU_CLUSTER = "gpu_cluster"
    TPU = "tpu"
    QUANTUM = "quantum"
    
    @property
    def helium_footprint(self) -> float:
        footprints = {
            WorkerType.STANDARD_CPU: 0.10,
            WorkerType.GPU_SINGLE: 0.75,
            WorkerType.GPU_CLUSTER: 0.95,
            WorkerType.TPU: 0.85,
            WorkerType.QUANTUM: 0.99
        }
        return footprints[self]

@dataclass
class ExecutionResult:
    """Enhanced execution result with helium metrics"""
    success: bool
    task_id: str
    accuracy: float
    energy_consumed_kwh: float
    carbon_emitted_kg: float
    execution_time_ms: int
    worker_type: str
    
    # NEW: Helium metrics
    helium_usage: float = 0.0
    helium_zone: Optional[str] = None
    fallback_used: bool = False
    optimization_level: str = "none"

class HeliumAwareRayExecutor:
    """
    Ray executor with helium-aware routing and fallback paths
    """
    
    def __init__(self, config: Optional[Dict] = None):
        self.config = config or {}
        self.ray_address = self.config.get('ray_address', 'auto')
        
        # Worker pool configuration
        self.worker_pools = {
            WorkerType.STANDARD_CPU: {
                'available': True,
                'capacity': self.config.get('cpu_workers', 10),
                'helium_footprint': 0.10,
                'cost_factor': 1.0
            },
            WorkerType.GPU_SINGLE: {
                'available': self.config.get('gpu_available', True),
                'capacity': self.config.get('gpu_workers', 4),
                'helium_footprint': 0.75,
                'cost_factor': 3.0
            },
            WorkerType.GPU_CLUSTER: {
                'available': self.config.get('gpu_cluster_available', True),
                'capacity': self.config.get('gpu_cluster_workers', 2),
                'helium_footprint': 0.95,
                'cost_factor': 8.0
            },
            WorkerType.TPU: {
                'available': self.config.get('tpu_available', False),
                'capacity': self.config.get('tpu_workers', 2),
                'helium_footprint': 0.85,
                'cost_factor': 5.0
            },
            WorkerType.QUANTUM: {
                'available': self.config.get('quantum_available', False),
                'capacity': self.config.get('quantum_workers', 1),
                'helium_footprint': 0.99,
                'cost_factor': 20.0
            }
        }
        
        # Fallback configuration
        self.fallback_enabled = self.config.get('fallback_enabled', True)
        self.execution_timeout_seconds = self.config.get('execution_timeout', 300)
    
    async def execute_task(self, task: Any, workload_profile, 
                          execution_decision) -> ExecutionResult:
        """
        Execute task with helium-aware routing
        """
        
        helium_profile = getattr(workload_profile, 'helium_profile', None)
        
        # Determine target worker type based on helium constraints
        target_worker = self._select_worker_pool(execution_decision, helium_profile)
        
        if not target_worker:
            # No suitable worker available, try fallback
            if self.fallback_enabled:
                return await self._execute_fallback(task, workload_profile, execution_decision)
            else:
                return ExecutionResult(
                    success=False,
                    task_id=getattr(task, 'id', 'unknown'),
                    accuracy=0.0,
                    energy_consumed_kwh=0.0,
                    carbon_emitted_kg=0.0,
                    execution_time_ms=0,
                    worker_type='none',
                    helium_usage=0.0,
                    fallback_used=False
                )
        
        # Execute on selected worker
        logger.info(f"Executing task on {target_worker.value} (helium footprint: {self.worker_pools[target_worker]['helium_footprint']})")
        
        # Simulate execution (in production, actual Ray submission)
        import random
        execution_time = random.uniform(100, 1000)  # ms
        
        # Calculate helium usage based on worker type and power budget
        helium_usage = (self.worker_pools[target_worker]['helium_footprint'] * 
                       (1 - execution_decision.power_budget) * 0.5 +
                       execution_decision.power_budget * 0.5)
        
        result = ExecutionResult(
            success=True,
            task_id=getattr(task, 'id', 'unknown'),
            accuracy=0.95 * execution_decision.power_budget,  # Simulated accuracy drop
            energy_consumed_kwh=0.5 * execution_decision.power_budget,
            carbon_emitted_kg=0.2 * execution_decision.power_budget,
            execution_time_ms=execution_time,
            worker_type=target_worker.value,
            helium_usage=helium_usage,
            helium_zone=execution_decision.helium_zone.value if execution_decision.helium_zone else None,
            fallback_used=False,
            optimization_level=self._get_optimization_level(execution_decision)
        )
        
        return result
    
    def _select_worker_pool(self, execution_decision, helium_profile) -> Optional[WorkerType]:
        """
        Select appropriate worker pool based on helium constraints
        """
        
        # Check if helium is constraining
        if (execution_decision.helium_aware_flag and 
            execution_decision.helium_zone and
            execution_decision.helium_zone.value in ['helium_red', 'helium_critical']):
            
            # Helium scarce - prefer low-footprint workers
            if helium_profile and helium_profile.can_run_on_cpu:
                return WorkerType.STANDARD_CPU
            else:
                # Try single GPU as compromise
                if self.worker_pools[WorkerType.GPU_SINGLE]['available']:
                    return WorkerType.GPU_SINGLE
                else:
                    return WorkerType.STANDARD_CPU
                    
        elif (execution_decision.helium_aware_flag and 
              execution_decision.helium_zone and
              execution_decision.helium_zone.value == 'helium_yellow'):
            
            # Helium caution - prefer single GPU over clusters
            if self.worker_pools[WorkerType.GPU_SINGLE]['available']:
                return WorkerType.GPU_SINGLE
            elif self.worker_pools[WorkerType.STANDARD_CPU]['available']:
                return WorkerType.STANDARD_CPU
            else:
                return WorkerType.GPU_CLUSTER  # Last resort
        else:
            # Normal conditions - use optimal hardware based on workload
            if helium_profile:
                if helium_profile.dependency_score > 0.8:
                    return WorkerType.GPU_CLUSTER
                elif helium_profile.dependency_score > 0.5:
                    return WorkerType.GPU_SINGLE
                else:
                    return WorkerType.STANDARD_CPU
            else:
                return WorkerType.STANDARD_CPU
    
    async def _execute_fallback(self, task, workload_profile, execution_decision) -> ExecutionResult:
        """
        Execute fallback path for critical helium scarcity
        """
        logger.warning(f"Executing fallback for task {getattr(task, 'id', 'unknown')} due to helium scarcity")
        
        # Option 1: Use distilled model if available
        helium_profile = getattr(workload_profile, 'helium_profile', None)
        
        if helium_profile and helium_profile.can_use_distilled_model:
            logger.info("Fallback: Using distilled model")
            # Simulate distilled model execution
            return ExecutionResult(
                success=True,
                task_id=getattr(task, 'id', 'unknown'),
                accuracy=0.85,  # 15% accuracy drop
                energy_consumed_kwh=0.2,
                carbon_emitted_kg=0.08,
                execution_time_ms=150,
                worker_type='distilled_cpu',
                helium_usage=0.1,
                fallback_used=True,
                optimization_level='distilled'
            )
        
        # Option 2: Execute on CPU with degraded performance
        elif helium_profile and helium_profile.can_run_on_cpu:
            logger.info("Fallback: Executing on CPU")
            return ExecutionResult(
                success=True,
                task_id=getattr(task, 'id', 'unknown'),
                accuracy=0.70,  # 30% accuracy drop
                energy_consumed_kwh=0.15,
                carbon_emitted_kg=0.06,
                execution_time_ms=500,  # Much slower
                worker_type='cpu_fallback',
                helium_usage=0.05,
                fallback_used=True,
                optimization_level='degraded'
            )
        
        # Option 3: Defer
        else:
            logger.warning("Fallback: Deferring task due to no viable path")
            return ExecutionResult(
                success=False,
                task_id=getattr(task, 'id', 'unknown'),
                accuracy=0.0,
                energy_consumed_kwh=0.0,
                carbon_emitted_kg=0.0,
                execution_time_ms=0,
                worker_type='none',
                helium_usage=0.0,
                fallback_used=True
            )
    
    def _get_optimization_level(self, execution_decision) -> str:
        """Determine optimization level based on execution decision"""
        if not execution_decision.helium_aware_flag:
            return 'none'
        
        if execution_decision.power_budget >= 0.8:
            return 'light'
        elif execution_decision.power_budget >= 0.5:
            return 'moderate'
        elif execution_decision.power_budget >= 0.2:
            return 'aggressive'
        else:
            return 'deferred'
    
    def get_worker_pool_status(self) -> Dict:
        """Get current status of all worker pools"""
        return {
            worker.value: {
                'available': config['available'],
                'capacity': config['capacity'],
                'helium_footprint': config['helium_footprint']
            }
            for worker, config in self.worker_pools.items()
        }
