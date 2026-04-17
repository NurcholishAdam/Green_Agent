"""
Green Agent v5.0.0 - Unified Orchestrator
Central coordination layer managing all 12 execution layers
File: src/integration/unified_orchestrator.py
"""

from typing import Dict, List, Optional, Any
from dataclasses import dataclass, field
from datetime import datetime
import asyncio
import logging
from enum import Enum

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class ExecutionMode(Enum):
    """Supported execution modes"""
    LEGACY = "legacy"
    UNIFIED = "unified"
    COMPARE = "compare"


class CarbonZone(Enum):
    """Carbon intensity zones"""
    GREEN = "green"        # < 50 gCO2/kWh
    YELLOW = "yellow"      # 50-200 gCO2/kWh
    RED = "red"            # 200-400 gCO2/kWh
    CRITICAL = "critical"  # > 400 gCO2/kWh


@dataclass
class WorkloadProfile:
    """Layer 0: Workload interpretation result"""
    task_id: str
    complexity: float  # 0.0-1.0
    energy_estimate_kwh: float
    carbon_estimate_kg: float
    memory_estimate_mb: float
    cpu_estimate_percent: float
    deferrable: bool
    priority: int  # 1-10
    deadline: Optional[datetime] = None


@dataclass
class ExecutionDecision:
    """Layer 3: Carbon-aware decision"""
    action: str  # execute_full, execute_throttled, defer, execute_minimal
    power_budget: float  # 0.0-1.0
    carbon_zone: CarbonZone
    reasoning: List[str] = field(default_factory=list)
    deferred_until: Optional[datetime] = None


@dataclass
class UnifiedResult:
    """Final execution result across all layers"""
    task_id: str
    success: bool
    execution_time: float
    accuracy: float
    energy_consumed: float
    carbon_emitted: float
    negawatt_reward: float
    carbon_zone: str
    metrics: Dict[str, Any] = field(default_factory=dict)
    errors: List[str] = field(default_factory=list)


class UnifiedGreenAgent:
    """
    Main orchestrator coordinating all 12 layers
    """
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.mode = ExecutionMode(config.get('system', {}).get('mode', 'unified'))
        self.debug = config.get('system', {}).get('debug', False)
        
        # Components (initialized in initialize())
        self.interpreter = None
        self.decision_core = None
        self.executor = None
        self.carbon_forecaster = None
        self.carbon_ledger = None
        self.dashboard = None
        
        # State
        self.running = False
        self.tasks_executed = 0
        self.start_time = datetime.now()
        
        logger.info(f"UnifiedGreenAgent initialized in {self.mode.value} mode")
    
    async def initialize(self):
        """Initialize all components"""
        logger.info("Initializing Unified Green Agent components...")
        
        # Import and initialize components
        from src.interpretation.workload_interpreter import WorkloadInterpreter
        from src.decision.carbon_aware_decision_core import CarbonAwareDecisionCore
        from src.distributed.ray_cluster_manager import RayExecutor
        from src.carbon.forecasting_engine import CarbonForecaster
        from src.governance.carbon_ledger import CarbonLedger
        
        self.interpreter = WorkloadInterpreter(self.config)
        self.decision_core = CarbonAwareDecisionCore(self.config)
        self.executor = RayExecutor(self.config)
        self.carbon_forecaster = CarbonForecaster(self.config)
        self.carbon_ledger = CarbonLedger(self.config)
        
        # Initialize each component
        await self.interpreter.initialize()
        await self.decision_core.initialize()
        await self.executor.initialize()
        await self.carbon_forecaster.initialize()
        await self.carbon_ledger.initialize()
        
        self.running = True
        logger.info("✅ Unified Green Agent initialized")
    
    async def shutdown(self):
        """Gracefully shutdown all components"""
        logger.info("Shutting down Unified Green Agent...")
        self.running = False
        
        if self.executor:
            await self.executor.shutdown()
        if self.carbon_ledger:
            await self.carbon_ledger.shutdown()
        
        logger.info("✅ Unified Green Agent shutdown complete")
    
    async def execute_task(self, task: Dict[str, Any]) -> UnifiedResult:
        """Execute a single task through all 12 layers"""
        start_time = datetime.now()
        task_id = task.get('id', 'unknown')
        
        logger.info(f"Executing task {task_id} in {self.mode.value} mode")
        
        try:
            # Layer 0: Interpret workload
            profile = await self.interpreter.analyze(task)
            
            # Layer 7: Get carbon intensity
            carbon_intensity = await self.carbon_forecaster.get_current_intensity()
            
            # Layer 3: Carbon-aware decision
            decision = await self.decision_core.evaluate(profile, carbon_intensity)
            
            # Layer 6: Distributed execution
            result = await self.executor.run(task, profile, decision)
            
            # Layer 8: Record carbon metrics
            await self.carbon_ledger.record(result, decision)
            
            # Update counters
            self.tasks_executed += 1
            
            logger.info(f"Task {task_id} completed successfully")
            return result
            
        except Exception as e:
            logger.error(f"Task {task_id} failed: {str(e)}")
            return self._create_error_result(task_id, str(e), start_time)
    
    def _create_error_result(self, task_id: str, error: str, start_time: datetime) -> UnifiedResult:
        """Create error result when task fails"""
        return UnifiedResult(
            task_id=task_id,
            success=False,
            execution_time=(datetime.now() - start_time).total_seconds(),
            accuracy=0.0,
            energy_consumed=0.0,
            carbon_emitted=0.0,
            negawatt_reward=0.0,
            carbon_zone="unknown",
            errors=[error]
        )


# Test entry point
async def main():
    """Test the orchestrator"""
    config = {'system': {'mode': 'unified', 'debug': True}}
    agent = UnifiedGreenAgent(config)
    await agent.initialize()
    
    task = {'id': 'test_001', 'type': 'ml_inference', 'priority': 5, 'deferrable': True}
    result = await agent.execute_task(task)
    
    print(f"\n✅ Task {result.task_id}: {'Success' if result.success else 'Failed'}")
    print(f"   Energy: {result.energy_consumed:.4f} kWh")
    print(f"   Carbon: {result.carbon_emitted:.4f} kg CO2")
    
    await agent.shutdown()


if __name__ == "__main__":
    asyncio.run(main())
