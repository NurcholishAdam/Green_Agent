"""
Green Agent v5.0.0 - Unified Orchestrator
Central coordination layer managing all 12 execution layers

File: src/integration/unified_orchestrator.py
Status: FOUNDATIONAL - Tier 1
"""

from typing import Dict, List, Optional, Any
from dataclasses import dataclass, field
from datetime import datetime
import asyncio
import logging
from enum import Enum
import json

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class ExecutionMode(Enum):
    LEGACY = "legacy"
    UNIFIED = "unified"
    COMPARE = "compare"


class CarbonZone(Enum):
    GREEN = "green"
    YELLOW = "yellow"
    RED = "red"
    CRITICAL = "critical"


@dataclass
class WorkloadProfile:
    task_id: str
    complexity: float
    energy_estimate_kwh: float
    carbon_estimate_kg: float
    memory_estimate_mb: float
    cpu_estimate_percent: float
    deferrable: bool
    priority: int
    deadline: Optional[datetime] = None


@dataclass
class ExecutionDecision:
    action: str
    power_budget: float
    carbon_zone: CarbonZone
    reasoning: List[str] = field(default_factory=list)
    deferred_until: Optional[datetime] = None


@dataclass
class UnifiedResult:
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
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.mode = ExecutionMode(config.get('system', {}).get('mode', 'unified'))
        self.debug = config.get('system', {}).get('debug', False)
        
        self.interpreter = None
        self.decision_core = None
        self.executor = None
        self.carbon_forecaster = None
        self.carbon_ledger = None
        self.dashboard = None
        
        self.running = False
        self.tasks_executed = 0
        self.start_time = datetime.now()
        
        logger.info(f"UnifiedGreenAgent initialized in {self.mode.value} mode")
    
    async def initialize(self):
        logger.info("Initializing Unified Green Agent components...")
        
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
        
        await self.interpreter.initialize()
        await self.decision_core.initialize()
        await self.executor.initialize()
        await self.carbon_forecaster.initialize()
        await self.carbon_ledger.initialize()
        
        self.running = True
        logger.info("✅ Unified Green Agent initialized")
    
    async def shutdown(self):
        logger.info("Shutting down Unified Green Agent...")
        self.running = False
        
        if self.executor:
            await self.executor.shutdown()
        if self.carbon_ledger:
            await self.carbon_ledger.shutdown()
        
        logger.info("✅ Unified Green Agent shutdown complete")
    
    async def execute_task(self, task: Dict[str, Any]) -> UnifiedResult:
        start_time = datetime.now()
        task_id = task.get('id', 'unknown')
        
        logger.info(f"Executing task {task_id} in {self.mode.value} mode")
        
        try:
            profile = await self.interpreter.analyze(task)
            carbon_intensity = await self.carbon_forecaster.get_current_intensity()
            decision = await self.decision_core.evaluate(profile, carbon_intensity)
            result = await self.executor.run(task, profile, decision)
            await self.carbon_ledger.record(result, decision)
            self.tasks_executed += 1
            
            logger.info(f"Task {task_id} completed successfully")
            return result
            
        except Exception as e:
            logger.error(f"Task {task_id} failed: {str(e)}")
            return self._create_error_result(task_id, str(e), start_time)
    
    def _create_error_result(self, task_id: str, error: str, start_time: datetime) -> UnifiedResult:
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


async def main():
    config = {'system': {'mode': 'unified', 'debug': True}}
    agent = UnifiedGreenAgent(config)
    await agent.initialize()
    
    task = {'id': 'test_001', 'type': 'ml_inference', 'priority': 5, 'deferrable': True}
    result = await agent.execute_task(task)
    
    print(f"\n✅ Task {result.task_id}: {'Success' if result.success else 'Failed'}")
    print(f"   Energy: {result.energy_consumed:.4f} kWh")
    print(f"   Carbon: {result.carbon_emitted:.4f} kg CO2")
    print(f"   Accuracy: {result.accuracy:.2f}")
    
    await agent.shutdown()


if __name__ == "__main__":
    asyncio.run(main())
