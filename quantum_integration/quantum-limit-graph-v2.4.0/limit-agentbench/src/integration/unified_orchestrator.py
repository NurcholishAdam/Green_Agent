# src/integration/unified_orchestrator.py (EXTENDED)

from typing import Dict, Optional, Any
import asyncio
import logging

logger = logging.getLogger(__name__)

class UnifiedOrchestrator:
    """
    Unified orchestrator integrating all 12 layers with helium awareness
    """
    
    def __init__(self, config: Optional[Dict] = None):
        self.config = config or {}
        
        # Initialize all layers with helium awareness
        from src.interpretation.workload_interpreter import WorkloadInterpreter
        from src.governance.helium_policy_adapter import HeliumPolicyAdapter
        from src.decision.carbon_aware_decision_core import CarbonAwareDecisionCore
        from src.optimization.ml_optimizer import HeliumAwareMLOptimizer, HeliumAwareDataOptimizer
        from src.distributed.ray_cluster_manager import HeliumAwareRayExecutor
        from src.carbon.helium_monitor import HeliumMonitor
        from src.carbon.carbon_ledger import ExtendedCarbonLedger
        from src.governance.benchmark_engine import HeliumAwareBenchmarkEngine
        
        # Layer 0
        self.workload_interpreter = WorkloadInterpreter(config)
        
        # Layer 1 (with helium policy)
        self.helium_policy = HeliumPolicyAdapter(config)
        
        # Layer 3 (enhanced)
        self.decision_core = CarbonAwareDecisionCore(config)
        
        # Layer 4-5 (helium-aware)
        self.ml_optimizer = HeliumAwareMLOptimizer(config)
        self.data_optimizer = HeliumAwareDataOptimizer(config)
        
        # Layer 6
        self.executor = HeliumAwareRayExecutor(config)
        
        # Layer 7
        self.helium_monitor = HeliumMonitor(config)
        
        # Layer 8
        self.carbon_ledger = ExtendedCarbonLedger(config)
        
        # Layer 9
        self.benchmark_engine = HeliumAwareBenchmarkEngine(config)
        
        # State
        self.is_running = False
    
    async def process_task(self, task_json: Dict) -> Dict:
        """
        Process a single task through all 12 layers with helium awareness
        """
        task_id = task_json.get('task_id', 'unknown')
        logger.info(f"Processing task {task_id} with helium-aware orchestrator")
        
        # LAYER 0: Workload Interpretation
        workload_profile = self.workload_interpreter.analyze_task(task_json)
        logger.info(f"Layer 0: WorkloadProfile created (helium dependency: {workload_profile.helium_profile.dependency_score if workload_profile.helium_profile else 'N/A'})")
        
        # LAYER 1: Get helium supply status and adapt policy
        helium_supply = await self.helium_monitor.get_current_supply()
        adapted_policy = self.helium_policy.adapt_policy(workload_profile, None)
        
        if adapted_policy.action == 'defer':
            logger.info(f"Layer 1: Task deferred due to {adapted_policy.reason}")
            return {
                'status': 'deferred',
                'task_id': task_id,
                'reason': adapted_policy.reason,
                'helium_aware': adapted_policy.helium_aware
            }
        
        # LAYER 3: Carbon-Aware Decision with Helium Zones
        carbon_intensity = self._get_carbon_intensity()  # Placeholder
        execution_decision = self.decision_core.make_decision(
            workload_profile, carbon_intensity, helium_supply
        )
        logger.info(f"Layer 3: Decision={execution_decision.action}, HeliumZone={execution_decision.helium_zone}")
        
        # LAYER 4-5: Optimize model and data
        optimization_config = {}
        if execution_decision.helium_aware_flag:
            # Apply helium-aware optimizations
            model_opt_results = self.ml_optimizer.optimize_model(task_json.get('model'), execution_decision)
            data_opt_results = self.data_optimizer.optimize_data_pipeline(task_json.get('data'), execution_decision)
            optimization_config = {
                'model': model_opt_results,
                'data': data_opt_results
            }
        
        # LAYER 6: Execute with helium-aware routing
        execution_result = await self.executor.execute_task(
            task_json, workload_profile, execution_decision
        )
        logger.info(f"Layer 6: Execution complete on {execution_result.worker_type}, helium_usage={execution_result.helium_usage:.3f}")
        
        # LAYER 7-8: Account for helium usage
        ledger_entry = self.carbon_ledger.add_entry(
            execution_result, execution_decision, helium_supply
        )
        logger.info(f"Layer 8: Ledger entry created with hash {ledger_entry.hash[:8]}...")
        
        # LAYER 9: Benchmark with helium metrics
        benchmark_report = self.benchmark_engine.update_pareto_frontier(
            execution_result, helium_supply
        )
        logger.info(f"Layer 9: Helium efficiency={benchmark_report.helium_efficiency:.2f}, resilience={benchmark_report.helium_resilience_score:.2f}")
        
        # Record helium usage for future policy learning
        await self.helium_policy.record_helium_usage(
            task_id, execution_result.helium_usage, {'success': execution_result.success}
        )
        
        # Return comprehensive result
        return {
            'status': 'completed' if execution_result.success else 'failed',
            'task_id': task_id,
            'execution_decision': {
                'action': execution_decision.action,
                'carbon_zone': execution_decision.carbon_zone.value,
                'helium_zone': execution_decision.helium_zone.value if execution_decision.helium_zone else None,
                'power_budget': execution_decision.power_budget
            },
            'execution_result': {
                'accuracy': execution_result.accuracy,
                'energy_kwh': execution_result.energy_consumed_kwh,
                'carbon_kg': execution_result.carbon_emitted_kg,
                'helium_usage': execution_result.helium_usage,
                'worker_type': execution_result.worker_type,
                'fallback_used': execution_result.fallback_used
            },
            'benchmark': {
                'helium_efficiency': benchmark_report.helium_efficiency,
                'helium_resilience_score': benchmark_report.helium_resilience_score,
                'recommendations': benchmark_report.recommendations
            },
            'ledger_hash': ledger_entry.hash
        }
    
    def _get_carbon_intensity(self) -> float:
        """Placeholder for carbon intensity API call"""
        # In production, call Layer 7 Carbon Monitoring
        return 150.0  # Example value
    
    async def get_helium_status(self) -> Dict:
        """Get current helium supply status"""
        supply = await self.helium_monitor.get_current_supply()
        if supply:
            return {
                'scarcity_level': supply.scarcity_level.value,
                'scarcity_score': supply.scarcity_score,
                'spot_price_usd': supply.spot_price_usd_per_liter,
                'fab_inventory_days': supply.fab_inventory_days,
                'timestamp': supply.timestamp.isoformat()
            }
        return {'error': 'No data available'}
    
    async def get_helium_report(self) -> Dict:
        """Get comprehensive helium report"""
        ledger_report = self.carbon_ledger.get_helium_efficiency_report()
        helium_status = await self.get_helium_status()
        ranking = self.benchmark_engine.get_helium_ranking(5)
        
        return {
            'current_supply': helium_status,
            'efficiency_report': ledger_report,
            'top_efficient_tasks': ranking,
            'worker_pools': self.executor.get_worker_pool_status()
        }
