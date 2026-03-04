"""
Green Agent Complete Integration Orchestrator
==============================================

Integrates all 8 layers of the enhanced architecture into a closed-loop system.

Location: src/integration/green_agent_orchestrator.py
"""

from typing import Dict, Any, Optional
from dataclasses import dataclass
from datetime import datetime, timedelta
import asyncio
import logging

# Import all modules
from task_carbon_profiler import TaskCarbonProfiler
from multi_objective_scheduler import MultiObjectiveScheduler, ExecutionMode
from adaptation_classifier import AdaptationStrategyClassifier
from policy_engine import ParameterEfficiencyPolicyEngine, PolicyMode
from carbon_ledger import CarbonLedgerService
from forecasting_engine import CarbonForecaster
from eco_mode_controller import EcoModeController
from ray_cluster_manager import RayClusterManager

logger = logging.getLogger(__name__)


@dataclass
class WorkflowResult:
    """Complete workflow execution result"""
    task_id: str
    status: str  # "completed", "deferred", "blocked"
    execution_time_hours: float
    energy_kwh: float
    carbon_kgco2e: float
    carbon_saved_kgco2e: float
    carbon_savings_pct: float
    final_accuracy: Optional[float]
    workflow_steps: list
    reasoning: str


class GreenAgentOrchestrator:
    """
    Complete orchestrator for sustainable AI workload management
    
    Architecture Layers:
    1. Carbon-Aware Scheduler (Carbon profiler + Multi-objective scheduler)
    2. Efficient Fine-Tuning Enforcement (Adaptation classifier + Policy engine)
    3. Carbon Budget Governance (Carbon ledger)
    4. Distributed Execution (Ray cluster + Eco-mode controller)
    """
    
    def __init__(
        self,
        carbon_forecaster: CarbonForecaster,
        ray_cluster: RayClusterManager,
        policy_mode: PolicyMode = PolicyMode.MODERATE
    ):
        # Layer 1: Carbon-Aware Scheduler
        self.task_profiler = TaskCarbonProfiler()
        self.multi_obj_scheduler = MultiObjectiveScheduler(
            carbon_forecaster=carbon_forecaster,
            task_profiler=self.task_profiler,
            ray_cluster=ray_cluster
        )
        
        # Layer 2: Efficient Fine-Tuning Enforcement
        self.adaptation_classifier = AdaptationStrategyClassifier()
        self.policy_engine = ParameterEfficiencyPolicyEngine(policy_mode=policy_mode)
        
        # Layer 3: Carbon Budget Governance
        self.carbon_ledger = CarbonLedgerService()
        
        # Layer 4: Distributed Execution
        self.carbon_forecaster = carbon_forecaster
        self.eco_mode_controller = EcoModeController(carbon_forecaster=carbon_forecaster)
        self.ray_cluster = ray_cluster
        
        # Statistics
        self.total_tasks_processed = 0
        self.total_carbon_saved_kgco2e = 0.0
        
        logger.info("🌿 Green Agent Orchestrator initialized")
    
    async def execute_workflow(
        self,
        task: Dict[str, Any]
    ) -> WorkflowResult:
        """
        Execute complete sustainable AI workflow
        
        Workflow Steps:
        1. Task submitted
        2. Estimate carbon (Task Profiler)
        3. Check budget (Carbon Ledger)
        4. Classify fine-tuning strategy (Adaptation Classifier)
        5. Enforce policy (Policy Engine)
        6. Schedule execution (Multi-Objective Scheduler)
        7. Apply eco-mode throttling (Eco-Mode Controller)
        8. Execute on Ray cluster
        9. Record telemetry (Task Profiler + Carbon Ledger)
        10. Return results
        """
        
        task_id = task.get("task_id", f"task_{self.total_tasks_processed}")
        team = task.get("team", "default_team")
        workflow_steps = []
        start_time = datetime.now()
        
        logger.info(f"🚀 Starting workflow for task: {task_id}")
        
        # Step 1: Task Submitted
        workflow_steps.append({
            "step": 1,
            "name": "Task Submitted",
            "status": "completed",
            "timestamp": datetime.now().isoformat()
        })
        
        # Step 2: Estimate Carbon (Task Carbon Profiler)
        logger.info(f"📊 Step 2: Estimating carbon for {task_id}")
        carbon_intensity = await self.carbon_forecaster.get_current_intensity(
            task.get("region", "US-CA")
        )
        carbon_estimate = await self.task_profiler.estimate_energy(
            task=task,
            carbon_intensity=carbon_intensity
        )
        workflow_steps.append({
            "step": 2,
            "name": "Carbon Estimation",
            "status": "completed",
            "estimated_carbon": carbon_estimate.expected_carbon_kgco2e,
            "confidence": carbon_estimate.confidence
        })
        
        # Step 3: Check Budget (Carbon Ledger)
        logger.info(f"💰 Step 3: Checking carbon budget for team {team}")
        team_budget = self.carbon_ledger.get_team_budget(team)
        
        if team_budget:
            has_budget = self.carbon_ledger.check_budget_available(
                team=team,
                required_carbon=carbon_estimate.expected_carbon_kgco2e
            )
            
            if not has_budget:
                logger.warning(f"❌ Task {task_id} blocked: insufficient carbon budget")
                return WorkflowResult(
                    task_id=task_id,
                    status="blocked",
                    execution_time_hours=0.0,
                    energy_kwh=0.0,
                    carbon_kgco2e=0.0,
                    carbon_saved_kgco2e=0.0,
                    carbon_savings_pct=0.0,
                    final_accuracy=None,
                    workflow_steps=workflow_steps,
                    reasoning=f"Carbon budget exhausted: {team_budget.remaining_kgco2e:.3f} kgCO2e remaining"
                )
            
            workflow_steps.append({
                "step": 3,
                "name": "Budget Check",
                "status": "approved",
                "remaining_budget": team_budget.remaining_kgco2e
            })
        else:
            workflow_steps.append({
                "step": 3,
                "name": "Budget Check",
                "status": "skipped",
                "note": "No budget set for team"
            })
        
        # Step 4: Classify Fine-Tuning Strategy (Adaptation Classifier)
        if task.get("task_type") == "fine_tuning":
            logger.info(f"🎓 Step 4: Classifying fine-tuning strategy")
            strategy_rec = self.adaptation_classifier.classify(
                task_scope=task.get("task_scope", "single_task"),
                dataset_size=task.get("dataset_size", 10_000),
                domain_shift=task.get("domain_shift", "moderate"),
                carbon_budget=team_budget.remaining_kgco2e if team_budget else 1.0,
                target_accuracy=task.get("target_accuracy", 0.90),
                model_size_params=task.get("num_parameters")
            )
            
            task["recommended_strategy"] = strategy_rec.strategy.value
            workflow_steps.append({
                "step": 4,
                "name": "Strategy Classification",
                "status": "completed",
                "recommended": strategy_rec.strategy.value,
                "reasoning": strategy_rec.reasoning
            })
        else:
            workflow_steps.append({
                "step": 4,
                "name": "Strategy Classification",
                "status": "skipped",
                "note": "Not a fine-tuning task"
            })
            strategy_rec = None
        
        # Step 5: Enforce Policy (Policy Engine)
        if strategy_rec:
            logger.info(f"🔒 Step 5: Enforcing parameter-efficiency policy")
            policy_context = {
                "carbon_budget": team_budget.budget_kgco2e if team_budget else 10.0,
                "carbon_remaining": team_budget.remaining_kgco2e if team_budget else 10.0,
                "dataset_size": task.get("dataset_size", 10_000),
                "model_params": task.get("num_parameters", 100_000_000)
            }
            
            policy_decision = self.policy_engine.enforce(
                requested_strategy=task.get("fine_tuning_method", "full_fine_tuning"),
                recommended_strategy=strategy_rec.strategy.value,
                policy_context=policy_context
            )
            
            if not policy_decision.approved and not policy_decision.override_allowed:
                logger.warning(f"❌ Task {task_id} blocked by policy")
                return WorkflowResult(
                    task_id=task_id,
                    status="blocked",
                    execution_time_hours=0.0,
                    energy_kwh=0.0,
                    carbon_kgco2e=0.0,
                    carbon_saved_kgco2e=0.0,
                    carbon_savings_pct=0.0,
                    final_accuracy=None,
                    workflow_steps=workflow_steps,
                    reasoning=f"Policy violation: {policy_decision.reasoning}"
                )
            
            # Apply enforced strategy
            if policy_decision.enforced_strategy:
                task["fine_tuning_method"] = policy_decision.enforced_strategy
            
            workflow_steps.append({
                "step": 5,
                "name": "Policy Enforcement",
                "status": "completed",
                "approved": policy_decision.approved,
                "enforced_strategy": policy_decision.enforced_strategy,
                "carbon_levy": policy_decision.carbon_levy
            })
        else:
            workflow_steps.append({
                "step": 5,
                "name": "Policy Enforcement",
                "status": "skipped"
            })
        
        # Step 6: Schedule Execution (Multi-Objective Scheduler)
        logger.info(f"📅 Step 6: Scheduling optimal execution")
        scheduling_decision = await self.multi_obj_scheduler.schedule(task)
        
        if scheduling_decision.chosen_option.mode == ExecutionMode.DEFERRED:
            logger.info(
                f"⏰ Task {task_id} deferred to {scheduling_decision.chosen_option.start_time}"
            )
            workflow_steps.append({
                "step": 6,
                "name": "Scheduling",
                "status": "deferred",
                "scheduled_time": scheduling_decision.chosen_option.start_time.isoformat(),
                "reasoning": scheduling_decision.reasoning
            })
            
            return WorkflowResult(
                task_id=task_id,
                status="deferred",
                execution_time_hours=0.0,
                energy_kwh=0.0,
                carbon_kgco2e=0.0,
                carbon_saved_kgco2e=(
                    carbon_estimate.expected_carbon_kgco2e -
                    scheduling_decision.chosen_option.carbon_kgco2e
                ),
                carbon_savings_pct=0.0,
                final_accuracy=None,
                workflow_steps=workflow_steps,
                reasoning=scheduling_decision.reasoning
            )
        
        workflow_steps.append({
            "step": 6,
            "name": "Scheduling",
            "status": "completed",
            "mode": scheduling_decision.chosen_option.mode.value,
            "carbon_estimate": scheduling_decision.chosen_option.carbon_kgco2e
        })
        
        # Step 7: Apply Eco-Mode Throttling (Eco-Mode Controller)
        logger.info(f"🌱 Step 7: Applying eco-mode throttling")
        throttling_decision = await self.eco_mode_controller.apply_throttling(task)
        
        if throttling_decision.should_defer:
            workflow_steps.append({
                "step": 7,
                "name": "Eco-Mode",
                "status": "deferred",
                "defer_until": throttling_decision.defer_until.isoformat()
            })
            # Return deferred
            return WorkflowResult(
                task_id=task_id,
                status="deferred",
                execution_time_hours=0.0,
                energy_kwh=0.0,
                carbon_kgco2e=0.0,
                carbon_saved_kgco2e=0.0,
                carbon_savings_pct=0.0,
                final_accuracy=None,
                workflow_steps=workflow_steps,
                reasoning=f"Deferred by eco-mode to {throttling_decision.defer_until}"
            )
        
        # Apply throttled parameters
        task.update(throttling_decision.throttled_task)
        
        workflow_steps.append({
            "step": 7,
            "name": "Eco-Mode",
            "status": "completed",
            "eco_mode": throttling_decision.eco_mode.value,
            "energy_reduction": throttling_decision.estimated_energy_reduction_percent
        })
        
        # Step 8: Execute on Ray Cluster
        logger.info(f"⚡ Step 8: Executing task on Ray cluster")
        
        # Simulate execution (in production, this would be real execution)
        await asyncio.sleep(0.1)  # Simulate processing time
        
        # Calculate actual energy and carbon (using throttled estimates)
        actual_energy = carbon_estimate.expected_energy_kwh * (
            1.0 - throttling_decision.estimated_energy_reduction_percent / 100
        )
        actual_carbon = actual_energy * carbon_intensity / 1000
        
        execution_time = (datetime.now() - start_time).total_seconds() / 3600
        
        workflow_steps.append({
            "step": 8,
            "name": "Execution",
            "status": "completed",
            "energy_kwh": actual_energy,
            "carbon_kgco2e": actual_carbon,
            "duration_hours": execution_time
        })
        
        # Step 9: Record Telemetry (Task Profiler + Carbon Ledger)
        logger.info(f"📝 Step 9: Recording telemetry and updating ledger")
        
        # Add to telemetry
        self.task_profiler.add_telemetry_record(
            task=task,
            actual_energy=actual_energy,
            actual_carbon=actual_carbon
        )
        
        # Record in ledger
        cost_usd = actual_energy * 0.20  # $0.20 per kWh
        self.carbon_ledger.record_transaction(
            team=team,
            task_id=task_id,
            energy_kwh=actual_energy,
            carbon_kgco2e=actual_carbon,
            cost_usd=cost_usd
        )
        
        workflow_steps.append({
            "step": 9,
            "name": "Telemetry Recording",
            "status": "completed"
        })
        
        # Step 10: Calculate Carbon Savings
        baseline_carbon = carbon_estimate.expected_carbon_kgco2e
        carbon_saved = baseline_carbon - actual_carbon
        carbon_savings_pct = (carbon_saved / baseline_carbon * 100) if baseline_carbon > 0 else 0
        
        self.total_tasks_processed += 1
        self.total_carbon_saved_kgco2e += carbon_saved
        
        workflow_steps.append({
            "step": 10,
            "name": "Results",
            "status": "completed",
            "carbon_saved": carbon_saved,
            "carbon_savings_pct": carbon_savings_pct
        })
        
        logger.info(
            f"✅ Task {task_id} completed: {actual_carbon:.4f} kgCO2e "
            f"(saved {carbon_savings_pct:.1f}%)"
        )
        
        return WorkflowResult(
            task_id=task_id,
            status="completed",
            execution_time_hours=execution_time,
            energy_kwh=actual_energy,
            carbon_kgco2e=actual_carbon,
            carbon_saved_kgco2e=carbon_saved,
            carbon_savings_pct=carbon_savings_pct,
            final_accuracy=task.get("expected_accuracy", 0.90),
            workflow_steps=workflow_steps,
            reasoning=f"Completed successfully with {carbon_savings_pct:.1f}% carbon savings"
        )
    
    def get_system_statistics(self) -> Dict[str, Any]:
        """Get comprehensive system statistics"""
        return {
            "total_tasks_processed": self.total_tasks_processed,
            "total_carbon_saved_kgco2e": self.total_carbon_saved_kgco2e,
            "task_profiler": self.task_profiler.get_statistics(),
            "scheduler": self.multi_obj_scheduler.get_statistics(),
            "policy_engine": self.policy_engine.get_statistics(),
            "eco_mode_controller": self.eco_mode_controller.get_statistics()
        }


if __name__ == "__main__":
    async def main():
        # Initialize components
        from forecasting_engine import create_forecaster
        from ray_cluster_manager import create_ray_cluster
        
        forecaster = await create_forecaster(region="US-CA", train_days=30)
        cluster = create_ray_cluster(num_workers=4)
        
        # Create orchestrator
        orchestrator = GreenAgentOrchestrator(
            carbon_forecaster=forecaster,
            ray_cluster=cluster,
            policy_mode=PolicyMode.MODERATE
        )
        
        # Set team budget
        orchestrator.carbon_ledger.set_team_budget(
            team="nlp_research",
            period="2026-03",
            budget_kgco2e=20.0
        )
        
        # Execute workflow
        task = {
            "task_id": "bert_sentiment_demo",
            "team": "nlp_research",
            "model_name": "bert-base-uncased",
            "task_type": "fine_tuning",
            "dataset_size": 10_000,
            "num_epochs": 3,
            "batch_size": 32,
            "hardware": "V100",
            "region": "US-CA",
            "priority": 0.7,
            "deferrable": True,
            "deadline": datetime.now() + timedelta(hours=48),
            "fine_tuning_method": "full_fine_tuning",
            "target_accuracy": 0.92
        }
        
        result = await orchestrator.execute_workflow(task)
        
        print(f"\n{'='*60}")
        print(f"GREEN AGENT WORKFLOW RESULT")
        print(f"{'='*60}")
        print(f"Task ID: {result.task_id}")
        print(f"Status: {result.status}")
        print(f"Energy: {result.energy_kwh:.4f} kWh")
        print(f"Carbon: {result.carbon_kgco2e:.4f} kgCO2e")
        print(f"Carbon Saved: {result.carbon_saved_kgco2e:.4f} kgCO2e ({result.carbon_savings_pct:.1f}%)")
        print(f"Execution Time: {result.execution_time_hours:.2f} hours")
        print(f"Reasoning: {result.reasoning}")
        print(f"\nWorkflow Steps:")
        for step in result.workflow_steps:
            print(f"  {step['step']}. {step['name']}: {step['status']}")
        
        # System statistics
        stats = orchestrator.get_system_statistics()
        print(f"\n{'='*60}")
        print(f"SYSTEM STATISTICS")
        print(f"{'='*60}")
        print(f"Total Tasks: {stats['total_tasks_processed']}")
        print(f"Total Carbon Saved: {stats['total_carbon_saved_kgco2e']:.4f} kgCO2e")
        
        cluster.shutdown()
    
    asyncio.run(main())
