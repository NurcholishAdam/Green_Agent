"""
Carbon-Aware Decision Core
===========================

The BRAIN of Green Agent. Unifies all decision-making components.

Decides: WHEN to run | WHERE to run | HOW to run

Location: src/decision/carbon_aware_decision_core.py
"""

from typing import Dict, Any, Optional, List
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
import logging

logger = logging.getLogger(__name__)


class ExecutionDecision(Enum):
    """High-level execution decisions"""
    EXECUTE_NOW = "execute_now"
    DEFER_TO_OPTIMAL = "defer_to_optimal"
    BLOCK_INSUFFICIENT_BUDGET = "block_insufficient_budget"
    ENFORCE_EFFICIENT_METHOD = "enforce_efficient_method"
    ROUTE_TO_GREEN_REGION = "route_to_green_region"


@dataclass
class DecisionContext:
    """Context for decision-making"""
    # Task info
    task_id: str
    task_type: str
    model_params: int
    dataset_size: int
    team: str
    
    # Carbon context
    carbon_intensity_current: float
    carbon_intensity_forecast_avg: float
    carbon_budget_remaining: float
    carbon_estimate_kwh: float
    
    # Performance context
    priority: float  # 0-1
    deferrable: bool
    deadline: Optional[datetime]
    
    # Policy context
    requested_method: str  # e.g., "full_fine_tuning"
    recommended_method: str  # e.g., "lora"


@dataclass
class CoreDecision:
    """Complete decision from the core"""
    decision_type: ExecutionDecision
    when: datetime  # When to execute
    where: str  # Which node/region
    how: str  # Which method (full-FT, LoRA, etc.)
    reasoning: str
    estimated_carbon_kgco2e: float
    estimated_savings_kgco2e: float
    estimated_savings_percent: float


class CarbonAwareDecisionCore:
    """
    Unified decision-making brain for Green Agent
    
    Integrates:
    1. Carbon Forecaster (WHEN to run)
    2. Multi-Objective Scheduler (WHERE to run)
    3. Adaptation Classifier (HOW to optimize)
    4. Policy Engine (WHAT is allowed)
    5. Budget Controller (CAN we afford it)
    
    Decision Flow:
    1. Check carbon budget → BLOCK or CONTINUE
    2. Classify fine-tuning strategy → RECOMMEND method
    3. Enforce policy → ENFORCE or ALLOW
    4. Schedule execution → IMMEDIATE or DEFERRED or ROUTED
    5. Return complete decision
    """
    
    def __init__(
        self,
        carbon_forecaster,
        multi_obj_scheduler,
        adaptation_classifier,
        policy_engine,
        carbon_ledger
    ):
        self.carbon_forecaster = carbon_forecaster
        self.multi_obj_scheduler = multi_obj_scheduler
        self.adaptation_classifier = adaptation_classifier
        self.policy_engine = policy_engine
        self.carbon_ledger = carbon_ledger
        
        # Decision statistics
        self.total_decisions = 0
        self.decisions_by_type = {}
        self.total_carbon_saved_kgco2e = 0.0
        
        logger.info("🧠 Carbon-Aware Decision Core initialized")
    
    async def make_decision(
        self,
        task: Dict[str, Any],
        context: DecisionContext
    ) -> CoreDecision:
        """
        Make complete execution decision
        
        Args:
            task: Task specification
            context: Decision context with all relevant info
        
        Returns:
            CoreDecision with WHEN, WHERE, HOW
        """
        
        logger.info(f"🧠 Making decision for task: {context.task_id}")
        
        # Step 1: Check carbon budget
        budget_check = self._check_budget(context)
        if not budget_check["approved"]:
            return self._create_blocked_decision(context, budget_check["reason"])
        
        # Step 2: Classify adaptation strategy (for fine-tuning tasks)
        if context.task_type == "fine_tuning":
            strategy = self._classify_strategy(task, context)
        else:
            strategy = {"recommended": context.requested_method, "reasoning": "Not a fine-tuning task"}
        
        # Step 3: Enforce policy
        policy_decision = self._enforce_policy(context, strategy["recommended"])
        if not policy_decision["approved"]:
            if not policy_decision["override_allowed"]:
                return self._create_blocked_decision(context, policy_decision["reasoning"])
            # Policy enforces different method
            enforced_method = policy_decision["enforced_method"]
        else:
            enforced_method = strategy["recommended"]
        
        # Step 4: Schedule execution
        scheduling_decision = await self._schedule_execution(task, context, enforced_method)
        
        # Step 5: Compile complete decision
        decision = self._compile_decision(
            context=context,
            enforced_method=enforced_method,
            scheduling_decision=scheduling_decision,
            strategy_reasoning=strategy["reasoning"],
            policy_reasoning=policy_decision["reasoning"]
        )
        
        # Update statistics
        self.total_decisions += 1
        self.decisions_by_type[decision.decision_type.value] = \
            self.decisions_by_type.get(decision.decision_type.value, 0) + 1
        self.total_carbon_saved_kgco2e += decision.estimated_savings_kgco2e
        
        logger.info(
            f"✅ Decision: {decision.decision_type.value} | "
            f"Method: {decision.how} | "
            f"Carbon savings: {decision.estimated_savings_percent:.1f}%"
        )
        
        return decision
    
    def _check_budget(self, context: DecisionContext) -> Dict[str, Any]:
        """Check if carbon budget is available"""
        
        team_budget = self.carbon_ledger.get_team_budget(context.team)
        
        if not team_budget:
            # No budget set = unlimited
            return {"approved": True, "reason": "No budget constraints"}
        
        # Check if budget is sufficient
        if team_budget.remaining_kgco2e < context.carbon_estimate_kwh:
            return {
                "approved": False,
                "reason": f"Insufficient budget: {team_budget.remaining_kgco2e:.3f} kgCO2e remaining, "
                         f"{context.carbon_estimate_kwh:.3f} kgCO2e required"
            }
        
        # Check if budget is critically low (<5%)
        utilization = team_budget.used_kgco2e / team_budget.budget_kgco2e
        if utilization > 0.95:
            return {
                "approved": False,
                "reason": f"Budget critically low: {utilization:.1%} utilized"
            }
        
        return {
            "approved": True,
            "reason": f"Budget available: {team_budget.remaining_kgco2e:.3f} kgCO2e remaining"
        }
    
    def _classify_strategy(
        self,
        task: Dict[str, Any],
        context: DecisionContext
    ) -> Dict[str, Any]:
        """Classify optimal fine-tuning strategy"""
        
        recommendation = self.adaptation_classifier.classify(
            task_scope=task.get("task_scope", "single_task"),
            dataset_size=context.dataset_size,
            domain_shift=task.get("domain_shift", "moderate"),
            carbon_budget=context.carbon_budget_remaining,
            target_accuracy=task.get("target_accuracy", 0.90),
            model_size_params=context.model_params
        )
        
        return {
            "recommended": recommendation.strategy.value,
            "reasoning": recommendation.reasoning,
            "trainable_pct": recommendation.trainable_params_pct,
            "energy_multiplier": recommendation.expected_energy_multiplier
        }
    
    def _enforce_policy(
        self,
        context: DecisionContext,
        recommended_method: str
    ) -> Dict[str, Any]:
        """Enforce parameter-efficiency policy"""
        
        policy_context = {
            "carbon_budget": context.carbon_budget_remaining * 10,  # Rough estimate of total
            "carbon_remaining": context.carbon_budget_remaining,
            "dataset_size": context.dataset_size,
            "model_params": context.model_params
        }
        
        decision = self.policy_engine.enforce(
            requested_strategy=context.requested_method,
            recommended_strategy=recommended_method,
            policy_context=policy_context
        )
        
        return {
            "approved": decision.approved,
            "override_allowed": decision.override_allowed,
            "enforced_method": decision.enforced_strategy or recommended_method,
            "reasoning": decision.reasoning,
            "carbon_levy": decision.carbon_levy
        }
    
    async def _schedule_execution(
        self,
        task: Dict[str, Any],
        context: DecisionContext,
        method: str
    ) -> Dict[str, Any]:
        """Schedule task execution (WHEN and WHERE)"""
        
        # Update task with enforced method
        task_updated = task.copy()
        task_updated["fine_tuning_method"] = method
        
        # Use multi-objective scheduler
        scheduling_decision = await self.multi_obj_scheduler.schedule(task_updated)
        
        return {
            "execution_mode": scheduling_decision.chosen_option.mode.value,
            "scheduled_time": scheduling_decision.chosen_option.start_time,
            "node_id": scheduling_decision.chosen_option.node_id,
            "region": scheduling_decision.chosen_option.region,
            "carbon_kgco2e": scheduling_decision.chosen_option.carbon_kgco2e,
            "reasoning": scheduling_decision.reasoning
        }
    
    def _compile_decision(
        self,
        context: DecisionContext,
        enforced_method: str,
        scheduling_decision: Dict[str, Any],
        strategy_reasoning: str,
        policy_reasoning: str
    ) -> CoreDecision:
        """Compile complete decision"""
        
        # Determine decision type
        execution_mode = scheduling_decision["execution_mode"]
        
        if execution_mode == "deferred":
            decision_type = ExecutionDecision.DEFER_TO_OPTIMAL
        elif execution_mode == "green_routed":
            decision_type = ExecutionDecision.ROUTE_TO_GREEN_REGION
        elif enforced_method != context.requested_method:
            decision_type = ExecutionDecision.ENFORCE_EFFICIENT_METHOD
        else:
            decision_type = ExecutionDecision.EXECUTE_NOW
        
        # Calculate savings
        baseline_carbon = context.carbon_estimate_kwh
        actual_carbon = scheduling_decision["carbon_kgco2e"]
        carbon_saved = baseline_carbon - actual_carbon
        savings_percent = (carbon_saved / baseline_carbon * 100) if baseline_carbon > 0 else 0
        
        # Compile reasoning
        reasoning_parts = [
            f"Strategy: {strategy_reasoning}",
            f"Policy: {policy_reasoning}",
            f"Scheduling: {scheduling_decision['reasoning']}"
        ]
        reasoning = " | ".join(reasoning_parts)
        
        return CoreDecision(
            decision_type=decision_type,
            when=scheduling_decision["scheduled_time"],
            where=f"{scheduling_decision['region']}/{scheduling_decision['node_id']}",
            how=enforced_method,
            reasoning=reasoning,
            estimated_carbon_kgco2e=actual_carbon,
            estimated_savings_kgco2e=carbon_saved,
            estimated_savings_percent=savings_percent
        )
    
    def _create_blocked_decision(
        self,
        context: DecisionContext,
        reason: str
    ) -> CoreDecision:
        """Create decision for blocked task"""
        
        return CoreDecision(
            decision_type=ExecutionDecision.BLOCK_INSUFFICIENT_BUDGET,
            when=datetime.now(),
            where="N/A",
            how="N/A",
            reasoning=reason,
            estimated_carbon_kgco2e=0.0,
            estimated_savings_kgco2e=0.0,
            estimated_savings_percent=0.0
        )
    
    def get_statistics(self) -> Dict[str, Any]:
        """Get decision core statistics"""
        
        return {
            "total_decisions": self.total_decisions,
            "decisions_by_type": self.decisions_by_type,
            "total_carbon_saved_kgco2e": self.total_carbon_saved_kgco2e,
            "avg_carbon_saved_per_decision": (
                self.total_carbon_saved_kgco2e / self.total_decisions
                if self.total_decisions > 0 else 0
            ),
            "decision_type_distribution": {
                decision_type: count / self.total_decisions * 100
                for decision_type, count in self.decisions_by_type.items()
            } if self.total_decisions > 0 else {}
        }


if __name__ == "__main__":
    import asyncio
    from datetime import timedelta
    from carbon_ledger import CarbonLedgerService
    from adaptation_classifier import AdaptationStrategyClassifier
    from policy_engine import ParameterEfficiencyPolicyEngine, PolicyMode
    from forecasting_engine import create_forecaster
    from multi_objective_scheduler import MultiObjectiveScheduler
    from ray_cluster_manager import create_ray_cluster
    from task_carbon_profiler import TaskCarbonProfiler
    
    async def main():
        # Initialize all components
        forecaster = await create_forecaster(region="US-CA", train_days=30)
        cluster = create_ray_cluster(num_workers=4)
        profiler = TaskCarbonProfiler()
        
        scheduler = MultiObjectiveScheduler(
            carbon_forecaster=forecaster,
            task_profiler=profiler,
            ray_cluster=cluster
        )
        
        classifier = AdaptationStrategyClassifier()
        policy_engine = ParameterEfficiencyPolicyEngine(policy_mode=PolicyMode.MODERATE)
        ledger = CarbonLedgerService()
        
        # Create decision core
        core = CarbonAwareDecisionCore(
            carbon_forecaster=forecaster,
            multi_obj_scheduler=scheduler,
            adaptation_classifier=classifier,
            policy_engine=policy_engine,
            carbon_ledger=ledger
        )
        
        # Set team budget
        ledger.set_team_budget("nlp_research", "2026-03", 20.0)
        
        # Create decision context
        context = DecisionContext(
            task_id="bert_sentiment_test",
            task_type="fine_tuning",
            model_params=110_000_000,
            dataset_size=10_000,
            team="nlp_research",
            carbon_intensity_current=450.0,
            carbon_intensity_forecast_avg=200.0,
            carbon_budget_remaining=15.0,
            carbon_estimate_kwh=0.8,
            priority=0.7,
            deferrable=True,
            deadline=datetime.now() + timedelta(hours=48),
            requested_method="full_fine_tuning",
            recommended_method="lora"
        )
        
        # Create task
        task = {
            "task_id": "bert_sentiment_test",
            "model_name": "bert-base-uncased",
            "task_type": "fine_tuning",
            "dataset_size": 10_000,
            "num_epochs": 3,
            "batch_size": 32,
            "hardware": "V100",
            "region": "US-CA",
            "priority": 0.7,
            "deferrable": True,
            "deadline": datetime.now() + timedelta(hours=48)
        }
        
        # Make decision
        decision = await core.make_decision(task, context)
        
        print(f"\n{'='*80}")
        print(f"CARBON-AWARE DECISION CORE RESULT")
        print(f"{'='*80}")
        print(f"Decision Type: {decision.decision_type.value}")
        print(f"WHEN: {decision.when}")
        print(f"WHERE: {decision.where}")
        print(f"HOW: {decision.how}")
        print(f"Estimated Carbon: {decision.estimated_carbon_kgco2e:.4f} kgCO2e")
        print(f"Carbon Saved: {decision.estimated_savings_kgco2e:.4f} kgCO2e ({decision.estimated_savings_percent:.1f}%)")
        print(f"Reasoning: {decision.reasoning}")
        
        # Statistics
        stats = core.get_statistics()
        print(f"\n{'='*80}")
        print(f"DECISION CORE STATISTICS")
        print(f"{'='*80}")
        print(f"Total Decisions: {stats['total_decisions']}")
        print(f"Total Carbon Saved: {stats['total_carbon_saved_kgco2e']:.4f} kgCO2e")
        print(f"Decisions by Type: {stats['decisions_by_type']}")
        
        cluster.shutdown()
    
    asyncio.run(main())
