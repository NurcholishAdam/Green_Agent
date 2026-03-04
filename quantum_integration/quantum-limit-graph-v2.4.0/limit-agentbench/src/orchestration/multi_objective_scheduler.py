"""
Multi-Objective Scheduler
==========================

Optimizes task scheduling across 4 competing objectives:
- Carbon intensity (minimize)
- Energy consumption (minimize)
- Cost (minimize)
- Performance/latency (maximize)

Location: src/orchestration/multi_objective_scheduler.py
"""

from typing import Dict, Any, Optional, List, Tuple
from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import Enum
import numpy as np
import logging

logger = logging.getLogger(__name__)


class ExecutionMode(Enum):
    """Execution modes for tasks"""
    IMMEDIATE = "immediate"        # Execute now
    DEFERRED = "deferred"          # Execute in optimal window
    REALLOCATED = "reallocated"    # Execute on different node
    GREEN_ROUTED = "green_routed"  # Route to low-carbon region


@dataclass
class SchedulingOption:
    """A scheduling option with associated costs"""
    mode: ExecutionMode
    node_id: str
    region: str
    start_time: datetime
    carbon_kgco2e: float
    energy_kwh: float
    cost_usd: float
    latency_hours: float
    score: float  # Multi-objective score


@dataclass
class SchedulingDecision:
    """Final scheduling decision"""
    task_id: str
    chosen_option: SchedulingOption
    alternatives: List[SchedulingOption]
    reasoning: str
    pareto_efficient: bool


class MultiObjectiveScheduler:
    """
    Schedules tasks to minimize carbon + energy + cost - performance
    
    Objective Function:
    minimize( α·carbon + β·energy + γ·cost - δ·performance_priority )
    
    Where:
    - α, β, γ, δ are weight parameters (sum to 1.0)
    - carbon: expected CO2 emissions (kgCO2e)
    - energy: expected energy consumption (kWh)
    - cost: monetary cost ($)
    - performance_priority: user-specified urgency (0-1)
    """
    
    def __init__(
        self,
        carbon_forecaster,
        task_profiler,
        ray_cluster,
        alpha: float = 0.5,  # Carbon weight
        beta: float = 0.3,   # Energy weight
        gamma: float = 0.1,  # Cost weight
        delta: float = 0.1   # Performance weight
    ):
        self.carbon_forecaster = carbon_forecaster
        self.task_profiler = task_profiler
        self.ray_cluster = ray_cluster
        
        # Objective weights (should sum to ~1.0)
        self.alpha = alpha
        self.beta = beta
        self.gamma = gamma
        self.delta = delta
        
        # Cost parameters
        self.electricity_cost_per_kwh = {
            "US-CA": 0.20,
            "US-NY": 0.18,
            "EU-DE": 0.25,
            "EU-FR": 0.22
        }
        
        # Scheduling history
        self.scheduling_history: List[SchedulingDecision] = []
        
        logger.info(
            f"Multi-objective scheduler initialized "
            f"(α={alpha}, β={beta}, γ={gamma}, δ={delta})"
        )
    
    async def schedule(
        self,
        task: Dict[str, Any],
        consider_deferral: bool = True,
        consider_reallocation: bool = True
    ) -> SchedulingDecision:
        """
        Find optimal scheduling for task
        
        Args:
            task: Task specification with priority, deadline, etc.
            consider_deferral: Allow deferring to optimal time window
            consider_reallocation: Allow allocating to different region
        
        Returns:
            SchedulingDecision with chosen option and alternatives
        """
        
        # Generate scheduling options
        options = await self._generate_options(
            task=task,
            consider_deferral=consider_deferral,
            consider_reallocation=consider_reallocation
        )
        
        if not options:
            raise ValueError("No feasible scheduling options found")
        
        # Calculate multi-objective scores for each option
        scored_options = self._calculate_scores(task, options)
        
        # Sort by score (lower is better)
        scored_options.sort(key=lambda opt: opt.score)
        
        # Select best option
        best_option = scored_options[0]
        
        # Check if Pareto efficient (not dominated by any other option)
        pareto_efficient = self._is_pareto_efficient(best_option, scored_options[1:])
        
        # Generate reasoning
        reasoning = self._generate_reasoning(best_option, scored_options)
        
        decision = SchedulingDecision(
            task_id=task.get("task_id", "unknown"),
            chosen_option=best_option,
            alternatives=scored_options[1:5],  # Top 5 alternatives
            reasoning=reasoning,
            pareto_efficient=pareto_efficient
        )
        
        self.scheduling_history.append(decision)
        
        logger.info(
            f"Scheduled task {task.get('task_id')} as {best_option.mode.value} "
            f"(score: {best_option.score:.3f}, carbon: {best_option.carbon_kgco2e:.4f} kgCO2e)"
        )
        
        return decision
    
    async def _generate_options(
        self,
        task: Dict[str, Any],
        consider_deferral: bool,
        consider_reallocation: bool
    ) -> List[SchedulingOption]:
        """Generate feasible scheduling options"""
        
        options = []
        
        # Get current carbon intensity
        current_region = task.get("region", "US-CA")
        current_intensity = await self.carbon_forecaster.get_current_intensity(current_region)
        
        # Option 1: Execute immediately on current node
        immediate_option = await self._create_immediate_option(
            task=task,
            region=current_region,
            carbon_intensity=current_intensity
        )
        options.append(immediate_option)
        
        # Option 2: Defer to optimal time window (if allowed and has deadline)
        if consider_deferral and task.get("deferrable", True):
            deadline = task.get("deadline")
            if deadline:
                deferred_option = await self._create_deferred_option(
                    task=task,
                    region=current_region,
                    deadline=deadline
                )
                if deferred_option:
                    options.append(deferred_option)
        
        # Option 3: Reallocate to different node in same region
        if consider_reallocation:
            # Get cluster nodes
            cluster_info = self.ray_cluster.get_cluster_info()
            available_nodes = cluster_info.get("nodes", [])
            
            for node in available_nodes[:3]:  # Consider top 3 nodes
                if node["node_id"] != immediate_option.node_id:
                    realloc_option = await self._create_reallocated_option(
                        task=task,
                        node=node,
                        region=current_region,
                        carbon_intensity=current_intensity
                    )
                    options.append(realloc_option)
        
        # Option 4: Route to different region (green routing)
        if consider_reallocation:
            alternative_regions = ["US-CA", "US-NY", "EU-DE", "EU-FR"]
            alternative_regions = [r for r in alternative_regions if r != current_region]
            
            for region in alternative_regions[:2]:  # Consider top 2 regions
                green_option = await self._create_green_routed_option(
                    task=task,
                    target_region=region
                )
                if green_option:
                    options.append(green_option)
        
        return options
    
    async def _create_immediate_option(
        self,
        task: Dict[str, Any],
        region: str,
        carbon_intensity: float
    ) -> SchedulingOption:
        """Create option for immediate execution"""
        
        # Estimate energy and carbon
        estimate = await self.task_profiler.estimate_energy(
            task=task,
            carbon_intensity=carbon_intensity
        )
        
        # Calculate cost
        electricity_cost = self.electricity_cost_per_kwh.get(region, 0.20)
        cost_usd = estimate.expected_energy_kwh * electricity_cost
        
        return SchedulingOption(
            mode=ExecutionMode.IMMEDIATE,
            node_id="current_node",
            region=region,
            start_time=datetime.now(),
            carbon_kgco2e=estimate.expected_carbon_kgco2e,
            energy_kwh=estimate.expected_energy_kwh,
            cost_usd=cost_usd,
            latency_hours=0.0,  # No delay
            score=0.0  # Will be calculated later
        )
    
    async def _create_deferred_option(
        self,
        task: Dict[str, Any],
        region: str,
        deadline: datetime
    ) -> Optional[SchedulingOption]:
        """Create option for deferred execution"""
        
        try:
            # Find optimal window
            duration_hours = task.get("estimated_duration_hours", 1.0)
            optimal_window = await self.carbon_forecaster.find_optimal_execution_window(
                duration_hours=duration_hours,
                deadline=deadline
            )
            
            # Estimate energy and carbon at optimal time
            estimate = await self.task_profiler.estimate_energy(
                task=task,
                carbon_intensity=optimal_window.avg_intensity
            )
            
            # Calculate cost
            electricity_cost = self.electricity_cost_per_kwh.get(region, 0.20)
            cost_usd = estimate.expected_energy_kwh * electricity_cost
            
            # Calculate latency
            latency_hours = (optimal_window.start_time - datetime.now()).total_seconds() / 3600
            
            return SchedulingOption(
                mode=ExecutionMode.DEFERRED,
                node_id="current_node",
                region=region,
                start_time=optimal_window.start_time,
                carbon_kgco2e=estimate.expected_carbon_kgco2e,
                energy_kwh=estimate.expected_energy_kwh,
                cost_usd=cost_usd,
                latency_hours=latency_hours,
                score=0.0
            )
        except Exception as e:
            logger.warning(f"Could not create deferred option: {e}")
            return None
    
    async def _create_reallocated_option(
        self,
        task: Dict[str, Any],
        node: Dict[str, Any],
        region: str,
        carbon_intensity: float
    ) -> SchedulingOption:
        """Create option for reallocated execution"""
        
        # Estimate energy (might differ by node hardware)
        estimate = await self.task_profiler.estimate_energy(
            task=task,
            carbon_intensity=carbon_intensity
        )
        
        # Adjust for node load (higher load = slightly higher energy)
        node_load = node.get("current_load", 0.5)
        adjusted_energy = estimate.expected_energy_kwh * (1.0 + node_load * 0.1)
        adjusted_carbon = adjusted_energy * carbon_intensity / 1000
        
        # Calculate cost
        electricity_cost = self.electricity_cost_per_kwh.get(region, 0.20)
        cost_usd = adjusted_energy * electricity_cost
        
        return SchedulingOption(
            mode=ExecutionMode.REALLOCATED,
            node_id=node["node_id"],
            region=region,
            start_time=datetime.now(),
            carbon_kgco2e=adjusted_carbon,
            energy_kwh=adjusted_energy,
            cost_usd=cost_usd,
            latency_hours=0.1,  # Small overhead for data transfer
            score=0.0
        )
    
    async def _create_green_routed_option(
        self,
        task: Dict[str, Any],
        target_region: str
    ) -> Optional[SchedulingOption]:
        """Create option for green region routing"""
        
        try:
            # Get carbon intensity in target region
            target_intensity = await self.carbon_forecaster.get_current_intensity(target_region)
            
            # Estimate energy and carbon
            estimate = await self.task_profiler.estimate_energy(
                task=task,
                carbon_intensity=target_intensity
            )
            
            # Calculate cost (may be different in target region)
            electricity_cost = self.electricity_cost_per_kwh.get(target_region, 0.20)
            cost_usd = estimate.expected_energy_kwh * electricity_cost
            
            # Add data transfer cost and latency
            data_transfer_overhead = 0.05  # 5% overhead
            cost_usd *= (1.0 + data_transfer_overhead)
            
            return SchedulingOption(
                mode=ExecutionMode.GREEN_ROUTED,
                node_id=f"{target_region}_node",
                region=target_region,
                start_time=datetime.now(),
                carbon_kgco2e=estimate.expected_carbon_kgco2e,
                energy_kwh=estimate.expected_energy_kwh,
                cost_usd=cost_usd,
                latency_hours=0.2,  # Overhead for cross-region routing
                score=0.0
            )
        except Exception as e:
            logger.warning(f"Could not create green routed option: {e}")
            return None
    
    def _calculate_scores(
        self,
        task: Dict[str, Any],
        options: List[SchedulingOption]
    ) -> List[SchedulingOption]:
        """Calculate multi-objective scores for options"""
        
        # Get task priority (0-1, higher = more urgent)
        priority = task.get("priority", 0.5)
        
        # Normalize values to [0, 1] range for fair comparison
        carbon_values = [opt.carbon_kgco2e for opt in options]
        energy_values = [opt.energy_kwh for opt in options]
        cost_values = [opt.cost_usd for opt in options]
        latency_values = [opt.latency_hours for opt in options]
        
        max_carbon = max(carbon_values) if carbon_values else 1.0
        max_energy = max(energy_values) if energy_values else 1.0
        max_cost = max(cost_values) if cost_values else 1.0
        max_latency = max(latency_values) if latency_values else 1.0
        
        # Calculate scores for each option
        for option in options:
            normalized_carbon = option.carbon_kgco2e / max_carbon if max_carbon > 0 else 0
            normalized_energy = option.energy_kwh / max_energy if max_energy > 0 else 0
            normalized_cost = option.cost_usd / max_cost if max_cost > 0 else 0
            normalized_latency = option.latency_hours / max_latency if max_latency > 0 else 0
            
            # Multi-objective score (lower is better)
            option.score = (
                self.alpha * normalized_carbon +
                self.beta * normalized_energy +
                self.gamma * normalized_cost +
                self.delta * normalized_latency * (1.0 - priority)  # Latency matters less for low priority
            )
        
        return options
    
    def _is_pareto_efficient(
        self,
        option: SchedulingOption,
        alternatives: List[SchedulingOption]
    ) -> bool:
        """Check if option is Pareto efficient (not dominated)"""
        
        for alt in alternatives:
            # Check if alternative dominates this option
            # (better or equal on all objectives, strictly better on at least one)
            if (alt.carbon_kgco2e <= option.carbon_kgco2e and
                alt.energy_kwh <= option.energy_kwh and
                alt.cost_usd <= option.cost_usd and
                alt.latency_hours <= option.latency_hours and
                (alt.carbon_kgco2e < option.carbon_kgco2e or
                 alt.energy_kwh < option.energy_kwh or
                 alt.cost_usd < option.cost_usd or
                 alt.latency_hours < option.latency_hours)):
                return False  # Dominated by alternative
        
        return True
    
    def _generate_reasoning(
        self,
        chosen: SchedulingOption,
        alternatives: List[SchedulingOption]
    ) -> str:
        """Generate human-readable reasoning for decision"""
        
        immediate = next((opt for opt in alternatives if opt.mode == ExecutionMode.IMMEDIATE), None)
        
        if immediate and chosen != immediate:
            carbon_savings = immediate.carbon_kgco2e - chosen.carbon_kgco2e
            carbon_savings_pct = (carbon_savings / immediate.carbon_kgco2e * 100) if immediate.carbon_kgco2e > 0 else 0
            
            reasoning = (
                f"Chose {chosen.mode.value} execution "
                f"saving {carbon_savings:.4f} kgCO2e ({carbon_savings_pct:.1f}%) "
                f"vs immediate execution. "
            )
            
            if chosen.latency_hours > 0:
                reasoning += f"Delay: {chosen.latency_hours:.1f} hours. "
            
            if chosen.region != immediate.region:
                reasoning += f"Routed to {chosen.region} (cleaner grid). "
            
            return reasoning
        else:
            return f"Immediate execution optimal (score: {chosen.score:.3f})"
    
    def get_statistics(self) -> Dict[str, Any]:
        """Get scheduling statistics"""
        
        if not self.scheduling_history:
            return {"num_decisions": 0}
        
        mode_counts = {}
        total_carbon = 0.0
        total_energy = 0.0
        pareto_count = 0
        
        for decision in self.scheduling_history:
            mode = decision.chosen_option.mode.value
            mode_counts[mode] = mode_counts.get(mode, 0) + 1
            total_carbon += decision.chosen_option.carbon_kgco2e
            total_energy += decision.chosen_option.energy_kwh
            if decision.pareto_efficient:
                pareto_count += 1
        
        return {
            "num_decisions": len(self.scheduling_history),
            "mode_distribution": mode_counts,
            "total_carbon_kgco2e": total_carbon,
            "total_energy_kwh": total_energy,
            "avg_carbon_per_task": total_carbon / len(self.scheduling_history),
            "pareto_efficient_rate": pareto_count / len(self.scheduling_history)
        }


if __name__ == "__main__":
    import asyncio
    from task_carbon_profiler import TaskCarbonProfiler
    from forecasting_engine import create_forecaster
    from ray_cluster_manager import create_ray_cluster
    
    async def main():
        # Create dependencies
        profiler = TaskCarbonProfiler()
        forecaster = await create_forecaster(region="US-CA", train_days=30)
        cluster = create_ray_cluster(num_workers=4)
        
        # Create scheduler
        scheduler = MultiObjectiveScheduler(
            carbon_forecaster=forecaster,
            task_profiler=profiler,
            ray_cluster=cluster,
            alpha=0.5, beta=0.3, gamma=0.1, delta=0.1
        )
        
        # Example task
        task = {
            "task_id": "bert_sentiment",
            "model_name": "bert-base-uncased",
            "dataset_size": 10_000,
            "num_epochs": 3,
            "batch_size": 32,
            "hardware": "V100",
            "region": "US-CA",
            "priority": 0.7,
            "deferrable": True,
            "deadline": datetime.now() + timedelta(hours=48)
        }
        
        # Schedule task
        decision = await scheduler.schedule(task)
        
        print(f"Task: {decision.task_id}")
        print(f"Chosen: {decision.chosen_option.mode.value}")
        print(f"Carbon: {decision.chosen_option.carbon_kgco2e:.4f} kgCO2e")
        print(f"Energy: {decision.chosen_option.energy_kwh:.4f} kWh")
        print(f"Cost: ${decision.chosen_option.cost_usd:.2f}")
        print(f"Latency: {decision.chosen_option.latency_hours:.1f}h")
        print(f"Reasoning: {decision.reasoning}")
        
        cluster.shutdown()
    
    asyncio.run(main())
