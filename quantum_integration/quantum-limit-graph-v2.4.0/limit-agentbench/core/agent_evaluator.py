# -*- coding: utf-8 -*-
"""
Agent Evaluator
Unified evaluation framework for agents across different frameworks
"""

from typing import Dict, Any, List, Optional
import logging
from .agentbench_adapter import AgentBenchAdapter
from .green_metrics import GreenMetricsTracker

logger = logging.getLogger(__name__)


class AgentEvaluator:
    """
    Unified agent evaluation framework.
    
    Evaluates agents from different frameworks with consistent metrics
    including green metrics (energy, carbon).
    """
    
    def __init__(
        self,
        grid_region: str = "GLOBAL",
        hardware_profile: str = "default",
        track_green_metrics: bool = True
    ):
        """
        Initialize agent evaluator.
        
        Args:
            grid_region: Grid region for carbon calculations
            hardware_profile: Hardware profile for power estimation
            track_green_metrics: Whether to track energy/carbon
        """
        self.grid_region = grid_region
        self.hardware_profile = hardware_profile
        self.track_green_metrics = track_green_metrics
        self.adapter = AgentBenchAdapter()
        
        logger.info(f"Initialized AgentEvaluator with green_metrics={track_green_metrics}")
    
    def evaluate(
        self,
        agent: Any,
        task: Dict[str, Any],
        backend: Optional[str] = None,
        rank: Optional[int] = None
    ) -> Dict[str, Any]:
        """
        Evaluate an agent on a task.
        
        Args:
            agent: Agent instance to evaluate
            task: Task definition
            backend: Quantum backend (if applicable)
            rank: NSN rank (if applicable)
            
        Returns:
            Evaluation result with metrics
        """
        logger.info(f"Evaluating agent on task {task.get('task_id', 'unknown')}")
        
        result = self.adapter.evaluate_agent(
            agent=agent,
            task=task,
            track_energy=self.track_green_metrics,
            track_carbon=self.track_green_metrics,
            backend=backend,
            rank=rank
        )
        
        # Calculate sustainability index if metrics available
        if self.track_green_metrics and result.get("metrics"):
            metrics = result["metrics"]
            if all(k in metrics for k in ["accuracy", "energy_kwh", "carbon_co2e_kg"]):
                tracker = GreenMetricsTracker()
                sustainability_index = tracker.get_sustainability_index(
                    accuracy=metrics["accuracy"],
                    energy_kwh=metrics["energy_kwh"],
                    carbon_co2e_kg=metrics["carbon_co2e_kg"]
                )
                result["metrics"]["sustainability_index"] = sustainability_index
        
        return result
    
    def evaluate_suite(
        self,
        agent: Any,
        tasks: List[Dict[str, Any]],
        backend: Optional[str] = None,
        rank: Optional[int] = None
    ) -> Dict[str, Any]:
        """
        Evaluate an agent on a suite of tasks.
        
        Args:
            agent: Agent instance to evaluate
            tasks: List of task definitions
            backend: Quantum backend (if applicable)
            rank: NSN rank (if applicable)
            
        Returns:
            Aggregated evaluation results
        """
        logger.info(f"Evaluating agent on {len(tasks)} tasks")
        
        results = []
        for task in tasks:
            result = self.evaluate(agent, task, backend, rank)
            results.append(result)
        
        # Aggregate metrics
        aggregated = self._aggregate_results(results)
        
        return {
            "agent_name": getattr(agent, 'name', agent.__class__.__name__),
            "framework": getattr(agent, 'framework', 'unknown'),
            "num_tasks": len(tasks),
            "results": results,
            "aggregated_metrics": aggregated
        }
    
    def _aggregate_results(self, results: List[Dict[str, Any]]) -> Dict[str, float]:
        """Aggregate metrics across multiple results."""
        if not results:
            return {}
        
        # Collect all metric values
        metric_values = {}
        for result in results:
            if not result.get("success"):
                continue
            metrics = result.get("metrics", {})
            for key, value in metrics.items():
                if isinstance(value, (int, float)):
                    if key not in metric_values:
                        metric_values[key] = []
                    metric_values[key].append(value)
        
        # Calculate averages
        aggregated = {}
        for key, values in metric_values.items():
            if values:
                aggregated[f"avg_{key}"] = sum(values) / len(values)
                aggregated[f"min_{key}"] = min(values)
                aggregated[f"max_{key}"] = max(values)
        
        # Calculate success rate
        success_count = sum(1 for r in results if r.get("success"))
        aggregated["success_rate"] = success_count / len(results)
        
        return aggregated
    
    def compare_agents(
        self,
        agents: List[Any],
        tasks: List[Dict[str, Any]],
        sort_by: str = "sustainability_index"
    ) -> Dict[str, Any]:
        """
        Compare multiple agents on the same tasks.
        
        Args:
            agents: List of agent instances
            tasks: List of task definitions
            sort_by: Metric to sort by
            
        Returns:
            Comparison results with rankings
        """
        logger.info(f"Comparing {len(agents)} agents on {len(tasks)} tasks")
        
        agent_results = []
        for agent in agents:
            suite_result = self.evaluate_suite(agent, tasks)
            agent_results.append(suite_result)
        
        # Sort by specified metric
        if sort_by in agent_results[0].get("aggregated_metrics", {}):
            agent_results.sort(
                key=lambda x: x["aggregated_metrics"].get(f"avg_{sort_by}", 0),
                reverse=True
            )
        
        return {
            "num_agents": len(agents),
            "num_tasks": len(tasks),
            "sort_by": sort_by,
            "rankings": agent_results
        }
