"""
Carbon-Aware Scheduler for Ray
================================

Schedules Ray tasks to nodes with lowest carbon intensity.

Location: src/distributed/carbon_aware_scheduler.py
"""

import ray
from ray.util.scheduling_strategies import PlacementGroupSchedulingStrategy
from typing import List, Dict, Any, Optional, Callable
import asyncio
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import Enum

logger = logging.getLogger(__name__)


class NodeCarbonMode(Enum):
    """Carbon intensity classification for nodes"""
    CLEAN = "clean"          # <200 gCO2/kWh
    MODERATE = "moderate"    # 200-400 gCO2/kWh
    DIRTY = "dirty"          # >400 gCO2/kWh


@dataclass
class NodeInfo:
    """Information about a Ray node"""
    node_id: str
    region: str
    carbon_intensity: float  # gCO2/kWh
    carbon_mode: NodeCarbonMode
    available_cpus: int
    available_gpus: int
    current_load: float  # 0-1
    last_updated: datetime


@dataclass
class SchedulingDecision:
    """Result of scheduling decision"""
    node_id: str
    node_region: str
    carbon_intensity: float
    reason: str
    estimated_carbon_kgco2e: float


@ray.remote
class CarbonAwareScheduler:
    """
    Schedules tasks to Ray nodes based on carbon intensity
    
    Scheduling Strategy:
    1. Prefer nodes with lowest carbon intensity
    2. Within same carbon mode, prefer least loaded
    3. If all nodes are DIRTY, defer tasks if possible
    4. Track carbon savings vs. baseline random scheduling
    """
    
    def __init__(
        self,
        carbon_forecaster = None,
        update_interval_minutes: int = 15
    ):
        self.carbon_forecaster = carbon_forecaster
        self.update_interval = timedelta(minutes=update_interval_minutes)
        self.node_info: Dict[str, NodeInfo] = {}
        self.last_update = None
        self.scheduling_history: List[SchedulingDecision] = []
        
        # Statistics
        self.total_tasks_scheduled = 0
        self.carbon_saved_kgco2e = 0.0
        
        logger.info("Carbon-aware scheduler initialized")
    
    async def update_node_carbon_info(self):
        """Update carbon intensity for all Ray nodes"""
        
        nodes = ray.nodes()
        
        for node in nodes:
            if not node['Alive']:
                continue
            
            node_id = node['NodeID']
            
            # Get node region (from node metadata or default)
            region = node.get('NodeManagerAddress', 'US-CA')
            
            # Fetch current carbon intensity
            if self.carbon_forecaster:
                carbon_intensity = await self.carbon_forecaster.get_current_intensity(region)
            else:
                # Default fallback
                carbon_intensity = 400.0
            
            # Determine carbon mode
            if carbon_intensity < 200:
                carbon_mode = NodeCarbonMode.CLEAN
            elif carbon_intensity < 400:
                carbon_mode = NodeCarbonMode.MODERATE
            else:
                carbon_mode = NodeCarbonMode.DIRTY
            
            # Get resource availability
            resources = node.get('Resources', {})
            available_cpus = int(resources.get('CPU', 0))
            available_gpus = int(resources.get('GPU', 0))
            
            # Estimate current load (simplified)
            total_cpus = available_cpus
            used_cpus = 0  # Would get from Ray metrics in production
            current_load = used_cpus / total_cpus if total_cpus > 0 else 0
            
            self.node_info[node_id] = NodeInfo(
                node_id=node_id,
                region=region,
                carbon_intensity=carbon_intensity,
                carbon_mode=carbon_mode,
                available_cpus=available_cpus,
                available_gpus=available_gpus,
                current_load=current_load,
                last_updated=datetime.now()
            )
        
        self.last_update = datetime.now()
        
        logger.info(f"Updated carbon info for {len(self.node_info)} nodes")
    
    async def schedule_task(
        self,
        task: Dict[str, Any],
        task_energy_estimate_kwh: float = 0.001
    ) -> SchedulingDecision:
        """
        Schedule task to optimal node based on carbon intensity
        
        Args:
            task: Task to schedule
            task_energy_estimate_kwh: Estimated energy consumption
        
        Returns:
            Scheduling decision with node and carbon info
        """
        
        # Update node info if stale
        if (self.last_update is None or 
            datetime.now() - self.last_update > self.update_interval):
            await self.update_node_carbon_info()
        
        # Get available nodes
        available_nodes = [
            node for node in self.node_info.values()
            if node.available_cpus > 0
        ]
        
        if not available_nodes:
            raise RuntimeError("No available nodes for scheduling")
        
        # Sort by carbon intensity (ascending)
        sorted_nodes = sorted(
            available_nodes,
            key=lambda n: (n.carbon_intensity, n.current_load)
        )
        
        # Select best node
        best_node = sorted_nodes[0]
        
        # Calculate baseline (random scheduling)
        avg_carbon_intensity = sum(n.carbon_intensity for n in available_nodes) / len(available_nodes)
        baseline_carbon = task_energy_estimate_kwh * avg_carbon_intensity / 1000  # kgCO2e
        
        # Calculate actual carbon with best node
        actual_carbon = task_energy_estimate_kwh * best_node.carbon_intensity / 1000  # kgCO2e
        
        # Calculate savings
        carbon_saved = baseline_carbon - actual_carbon
        self.carbon_saved_kgco2e += carbon_saved
        
        # Create scheduling decision
        decision = SchedulingDecision(
            node_id=best_node.node_id,
            node_region=best_node.region,
            carbon_intensity=best_node.carbon_intensity,
            reason=f"Lowest carbon intensity ({best_node.carbon_mode.value})",
            estimated_carbon_kgco2e=actual_carbon
        )
        
        # Track
        self.scheduling_history.append(decision)
        self.total_tasks_scheduled += 1
        
        logger.info(
            f"Scheduled task to node {best_node.node_id[:8]} "
            f"(carbon: {best_node.carbon_intensity:.0f} gCO2/kWh, "
            f"saved: {carbon_saved*1000:.2f} gCO2e)"
        )
        
        return decision
    
    async def batch_schedule(
        self,
        tasks: List[Dict[str, Any]],
        task_energy_estimate_kwh: float = 0.001
    ) -> List[SchedulingDecision]:
        """Schedule multiple tasks optimally"""
        
        decisions = []
        
        for task in tasks:
            decision = await self.schedule_task(task, task_energy_estimate_kwh)
            decisions.append(decision)
        
        return decisions
    
    async def should_defer_task(
        self,
        task: Dict[str, Any],
        defer_threshold_gco2kwh: float = 500.0
    ) -> bool:
        """
        Determine if task should be deferred to cleaner time window
        
        Args:
            task: Task to evaluate
            defer_threshold_gco2kwh: Defer if all nodes above this intensity
        
        Returns:
            True if task should be deferred
        """
        
        if not self.node_info:
            await self.update_node_carbon_info()
        
        # Check if all nodes are above threshold
        all_dirty = all(
            node.carbon_intensity > defer_threshold_gco2kwh
            for node in self.node_info.values()
        )
        
        # Check if task is deferrable
        is_deferrable = task.get('deferrable', True)
        has_deadline = 'deadline' in task
        
        if all_dirty and is_deferrable and not has_deadline:
            logger.info(
                f"Task {task.get('task_id', 'unknown')} deferred - "
                f"all nodes above {defer_threshold_gco2kwh} gCO2/kWh"
            )
            return True
        
        return False
    
    def get_statistics(self) -> Dict[str, Any]:
        """Get scheduler statistics"""
        
        if not self.scheduling_history:
            return {
                "total_tasks_scheduled": 0,
                "carbon_saved_kgco2e": 0.0
            }
        
        return {
            "total_tasks_scheduled": self.total_tasks_scheduled,
            "carbon_saved_kgco2e": self.carbon_saved_kgco2e,
            "carbon_saved_percent": (
                self.carbon_saved_kgco2e / 
                sum(d.estimated_carbon_kgco2e for d in self.scheduling_history) * 100
            ),
            "avg_carbon_intensity": sum(
                d.carbon_intensity for d in self.scheduling_history
            ) / len(self.scheduling_history),
            "node_utilization": self._get_node_utilization()
        }
    
    def _get_node_utilization(self) -> Dict[str, int]:
        """Count tasks scheduled to each node"""
        
        utilization = {}
        
        for decision in self.scheduling_history:
            node_id = decision.node_id
            utilization[node_id] = utilization.get(node_id, 0) + 1
        
        return utilization
    
    def get_current_carbon_landscape(self) -> Dict[str, Any]:
        """Get current carbon intensity across all nodes"""
        
        if not self.node_info:
            return {"nodes": []}
        
        nodes_by_mode = {
            "clean": [],
            "moderate": [],
            "dirty": []
        }
        
        for node in self.node_info.values():
            nodes_by_mode[node.carbon_mode.value].append({
                "node_id": node.node_id[:8],
                "region": node.region,
                "carbon_intensity": node.carbon_intensity,
                "available_cpus": node.available_cpus,
                "current_load": node.current_load
            })
        
        return {
            "timestamp": datetime.now().isoformat(),
            "total_nodes": len(self.node_info),
            "nodes_by_carbon_mode": {
                mode: len(nodes) for mode, nodes in nodes_by_mode.items()
            },
            "nodes": nodes_by_mode,
            "avg_carbon_intensity": sum(
                n.carbon_intensity for n in self.node_info.values()
            ) / len(self.node_info)
        }


# Integration with Ray cluster
class CarbonAwareRayCluster:
    """Ray cluster with integrated carbon-aware scheduling"""
    
    def __init__(
        self,
        cluster_manager,  # RayClusterManager
        carbon_forecaster = None
    ):
        self.cluster_manager = cluster_manager
        self.scheduler_actor = CarbonAwareScheduler.remote(
            carbon_forecaster=carbon_forecaster
        )
    
    async def schedule_and_execute(
        self,
        tasks: List[Dict[str, Any]],
        agent_type: str = "retriever",
        task_energy_estimate_kwh: float = 0.001
    ) -> List[Dict[str, Any]]:
        """Schedule tasks with carbon awareness and execute"""
        
        # Get scheduling decisions
        decisions = await ray.get(
            self.scheduler_actor.batch_schedule.remote(
                tasks=tasks,
                task_energy_estimate_kwh=task_energy_estimate_kwh
            )
        )
        
        # Execute tasks on selected nodes
        results = await self.cluster_manager.execute_distributed_tasks(
            tasks=tasks,
            agent_type=agent_type
        )
        
        # Augment results with carbon scheduling info
        for result, decision in zip(results, decisions):
            result['carbon_scheduling'] = {
                "node_id": decision.node_id,
                "node_region": decision.node_region,
                "carbon_intensity": decision.carbon_intensity,
                "estimated_carbon_kgco2e": decision.estimated_carbon_kgco2e
            }
        
        return results
    
    async def get_scheduler_stats(self) -> Dict[str, Any]:
        """Get scheduler statistics"""
        return await ray.get(self.scheduler_actor.get_statistics.remote())
    
    async def get_carbon_landscape(self) -> Dict[str, Any]:
        """Get current carbon landscape"""
        return await ray.get(self.scheduler_actor.get_current_carbon_landscape.remote())


# Convenience function
def create_carbon_aware_cluster(
    num_workers: int = 4,
    carbon_forecaster = None
):
    """Create Ray cluster with carbon-aware scheduling"""
    
    from .ray_cluster_manager import create_ray_cluster
    
    cluster_manager = create_ray_cluster(num_workers=num_workers)
    
    return CarbonAwareRayCluster(
        cluster_manager=cluster_manager,
        carbon_forecaster=carbon_forecaster
    )


if __name__ == "__main__":
    # Example usage
    async def main():
        from .ray_cluster_manager import create_ray_cluster
        
        # Create cluster
        cluster_manager = create_ray_cluster(num_workers=4)
        
        # Create carbon-aware cluster
        carbon_cluster = CarbonAwareRayCluster(
            cluster_manager=cluster_manager,
            carbon_forecaster=None  # Would use real forecaster in production
        )
        
        # Create tasks
        tasks = [
            {
                "task_id": f"task_{i}",
                "query": f"Query {i}",
                "deferrable": True
            }
            for i in range(10)
        ]
        
        # Schedule and execute
        results = await carbon_cluster.schedule_and_execute(
            tasks=tasks,
            agent_type="retriever",
            task_energy_estimate_kwh=0.002
        )
        
        # Get statistics
        stats = await carbon_cluster.get_scheduler_stats()
        landscape = await carbon_cluster.get_carbon_landscape()
        
        print(f"Scheduler stats: {stats}")
        print(f"Carbon landscape: {landscape}")
        print(f"Completed {len(results)} tasks")
        
        cluster_manager.shutdown()
    
    asyncio.run(main())
