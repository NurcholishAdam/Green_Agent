"""
Ray Cluster Manager for Green Agent
====================================

Manages distributed execution of AI agents across Ray cluster with carbon awareness.

Location: src/distributed/ray_cluster_manager.py
"""

import ray
from ray.util.actor_pool import ActorPool
from ray import serve
from typing import List, Dict, Any, Optional, Callable
import asyncio
import logging
from dataclasses import dataclass
from datetime import datetime
import psutil
import numpy as np

logger = logging.getLogger(__name__)


@dataclass
class ClusterConfig:
    """Ray cluster configuration"""
    address: Optional[str] = None  # None = local, "auto" = connect to existing
    num_cpus: Optional[int] = None  # None = auto-detect
    num_gpus: Optional[int] = None  # None = auto-detect
    object_store_memory: Optional[int] = None  # Bytes, None = auto
    dashboard_host: str = "0.0.0.0"
    dashboard_port: int = 8265
    include_dashboard: bool = True
    log_to_driver: bool = True


@dataclass
class TaskMetrics:
    """Metrics for a completed task"""
    task_id: str
    agent_type: str
    execution_time: float  # seconds
    energy_kwh: float
    carbon_kgco2e: float
    result_quality: float  # 0-1 score
    worker_node_id: str
    timestamp: datetime


@ray.remote(num_cpus=1, num_gpus=0)
class GreenAgentActor:
    """Base Ray actor for Green Agent tasks"""
    
    def __init__(self, agent_type: str, carbon_budget: float):
        self.agent_type = agent_type
        self.carbon_budget = carbon_budget
        self.tasks_completed = 0
        self.total_energy_kwh = 0.0
        self.total_carbon_kgco2e = 0.0
        
        # Initialize energy tracker
        from ..utils.energy_tracker import EnergyTracker
        self.energy_tracker = EnergyTracker()
        
        logger.info(f"Initialized {agent_type} actor with carbon budget {carbon_budget} kgCO2e")
    
    async def execute(self, task: Dict[str, Any]) -> Dict[str, Any]:
        """Execute task with energy tracking"""
        
        self.energy_tracker.start()
        start_time = datetime.now()
        
        try:
            # Execute based on agent type
            if self.agent_type == "retriever":
                result = await self._retrieve(task)
            elif self.agent_type == "reasoner":
                result = await self._reason(task)
            elif self.agent_type == "critic":
                result = await self._critique(task)
            elif self.agent_type == "synthesizer":
                result = await self._synthesize(task)
            else:
                raise ValueError(f"Unknown agent type: {self.agent_type}")
            
            # Track metrics
            energy_kwh = self.energy_tracker.stop()
            execution_time = (datetime.now() - start_time).total_seconds()
            
            # Get grid carbon intensity (simplified - in production, fetch from API)
            carbon_intensity = await self._get_carbon_intensity()
            carbon_kgco2e = energy_kwh * carbon_intensity
            
            # Update totals
            self.tasks_completed += 1
            self.total_energy_kwh += energy_kwh
            self.total_carbon_kgco2e += carbon_kgco2e
            
            # Check carbon budget
            if self.total_carbon_kgco2e > self.carbon_budget:
                logger.warning(
                    f"Actor {self.agent_type} exceeded carbon budget: "
                    f"{self.total_carbon_kgco2e:.4f} > {self.carbon_budget:.4f} kgCO2e"
                )
            
            return {
                "result": result,
                "metrics": {
                    "energy_kwh": energy_kwh,
                    "carbon_kgco2e": carbon_kgco2e,
                    "execution_time": execution_time,
                    "worker_id": ray.get_runtime_context().worker.worker_id
                }
            }
        
        except Exception as e:
            logger.error(f"Task execution failed: {e}")
            self.energy_tracker.stop()
            raise
    
    async def _retrieve(self, task: Dict[str, Any]) -> Any:
        """Retrieval agent logic"""
        from ..retrieval.vimrag_integration import VimRAGIntegration
        
        vimrag = VimRAGIntegration()
        result = await vimrag.retrieve(
            query=task['query'],
            top_k=task.get('top_k', 10),
            pipeline_mode=task.get('pipeline_mode', 'BALANCED')
        )
        
        return result
    
    async def _reason(self, task: Dict[str, Any]) -> Any:
        """Reasoning agent logic"""
        # Placeholder - integrate with your LLM reasoning logic
        context = task.get('context', '')
        query = task['query']
        
        # Simulate reasoning (replace with actual LLM call)
        await asyncio.sleep(0.5)  # Simulate processing
        
        return {
            "reasoning": f"Based on {context}, the answer to {query} is...",
            "confidence": 0.85
        }
    
    async def _critique(self, task: Dict[str, Any]) -> Any:
        """Critic agent logic"""
        responses = task.get('responses', [])
        
        # Simulate critique
        await asyncio.sleep(0.3)
        
        return {
            "best_response_idx": 0,
            "critique": "Response 0 is most accurate and comprehensive",
            "confidence": 0.90
        }
    
    async def _synthesize(self, task: Dict[str, Any]) -> Any:
        """Synthesizer agent logic"""
        results = task.get('results', [])
        
        # Simulate synthesis
        await asyncio.sleep(0.4)
        
        return {
            "synthesized_response": f"Combining {len(results)} results...",
            "confidence": 0.88
        }
    
    async def _get_carbon_intensity(self) -> float:
        """Get current grid carbon intensity (g CO2/kWh)"""
        # In production, fetch from carbon controller
        # For now, return a default value
        return 400.0  # gCO2/kWh
    
    def get_stats(self) -> Dict[str, Any]:
        """Get actor statistics"""
        return {
            "agent_type": self.agent_type,
            "tasks_completed": self.tasks_completed,
            "total_energy_kwh": self.total_energy_kwh,
            "total_carbon_kgco2e": self.total_carbon_kgco2e,
            "avg_energy_per_task": (
                self.total_energy_kwh / self.tasks_completed 
                if self.tasks_completed > 0 else 0
            )
        }


class RayClusterManager:
    """Manages Ray cluster for distributed Green Agent execution"""
    
    def __init__(self, config: Optional[ClusterConfig] = None):
        self.config = config or ClusterConfig()
        self.cluster_initialized = False
        self.agent_pools: Dict[str, ActorPool] = {}
        self.task_metrics: List[TaskMetrics] = []
        
    def initialize_cluster(self):
        """Initialize Ray cluster"""
        
        if self.cluster_initialized:
            logger.warning("Cluster already initialized")
            return
        
        ray_kwargs = {
            "address": self.config.address,
            "include_dashboard": self.config.include_dashboard,
            "log_to_driver": self.config.log_to_driver,
            "_temp_dir": "/tmp/ray",
        }
        
        if self.config.dashboard_host and self.config.include_dashboard:
            ray_kwargs["dashboard_host"] = self.config.dashboard_host
            ray_kwargs["dashboard_port"] = self.config.dashboard_port
        
        if self.config.num_cpus:
            ray_kwargs["num_cpus"] = self.config.num_cpus
        
        if self.config.num_gpus:
            ray_kwargs["num_gpus"] = self.config.num_gpus
        
        if self.config.object_store_memory:
            ray_kwargs["object_store_memory"] = self.config.object_store_memory
        
        ray.init(**ray_kwargs)
        
        self.cluster_initialized = True
        
        logger.info(f"Ray cluster initialized: {self.get_cluster_info()}")
    
    def create_agent_pool(
        self,
        agent_type: str,
        num_agents: int = 4,
        carbon_budget_per_agent: float = 0.01
    ) -> ActorPool:
        """Create pool of reusable agents"""
        
        if not self.cluster_initialized:
            self.initialize_cluster()
        
        # Create agent actors
        agents = [
            GreenAgentActor.remote(
                agent_type=agent_type,
                carbon_budget=carbon_budget_per_agent
            )
            for _ in range(num_agents)
        ]
        
        pool = ActorPool(agents)
        self.agent_pools[agent_type] = pool
        
        logger.info(f"Created agent pool '{agent_type}' with {num_agents} agents")
        
        return pool
    
    async def execute_distributed_tasks(
        self,
        tasks: List[Dict[str, Any]],
        agent_type: str = "retriever",
        max_parallel: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """Execute tasks in parallel across Ray cluster"""
        
        # Get or create agent pool
        if agent_type not in self.agent_pools:
            self.create_agent_pool(agent_type)
        
        pool = self.agent_pools[agent_type]
        
        # Submit tasks
        futures = []
        for task in tasks:
            future = pool.submit(
                lambda actor, t: actor.execute.remote(t),
                task
            )
            futures.append(future)
        
        # Gather results with optional parallelism limit
        if max_parallel:
            # Process in batches
            results = []
            for i in range(0, len(futures), max_parallel):
                batch = futures[i:i+max_parallel]
                batch_results = await asyncio.gather(*[
                    asyncio.wrap_future(ray.get(f)) for f in batch
                ])
                results.extend(batch_results)
        else:
            # Process all in parallel
            results = await asyncio.gather(*[
                asyncio.wrap_future(ray.get(f)) for f in futures
            ])
        
        # Track metrics
        for task, result in zip(tasks, results):
            if "metrics" in result:
                metrics = TaskMetrics(
                    task_id=task.get('task_id', 'unknown'),
                    agent_type=agent_type,
                    execution_time=result['metrics']['execution_time'],
                    energy_kwh=result['metrics']['energy_kwh'],
                    carbon_kgco2e=result['metrics']['carbon_kgco2e'],
                    result_quality=result.get('quality', 1.0),
                    worker_node_id=result['metrics']['worker_id'],
                    timestamp=datetime.now()
                )
                self.task_metrics.append(metrics)
        
        return results
    
    def get_cluster_info(self) -> Dict[str, Any]:
        """Get current cluster information"""
        
        if not self.cluster_initialized:
            return {"status": "not_initialized"}
        
        nodes = ray.nodes()
        resources = ray.cluster_resources()
        
        return {
            "status": "active",
            "num_nodes": len(nodes),
            "total_cpus": resources.get('CPU', 0),
            "total_gpus": resources.get('GPU', 0),
            "object_store_memory_gb": resources.get('object_store_memory', 0) / (1024**3),
            "dashboard_url": f"http://{self.config.dashboard_host}:{self.config.dashboard_port}",
            "nodes": [
                {
                    "node_id": node['NodeID'],
                    "alive": node['Alive'],
                    "resources": node.get('Resources', {})
                }
                for node in nodes
            ]
        }
    
    def get_agent_pool_stats(self) -> Dict[str, Dict[str, Any]]:
        """Get statistics for all agent pools"""
        
        stats = {}
        
        for agent_type, pool in self.agent_pools.items():
            # Get stats from each actor in pool
            actor_stats = ray.get([
                actor.get_stats.remote()
                for actor in pool._idle_actors + pool._busy_actors
            ])
            
            stats[agent_type] = {
                "num_actors": len(actor_stats),
                "total_tasks": sum(s['tasks_completed'] for s in actor_stats),
                "total_energy_kwh": sum(s['total_energy_kwh'] for s in actor_stats),
                "total_carbon_kgco2e": sum(s['total_carbon_kgco2e'] for s in actor_stats),
                "avg_energy_per_task": np.mean([
                    s['avg_energy_per_task'] for s in actor_stats
                ])
            }
        
        return stats
    
    def get_task_metrics_summary(self) -> Dict[str, Any]:
        """Get summary of all task metrics"""
        
        if not self.task_metrics:
            return {"num_tasks": 0}
        
        total_energy = sum(m.energy_kwh for m in self.task_metrics)
        total_carbon = sum(m.carbon_kgco2e for m in self.task_metrics)
        total_time = sum(m.execution_time for m in self.task_metrics)
        
        return {
            "num_tasks": len(self.task_metrics),
            "total_energy_kwh": total_energy,
            "total_carbon_kgco2e": total_carbon,
            "total_execution_time": total_time,
            "avg_energy_per_task": total_energy / len(self.task_metrics),
            "avg_carbon_per_task": total_carbon / len(self.task_metrics),
            "avg_execution_time": total_time / len(self.task_metrics),
            "agent_type_breakdown": self._get_agent_type_breakdown()
        }
    
    def _get_agent_type_breakdown(self) -> Dict[str, Dict[str, float]]:
        """Breakdown metrics by agent type"""
        
        breakdown = {}
        
        for agent_type in set(m.agent_type for m in self.task_metrics):
            type_metrics = [m for m in self.task_metrics if m.agent_type == agent_type]
            
            breakdown[agent_type] = {
                "num_tasks": len(type_metrics),
                "total_energy_kwh": sum(m.energy_kwh for m in type_metrics),
                "total_carbon_kgco2e": sum(m.carbon_kgco2e for m in type_metrics),
                "avg_execution_time": np.mean([m.execution_time for m in type_metrics])
            }
        
        return breakdown
    
    def shutdown(self):
        """Shutdown Ray cluster"""
        
        if self.cluster_initialized:
            ray.shutdown()
            self.cluster_initialized = False
            logger.info("Ray cluster shutdown complete")
    
    def __enter__(self):
        """Context manager entry"""
        self.initialize_cluster()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        self.shutdown()


# Convenience function
def create_ray_cluster(
    num_workers: int = 4,
    agent_types: List[str] = None,
    carbon_budget: float = 0.1
) -> RayClusterManager:
    """
    Create and initialize Ray cluster with agent pools
    
    Args:
        num_workers: Number of worker agents per type
        agent_types: List of agent types to create pools for
        carbon_budget: Carbon budget per agent (kgCO2e)
    
    Returns:
        Initialized RayClusterManager
    """
    
    if agent_types is None:
        agent_types = ["retriever", "reasoner", "critic", "synthesizer"]
    
    manager = RayClusterManager()
    manager.initialize_cluster()
    
    # Create pools for each agent type
    for agent_type in agent_types:
        manager.create_agent_pool(
            agent_type=agent_type,
            num_agents=num_workers,
            carbon_budget_per_agent=carbon_budget / num_workers
        )
    
    return manager


if __name__ == "__main__":
    # Example usage
    async def main():
        # Create cluster
        with create_ray_cluster(num_workers=4) as cluster:
            
            # Create tasks
            tasks = [
                {
                    "task_id": f"task_{i}",
                    "query": f"What is the capital of country {i}?",
                    "top_k": 5
                }
                for i in range(20)
            ]
            
            # Execute distributed
            results = await cluster.execute_distributed_tasks(
                tasks=tasks,
                agent_type="retriever"
            )
            
            # Print results
            print(f"Completed {len(results)} tasks")
            print(f"Cluster info: {cluster.get_cluster_info()}")
            print(f"Pool stats: {cluster.get_agent_pool_stats()}")
            print(f"Task metrics: {cluster.get_task_metrics_summary()}")
    
    asyncio.run(main())
