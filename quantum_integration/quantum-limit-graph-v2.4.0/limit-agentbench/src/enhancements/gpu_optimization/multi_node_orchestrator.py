"""
Multi‑node orchestrator for FlexGen policies.
Selects the best node and policy combination from a set of candidate nodes.
"""

import asyncio
from typing import List, Dict, Any, Optional, Tuple

from .flexgen_policy import FlexGenPolicy, MockFlexGenExecutor
from .flexgen_policy_selector import DistillationFlexGenSelector, FlexGenState
from .flexgen_cost_model import FlexGenCostModel
from ..schemas.node_descriptor import NodeDescriptor
from ..schemas.workload_descriptor import WorkloadDescriptor
from ..pareto_gating import ParetoGating
from ..logger import logger


class MultiNodeOrchestrator:
    def __init__(
        self,
        nodes: List[NodeDescriptor],
        workload: WorkloadDescriptor,
        carbon_intensity: float,
        selector: Optional[DistillationFlexGenSelector] = None,
    ):
        self.nodes = nodes
        self.workload = workload
        self.carbon_intensity = carbon_intensity
        self.selector = selector or DistillationFlexGenSelector()
        self.cost_model = FlexGenCostModel(carbon_intensity_g_per_kwh=carbon_intensity)
        self.executor = MockFlexGenExecutor(carbon_intensity_g_per_kwh=carbon_intensity)
        self.pareto = ParetoGating(
            objectives=[
                {"key": "latency_ms", "direction": "min"},
                {"key": "energy_joules", "direction": "min"},
                {"key": "carbon_g", "direction": "min"},
            ]
        )

    async def select_best(self, candidates: List[FlexGenPolicy]) -> Dict:
        """
        Evaluate policies on all nodes and return the best (node, policy) pair.
        """
        best_global = None
        best_reward = -1
        for node in self.nodes:
            # Evaluate policies on this node
            results = []
            for policy in candidates:
                metrics = self.executor.execute(policy, node, self.workload)
                if metrics["success"]:
                    metrics["node_id"] = node.id
                    results.append(metrics)
            if not results:
                continue
            # Apply Pareto filter to get non-dominated policies for this node
            pareto_list = self.pareto.filter(results)
            # Choose best policy via selector for this node
            state = FlexGenState(
                tokens=self.workload.tokens,
                latency_target=self.workload.latency_target,
                gpu_memory_gb=node.metadata.get("gpu_memory_gb", 16),
                cpu_memory_gb=node.metadata.get("cpu_memory_gb", 64),
                disk_bandwidth_gbps=node.metadata.get("disk_bandwidth_gbps", 2),
                carbon_intensity=self.carbon_intensity,
                recent_success_rate=0.8,
                avg_reward=0.6,
                policy_idx=0,
            )
            # Use first Pareto candidate as action for simplicity (real selector would choose)
            if pareto_list:
                chosen = pareto_list[0]
                reward = self._compute_reward(chosen, self.workload)
                if reward > best_reward:
                    best_reward = reward
                    best_global = {
                        "node": node,
                        "policy": chosen["policy"],
                        "metrics": chosen,
                        "reward": reward,
                    }
        return best_global

    def _compute_reward(self, metrics, workload):
        latency_score = 1.0 - min(1.0, metrics["latency_ms"] / max(workload.latency_target, 1.0))
        energy_score = 1.0 - min(1.0, metrics["energy_joules"] / 100.0)
        carbon_score = 1.0 - min(1.0, metrics["carbon_g"] / 10.0)
        return 0.4 + 0.2*latency_score + 0.2*energy_score + 0.2*carbon_score
