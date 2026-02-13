"""
Coordinates multiple Green Agents in distributed mode.
"""

from typing import List
from distributed.agent_node import AgentNode


class DistributedCoordinator:
    """
    Manages multiple agent nodes and aggregates Pareto metrics.
    """

    def __init__(self):
        self.nodes: List[AgentNode] = []

    def register(self, node: AgentNode):
        self.nodes.append(node)

    def run_all(self, task_input: str):
        results = []
        for node in self.nodes:
            result = node.run(task_input)
            results.append(result)

        return self.aggregate(results)

    def aggregate(self, results):
        total_energy = sum(r["metrics"]["energy_kwh"] for r in results)
        total_latency = sum(r["metrics"]["latency"] for r in results)

        return {
            "total_energy": total_energy,
            "total_latency": total_latency,
            "agents": results
        }
