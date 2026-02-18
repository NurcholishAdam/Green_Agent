# runtime/distributed_runtime.py

import asyncio
import logging
import random
import os
import json
from typing import Dict

import ray

from analytics.pareto_analyzer import ParetoAnalyzer
from sustainability.carbon_intensity_provider import CarbonIntensityProvider
from sustainability.eco_mode_controller import EcoModeController
from metrics.quantum_efficiency import QuantumEfficiencyMetric
from rewards.negawatt_reward import NegawattReward
from rl.q_learning import QLearningAgent
from rl.ppo_trainer import PPOTrainer
from policy.policy_engine import PolicyEngine


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# --------------------------------------------------
# Shared Cluster-Level Aggregator
# --------------------------------------------------

@ray.remote
class ClusterMetricsAggregator:

    def __init__(self):
        self.pareto = ParetoAnalyzer()
        self.total_energy = 0.0
        self.total_accuracy = 0.0
        self.total_carbon = 0.0

    def report(self, record: Dict):

        self.total_energy += record["energy"]
        self.total_accuracy += record["accuracy"]
        self.total_carbon += record["carbon"]

        self.pareto.add_record(
            energy_joules=record["energy"],
            accuracy=record["accuracy"] * 100,
            carbon_grams=record["carbon"],
            label=record["label"],
            metadata={"agent_id": record["agent_id"]}
        )

    def finalize(self):

        frontier = self.pareto.compute_frontier()
        self.pareto.export_json("cluster_pareto.json")

        return {
            "total_energy": self.total_energy,
            "total_accuracy": self.total_accuracy,
            "total_carbon": self.total_carbon,
            "frontier_size": len(frontier)
        }


# --------------------------------------------------
# Ray Distributed Green Agent
# --------------------------------------------------

@ray.remote(max_restarts=3)
class GreenAgentWorker:

    def __init__(self, agent_id: int, config: Dict):

        self.agent_id = agent_id
        self.config = config

        self.policy_engine = PolicyEngine(
            energy_budget=config["energy_budget"],
            baseline_energy=config.get("baseline_energy")
        )

        self.negawatt = NegawattReward(config.get("baseline_energy", 1000))

        self.q_agent = QLearningAgent(
            state_space=10,
            action_space=5,
            persistence_path=f"q_table_{agent_id}.pkl"
        )

        self.ppo = PPOTrainer(
            state_dim=10,
            action_dim=5
        )

        self.carbon_provider = CarbonIntensityProvider(
            region=config.get("region")
        )

        self.eco_controller = EcoModeController(self.policy_engine)

        self.quantum_metric = QuantumEfficiencyMetric()

        self.episode = 0

    async def run_episode(self, aggregator):

        self.episode += 1

        # Carbon adapt
        intensity = self.carbon_provider.get_current_intensity()
        self.eco_controller.update(intensity)

        # Simulated task
        energy = random.uniform(20, 100)
        accuracy = random.uniform(0.7, 1.0)
        quantum_energy = energy * 0.2

        reward = self.negawatt.combined_reward(
            accuracy=accuracy,
            energy=energy
        )

        # RL update
        state = random.randint(0, 9)
        action = random.randint(0, 4)

        self.q_agent.update(state, action, reward)
        self.ppo.store_transition(state, action, reward)

        self.quantum_metric.add_quantum_energy(quantum_energy)
        self.quantum_metric.set_task_completion_ratio(accuracy)

        carbon = energy * intensity / 1000

        await aggregator.report.remote({
            "agent_id": self.agent_id,
            "energy": energy,
            "accuracy": accuracy,
            "carbon": carbon,
            "label": f"A{self.agent_id}_E{self.episode}"
        })

        if self.episode % 5 == 0:
            self.ppo.train()

        return reward


# --------------------------------------------------
# Async Ray Cluster Orchestrator
# --------------------------------------------------

class RayDistributedGreenCluster:

    def __init__(self, num_agents: int, config: Dict):

        ray.init(ignore_reinit_error=True)

        self.aggregator = ClusterMetricsAggregator.remote()

        self.agents = [
            GreenAgentWorker.remote(i, config)
            for i in range(num_agents)
        ]

        self.num_agents = num_agents

    async def run_async(self, episodes_per_agent: int):

        for ep in range(episodes_per_agent):

            tasks = [
                agent.run_episode.remote(self.aggregator)
                for agent in self.agents
            ]

            # parallel execution
            await asyncio.gather(
                *[asyncio.to_thread(ray.get, t) for t in tasks]
            )

        summary = ray.get(self.aggregator.finalize.remote())
        logger.info("Cluster Summary:")
        logger.info(summary)

        return summary


# --------------------------------------------------
# Entry Point
# --------------------------------------------------

if __name__ == "__main__":

    config = {
        "energy_budget": 10000,
        "baseline_energy": 120,
        "region": "ID"
    }

    cluster = RayDistributedGreenCluster(
        num_agents=4,
        config=config
    )

    asyncio.run(cluster.run_async(episodes_per_agent=20))
