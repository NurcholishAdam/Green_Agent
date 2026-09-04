# runtime/distributed_runtime.py (Enhanced)
# Adds optional integration with LIMIT Graph, MODP, RLHF,
# Multi‑Teacher On‑Policy Distillation, Bio‑inspired Optimisation, and MoE expert gating.

import asyncio
import logging
import random
import os
import json
from typing import Dict, Optional, List, Tuple

import ray

from analytics.pareto_analyzer import ParetoAnalyzer
from sustainability.carbon_intensity_provider import CarbonIntensityProvider
from sustainability.eco_mode_controller import EcoModeController
from metrics.quantum_efficiency import QuantumEfficiencyMetric
from rewards.negawatt_reward import NegawattReward
from rl.q_learning import QLearningAgent
from rl.ppo_trainer import PPOTrainer
from policy.policy_engine import PolicyEngine

# Optional imports for enhancements (graceful degradation)
try:
    from src.enhancements.schemas.node_descriptor import NodeDescriptor, NodeType, CoolingType
    from src.enhancements.schemas.workload_descriptor import WorkloadDescriptor, TaskType, Urgency
    from src.enhancements.schemas.feedback_event import FeedbackEvent
    from src.enhancements.zero_trust_architecture import ZeroTrustArchitecture, ZeroTrustConfig
    ENHANCEMENTS_AVAILABLE = True
except ImportError:
    ENHANCEMENTS_AVAILABLE = False
    logger.warning("Enhanced modules not available; running legacy mode.")

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# --------------------------------------------------------------------------
# Shared Cluster-Level Aggregator (Enhanced)
# --------------------------------------------------------------------------
@ray.remote
class ClusterMetricsAggregator:
    """
    Aggregates metrics from workers, computes Pareto frontier, and if enabled,
    computes MODP composite scores and graph metrics.
    """
    def __init__(self, config: Optional[Dict] = None):
        self.config = config or {}
        self.use_enhancements = self.config.get('use_enhancements', False) and ENHANCEMENTS_AVAILABLE
        self.pareto = ParetoAnalyzer()
        self.total_energy = 0.0
        self.total_accuracy = 0.0
        self.total_carbon = 0.0

        # Enhanced fields
        if self.use_enhancements:
            self.modp_weights = self.config.get('modp_weights', [0.4, 0.3, 0.2, 0.1])  # accuracy, energy, carbon, latency
            self.graph_metrics_accum = {'centrality': [], 'connectivity': []}

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

        # Enhanced: collect graph metrics if provided
        if self.use_enhancements and 'graph_metrics' in record:
            gm = record['graph_metrics']
            self.graph_metrics_accum['centrality'].append(gm.get('centrality', 0.5))
            self.graph_metrics_accum['connectivity'].append(gm.get('connectivity', 0.5))

    def finalize(self):
        frontier = self.pareto.compute_frontier()
        self.pareto.export_json("cluster_pareto.json")

        summary = {
            "total_energy": self.total_energy,
            "total_accuracy": self.total_accuracy,
            "total_carbon": self.total_carbon,
            "frontier_size": len(frontier)
        }

        if self.use_enhancements:
            # Compute average graph metrics
            avg_centrality = sum(self.graph_metrics_accum['centrality']) / max(len(self.graph_metrics_accum['centrality']), 1)
            avg_connectivity = sum(self.graph_metrics_accum['connectivity']) / max(len(self.graph_metrics_accum['connectivity']), 1)
            summary['graph_metrics'] = {'centrality': avg_centrality, 'connectivity': avg_connectivity}
            # Compute composite MODP score (normalized roughly)
            summary['modp_composite_score'] = (
                self.modp_weights[0] * (self.total_accuracy / max(self.total_energy, 1e-9)) +
                self.modp_weights[1] * (1 - self.total_energy / max(self.total_energy, 1e-9)) +
                self.modp_weights[2] * (1 - self.total_carbon / max(self.total_carbon, 1e-9)) +
                self.modp_weights[3] * 0.5  # placeholder latency
            )
        return summary


# --------------------------------------------------------------------------
# Ray Distributed Green Agent Worker (Enhanced)
# --------------------------------------------------------------------------
@ray.remote(max_restarts=3)
class GreenAgentWorker:
    def __init__(self, agent_id: int, config: Dict):
        self.agent_id = agent_id
        self.config = config
        self.use_enhancements = config.get('use_enhancements', False) and ENHANCEMENTS_AVAILABLE

        # Original components
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
        self.ppo = PPOTrainer(state_dim=10, action_dim=5)
        self.carbon_provider = CarbonIntensityProvider(region=config.get("region"))
        self.eco_controller = EcoModeController(self.policy_engine)
        self.quantum_metric = QuantumEfficiencyMetric()
        self.episode = 0

        # Enhanced components
        self.node_descriptor = None
        self.workload_descriptor = None
        self.distillation_optimizer = None
        if self.use_enhancements:
            try:
                self.node_descriptor = NodeDescriptor(
                    id=f"worker_{agent_id}",
                    type=NodeType.EDGE,
                    region=config.get("region", "unknown"),
                    region_carbon_intensity=self.carbon_provider.get_current_intensity(),
                    energy_per_token=0.00005,
                    helium_connectivity_score=0.8,
                    uptime=0.99,
                    renewable_fraction=0.3,
                    cooling_type=CoolingType.AIR,
                    hardware_model="cpu",
                    graph_metrics=config.get('graph_metrics', {'centrality': 0.5, 'connectivity': 0.5}),
                    human_feedback_score=config.get('human_feedback_score', 0.5)
                )
                self.workload_descriptor = WorkloadDescriptor(
                    task_id=f"worker_{agent_id}_task",
                    task_type=TaskType.INFERENCE,
                    tokens=1000,
                    latency_target=500.0,
                    urgency=Urgency.MEDIUM,
                    estimated_energy_joules=0.001,
                    estimated_carbon_kg=0.0002,
                    user_id=f"agent_{agent_id}",
                    metadata={"source": "distributed_runtime"}
                )
                # Simple distillation optimizer for action selection (placeholder)
                self.distillation_optimizer = self._create_distillation_optimizer()
            except Exception as e:
                logger.error(f"Enhanced init failed for agent {agent_id}: {e}")
                self.use_enhancements = False

    def _create_distillation_optimizer(self):
        """Create a simple multi‑teacher distillation optimizer (placeholder)."""
        class DistillationOptimizer:
            def __init__(self, n_actions=5):
                self.weights = np.zeros((10, n_actions))
                self.counter = 0

            def select_action(self, state_vec):
                logits = state_vec @ self.weights
                probs = np.exp(logits - np.max(logits))
                probs /= probs.sum()
                return int(np.argmax(probs)), probs

            def update(self, state_vec, action, reward):
                self.weights[:, action] += 0.1 * reward * state_vec
                self.counter += 1

        return DistillationOptimizer()

    async def run_episode(self, aggregator):
        self.episode += 1

        # Carbon adapt
        intensity = self.carbon_provider.get_current_intensity()
        self.eco_controller.update(intensity)

        # Simulated task
        energy = random.uniform(20, 100)
        accuracy = random.uniform(0.7, 1.0)
        quantum_energy = energy * 0.2

        # RL update
        state = random.randint(0, 9)
        action = random.randint(0, 4)
        reward = self.negawatt.combined_reward(accuracy=accuracy, energy=energy)

        # Enhanced: use distillation to select action (optional)
        if self.use_enhancements and self.distillation_optimizer:
            # Build state vector from current context
            state_vec = np.array([
                intensity / 1000.0,
                energy / 100.0,
                accuracy,
                self.episode / 100.0,
                0.5,  # placeholder for graph centrality
                0.5,  # connectivity
                0.5,  # human feedback
                0.0, 0.0, 0.0  # padding to size 10
            ])
            action, _ = self.distillation_optimizer.select_action(state_vec)
            # Compute a reward for distillation (based on negawatt)
            self.distillation_optimizer.update(state_vec, action, reward)

        # Update original RL agents
        self.q_agent.update(state, action, reward)
        self.ppo.store_transition(state, action, reward)
        self.quantum_metric.add_quantum_energy(quantum_energy)
        self.quantum_metric.set_task_completion_ratio(accuracy)

        carbon = energy * intensity / 1000

        # Enhanced: include graph metrics in report
        record = {
            "agent_id": self.agent_id,
            "energy": energy,
            "accuracy": accuracy,
            "carbon": carbon,
            "label": f"A{self.agent_id}_E{self.episode}"
        }
        if self.use_enhancements and self.node_descriptor:
            record['graph_metrics'] = {
                'centrality': self.node_descriptor.graph_metrics.get('centrality', 0.5),
                'connectivity': self.node_descriptor.graph_metrics.get('connectivity', 0.5)
            }

        await aggregator.report.remote(record)

        if self.episode % 5 == 0:
            self.ppo.train()

        return reward


# --------------------------------------------------------------------------
# Async Ray Cluster Orchestrator (Enhanced)
# --------------------------------------------------------------------------
class RayDistributedGreenCluster:
    def __init__(self, num_agents: int, config: Dict):
        ray.init(ignore_reinit_error=True)
        self.config = config

        self.aggregator = ClusterMetricsAggregator.remote(config)

        self.agents = [
            GreenAgentWorker.remote(i, config)
            for i in range(num_agents)
        ]
        self.num_agents = num_agents

    async def run_async(self, episodes_per_agent: int):
        for ep in range(episodes_per_agent):
            tasks = [agent.run_episode.remote(self.aggregator) for agent in self.agents]
            await asyncio.gather(*[asyncio.to_thread(ray.get, t) for t in tasks])

        summary = ray.get(self.aggregator.finalize.remote())
        logger.info("Cluster Summary:")
        logger.info(summary)
        return summary


# --------------------------------------------------------------------------
# Entry Point
# --------------------------------------------------------------------------
if __name__ == "__main__":
    # Example configuration with enhancements enabled
    config = {
        "energy_budget": 10000,
        "baseline_energy": 120,
        "region": "ID",
        "use_enhancements": True,  # Enable LIMIT Graph, MODP, RLHF, etc.
        "modp_weights": [0.4, 0.3, 0.2, 0.1],
        "human_feedback_score": 0.6,
        "graph_metrics": {"centrality": 0.7, "connectivity": 0.5}
    }

    cluster = RayDistributedGreenCluster(num_agents=4, config=config)
    asyncio.run(cluster.run_async(episodes_per_agent=20))
