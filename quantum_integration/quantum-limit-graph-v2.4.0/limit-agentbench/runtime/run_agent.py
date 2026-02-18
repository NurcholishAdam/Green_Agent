# runtime/run_agent.py

import os
import time
import json
import logging
import traceback
import random
from typing import Dict

from analytics.pareto_analyzer import ParetoAnalyzer
from sustainability.carbon_intensity_provider import CarbonIntensityProvider
from sustainability.eco_mode_controller import EcoModeController
from metrics.quantum_efficiency import QuantumEfficiencyMetric
from rewards.negawatt_reward import NegawattReward
from rl.q_learning import QLearningAgent
from rl.ppo_trainer import PPOTrainer
from coordination.distributed_manager import DistributedCoordinator
from telemetry.telemetry_exporter import TelemetryExporter
from policy.policy_engine import PolicyEngine


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class RobustGreenAgentRuntime:

    def __init__(self, config: Dict):

        self.config = config
        self.checkpoint_path = "runtime_checkpoint.json"

        # Core Engines
        self.policy_engine = PolicyEngine(
            energy_budget=config["energy_budget"],
            baseline_energy=config.get("baseline_energy")
        )

        self.negawatt = NegawattReward(config.get("baseline_energy", 1000))

        self.q_agent = QLearningAgent(
            state_space=10,
            action_space=5,
            persistence_path="q_table.pkl"
        )

        self.ppo = PPOTrainer(
            state_dim=10,
            action_dim=5
        )

        self.distributed = DistributedCoordinator()

        self.carbon_provider = CarbonIntensityProvider(
            region=config.get("region")
        )

        self.eco_controller = EcoModeController(self.policy_engine)

        self.quantum_metric = QuantumEfficiencyMetric()
        self.pareto = ParetoAnalyzer()

        self.telemetry = TelemetryExporter()

        self.total_energy = 0.0
        self.total_accuracy = 0.0
        self.total_carbon = 0.0

        self.episode = 0

    # ------------------------------------------------
    # Meta-Cognitive Lifecycle
    # ------------------------------------------------

    def plan(self):
        logger.info("Planning phase...")
        return {"strategy": "adaptive"}

    def execute(self, task):
        logger.info("Execution phase...")

        energy_used = random.uniform(20, 100)
        accuracy = random.uniform(0.7, 1.0)

        quantum_energy = energy_used * 0.2

        return {
            "energy": energy_used,
            "accuracy": accuracy,
            "quantum_energy": quantum_energy
        }

    def reflect(self, result):
        logger.info("Reflection phase...")

        reward = self.negawatt.combined_reward(
            accuracy=result["accuracy"],
            energy=result["energy"]
        )

        state = random.randint(0, 9)
        action = random.randint(0, 4)

        self.q_agent.update(state, action, reward)

        self.ppo.store_transition(state, action, reward)

        return reward

    # ------------------------------------------------
    # Carbon-aware Adaptation
    # ------------------------------------------------

    def carbon_adapt(self):
        intensity = self.carbon_provider.get_current_intensity()
        self.eco_controller.update(intensity)

    # ------------------------------------------------
    # Checkpointing
    # ------------------------------------------------

    def save_checkpoint(self):
        data = {
            "episode": self.episode,
            "total_energy": self.total_energy,
            "total_accuracy": self.total_accuracy,
            "total_carbon": self.total_carbon
        }
        with open(self.checkpoint_path, "w") as f:
            json.dump(data, f)

        logger.info("Checkpoint saved.")

    def load_checkpoint(self):
        if os.path.exists(self.checkpoint_path):
            with open(self.checkpoint_path) as f:
                data = json.load(f)

            self.episode = data["episode"]
            self.total_energy = data["total_energy"]
            self.total_accuracy = data["total_accuracy"]
            self.total_carbon = data["total_carbon"]

            logger.info("Checkpoint restored.")

    # ------------------------------------------------
    # Stress Testing
    # ------------------------------------------------

    def chaos_injection(self):
        if random.random() < 0.05:
            raise RuntimeError("Injected failure for robustness testing.")

    # ------------------------------------------------
    # Main Loop
    # ------------------------------------------------

    def run(self, num_episodes=10):

        self.load_checkpoint()

        for _ in range(num_episodes):

            try:
                self.episode += 1

                self.chaos_injection()

                self.carbon_adapt()

                task = self.plan()

                result = self.execute(task)

                reward = self.reflect(result)

                self.total_energy += result["energy"]
                self.total_accuracy += result["accuracy"]
                self.total_carbon += (
                    result["energy"] *
                    self.carbon_provider.get_current_intensity() / 1000
                )

                # Quantum efficiency update
                self.quantum_metric.add_quantum_energy(
                    result["quantum_energy"]
                )
                self.quantum_metric.set_task_completion_ratio(
                    result["accuracy"]
                )

                # Pareto tracking
                self.pareto.add_record(
                    energy_joules=result["energy"],
                    accuracy=result["accuracy"] * 100,
                    carbon_grams=result["energy"] *
                    self.carbon_provider.get_current_intensity() / 1000,
                    label=f"Episode_{self.episode}",
                    metadata={"reward": reward}
                )

                # Distributed coordination sync
                self.distributed.sync_state(self.episode)

                # Telemetry
                self.telemetry.export(
                    episode=self.episode,
                    energy=result["energy"],
                    accuracy=result["accuracy"],
                    reward=reward
                )

                # PPO training periodically
                if self.episode % 5 == 0:
                    self.ppo.train()

                self.save_checkpoint()

            except Exception as e:
                logger.error("Crash detected. Recovering...")
                logger.error(traceback.format_exc())
                self.load_checkpoint()

        # Final exports
        frontier = self.pareto.compute_frontier()
        self.pareto.export_json()

        quantum_eff = self.quantum_metric.compute()

        logger.info("Run completed.")
        logger.info(f"Quantum Efficiency: {quantum_eff}")
        logger.info(f"Pareto frontier size: {len(frontier)}")


# ------------------------------------------------
# Entry
# ------------------------------------------------

if __name__ == "__main__":

    config = {
        "energy_budget": 10000,
        "baseline_energy": 120,
        "region": "ID"
    }

    runtime = RobustGreenAgentRuntime(config)
    runtime.run(num_episodes=20)
