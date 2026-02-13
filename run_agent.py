"""
run_agent.py

Green Agent Main Execution Entry Point

Implements:
- Meta-cognitive lifecycle
- Real-time monitoring
- RL-based adaptive policy selection
- Policy enforcement (budgets + meta rules)
- LangChain / AutoGen runtime abstraction
- Pareto analysis with provenance
- Reflective feedback
- Chaos injection
- Episodic memory persistence
- Telemetry export for dashboard
- Optional multi-agent coordination
"""

import time
import json
import argparse
from typing import Dict

# Monitoring
from telemetry.energy_monitor import EnergyMonitor
from telemetry.latency_monitor import LatencyMonitor
from telemetry.carbon_monitor import CarbonMonitor

# Carbon grid awareness
from carbon.grid_intensity import GridCarbonIntensity

# Policy + analysis
from policy.policy_engine import PolicyEngine
from policy.policy_feedback import PolicyFeedback
from analysis.pareto_analyzer import ParetoAnalyzer

# RL
from rl.q_learning import QLearningAgent

# Memory
from memory.episodic_memory import EpisodicMemory

# Runtime
from runtime.langchain_runtime import LangChainRuntime
from runtime.autogen_runtime import AutoGenRuntime

# Chaos
from chaos import ChaosInjector

# Multi-agent
from multi_agent.coordinator import MultiAgentCoordinator
from multi_agent.agent_node import AgentNode


TELEMETRY_STREAM_FILE = "telemetry_stream.json"


class GreenAgentRunner:

    def __init__(self, runtime_type="langchain", multi_agent=False):

        # Runtime selection
        if runtime_type == "langchain":
            self.runtime = LangChainRuntime()
        elif runtime_type == "autogen":
            self.runtime = AutoGenRuntime()
        else:
            raise ValueError("Unsupported runtime")

        # Monitoring
        self.energy_monitor = EnergyMonitor()
        self.latency_monitor = LatencyMonitor()
        self.carbon_monitor = CarbonMonitor()
        self.grid = GridCarbonIntensity()

        # Policy
        self.policy_engine = PolicyEngine("green_policy.yml")
        self.policy_feedback = PolicyFeedback()
        self.pareto = ParetoAnalyzer()

        # RL
        self.rl_agent = QLearningAgent()

        # Memory
        self.memory = EpisodicMemory()

        # Chaos
        self.chaos = ChaosInjector()

        # Multi-agent
        self.multi_agent = multi_agent

    # ==========================================================
    # META-COGNITIVE EXECUTION LIFECYCLE
    # ==========================================================

    def run(self, task: str) -> Dict:

        print("\n--- GREEN AGENT EXECUTION START ---")

        # -------------------------------
        # 1️⃣ SELF-MONITORING START
        # -------------------------------
        self.energy_monitor.start()
        self.latency_monitor.start()

        # -------------------------------
        # 2️⃣ RL STRATEGY SELECTION
        # -------------------------------
        state = "normal"

        strategy = self.rl_agent.choose_action(state)
        print(f"[RL] Selected Strategy: {strategy}")

        # -------------------------------
        # 3️⃣ CHAOS INJECTION (optional)
        # -------------------------------
        self.chaos.inject()

        # -------------------------------
        # 4️⃣ EXECUTION (single or multi-agent)
        # -------------------------------
        if self.multi_agent:

            agent_node = AgentNode("agent-1", self.runtime)
            coordinator = MultiAgentCoordinator([agent_node])
            results = coordinator.distribute(task)
            result = results[0]

        else:
            result = self.runtime.run(task, strategy=strategy)

        # -------------------------------
        # 5️⃣ STOP MONITORING
        # -------------------------------
        energy_kwh = self.energy_monitor.stop()
        latency = self.latency_monitor.stop()

        grid_intensity = self.grid.get_current_intensity()
        carbon_kg = energy_kwh * grid_intensity

        metrics = {
            "energy_kwh": energy_kwh,
            "latency": latency,
            "carbon_kg": carbon_kg,
            "grid_intensity": grid_intensity
        }

        print(f"[Metrics] {metrics}")

        # -------------------------------
        # 6️⃣ POLICY ENFORCEMENT
        # -------------------------------
        violations = self.policy_engine.evaluate(metrics)

        # -------------------------------
        # 7️⃣ PARETO ANALYSIS (with provenance)
        # -------------------------------
        pareto_result = self.pareto.analyze(metrics)

        # -------------------------------
        # 8️⃣ REFLECTIVE FEEDBACK
        # -------------------------------
        reflection = self.policy_feedback.generate(
            metrics=metrics,
            strategy=strategy,
            violations=violations
        )

        # -------------------------------
        # 9️⃣ RL UPDATE
        # -------------------------------
        reward = -metrics["energy_kwh"] - metrics["latency"]

        next_state = "normal"
        self.rl_agent.update(state, strategy, reward, next_state)

        # -------------------------------
        # 🔟 EPISODIC MEMORY STORE
        # -------------------------------
        report = {
            "task": task,
            "strategy": strategy,
            "metrics": metrics,
            "violations": violations,
            "pareto": pareto_result,
            "reflection": reflection,
            "result": result
        }

        self.memory.store(report)

        # -------------------------------
        # 1️⃣1️⃣ TELEMETRY EXPORT
        # -------------------------------
        with open(TELEMETRY_STREAM_FILE, "a") as f:
            f.write(json.dumps(metrics) + "\n")

        print("\n--- GREEN AGENT EXECUTION END ---")

        return report


# ==========================================================
# CLI ENTRY POINT
# ==========================================================

def main():

    parser = argparse.ArgumentParser()
    parser.add_argument("--task", type=str, required=True)
    parser.add_argument("--runtime", type=str, default="langchain")
    parser.add_argument("--multi_agent", action="store_true")

    args = parser.parse_args()

    runner = GreenAgentRunner(
        runtime_type=args.runtime,
        multi_agent=args.multi_agent
    )

    report = runner.run(args.task)

    print("\nFinal Report:")
    print(json.dumps(report, indent=4))


if __name__ == "__main__":
    main()
