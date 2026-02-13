"""
run_agent.py
============

Main execution entrypoint for Green_Agent.

Implements:
- Meta-cognitive execution lifecycle
- Real-time resource self-monitoring
- Policy enforcement (budgets + meta-rules)
- Adaptive strategy switching
- LangChain / AutoGen runtime execution
- Pareto analysis with provenance tags
- Reflective feedback reporting
- Chaos injection support
- Telemetry export for dashboard

Execution Lifecycle:

    Purple Agent Task
            ↓
    Metrics Collection (energy, latency, memory)
            ↓
    Self-Reflection (compare vs budgets)
            ↓
    Adaptive Adjustment (strategy switch if needed)
            ↓
    External Evaluation (Pareto frontier + policy scoring)
"""

import time
import json
import argparse
from typing import Dict, Any

from policy.policy_engine import PolicyEngine
from policy.policy_feedback import PolicyFeedback
from analysis.pareto_analyzer import ParetoAnalyzer
from runtime.langchain_runtime import LangChainRuntime
from runtime.autogen_runtime import AutoGenRuntime
from monitoring.self_monitor import SelfMonitor
from monitoring.energy_collectors import EnergyCollector
from chaos import ChaosInjector


# ============================================================
# Green Agent Runner
# ============================================================

class GreenAgentRunner:
    """
    Coordinates execution of Purple Agent under Green constraints.
    """

    def __init__(
        self,
        runtime_type: str,
        policy_path: str,
        chaos: bool = False
    ):
        self.runtime_type = runtime_type
        self.policy_engine = PolicyEngine(policy_path)
        self.policy_feedback = PolicyFeedback()
        self.pareto = ParetoAnalyzer()
        self.self_monitor = SelfMonitor()
        self.energy_collector = EnergyCollector()
        self.chaos = ChaosInjector(enabled=chaos)

        self.runtime = self._initialize_runtime(runtime_type)

    # --------------------------------------------------------

    def _initialize_runtime(self, runtime_type: str):
        if runtime_type == "langchain":
            return LangChainRuntime()
        elif runtime_type == "autogen":
            return AutoGenRuntime()
        else:
            raise ValueError("Unsupported runtime type")

    # --------------------------------------------------------

    def run(self, task_input: str) -> Dict[str, Any]:
        """
        Execute task under meta-cognitive green loop.
        """

        print("🌱 Starting Green Agent Execution...")
        self.energy_collector.start()
        self.self_monitor.start()

        start_time = time.time()

        reflection_interval = self.policy_engine.get_meta_rule(
            "reflection_interval", default=5
        )

        step_counter = 0
        final_output = None

        while True:
            step_counter += 1

            # Chaos injection (optional)
            self.chaos.inject_if_needed(step_counter)

            # Execute step
            output = self.runtime.step(task_input)
            final_output = output

            # Collect metrics snapshot
            metrics = self.self_monitor.snapshot()
            metrics.update(self.energy_collector.snapshot())

            # Policy evaluation
            policy_status = self.policy_engine.evaluate(metrics)

            # Reflection checkpoint
            if step_counter % reflection_interval == 0:
                reflection = self.policy_feedback.generate_self_reflection(
                    metrics=metrics,
                    policy_status=policy_status
                )
                print(f"🔁 Reflection: {reflection}")

            # Adaptive strategy switch if violated
            if policy_status["violated"]:
                print("⚠ Budget violation detected. Switching strategy.")
                self.runtime.switch_mode("low_energy")

            if self.runtime.is_finished():
                break

        end_time = time.time()

        # Final metrics
        final_metrics = self.self_monitor.snapshot()
        final_metrics.update(self.energy_collector.stop())

        final_metrics["latency"] = end_time - start_time

        # Pareto analysis
        pareto_result = self.pareto.compute(
            metrics=final_metrics,
            policy_weights=self.policy_engine.get_weights()
        )

        # Generate final report
        report = {
            "output": final_output,
            "metrics": final_metrics,
            "policy_status": policy_status,
            "pareto": pareto_result,
            "reflection": self.policy_feedback.final_summary()
        }

        self._export_telemetry(report)

        print("✅ Green Agent Execution Complete")
        return report

    # --------------------------------------------------------

    def _export_telemetry(self, report: Dict[str, Any]):
        """
        Export telemetry for dashboard.
        """
        with open("green_agent_report.json", "w") as f:
            json.dump(report, f, indent=2)


# ============================================================
# CLI ENTRYPOINT
# ============================================================

def main():
    parser = argparse.ArgumentParser(description="Green Agent Runner")

    parser.add_argument(
        "--runtime",
        type=str,
        required=True,
        choices=["langchain", "autogen"],
        help="Runtime backend"
    )

    parser.add_argument(
        "--policy",
        type=str,
        default="green_policy.yml",
        help="Path to green policy file"
    )

    parser.add_argument(
        "--task",
        type=str,
        required=True,
        help="Task input prompt"
    )

    parser.add_argument(
        "--chaos",
        action="store_true",
        help="Enable chaos injection"
    )

    args = parser.parse_args()

    runner = GreenAgentRunner(
        runtime_type=args.runtime,
        policy_path=args.policy,
        chaos=args.chaos
    )

    result = runner.run(args.task)

    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
