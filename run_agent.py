"""
run_agent.py

Green Agent – Full Meta-Cognitive Execution Orchestrator

Features:
- Meta-cognitive lifecycle phases
- Real-time resource monitoring
- Policy enforcement + meta-rules
- RL-based adaptive strategy switching
- LangChain / AutoGen runtime execution
- Pareto analysis with provenance tracking
- Reflective reporting
- Chaos injection testing
- Telemetry export for dashboard
"""

import time
import uuid
import json
import random
from typing import Dict, Any

from policy.policy_engine import PolicyEngine
from analysis.pareto_analyzer import ParetoAnalyzer
from policy.policy_feedback import PolicyFeedback
from telemetry.energy_monitor import EnergyMonitor
from telemetry.carbon_monitor import CarbonMonitor
from telemetry.latency_monitor import LatencyMonitor


# ============================================================
# GreenAgentRunner
# ============================================================

class GreenAgentRunner:
    """
    Research-grade orchestration engine for sustainable AI execution.
    """

    def __init__(
        self,
        runtime,
        policy_config: Dict,
        chaos_probability: float = 0.0,
        enable_meta_rules: bool = True,
    ):
        self.runtime = runtime
        self.policy_engine = PolicyEngine(policy_config)
        self.pareto = ParetoAnalyzer()
        self.feedback = PolicyFeedback()

        self.energy_monitor = EnergyMonitor()
        self.carbon_monitor = CarbonMonitor()
        self.latency_monitor = LatencyMonitor()

        self.chaos_probability = chaos_probability
        self.enable_meta_rules = enable_meta_rules

        self.execution_id = str(uuid.uuid4())

    # ============================================================
    # Public API
    # ============================================================

    def run(self, task_input: str) -> Dict[str, Any]:

        lifecycle_trace = []

        # ========================================================
        # Phase 1: Intent Analysis (Meta-Cognition)
        # ========================================================
        phase = "intent_analysis"
        lifecycle_trace.append(phase)

        strategy = self._select_initial_strategy(task_input)

        # ========================================================
        # Phase 2: Pre-Execution Resource Baseline
        # ========================================================
        phase = "resource_baseline"
        lifecycle_trace.append(phase)

        baseline_metrics = self._capture_metrics()

        # ========================================================
        # Phase 3: Chaos Injection (Optional)
        # ========================================================
        phase = "chaos_injection"
        lifecycle_trace.append(phase)

        if self._should_inject_chaos():
            self._inject_chaos()

        # ========================================================
        # Phase 4: Runtime Execution
        # ========================================================
        phase = "runtime_execution"
        lifecycle_trace.append(phase)

        start_time = time.time()
        result = self._execute_runtime(task_input, strategy)
        end_time = time.time()

        # ========================================================
        # Phase 5: Post-Execution Monitoring
        # ========================================================
        phase = "post_execution_monitoring"
        lifecycle_trace.append(phase)

        metrics = self._collect_runtime_metrics(start_time, end_time)

        # Add provenance tags
        metrics["execution_id"] = self.execution_id
        metrics["strategy"] = strategy
        metrics["timestamp"] = time.time()

        # ========================================================
        # Phase 6: Policy Enforcement
        # ========================================================
        phase = "policy_enforcement"
        lifecycle_trace.append(phase)

        budget_ok = self.policy_engine.enforce_budgets(metrics)

        if self.enable_meta_rules:
            budget_ok = budget_ok and self._meta_rules(metrics)

        # ========================================================
        # Phase 7: Adaptive RL Tuning
        # ========================================================
        phase = "adaptive_policy_update"
        lifecycle_trace.append(phase)

        self.policy_engine.adapt(metrics)

        # ========================================================
        # Phase 8: Adaptive Strategy Switching
        # ========================================================
        phase = "strategy_switching"
        lifecycle_trace.append(phase)

        new_strategy = self._adaptive_strategy_switch(metrics)

        # ========================================================
        # Phase 9: Pareto Analysis
        # ========================================================
        phase = "pareto_analysis"
        lifecycle_trace.append(phase)

        pareto_score = self.pareto.compute(metrics)

        # ========================================================
        # Phase 10: Reflective Feedback
        # ========================================================
        phase = "reflection"
        lifecycle_trace.append(phase)

        reflection = self.feedback.generate(metrics, pareto_score)

        # ========================================================
        # Phase 11: Telemetry Export
        # ========================================================
        phase = "telemetry_export"
        lifecycle_trace.append(phase)

        telemetry_bundle = self._export_telemetry(metrics)

        # ========================================================
        # Final Report
        # ========================================================

        report = {
            "execution_id": self.execution_id,
            "result": result,
            "metrics": metrics,
            "pareto_score": pareto_score,
            "policy_weights": self.policy_engine.weights,
            "budget_compliant": budget_ok,
            "lifecycle_trace": lifecycle_trace,
            "reflection": reflection,
            "next_recommended_strategy": new_strategy,
            "telemetry_bundle": telemetry_bundle,
        }

        self._persist_report(report)

        return report

    # ============================================================
    # Strategy Selection
    # ============================================================

    def _select_initial_strategy(self, task_input: str) -> str:
        if len(task_input) > 200:
            return "energy_saver"
        return "balanced"

    def _adaptive_strategy_switch(self, metrics: Dict) -> str:
        if metrics["energy_kwh"] > 0.5:
            return "energy_saver"
        if metrics["latency"] > 5.0:
            return "latency_optimized"
        return "balanced"

    # ============================================================
    # Runtime Execution (LangChain / AutoGen)
    # ============================================================

    def _execute_runtime(self, task_input: str, strategy: str):

        # Strategy injection
        runtime_config = {"mode": strategy}

        if hasattr(self.runtime, "run"):
            return self.runtime.run(task_input, runtime_config)

        raise RuntimeError("Runtime does not implement run()")

    # ============================================================
    # Monitoring
    # ============================================================

    def _capture_metrics(self) -> Dict:
        return {
            "energy_kwh": self.energy_monitor.read(),
            "carbon_kg": self.carbon_monitor.read(),
            "latency": self.latency_monitor.read(),
        }

    def _collect_runtime_metrics(self, start: float, end: float) -> Dict:
        return {
            "energy_kwh": self.energy_monitor.read(),
            "carbon_kg": self.carbon_monitor.read(),
            "latency": end - start,
        }

    # ============================================================
    # Policy Meta-Rules
    # ============================================================

    def _meta_rules(self, metrics: Dict) -> bool:
        """
        Meta-rules for system safety:
        - Avoid runaway carbon spikes
        - Prevent extreme latency loops
        """

        if metrics["carbon_kg"] > 1.0:
            return False

        if metrics["latency"] > 30:
            return False

        return True

    # ============================================================
    # Chaos Engineering
    # ============================================================

    def _should_inject_chaos(self) -> bool:
        return random.random() < self.chaos_probability

    def _inject_chaos(self):
        print("[Chaos] Injecting synthetic latency spike...")
        time.sleep(random.uniform(0.5, 1.5))

    # ============================================================
    # Telemetry Export
    # ============================================================

    def _export_telemetry(self, metrics: Dict) -> Dict:

        telemetry = {
            "execution_id": self.execution_id,
            "energy_kwh": metrics["energy_kwh"],
            "carbon_kg": metrics["carbon_kg"],
            "latency": metrics["latency"],
            "policy_weights": self.policy_engine.weights,
            "timestamp": metrics["timestamp"],
        }

        with open("telemetry_stream.json", "a") as f:
            f.write(json.dumps(telemetry) + "\n")

        return telemetry

    # ============================================================
    # Persistence
    # ============================================================

    def _persist_report(self, report: Dict):
        with open(f"execution_report_{self.execution_id}.json", "w") as f:
            json.dump(report, f, indent=4)
