# -*- coding: utf-8 -*-
"""
Benchmark Harness (Enhanced)
Orchestration engine for running comprehensive benchmarks.
Supports optional integration with LIMIT Graph, MODP, RLHF,
Multi‑Teacher On‑Policy Distillation, Bio‑inspired Optimisation, and MoE expert gating.
"""

import json
from typing import Dict, Any, List, Optional
from pathlib import Path
import logging
from datetime import datetime

from .agent_evaluator import AgentEvaluator
from .agentbench_adapter import AgentBenchAdapter

logger = logging.getLogger(__name__)

# Optional imports for enhancements
try:
    from src.enhancements.schemas.node_descriptor import NodeDescriptor
    from src.enhancements.schemas.workload_descriptor import WorkloadDescriptor
    from src.enhancements.zero_trust_architecture import ZeroTrustArchitecture
    from src.enhancements.schemas.feedback_event import FeedbackEvent
    ENHANCEMENTS_AVAILABLE = True
except ImportError:
    ENHANCEMENTS_AVAILABLE = False
    NodeDescriptor = None
    WorkloadDescriptor = None
    ZeroTrustArchitecture = None
    FeedbackEvent = None


class BenchmarkHarness:
    """
    Orchestration engine for running comprehensive agent benchmarks.

    Manages:
    - Task suite loading
    - Agent evaluation
    - Result aggregation
    - Report generation

    If `use_enhancements` is True in config, the harness will pass
    graph metrics, human feedback, and other enhancement parameters
    through to the evaluator, enabling LIMIT Graph, MODP, RLHF,
    distillation, MoE, and bio‑inspired decision‑making.
    """

    def __init__(
        self,
        output_dir: str = "./benchmark_results",
        grid_region: str = "GLOBAL",
        hardware_profile: str = "default",
        config: Optional[Dict[str, Any]] = None
    ):
        """
        Initialize benchmark harness.

        Args:
            output_dir: Directory for saving results
            grid_region: Grid region for carbon calculations
            hardware_profile: Hardware profile for power estimation
            config: Optional configuration dict. May include:
                - use_enhancements (bool)
                - modp_weights (list)
                - graph_metrics (dict)
                - human_feedback_score (float)
                - distillation params, etc.
        """
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

        self.config = config or {}
        self.use_enhancements = self.config.get('use_enhancements', False) and ENHANCEMENTS_AVAILABLE

        # Initialize evaluator with config (so enhancements are active)
        self.evaluator = AgentEvaluator(
            grid_region=grid_region,
            hardware_profile=hardware_profile,
            track_green_metrics=True,
            config=self.config
        )
        self.adapter = AgentBenchAdapter(protocol_version="1.0", config=self.config)

        logger.info(f"Initialized BenchmarkHarness: output_dir={output_dir}, "
                    f"enhancements={'on' if self.use_enhancements else 'off'}")

    def load_task_suite(self, suite_path: str) -> List[Dict[str, Any]]:
        """Load task suite from JSON file."""
        with open(suite_path, 'r') as f:
            tasks = json.load(f)
        logger.info(f"Loaded {len(tasks)} tasks from {suite_path}")
        return tasks

    def run_benchmark(
        self,
        agent: Any,
        task_suite: List[Dict[str, Any]],
        benchmark_name: str,
        backend: Optional[str] = None,
        rank: Optional[int] = None,
        graph_metrics: Optional[Dict[str, float]] = None,
        human_feedback_score: Optional[float] = None
    ) -> Dict[str, Any]:
        """
        Run benchmark on an agent, optionally passing enhancement data.

        Args:
            agent: Agent instance to benchmark
            task_suite: List of tasks
            benchmark_name: Name of the benchmark
            backend: Quantum backend (if applicable)
            rank: NSN rank (if applicable)
            graph_metrics: Optional LIMIT Graph metrics for the benchmark
            human_feedback_score: Optional RLHF feedback (0-1)

        Returns:
            Benchmark results
        """
        logger.info(f"Running benchmark '{benchmark_name}' on agent")

        start_time = datetime.utcnow()

        # Pass enhancement parameters if provided or from config
        if graph_metrics is None:
            graph_metrics = self.config.get('graph_metrics')
        if human_feedback_score is None:
            human_feedback_score = self.config.get('human_feedback_score', 0.5)

        # Evaluate agent on task suite
        suite_result = self.evaluator.evaluate_suite(
            agent=agent,
            tasks=task_suite,
            backend=backend,
            rank=rank,
            graph_metrics=graph_metrics,
            human_feedback_score=human_feedback_score
        )

        end_time = datetime.utcnow()
        duration = (end_time - start_time).total_seconds()

        benchmark_result = {
            "benchmark_name": benchmark_name,
            "agent_name": suite_result["agent_name"],
            "framework": suite_result["framework"],
            "num_tasks": suite_result["num_tasks"],
            "duration_seconds": duration,
            "start_time": start_time.isoformat() + "Z",
            "end_time": end_time.isoformat() + "Z",
            "aggregated_metrics": suite_result["aggregated_metrics"],
            "task_results": suite_result["results"],
            "backend": backend,
            "rank": rank,
        }

        # Add enhancement info if available
        if self.use_enhancements:
            benchmark_result["graph_metrics"] = graph_metrics
            benchmark_result["human_feedback_score"] = human_feedback_score
            if "distillation_summary" in suite_result:
                benchmark_result["distillation_summary"] = suite_result["distillation_summary"]

        # Save result
        self._save_result(benchmark_result, benchmark_name)

        logger.info(f"Benchmark '{benchmark_name}' completed in {duration:.2f}s")
        return benchmark_result

    def run_multi_agent_benchmark(
        self,
        agents: List[Any],
        task_suite: List[Dict[str, Any]],
        benchmark_name: str,
        sort_by: str = "sustainability_index",
        graph_metrics: Optional[Dict[str, float]] = None,
        human_feedback_score: Optional[float] = None
    ) -> Dict[str, Any]:
        """
        Run benchmark on multiple agents with optional enhancements.

        Args:
            agents: List of agent instances
            task_suite: List of tasks
            benchmark_name: Name of the benchmark
            sort_by: Metric to sort by
            graph_metrics: Optional LIMIT Graph metrics
            human_feedback_score: Optional RLHF feedback

        Returns:
            Multi-agent benchmark results with rankings
        """
        logger.info(f"Running multi-agent benchmark '{benchmark_name}' "
                    f"on {len(agents)} agents")

        start_time = datetime.utcnow()

        if graph_metrics is None:
            graph_metrics = self.config.get('graph_metrics')
        if human_feedback_score is None:
            human_feedback_score = self.config.get('human_feedback_score', 0.5)

        comparison = self.evaluator.compare_agents(
            agents=agents,
            tasks=task_suite,
            sort_by=sort_by,
            graph_metrics=graph_metrics,
            human_feedback_score=human_feedback_score
        )

        end_time = datetime.utcnow()
        duration = (end_time - start_time).total_seconds()

        benchmark_result = {
            "benchmark_name": benchmark_name,
            "num_agents": comparison["num_agents"],
            "num_tasks": comparison["num_tasks"],
            "sort_by": sort_by,
            "duration_seconds": duration,
            "start_time": start_time.isoformat() + "Z",
            "end_time": end_time.isoformat() + "Z",
            "rankings": comparison["rankings"],
        }

        if self.use_enhancements:
            benchmark_result["graph_metrics"] = graph_metrics
            benchmark_result["human_feedback_score"] = human_feedback_score

        self._save_result(benchmark_result, f"{benchmark_name}_multi_agent")

        logger.info(f"Multi-agent benchmark '{benchmark_name}' completed")
        return benchmark_result

    def _save_result(self, result: Dict[str, Any], name: str):
        """Save benchmark result to JSON file."""
        timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        filename = f"{name}_{timestamp}.json"
        filepath = self.output_dir / filename

        with open(filepath, 'w') as f:
            json.dump(result, f, indent=2)

        logger.info(f"Saved benchmark result to {filepath}")

    def generate_report(
        self,
        benchmark_result: Dict[str, Any],
        format: str = "markdown"
    ) -> str:
        """Generate human-readable benchmark report."""
        if format == "markdown":
            return self._generate_markdown_report(benchmark_result)
        else:
            return self._generate_text_report(benchmark_result)

    def _generate_markdown_report(self, result: Dict[str, Any]) -> str:
        """Generate markdown report, including enhanced metrics if present."""
        lines = [
            f"# Benchmark Report: {result['benchmark_name']}",
            "",
            f"**Date**: {result['start_time']}",
            f"**Duration**: {result['duration_seconds']:.2f}s",
            ""
        ]

        # Enhanced info header
        if 'graph_metrics' in result:
            lines.extend([
                f"**Graph Metrics**: {result['graph_metrics']}",
                f"**Human Feedback Score**: {result['human_feedback_score']}",
                ""
            ])

        if "rankings" in result:
            lines.extend([
                f"## Rankings (sorted by {result['sort_by']})",
                ""
            ])
            for i, agent_result in enumerate(result["rankings"], 1):
                metrics = agent_result["aggregated_metrics"]
                lines.extend([
                    f"### {i}. {agent_result['agent_name']} ({agent_result['framework']})",
                    "",
                    f"- **Success Rate**: {metrics.get('success_rate', 0):.2%}",
                    f"- **Avg Accuracy**: {metrics.get('avg_accuracy', 0):.4f}",
                    f"- **Avg Energy**: {metrics.get('avg_energy_kwh', 0):.6f} kWh",
                    f"- **Avg Carbon**: {metrics.get('avg_carbon_co2e_kg', 0):.6f} kg CO2e",
                    f"- **Avg Sustainability Index**: {metrics.get('avg_sustainability_index', 0):.2f}",
                    ""
                ])
                if self.use_enhancements and "avg_modp_reward" in metrics:
                    lines.append(f"- **Avg MODP Reward**: {metrics['avg_modp_reward']:.4f}")
        else:
            metrics = result["aggregated_metrics"]
            lines.extend([
                f"## Agent: {result['agent_name']} ({result['framework']})",
                "",
                f"**Tasks**: {result['num_tasks']}",
                "",
                "### Aggregated Metrics",
                "",
                f"- **Success Rate**: {metrics.get('success_rate', 0):.2%}",
                f"- **Avg Accuracy**: {metrics.get('avg_accuracy', 0):.4f}",
                f"- **Avg Latency**: {metrics.get('avg_latency_ms', 0):.2f} ms",
                f"- **Avg Energy**: {metrics.get('avg_energy_kwh', 0):.6f} kWh",
                f"- **Avg Carbon**: {metrics.get('avg_carbon_co2e_kg', 0):.6f} kg CO2e",
                f"- **Avg Sustainability Index**: {metrics.get('avg_sustainability_index', 0):.2f}",
                ""
            ])
            if self.use_enhancements and "avg_modp_reward" in metrics:
                lines.append(f"- **Avg MODP Reward**: {metrics['avg_modp_reward']:.4f}")

        return "\n".join(lines)

    def _generate_text_report(self, result: Dict[str, Any]) -> str:
        """Generate plain text report."""
        return self._generate_markdown_report(result).replace("#", "").replace("**", "")
