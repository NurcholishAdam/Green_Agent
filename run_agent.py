# run_agent.py

import json
import sys

from docker_metrics_collector import DockerMetricsCollector
from src.analysis.runtime_adapter import NativeAgentRuntime
from src.analysis.langchain_runtime import LangChainRuntime
from src.analysis.autogen_runtime import AutoGenRuntime
from src.analysis.pareto_analyzer import ParetoAnalyzer
from src.constraints.budget_enforcer import BudgetEnforcer, BudgetExceeded
from src.feedback.metric_sink import StdoutSink


def main():
    payload = json.load(sys.stdin)

    queries = payload.get("queries", [])
    config = payload.get("config", {})

    runtime_type = config.get("runtime", "native")
    if runtime_type == "langchain":
        runtime = LangChainRuntime()
    elif runtime_type == "autogen":
        runtime = AutoGenRuntime()
    else:
        runtime = NativeAgentRuntime()
    
    runtime.init(config)

    
    metrics_collector = DockerMetricsCollector()
    sink = StdoutSink()
    pareto = ParetoAnalyzer()
    enforcer = BudgetEnforcer(
        max_energy=config.get("max_energy"),
        max_carbon=config.get("max_carbon"),
        max_latency=config.get("max_latency"),
    )

    all_results = []

    for query in queries:

        def run_once():
            result = runtime.run(query)
            return result["accuracy"]

        metrics = metrics_collector.run_and_measure(run_once)

        try:
            enforcer.check(metrics)
        except BudgetExceeded as e:
            metrics["budget_exceeded"] = True
            metrics["error"] = str(e)

        sink.emit(metrics)
        all_results.append(metrics)

    frontier = pareto.pareto_frontier(all_results)

    output = {
        "results": all_results,
        "pareto_frontier": frontier,
    }

    print(json.dumps(output))


if __name__ == "__main__":
    main()
