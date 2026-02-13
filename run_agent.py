import json
import time
import argparse

from src.policy.policy_engine import PolicyEngine
from src.policy.policy_feedback import PolicyFeedback
from src.policy.adaptive_controller import AdaptiveController
from src.analysis.pareto_analyzer import ParetoAnalyzer
from src.analysis.self_monitor import SelfMonitor
from src.runtime.langchain_runtime import LangChainRuntime
from src.runtime.autogen_runtime import AutoGenRuntime
from src.chaos import inject_energy_spike


def runtime_factory(name):
    if name == "langchain":
        return LangChainRuntime()
    elif name == "autogen":
        return AutoGenRuntime()
    else:
        raise ValueError("Unknown runtime")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True)
    args = parser.parse_args()

    config = json.load(open(args.config))

    runtime = runtime_factory(config["framework"])
    runtime.init(config.get("runtime", {}))

    policy = PolicyEngine(config.get("policy", {}))
    feedback = PolicyFeedback()
    adaptive = AdaptiveController()
    pareto = ParetoAnalyzer()
    monitor = SelfMonitor()

    results = []

    for step, query in enumerate(config["queries"]):
        start = time.time()

        try:
            output = runtime.run(query)

            metrics = {
                "query_id": query.get("id"),
                "latency": time.time() - start,
                "energy": 0.02,
                "carbon": 0.00001,
                **output
            }

            metrics = inject_energy_spike(metrics)
            policy.enforce(metrics)

            monitor.snapshot(
                tool_calls=output.get("tool_calls", 0),
                depth=output.get("conversation_depth", 0),
            )

            if policy.should_reflect(step):
                trend = monitor.last_trend()
                mode = adaptive.evaluate(trend)
                adaptive.apply(runtime)

                reflection = feedback.generate(
                    decision=mode,
                    before={"energy": 0.03, "latency": 1.0},
                    after=metrics
                )
                metrics["reflection"] = reflection

            results.append(metrics)

        except Exception as e:
            results.append({
                "query_id": query.get("id"),
                "error": str(e)
            })
            break

    frontier = pareto.compute_frontier(results)

    output = {
        "framework": config["framework"],
        "results": results,
        "pareto_frontier": frontier
    }

    with open("agentbeats_results.json", "w") as f:
        json.dump(output, f, indent=2)

    runtime.finalize()
    print("Green Agent execution complete.")


if __name__ == "__main__":
    main()
