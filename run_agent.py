#!/usr/bin/env python3
import json
import time
import traceback
from typing import Dict, List

from src.analysis.policy_loader import load_policy
from src.constraints.green_policy_enforcer import (
    GreenPolicyEnforcer,
    PolicyViolation,
)
from src.analysis.carbon_estimator import CarbonEstimator
from src.analysis.pareto_analyzer import ParetoAnalyzer
from src.feedback.policy_feedback import PolicyFeedback
from src.reporting.policy_reporter import generate_policy_report


# ---------------------------------------------------------------------
# Mock runtime (replace later with LangChain / AutoGen adapter)
# ---------------------------------------------------------------------

def run_primary_agent() -> Dict:
    """
    Simulated agent execution.
    Must NEVER raise.
    """
    time.sleep(0.4)

    return {
        "accuracy": 0.83,
        "energy": 2.6,                     # Wh
        "latency": 0.42,                   # seconds
        "memory": 512,                     # MB
        "framework_overhead_latency": 0.05,
        "framework_overhead_energy": 0.12,
        "tool_calls": 4,
        "conversation_depth": 2,
    }


# ---------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------

def main():
    policy = load_policy("green_policy.yaml")
    enforcer = GreenPolicyEnforcer(policy)
    feedback = PolicyFeedback()
    pareto = ParetoAnalyzer()

    all_results: List[Dict] = []
    framework = "standalone"

    try:
        enforcer.preflight_check()
    except Exception:
        # Preflight should never kill execution
        pass

    run_id = "run-001"

    try:
        raw_metrics = run_primary_agent()

        carbon_estimator = CarbonEstimator(
            grid_intensity_g_kwh=policy.carbon["grid_intensity_g_kwh"],
            pue=policy.carbon["pue_factor"],
        )

        raw_metrics["carbon"] = carbon_estimator.estimate(
            raw_metrics["energy"]
        )

        verdict = enforcer.check_runtime(raw_metrics)
        raw_metrics["policy"] = verdict

    except PolicyViolation as e:
        raw_metrics = {
            "policy": {
                "compliant": False,
                "violations": str(e).split(","),
            }
        }

    except Exception:
        raw_metrics = {
            "policy": {
                "compliant": False,
                "violations": ["runtime_exception"],
            },
            "error": traceback.format_exc(),
        }

    feedback.emit(
        run_id=run_id,
        verdict=raw_metrics["policy"],
        policy_hash=policy.hash,
        framework=framework,
    )

    all_results.append(raw_metrics)

    compliant_runs = [
        r for r in all_results if r["policy"]["compliant"]
    ]

    frontier = pareto.pareto_frontier(compliant_runs)

    report = generate_policy_report(
        all_results=all_results,
        policy_hash=policy.hash,
    )

    output = {
        "agent": policy.identity["name"],
        "policy_hash": policy.hash,
        "framework": framework,
        "results": all_results,
        "pareto_frontier": frontier,
        "policy_report": report,
    }

    with open("agentbeats_results.json", "w") as f:
        json.dump(output, f, indent=2)

    print("✅ Green Policy execution complete")
    print("Pareto frontier size:", len(frontier))


if __name__ == "__main__":
    main()
