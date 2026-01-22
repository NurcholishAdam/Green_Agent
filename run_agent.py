import json
import sys
import traceback
from typing import Any, Dict

# ---- Import metrics collector ----
from docker_metrics_collector import measure_execution

# ---- Import agent logic ----
# Adjust this import if your agent entry differs
from src.agentbeats.agent import run_agent as agent_run


def load_input() -> Dict[str, Any]:
    """
    Load JSON input from STDIN (AgentBeats contract).
    """
    try:
        return json.load(sys.stdin)
    except json.JSONDecodeError as e:
        raise RuntimeError(f"Invalid JSON input: {e}")


def main():
    try:
        # 1. Read benchmark query
        query = load_input()

        # Expected (flexible) structure
        task_input = query.get("task", query)

        # 2. Execute agent under measurement
        result, metrics = measure_execution(
            agent_run,
            task_input
        )

        # 3. Standardized output schema
        output = {
            "status": "ok",
            "result": result,
            "metrics": {
                "latency_ms": metrics.get("latency_ms"),
                "cpu_time_ms": metrics.get("cpu_time_ms"),
                "memory_mb": metrics.get("memory_mb"),
                "energy_joules": metrics.get("energy_joules"),
                "carbon_g": metrics.get("carbon_g"),
            }
        }

        # 4. Emit JSON to STDOUT
        json.dump(output, sys.stdout)
        sys.stdout.flush()

    except Exception as e:
        # HARD FAIL must still return JSON
        error_output = {
            "status": "error",
            "error": str(e),
            "traceback": traceback.format_exc()
        }
        json.dump(error_output, sys.stdout)
        sys.stdout.flush()
        sys.exit(1)


if __name__ == "__main__":
    main()
