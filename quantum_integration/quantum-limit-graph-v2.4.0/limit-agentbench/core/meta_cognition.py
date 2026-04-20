"""
Phase 2 — Meta-Cognition Layer
================================
Wraps the CausalGraph and exposes a clean observe → diagnose → feedback
loop that integrates with AgentEvaluator.

The key behavioural shift over the current system:
  BEFORE:  anomaly flag = "CarbonIntensity exceeded threshold"
  AFTER:   structured report = {
               "root_cause": "WeatherEvent",
               "path": ["WeatherEvent","RenewableShortfall","GridStrain","CarbonIntensity"],
               "recommended_action": "defer_until_grid_stabilizes",
               "confidence": 0.72,
           }

Integration points:
  • Call observe_snapshot() at the START of each benchmark run with live telemetry.
  • Call diagnose() to get the root-cause report — log it alongside the SI score.
  • Call feedback() AFTER the result is confirmed to update edge weights.

Priority: SECOND — same phase as CausalGraph.
"""

from .causal_graph import CausalGraph


# Recommended actions keyed on root-cause variable name
RECOMMENDATION_MAP: dict[str, str] = {
    "WeatherEvent":          "defer_until_grid_stabilizes",
    "RenewableShortfall":    "defer_until_grid_stabilizes",
    "GridStrain":            "throttle_model_accuracy",
    "QueueDepth":            "throttle_model_accuracy",
    "BatteryLevel":          "switch_to_low_power_mode",
    "TaskPriority":          "execute_immediately",
    "ModelThrottleDecision": "review_throttle_policy",
}


class MetaCognitionLayer:
    """
    Monitors benchmark execution state, diagnoses anomalies via causal
    graph traversal, and feeds outcome results back into the graph as
    online learning signal.
    """

    def __init__(self, causal_graph: CausalGraph = None):
        self.graph = causal_graph or CausalGraph()
        self._history: list[dict] = []
        self._last_report: dict = {}

    # ------------------------------------------------------------------
    # Observe
    # ------------------------------------------------------------------

    def observe_snapshot(self, snapshot: dict):
        """
        Ingest a runtime telemetry snapshot and update the causal graph.

        snapshot keys are variable names; optional {var}_high / {var}_low
        keys override default anomaly thresholds.

        Example:
            {
                "CarbonIntensity": 430,
                "CarbonIntensity_high": 400,
                "GridStrain": 0.92,
                "BatteryLevel": 0.18,
                "TaskPriority": 0.9,
                "QueueDepth": 45,
            }
        """
        self.graph.observe_batch(snapshot)

    # ------------------------------------------------------------------
    # Diagnose
    # ------------------------------------------------------------------

    def diagnose(self, max_chains: int = 3) -> dict:
        """
        Run anomaly detection and backward causal traversal.

        Returns a structured diagnosis report:
        {
            "status": "nominal" | "anomaly_detected",
            "anomalies": ["CarbonIntensity", "GridStrain"],
            "root_causes": [
                {
                    "root_cause": "WeatherEvent",
                    "path": ["WeatherEvent", ..., "CarbonIntensity"],
                    "path_labels": ["causes", ...],
                    "cumulative_weight": 0.612,
                    "anomaly_variable": "CarbonIntensity",
                },
                ...
            ],
            "recommended_action": "defer_until_grid_stabilizes",
            "graph_state": { ... },
        }
        """
        anomalies = self.graph.get_anomalies()
        if not anomalies:
            report = {
                "status": "nominal",
                "anomalies": [],
                "root_causes": [],
                "recommended_action": "proceed",
                "graph_state": self.graph.export_state(),
            }
        else:
            all_chains: list[dict] = []
            for anomaly in anomalies:
                chains = self.graph.trace_root_causes(anomaly, max_depth=5)
                for chain in chains:
                    chain["anomaly_variable"] = anomaly
                    all_chains.append(chain)

            all_chains.sort(key=lambda c: c["cumulative_weight"], reverse=True)
            top_chains = all_chains[:max_chains]

            action = self._recommend(top_chains)
            report = {
                "status": "anomaly_detected",
                "anomalies": anomalies,
                "root_causes": top_chains,
                "recommended_action": action,
                "graph_state": self.graph.export_state(),
            }

        self._last_report = report
        self._history.append(report)
        return report

    # ------------------------------------------------------------------
    # Feedback (online learning)
    # ------------------------------------------------------------------

    def feedback(self, decision_was_correct: bool, learning_rate: float = 0.05):
        """
        Provide outcome feedback to update causal edge weights.

        Call after each benchmark result has been validated:
            meta.feedback(decision_was_correct=(actual_si > prior_mean_si))

        This progressively calibrates the causal graph to the real deployment
        environment — false-positive grid strain signals get down-weighted
        automatically over many benchmark runs.
        """
        if not self._last_report:
            return
        for chain in self._last_report.get("root_causes", []):
            path = chain.get("path", [])
            for i in range(len(path) - 1):
                self.graph.update_edge_weight(
                    source=path[i],
                    target=path[i + 1],
                    outcome_correct=decision_was_correct,
                    learning_rate=learning_rate,
                )

    # ------------------------------------------------------------------
    # Convenience
    # ------------------------------------------------------------------

    def _recommend(self, chains: list[dict]) -> str:
        if not chains:
            return "no_action"
        top_root = chains[0].get("root_cause", "")
        return RECOMMENDATION_MAP.get(top_root, "investigate_manually")

    def get_history(self) -> list[dict]:
        return list(self._history)

    def summary(self) -> dict:
        total = len(self._history)
        anomaly_runs = sum(1 for r in self._history if r["status"] == "anomaly_detected")
        actions = {}
        for r in self._history:
            a = r.get("recommended_action", "unknown")
            actions[a] = actions.get(a, 0) + 1
        return {
            "total_diagnoses": total,
            "anomaly_rate": round(anomaly_runs / total, 3) if total else 0,
            "action_distribution": actions,
        }
