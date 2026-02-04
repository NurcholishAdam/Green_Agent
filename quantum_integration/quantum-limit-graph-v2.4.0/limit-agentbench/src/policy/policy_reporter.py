"""
Policy reporting and alerting.
"""

from typing import Dict


class PolicyReporter:
    def __init__(self, policy: Dict):
        self.threshold = policy.get("reporting", {}).get(
            "alert_threshold_energy_pct", 80
        )

    def report(self, metrics: Dict) -> Dict:
        alerts = []

        if metrics.get("energy_pct", 0) >= self.threshold:
            alerts.append("Energy budget nearing limit")

        metrics["policy_alerts"] = alerts
        return metrics
