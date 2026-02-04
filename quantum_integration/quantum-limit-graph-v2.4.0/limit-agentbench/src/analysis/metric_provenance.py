"""
Metric provenance tracking.

Tags metrics as:
- measured
- estimated
"""

from typing import Dict


class MetricProvenance:
    def __init__(self):
        self.tags: Dict[str, str] = {}

    def measured(self, key: str):
        self.tags[key] = "measured"

    def estimated(self, key: str):
        self.tags[key] = "estimated"

    def attach(self, metrics: Dict) -> Dict:
        metrics["_provenance"] = self.tags.copy()
        return metrics
