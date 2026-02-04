"""
Reporting layer for policy and benchmark results.
"""

import json
from typing import List, Dict


class PolicyReporter:
    def write_json(self, path: str, data: List[Dict]) -> None:
        with open(path, "w") as f:
            json.dump(data, f, indent=2)

    def emit_summary(self, results: List[Dict]) -> Dict:
        return {
            "total_runs": len(results),
            "policy_pass_rate": sum(1 for r in results if r.get("policy_pass")) / max(len(results), 1),
            "avg_energy_wh": sum(r.get("energy_wh", 0) for r in results) / max(len(results), 1),
        }
