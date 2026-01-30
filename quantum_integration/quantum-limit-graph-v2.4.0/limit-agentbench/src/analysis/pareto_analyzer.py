from typing import Dict, List


class ParetoAnalyzer:
    COSTS = [
        "energy",
        "carbon",
        "latency",
        "memory",
        "framework_overhead_latency",
        "framework_overhead_energy",
        "tool_calls",
        "conversation_depth",
    ]

    BENEFITS = ["accuracy"]

    def dominates(self, a: Dict, b: Dict) -> bool:
        no_worse = True
        better = False

        for k in self.COSTS:
            if a[k] > b[k]:
                no_worse = False
            elif a[k] < b[k]:
                better = True

        for k in self.BENEFITS:
            if a[k] < b[k]:
                no_worse = False
            elif a[k] > b[k]:
                better = True

        return no_worse and better

    def pareto_frontier(self, data: List[Dict]) -> List[Dict]:
        return [
            p for p in data
            if not any(self.dominates(q, p) for q in data if q is not p)
        ]
