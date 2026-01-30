from typing import Dict, List


class ParetoAnalyzer:
    """
    Computes a Pareto frontier across green + performance dimensions.
    Lower is better for cost metrics, higher is better for accuracy.
    """

    COST_KEYS = [
        "energy",
        "carbon",
        "latency",
        "memory",
        "framework_overhead_latency",
        "framework_overhead_energy",
        "tool_calls",
        "conversation_depth",
    ]

    BENEFIT_KEYS = ["accuracy"]

    def dominates(self, a: Dict, b: Dict) -> bool:
        """
        a dominates b if:
        - a is no worse in all dimensions
        - a is strictly better in at least one dimension
        """

        no_worse = True
        strictly_better = False

        for k in self.COST_KEYS:
            if a.get(k, float("inf")) > b.get(k, float("inf")):
                no_worse = False
            elif a.get(k, float("inf")) < b.get(k, float("inf")):
                strictly_better = True

        for k in self.BENEFIT_KEYS:
            if a.get(k, 0.0) < b.get(k, 0.0):
                no_worse = False
            elif a.get(k, 0.0) > b.get(k, 0.0):
                strictly_better = True

        return no_worse and strictly_better

    def pareto_frontier(self, metrics: List[Dict]) -> List[Dict]:
        frontier = []
        for m in metrics:
            if not any(self.dominates(o, m) for o in metrics if o is not m):
                frontier.append(m)
        return frontier
