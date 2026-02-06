"""
Pareto Analyzer with provenance and framework overhead support.
"""

class ParetoAnalyzer:
    def dominates(self, a: dict, b: dict) -> bool:
        better_or_equal = (
            a["energy"] <= b["energy"]
            and a["latency"] <= b["latency"]
            and a["carbon"] <= b["carbon"]
        )
        strictly_better = (
            a["energy"] < b["energy"]
            or a["latency"] < b["latency"]
            or a["carbon"] < b["carbon"]
        )
        return better_or_equal and strictly_better

    def pareto_frontier(self, points: list) -> list:
        frontier = []
        for p in points:
            dominated = False
            for q in points:
                if q is not p and self.dominates(q, p):
                    dominated = True
                    break
            if not dominated:
                p["metric_provenance"] = {
                    "energy": "measured",
                    "latency": "measured",
                    "carbon": "estimated",
                    "framework_overhead": "estimated",
                }
                frontier.append(p)
        return frontier
