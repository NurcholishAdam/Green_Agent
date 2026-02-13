from .metric_provenance import attach_provenance

class ParetoAnalyzer:
    """
    Multi-objective Pareto frontier with provenance tagging.
    """

    def dominates(self, a, b):
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

    def compute_frontier(self, points):
        frontier = []
        for p in points:
            dominated = False
            for q in points:
                if q is not p and self.dominates(q, p):
                    dominated = True
                    break
            if not dominated:
                frontier.append(attach_provenance(p))
        return frontier
