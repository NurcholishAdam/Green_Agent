from typing import List, Dict


class ParetoAnalyzer:
    """
    Computes Pareto frontier with metric provenance tags.
    """

    def compute_frontier(self, results: List[Dict]) -> List[Dict]:
        frontier = []

        for candidate in results:
            dominated = False
            for other in results:
                if (
                    other["energy"] <= candidate["energy"]
                    and other["accuracy"] >= candidate["accuracy"]
                    and other != candidate
                ):
                    dominated = True
                    break

            if not dominated:
                frontier.append(candidate)

        return frontier

{
    "accuracy": 0.9,
    "energy": 120,
    "latency": 1.2,
    "provenance": {
        "energy": "measured",
        "latency": "measured",
        "accuracy": "evaluated"
    }
}
