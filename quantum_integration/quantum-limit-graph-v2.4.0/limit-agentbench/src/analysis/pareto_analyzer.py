# src/analysis/pareto_analyzer.py
from typing import List, Dict

class ParetoAnalyzer:
    def pareto_frontier(self, points: List[Dict]) -> List[Dict]:
        frontier = []

        for p in points:
            dominated = False
            for q in points:
                if self._dominates(q, p):
                    dominated = True
                    break
            if not dominated:
                frontier.append(p)

        return frontier

    def _dominates(self, a: Dict, b: Dict) -> bool:
        return (
            a["accuracy"] >= b["accuracy"]
            and a["energy"] + a.get("framework_overhead_energy", 0)
               <= b["energy"] + b.get("framework_overhead_energy", 0)
            and a["latency"] + a.get("framework_overhead_latency", 0)
               <= b["latency"] + b.get("framework_overhead_latency", 0)
            and (
                a["accuracy"] > b["accuracy"]
                or a["energy"] < b["energy"]
            )
        )
