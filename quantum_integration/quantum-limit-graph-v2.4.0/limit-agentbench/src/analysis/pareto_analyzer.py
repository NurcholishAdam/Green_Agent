# src/analysis/pareto_analyzer.py

from dataclasses import dataclass
from typing import Dict, List, Any


# ---------------------------------------------------------------------
# Pareto point representation
# ---------------------------------------------------------------------

@dataclass(frozen=True)
class ParetoPoint:
    """
    A single evaluation point used for Pareto comparison.
    Lower is better for all metrics.
    """
    energy: float
    carbon: float
    latency: float
    memory: float
    framework_overhead_latency: float
    framework_overhead_energy: float
    tool_calls: int
    conversation_depth: int
    raw: Dict[str, Any]


# ---------------------------------------------------------------------
# Pareto frontier analyzer
# ---------------------------------------------------------------------

class ParetoFrontierAnalyzer:
    """
    Computes a Pareto frontier where ALL metrics are minimized.
    """

    METRICS = [
        "energy",
        "carbon",
        "latency",
        "memory",
        "framework_overhead_latency",
        "framework_overhead_energy",
        "tool_calls",
        "conversation_depth",
    ]

    def _to_point(self, metrics: Dict[str, Any]) -> ParetoPoint:
        """
        Convert a raw metrics dict into a ParetoPoint.
        Missing metrics default to 0 (AgentBeats-safe).
        """
        return ParetoPoint(
            energy=float(metrics.get("energy", 0.0)),
            carbon=float(metrics.get("carbon", 0.0)),
            latency=float(metrics.get("latency", 0.0)),
            memory=float(metrics.get("memory", 0.0)),
            framework_overhead_latency=float(
                metrics.get("framework_overhead_latency", 0.0)
            ),
            framework_overhead_energy=float(
                metrics.get("framework_overhead_energy", 0.0)
            ),
            tool_calls=int(metrics.get("tool_calls", 0)),
            conversation_depth=int(metrics.get("conversation_depth", 0)),
            raw=metrics,
        )

    def _dominates(self, a: ParetoPoint, b: ParetoPoint) -> bool:
        """
        Returns True if point `a` Pareto-dominates point `b`.
        """
        better_or_equal = True
        strictly_better = False

        for field in self.METRICS:
            av = getattr(a, field)
            bv = getattr(b, field)

            if av > bv:
                better_or_equal = False
                break
            if av < bv:
                strictly_better = True

        return better_or_equal and strictly_better

    def pareto_frontier(self, results: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Compute Pareto frontier from raw metrics dicts.
        Returns the ORIGINAL dicts (AgentBeats-compatible).
        """
        points = [self._to_point(r) for r in results]
        frontier: List[ParetoPoint] = []

        for candidate in points:
            dominated = False
            to_remove: List[ParetoPoint] = []

            for existing in frontier:
                if self._dominates(existing, candidate):
                    dominated = True
                    break
                if self._dominates(candidate, existing):
                    to_remove.append(existing)

            if not dominated:
                for r in to_remove:
                    frontier.remove(r)
                frontier.append(candidate)

        return [p.raw for p in frontier]


# ---------------------------------------------------------------------
# Backward-compatible public API (CRITICAL)
# ---------------------------------------------------------------------

class ParetoAnalyzer(ParetoFrontierAnalyzer):
    """
    Compatibility alias.

    run_agent.py and AgentBeats reviewers expect `ParetoAnalyzer`.
    This class intentionally adds no behavior.
    """
    pass
