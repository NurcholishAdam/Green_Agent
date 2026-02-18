# analytics/pareto_frontier.py

from dataclasses import dataclass
from typing import List, Tuple
import logging

logger = logging.getLogger(__name__)


@dataclass
class AgentPerformancePoint:
    energy_joules: float
    accuracy_percent: float
    label: str


class SustainabilityPareto:
    """
    Computes and manages the Sustainability Pareto Frontier
    (Energy vs Accuracy).
    Lower energy and higher accuracy are preferred.
    """

    def __init__(self):
        self.points: List[AgentPerformancePoint] = []

    def add_point(self, energy_joules: float, accuracy_percent: float, label: str):
        if energy_joules < 0 or accuracy_percent < 0:
            raise ValueError("Energy and accuracy must be non-negative.")

        point = AgentPerformancePoint(
            energy_joules=energy_joules,
            accuracy_percent=accuracy_percent,
            label=label
        )

        self.points.append(point)
        logger.info(f"Added performance point: {point}")

    def compute_frontier(self) -> List[AgentPerformancePoint]:
        """
        Returns the Pareto-optimal subset.
        """
        sorted_points = sorted(
            self.points,
            key=lambda p: (p.energy_joules, -p.accuracy_percent)
        )

        frontier = []
        best_accuracy = -1

        for point in sorted_points:
            if point.accuracy_percent > best_accuracy:
                frontier.append(point)
                best_accuracy = point.accuracy_percent

        logger.info(f"Computed Pareto frontier with {len(frontier)} points.")
        return frontier

    def as_tuples(self) -> List[Tuple[float, float, str]]:
        return [
            (p.energy_joules, p.accuracy_percent, p.label)
            for p in self.points
        ]
