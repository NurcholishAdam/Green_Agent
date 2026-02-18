# analytics/pareto_analyzer.py

from dataclasses import dataclass, asdict
from typing import List, Dict
import json
import logging
import os

logger = logging.getLogger(__name__)


@dataclass
class ParetoPoint:
    energy_joules: float
    accuracy: float
    carbon_grams: float
    label: str
    metadata: Dict


class ParetoAnalyzer:
    """
    Sustainability Pareto Analyzer:
    - X axis: Energy (Joules)
    - Y axis: Accuracy (%)
    - Stores provenance metadata
    - Computes Pareto frontier
    """

    def __init__(self):
        self.points: List[ParetoPoint] = []

    def add_record(
        self,
        energy_joules: float,
        accuracy: float,
        carbon_grams: float,
        label: str,
        metadata: Dict = None
    ):
        if metadata is None:
            metadata = {}

        point = ParetoPoint(
            energy_joules=energy_joules,
            accuracy=accuracy,
            carbon_grams=carbon_grams,
            label=label,
            metadata=metadata
        )

        self.points.append(point)
        logger.info(f"Added Pareto record: {label}")

    def compute_frontier(self) -> List[ParetoPoint]:
        """
        Pareto-optimal points:
        Lower energy + Higher accuracy
        """

        sorted_pts = sorted(
            self.points,
            key=lambda p: (p.energy_joules, -p.accuracy)
        )

        frontier = []
        best_accuracy = -1

        for p in sorted_pts:
            if p.accuracy > best_accuracy:
                frontier.append(p)
                best_accuracy = p.accuracy

        logger.info(f"Computed Pareto frontier ({len(frontier)} points)")
        return frontier

    def export_json(self, path="pareto_results.json"):
        with open(path, "w") as f:
            json.dump([asdict(p) for p in self.points], f, indent=4)

        logger.info(f"Pareto data exported to {path}")
