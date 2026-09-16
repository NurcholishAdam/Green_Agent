"""
MetricGovernance — enforce metric definitions and units.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional


@dataclass
class MetricDefinition:
    name: str
    unit: str
    direction: str                  # "min" | "max"
    description: str = ""


class MetricGovernance:
    """Registry of approved metric definitions."""

    def __init__(self):
        self._metrics: Dict[str, MetricDefinition] = {}

    def register(self, definition: MetricDefinition) -> None:
        self._metrics[definition.name] = definition

    def validate(self, metrics: Dict[str, float]) -> List[str]:
        """Return a list of validation errors."""
        errors: List[str] = []
        for name, value in metrics.items():
            if name not in self._metrics:
                errors.append(f"metric '{name}' not governed")
                continue
            if not isinstance(value, (int, float)):
                errors.append(
                    f"metric '{name}' value is not numeric"
                )
        return errors

    def definitions(self) -> Dict[str, Dict]:
        return {
            name: {
                "unit": d.unit,
                "direction": d.direction,
                "description": d.description,
            }
            for name, d in self._metrics.items()
        }
