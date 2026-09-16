"""
ReproducibilityRules — what a benchmark run must declare to be
considered reproducible.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import List


@dataclass
class ReproducibilityRules:
    """
    The minimum context a benchmark submission must include.
    """
    required_fields: List[str] = field(
        default_factory=lambda: [
            "system_version",
            "policy_version",
            "dataset_id",
            "random_seed",
            "hardware_profile",
            "region",
        ]
    )

    def validate(self, submission: dict) -> List[str]:
        """Return a list of missing required fields."""
        return [
            f for f in self.required_fields
            if f not in submission or submission[f] is None
        ]
