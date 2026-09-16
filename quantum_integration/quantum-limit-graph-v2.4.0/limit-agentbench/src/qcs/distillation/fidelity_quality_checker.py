"""
FidelityQualityChecker — verify distillation fidelity against baseline.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Optional


@dataclass
class FidelityCheckResult:
    """Outcome of a fidelity check."""
    fidelity: Optional[float]
    threshold: float
    passes: bool
    reasons: List[str]


class FidelityQualityChecker:
    """
    Compares student fidelity against a required threshold.

    Fidelity is defined as the fraction of student outputs that match
    the teacher's top output, per unit of test set.
    """

    def __init__(self, *, min_fidelity: float = 0.9):
        self.min_fidelity = min_fidelity

    def check(
        self,
        *,
        teacher_outputs: List[str],
        student_outputs: List[str],
    ) -> FidelityCheckResult:
        if not teacher_outputs or len(teacher_outputs) != len(student_outputs):
            return FidelityCheckResult(
                fidelity=None,
                threshold=self.min_fidelity,
                passes=False,
                reasons=["output length mismatch or empty"],
            )
        matches = sum(
            1 for t, s in zip(teacher_outputs, student_outputs) if t == s
        )
        fidelity = matches / len(teacher_outputs)
        passes = fidelity >= self.min_fidelity
        reasons = (
            [f"fidelity {fidelity:.3f} meets threshold {self.min_fidelity}"]
            if passes else
            [f"fidelity {fidelity:.3f} below threshold {self.min_fidelity}"]
        )
        return FidelityCheckResult(
            fidelity=fidelity,
            threshold=self.min_fidelity,
            passes=passes,
            reasons=reasons,
        )
