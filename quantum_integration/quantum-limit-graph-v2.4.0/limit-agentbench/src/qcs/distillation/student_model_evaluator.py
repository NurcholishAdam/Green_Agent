"""
StudentModelEvaluator — quality/energy comparison for distilled models.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional


@dataclass
class EvaluationResult:
    """A quality/energy evaluation of a student model."""
    model_id: str
    quality: Optional[float]
    energy_kwh: float
    co2e_kg: float
    passes: bool
    reasons: List[str]


class StudentModelEvaluator:
    """
    Evaluates a student model against quality and resource thresholds.
    """

    def __init__(
        self,
        *,
        min_quality_retention: float = 0.85,
        max_energy_ratio: float = 0.7,
    ):
        self.min_quality_retention = min_quality_retention
        self.max_energy_ratio = max_energy_ratio

    def evaluate(
        self,
        *,
        model_id: str,
        teacher_quality: Optional[float],
        student_quality: Optional[float],
        teacher_energy_kwh: float,
        student_energy_kwh: float,
        student_co2e_kg: float = 0.0,
    ) -> EvaluationResult:
        reasons: List[str] = []
        retention = (
            student_quality / teacher_quality
            if teacher_quality and teacher_quality > 0 else None
        )
        energy_ratio = (
            student_energy_kwh / teacher_energy_kwh
            if teacher_energy_kwh > 0 else None
        )

        if retention is None:
            reasons.append("cannot compute retention")
        elif retention < self.min_quality_retention:
            reasons.append(
                f"retention {retention:.3f} below "
                f"threshold {self.min_quality_retention}"
            )

        if energy_ratio is None:
            reasons.append("cannot compute energy ratio")
        elif energy_ratio > self.max_energy_ratio:
            reasons.append(
                f"energy ratio {energy_ratio:.3f} above "
                f"threshold {self.max_energy_ratio}"
            )

        return EvaluationResult(
            model_id=model_id,
            quality=student_quality,
            energy_kwh=student_energy_kwh,
            co2e_kg=student_co2e_kg,
            passes=not reasons,
            reasons=reasons or ["student passes all thresholds"],
        )
