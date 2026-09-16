"""
QuantumDistiller — orchestrate teacher→student distillation experiments.
"""

from __future__ import annotations

import logging
import uuid
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Optional

logger = logging.getLogger(__name__)


@dataclass
class DistillationResult:
    """Result of a single distillation experiment."""
    distillation_id: str
    task_id: str
    teacher_backend: Optional[str] = None
    student_precision: str = "int8"
    teacher_quality: Optional[float] = None
    student_quality: Optional[float] = None
    quality_retention: Optional[float] = None
    teacher_energy_kwh: float = 0.0
    student_energy_kwh: float = 0.0
    energy_reduction_pct: float = 0.0
    teacher_co2e_kg: float = 0.0
    student_co2e_kg: float = 0.0
    signature: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    def to_dict(self) -> Dict[str, Any]:
        out = asdict(self)
        out["at"] = self.at.isoformat()
        return out


class QuantumDistiller:
    """
    Runs teacher→student distillation experiments.

    The distiller does not implement training — it orchestrates:

      1. Submitting a teacher (quantum or large classical) job.
      2. Submitting a student (distilled classical) job.
      3. Comparing quality, energy, and carbon.

    Callers supply the teacher and student callables. The distiller
    records results and returns a `DistillationResult`.
    """

    def __init__(self):
        self._history: List[DistillationResult] = []

    async def run(
        self,
        *,
        task_id: str,
        teacher_callable: Callable[[], Any],
        student_callable: Callable[[], Any],
        teacher_backend: Optional[str] = None,
        student_precision: str = "int8",
        metadata: Optional[Dict[str, Any]] = None,
    ) -> DistillationResult:
        distillation_id = uuid.uuid4().hex[:12]

        teacher = await _maybe_await(teacher_callable())
        student = await _maybe_await(student_callable())

        teacher_quality = _extract_quality(teacher)
        student_quality = _extract_quality(student)
        teacher_energy = _extract_energy(teacher)
        student_energy = _extract_energy(student)
        teacher_co2e = _extract_co2e(teacher)
        student_co2e = _extract_co2e(student)

        retention = (
            student_quality / teacher_quality
            if teacher_quality and teacher_quality > 0 else None
        )
        energy_reduction = (
            max(0.0, (teacher_energy - student_energy) / teacher_energy * 100)
            if teacher_energy > 0 else 0.0
        )

        result = DistillationResult(
            distillation_id=distillation_id,
            task_id=task_id,
            teacher_backend=teacher_backend,
            student_precision=student_precision,
            teacher_quality=teacher_quality,
            student_quality=student_quality,
            quality_retention=retention,
            teacher_energy_kwh=teacher_energy,
            student_energy_kwh=student_energy,
            energy_reduction_pct=energy_reduction,
            teacher_co2e_kg=teacher_co2e,
            student_co2e_kg=student_co2e,
            metadata=dict(metadata or {}),
        )
        self._history.append(result)
        logger.info(
            f"Distillation {distillation_id}: "
            f"retention={retention}, energy_reduction={energy_reduction:.1f}%"
        )
        return result

    def history(self) -> List[Dict[str, Any]]:
        return [r.to_dict() for r in self._history]


# --- Helpers ---

def _extract_quality(obj: Any) -> Optional[float]:
    for attr in ("quality", "accuracy", "fidelity"):
        v = getattr(obj, attr, None)
        if isinstance(v, (int, float)):
            return float(v)
    if isinstance(obj, dict):
        for k in ("quality", "accuracy", "fidelity"):
            if isinstance(obj.get(k), (int, float)):
                return float(obj[k])
    return None


def _extract_energy(obj: Any) -> float:
    for attr in (
        "estimated_energy_kwh", "energy_kwh", "energy_consumed_kwh",
    ):
        v = getattr(obj, attr, None)
        if isinstance(v, (int, float)):
            return float(v)
    return 0.0


def _extract_co2e(obj: Any) -> float:
    for attr in ("estimated_co2e_kg", "carbon_emitted_kg", "co2e_kg"):
        v = getattr(obj, attr, None)
        if isinstance(v, (int, float)):
            return float(v)
    return 0.0


async def _maybe_await(value: Any) -> Any:
    if hasattr(value, "__await__"):
        return await value
    return value
