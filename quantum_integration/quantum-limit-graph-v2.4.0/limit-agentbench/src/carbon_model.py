"""
src/carbon_model.py — Calibrated order-of-magnitude energy/carbon estimator.

⚠  These constants are tuned so that a BERT-base fine-tune on 100k samples
    lands within an order of magnitude of published estimates (Strubell 2019,
    Patterson 2021). They are NOT a substitute for measuring actual runs.
    See KNOWN_LIMITATIONS.md §1.

  Effective FLOP/J (not peak; includes memory traffic, launch overhead,
  idle time):
      fine_tuning   1e7
      inference     1e10
      agent         1e8
      benchmark     1e9

  Grid intensity defaults to 400 gCO₂/kWh — an estimate, not a live signal.
"""
from __future__ import annotations

from src.types import TaskKind

__all__ = ["CarbonModel"]


_EFFECTIVE_FLOPS_PER_JOULE: dict[TaskKind, float] = {
    TaskKind.FINE_TUNING: 1e7,
    TaskKind.INFERENCE:   1e10,
    TaskKind.AGENT:       1e8,
    TaskKind.BENCHMARK:   1e9,
}


class CarbonModel:
    def __init__(
        self,
        *,
        grid_intensity_g_per_kwh: float = 400.0,
    ) -> None:
        if grid_intensity_g_per_kwh <= 0:
            raise ValueError("grid_intensity_g_per_kwh must be > 0")
        self._intensity = float(grid_intensity_g_per_kwh)

    @property
    def grid_intensity_g_per_kwh(self) -> float:
        return self._intensity

    def estimate_energy_kwh(self, *, task_kind: TaskKind, flops: float) -> float:
        if flops <= 0:
            return 0.0
        rate = _EFFECTIVE_FLOPS_PER_JOULE[task_kind]
        joules = flops / rate
        return joules / 3_600_000.0

    def estimate_carbon_kg(
        self, *,
        energy_kwh: float,
        grid_intensity_g_per_kwh: float | None = None,
    ) -> float:
        if energy_kwh <= 0:
            return 0.0
        intensity = (
            self._intensity
            if grid_intensity_g_per_kwh is None
            else float(grid_intensity_g_per_kwh)
        )
        return energy_kwh * intensity / 1000.0
