# metrics/quantum_efficiency.py

import logging

logger = logging.getLogger(__name__)


class QuantumEfficiencyMetric:
    """
    Computes Energy-per-Bit inspired metric:

    E_eff = TaskCompletionRatio / QuantumEnergyConsumed
    """

    def __init__(self):
        self._quantum_energy = 0.0
        self._task_completion_ratio = 0.0

    def add_quantum_energy(self, energy_joules: float):
        if energy_joules < 0:
            raise ValueError("Energy must be non-negative.")
        self._quantum_energy += energy_joules

    def set_task_completion_ratio(self, ratio: float):
        if not 0 <= ratio <= 1:
            raise ValueError("Task completion ratio must be between 0 and 1.")
        self._task_completion_ratio = ratio

    def compute(self) -> float:
        if self._quantum_energy == 0:
            logger.warning("Quantum energy is zero. Efficiency undefined.")
            return 0.0

        efficiency = self._task_completion_ratio / self._quantum_energy
        logger.info(f"Quantum efficiency computed: {efficiency}")
        return efficiency

    def reset(self):
        self._quantum_energy = 0.0
        self._task_completion_ratio = 0.0
