"""
Energy and carbon budget constraints for green benchmarking.
"""

from typing import Dict, Tuple, Optional


def check_energy_budget(
    metrics: Dict[str, float],
    max_energy_wh: Optional[float] = None,
    max_carbon_kg: Optional[float] = None,
) -> Tuple[bool, str]:
    """
    Check whether collected metrics satisfy energy/carbon constraints.

    Returns
    -------
    (bool, str)
        Whether constraints passed and reason code
    """
    energy = metrics.get("energy", 0.0)
    carbon = metrics.get("carbon", 0.0)

    if max_energy_wh is not None and energy > max_energy_wh:
        return False, "energy_budget_exceeded"

    if max_carbon_kg is not None and carbon > max_carbon_kg:
        return False, "carbon_budget_exceeded"

    return True, "ok"
