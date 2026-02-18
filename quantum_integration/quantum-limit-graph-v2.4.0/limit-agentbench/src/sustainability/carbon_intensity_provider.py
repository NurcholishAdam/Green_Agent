# sustainability/carbon_intensity_provider.py

import logging
from typing import Optional

logger = logging.getLogger(__name__)


class CarbonIntensityProvider:
    """
    Provides current carbon intensity (gCO2/kWh).
    Can be extended to fetch from real-time API.
    """

    def __init__(self, region: Optional[str] = None):
        self.region = region
        self._current_intensity = 0.0

    def update_manual(self, value: float):
        if value < 0:
            raise ValueError("Carbon intensity must be non-negative.")

        self._current_intensity = value
        logger.info(f"Carbon intensity updated: {value} gCO2/kWh")

    def get_current_intensity(self) -> float:
        return self._current_intensity

    def is_grid_dirty(self, threshold: float = 400.0) -> bool:
        return self._current_intensity > threshold
