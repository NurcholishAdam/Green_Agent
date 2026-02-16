import random
from typing import Dict


class CarbonForecast:
    """
    Carbon intensity forecast provider.
    Replace with real grid API in production.
    """

    def current_intensity(self) -> float:
        return random.uniform(300, 600)

    def forecast_next_hours(self, hours: int = 4) -> Dict[int, float]:
        return {
            h: random.uniform(150, 500)
            for h in range(1, hours + 1)
        }
