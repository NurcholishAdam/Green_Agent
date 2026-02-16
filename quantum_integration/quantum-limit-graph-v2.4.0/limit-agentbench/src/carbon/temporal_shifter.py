from typing import Dict, Tuple


class TemporalShifter:
    """
    Suggest optimal delay for carbon reduction.
    """

    def suggest(
        self,
        current_intensity: float,
        forecast: Dict[int, float],
        energy_kwh: float,
    ) -> Tuple[int, float]:

        current_carbon = current_intensity * energy_kwh / 1000

        best_hour = 0
        best_saving = 0.0

        for hour, future_intensity in forecast.items():
            future_carbon = future_intensity * energy_kwh / 1000
            saving = current_carbon - future_carbon

            if saving > best_saving:
                best_saving = saving
                best_hour = hour

        return best_hour, best_saving
