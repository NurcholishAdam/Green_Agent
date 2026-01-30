class CarbonEstimator:
    def __init__(self, grid_intensity_g_kwh=400.0, pue=1.0):
        self.grid_intensity = grid_intensity_g_kwh
        self.pue = pue

    def estimate(self, energy_wh: float) -> float:
        # Convert Wh → kWh
        kwh = energy_wh / 1000.0
        adjusted = kwh * self.pue
        return (adjusted * self.grid_intensity) / 1000.0
