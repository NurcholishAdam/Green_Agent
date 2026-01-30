class CarbonEstimator:
    def __init__(self, grid_intensity_g_kwh: float, pue: float):
        self.grid = grid_intensity_g_kwh
        self.pue = pue

    def estimate(self, energy_wh: float) -> float:
        kwh = energy_wh / 1000.0
        return (kwh * self.grid * self.pue) / 1000.0
