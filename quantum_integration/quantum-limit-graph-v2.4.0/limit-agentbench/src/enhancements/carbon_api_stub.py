"""
carbon_api_stub.py

Mock carbon intensity API for testing. Replace with real API later.
"""
import time
import random
from typing import List, Tuple


class CarbonAPIStub:
    def __init__(self, base_intensity: float = 200.0, volatility: float = 50.0):
        self.base = base_intensity
        self.volatility = volatility
        self._start_time = time.time()

    def get_current(self) -> float:
        """Returns a simulated gCO2/kWh that oscillates."""
        # Sine wave with noise to simulate daily patterns
        cycle = (time.time() - self._start_time) / 3600.0  # hours
        intensity = self.base + self.volatility * (0.5 * (1 + (2 * 3.14159 * cycle / 12))) + random.gauss(0, 5)
        return max(50.0, intensity)

    def get_forecast(self, minutes: int = 60) -> List[Tuple[float, float]]:
        """Returns list of (timestamp, intensity) for future minutes."""
        now = time.time()
        forecast = []
        for i in range(0, minutes, 10):  # every 10 minutes
            ts = now + i * 60
            cycle = (ts - self._start_time) / 3600.0
            intensity = self.base + self.volatility * (0.5 * (1 + (2 * 3.14159 * cycle / 12))) + random.gauss(0, 5)
            forecast.append((ts, max(50.0, intensity)))
        return forecast
