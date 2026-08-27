"""
MODP‑based time‑shifting scheduler.
Decides whether to run a workload now or defer based on carbon forecasts.
Uses a simple rule‑based approximation of MODP; real implementation would
use dynamic programming or reinforcement learning.
"""

import asyncio
from typing import Dict, Any, Optional, Tuple
from datetime import datetime, timedelta

from ..data_integration.carbon_intensity import CarbonIntensityFetcher
from ..logger import logger


class MODPScheduler:
    def __init__(self, carbon_fetcher: Optional[CarbonIntensityFetcher] = None):
        self.carbon_fetcher = carbon_fetcher

    async def get_carbon_forecast(self, hours: int = 6) -> list:
        """Return predicted carbon intensity for next `hours` hours."""
        if self.carbon_fetcher:
            forecast = await self.carbon_fetcher.forecast_carbon_prices(hours=hours)
            if forecast.get('status') == 'success':
                return forecast['predictions']
        # Fallback: constant carbon intensity
        return [400] * hours

    async def decide(self, workload, node) -> Tuple[str, int]:
        """
        Decide whether to run now or defer, and how long to defer.
        Returns (action, delay_hours).
        """
        # Get current carbon intensity and forecast
        current_carbon = await self.carbon_fetcher.get_current_intensity() if self.carbon_fetcher else 400
        forecast = await self.get_carbon_forecast(6)

        # If current intensity is low, run now
        if current_carbon < 300:
            return ("run_now", 0)

        # Find best future hour with lower carbon
        min_carbon = min(forecast)
        min_idx = forecast.index(min_carbon)
        if min_carbon < current_carbon - 50:
            return ("defer", min_idx)
        else:
            return ("run_now", 0)
