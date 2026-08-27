"""
carbon_api.py

Enhanced Carbon Intensity API module.

Provides:
- A stub implementation for testing (with configurable daily/seasonal patterns).
- A real implementation that queries ElectricityMap (or other providers) with caching,
  error handling, fallback, and region support.
- A CarbonIntensityFetcher adapter that exposes async methods compatible with
  FlexGen/MODP modules (get_current_intensity, forecast_carbon_prices).

Usage:
    Set environment variable CARBON_API_MODE=stub|real.
    Optionally set CARBON_API_KEY, CARBON_API_REGION, CARBON_API_CACHE_TTL.
"""

import os
import time
import random
import json
import logging
import asyncio
from typing import List, Tuple, Optional, Dict, Any
from abc import ABC, abstractmethod
from datetime import datetime, timedelta

try:
    import requests
except ImportError:
    requests = None
    logging.warning("requests not installed; RealCarbonAPI will not work.")

try:
    import numpy as np
    NUMPY_AVAILABLE = True
except ImportError:
    NUMPY_AVAILABLE = False
    import math
    def sin(x):
        return math.sin(x)
    np = None


# ----------------------------------------------------------------------
# Abstract base class
# ----------------------------------------------------------------------

class CarbonAPI(ABC):
    """Interface for carbon intensity providers."""

    @abstractmethod
    def get_current(self) -> float:
        """Return current carbon intensity in gCO2/kWh."""
        pass

    @abstractmethod
    def get_forecast(self, minutes: int = 60) -> List[Tuple[float, float]]:
        """Return list of (timestamp, intensity) for the next `minutes`."""
        pass


# ----------------------------------------------------------------------
# Stub Implementation (Enhanced with configurable patterns)
# ----------------------------------------------------------------------

class CarbonAPIStub(CarbonAPI):
    """
    Simulated carbon intensity with daily and seasonal cycles plus noise.
    Can be configured via constructor parameters.
    """
    def __init__(
        self,
        base_intensity: float = 200.0,
        daily_amplitude: float = 80.0,
        seasonal_amplitude: float = 30.0,
        noise_std: float = 5.0,
        start_time: Optional[float] = None,
    ):
        """
        Args:
            base_intensity: Mean intensity (gCO2/kWh).
            daily_amplitude: Amplitude of daily oscillation.
            seasonal_amplitude: Amplitude of seasonal (yearly) oscillation.
            noise_std: Standard deviation of Gaussian noise.
            start_time: Unix timestamp for simulation start (default: now).
        """
        self.base = base_intensity
        self.daily_amp = daily_amplitude
        self.seasonal_amp = seasonal_amplitude
        self.noise_std = noise_std
        self._start_time = start_time or time.time()
        self._rng = random.Random(42)  # deterministic for testing if needed

    def _simulate(self, timestamp: float) -> float:
        """Compute intensity at a given timestamp."""
        # Hours since start
        hours = (timestamp - self._start_time) / 3600.0
        # Daily cycle (24-hour period)
        if NUMPY_AVAILABLE:
            daily = self.daily_amp * 0.5 * (1 + np.sin(2 * np.pi * hours / 24))
            seasonal = self.seasonal_amp * 0.5 * (1 + np.sin(2 * np.pi * hours / (365 * 24)))
            noise = self._rng.gauss(0, self.noise_std)
        else:
            daily = self.daily_amp * 0.5 * (1 + sin(2 * 3.14159 * hours / 24))
            seasonal = self.seasonal_amp * 0.5 * (1 + sin(2 * 3.14159 * hours / (365 * 24)))
            noise = self._rng.gauss(0, self.noise_std)
        return max(50.0, self.base + daily + seasonal + noise)

    def get_current(self) -> float:
        return self._simulate(time.time())

    def get_forecast(self, minutes: int = 60) -> List[Tuple[float, float]]:
        now = time.time()
        step = 10  # minutes between forecast points
        forecast = []
        for i in range(0, minutes, step):
            ts = now + i * 60
            forecast.append((ts, self._simulate(ts)))
        return forecast


# ----------------------------------------------------------------------
# Real Implementation (ElectricityMap)
# ----------------------------------------------------------------------

class RealCarbonAPI(CarbonAPI):
    """
    Queries the ElectricityMap API for real‑time and forecast carbon intensity.
    Requires an API key (free tier available at https://www.electricitymap.org/).

    Environment variables:
        CARBON_API_KEY: Your API key.
        CARBON_API_REGION: Region code (e.g., 'DE', 'FR', 'US-CA'). Default: 'DE'.
        CARBON_API_CACHE_TTL: Cache TTL in seconds (default: 300).
    """
    BASE_URL = "https://api.electricitymap.org/v3"

    def __init__(
        self,
        api_key: Optional[str] = None,
        region: Optional[str] = None,
        cache_ttl: int = 300,
    ):
        if requests is None:
            raise ImportError("requests library is required for RealCarbonAPI.")

        self.api_key = api_key or os.environ.get("CARBON_API_KEY")
        if not self.api_key:
            raise ValueError("API key is required. Set CARBON_API_KEY env var.")

        self.region = region or os.environ.get("CARBON_API_REGION", "DE")
        self.cache_ttl = cache_ttl or int(os.environ.get("CARBON_API_CACHE_TTL", 300))

        self._cache = {}  # key: (endpoint, params) -> (timestamp, data)
        self.logger = logging.getLogger(__name__)

    def _api_call(self, endpoint: str, params: Optional[Dict] = None) -> Optional[Dict]:
        """Make a request to the API with caching."""
        cache_key = (endpoint, frozenset((params or {}).items()))
        now = time.time()

        if cache_key in self._cache:
            ts, data = self._cache[cache_key]
            if now - ts < self.cache_ttl:
                return data

        url = f"{self.BASE_URL}/{endpoint}"
        headers = {"auth-token": self.api_key}
        try:
            resp = requests.get(url, headers=headers, params=params, timeout=10)
            resp.raise_for_status()
            data = resp.json()
            self._cache[cache_key] = (now, data)
            return data
        except requests.exceptions.RequestException as e:
            self.logger.error(f"API request failed: {e}")
            # Return cached data even if expired (fallback)
            if cache_key in self._cache:
                self.logger.warning("Using stale cached data.")
                return self._cache[cache_key][1]
            return None

    def get_current(self) -> float:
        """Get current carbon intensity for the configured region."""
        endpoint = f"carbon-intensity/latest"
        params = {"zone": self.region}
        data = self._api_call(endpoint, params)
        if data and "carbonIntensity" in data:
            return data["carbonIntensity"]
        # Fallback to a reasonable default
        self.logger.warning("Using fallback intensity (200 gCO2/kWh).")
        return 200.0

    def get_forecast(self, minutes: int = 60) -> List[Tuple[float, float]]:
        """Get forecast for the next `minutes` (max 24h)."""
        endpoint = f"carbon-intensity/forecast"
        params = {"zone": self.region}
        data = self._api_call(endpoint, params)
        if not data or "forecast" not in data:
            # Fallback: use stub simulation with base parameters
            self.logger.warning("Forecast API failed; using stub simulation.")
            stub = CarbonAPIStub()
            return stub.get_forecast(minutes)

        forecast = []
        for entry in data["forecast"]:
            timestamp = datetime.fromisoformat(entry["datetime"].replace("Z", "+00:00")).timestamp()
            intensity = entry["carbonIntensity"]
            # Only include data within the requested horizon
            if timestamp <= time.time() + minutes * 60:
                forecast.append((timestamp, intensity))
            else:
                break
        return forecast


# ----------------------------------------------------------------------
# Factory to get the appropriate implementation
# ----------------------------------------------------------------------

def get_carbon_api(mode: Optional[str] = None, **kwargs) -> CarbonAPI:
    """
    Factory function that returns a CarbonAPI instance based on the mode.
    Mode can be 'stub' or 'real'. If not provided, reads CARBON_API_MODE env var.
    Additional kwargs are passed to the constructor.
    """
    mode = mode or os.environ.get("CARBON_API_MODE", "stub")
    if mode.lower() == "real":
        return RealCarbonAPI(**kwargs)
    else:
        # Use stub mode, optionally pass any stub parameters
        return CarbonAPIStub(**kwargs)


# ----------------------------------------------------------------------
# CarbonIntensityFetcher adapter for FlexGen / MODP
# ----------------------------------------------------------------------

class CarbonIntensityFetcher:
    """
    Asynchronous wrapper around CarbonAPI that provides the interface expected
    by FlexGen and MODP modules:

    - `async get_current_intensity()` -> float
    - `async forecast_carbon_prices(hours)` -> dict with 'status' and 'predictions'
    """
    def __init__(self, api: Optional[CarbonAPI] = None, mode: Optional[str] = None, **kwargs):
        self.api = api or get_carbon_api(mode, **kwargs)

    async def get_current_intensity(self) -> float:
        """Return current carbon intensity asynchronously."""
        # Run blocking call in a thread to avoid blocking the event loop
        return await asyncio.to_thread(self.api.get_current)

    async def forecast_carbon_prices(self, hours: int = 24) -> Dict[str, Any]:
        """
        Return a forecast in the format expected by FlexGen MODP:
        {
            'status': 'success',
            'predictions': [float, ...],   # hourly average intensity for next `hours`
            'timestamps': [float, ...],    # optional Unix timestamps
            'region': str,                 # optional
        }
        """
        minutes = hours * 60
        # Fetch forecast with underlying API (blocking)
        forecast = await asyncio.to_thread(self.api.get_forecast, minutes)

        if not forecast:
            return {"status": "insufficient_data", "predictions": []}

        # Convert list of (timestamp, intensity) to hourly averages
        hourly = {}
        for ts, intensity in forecast:
            hour_bucket = int(ts // 3600) * 3600
            hourly.setdefault(hour_bucket, []).append(intensity)

        timestamps = sorted(hourly.keys())
        predictions = [sum(hourly[ts]) / len(hourly[ts]) for ts in timestamps]

        return {
            "status": "success",
            "predictions": predictions,
            "timestamps": timestamps,
            "region": getattr(self.api, 'region', 'unknown'),
        }


# ----------------------------------------------------------------------
# Example usage (when run as script)
# ----------------------------------------------------------------------

if __name__ == "__main__":
    # Setup logging
    logging.basicConfig(level=logging.INFO)

    # Example: use stub by default
    api = get_carbon_api()
    print(f"Current intensity (stub): {api.get_current():.1f} gCO2/kWh")
    print("Forecast (stub) first 5 entries:")
    for ts, intensity in api.get_forecast(30)[:5]:
        print(f"  {datetime.fromtimestamp(ts)}: {intensity:.1f}")

    # Demonstrate async fetcher for FlexGen
    fetcher = CarbonIntensityFetcher(api)

    async def async_demo():
        current = await fetcher.get_current_intensity()
        forecast = await fetcher.forecast_carbon_prices(hours=6)
        print(f"\nAsync current intensity: {current:.1f}")
        print(f"Async forecast status: {forecast['status']}, points: {len(forecast.get('predictions', []))}")
        if forecast['predictions']:
            print(f"First 3 hourly predictions: {[round(p,1) for p in forecast['predictions'][:3]]}")

    asyncio.run(async_demo())

    # If you have a real API key, uncomment to test:
    # api_real = get_carbon_api(mode="real", region="DE")
    # print(f"Current intensity (real): {api_real.get_current():.1f}")
    # print("Forecast (real) first 5 entries:")
    # for ts, intensity in api_real.get_forecast(60)[:5]:
    #     print(f"  {datetime.fromtimestamp(ts)}: {intensity:.1f}")
