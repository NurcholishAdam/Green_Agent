"""
carbon_api.py

Enhanced Carbon Intensity API module.

Provides:
- A stub implementation for testing (with configurable daily/seasonal patterns).
- A real implementation that queries ElectricityMap (or other providers) with caching,
  error handling, fallback, and region support.
- A CarbonIntensityFetcher adapter that exposes async methods compatible with
  FlexGen/MODP modules (get_current_intensity, forecast_carbon_prices).

NEW ENHANCEMENTS (v2.0):
- CarbonOffsetBroker: purchase carbon offsets when intensity exceeds threshold.
- Safety checks: is_safe() method in CarbonIntensityFetcher.
- Feedback loop for stub: update_from_feedback() to adjust simulation.
- ChaosCarbonAPI wrapper for resilience testing.
- Explainability metadata in forecast responses.
- FederatedCarbonAggregator for combining data from multiple deployments.
- Simple temporal safety rules (e.g., intensity must not exceed limit for too long).

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
        self._recent_feedback = []  # store feedback for adaptation

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

    def update_from_feedback(self, actual_intensity: float):
        """
        Adjust the stub's baseline parameters based on feedback from real data.
        This is a simple online learning mechanism.
        """
        self._recent_feedback.append(actual_intensity)
        if len(self._recent_feedback) > 10:
            self._recent_feedback.pop(0)
        if self._recent_feedback:
            self.base = sum(self._recent_feedback) / len(self._recent_feedback)
            # Reduce noise to make simulation more stable after feedback
            self.noise_std = max(1.0, self.noise_std * 0.95)


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
# Chaos Wrapper for Resilience Testing
# ----------------------------------------------------------------------

class ChaosCarbonAPI(CarbonAPI):
    """
    Wraps any CarbonAPI instance and injects random failures to test
    downstream resilience. Useful for chaos engineering experiments.

    Args:
        base_api: Underlying CarbonAPI instance.
        failure_probability: Probability of failure on any call (0-1).
        delay_mean: Mean delay (seconds) to add before returning.
        delay_std: Standard deviation of delay.
        seed: Random seed for reproducibility.
    """
    def __init__(
        self,
        base_api: CarbonAPI,
        failure_probability: float = 0.1,
        delay_mean: float = 0.5,
        delay_std: float = 0.2,
        seed: Optional[int] = None,
    ):
        self.api = base_api
        self.failure_prob = failure_probability
        self.delay_mean = delay_mean
        self.delay_std = delay_std
        self._rng = random.Random(seed)

    def _maybe_fail_or_delay(self):
        # Simulate delay
        if self.delay_mean > 0:
            time.sleep(max(0, self._rng.gauss(self.delay_mean, self.delay_std)))
        # Simulate failure
        if self._rng.random() < self.failure_prob:
            raise Exception("Simulated carbon API failure")

    def get_current(self) -> float:
        self._maybe_fail_or_delay()
        return self.api.get_current()

    def get_forecast(self, minutes: int = 60) -> List[Tuple[float, float]]:
        self._maybe_fail_or_delay()
        return self.api.get_forecast(minutes)


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
# Carbon Offset Broker (for purchasing carbon credits)
# ----------------------------------------------------------------------

class CarbonOffsetBroker:
    """
    Handles purchasing of carbon offsets when carbon intensity exceeds a threshold.
    This is a stub implementation that simulates the purchase and returns a receipt.
    In production, you would integrate with a real marketplace (e.g., Patch, Cloverly).
    """
    def __init__(
        self,
        threshold_intensity: float = 400.0,
        cost_per_kg_co2: float = 0.10,
        api_key: Optional[str] = None,
    ):
        self.threshold = threshold_intensity
        self.cost_per_kg = cost_per_kg_co2
        self.api_key = api_key or os.environ.get("CARBON_OFFSET_API_KEY")
        self.total_offset_kg = 0.0
        self.total_cost = 0.0

    def should_offset(self, current_intensity: float) -> bool:
        """Return True if current intensity exceeds threshold and offsetting is recommended."""
        return current_intensity > self.threshold

    def purchase_offsets(self, co2_kg: float, metadata: Optional[Dict] = None) -> Dict:
        """
        Simulate purchasing offsets for the given amount of CO2.
        Returns a receipt with transaction details.
        """
        if co2_kg <= 0:
            return {"status": "no_action", "message": "No offset needed"}
        cost = co2_kg * self.cost_per_kg
        self.total_offset_kg += co2_kg
        self.total_cost += cost
        receipt = {
            "status": "success",
            "offset_kg": co2_kg,
            "cost_usd": cost,
            "timestamp": time.time(),
            "provider": "mock_offset_provider",
            "metadata": metadata or {},
        }
        return receipt


# ----------------------------------------------------------------------
# Federated Aggregator (combines data from multiple deployments)
# ----------------------------------------------------------------------

class FederatedCarbonAggregator:
    """
    Aggregates carbon intensity data from multiple sources/deployments
    using a simple average (FedAvg-like) approach. This can be extended
    with more sophisticated federated learning algorithms.
    """
    def __init__(self):
        self.participant_data: Dict[str, List[float]] = {}
        self.last_aggregation_time = None

    def add_participant_data(self, participant_id: str, intensities: List[float]):
        """Add a list of recent intensity readings from a participant."""
        self.participant_data[participant_id] = intensities

    def aggregate(self) -> Dict:
        """
        Compute the federated average of the latest readings from all participants.
        Returns a dict with summary statistics.
        """
        if not self.participant_data:
            return {"status": "no_data", "average": None}

        all_values = []
        for pid, values in self.participant_data.items():
            if values:
                # Use the most recent value from each participant
                all_values.append(values[-1])

        if not all_values:
            return {"status": "no_data", "average": None}

        avg = sum(all_values) / len(all_values)
        self.last_aggregation_time = time.time()
        return {
            "status": "success",
            "average": avg,
            "count": len(all_values),
            "timestamp": self.last_aggregation_time,
        }


# ----------------------------------------------------------------------
# CarbonIntensityFetcher adapter for FlexGen / MODP
# ----------------------------------------------------------------------

class CarbonIntensityFetcher:
    """
    Asynchronous wrapper around CarbonAPI that provides the interface expected
    by FlexGen and MODP modules:

    - `async get_current_intensity()` -> float
    - `async forecast_carbon_prices(hours)` -> dict with 'status' and 'predictions'
    - `async is_safe(threshold)` -> bool for safety monitoring
    - `async purchase_offsets(co2_kg)` -> dict (optional)
    - `update_feedback(actual_intensity)` -> None (for learning)
    """
    def __init__(self, api: Optional[CarbonAPI] = None, mode: Optional[str] = None, **kwargs):
        self.api = api or get_carbon_api(mode, **kwargs)
        self.offset_broker = CarbonOffsetBroker()  # default threshold
        self.federated = FederatedCarbonAggregator()  # for future use

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
            'source': str,                 # 'stub' or 'real' (added for XAI)
            'cache_age_sec': float,        # optional, if from cache
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

        # Add metadata for explainability
        source = "stub" if isinstance(self.api, CarbonAPIStub) else "real"
        metadata = {
            "source": source,
            "region": getattr(self.api, 'region', 'unknown'),
            "forecast_points": len(predictions),
            "cache_age_sec": 0.0,  # could be computed if caching is used
        }

        return {
            "status": "success",
            "predictions": predictions,
            "timestamps": timestamps,
            "region": metadata["region"],
            "source": metadata["source"],
            "metadata": metadata,
        }

    async def is_safe(self, threshold: float = 500.0) -> bool:
        """
        Check if current carbon intensity is below a safety threshold.
        Returns True if safe, False otherwise.
        """
        intensity = await self.get_current_intensity()
        return intensity <= threshold

    async def purchase_offsets(self, co2_kg: float) -> Dict:
        """Purchase carbon offsets for a given amount of CO2."""
        return await asyncio.to_thread(self.offset_broker.purchase_offsets, co2_kg)

    def update_feedback(self, actual_intensity: float):
        """
        Provide feedback to the underlying API (if it's a stub) to improve its simulation.
        This is a form of active learning.
        """
        if isinstance(self.api, CarbonAPIStub):
            self.api.update_from_feedback(actual_intensity)

    async def run_safety_check(self, max_consecutive_minutes: int = 30, threshold: float = 500.0) -> Dict:
        """
        Simple temporal logic check: returns False if carbon intensity has exceeded
        the threshold for more than max_consecutive_minutes (based on forecast).
        """
        # Use forecast to estimate future intensity
        forecast = await asyncio.to_thread(self.api.get_forecast, max_consecutive_minutes)
        if not forecast:
            return {"safe": False, "reason": "No forecast data"}

        # Check if all forecast points are above threshold
        consecutive_high = 0
        for ts, intensity in forecast:
            if intensity > threshold:
                consecutive_high += 10  # forecast interval is 10 minutes in stub
            else:
                consecutive_high = 0
            if consecutive_high >= max_consecutive_minutes:
                return {"safe": False, "reason": f"Intensity above {threshold} for too long"}

        return {"safe": True}


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
        # Current intensity
        current = await fetcher.get_current_intensity()
        print(f"\nAsync current intensity: {current:.1f}")

        # Forecast
        forecast = await fetcher.forecast_carbon_prices(hours=6)
        print(f"Async forecast status: {forecast['status']}, source: {forecast['metadata']['source']}")
        if forecast['predictions']:
            print(f"First 3 hourly predictions: {[round(p,1) for p in forecast['predictions'][:3]]}")

        # Safety check
        safe = await fetcher.is_safe(threshold=450)
        print(f"Safety check (threshold 450): {'safe' if safe else 'unsafe'}")

        # Temporal safety check
        temporal = await fetcher.run_safety_check(max_consecutive_minutes=20, threshold=450)
        print(f"Temporal safety check: {temporal}")

        # Offset purchase
        receipt = await fetcher.purchase_offsets(co2_kg=2.5)
        print(f"Offset purchase: {receipt['status']}, cost: ${receipt['cost_usd']:.2f}")

        # Feedback update (simulate real observation)
        fetcher.update_feedback(210.0)
        print("Feedback update applied to stub.")

        # Demonstrate chaos wrapper
        chaos_api = ChaosCarbonAPI(api, failure_probability=0.3, delay_mean=0.1)
        chaos_fetcher = CarbonIntensityFetcher(chaos_api)
        try:
            chaos_current = await chaos_fetcher.get_current_intensity()
            print(f"Chaos API current intensity: {chaos_current:.1f}")
        except Exception as e:
            print(f"Chaos API failed as expected: {e}")

        # Federated aggregation demo
        fed = FederatedCarbonAggregator()
        fed.add_participant_data("deployment1", [300, 310, 320])
        fed.add_participant_data("deployment2", [280, 290, 295])
        agg = fed.aggregate()
        print(f"Federated aggregation: {agg}")

    asyncio.run(async_demo())

    # If you have a real API key, uncomment to test:
    # api_real = get_carbon_api(mode="real", region="DE")
    # print(f"Current intensity (real): {api_real.get_current():.1f}")
    # print("Forecast (real) first 5 entries:")
    # for ts, intensity in api_real.get_forecast(60)[:5]:
    #     print(f"  {datetime.fromtimestamp(ts)}: {intensity:.1f}")
