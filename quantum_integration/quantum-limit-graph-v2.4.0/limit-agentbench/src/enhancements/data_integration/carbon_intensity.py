import aiohttp
import asyncio
from datetime import datetime, timedelta
from typing import Optional, Dict
from ..cache.cache_manager import CacheManager

class CarbonIntensityFetcher:
    """Fetches real‑time carbon intensity from multiple providers."""
    def __init__(self, cache: CacheManager, api_keys: Optional[Dict[str, str]] = None):
        self.cache = cache
        self.api_keys = api_keys or {}
        self.providers = {
            "climate_trace": self._fetch_climate_trace,
            "os_climate": self._fetch_os_climate,
            "electricity_maps": self._fetch_electricity_maps,
        }
        self.default_provider = "climate_trace"

    async def get_intensity(self, region: str, timestamp: Optional[datetime] = None) -> float:
        """Get carbon intensity (kg CO₂/kWh) for a region at a given time."""
        if timestamp is None:
            timestamp = datetime.utcnow()
        cache_key = f"carbon:{region}:{timestamp.strftime('%Y%m%d%H')}"
        cached = await self.cache.get(cache_key)
        if cached is not None:
            return float(cached)

        intensity = None
        for provider in ["climate_trace", "os_climate", "electricity_maps"]:
            try:
                intensity = await self.providers[provider](region, timestamp)
                if intensity is not None:
                    break
            except Exception as e:
                print(f"Provider {provider} failed: {e}")

        if intensity is None:
            # fallback to region average from OS-Climate
            intensity = await self._fetch_os_climate_average(region)

        await self.cache.set(cache_key, str(intensity), ttl=3600)  # 1 hour TTL
        return intensity

    async def _fetch_climate_trace(self, region: str, timestamp: datetime) -> Optional[float]:
        """Climate TRACE API (stub)."""
        # In production: https://api.climatetrace.org/ with API key
        await asyncio.sleep(0.1)
        # Mock data based on region
        mock = {
            "us-east": 0.42,
            "us-west": 0.35,
            "eu-west": 0.28,
            "eu-north": 0.22,
            "asia-east": 0.50,
            "asia-southeast": 0.48
        }
        return mock.get(region, 0.40)

    async def _fetch_os_climate(self, region: str, timestamp: datetime) -> Optional[float]:
        """OS‑Climate API (stub)."""
        await asyncio.sleep(0.1)
        return await self._fetch_os_climate_average(region)

    async def _fetch_os_climate_average(self, region: str) -> float:
        """Fallback: average intensity from OS‑Climate data."""
        averages = {
            "us-east": 0.41,
            "us-west": 0.34,
            "eu-west": 0.27,
            "eu-north": 0.21,
            "asia-east": 0.49,
            "asia-southeast": 0.47
        }
        return averages.get(region, 0.39)

    async def _fetch_electricity_maps(self, region: str, timestamp: datetime) -> Optional[float]:
        """Electricity Maps API (stub)."""
        # Requires API key; if not set, return None
        await asyncio.sleep(0.1)
        return None
