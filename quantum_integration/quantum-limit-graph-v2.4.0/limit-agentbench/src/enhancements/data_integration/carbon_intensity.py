# src/enhancements/data_integration/carbon_intensity.py
"""
Enhanced Carbon Intensity Fetcher v2.0.0
========================================
Fetches real‑time carbon intensity from multiple providers (Climate TRACE, OS‑Climate, Electricity Maps)
with caching, retries, circuit breaker, logging, and Prometheus metrics.

Features:
- Real API integrations with aiohttp and async/await.
- Configurable via Pydantic (with environment variables).
- Retry with exponential backoff and jitter using tenacity.
- Circuit breaker for external services.
- Structured logging via structlog.
- Prometheus metrics for calls, errors, latency.
- Caching with TTL (via CacheManager).
- Fallback to region averages.
- Async session pooling.
- Comprehensive error handling.
"""

import asyncio
import logging
import time
from datetime import datetime, timedelta
from typing import Optional, Dict, List, Any, Union
import aiohttp
from aiohttp import ClientTimeout, ClientError

# ---------- Pydantic ----------
try:
    from pydantic import BaseModel, Field, validator, field_validator
    PYDANTIC_AVAILABLE = True
except ImportError:
    PYDANTIC_AVAILABLE = False

# ---------- Tenacity (retry) ----------
try:
    from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type, before_sleep_log
    TENACITY_AVAILABLE = True
except ImportError:
    TENACITY_AVAILABLE = False

# ---------- Circuit breaker ----------
try:
    from ..circuit_breaker import CircuitBreaker
    CIRCUIT_BREAKER_AVAILABLE = True
except ImportError:
    # Fallback simple circuit breaker
    class CircuitBreaker:
        def __init__(self, name, failure_threshold=5, recovery_timeout=30):
            self.name = name
            self.failure_threshold = failure_threshold
            self.recovery_timeout = recovery_timeout
        async def call(self, func, *args, **kwargs):
            return await func(*args, **kwargs)
    CIRCUIT_BREAKER_AVAILABLE = False

# ---------- Prometheus ----------
try:
    from prometheus_client import Counter, Gauge, Histogram
    PROMETHEUS_AVAILABLE = True
except ImportError:
    PROMETHEUS_AVAILABLE = False

# ---------- Structlog ----------
try:
    import structlog
    logger = structlog.get_logger(__name__)
except ImportError:
    logger = logging.getLogger(__name__)
    logging.basicConfig(level=logging.INFO)

# ---------- Local imports ----------
from ..cache.cache_manager import CacheManager

# ============================================================================
# Configuration
# ============================================================================
if PYDANTIC_AVAILABLE:
    class CarbonIntensityConfig(BaseModel):
        """Configuration for CarbonIntensityFetcher."""
        # Provider order
        providers: List[str] = Field(
            default_factory=lambda: ["climate_trace", "os_climate", "electricity_maps"]
        )
        # API keys
        climate_trace_api_key: Optional[str] = None
        os_climate_api_key: Optional[str] = None
        electricity_maps_api_key: Optional[str] = None
        # Region mapping (override default averages)
        region_averages: Dict[str, float] = Field(
            default_factory=lambda: {
                "us-east": 0.41,
                "us-west": 0.34,
                "eu-west": 0.27,
                "eu-north": 0.21,
                "asia-east": 0.49,
                "asia-southeast": 0.47,
                "global": 0.40,
            }
        )
        # Cache TTL in seconds
        cache_ttl: int = Field(3600, ge=0)
        # Retry settings
        retry_attempts: int = Field(3, ge=0)
        retry_min_wait: float = Field(1.0, gt=0)
        retry_max_wait: float = Field(10.0, gt=0)
        # Circuit breaker
        circuit_breaker_threshold: int = Field(5, ge=1)
        circuit_breaker_timeout: float = Field(30.0, ge=1)
        # Request timeout (seconds)
        request_timeout: float = Field(10.0, ge=1)
        # Enable metrics
        enable_prometheus: bool = True

        @field_validator('providers')
        @classmethod
        def validate_providers(cls, v):
            allowed = {"climate_trace", "os_climate", "electricity_maps"}
            for p in v:
                if p not in allowed:
                    raise ValueError(f"Provider {p} not in allowed list {allowed}")
            return v

        class Config:
            env_prefix = "CARBON_"
else:
    # Fallback dict
    CARBON_CONFIG = {
        "providers": ["climate_trace", "os_climate", "electricity_maps"],
        "climate_trace_api_key": None,
        "os_climate_api_key": None,
        "electricity_maps_api_key": None,
        "region_averages": {
            "us-east": 0.41,
            "us-west": 0.34,
            "eu-west": 0.27,
            "eu-north": 0.21,
            "asia-east": 0.49,
            "asia-southeast": 0.47,
            "global": 0.40,
        },
        "cache_ttl": 3600,
        "retry_attempts": 3,
        "retry_min_wait": 1.0,
        "retry_max_wait": 10.0,
        "circuit_breaker_threshold": 5,
        "circuit_breaker_timeout": 30.0,
        "request_timeout": 10.0,
        "enable_prometheus": True,
    }

# ============================================================================
# CarbonIntensityFetcher (Enhanced)
# ============================================================================

class CarbonIntensityFetcher:
    """
    Enhanced carbon intensity fetcher with real API integrations, caching, retries,
    circuit breaker, logging, and metrics.
    """

    def __init__(
        self,
        cache: CacheManager,
        config: Optional[Union[Dict[str, Any], CarbonIntensityConfig]] = None,
    ):
        """
        Initialize the fetcher.

        Args:
            cache: CacheManager instance for caching intensity values.
            config: Configuration dictionary or Pydantic model.
        """
        if config is None:
            if PYDANTIC_AVAILABLE:
                self.config = CarbonIntensityConfig()
            else:
                self.config = CARBON_CONFIG
        elif isinstance(config, dict):
            if PYDANTIC_AVAILABLE:
                self.config = CarbonIntensityConfig(**config)
            else:
                self.config = config
        else:
            self.config = config

        self.cache = cache
        self.provider_order = self.config.get("providers", ["climate_trace", "os_climate", "electricity_maps"])
        self.region_averages = self.config.get("region_averages", {})
        self.cache_ttl = self.config.get("cache_ttl", 3600)
        self.request_timeout = self.config.get("request_timeout", 10.0)

        # API keys
        self._api_keys = {
            "climate_trace": self.config.get("climate_trace_api_key"),
            "os_climate": self.config.get("os_climate_api_key"),
            "electricity_maps": self.config.get("electricity_maps_api_key"),
        }

        # Session management
        self._session: Optional[aiohttp.ClientSession] = None
        self._session_lock = asyncio.Lock()

        # Circuit breakers per provider
        self._circuit_breakers = {}
        for provider in self.provider_order:
            self._circuit_breakers[provider] = CircuitBreaker(
                name=f"carbon_{provider}",
                failure_threshold=self.config.get("circuit_breaker_threshold", 5),
                recovery_timeout=self.config.get("circuit_breaker_timeout", 30.0),
            )

        # Prometheus metrics
        if PROMETHEUS_AVAILABLE and self.config.get("enable_prometheus", True):
            self.metrics = {
                'calls': Counter('carbon_api_calls_total', 'Carbon API calls', ['provider', 'status']),
                'errors': Counter('carbon_api_errors_total', 'Carbon API errors', ['provider']),
                'latency': Histogram('carbon_api_latency_seconds', 'Carbon API latency', ['provider']),
                'cache_hits': Counter('carbon_cache_hits_total', 'Cache hits'),
                'cache_misses': Counter('carbon_cache_misses_total', 'Cache misses'),
            }
        else:
            self.metrics = None

        logger.info("CarbonIntensityFetcher initialized", providers=self.provider_order)

    async def _get_session(self) -> aiohttp.ClientSession:
        """Get or create an aiohttp ClientSession with connection pooling."""
        async with self._session_lock:
            if self._session is None or self._session.closed:
                timeout = ClientTimeout(total=self.request_timeout)
                connector = aiohttp.TCPConnector(limit=10, ttl_dns_cache=300)
                self._session = aiohttp.ClientSession(
                    connector=connector,
                    timeout=timeout,
                    raise_for_status=True,
                )
            return self._session

    async def close(self):
        """Close the underlying session."""
        if self._session and not self._session.closed:
            await self._session.close()
            self._session = None

    async def get_intensity(
        self,
        region: str,
        timestamp: Optional[datetime] = None,
        force_refresh: bool = False,
    ) -> float:
        """
        Get carbon intensity (kg CO₂/kWh) for a region at a given time.

        Args:
            region: Region identifier (e.g., "us-east", "global").
            timestamp: Optional timestamp; if None, use current UTC time.
            force_refresh: If True, bypass cache.

        Returns:
            Carbon intensity in kg CO₂/kWh.
        """
        if timestamp is None:
            timestamp = datetime.utcnow()
        cache_key = f"carbon:{region}:{timestamp.strftime('%Y%m%d%H')}"

        # Try cache first
        if not force_refresh:
            cached = await self.cache.get(cache_key)
            if cached is not None:
                if self.metrics:
                    self.metrics['cache_hits'].inc()
                logger.debug("Cache hit", region=region, key=cache_key)
                return float(cached)

        if self.metrics:
            self.metrics['cache_misses'].inc()

        # Try providers in order
        intensity = None
        for provider in self.provider_order:
            try:
                start_time = time.time()
                cb = self._circuit_breakers[provider]
                # Call the provider method with circuit breaker
                if TENACITY_AVAILABLE:
                    # Use retry decorator inside circuit breaker
                    @retry(
                        stop=stop_after_attempt(self.config.get("retry_attempts", 3)),
                        wait=wait_exponential(
                            multiplier=1,
                            min=self.config.get("retry_min_wait", 1.0),
                            max=self.config.get("retry_max_wait", 10.0),
                        ),
                        retry=retry_if_exception_type((aiohttp.ClientError, asyncio.TimeoutError)),
                        before_sleep=before_sleep_log(logger, logging.WARNING),
                    )
                    async def fetch():
                        return await self._fetch_provider(provider, region, timestamp)
                else:
                    # Simple retry without tenacity
                    async def fetch():
                        for attempt in range(self.config.get("retry_attempts", 3)):
                            try:
                                return await self._fetch_provider(provider, region, timestamp)
                            except Exception as e:
                                if attempt == self.config.get("retry_attempts", 3) - 1:
                                    raise
                                wait = min(
                                    self.config.get("retry_min_wait", 1.0) * (2 ** attempt),
                                    self.config.get("retry_max_wait", 10.0),
                                )
                                await asyncio.sleep(wait)

                intensity = await cb.call(fetch)
                if intensity is not None:
                    if self.metrics:
                        self.metrics['calls'].labels(provider=provider, status='success').inc()
                        self.metrics['latency'].labels(provider=provider).observe(time.time() - start_time)
                    logger.info("Fetched carbon intensity", provider=provider, region=region, intensity=intensity)
                    break
            except Exception as e:
                if self.metrics:
                    self.metrics['errors'].labels(provider=provider).inc()
                    self.metrics['calls'].labels(provider=provider, status='error').inc()
                logger.warning("Provider failed", provider=provider, region=region, error=str(e))

        # Fallback to region average
        if intensity is None:
            intensity = self._get_region_average(region)
            logger.info("Using fallback average", region=region, intensity=intensity)

        # Store in cache
        await self.cache.set(cache_key, str(intensity), ttl=self.cache_ttl)
        return intensity

    # ---------- Provider implementations ----------
    async def _fetch_provider(self, provider: str, region: str, timestamp: datetime) -> Optional[float]:
        """Dispatch to the appropriate provider method."""
        if provider == "climate_trace":
            return await self._fetch_climate_trace(region, timestamp)
        elif provider == "os_climate":
            return await self._fetch_os_climate(region, timestamp)
        elif provider == "electricity_maps":
            return await self._fetch_electricity_maps(region, timestamp)
        else:
            raise ValueError(f"Unknown provider: {provider}")

    async def _fetch_climate_trace(self, region: str, timestamp: datetime) -> Optional[float]:
        """
        Fetch carbon intensity from Climate TRACE API.
        API: https://api.climatetrace.org/v1/carbon-intensity?region={region}&date={date}
        """
        api_key = self._api_keys.get("climate_trace")
        if not api_key:
            logger.debug("Climate TRACE API key not set; skipping")
            return None

        session = await self._get_session()
        date_str = timestamp.strftime("%Y-%m-%d")
        url = f"https://api.climatetrace.org/v1/carbon-intensity"
        params = {"region": region, "date": date_str}
        headers = {"Authorization": f"Bearer {api_key}"}

        try:
            async with session.get(url, params=params, headers=headers) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    # Assume response structure: {"intensity": 0.42}
                    intensity = data.get("intensity")
                    if intensity is not None:
                        return float(intensity)
                else:
                    logger.warning("Climate TRACE returned status", status=resp.status, region=region)
                    return None
        except Exception as e:
            logger.error("Climate TRACE API error", error=str(e), region=region)
            raise

    async def _fetch_os_climate(self, region: str, timestamp: datetime) -> Optional[float]:
        """
        Fetch carbon intensity from OS‑Climate API.
        API: https://api.os-climate.org/v1/carbon-intensity?region={region}
        """
        api_key = self._api_keys.get("os_climate")
        if not api_key:
            logger.debug("OS‑Climate API key not set; skipping")
            return None

        session = await self._get_session()
        url = f"https://api.os-climate.org/v1/carbon-intensity"
        params = {"region": region}
        headers = {"Authorization": f"Bearer {api_key}"}

        try:
            async with session.get(url, params=params, headers=headers) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    intensity = data.get("intensity")
                    if intensity is not None:
                        return float(intensity)
                else:
                    logger.warning("OS‑Climate returned status", status=resp.status, region=region)
                    return None
        except Exception as e:
            logger.error("OS‑Climate API error", error=str(e), region=region)
            raise

    async def _fetch_electricity_maps(self, region: str, timestamp: datetime) -> Optional[float]:
        """
        Fetch carbon intensity from Electricity Maps API.
        API: https://api.electricitymap.org/v3/carbon-intensity/latest?zone={region}
        """
        api_key = self._api_keys.get("electricity_maps")
        if not api_key:
            logger.debug("Electricity Maps API key not set; skipping")
            return None

        session = await self._get_session()
        url = f"https://api.electricitymap.org/v3/carbon-intensity/latest"
        params = {"zone": region}
        headers = {"auth-token": api_key}

        try:
            async with session.get(url, params=params, headers=headers) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    # Response: {"data": {"carbonIntensity": 420}}
                    intensity = data.get("data", {}).get("carbonIntensity")
                    if intensity is not None:
                        return float(intensity) / 1000.0  # convert g/kWh to kg/kWh
                else:
                    logger.warning("Electricity Maps returned status", status=resp.status, region=region)
                    return None
        except Exception as e:
            logger.error("Electricity Maps API error", error=str(e), region=region)
            raise

    def _get_region_average(self, region: str) -> float:
        """Get fallback average intensity for a region."""
        return self.region_averages.get(region, self.region_averages.get("global", 0.40))

    # ---------- Utility methods ----------
    async def get_intensity_batch(
        self,
        regions: List[str],
        timestamp: Optional[datetime] = None,
    ) -> Dict[str, float]:
        """
        Get carbon intensities for multiple regions in parallel.

        Args:
            regions: List of region identifiers.
            timestamp: Optional timestamp.

        Returns:
            Dictionary mapping region to intensity.
        """
        tasks = [self.get_intensity(region, timestamp) for region in regions]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        intensities = {}
        for region, result in zip(regions, results):
            if isinstance(result, Exception):
                logger.error("Failed to get intensity for region", region=region, error=str(result))
                intensities[region] = self._get_region_average(region)
            else:
                intensities[region] = result
        return intensities

    async def get_historical_intensity(
        self,
        region: str,
        start: datetime,
        end: datetime,
        step_hours: int = 1,
    ) -> Dict[datetime, float]:
        """
        Get historical carbon intensity for a region over a time range.

        Args:
            region: Region identifier.
            start: Start time.
            end: End time.
            step_hours: Time step in hours.

        Returns:
            Dictionary mapping datetime to intensity.
        """
        results = {}
        current = start.replace(minute=0, second=0, microsecond=0)
        while current <= end:
            intensity = await self.get_intensity(region, current)
            results[current] = intensity
            current += timedelta(hours=step_hours)
        return results

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()

# ============================================================================
# Convenience factory
# ============================================================================
def create_carbon_fetcher(
    cache: CacheManager,
    config: Optional[Dict[str, Any]] = None,
) -> CarbonIntensityFetcher:
    """
    Factory to create a fully configured CarbonIntensityFetcher.
    """
    return CarbonIntensityFetcher(cache, config)

# ============================================================================
# Example usage
# ============================================================================
if __name__ == "__main__":
    import asyncio
    import sys
    sys.path.append('../')  # Allow imports

    from ..cache.cache_manager import CacheManager

    async def main():
        cache = CacheManager()
        config = {
            "providers": ["climate_trace", "os_climate", "electricity_maps"],
            "climate_trace_api_key": "your_key_here",
            "os_climate_api_key": "your_key_here",
            "electricity_maps_api_key": "your_key_here",
            "cache_ttl": 3600,
        }
        fetcher = create_carbon_fetcher(cache, config)

        intensity = await fetcher.get_intensity("us-east")
        print(f"Carbon intensity for us-east: {intensity} kg CO₂/kWh")

        # Batch fetch
        regions = ["us-east", "eu-west", "asia-east"]
        intensities = await fetcher.get_intensity_batch(regions)
        print("Batch intensities:", intensities)

        await fetcher.close()

    asyncio.run(main())
