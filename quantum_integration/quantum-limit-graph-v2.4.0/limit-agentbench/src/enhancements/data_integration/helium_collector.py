# src/enhancements/data_integration/helium_collector.py
"""
Enhanced Helium Collector v2.0.0
==================================
Collects Helium hotspot connectivity data from live API and/or offline Parquet snapshots.
Provides a connectivity score (0‑1) based on RSSI, SNR, and other metrics.

Features:
- Real API integration with aiohttp (stubbed but ready for live usage).
- Snapshot fallback (Parquet) for offline data.
- Async session pooling with connection limits.
- Retry logic with exponential backoff using tenacity.
- Circuit breaker per external service.
- Structured logging via structlog.
- Prometheus metrics for calls, errors, latency.
- Caching with configurable TTL via CacheManager.
- Batch fetching with concurrency control.
- Comprehensive error handling and fallback to defaults.
- Configurable via Pydantic with environment variable support.
"""

import asyncio
import logging
import time
from pathlib import Path
from typing import Dict, List, Optional, Any, Union
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
    class HeliumConfig(BaseModel):
        """Configuration for HeliumCollector."""
        # API endpoint
        api_url: str = Field("https://api.helium.io/v1/")
        api_key: Optional[str] = None
        # Snapshot path
        snapshot_path: Optional[Path] = None
        # Cache TTL (seconds)
        cache_ttl: int = Field(600, ge=0)
        # Retry settings
        retry_attempts: int = Field(3, ge=0)
        retry_min_wait: float = Field(1.0, gt=0)
        retry_max_wait: float = Field(10.0, gt=0)
        # Circuit breaker
        circuit_breaker_threshold: int = Field(5, ge=1)
        circuit_breaker_timeout: float = Field(30.0, ge=1)
        # Request timeout (seconds)
        request_timeout: float = Field(10.0, ge=1)
        # RSSI normalization range (dBm)
        rssi_min: float = Field(-120.0)
        rssi_max: float = Field(-30.0)
        # Enable metrics
        enable_prometheus: bool = True

        @field_validator('api_url')
        @classmethod
        def validate_api_url(cls, v):
            if not v.endswith('/'):
                v += '/'
            return v

        class Config:
            env_prefix = "HELIUM_"
else:
    # Fallback dict
    HELIUM_CONFIG = {
        "api_url": "https://api.helium.io/v1/",
        "api_key": None,
        "snapshot_path": None,
        "cache_ttl": 600,
        "retry_attempts": 3,
        "retry_min_wait": 1.0,
        "retry_max_wait": 10.0,
        "circuit_breaker_threshold": 5,
        "circuit_breaker_timeout": 30.0,
        "request_timeout": 10.0,
        "rssi_min": -120.0,
        "rssi_max": -30.0,
        "enable_prometheus": True,
    }

# ============================================================================
# HeliumCollector (Enhanced)
# ============================================================================

class HeliumCollector:
    """
    Enhanced Helium collector with real API integration, snapshot fallback, caching,
    retries, circuit breaker, logging, and metrics.
    """

    def __init__(
        self,
        cache: CacheManager,
        config: Optional[Union[Dict[str, Any], HeliumConfig]] = None,
    ):
        """
        Initialize the collector.

        Args:
            cache: CacheManager instance.
            config: Configuration dictionary or Pydantic model.
        """
        if config is None:
            if PYDANTIC_AVAILABLE:
                self.config = HeliumConfig()
            else:
                self.config = HELIUM_CONFIG
        elif isinstance(config, dict):
            if PYDANTIC_AVAILABLE:
                self.config = HeliumConfig(**config)
            else:
                self.config = config
        else:
            self.config = config

        self.cache = cache
        self.api_url = self.config.get("api_url", "https://api.helium.io/v1/")
        self.api_key = self.config.get("api_key")
        self.snapshot_path = self.config.get("snapshot_path")
        self.cache_ttl = self.config.get("cache_ttl", 600)
        self.request_timeout = self.config.get("request_timeout", 10.0)
        self.rssi_min = self.config.get("rssi_min", -120.0)
        self.rssi_max = self.config.get("rssi_max", -30.0)

        # Session management
        self._session: Optional[aiohttp.ClientSession] = None
        self._session_lock = asyncio.Lock()

        # Circuit breaker for API calls
        self._circuit_breaker = CircuitBreaker(
            name="helium_api",
            failure_threshold=self.config.get("circuit_breaker_threshold", 5),
            recovery_timeout=self.config.get("circuit_breaker_timeout", 30.0),
        )

        # Prometheus metrics
        if PROMETHEUS_AVAILABLE and self.config.get("enable_prometheus", True):
            self.metrics = {
                'calls': Counter('helium_api_calls_total', 'Helium API calls', ['status']),
                'errors': Counter('helium_api_errors_total', 'Helium API errors'),
                'latency': Histogram('helium_api_latency_seconds', 'Helium API latency'),
                'cache_hits': Counter('helium_cache_hits_total', 'Cache hits'),
                'cache_misses': Counter('helium_cache_misses_total', 'Cache misses'),
                'connectivity_score': Gauge('helium_connectivity_score', 'Hotspot connectivity score', ['hotspot_id']),
            }
        else:
            self.metrics = None

        logger.info("HeliumCollector initialized", snapshot=self.snapshot_path)

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

    async def get_connectivity_score(self, hotspot_id: str, force_refresh: bool = False) -> float:
        """
        Compute a connectivity score (0‑1) for a hotspot.

        Args:
            hotspot_id: Identifier of the hotspot.
            force_refresh: If True, bypass cache.

        Returns:
            Score between 0 and 1.
        """
        cache_key = f"helium:score:{hotspot_id}"

        # Try cache first
        if not force_refresh:
            cached = await self.cache.get(cache_key)
            if cached is not None:
                if self.metrics:
                    self.metrics['cache_hits'].inc()
                logger.debug("Cache hit", hotspot_id=hotspot_id)
                return float(cached)

        if self.metrics:
            self.metrics['cache_misses'].inc()

        # Fetch data from API or snapshot
        data = await self._fetch_hotspot_data(hotspot_id)
        score = self._compute_score(data)

        # Cache and return
        await self.cache.set(cache_key, str(score), ttl=self.cache_ttl)
        if self.metrics:
            self.metrics['connectivity_score'].labels(hotspot_id=hotspot_id).set(score)
        return score

    async def _fetch_hotspot_data(self, hotspot_id: str) -> List[Dict]:
        """
        Fetch hotspot data from snapshot or live API.

        Returns:
            A list of dictionaries containing hotspot readings.
        """
        # Try snapshot first
        if self.snapshot_path and self.snapshot_path.exists():
            try:
                df = pd.read_parquet(self.snapshot_path)
                filtered = df[df['hotspot_id'] == hotspot_id]
                if not filtered.empty:
                    logger.debug("Found hotspot data in snapshot", hotspot_id=hotspot_id)
                    return filtered.to_dict('records')
            except Exception as e:
                logger.warning("Failed to read snapshot", error=str(e))

        # Fallback to live API
        try:
            async def fetch():
                session = await self._get_session()
                url = f"{self.api_url}hotspots/{hotspot_id}/stats"
                headers = {}
                if self.api_key:
                    headers["Authorization"] = f"Bearer {self.api_key}"

                async with session.get(url, headers=headers) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        # Parse and return a list of readings
                        # Example response structure might be:
                        # {"data": {"stats": {"rssi": -70, "snr": 12, ...}}}
                        # Adapt based on actual API.
                        # For now, return a single simulated reading.
                        return [{
                            'hotspot_id': hotspot_id,
                            'rssi': data.get('rssi', -70),
                            'snr': data.get('snr', 12),
                            'timestamp': datetime.now().isoformat(),
                        }]
                    else:
                        logger.warning("API returned error", status=resp.status, hotspot_id=hotspot_id)
                        return []

            # Use retry and circuit breaker
            if TENACITY_AVAILABLE:
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
                async def fetch_with_retry():
                    return await fetch()
            else:
                async def fetch_with_retry():
                    for attempt in range(self.config.get("retry_attempts", 3)):
                        try:
                            return await fetch()
                        except Exception as e:
                            if attempt == self.config.get("retry_attempts", 3) - 1:
                                raise
                            wait = min(
                                self.config.get("retry_min_wait", 1.0) * (2 ** attempt),
                                self.config.get("retry_max_wait", 10.0),
                            )
                            await asyncio.sleep(wait)

            start_time = time.time()
            data = await self._circuit_breaker.call(fetch_with_retry)
            if self.metrics:
                self.metrics['calls'].labels(status='success').inc()
                self.metrics['latency'].observe(time.time() - start_time)
            return data
        except Exception as e:
            if self.metrics:
                self.metrics['errors'].inc()
                self.metrics['calls'].labels(status='error').inc()
            logger.error("Helium API fetch failed", hotspot_id=hotspot_id, error=str(e))
            # Return empty list, score will be default
            return []

    def _compute_score(self, data: List[Dict]) -> float:
        """
        Compute connectivity score from data.

        Args:
            data: List of readings.

        Returns:
            Score between 0 and 1.
        """
        if not data:
            return 0.5

        # Extract RSSI values
        rssi_values = [entry['rssi'] for entry in data if 'rssi' in entry]
        if not rssi_values:
            return 0.5

        avg_rssi = sum(rssi_values) / len(rssi_values)
        # Normalize RSSI from -120..-30 to 0..1
        score = (avg_rssi - self.rssi_min) / (self.rssi_max - self.rssi_min)
        # Clamp to [0, 1]
        return max(0.0, min(1.0, score))

    async def fetch_batch_scores(self, hotspot_ids: List[str], max_concurrency: int = 10) -> Dict[str, float]:
        """
        Fetch scores for multiple hotspots with limited concurrency.

        Args:
            hotspot_ids: List of hotspot identifiers.
            max_concurrency: Maximum number of concurrent requests.

        Returns:
            Dictionary mapping hotspot_id to score.
        """
        semaphore = asyncio.Semaphore(max_concurrency)

        async def fetch_with_semaphore(hid: str) -> Tuple[str, float]:
            async with semaphore:
                score = await self.get_connectivity_score(hid)
                return hid, score

        tasks = [fetch_with_semaphore(hid) for hid in hotspot_ids]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        scores = {}
        for result in results:
            if isinstance(result, Exception):
                # Handle error, skip or assign default
                logger.error("Batch fetch error", error=str(result))
            else:
                hid, score = result
                scores[hid] = score
        return scores

    # ---------- Utility methods ----------
    async def update_snapshot(self, snapshot_path: Path) -> None:
        """Update the snapshot path."""
        self.snapshot_path = snapshot_path
        logger.info("Snapshot path updated", path=snapshot_path)

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()

# ============================================================================
# Convenience factory
# ============================================================================
def create_helium_collector(
    cache: CacheManager,
    config: Optional[Dict[str, Any]] = None,
) -> HeliumCollector:
    """
    Factory to create a fully configured HeliumCollector.
    """
    return HeliumCollector(cache, config)

# ============================================================================
# Example usage
# ============================================================================
if __name__ == "__main__":
    import asyncio
    import sys
    sys.path.append('../')

    from ..cache.cache_manager import CacheManager

    async def main():
        cache = CacheManager()
        config = {
            "api_url": "https://api.helium.io/v1/",
            "api_key": "your_key_here",
            "cache_ttl": 600,
        }
        collector = create_helium_collector(cache, config)

        # Single fetch
        score = await collector.get_connectivity_score("hotspot_123")
        print(f"Connectivity score: {score}")

        # Batch fetch
        scores = await collector.fetch_batch_scores(["hotspot_123", "hotspot_456"])
        print("Batch scores:", scores)

        await collector.close()

    asyncio.run(main())
