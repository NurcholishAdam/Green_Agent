# src/dpq/carbon_intensity_monitor.py

from typing import Dict, Optional, Callable
from dataclasses import dataclass
from enum import Enum
import asyncio
from datetime import datetime, timedelta

class CarbonZone(Enum):
    GREEN = "green"      # < 50 gCO2/kWh
    YELLOW = "yellow"    # 50-200 gCO2/kWh
    RED = "red"          # 200-400 gCO2/kWh
    CRITICAL = "critical" # > 400 gCO2/kWh

@dataclass
class CarbonIntensityUpdate:
    timestamp: datetime
    intensity_gco2_kwh: float
    zone: CarbonZone
    region: str
    forecast_15min: Optional[float] = None
    forecast_60min: Optional[float] = None

class CarbonIntensityMonitor:
    """
    Real-time carbon intensity tracker with zone prediction
    
    Responsibilities:
    - Subscribe to Carbon Forecaster API (Layer 6)
    - Track current zone and predict transitions
    - Notify PrecisionController on zone changes
    - Cache responses to handle API failures
    """
    
    def __init__(
        self,
        carbon_forecaster_url: str,
        update_interval_seconds: int = 900,  # 15 minutes
        cache_ttl_seconds: int = 1800,  # 30 minutes
        prediction_horizon_minutes: int = 60
    ):
        self.carbon_forecaster_url = carbon_forecaster_url
        self.update_interval = update_interval_seconds
        self.cache_ttl = cache_ttl_seconds
        self.prediction_horizon = prediction_horizon_minutes
        
        self._cache: Dict[str, CarbonIntensityUpdate] = {}
        self._callbacks: list[Callable[[CarbonIntensityUpdate], None]] = []
        self._running = False
        
    async def start(self):
        """Start monitoring loop"""
        self._running = True
        asyncio.create_task(self._monitoring_loop())
        
    async def stop(self):
        """Stop monitoring loop"""
        self._running = False
        
    def register_callback(self, callback: Callable[[CarbonIntensityUpdate], None]):
        """Register callback for zone change notifications"""
        self._callbacks.append(callback)
        
    async def get_current_intensity(self, region: str = "default") -> CarbonIntensityUpdate:
        """Get current carbon intensity with caching"""
        cache_key = f"{region}_current"
        
        # Check cache first
        if cache_key in self._cache:
            cached = self._cache[cache_key]
            if (datetime.now() - cached.timestamp).total_seconds() < self.cache_ttl:
                return cached
        
        # Fetch from API
        update = await self._fetch_from_api(region)
        self._cache[cache_key] = update
        
        # Notify callbacks if zone changed
        if self._zone_changed(cache_key, update):
            for callback in self._callbacks:
                try:
                    callback(update)
                except Exception as e:
                    logger.error(f"Callback failed: {e}")
        
        return update
        
    async def _fetch_from_api(self, region: str) -> CarbonIntensityUpdate:
        """Fetch carbon intensity from Carbon Forecaster API"""
        # Implementation: HTTP client with retry logic
        # Returns parsed CarbonIntensityUpdate
        pass
        
    def _zone_changed(self, cache_key: str, new_update: CarbonIntensityUpdate) -> bool:
        """Check if carbon zone has changed"""
        if cache_key not in self._cache:
            return True
        return self._cache[cache_key].zone != new_update.zone
        
    async def _monitoring_loop(self):
        """Background loop for periodic updates"""
        while self._running:
            try:
                await self.get_current_intensity()
                await asyncio.sleep(self.update_interval)
            except Exception as e:
                logger.error(f"Monitoring loop error: {e}")
                await asyncio.sleep(60)  # Retry after 1 minute
