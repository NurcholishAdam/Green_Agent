"""
Green Agent v5.0.0 - Carbon Forecasting Engine
Layer 7: Real-time carbon intensity tracking and forecasting
File: src/carbon/forecasting_engine.py
"""

from typing import Dict, Optional
from datetime import datetime, timedelta
import logging
import asyncio

logger = logging.getLogger(__name__)


class CarbonForecaster:
    """
    Carbon intensity forecaster with multiple provider support
    """
    
    def __init__(self, config: Dict):
        self.config = config
        self.api_provider = config.get('carbon', {}).get('api_provider', 'simulation')
        self.default_region = config.get('carbon', {}).get('default_region', 'US-CA')
        self._cache = {}
        self._cache_ttl_seconds = 900  # 15 minutes
    
    async def initialize(self):
        """Initialize the forecaster"""
        logger.info(f"CarbonForecaster initialized with {self.api_provider} provider")
    
    async def shutdown(self):
        """Cleanup resources"""
        logger.info("CarbonForecaster shutdown complete")
    
    async def get_current_intensity(self, region: Optional[str] = None) -> float:
        """
        Get current carbon intensity for a region
        
        Args:
            region: Region code (e.g., 'US-CA', 'GB', 'DE')
            
        Returns:
            Carbon intensity in gCO2/kWh
        """
        region = region or self.default_region
        
        # Check cache first
        if region in self._cache:
            cached = self._cache[region]
            age = (datetime.now() - cached['timestamp']).total_seconds()
            if age < self._cache_ttl_seconds:
                return cached['intensity']
        
        # Fetch from provider
        if self.api_provider == 'simulation':
            intensity = await self._simulate_intensity(region)
        elif self.api_provider == 'electricitymap':
            intensity = await self._fetch_from_electricity_map(region)
        elif self.api_provider == 'carbonintensity':
            intensity = await self._fetch_from_carbon_intensity(region)
        else:
            intensity = await self._simulate_intensity(region)
        
        # Cache result
        self._cache[region] = {
            'intensity': intensity,
            'timestamp': datetime.now()
        }
        
        return intensity
    
    async def _simulate_intensity(self, region: str) -> float:
        """Simulate carbon intensity for testing"""
        import random
        hour = datetime.now().hour
        
        # Simulate daily pattern: higher during day, lower at night
        if 6 <= hour <= 18:
            # Daytime: mix of solar/wind + fossil
            base = 200 + random.uniform(-50, 100)
        else:
            # Night: more fossil, less solar
            base = 150 + random.uniform(-30, 50)
        
        # Add some regional variation
        regional_factor = {
            'US-CA': 0.9,   # California: more renewables
            'US-TX': 1.1,   # Texas: more fossil
            'GB': 0.8,      # UK: more wind
            'DE': 1.0,      # Germany: mixed
        }.get(region, 1.0)
        
        intensity = base * regional_factor
        return max(30, min(600, intensity))  # Clamp to realistic range
    
    async def _fetch_from_electricity_map(self, region: str) -> float:
        """Fetch from ElectricityMap API"""
        try:
            import aiohttp
            
            api_key = self.config.get('carbon', {}).get('api_key')
            if not api_key:
                logger.warning("ElectricityMap API key not set, using simulation")
                return await self._simulate_intensity(region)
            
            url = f"https://api.electricitymap.org/v3/carbon-intensity/latest?zone={region}"
            headers = {'auth-token': api_key}
            
            async with aiohttp.ClientSession() as session:
                async with session.get(url, headers=headers, timeout=10) as response:
                    if response.status == 200:
                        data = await response.json()
                        return data.get('carbonIntensity', 200)
                    else:
                        logger.warning(f"ElectricityMap API error: {response.status}")
                        return await self._simulate_intensity(region)
                        
        except Exception as e:
            logger.warning(f"ElectricityMap API failed: {e}, using simulation")
            return await self._simulate_intensity(region)
    
    async def _fetch_from_carbon_intensity(self, region: str) -> float:
        """Fetch from CarbonIntensity.org API (UK-focused)"""
        try:
            import aiohttp
            
            # Only supports UK regions
            if not region.startswith('GB'):
                return await self._simulate_intensity(region)
            
            url = f"https://api.carbonintensity.org.uk/regional/data/region/{region}"
            
            async with aiohttp.ClientSession() as session:
                async with session.get(url, timeout=10) as response:
                    if response.status == 200:
                        data = await response.json()
                        # Parse the response structure
                        if 'data' in data and 'data' in data['data']:
                            intensity = data['data']['data'][0].get('carbonIntensity', 200)
                            return intensity
                    return await self._simulate_intensity(region)
                    
        except Exception as e:
            logger.warning(f"CarbonIntensity API failed: {e}, using simulation")
            return await self._simulate_intensity(region)
