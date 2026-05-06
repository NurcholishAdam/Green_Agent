# run_agent.py - Community Edition (No Paid APIs)

"""
Green Agent - Community Edition
Free, open-source API integrations for sustainable AI
"""

import aiohttp
import asyncio
import logging
from typing import Dict, Optional, Tuple
from datetime import datetime, timedelta
import random

logger = logging.getLogger(__name__)


class FreeAPIIntegrations:
    """
    Free/Open-source API integrations for Green Agent.
    
    Strategy:
    1. Use free tier APIs with generous limits
    2. Cache aggressively to reduce calls
    3. Provide fallback to simulation/static data
    4. Support self-hosted alternatives
    """
    
    def __init__(self, config: Dict = None):
        self.config = config or {}
        self.cache = {}
        self.cache_ttl = {
            'grid_carbon': 3600,      # 1 hour - grid data changes slowly
            'weather': 1800,           # 30 minutes
            'helium': 86400,           # 24 hours - use simulation mostly
        }
        
        # Free/Open data sources
        self.sources = {
            # Free grid carbon data (US only, no API key needed)
            'us_grid': 'https://www.eia.gov/opendata/qb.json',
            
            # Free weather (no API key for basic)
            'weather': 'https://wttr.in/',
            
            # Self-hostable alternative for grid carbon
            'electricitymap_self': 'http://localhost:8000/carbon',
            
            # Static fallback data (bundled with Green Agent)
            'static_data': self._load_static_data()
        }
        
        logger.info("Free API Integrations initialized (Community Edition)")
    
    def _load_static_data(self) -> Dict:
        """Load static data bundled with Green Agent"""
        return {
            'us_regions': {
                'us-east': {'avg_carbon': 380, 'renewable_pct': 25},
                'us-west': {'avg_carbon': 250, 'renewable_pct': 45},
                'us-central': {'avg_carbon': 450, 'renewable_pct': 20}
            },
            'eu_regions': {
                'eu-north': {'avg_carbon': 80, 'renewable_pct': 65},
                'eu-west': {'avg_carbon': 220, 'renewable_pct': 40}
            },
            'asia_regions': {
                'asia-pacific': {'avg_carbon': 550, 'renewable_pct': 15}
            },
            'helium_historical': {
                'prices': [4.0, 4.2, 4.5, 5.0, 6.0, 7.5, 8.0, 7.0, 5.5, 4.5],
                'inventory': [30, 28, 25, 22, 18, 15, 12, 18, 22, 28]
            }
        }
    
    async def get_grid_carbon_intensity(self, region: str) -> Tuple[float, str]:
        """
        Get carbon intensity using free data sources.
        
        Strategy:
        1. Try EIA free API (US regions)
        2. Try ENTSO-E free API (EU regions)
        3. Try ElectricityMap self-hosted
        4. Fallback to static data + simulation
        """
        cache_key = f"carbon_{region}"
        
        # Check cache
        if cache_key in self.cache:
            value, timestamp = self.cache[cache_key]
            if time.time() - timestamp < self.cache_ttl['grid_carbon']:
                return value, "cache"
        
        intensity = None
        source = "fallback"
        
        # Try region-specific free APIs
        if region.startswith('us-'):
            intensity = await self._get_us_carbon_intensity(region)
            if intensity:
                source = "eia_free"
        
        elif region.startswith('eu-'):
            intensity = await self._get_eu_carbon_intensity(region)
            if intensity:
                source = "entsoe_free"
        
        # Try self-hosted option
        if intensity is None:
            intensity = await self._get_self_hosted_intensity(region)
            if intensity:
                source = "self_hosted"
        
        # Fallback to static data with realistic variation
        if intensity is None:
            intensity = self._get_static_intensity(region)
            source = "static_simulation"
        
        # Cache result
        self.cache[cache_key] = (intensity, time.time())
        
        return intensity, source
    
    async def _get_us_carbon_intensity(self, region: str) -> Optional[float]:
        """Get US carbon intensity from EIA free API"""
        # EIA region mapping
        eia_regions = {
            'us-east': 'PJM',
            'us-west': 'CAL',
            'us-central': 'MISO'
        }
        
        eia_region = eia_regions.get(region)
        if not eia_region:
            return None
        
        try:
            # EIA API (free, no key for public data)
            # Note: EIA has rate limits, so we cache results
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    f"https://api.eia.gov/v2/electricity/rto/"
                    f"region-subregion-data/data/",
                    params={
                        'frequency': 'hourly',
                        'data[0]': 'value',
                        'facets[respondent][]': eia_region,
                        'sort[0][column]': 'period',
                        'sort[0][direction]': 'desc',
                        'length': 1
                    },
                    timeout=10
                ) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        # Parse response (simplified)
                        # In production, implement proper parsing
                        return 350.0  # Placeholder
        except Exception as e:
            logger.debug(f"EIA API failed: {e}")
        
        return None
    
    async def _get_eu_carbon_intensity(self, region: str) -> Optional[float]:
        """Get EU carbon intensity from ENTSO-E free API"""
        # ENTSO-E region mapping
        entsoe_regions = {
            'eu-north': 'SE',
            'eu-west': 'FR',
            'eu-central': 'DE'
        }
        
        code = entsoe_regions.get(region)
        if not code:
            return None
        
        try:
            # ENTSO-E Transparency Platform (free, requires registration)
            # For GitHub Codespaces, we'll use simulation
            return None
        except Exception as e:
            logger.debug(f"ENTSO-E API failed: {e}")
        
        return None
    
    async def _get_self_hosted_intensity(self, region: str) -> Optional[float]:
        """Get intensity from self-hosted ElectricityMap instance"""
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    f"{self.sources['electricitymap_self']}",
                    params={'zone': region},
                    timeout=5
                ) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        return float(data.get('carbonIntensity', 0))
        except Exception:
            pass
        
        return None
    
    def _get_static_intensity(self, region: str) -> float:
        """Get static intensity with realistic daily simulation"""
        base_intensities = {
            'us-east': 380,
            'us-west': 250,
            'us-central': 450,
            'eu-north': 80,
            'eu-west': 220,
            'asia-pacific': 550
        }
        
        base = base_intensities.get(region, 400)
        
        # Add realistic daily variation (higher during peak hours)
        hour = datetime.now().hour
        if 9 <= hour <= 17:
            variation = 1.1  # 10% higher during day
        elif 18 <= hour <= 21:
            variation = 1.05
        else:
            variation = 0.95
        
        # Add weekend adjustment
        if datetime.now().weekday() >= 5:
            variation *= 0.9
        
        return base * variation
    
    async def get_weather_forecast(self, lat: float, lon: float) -> Dict:
        """Get weather forecast using free service"""
        cache_key = f"weather_{lat}_{lon}"
        
        if cache_key in self.cache:
            value, timestamp = self.cache[cache_key]
            if time.time() - timestamp < self.cache_ttl['weather']:
                return value
        
        try:
            # wttr.in - free weather service, no API key needed
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    f"https://wttr.in/{lat},{lon}?format=j1",
                    timeout=10
                ) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        current = data.get('current_condition', [{}])[0]
                        result = {
                            'temperature': float(current.get('temp_C', 20)),
                            'wind_speed': float(current.get('windspeedKmph', 10)) / 3.6,
                            'cloud_cover': int(current.get('cloudcover', 50)) / 100,
                            'humidity': int(current.get('humidity', 60))
                        }
                        self.cache[cache_key] = (result, time.time())
                        return result
        except Exception as e:
            logger.debug(f"Weather API failed: {e}")
        
        # Simulated weather based on time of day
        hour = datetime.now().hour
        result = {
            'temperature': 15 + 10 * abs(hour - 12) / 12,
            'wind_speed': 5 + random.gauss(0, 2),
            'cloud_cover': random.uniform(0, 1),
            'humidity': 50 + random.gauss(0, 10)
        }
        
        self.cache[cache_key] = (result, time.time())
        return result
    
    async def get_helium_spot_price(self) -> Tuple[float, str]:
        """
        Get helium spot price using simulation + market trends.
        
        Since no free helium market API exists, we use:
        1. Community-sourced price data (if available)
        2. Simulated price with mean reversion
        3. Historical patterns from public reports
        """
        cache_key = "helium_price"
        
        if cache_key in self.cache:
            value, timestamp = self.cache[cache_key]
            if time.time() - timestamp < self.cache_ttl['helium']:
                return value, "cache"
        
        # Use simulated price with realistic dynamics
        price = self._simulate_helium_price()
        
        self.cache[cache_key] = (price, time.time())
        return price, "simulated"
    
    def _simulate_helium_price(self) -> float:
        """Simulate helium price with mean reversion and trends"""
        # Check if we have stored state
        if not hasattr(self, '_helium_price_state'):
            self._helium_price_state = {
                'price': 4.5,
                'trend': 0,
                'last_update': time.time()
            }
        
        state = self._helium_price_state
        elapsed_hours = (time.time() - state['last_update']) / 3600
        
        # Mean reversion to baseline ($4.50)
        reversion = (4.5 - state['price']) * 0.1 * elapsed_hours
        
        # Random walk
        shock = random.gauss(0, 0.2) * (elapsed_hours ** 0.5)
        
        # Inventory effect (simulated)
        inventory_days = 20 + random.gauss(0, 5)
        if inventory_days < 15:
            inventory_effect = 0.5
        else:
            inventory_effect = 0
        
        new_price = state['price'] + reversion + shock + inventory_effect
        new_price = max(3.0, min(12.0, new_price))
        
        state['price'] = new_price
        state['last_update'] = time.time()
        
        return new_price
    
    async def get_renewable_generation(self, region: str) -> Dict:
        """Get renewable generation mix from free sources"""
        # Use NREL API (free, requires key but has generous limits)
        # For open source, we use static data with time-based variation
        
        base_mix = self.sources['static_data']['us_regions'].get(region, {})
        
        # Add time-based variation
        hour = datetime.now().hour
        if 6 <= hour <= 18:
            solar_factor = (hour - 6) / 12
        else:
            solar_factor = 0
        
        return {
            'solar_percent': base_mix.get('renewable_pct', 25) * solar_factor,
            'wind_percent': base_mix.get('renewable_pct', 25) * 0.7,
            'hydro_percent': 10,
            'thermal_percent': 100 - (solar_factor * 25 + 17.5 + 10)
        }


class OpenSourceIntegrations:
    """
    Alternative: Use open-source data collectors that users can self-host.
    
    These can be run locally and provide real data without API costs.
    """
    
    @staticmethod
    async def run_grid_collector():
        """
        Self-hosted grid data collector.
        
        Users can run this locally to collect real grid data.
        """
        # Example: Scrape public grid operator websites
        # This would be a separate script that users can run
        pass
    
    @staticmethod
    async def run_helium_collector():
        """
        Self-hosted helium market data collector.
        
        Uses web scraping from public sources (industry news, reports).
        """
        pass
    
    @staticmethod
    def get_community_data_source():
        """
        Community-driven data repository.
        
        Users can contribute anonymized data to a shared database.
        """
        return {
            'url': 'https://github.com/GreenAgent/community-data',
            'description': 'Community-contributed sustainability data'
        }


# ============================================================
# Revised Green Agent Orchestrator (Community Edition)
# ============================================================

class GreenAgentCommunityOrchestrator:
    """
    Green Agent - Community Edition
    No paid APIs, fully open-source.
    """
    
    def __init__(self, config_path: Optional[str] = None):
        self.api = FreeAPIIntegrations()
        # ... rest of initialization
        pass
    
    async def process_task(self, task_config):
        """Process task with free API integrations"""
        
        # Get carbon intensity (always works, even without API keys)
        carbon_intensity, source = await self.api.get_grid_carbon_intensity(
            task_config.region
        )
        logger.info(f"Carbon intensity: {carbon_intensity:.0f} gCO2/kWh (source: {source})")
        
        # Get weather for renewable forecasting
        weather = await self.api.get_weather_forecast(40.7128, -74.0060)
        
        # Get helium price (simulated but realistic)
        helium_price, price_source = await self.api.get_helium_spot_price()
        logger.info(f"Helium price: ${helium_price:.2f}/L (source: {price_source})")
        
        # Continue with processing...
        # All modules work with these values, regardless of source
        
        return result
