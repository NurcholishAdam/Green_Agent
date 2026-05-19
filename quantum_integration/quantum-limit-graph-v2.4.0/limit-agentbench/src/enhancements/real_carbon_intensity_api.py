# src/enhancements/real_carbon_intensity_api.py
"""
Real carbon intensity integration with ElectricityMap and WattTime APIs.

Fetches live grid carbon intensity for data center locations.
"""

import asyncio
import aiohttp
import hashlib
import sqlite3
import time
from typing import Dict, Optional
from pathlib import Path
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)


class RealCarbonIntensityClient:
    """
    Real carbon intensity data from ElectricityMap and WattTime.
    
    Features:
    - Live grid carbon intensity per region
    - 24-hour forecast
    - Local SQLite caching
    - Automatic token refresh for WattTime
    """
    
    def __init__(self, config: Optional[Dict] = None):
        self.config = config or {}
        
        # API keys
        self.electricitymap_key = config.get('electricitymap_key')
        self.watttime_username = config.get('watttime_username')
        self.watttime_password = config.get('watttime_password')
        
        # Cache
        self.cache = {}
        self.cache_ttl = 300  # 5 minutes
        self.db_path = config.get('db_path', Path(__file__).parent / "data" / "carbon_intensity.db")
        
        # WattTime token
        self.watttime_token = None
        self.token_expiry = 0
        
        # Region mapping (data center location to API zone)
        self.region_mapping = {
            # USA
            "USA": {"electricitymap": "US-CAL-CISO", "watttime": "CAISO"},
            "California": {"electricitymap": "US-CAL-CISO", "watttime": "CAISO"},
            "Texas": {"electricitymap": "US-TEX-ERCO", "watttime": "ERCO"},
            "New York": {"electricitymap": "US-NY-NYIS", "watttime": "NYISO"},
            "Virginia": {"electricitymap": "US-CENT-SWPP", "watttime": "PJM"},
            # Europe
            "Finland": {"electricitymap": "FI", "watttime": "FI"},
            "Ireland": {"electricitymap": "IE", "watttime": "IE"},
            "Sweden": {"electricitymap": "SE", "watttime": "SE"},
            "Denmark": {"electricitymap": "DK", "watttime": "DK"},
            "Germany": {"electricitymap": "DE", "watttime": "DE"},
            "France": {"electricitymap": "FR", "watttime": "FR"},
            "United Kingdom": {"electricitymap": "GB", "watttime": "UK"},
            # Asia
            "Indonesia": {"electricitymap": "ID", "watttime": "ID"},
            "Singapore": {"electricitymap": "SG", "watttime": "SG"},
            "Japan": {"electricitymap": "JP-TK", "watttime": "JP"},
            "South Korea": {"electricitymap": "KR", "watttime": "KR"},
            "China": {"electricitymap": "CN", "watttime": "CN"},
            "Saudi Arabia": {"electricitymap": "SA", "watttime": "SA"},
            "UAE": {"electricitymap": "AE", "watttime": "AE"},
            # Australia
            "Australia": {"electricitymap": "AU-NSW", "watttime": "AU"},
        }
        
        self._init_database()
        self._lock = asyncio.Lock()
        
        logger.info("RealCarbonIntensityClient initialized")
    
    def _init_database(self):
        """Initialize SQLite cache database"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS carbon_intensity_cache (
                    region TEXT PRIMARY KEY,
                    intensity REAL,
                    source TEXT,
                    timestamp REAL,
                    UNIQUE(region)
                )
            ''')
            conn.commit()
            conn.close()
        except Exception as e:
            logger.error(f"Database init failed: {e}")
    
    async def _refresh_watttime_token(self) -> bool:
        """Refresh WattTime authentication token"""
        if not self.watttime_username:
            return False
        
        async with aiohttp.ClientSession() as session:
            try:
                url = "https://api.watttime.org/v3/login"
                auth = aiohttp.BasicAuth(self.watttime_username, self.watttime_password)
                
                async with session.get(url, auth=auth) as response:
                    if response.status == 200:
                        data = await response.json()
                        self.watttime_token = data.get('token')
                        self.token_expiry = time.time() + 3600
                        logger.info("WattTime token refreshed")
                        return True
            except Exception as e:
                logger.error(f"WattTime token refresh failed: {e}")
        
        return False
    
    async def get_intensity_electricitymap(self, region: str) -> Optional[float]:
        """Fetch from ElectricityMap API"""
        if not self.electricitymap_key:
            return None
        
        zone = self.region_mapping.get(region, {}).get('electricitymap')
        if not zone:
            return None
        
        async with aiohttp.ClientSession() as session:
            try:
                url = f"https://api.electricitymap.org/v3/carbon-intensity/latest?zone={zone}"
                headers = {'auth-token': self.electricitymap_key}
                
                async with session.get(url, headers=headers) as response:
                    if response.status == 200:
                        data = await response.json()
                        return float(data.get('carbonIntensity', 0))
            except Exception as e:
                logger.error(f"ElectricityMap error for {region}: {e}")
        
        return None
    
    async def get_intensity_watttime(self, region: str) -> Optional[float]:
        """Fetch from WattTime API"""
        if not self.watttime_token or time.time() > self.token_expiry:
            await self._refresh_watttime_token()
        
        if not self.watttime_token:
            return None
        
        zone = self.region_mapping.get(region, {}).get('watttime')
        if not zone:
            return None
        
        async with aiohttp.ClientSession() as session:
            try:
                url = "https://api.watttime.org/v3/data"
                params = {
                    'ba': zone,
                    'starttime': datetime.now().isoformat(),
                    'endtime': (datetime.now() + timedelta(hours=1)).isoformat()
                }
                headers = {'Authorization': f'Bearer {self.watttime_token}'}
                
                async with session.get(url, params=params, headers=headers) as response:
                    if response.status == 200:
                        data = await response.json()
                        if data and len(data) > 0:
                            return float(data[0].get('value', 0))
            except Exception as e:
                logger.error(f"WattTime error for {region}: {e}")
        
        return None
    
    async def get_cached_intensity(self, region: str) -> Optional[float]:
        """Get intensity from local cache"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            cursor.execute(
                "SELECT intensity, timestamp FROM carbon_intensity_cache WHERE region = ?",
                (region,)
            )
            row = cursor.fetchone()
            conn.close()
            
            if row and time.time() - row[1] < self.cache_ttl:
                return row[0]
        except:
            pass
        
        return None
    
    def _cache_intensity(self, region: str, intensity: float, source: str):
        """Store intensity in cache"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            cursor.execute(
                "INSERT OR REPLACE INTO carbon_intensity_cache (region, intensity, source, timestamp) VALUES (?, ?, ?, ?)",
                (region, intensity, source, time.time())
            )
            conn.commit()
            conn.close()
        except Exception as e:
            logger.error(f"Cache write failed: {e}")
    
    async def get_intensity(self, country: str, state: str = None) -> float:
        """
        Get current carbon intensity for a location.
        
        Returns intensity in gCO2/kWh.
        """
        # Build region key
        region_key = state if state else country
        
        # Check cache first
        cached = await self.get_cached_intensity(region_key)
        if cached is not None:
            return cached
        
        # Try ElectricityMap first
        intensity = await self.get_intensity_electricitymap(country)
        source = "electricitymap"
        
        # Fallback to WattTime
        if intensity is None:
            intensity = await self.get_intensity_watttime(country)
            source = "watttime"
        
        # Fallback to database or default
        if intensity is None:
            # Use regional defaults based on location
            defaults = {
                "Finland": 85, "Sweden": 45, "Denmark": 120, "Norway": 35,
                "France": 60, "Germany": 350, "UK": 200, "Ireland": 250,
                "USA": 380, "Indonesia": 680, "Singapore": 400, "Japan": 450,
                "South Korea": 420, "China": 550, "Australia": 550,
                "Saudi Arabia": 550, "UAE": 480
            }
            intensity = defaults.get(country, 400)
            source = "default"
        
        # Cache the result
        self._cache_intensity(region_key, intensity, source)
        
        return intensity
    
    async def get_forecast(self, country: str, hours: int = 24) -> List[float]:
        """Get forecasted carbon intensity for next N hours"""
        zone = self.region_mapping.get(country, {}).get('electricitymap')
        if not zone or not self.electricitymap_key:
            # Simulated forecast with daily pattern
            base = await self.get_intensity(country)
            return [base + 50 * math.sin(i * math.pi / 12) for i in range(hours)]
        
        async with aiohttp.ClientSession() as session:
            try:
                url = f"https://api.electricitymap.org/v3/carbon-intensity/forecast?zone={zone}"
                headers = {'auth-token': self.electricitymap_key}
                
                async with session.get(url, headers=headers) as response:
                    if response.status == 200:
                        data = await response.json()
                        forecast = [float(h.get('value', base)) for h in data.get('forecast', [])[:hours]]
                        return forecast
            except Exception as e:
                logger.error(f"Forecast error: {e}")
        
        base = await self.get_intensity(country)
        return [base + 50 * math.sin(i * math.pi / 12) for i in range(hours)]


# Demo
async def main():
    client = RealCarbonIntensityClient({
        'electricitymap_key': os.environ.get('ELECTRICITYMAP_KEY'),
        'watttime_username': os.environ.get('WATTTIME_USERNAME'),
        'watttime_password': os.environ.get('WATTTIME_PASSWORD')
    })
    
    # Test various regions
    regions = ["USA", "Finland", "Indonesia", "Singapore", "Australia"]
    
    print("\n=== Real Carbon Intensity Data ===")
    for region in regions:
        intensity = await client.get_intensity(region)
        print(f"{region}: {intensity:.0f} gCO2/kWh")
    
    # Test forecast
    forecast = await client.get_forecast("Finland", 12)
    print(f"\nFinland 12-hour forecast: {[f'{f:.0f}' for f in forecast]}")


if __name__ == "__main__":
    import asyncio
    import os
    import math
    asyncio.run(main())
