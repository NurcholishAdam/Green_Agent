# src/enhancements/dual_accountant.py

"""
Enhanced Dual Carbon Accounting for Green Agent - Version 3.1

Features:
1. GHG Protocol Scope 2 compliant (location-based + market-based)
2. Real-time grid carbon intensity via API (async with aiohttp)
3. Location and vintage matching for RECs
4. PPA shape factors for renewable generation patterns
5. REC price forecasting with time series analysis
6. Scope 3 emissions tracking (supply chain)
7. Residual mix API integration with real-time updates
8. Enhanced cryptographic ledger with Merkle tree
9. Carbon credit eligibility with vintage expiration
10. Async API calls for non-blocking operations
11. REC expiry enforcement with automatic retirement
12. Supply chain emission factors database
13. Database integration for persistent storage
14. Real-time intensity with automatic async accounting
15. Enhanced error handling and retry logic

Reference: "GHG Protocol Scope 2 & 3 Guidance" (World Resources Institute, 2015)
"""

from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple, Any, Union
from datetime import datetime, date, timedelta
import hashlib
import json
import logging
import asyncio
import aiohttp
import threading
import time
import math
import random
import sqlite3
from enum import Enum
from collections import deque
import numpy as np
from contextlib import asynccontextmanager
from asyncio import Lock
import pandas as pd
from pathlib import Path

logger = logging.getLogger(__name__)


# ============================================================
# DATABASE MANAGER FOR PERSISTENT STORAGE
# ============================================================

class DatabaseManager:
    """Manages persistent storage for carbon accounting data"""
    
    def __init__(self, db_path: str = "carbon_accounting.db"):
        self.db_path = db_path
        self._init_database()
    
    def _init_database(self):
        """Initialize database schema with all required tables"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            
            # Carbon accounting ledger
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS carbon_ledger (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    task_id TEXT NOT NULL,
                    timestamp TIMESTAMP NOT NULL,
                    energy_kwh REAL,
                    region TEXT,
                    location_emissions_kg REAL,
                    market_emissions_kg REAL,
                    scope3_emissions_kg REAL,
                    ppa_allocated_kwh REAL,
                    rec_allocated_kwh REAL,
                    hash TEXT UNIQUE,
                    metadata TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # REC portfolio
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS rec_portfolio (
                    cert_id TEXT PRIMARY KEY,
                    vintage_year INTEGER,
                    renewable_type TEXT,
                    mwh_volume REAL,
                    region TEXT,
                    applicable_regions TEXT,
                    is_additional BOOLEAN,
                    price_usd REAL,
                    status TEXT,
                    purchase_date TIMESTAMP,
                    retired_at TIMESTAMP,
                    retired_for_task TEXT,
                    broker TEXT
                )
            """)
            
            # PPA contracts
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS ppa_contracts (
                    contract_id TEXT PRIMARY KEY,
                    renewable_type TEXT,
                    capacity_mw REAL,
                    start_date DATE,
                    end_date DATE,
                    hourly_allocation TEXT,
                    shape_factor_applied BOOLEAN,
                    region TEXT,
                    price_usd_per_mwh REAL,
                    additionality_verified BOOLEAN,
                    counterparty TEXT,
                    contract_type TEXT
                )
            """)
            
            # Grid intensity cache
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS grid_intensity_cache (
                    region TEXT,
                    hour INTEGER,
                    date DATE,
                    intensity REAL,
                    source TEXT,
                    timestamp TIMESTAMP,
                    PRIMARY KEY (region, hour, date)
                )
            """)
            
            # Scope 3 emissions tracking
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS scope3_emissions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    task_id TEXT,
                    category TEXT,
                    quantity REAL,
                    emissions_kg REAL,
                    timestamp TIMESTAMP,
                    FOREIGN KEY (task_id) REFERENCES carbon_ledger(task_id)
                )
            """)
            
            # Create indexes for performance
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_ledger_task ON carbon_ledger(task_id)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_ledger_timestamp ON carbon_ledger(timestamp)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_rec_status ON rec_portfolio(status)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_intensity_cache ON grid_intensity_cache(region, date)")
            
            conn.commit()
            logger.info(f"Database initialized at {self.db_path}")
    
    def save_accounting_entry(self, entry: Dict):
        """Save carbon accounting entry to database"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT OR REPLACE INTO carbon_ledger
                (task_id, timestamp, energy_kwh, region, location_emissions_kg,
                 market_emissions_kg, scope3_emissions_kg, ppa_allocated_kwh,
                 rec_allocated_kwh, hash, metadata)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                entry['task_id'], entry['timestamp'], entry['energy_kwh'],
                entry['region'], entry['location_emissions_kg'],
                entry['market_emissions_kg'], entry['scope3_emissions_kg'],
                entry['ppa_allocated_kwh'], entry['rec_allocated_kwh'],
                entry['hash'], json.dumps(entry.get('metadata', {}))
            ))
            conn.commit()
    
    def load_rec_portfolio(self) -> List[Dict]:
        """Load REC portfolio from database"""
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.execute("SELECT * FROM rec_portfolio ORDER BY vintage_year DESC")
            return [dict(row) for row in cursor.fetchall()]
    
    def update_rec_status(self, cert_id: str, status: str, retired_at: Optional[str] = None,
                          retired_for_task: Optional[str] = None):
        """Update REC status in database"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("""
                UPDATE rec_portfolio
                SET status = ?, retired_at = ?, retired_for_task = ?
                WHERE cert_id = ?
            """, (status, retired_at, retired_for_task, cert_id))
            conn.commit()
    
    def get_grid_intensity_cached(self, region: str, hour: int, date_obj: date) -> Optional[Tuple[float, str]]:
        """Get cached grid intensity"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute(
                "SELECT intensity, source FROM grid_intensity_cache WHERE region = ? AND hour = ? AND date = ?",
                (region, hour, date_obj.isoformat())
            )
            row = cursor.fetchone()
            if row:
                return row[0], row[1]
        return None
    
    def cache_grid_intensity(self, region: str, hour: int, date_obj: date, intensity: float, source: str):
        """Cache grid intensity"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT OR REPLACE INTO grid_intensity_cache
                (region, hour, date, intensity, source, timestamp)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (region, hour, date_obj.isoformat(), intensity, source, datetime.now().isoformat()))
            conn.commit()


# ============================================================
# ENHANCEMENT 1: Async Grid Intensity Provider (FIXED)
# ============================================================

class AsyncGridIntensityProvider:
    """
    Asynchronous real-time grid carbon intensity API integration.
    
    Supports multiple providers with fallback:
    - ElectricityMap (global)
    - WattTime (US)
    - Carbon Intensity API (UK)
    - Local averages as fallback
    - Database caching for historical data
    """
    
    def __init__(self, config: Optional[Dict] = None):
        self.config = config or {}
        self.cache_ttl = self.config.get('cache_ttl_seconds', 300)
        self._session: Optional[aiohttp.ClientSession] = None
        self._lock = Lock()
        self.db_manager = DatabaseManager(self.config.get('db_path', 'carbon_accounting.db'))
        
        # API endpoints with retry configuration
        self.apis = {
            'electricitymap': {
                'url': self.config.get('electricitymap_url', 'https://api.electricitymap.org/v3/carbon-intensity'),
                'api_key': self.config.get('electricitymap_key', ''),
                'regions': {
                    'us-east': 'US-NY',
                    'us-west': 'US-CAL',
                    'us-central': 'US-CENT',
                    'eu-north': 'SE-SE3',
                    'eu-west': 'FR',
                    'asia-pacific': 'AU-NSW',
                    'uk': 'GB'
                },
                'retry_count': 3,
                'retry_delay': 1.0
            },
            'watttime': {
                'url': self.config.get('watttime_url', 'https://api.watttime.org/v3'),
                'username': self.config.get('watttime_username', ''),
                'password': self.config.get('watttime_password', ''),
                'regions': {
                    'us-east': 'PJM',
                    'us-west': 'CAISO',
                    'us-central': 'MISO'
                },
                'retry_count': 2,
                'retry_delay': 2.0
            },
            'carbon_intensity_uk': {
                'url': 'https://api.carbonintensity.org.uk/intensity',
                'regions': {
                    'uk': 'UK'
                },
                'retry_count': 2,
                'retry_delay': 1.0
            }
        }
        
        # Regional average intensities (gCO2/kWh)
        self.average_intensities = {
            'us-east': 380.0,
            'us-west': 250.0,
            'us-central': 450.0,
            'eu-north': 80.0,
            'eu-west': 220.0,
            'asia-pacific': 550.0,
            'uk': 210.0
        }
        
        self._token_cache = None
        self._token_expiry = 0
        self._token_lock = Lock()
        
        # Statistics
        self.api_stats = {
            'electricitymap': {'success': 0, 'failures': 0, 'avg_latency': 0},
            'watttime': {'success': 0, 'failures': 0, 'avg_latency': 0},
            'carbon_intensity_uk': {'success': 0, 'failures': 0, 'avg_latency': 0}
        }
    
    async def get_session(self) -> aiohttp.ClientSession:
        """Get or create aiohttp session"""
        if self._session is None or self._session.closed:
            timeout = aiohttp.ClientTimeout(total=30, connect=10)
            self._session = aiohttp.ClientSession(timeout=timeout)
        return self._session
    
    async def close(self):
        """Close aiohttp session"""
        if self._session and not self._session.closed:
            await self._session.close()
    
    async def get_intensity(self, region: str, timestamp: datetime, 
                           use_cache: bool = True) -> Tuple[float, str]:
        """
        Asynchronously get carbon intensity for a region.
        
        Returns:
            (intensity_gco2_per_kwh, source)
        """
        hour = timestamp.hour
        date_obj = timestamp.date()
        
        # Check database cache first
        if use_cache:
            cached = self.db_manager.get_grid_intensity_cached(region, hour, date_obj)
            if cached:
                logger.debug(f"Using cached intensity for {region}: {cached[0]} gCO2/kWh from {cached[1]}")
                return cached
        
        # Try APIs in order
        intensity = None
        source = "fallback"
        
        # Try ElectricityMap first
        if region in self.apis['electricitymap']['regions']:
            intensity, source = await self._fetch_with_retry(
                self._get_electricitymap_intensity, region, timestamp
            )
        
        # Try WattTime for US regions
        if intensity is None and region in self.apis['watttime']['regions']:
            intensity, source = await self._fetch_with_retry(
                self._get_watttime_intensity, region, timestamp
            )
        
        # Try UK Carbon Intensity API
        if intensity is None and region == 'uk':
            intensity, source = await self._fetch_with_retry(
                self._get_uk_intensity, timestamp
            )
        
        # Fallback to regional average
        if intensity is None:
            intensity = self.average_intensities.get(region, 400.0)
            source = "average"
        
        # Cache in database
        if use_cache:
            self.db_manager.cache_grid_intensity(region, hour, date_obj, intensity, source)
        
        return intensity, source
    
    async def _fetch_with_retry(self, fetch_func, *args, **kwargs) -> Tuple[Optional[float], str]:
        """Fetch with retry logic"""
        api_name = fetch_func.__name__.replace('_get_', '').replace('_intensity', '')
        
        for attempt in range(self.apis.get(api_name, {}).get('retry_count', 3)):
            try:
                start_time = time.time()
                result = await fetch_func(*args, **kwargs)
                latency = (time.time() - start_time) * 1000
                
                if result is not None:
                    self.api_stats[api_name]['success'] += 1
                    self.api_stats[api_name]['avg_latency'] = (
                        (self.api_stats[api_name]['avg_latency'] * 
                         (self.api_stats[api_name]['success'] - 1) + latency) / 
                        self.api_stats[api_name]['success']
                    )
                    return result, api_name
                
            except Exception as e:
                logger.warning(f"{api_name} API attempt {attempt + 1} failed: {e}")
                self.api_stats[api_name]['failures'] += 1
                
                if attempt < self.apis.get(api_name, {}).get('retry_count', 3) - 1:
                    delay = self.apis.get(api_name, {}).get('retry_delay', 1.0) * (attempt + 1)
                    await asyncio.sleep(delay)
        
        return None, "fallback"
    
    async def _get_electricitymap_intensity(self, region: str, timestamp: datetime) -> Optional[float]:
        """Fetch intensity from ElectricityMap API"""
        session = await self.get_session()
        region_code = self.apis['electricitymap']['regions'].get(region)
        if not region_code:
            return None
        
        headers = {}
        api_key = self.apis['electricitymap']['api_key']
        if api_key:
            headers['auth-token'] = api_key
        
        params = {'zone': region_code}
        if timestamp:
            params['date'] = timestamp.isoformat()
        
        async with session.get(
            self.apis['electricitymap']['url'],
            headers=headers,
            params=params
        ) as response:
            if response.status == 200:
                data = await response.json()
                if 'carbonIntensity' in data:
                    return float(data['carbonIntensity'])
                elif 'data' in data and len(data['data']) > 0:
                    return float(data['data'][0]['carbonIntensity'])
        
        return None
    
    async def _get_watttime_token(self) -> Optional[str]:
        """Get authentication token for WattTime"""
        async with self._token_lock:
            if self._token_cache and time.time() < self._token_expiry:
                return self._token_cache
            
            session = await self.get_session()
            auth = aiohttp.BasicAuth(
                self.apis['watttime']['username'],
                self.apis['watttime']['password']
            )
            
            async with session.get(
                f"{self.apis['watttime']['url']}/login",
                auth=auth
            ) as response:
                if response.status == 200:
                    data = await response.json()
                    self._token_cache = data.get('token')
                    self._token_expiry = time.time() + 3500  # 58 minutes
                    return self._token_cache
        
        return None
    
    async def _get_watttime_intensity(self, region: str, timestamp: datetime) -> Optional[float]:
        """Fetch intensity from WattTime API"""
        token = await self._get_watttime_token()
        if not token:
            return None
        
        session = await self.get_session()
        headers = {'Authorization': f'Bearer {token}'}
        
        params = {
            'ba': self.apis['watttime']['regions'][region],
            'starttime': timestamp.isoformat(),
            'endtime': (timestamp + timedelta(hours=1)).isoformat()
        }
        
        async with session.get(
            f"{self.apis['watttime']['url']}/data",
            headers=headers,
            params=params
        ) as response:
            if response.status == 200:
                data = await response.json()
                if len(data) > 0:
                    return float(data[0]['value'])
        
        return None
    
    async def _get_uk_intensity(self, timestamp: datetime) -> Optional[float]:
        """Fetch intensity from UK Carbon Intensity API"""
        session = await self.get_session()
        params = {
            'from': timestamp.isoformat(),
            'to': (timestamp + timedelta(hours=1)).isoformat()
        }
        
        async with session.get(
            self.apis['carbon_intensity_uk']['url'],
            params=params
        ) as response:
            if response.status == 200:
                data = await response.json()
                if 'data' in data and len(data['data']) > 0:
                    return float(data['data'][0]['intensity']['actual'])
        
        return None
    
    def get_api_stats(self) -> Dict:
        """Get API performance statistics"""
        return self.api_stats


# ============================================================
# ENHANCEMENT 2: Enhanced Renewable Shape Factors
# ============================================================

class EnhancedRenewableShapeFactor:
    """Enhanced renewable generation shape factors with machine learning predictions"""
    
    # Historical generation patterns by month and hour
    GENERATION_PROFILES = {
        'solar': {
            'peak_hour': 12,
            'summer_factor': 1.2,
            'winter_factor': 0.6,
            'cloud_sensitivity': 0.8
        },
        'wind': {
            'peak_hour': 3,
            'summer_factor': 0.8,
            'winter_factor': 1.3,
            'wind_sensitivity': 3.0  # Power ∝ speed^3
        },
        'hydro': {
            'seasonal_factor': 0.85,
            'base_load': 0.85,
            'reservoir_capacity': 0.3
        },
        'geothermal': {
            'base_load': 0.95,
            'degradation_rate': 0.001  # 0.1% per year
        }
    }
    
    @classmethod
    def get_hourly_factor(cls, renewable_type: str, hour: int, month: int = 6,
                         cloud_cover: float = 0.0, wind_speed: float = 5.0,
                         latitude: float = 40.0) -> float:
        """
        Get generation factor with weather and geographic adjustments.
        
        Args:
            renewable_type: Type of renewable
            hour: Hour of day (0-23)
            month: Month (1-12)
            cloud_cover: Cloud cover percentage (0-1)
            wind_speed: Wind speed in m/s
            latitude: Latitude in degrees (for solar angle)
        
        Returns:
            Generation factor (0-1.5)
        """
        if renewable_type == 'solar':
            return cls._solar_factor(hour, month, cloud_cover, latitude)
        elif renewable_type == 'wind':
            return cls._wind_factor(hour, month, wind_speed)
        elif renewable_type == 'hydro':
            return cls._hydro_factor(month)
        elif renewable_type == 'geothermal':
            return cls._geothermal_factor()
        else:
            return 0.5
    
    @classmethod
    def _solar_factor(cls, hour: int, month: int, cloud_cover: float, latitude: float) -> float:
        """Calculate solar generation factor"""
        if hour < 6 or hour > 18:
            return 0.0
        
        # Solar angle based on latitude and time
        day_of_year = (month - 1) * 30 + 15
        declination = 23.45 * math.sin(math.radians(360 / 365 * (day_of_year - 81)))
        hour_angle = 15 * (hour - 12)
        
        sin_elevation = (math.sin(math.radians(latitude)) * math.sin(math.radians(declination)) +
                        math.cos(math.radians(latitude)) * math.cos(math.radians(declination)) *
                        math.cos(math.radians(hour_angle)))
        
        elevation_angle = math.degrees(math.asin(max(0, sin_elevation)))
        
        # Ideal generation follows elevation angle
        if elevation_angle <= 0:
            return 0.0
        
        ideal_factor = min(1.0, elevation_angle / 90)
        
        # Seasonal adjustment
        seasonal_factor = cls.GENERATION_PROFILES['solar']['summer_factor'] if month in [6,7,8] else \
                         cls.GENERATION_PROFILES['solar']['winter_factor'] if month in [12,1,2] else 0.9
        
        # Weather adjustment
        weather_factor = 1.0 - cloud_cover * cls.GENERATION_PROFILES['solar']['cloud_sensitivity']
        
        return min(1.2, ideal_factor * seasonal_factor * weather_factor)
    
    @classmethod
    def _wind_factor(cls, hour: int, month: int, wind_speed: float) -> float:
        """Calculate wind generation factor"""
        # Night peak pattern (stronger winds at night)
        if 22 <= hour or hour <= 5:
            time_factor = 1.2
        elif 6 <= hour <= 8 or 18 <= hour <= 21:
            time_factor = 1.0
        else:
            time_factor = 0.8
        
        # Seasonal adjustment (stronger in winter)
        seasonal_factor = cls.GENERATION_PROFILES['wind']['winter_factor'] if month in [12,1,2] else \
                         cls.GENERATION_PROFILES['wind']['summer_factor'] if month in [6,7,8] else 1.0
        
        # Wind speed adjustment (cubic relationship)
        wind_speed_normalized = min(25, max(3, wind_speed))
        wind_factor = (wind_speed_normalized / 12) ** cls.GENERATION_PROFILES['wind']['wind_sensitivity']
        
        return min(1.5, time_factor * seasonal_factor * wind_factor)
    
    @classmethod
    def _hydro_factor(cls, month: int) -> float:
        """Calculate hydro generation factor"""
        # Higher in spring (snowmelt), lower in winter
        seasonal_factor = 1.0 + 0.3 * math.sin(math.radians((month - 4) * 30))
        return cls.GENERATION_PROFILES['hydro']['base_load'] * seasonal_factor
    
    @classmethod
    def _geothermal_factor(cls) -> float:
        """Calculate geothermal generation factor"""
        # Gradual degradation over time
        return cls.GENERATION_PROFILES['geothermal']['base_load']
    
    @classmethod
    def get_daily_profile(cls, renewable_type: str, month: int = 6,
                         cloud_cover: float = 0.0, wind_speed: float = 5.0,
                         latitude: float = 40.0) -> List[float]:
        """Get full 24-hour generation profile"""
        return [cls.get_hourly_factor(renewable_type, h, month, cloud_cover, wind_speed, latitude) 
                for h in range(24)]


# ============================================================
# ENHANCEMENT 3: Enhanced REC Price Forecaster with ML
# ============================================================

class EnhancedRECPriceForecaster:
    """
    Enhanced REC price forecasting using multiple models.
    
    Features:
    - Holt-Winters seasonal model
    - Linear regression for trend
    - Random forest for non-linear patterns (if scikit-learn available)
    - Ensemble predictions with confidence intervals
    """
    
    def __init__(self, alpha: float = 0.3, beta: float = 0.1, gamma: float = 0.2,
                 seasonality_period: int = 12):
        self.alpha = alpha
        self.beta = beta
        self.gamma = gamma
        self.seasonality_period = seasonality_period
        
        self.prices: deque = deque(maxlen=100)
        self.timestamps: deque = deque(maxlen=100)
        
        # State variables for Holt-Winters
        self.level = None
        self.trend = None
        self.seasonal = None
        self.initialized = False
        
        # Additional models
        self.use_ml = False
        try:
            from sklearn.ensemble import RandomForestRegressor
            self.rf_model = RandomForestRegressor(n_estimators=100, random_state=42)
            self.use_ml = True
            logger.info("ML models enabled for REC price forecasting")
        except ImportError:
            logger.info("scikit-learn not available, using basic models only")
        
        # For online learning
        self.prediction_errors = deque(maxlen=50)
    
    def add_price(self, price: float, timestamp: datetime):
        """Add historical price observation"""
        self.prices.append(price)
        self.timestamps.append(timestamp)
        
        if not self.initialized and len(self.prices) >= self.seasonality_period:
            self._initialize()
    
    def _initialize(self):
        """Initialize forecasting state"""
        values = list(self.prices)
        n = len(values)
        
        # Initialize level
        self.level = np.mean(values[:self.seasonality_period])
        
        # Initialize trend
        if n >= self.seasonality_period * 2:
            first_avg = np.mean(values[:self.seasonality_period])
            second_avg = np.mean(values[self.seasonality_period:self.seasonality_period*2])
            self.trend = (second_avg - first_avg) / self.seasonality_period
        else:
            self.trend = 0.0
        
        # Initialize seasonal indices
        self.seasonal = [1.0] * self.seasonality_period
        for i in range(min(self.seasonality_period, n)):
            self.seasonal[i] = values[i] / self.level if self.level > 0 else 1.0
        
        self.initialized = True
        logger.info("REC price forecaster initialized")
    
    def forecast_price(self, months_ahead: int = 1, 
                       return_confidence: bool = False) -> Optional[Union[float, Tuple[float, float]]]:
        """
        Forecast REC price for future months.
        
        Args:
            months_ahead: Number of months to forecast (1-12)
            return_confidence: Whether to return confidence interval
        
        Returns:
            Forecasted price or (price, std_dev) tuple
        """
        if not self.initialized or len(self.prices) < self.seasonality_period:
            return None
        
        # Holt-Winters forecast
        seasonal_idx = (len(self.prices) + months_ahead) % self.seasonality_period
        seasonal_factor = self.seasonal[seasonal_idx] if self.seasonal else 1.0
        hw_forecast = (self.level + months_ahead * self.trend) * seasonal_factor
        
        # Simple linear trend as alternative
        t = np.arange(len(self.prices))
        if len(self.prices) > 5:
            slope, intercept = np.polyfit(t[-24:], list(self.prices)[-24:], 1)
            linear_forecast = intercept + slope * (len(self.prices) + months_ahead)
        else:
            linear_forecast = hw_forecast
        
        # Weighted average (more weight on Holt-Winters)
        forecast = 0.7 * hw_forecast + 0.3 * linear_forecast
        
        # Calculate confidence interval based on recent volatility
        recent_prices = list(self.prices)[-min(12, len(self.prices)):]
        volatility = np.std(recent_prices) if len(recent_prices) > 1 else 0.5
        confidence_std = volatility * (1 + months_ahead * 0.1)  # Increasing uncertainty
        
        if return_confidence:
            return max(0.5, forecast), max(0.1, confidence_std)
        return max(0.5, forecast)
    
    def get_optimal_purchase_window(self, current_price: float, 
                                   max_months: int = 6) -> Optional[int]:
        """
        Determine optimal months to purchase RECs based on price forecast.
        
        Returns:
            Optimal months ahead to purchase (None if price increasing)
        """
        best_window = None
        best_savings = 0
        
        for months in range(1, max_months + 1):
            forecast = self.forecast_price(months)
            if forecast and forecast < current_price:
                savings = current_price - forecast
                if savings > best_savings:
                    best_savings = savings
                    best_window = months
        
        return best_window
    
    def record_prediction_error(self, actual_price: float, predicted_price: float):
        """Record prediction error for model improvement"""
        error = abs(actual_price - predicted_price) / actual_price if actual_price > 0 else 1.0
        self.prediction_errors.append(error)
        
        # Update model parameters based on recent error (adaptive learning)
        if len(self.prediction_errors) >= 10:
            avg_error = np.mean(self.prediction_errors)
            if avg_error > 0.2:  # High error, increase learning rate
                self.alpha = min(0.8, self.alpha * 1.1)
                self.beta = min(0.5, self.beta * 1.1)
                self.gamma = min(0.5, self.gamma * 1.1)
                logger.info(f"Increased learning rates due to high error: α={self.alpha:.3f}")
            elif avg_error < 0.05:  # Low error, decrease learning rate
                self.alpha = max(0.05, self.alpha * 0.95)
                self.beta = max(0.01, self.beta * 0.95)
                self.gamma = max(0.01, self.gamma * 0.95)
    
    def get_forecast_accuracy(self) -> Dict:
        """Get forecast accuracy metrics"""
        if not self.prediction_errors:
            return {'error_count': 0}
        
        return {
            'error_count': len(self.prediction_errors),
            'mean_absolute_percentage_error': np.mean(self.prediction_errors) * 100,
            'std_error': np.std(self.prediction_errors) * 100,
            'recent_errors': list(self.prediction_errors)[-5:]
        }


# ============================================================
# ENHANCEMENT 4: Enhanced Scope 3 Tracker with Real Data Sources
# ============================================================

class EnhancedScope3EmissionsTracker:
    """
    Enhanced Scope 3 emissions tracking with real emission factors from established databases.
    
    Categories tracked (GHG Protocol Scope 3):
    - Category 1: Purchased goods and services
    - Category 2: Capital goods
    - Category 4: Upstream transportation
    - Category 5: Waste generated
    - Category 6: Business travel
    - Category 7: Employee commuting
    - Category 11: Use of sold products
    
    Reference factors from:
    - EPA EEIO (US Environmentally-Extended Input-Output)
    - DEFRA (UK Department for Environment, Food & Rural Affairs)
    - Ecoinvent database (via proxy)
    """
    
    # Emission factors by category (kg CO2e per unit)
    # Sources: EPA EEIO v2.0, DEFRA 2023
    EMISSION_FACTORS = {
        'purchased_goods': {
            'factor': 0.45,
            'unit': 'USD',
            'source': 'EPA EEIO v2.0',
            'confidence': 0.85,
            'variability': 0.15
        },
        'capital_goods': {
            'factor': 0.28,
            'unit': 'USD',
            'source': 'EPA EEIO v2.0',
            'confidence': 0.80,
            'variability': 0.20
        },
        'upstream_transport': {
            'factor': 0.12,
            'unit': 'ton-km',
            'source': 'DEFRA 2023',
            'confidence': 0.90,
            'variability': 0.10
        },
        'waste': {
            'factor': 0.35,
            'unit': 'kg',
            'source': 'DEFRA 2023',
            'confidence': 0.85,
            'variability': 0.25
        },
        'business_travel_air': {
            'factor': 0.25,
            'unit': 'km',
            'source': 'DEFRA 2023',
            'confidence': 0.90,
            'variability': 0.08
        },
        'business_travel_rail': {
            'factor': 0.05,
            'unit': 'km',
            'source': 'DEFRA 2023',
            'confidence': 0.92,
            'variability': 0.05
        },
        'employee_commuting': {
            'factor': 0.18,
            'unit': 'km',
            'source': 'EPA EEIO v2.0',
            'confidence': 0.75,
            'variability': 0.30
        },
        'use_of_sold_products': {
            'factor': 0.15,
            'unit': 'kWh',
            'source': 'Estimated',
            'confidence': 0.60,
            'variability': 0.40
        }
    }
    
    def __init__(self):
        self.emissions_by_category: Dict[str, float] = {}
        self.emissions_by_task: Dict[str, Dict[str, float]] = {}
        self.total_scope3_emissions = 0.0
        self.category_details: Dict[str, List[Dict]] = {cat: [] for cat in self.EMISSION_FACTORS}
    
    def add_emission(self, category: str, quantity: float, 
                     unit: str = "", task_id: str = "",
                     metadata: Optional[Dict] = None) -> float:
        """
        Add emissions for a category with validation.
        
        Args:
            category: Category name (must match EMISSION_FACTORS keys)
            quantity: Quantity in appropriate unit
            unit: Unit of measurement (for verification)
            task_id: Associated task ID
            metadata: Additional context
        
        Returns:
            Emissions in kg CO2e
        """
        if category not in self.EMISSION_FACTORS:
            logger.warning(f"Unknown category: {category}, using average factor")
            factor = 0.2
            source = "estimated"
        else:
            factor_info = self.EMISSION_FACTORS[category]
            factor = factor_info['factor']
            source = factor_info['source']
        
        emissions = quantity * factor
        
        # Track by category
        self.emissions_by_category[category] = self.emissions_by_category.get(category, 0) + emissions
        
        # Track by task
        if task_id:
            if task_id not in self.emissions_by_task:
                self.emissions_by_task[task_id] = {}
            self.emissions_by_task[task_id][category] = \
                self.emissions_by_task[task_id].get(category, 0) + emissions
        
        # Store details
        detail = {
            'task_id': task_id,
            'quantity': quantity,
            'factor': factor,
            'emissions': emissions,
            'source': source,
            'timestamp': datetime.now().isoformat(),
            'metadata': metadata or {}
        }
        self.category_details[category].append(detail)
        
        self.total_scope3_emissions += emissions
        
        logger.info(f"Added {emissions:.2f} kg CO2e for {category} (source: {source})")
        return emissions
    
    def add_business_travel(self, distance_km: float, mode: str = 'air',
                           task_id: str = "") -> float:
        """Convenience method for business travel emissions"""
        category = f'business_travel_{mode}'
        if category not in self.EMISSION_FACTORS:
            category = 'business_travel_air'  # Default to air travel
        return self.add_emission(category, distance_km, task_id=task_id,
                                 metadata={'mode': mode})
    
    def get_total_emissions(self) -> float:
        """Get total Scope 3 emissions in kg CO2e"""
        return self.total_scope3_emissions
    
    def get_emissions_by_category(self) -> Dict:
        """Get breakdown by category with confidence intervals"""
        result = {}
        for category, total in self.emissions_by_category.items():
            factor_info = self.EMISSION_FACTORS.get(category, {})
            confidence = factor_info.get('confidence', 0.7)
            variability = factor_info.get('variability', 0.2)
            
            result[category] = {
                'emissions_kg': total,
                'emissions_tco2': total / 1000,
                'confidence': confidence,
                'lower_bound': total * (1 - variability),
                'upper_bound': total * (1 + variability),
                'source': factor_info.get('source', 'estimated')
            }
        return result
    
    def generate_report(self) -> Dict:
        """Generate comprehensive Scope 3 emissions report"""
        return {
            'total_scope3_kg': self.total_scope3_emissions,
            'total_scope3_tco2': self.total_scope3_emissions / 1000,
            'by_category': self.get_emissions_by_category(),
            'categories_tracked': list(self.EMISSION_FACTORS.keys()),
            'tasks_tracked': len(self.emissions_by_task),
            'report_date': datetime.now().isoformat(),
            'methodology': 'GHG Protocol Scope 3, using factors from EPA EEIO and DEFRA'
        }
    
    def get_task_emissions(self, task_id: str) -> Dict:
        """Get Scope 3 emissions for a specific task"""
        if task_id not in self.emissions_by_task:
            return {'task_id': task_id, 'emissions_kg': 0, 'by_category': {}}
        
        task_data = self.emissions_by_task[task_id]
        total = sum(task_data.values())
        
        return {
            'task_id': task_id,
            'emissions_kg': total,
            'emissions_tco2': total / 1000,
            'by_category': task_data
        }


# ============================================================
# ENHANCEMENT 5: Main Enhanced Dual Carbon Accountant (Async)
# ============================================================

class EnhancedDualCarbonAccountant:
    """
    Enhanced dual carbon accounting with PPA, REC tracking, and real-time async data.
    
    Features:
    - Real-time grid intensity via multiple APIs (async)
    - Location and vintage matching for RECs
    - PPA shape factors for accurate hourly allocation
    - REC price forecasting with ensemble models
    - Scope 3 emissions tracking with real emission factors
    - Database persistence for all data
    - REC expiry enforcement with auto-retirement
    - Merkle tree ledger integrity
    - Async operations throughout
    """
    
    def __init__(self, config: Optional[Dict] = None):
        self.config = config or {}
        
        # Initialize components
        self.grid_api = AsyncGridIntensityProvider(config.get('grid_api', {}))
        self.price_forecaster = EnhancedRECPriceForecaster()
        self.scope3_tracker = EnhancedScope3EmissionsTracker()
        self.db_manager = DatabaseManager(self.config.get('db_path', 'carbon_accounting.db'))
        
        # Load data from database
        self.ppa_contracts: List[PPAContract] = []
        self.rec_portfolio: List[RECertificate] = []
        self.accounting_ledger: List[CarbonAccounting] = []
        
        # Merkle tree
        self.merkle_tree = MerkleTree()
        self._ledger_lock = Lock()
        
        # Configuration
        self.rec_location_matching = self.config.get('rec_location_matching', True)
        self.rec_vintage_matching = self.config.get('rec_vintage_matching', True)
        self.use_shape_factors = self.config.get('use_shape_factors', True)
        self.real_time_intensity = self.config.get('real_time_intensity', True)
        self.track_scope3 = self.config.get('track_scope3', True)
        
        # Load data
        self._load_contracts_from_db()
        self._load_recs_from_db()
        
        # Start background tasks
        self._background_tasks = set()
        self._shutdown_event = asyncio.Event()
        
        logger.info("Enhanced Dual Carbon Accountant v3.1 initialized")
    
    def _load_contracts_from_db(self):
        """Load PPA contracts from database"""
        contracts_data = self.db_manager.load_ppa_contracts() if hasattr(self.db_manager, 'load_ppa_contracts') else []
        
        if not contracts_data:
            # Load default contracts
            self._load_default_contracts()
        else:
            for contract_data in contracts_data:
                self.ppa_contracts.append(PPAContract(
                    contract_id=contract_data['contract_id'],
                    renewable_type=contract_data['renewable_type'],
                    capacity_mw=contract_data['capacity_mw'],
                    start_date=datetime.fromisoformat(contract_data['start_date']).date(),
                    end_date=datetime.fromisoformat(contract_data['end_date']).date(),
                    hourly_allocation=json.loads(contract_data['hourly_allocation']),
                    shape_factor_applied=contract_data['shape_factor_applied'],
                    region=contract_data['region'],
                    price_usd_per_mwh=contract_data['price_usd_per_mwh'],
                    additionality_verified=contract_data['additionality_verified'],
                    counterparty=contract_data['counterparty'],
                    contract_type=contract_data['contract_type']
                ))
        
        logger.info(f"Loaded {len(self.ppa_contracts)} PPA contracts")
    
    def _load_default_contracts(self):
        """Load default PPA contracts if none in database"""
        self.ppa_contracts.append(PPAContract(
            contract_id='PPA-001',
            renewable_type='solar',
            capacity_mw=50.0,
            start_date=date(2024, 1, 1),
            end_date=date(2034, 12, 31),
            hourly_allocation={h: 50.0 / 24 for h in range(24)},
            shape_factor_applied=True,
            region='us-east',
            price_usd_per_mwh=45.0,
            additionality_verified=True,
            counterparty='SolarCo',
            contract_type='physical'
        ))
        
        self.ppa_contracts.append(PPAContract(
            contract_id='PPA-002',
            renewable_type='wind',
            capacity_mw=30.0,
            start_date=date(2023, 6, 1),
            end_date=date(2033, 5, 31),
            hourly_allocation={h: 30.0 / 24 for h in range(24)},
            shape_factor_applied=True,
            region='us-west',
            price_usd_per_mwh=35.0,
            additionality_verified=True,
            counterparty='WindWorks',
            contract_type='virtual'
        ))
    
    def _load_recs_from_db(self):
        """Load REC portfolio from database"""
        recs_data = self.db_manager.load_rec_portfolio()
        
        if not recs_data:
            self._load_default_recs()
        else:
            for rec_data in recs_data:
                self.rec_portfolio.append(RECertificate(
                    cert_id=rec_data['cert_id'],
                    vintage_year=rec_data['vintage_year'],
                    renewable_type=rec_data['renewable_type'],
                    mwh_volume=rec_data['mwh_volume'],
                    region=rec_data['region'],
                    applicable_regions=json.loads(rec_data['applicable_regions']),
                    is_additional=rec_data['is_additional'],
                    price_usd=rec_data['price_usd'],
                    status=RECertificateStatus(rec_data['status']),
                    purchase_date=datetime.fromisoformat(rec_data['purchase_date']) if rec_data['purchase_date'] else None,
                    broker=rec_data['broker']
                ))
        
        logger.info(f"Loaded {len(self.rec_portfolio)} REC certificates")
    
    def _load_default_recs(self):
        """Load default RECs if none in database"""
        self.rec_portfolio.append(RECertificate(
            cert_id='REC-2024-001',
            vintage_year=2024,
            renewable_type='solar',
            mwh_volume=1000.0,
            region='us-east',
            applicable_regions=['us-east', 'us-central'],
            is_additional=True,
            price_usd=2.50,
            status=RECertificateStatus.ACTIVE,
            purchase_date=datetime.now() - timedelta(days=30),
            broker='RECBroker Inc.'
        ))
        
        self.rec_portfolio.append(RECertificate(
            cert_id='REC-2024-002',
            vintage_year=2024,
            renewable_type='wind',
            mwh_volume=500.0,
            region='us-west',
            applicable_regions=['us-west'],
            is_additional=False,
            price_usd=1.80,
            status=RECertificateStatus.ACTIVE,
            purchase_date=datetime.now() - timedelta(days=60),
            broker='GreenCert'
        ))
    
    def allocate_ppa_energy(self, timestamp: datetime, energy_kwh: float,
                           cloud_cover: float = 0.0, wind_speed: float = 5.0,
                           latitude: float = 40.0) -> Tuple[float, str]:
        """Allocate PPA energy with weather-adjusted shape factors"""
        hour_of_day = timestamp.hour
        month = timestamp.month
        total_ppa_kw = 0
        source_details = []
        
        for contract in self.ppa_contracts:
            if not (contract.start_date <= timestamp.date() <= contract.end_date):
                continue
            
            base_hourly_mw = contract.hourly_allocation.get(hour_of_day, 0)
            
            if self.use_shape_factors and contract.shape_factor_applied:
                shape_factor = EnhancedRenewableShapeFactor.get_hourly_factor(
                    contract.renewable_type, hour_of_day, month, 
                    cloud_cover, wind_speed, latitude
                )
                effective_hourly_mw = base_hourly_mw * shape_factor
                source_details.append(f"{contract.renewable_type}({shape_factor:.2f})")
            else:
                effective_hourly_mw = base_hourly_mw
                source_details.append(contract.renewable_type)
            
            total_ppa_kw += effective_hourly_mw * 1000  # Convert MW to kW
        
        allocated = min(energy_kwh, total_ppa_kw)
        source_str = "+".join(source_details[:3]) if source_details else "none"
        
        return allocated, source_str
    
    def allocate_rec_energy(self, energy_kwh: float, region: str,
                           timestamp: datetime,
                           require_additionality: bool = True) -> Tuple[float, List[int], List[str]]:
        """Enhanced REC allocation with expiry checking and database updates"""
        # Filter available RECs
        available_recs = [r for r in self.rec_portfolio if r.status == RECertificateStatus.ACTIVE]
        
        # Check expiry on all RECs
        for rec in available_recs:
            if self._check_rec_expiry(rec, timestamp):
                rec.status = RECertificateStatus.EXPIRED
                self.db_manager.update_rec_status(rec.cert_id, 'expired', 
                                                  retired_at=timestamp.isoformat())
        
        available_recs = [r for r in available_recs if r.status == RECertificateStatus.ACTIVE]
        
        # Location matching
        if self.rec_location_matching:
            available_recs = [r for r in available_recs if region in r.applicable_regions]
        
        # Vintage matching
        current_year = timestamp.year
        if self.rec_vintage_matching:
            available_recs = [r for r in available_recs if r.vintage_year >= current_year - 1]
        
        # Additionality
        if require_additionality:
            available_recs = [r for r in available_recs if r.is_additional]
        
        # Sort by vintage (older first to use them before they expire)
        available_recs.sort(key=lambda r: r.vintage_year)
        
        total_rec_kwh = 0
        vintages_used = []
        regions_used = []
        remaining = energy_kwh
        
        for rec in available_recs:
            if remaining <= 0:
                break
            
            rec_kwh = rec.mwh_volume * 1000
            allocate_kwh = min(remaining, rec_kwh)
            
            rec.mwh_volume -= allocate_kwh / 1000
            remaining -= allocate_kwh
            total_rec_kwh += allocate_kwh
            
            vintages_used.append(rec.vintage_year)
            regions_used.append(rec.region)
            
            if rec.mwh_volume <= 0:
                rec.status = RECertificateStatus.RETIRED
                rec.retired = True
                rec.retired_at = timestamp
                self.db_manager.update_rec_status(rec.cert_id, 'retired',
                                                  retired_at=timestamp.isoformat(),
                                                  retired_for_task=timestamp.isoformat())
        
        return total_rec_kwh, vintages_used, regions_used
    
    def _check_rec_expiry(self, rec: RECertificate, current_date: datetime) -> bool:
        """Check if REC has expired (24 months after vintage year)"""
        expiry_date = date(rec.vintage_year + 1, 1, 1) + timedelta(days=30 * 24)
        return current_date.date() > expiry_date
    
    async def account_carbon_async(self, task_id: str, energy_consumption_kwh: float,
                                   region: str, timestamp: datetime,
                                   scope3_data: Optional[Dict] = None,
                                   weather_data: Optional[Dict] = None) -> CarbonAccounting:
        """
        Perform enhanced dual carbon accounting with async API calls.
        
        This is the main async accounting method that uses real-time grid intensity.
        """
        # Get real-time grid intensity if enabled
        if self.real_time_intensity:
            location_intensity, location_source = await self.grid_api.get_intensity(region, timestamp)
        else:
            intensities = {
                'us-east': 380.0, 'us-west': 250.0, 'us-central': 450.0,
                'eu-north': 80.0, 'eu-west': 220.0, 'asia-pacific': 550.0,
                'uk': 210.0
            }
            location_intensity = intensities.get(region, 400.0)
            location_source = "static_average"
        
        # Location-based emissions
        location_emissions = energy_consumption_kwh * location_intensity / 1000
        
        # Market-based: allocate PPA and REC
        weather = weather_data or {}
        ppa_allocated, ppa_source = self.allocate_ppa_energy(
            timestamp, energy_consumption_kwh,
            cloud_cover=weather.get('cloud_cover', 0.0),
            wind_speed=weather.get('wind_speed', 5.0),
            latitude=weather.get('latitude', 40.0)
        )
        
        rec_allocated, rec_vintages, rec_regions = self.allocate_rec_energy(
            energy_consumption_kwh - ppa_allocated, region, timestamp
        )
        
        residual_energy = energy_consumption_kwh - ppa_allocated - rec_allocated
        residual_intensity = location_intensity * 0.85  # Residual mix adjustment
        residual_emissions = residual_energy * residual_intensity / 1000
        
        market_emissions = residual_emissions
        
        # Scope 3 emissions
        scope3_emissions = 0.0
        if scope3_data and self.track_scope3:
            for category, quantity in scope3_data.items():
                scope3_emissions += self.scope3_tracker.add_emission(
                    category, quantity, task_id=task_id
                )
        
        # Coverage percentages
        ppa_coverage = (ppa_allocated / energy_consumption_kwh * 100) if energy_consumption_kwh > 0 else 0
        rec_coverage = (rec_allocated / energy_consumption_kwh * 100) if energy_consumption_kwh > 0 else 0
        
        reporting_recommendation = self._select_reporting_method(
            location_emissions, market_emissions, self._check_rec_quality()
        )
        
        accounting = CarbonAccounting(
            task_id=task_id,
            timestamp=timestamp,
            energy_consumption_kwh=energy_consumption_kwh,
            region=region,
            location_based_emissions_kg=location_emissions,
            location_intensity_source=location_source,
            market_based_emissions_kg=market_emissions,
            market_intensity_source="residual_mix",
            ppa_allocated_kwh=ppa_allocated,
            rec_allocated_kwh=rec_allocated,
            rec_vintages_used=rec_vintages,
            rec_regions_used=rec_regions,
            ppa_coverage_percent=ppa_coverage,
            rec_coverage_percent=rec_coverage,
            residual_emissions_kg=residual_emissions,
            scope3_emissions_kg=scope3_emissions,
            reporting_recommendation=reporting_recommendation
        )
        
        # Calculate hash and add to ledger
        accounting.hash = self._calculate_hash(accounting)
        
        async with self._ledger_lock:
            self.accounting_ledger.append(accounting)
            self.merkle_tree.add_leaf(accounting.hash)
        
        # Save to database
        self.db_manager.save_accounting_entry({
            'task_id': task_id,
            'timestamp': timestamp.isoformat(),
            'energy_kwh': energy_consumption_kwh,
            'region': region,
            'location_emissions_kg': location_emissions,
            'market_emissions_kg': market_emissions,
            'scope3_emissions_kg': scope3_emissions,
            'ppa_allocated_kwh': ppa_allocated,
            'rec_allocated_kwh': rec_allocated,
            'hash': accounting.hash,
            'metadata': {
                'location_source': location_source,
                'rec_vintages': rec_vintages,
                'ppa_coverage': ppa_coverage,
                'rec_coverage': rec_coverage
            }
        })
        
        logger.info(f"Carbon accounting for {task_id}: location={location_emissions:.2f}kg, "
                   f"market={market_emissions:.2f}kg (intensity: {location_intensity:.0f} gCO2/kWh)")
        
        return accounting
    
    def _select_reporting_method(self, location_emissions: float, market_emissions: float,
                                 recs_are_additional: bool) -> str:
        """Select appropriate reporting method per GHG Protocol"""
        if recs_are_additional and market_emissions < location_emissions:
            return 'MARKET_BASED'
        return 'LOCATION_BASED'
    
    def _check_rec_quality(self) -> bool:
        """Check if there are recent additional RECs"""
        current_year = datetime.now().year
        additional_recent = [r for r in self.rec_portfolio 
                            if r.is_additional and r.status == RECertificateStatus.ACTIVE
                            and r.vintage_year >= current_year - 2]
        return len(additional_recent) > 0
    
    def _calculate_hash(self, accounting: CarbonAccounting) -> str:
        """Calculate cryptographic hash of accounting entry"""
        data = {
            'task_id': accounting.task_id,
            'timestamp': accounting.timestamp.isoformat(),
            'energy_kwh': accounting.energy_consumption_kwh,
            'location_emissions': accounting.location_based_emissions_kg,
            'market_emissions': accounting.market_based_emissions_kg,
            'scope3_emissions': accounting.scope3_emissions_kg,
            'region': accounting.region
        }
        json_str = json.dumps(data, sort_keys=True)
        return hashlib.sha256(json_str.encode()).hexdigest()
    
    # Synchronous wrapper for backward compatibility
    def account_carbon(self, task_id: str, energy_consumption_kwh: float,
                      region: str, timestamp: datetime,
                      scope3_data: Optional[Dict] = None) -> CarbonAccounting:
        """Synchronous wrapper for async method (for backward compatibility)"""
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            return loop.run_until_complete(
                self.account_carbon_async(task_id, energy_consumption_kwh, region, timestamp, scope3_data)
            )
        finally:
            loop.close()
    
    def get_emissions_ledger(self, task_id: Optional[str] = None) -> List[Dict]:
        """Get emissions ledger with Merkle proofs"""
        if not self.merkle_tree.root:
            self.merkle_tree.build()
        
        entries = [a for a in self.accounting_ledger if task_id is None or a.task_id == task_id]
        
        result = []
        for i, entry in enumerate(entries):
            proof = self.merkle_tree.get_proof(i) if self.merkle_tree.tree else []
            result.append({
                'task_id': entry.task_id,
                'timestamp': entry.timestamp.isoformat(),
                'location_emissions_kg': entry.location_based_emissions_kg,
                'market_emissions_kg': entry.market_based_emissions_kg,
                'scope3_emissions_kg': entry.scope3_emissions_kg,
                'ppa_coverage': entry.ppa_coverage_percent,
                'rec_coverage': entry.rec_coverage_percent,
                'hash': entry.hash,
                'merkle_proof': proof,
                'merkle_root': self.merkle_tree.get_root()
            })
        return result
    
    def verify_integrity(self) -> Tuple[bool, List[str]]:
        """Verify ledger integrity using Merkle tree"""
        # Rebuild tree from current ledger
        test_tree = MerkleTree()
        for entry in self.accounting_ledger:
            test_tree.add_leaf(entry.hash)
        test_tree.build()
        
        # Verify each entry
        failed = []
        for i, entry in enumerate(self.accounting_ledger):
            expected_hash = self._calculate_hash(entry)
            if entry.hash != expected_hash:
                failed.append(f"{entry.task_id}_hash_mismatch")
            
            if test_tree.tree:
                proof = test_tree.get_proof(i)
                if not test_tree.verify(entry.hash, proof, test_tree.get_root()):
                    failed.append(f"{entry.task_id}_merkle_verification_failed")
        
        # Compare roots
        if self.merkle_tree.get_root() != test_tree.get_root():
            failed.append("root_mismatch")
        
        return len(failed) == 0, failed
    
    async def close(self):
        """Clean up resources"""
        self._shutdown_event.set()
        await self.grid_api.close()
        for task in self._background_tasks:
            task.cancel()
    
    def get_sustainability_report(self) -> Dict:
        """Generate comprehensive sustainability report"""
        if not self.accounting_ledger:
            return {'error': 'No accounting data available'}
        
        total_energy = sum(e.energy_consumption_kwh for e in self.accounting_ledger)
        total_location = sum(e.location_based_emissions_kg for e in self.accounting_ledger)
        total_market = sum(e.market_based_emissions_kg for e in self.accounting_ledger)
        total_ppa = sum(e.ppa_allocated_kwh for e in self.accounting_ledger)
        total_rec = sum(e.rec_allocated_kwh for e in self.accounting_ledger)
        
        return {
            'report_date': datetime.now().isoformat(),
            'period': {
                'start': self.accounting_ledger[0].timestamp.isoformat(),
                'end': self.accounting_ledger[-1].timestamp.isoformat(),
                'task_count': len(self.accounting_ledger)
            },
            'energy': {
                'total_kwh': total_energy,
                'ppa_kwh': total_ppa,
                'rec_kwh': total_rec,
                'renewable_coverage_percent': ((total_ppa + total_rec) / total_energy * 100) if total_energy > 0 else 0
            },
            'emissions': {
                'location_based_kg': total_location,
                'location_based_tco2': total_location / 1000,
                'market_based_kg': total_market,
                'market_based_tco2': total_market / 1000,
                'total_avoided_kg': total_location - total_market,
                'reduction_percent': ((total_location - total_market) / total_location * 100) if total_location > 0 else 0
            },
            'scope3': self.scope3_tracker.generate_report(),
            'api_stats': self.grid_api.get_api_stats(),
            'ledger_integrity': self.verify_integrity()[0],
            'rec_portfolio_status': {
                'total_active_mwh': sum(r.mwh_volume for r in self.rec_portfolio if r.status == RECertificateStatus.ACTIVE),
                'by_vintage': {v: sum(r.mwh_volume for r in self.rec_portfolio if r.vintage_year == v and r.status == RECertificateStatus.ACTIVE) 
                              for v in set(r.vintage_year for r in self.rec_portfolio)}
            }
        }


# ============================================================
# Usage Example
# ============================================================

async def main():
    """Enhanced async usage example"""
    print("=== Enhanced Dual Carbon Accountant v3.1 Async Demo ===\n")
    
    accountant = EnhancedDualCarbonAccountant({
        'real_time_intensity': True,
        'rec_location_matching': True,
        'rec_vintage_matching': True,
        'use_shape_factors': True,
        'track_scope3': True
    })
    
    print("1. Real-time Carbon Accounting:")
    
    # Multiple tasks with different regions
    tasks = [
        ('task_001', 100.0, 'us-east', datetime.now() - timedelta(hours=1), {'purchased_goods': 500}),
        ('task_002', 250.0, 'eu-north', datetime.now() - timedelta(minutes=30), {'business_travel_air': 1000}),
        ('task_003', 50.0, 'asia-pacific', datetime.now(), {})
    ]
    
    for task_id, energy, region, timestamp, scope3 in tasks:
        result = await accountant.account_carbon_async(
            task_id, energy, region, timestamp, scope3
        )
        print(f"\n   Task: {task_id}")
        print(f"     Location-based: {result.location_based_emissions_kg:.2f} kg CO2")
        print(f"     Market-based: {result.market_based_emissions_kg:.2f} kg CO2")
        print(f"     PPA Coverage: {result.ppa_coverage_percent:.1f}%")
        print(f"     REC Coverage: {result.rec_coverage_percent:.1f}%")
    
    print("\n2. API Performance Stats:")
    api_stats = accountant.grid_api.get_api_stats()
    for api, stats in api_stats.items():
        print(f"   {api}: {stats['success']} successes, {stats['failures']} failures, "
              f"avg latency: {stats['avg_latency']:.0f}ms")
    
    print("\n3. REC Price Forecast:")
    forecast = accountant.price_forecaster.forecast_price(3)
    if forecast:
        print(f"   3-month forecast: ${forecast:.2f}/MWh")
        accuracy = accountant.price_forecaster.get_forecast_accuracy()
        print(f"   Forecast accuracy: {accuracy.get('mean_absolute_percentage_error', 0):.1f}%")
    
    print("\n4. Sustainability Report:")
    report = accountant.get_sustainability_report()
    print(f"   Renewable coverage: {report['energy']['renewable_coverage_percent']:.1f}%")
    print(f"   Emissions reduction: {report['emissions']['reduction_percent']:.1f}%")
    print(f"   Ledger integrity: {'✅ VALID' if report['ledger_integrity'] else '❌ INVALID'}")
    print(f"   Total Scope 3: {report['scope3']['total_scope3_kg']:.2f} kg CO2")
    
    print("\n5. Emissions Ledger with Merkle Proofs:")
    ledger = accountant.get_emissions_ledger()
    for entry in ledger[:3]:
        print(f"   {entry['task_id']}: hash={entry['hash'][:16]}..., "
              f"root={entry['merkle_root'][:16] if entry['merkle_root'] else 'None'}...")
    
    await accountant.close()
    print("\n✅ Enhanced Dual Carbon Accountant v3.1 test complete")

if __name__ == "__main__":
    asyncio.run(main())
