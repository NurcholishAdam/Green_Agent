# src/enhancements/synthetic_data_manager.py

"""
Complete synthetic data management for all enhancement modules
Provides realistic simulation of all external dependencies
"""

import numpy as np
import random
import threading
import time
import json
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple, Callable, Any
from enum import Enum
import logging

logger = logging.getLogger(__name__)


class DataQuality(Enum):
    """Simulated data quality levels for testing fallbacks"""
    PERFECT = "perfect"      # No noise, 100% available
    NOISY = "noisy"          # ±10% random noise
    DEGRADED = "degraded"    # ±30% noise, 20% missing values
    OFFLINE = "offline"      # Complete data source failure
    RECOVERING = "recovering" # Returning from offline (50% availability)


class ScenarioType(Enum):
    """Predefined test scenarios"""
    NORMAL = "normal"
    HEATWAVE = "heatwave"
    HIGH_CARBON = "high_carbon"
    HELIUM_CRISIS = "helium_crisis"
    RECOVERY_SUCCESS = "recovery_success"
    ALL_DEGRADED = "all_degraded"
    NETWORK_PARTITION = "network_partition"
    POWER_OUTAGE = "power_outage"


@dataclass
class SyntheticTemperatureData:
    """Complete thermal data from hardware sensors"""
    cpu_temp_c: float
    gpu_temp_c: float
    memory_temp_c: float
    ambient_temp_c: float
    cooling_power_w: float
    fan_speed_percent: float
    power_draw_w: float
    timestamp: datetime
    quality: DataQuality
    source: str = "synthetic"
    
    def to_dict(self) -> Dict:
        return {
            'cpu_temp_c': self.cpu_temp_c,
            'gpu_temp_c': self.gpu_temp_c,
            'ambient_temp_c': self.ambient_temp_c,
            'cooling_power_w': self.cooling_power_w,
            'timestamp': self.timestamp.isoformat()
        }


@dataclass
class SyntheticGridData:
    """Complete electricity grid data by region"""
    region: str
    average_intensity_gco2_per_kwh: float
    marginal_intensity_gco2_per_kwh: float
    total_demand_mw: float
    renewable_percentage: float
    coal_percentage: float
    gas_percentage: float
    nuclear_percentage: float
    forecast_next_6h: List[float]
    timestamp: datetime
    quality: DataQuality
    source: str = "synthetic"
    
    def to_dict(self) -> Dict:
        return {
            'region': self.region,
            'average_intensity': self.average_intensity_gco2_per_kwh,
            'marginal_intensity': self.marginal_intensity_gco2_per_kwh,
            'renewable_percentage': self.renewable_percentage,
            'timestamp': self.timestamp.isoformat()
        }


@dataclass
class SyntheticHeliumData:
    """Complete helium market data"""
    spot_price_usd_per_liter: float
    futures_price_1m: float
    futures_price_3m: float
    futures_price_6m: float
    global_inventory_days: int
    supply_disruption_risk: float
    demand_growth_rate: float  # y/y percentage
    primary_producers: Dict[str, float]  # supplier -> capacity
    timestamp: datetime
    quality: DataQuality
    source: str = "synthetic"
    
    def to_dict(self) -> Dict:
        return {
            'spot_price_usd': self.spot_price_usd_per_liter,
            'inventory_days': self.global_inventory_days,
            'disruption_risk': self.supply_disruption_risk,
            'timestamp': self.timestamp.isoformat()
        }


@dataclass
class SyntheticRecoveryData:
    """Complete helium recovery system data"""
    recovery_efficiency: float
    liters_recovered_ytd: float
    liters_recovered_current: float
    recovery_method: str  # capture, recycle, purification, liquefaction
    energy_cost_kwh_per_liter: float
    capex_usd: float
    opex_usd_per_year: float
    uptime_percentage: float
    timestamp: datetime
    quality: DataQuality
    source: str = "synthetic"


@dataclass
class SyntheticPPAData:
    """Power Purchase Agreement data"""
    contract_id: str
    renewable_type: str
    capacity_mw: float
    hourly_allocation_mwh: Dict[int, float]
    start_date: datetime
    end_date: datetime
    price_usd_per_mwh: float
    additionality_verified: bool
    timestamp: datetime
    quality: DataQuality


class SyntheticDataSource:
    """
    Complete synthetic data source for all enhancement modules.
    Provides realistic, time-varying data with configurable quality and scenarios.
    """
    
    def __init__(self, config: Optional[Dict] = None):
        self.config = config or {}
        self.seed = self.config.get('seed', 42)
        self.quality = DataQuality(self.config.get('quality', 'perfect'))
        self.current_scenario = ScenarioType.NORMAL
        self.update_interval_seconds = self.config.get('update_interval', 5)
        self.regions = self.config.get('regions', ['us-east', 'us-west', 'eu-north', 'asia-pacific'])
        
        # Internal state
        self._temperature_state = {}
        self._grid_state = {}
        self._helium_state = {}
        self._recovery_state = {}
        self._ppa_state = {}
        
        # Event history for trend analysis
        self._history: Dict[str, List] = {
            'temperature': [],
            'grid': [],
            'helium': [],
            'recovery': []
        }
        
        # Background thread
        self._running = False
        self._thread = None
        self._subscribers: Dict[str, List[Callable]] = {}
        
        # Set random seed
        np.random.seed(self.seed)
        random.seed(self.seed)
        
        # Initialize state
        self._init_state()
    
    def _init_state(self):
        """Initialize all state with realistic starting values"""
        
        # Temperature state (per device)
        self._temperature_state = {
            'cpu_temp': 55.0,
            'gpu_temp': 65.0,
            'memory_temp': 50.0,
            'ambient': 22.0,
            'cooling_power': 100.0,
            'fan_speed': 40.0,
            'power_draw': 250.0,
            'thermal_mass': 500.0,
            'cooling_capacity': 500.0
        }
        
        # Grid state (per region)
        self._grid_state = {}
        for region in self.regions:
            if region == 'us-east':
                base = {'average': 380, 'marginal': 350, 'demand': 50000, 'renewable': 0.25,
                       'coal': 0.40, 'gas': 0.30, 'nuclear': 0.05}
            elif region == 'us-west':
                base = {'average': 250, 'marginal': 220, 'demand': 40000, 'renewable': 0.45,
                       'coal': 0.20, 'gas': 0.25, 'nuclear': 0.10}
            elif region == 'eu-north':
                base = {'average': 80, 'marginal': 70, 'demand': 30000, 'renewable': 0.65,
                       'coal': 0.05, 'gas': 0.15, 'nuclear': 0.15}
            else:  # asia-pacific
                base = {'average': 550, 'marginal': 520, 'demand': 60000, 'renewable': 0.15,
                       'coal': 0.60, 'gas': 0.20, 'nuclear': 0.05}
            self._grid_state[region] = base
        
        # Helium state
        self._helium_state = {
            'spot_price': 4.50,
            'futures_1m': 4.80,
            'futures_3m': 5.20,
            'futures_6m': 5.80,
            'inventory': 30,
            'risk': 0.15,
            'demand_growth': 0.05,
            'producers': {'AirLiquide': 0.40, 'Linde': 0.35, 'AirProducts': 0.25}
        }
        
        # Recovery state
        self._recovery_state = {
            'efficiency': 0.75,
            'recovered_ytd': 0.0,
            'recovered_current': 0.0,
            'method': 'capture',
            'energy_cost': 0.5,
            'capex': 500000,
            'opex': 50000,
            'uptime': 0.99
        }
        
        # PPA state
        self._ppa_state = {
            'contracts': [
                {
                    'id': 'PPA-001',
                    'type': 'solar',
                    'capacity': 50.0,
                    'hourly': {h: 50.0/24 for h in range(24)},
                    'start': datetime.now(),
                    'end': datetime.now() + timedelta(days=365*10),
                    'price': 45.0,
                    'additional': True
                },
                {
                    'id': 'PPA-002',
                    'type': 'wind',
                    'capacity': 30.0,
                    'hourly': {h: 30.0/24 for h in range(24)},
                    'start': datetime.now(),
                    'end': datetime.now() + timedelta(days=365*8),
                    'price': 35.0,
                    'additional': True
                }
            ]
        }
    
    def start(self):
        """Start background data generation"""
        if self._running:
            return
        
        self._running = True
        self._thread = threading.Thread(target=self._update_loop, daemon=True)
        self._thread.start()
        logger.info("Synthetic data source started")
    
    def stop(self):
        """Stop background data generation"""
        self._running = False
        if self._thread:
            self._thread.join(timeout=5)
        logger.info("Synthetic data source stopped")
    
    def subscribe(self, data_type: str, callback: Callable):
        """Subscribe to data updates"""
        if data_type not in self._subscribers:
            self._subscribers[data_type] = []
        self._subscribers[data_type].append(callback)
    
    def _update_loop(self):
        """Main update loop for synthetic data generation"""
        while self._running:
            try:
                self._update_all()
                self._notify_subscribers()
                time.sleep(self.update_interval_seconds)
            except Exception as e:
                logger.error(f"Update error: {e}")
    
    def _update_all(self):
        """Update all data sources"""
        self._update_temperature()
        self._update_grid()
        self._update_helium()
        self._update_recovery()
    
    def _update_temperature(self):
        """Update thermal data with realistic dynamics"""
        dt = self.update_interval_seconds / 60.0  # minutes
        tau = 20.0  # thermal time constant
        
        # Apply scenario effects
        if self.current_scenario == ScenarioType.HEATWAVE:
            ambient_offset = 10
            workload_multiplier = 1.3
        elif self.current_scenario == ScenarioType.POWER_OUTAGE:
            ambient_offset = 5
            workload_multiplier = 0.5
        else:
            ambient_offset = 0
            workload_multiplier = 1.0
        
        # Workload simulation (daily pattern + random)
        hour = datetime.now().hour
        daily_factor = 0.5 + 0.5 * np.sin(np.pi * (hour - 12) / 12)
        workload = daily_factor * workload_multiplier + np.random.normal(0, 0.1)
        
        # Temperature dynamics (Newton's law of cooling)
        target_temp = 45 + workload * 60 + ambient_offset
        self._temperature_state['cpu_temp'] += (target_temp - self._temperature_state['cpu_temp']) * dt / tau
        self._temperature_state['gpu_temp'] += (target_temp * 1.2 - self._temperature_state['gpu_temp']) * dt / tau
        self._temperature_state['memory_temp'] += (target_temp * 0.9 - self._temperature_state['memory_temp']) * dt / tau
        
        # Add noise based on quality
        if self.quality == DataQuality.NOISY:
            for key in ['cpu_temp', 'gpu_temp']:
                self._temperature_state[key] += np.random.normal(0, 1)
        elif self.quality == DataQuality.DEGRADED:
            for key in ['cpu_temp', 'gpu_temp']:
                self._temperature_state[key] += np.random.normal(0, 3)
        
        # Cooling system responds to temperature
        temp_above_ambient = max(0, self._temperature_state['gpu_temp'] - self._temperature_state['ambient'])
        self._temperature_state['cooling_power'] = min(500, temp_above_ambient * 10 + np.random.normal(0, 10))
        self._temperature_state['fan_speed'] = min(100, temp_above_ambient * 2)
        
        # Store history
        self._history['temperature'].append({
            'timestamp': datetime.now(),
            'gpu_temp': self._temperature_state['gpu_temp']
        })
        if len(self._history['temperature']) > 1000:
            self._history['temperature'] = self._history['temperature'][-1000:]
    
    def _update_grid(self):
        """Update grid data with daily and weekly patterns"""
        now = datetime.now()
        hour = now.hour
        day_of_week = now.weekday()
        is_weekday = day_of_week < 5
        
        for region, state in self._grid_state.items():
            # Demand pattern: peak at 9-10am and 6-7pm
            morning_peak = 1.3 if 9 <= hour <= 11 else 1.0
            evening_peak = 1.4 if 17 <= hour <= 19 else 1.0
            night_low = 0.6 if 0 <= hour <= 5 else 1.0
            weekday_factor = 1.2 if is_weekday else 0.8
            
            demand_factor = max(night_low, morning_peak, evening_peak) * weekday_factor
            
            # Apply scenario effects
            if self.current_scenario == ScenarioType.HIGH_CARBON:
                carbon_factor = 1.5
                renewable_factor = 0.5
            else:
                carbon_factor = 1.0
                renewable_factor = 1.0
            
            # Update state with dynamics
            target_demand = 50000 * demand_factor
            state['demand'] = state['demand'] * 0.9 + target_demand * 0.1 + np.random.normal(0, 500)
            state['average'] = max(10, min(1000, state['average'] * carbon_factor + np.random.normal(0, 5)))
            state['marginal'] = state['average'] * (0.8 + 0.4 * np.random.random())
            state['renewable'] = max(0.05, min(0.95, state['renewable'] * renewable_factor + np.random.normal(0, 0.02)))
            
            # 6-hour forecast based on current trend
            forecast = []
            for h in range(6):
                future_hour = (hour + h) % 24
                future_factor = 1.2 if 9 <= future_hour <= 11 or 17 <= future_hour <= 19 else 0.8
                forecast.append(state['average'] * future_factor * carbon_factor)
            state['forecast'] = forecast
    
    def _update_helium(self):
        """Update helium market data with realistic dynamics"""
        # Mean-reverting price with occasional spikes
        reversion = 0.05 * (4.5 - self._helium_state['spot_price'])
        random_walk = np.random.normal(0, 0.1)
        
        # Apply scenario effects
        if self.current_scenario == ScenarioType.HELIUM_CRISIS:
            crisis_factor = 2.0
            inventory_delta = -1.0
        else:
            crisis_factor = 1.0
            inventory_delta = 0.3
        
        price_change = reversion + random_walk
        self._helium_state['spot_price'] = max(2.0, min(15.0, 
            self._helium_state['spot_price'] * crisis_factor + price_change))
        
        # Inventory dynamics
        self._helium_state['inventory'] = max(5, min(60,
            self._helium_state['inventory'] + inventory_delta + np.random.normal(0, 0.5)))
        
        # Risk is inverse of inventory
        self._helium_state['risk'] = max(0.05, min(0.8, 
            1.0 - self._helium_state['inventory'] / 60.0))
        
        # Update futures
        self._helium_state['futures_1m'] = self._helium_state['spot_price'] * 1.05
        self._helium_state['futures_3m'] = self._helium_state['spot_price'] * 1.12
        self._helium_state['futures_6m'] = self._helium_state['spot_price'] * 1.20
        
        # Demand growth (slowly increasing)
        self._helium_state['demand_growth'] = max(0.02, min(0.10,
            self._helium_state['demand_growth'] + np.random.normal(0, 0.001)))
    
    def _update_recovery(self):
        """Update recovery system data"""
        # Efficiency degrades slightly over time, then resets on maintenance
        if self.current_scenario == ScenarioType.RECOVERY_SUCCESS:
            efficiency_target = 0.95
        else:
            efficiency_target = 0.75
        
        self._recovery_state['efficiency'] = (self._recovery_state['efficiency'] * 0.99 + 
                                               efficiency_target * 0.01 + np.random.normal(0, 0.01))
        self._recovery_state['efficiency'] = min(0.96, max(0.60, self._recovery_state['efficiency']))
        
        # Accumulate recovered helium
        recovery_rate = 0.1  # liters per second at full efficiency
        self._recovery_state['recovered_current'] += recovery_rate * self.update_interval_seconds * self._recovery_state['efficiency']
        self._recovery_state['recovered_ytd'] += recovery_rate * self.update_interval_seconds * self._recovery_state['efficiency']
    
    def _notify_subscribers(self):
        """Notify all subscribers of new data"""
        for data_type, callbacks in self._subscribers.items():
            for callback in callbacks:
                try:
                    if data_type == 'temperature':
                        callback(self.get_temperature_data())
                    elif data_type == 'grid':
                        callback(self.get_grid_data())
                    elif data_type == 'helium':
                        callback(self.get_helium_data())
                    elif data_type == 'recovery':
                        callback(self.get_recovery_data())
                except Exception as e:
                    logger.error(f"Subscriber notification failed: {e}")
    
    def get_temperature_data(self, device: str = 'gpu') -> SyntheticTemperatureData:
        """Get current temperature data with quality handling"""
        if self.quality == DataQuality.OFFLINE:
            raise ConnectionError("Temperature data source offline")
        
        # Apply degraded quality degradation
        quality = self.quality
        if quality == DataQuality.DEGRADED and random.random() < 0.2:
            # 20% data loss in degraded mode
            raise TimeoutError("Temperature data timeout (degraded mode)")
        
        # Build response
        cpu_temp = self._temperature_state['cpu_temp']
        gpu_temp = self._temperature_state['gpu_temp']
        
        if quality == DataQuality.NOISY:
            cpu_temp += np.random.normal(0, 1)
            gpu_temp += np.random.normal(0, 1.5)
        elif quality == DataQuality.DEGRADED:
            cpu_temp += np.random.normal(0, 3)
            gpu_temp += np.random.normal(0, 4)
        
        return SyntheticTemperatureData(
            cpu_temp_c=cpu_temp,
            gpu_temp_c=gpu_temp,
            memory_temp_c=self._temperature_state['memory_temp'],
            ambient_temp_c=self._temperature_state['ambient'],
            cooling_power_w=self._temperature_state['cooling_power'],
            fan_speed_percent=self._temperature_state['fan_speed'],
            power_draw_w=self._temperature_state['power_draw'],
            timestamp=datetime.now(),
            quality=quality,
            source="synthetic"
        )
    
    def get_grid_data(self, region: str = 'us-east') -> SyntheticGridData:
        """Get current grid data with quality handling"""
        if self.quality == DataQuality.OFFLINE:
            raise ConnectionError(f"Grid data offline for {region}")
        
        state = self._grid_state.get(region, self._grid_state['us-east'])
        
        return SyntheticGridData(
            region=region,
            average_intensity_gco2_per_kwh=state['average'],
            marginal_intensity_gco2_per_kwh=state['marginal'],
            total_demand_mw=state['demand'],
            renewable_percentage=state['renewable'],
            coal_percentage=state['coal'],
            gas_percentage=state['gas'],
            nuclear_percentage=state['nuclear'],
            forecast_next_6h=state.get('forecast', []),
            timestamp=datetime.now(),
            quality=self.quality,
            source="synthetic"
        )
    
    def get_helium_data(self) -> SyntheticHeliumData:
        """Get current helium market data"""
        if self.quality == DataQuality.OFFLINE:
            raise ConnectionError("Helium market data offline")
        
        return SyntheticHeliumData(
            spot_price_usd_per_liter=self._helium_state['spot_price'],
            futures_price_1m=self._helium_state['futures_1m'],
            futures_price_3m=self._helium_state['futures_3m'],
            futures_price_6m=self._helium_state['futures_6m'],
            global_inventory_days=int(self._helium_state['inventory']),
            supply_disruption_risk=self._helium_state['risk'],
            demand_growth_rate=self._helium_state['demand_growth'],
            primary_producers=self._helium_state['producers'],
            timestamp=datetime.now(),
            quality=self.quality,
            source="synthetic"
        )
    
    def get_recovery_data(self) -> SyntheticRecoveryData:
        """Get current recovery system data"""
        return SyntheticRecoveryData(
            recovery_efficiency=self._recovery_state['efficiency'],
            liters_recovered_ytd=self._recovery_state['recovered_ytd'],
            liters_recovered_current=self._recovery_state['recovered_current'],
            recovery_method=self._recovery_state['method'],
            energy_cost_kwh_per_liter=self._recovery_state['energy_cost'],
            capex_usd=self._recovery_state['capex'],
            opex_usd_per_year=self._recovery_state['opex'],
            uptime_percentage=self._recovery_state['uptime'],
            timestamp=datetime.now(),
            quality=self.quality,
            source="synthetic"
        )
    
    def get_ppa_data(self) -> List[SyntheticPPAData]:
        """Get PPA contract data"""
        ppa_list = []
        for contract in self._ppa_state['contracts']:
            ppa_list.append(SyntheticPPAData(
                contract_id=contract['id'],
                renewable_type=contract['type'],
                capacity_mw=contract['capacity'],
                hourly_allocation_mwh=contract['hourly'],
                start_date=contract['start'],
                end_date=contract['end'],
                price_usd_per_mwh=contract['price'],
                additionality_verified=contract['additional'],
                timestamp=datetime.now(),
                quality=self.quality
            ))
        return ppa_list
    
    def set_quality(self, quality: DataQuality):
        """Manually set data quality for testing"""
        self.quality = quality
        logger.info(f"Data quality set to {quality.value}")
    
    def set_scenario(self, scenario: ScenarioType):
        """Set test scenario"""
        self.current_scenario = scenario
        logger.info(f"Scenario set to {scenario.value}")
        
        # Immediate scenario effects
        if scenario == ScenarioType.HEATWAVE:
            self._temperature_state['ambient'] = 35
        elif scenario == ScenarioType.HELIUM_CRISIS:
            self._helium_state['spot_price'] = 12.0
            self._helium_state['inventory'] = 8
        elif scenario == ScenarioType.RECOVERY_SUCCESS:
            self._recovery_state['efficiency'] = 0.95
    
    def get_scenario_metrics(self) -> Dict:
        """Get current scenario metrics"""
        return {
            'scenario': self.current_scenario.value,
            'quality': self.quality.value,
            'temperature': {
                'gpu': self._temperature_state['gpu_temp'],
                'cpu': self._temperature_state['cpu_temp']
            },
            'helium': {
                'price': self._helium_state['spot_price'],
                'inventory': self._helium_state['inventory']
            },
            'grid': {
                'us_east_average': self._grid_state['us-east']['average'],
                'renewable_percent': self._grid_state['us-east']['renewable'] * 100
            }
        }
    
    def generate_report(self) -> str:
        """Generate a report of current synthetic data state"""
        return json.dumps(self.get_scenario_metrics(), indent=2)
