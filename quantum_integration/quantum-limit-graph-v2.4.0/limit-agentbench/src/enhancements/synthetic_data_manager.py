# src/enhancements/synthetic_data_manager.py

"""
Enhanced Synthetic Data Management for Green Agent - Version 2.0

Features:
1. Comprehensive simulation of all external dependencies
2. 5 data quality levels for fallback testing (PERFECT, NOISY, DEGRADED, OFFLINE, RECOVERING)
3. 8 test scenarios (HEATWAVE, HIGH_CARBON, HELIUM_CRISIS, etc.)
4. Realistic physics-based dynamics (Newtonian cooling, mean reversion)
5. State persistence (save/load to disk)
6. Historical replay for regression testing
7. Cross-stream correlation between temperature, grid, and helium
8. Custom scenario API for user-defined scenarios
9. Network latency simulation
10. Batch history export for analysis
11. Subscriber/publisher pattern with backpressure handling
12. Comprehensive metrics and reporting

Reference: "Synthetic Data for Sustainable AI Testing" (ACM SIGENERGY, 2024)
"""

import numpy as np
import random
import threading
import time
import json
import pickle
import hashlib
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple, Callable, Any, Union
from enum import Enum
from collections import deque
import logging
import os

logger = logging.getLogger(__name__)


# ============================================================
# ENHANCEMENT 1: Enhanced Enums with Metadata
# ============================================================

class DataQuality(Enum):
    """Simulated data quality levels for testing fallbacks"""
    PERFECT = "perfect"      # No noise, 100% available
    NOISY = "noisy"          # ±10% random noise
    DEGRADED = "degraded"    # ±30% noise, 20% missing values
    OFFLINE = "offline"      # Complete data source failure
    RECOVERING = "recovering" # Returning from offline (50% availability)
    
    @property
    def noise_scale(self) -> float:
        """Get noise scaling factor for this quality level"""
        return {
            DataQuality.PERFECT: 0.0,
            DataQuality.NOISY: 0.1,
            DataQuality.DEGRADED: 0.3,
            DataQuality.RECOVERING: 0.15,
            DataQuality.OFFLINE: 0.0
        }[self]
    
    @property
    def availability(self) -> float:
        """Get data availability rate (0-1)"""
        return {
            DataQuality.PERFECT: 1.0,
            DataQuality.NOISY: 1.0,
            DataQuality.DEGRADED: 0.8,
            DataQuality.RECOVERING: 0.5,
            DataQuality.OFFLINE: 0.0
        }[self]


class ScenarioType(Enum):
    """Predefined test scenarios with effects"""
    NORMAL = "normal"
    HEATWAVE = "heatwave"
    HIGH_CARBON = "high_carbon"
    HELIUM_CRISIS = "helium_crisis"
    RECOVERY_SUCCESS = "recovery_success"
    ALL_DEGRADED = "all_degraded"
    NETWORK_PARTITION = "network_partition"
    POWER_OUTAGE = "power_outage"
    EXTREME_WEATHER = "extreme_weather"
    MARKET_CRASH = "market_crash"
    
    @property
    def temperature_offset(self) -> float:
        """Temperature offset for this scenario (°C)"""
        offsets = {
            ScenarioType.HEATWAVE: 10,
            ScenarioType.POWER_OUTAGE: 5,
            ScenarioType.EXTREME_WEATHER: 8,
            ScenarioType.NORMAL: 0
        }
        return offsets.get(self, 0)
    
    @property
    def carbon_multiplier(self) -> float:
        """Carbon intensity multiplier"""
        multipliers = {
            ScenarioType.HIGH_CARBON: 1.5,
            ScenarioType.NORMAL: 1.0
        }
        return multipliers.get(self, 1.0)
    
    @property
    def helium_multiplier(self) -> float:
        """Helium price multiplier"""
        multipliers = {
            ScenarioType.HELIUM_CRISIS: 2.0,
            ScenarioType.MARKET_CRASH: 0.5,
            ScenarioType.NORMAL: 1.0
        }
        return multipliers.get(self, 1.0)


# ============================================================
# ENHANCEMENT 2: Enhanced Data Classes with Versioning
# ============================================================

@dataclass
class SyntheticTemperatureData:
    """Enhanced thermal data from hardware sensors"""
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
    version: int = 2
    confidence: float = 1.0
    latency_ms: float = 0.0
    
    def to_dict(self) -> Dict:
        return {
            'cpu_temp_c': self.cpu_temp_c,
            'gpu_temp_c': self.gpu_temp_c,
            'ambient_temp_c': self.ambient_temp_c,
            'cooling_power_w': self.cooling_power_w,
            'timestamp': self.timestamp.isoformat(),
            'quality': self.quality.value,
            'confidence': self.confidence
        }


@dataclass
class SyntheticGridData:
    """Enhanced electricity grid data by region"""
    region: str
    average_intensity_gco2_per_kwh: float
    marginal_intensity_gco2_per_kwh: float
    total_demand_mw: float
    renewable_percentage: float
    coal_percentage: float
    gas_percentage: float
    nuclear_percentage: float
    carbon_price_usd: float
    forecast_next_6h: List[float]
    timestamp: datetime
    quality: DataQuality
    source: str = "synthetic"
    version: int = 2
    
    def to_dict(self) -> Dict:
        return {
            'region': self.region,
            'average_intensity': self.average_intensity_gco2_per_kwh,
            'marginal_intensity': self.marginal_intensity_gco2_per_kwh,
            'renewable_percentage': self.renewable_percentage,
            'carbon_price_usd': self.carbon_price_usd,
            'timestamp': self.timestamp.isoformat()
        }


@dataclass
class SyntheticHeliumData:
    """Enhanced helium market data"""
    spot_price_usd_per_liter: float
    futures_price_1m: float
    futures_price_3m: float
    futures_price_6m: float
    global_inventory_days: int
    supply_disruption_risk: float
    demand_growth_rate: float
    primary_producers: Dict[str, float]
    timestamp: datetime
    quality: DataQuality
    source: str = "synthetic"
    version: int = 2
    
    def to_dict(self) -> Dict:
        return {
            'spot_price_usd': self.spot_price_usd_per_liter,
            'inventory_days': self.global_inventory_days,
            'disruption_risk': self.supply_disruption_risk,
            'timestamp': self.timestamp.isoformat()
        }


@dataclass
class SyntheticRecoveryData:
    """Enhanced helium recovery system data"""
    recovery_efficiency: float
    liters_recovered_ytd: float
    liters_recovered_current: float
    recovery_method: str
    energy_cost_kwh_per_liter: float
    capex_usd: float
    opex_usd_per_year: float
    uptime_percentage: float
    maintenance_schedule: List[str]
    timestamp: datetime
    quality: DataQuality
    source: str = "synthetic"
    version: int = 2


@dataclass
class SyntheticPPAData:
    """Enhanced Power Purchase Agreement data"""
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
    version: int = 2


# ============================================================
# ENHANCEMENT 3: State Persistence Manager
# ============================================================

class StatePersistence:
    """Handle saving and loading of synthetic data state"""
    
    def __init__(self, data_source: 'SyntheticDataSource'):
        self.data_source = data_source
        self.save_dir = "synthetic_states"
        self._ensure_directory()
    
    def _ensure_directory(self):
        """Ensure save directory exists"""
        try:
            os.makedirs(self.save_dir, exist_ok=True)
        except Exception as e:
            logger.warning(f"Failed to create directory: {e}")
    
    def save_state(self, name: str = "latest") -> str:
        """Save current state to disk"""
        state = {
            'temperature_state': self.data_source._temperature_state.copy(),
            'grid_state': self.data_source._grid_state.copy(),
            'helium_state': self.data_source._helium_state.copy(),
            'recovery_state': self.data_source._recovery_state.copy(),
            'ppa_state': self.data_source._ppa_state.copy(),
            'metadata': {
                'timestamp': time.time(),
                'datetime': datetime.now().isoformat(),
                'scenario': self.data_source.current_scenario.value,
                'quality': self.data_source.quality.value,
                'update_interval': self.data_source.update_interval_seconds,
                'version': 2
            }
        }
        
        filepath = os.path.join(self.save_dir, f"{name}.json")
        with open(filepath, 'w') as f:
            json.dump(state, f, indent=2, default=str)
        
        # Also save checksum for integrity
        checksum = hashlib.sha256(json.dumps(state, sort_keys=True, default=str).encode()).hexdigest()
        checksum_path = os.path.join(self.save_dir, f"{name}.checksum")
        with open(checksum_path, 'w') as f:
            f.write(checksum)
        
        logger.info(f"State saved to {filepath}")
        return filepath
    
    def load_state(self, name: str = "latest") -> bool:
        """Load state from disk"""
        filepath = os.path.join(self.save_dir, f"{name}.json")
        checksum_path = os.path.join(self.save_dir, f"{name}.checksum")
        
        if not os.path.exists(filepath):
            logger.warning(f"State file not found: {filepath}")
            return False
        
        with open(filepath, 'r') as f:
            state = json.load(f)
        
        # Verify checksum if available
        if os.path.exists(checksum_path):
            with open(checksum_path, 'r') as f:
                expected_checksum = f.read()
            actual_checksum = hashlib.sha256(json.dumps(state, sort_keys=True, default=str).encode()).hexdigest()
            if actual_checksum != expected_checksum:
                logger.error("Checksum mismatch! State may be corrupted.")
                return False
        
        # Restore state
        self.data_source._temperature_state = state['temperature_state']
        self.data_source._grid_state = state['grid_state']
        self.data_source._helium_state = state['helium_state']
        self.data_source._recovery_state = state['recovery_state']
        self.data_source._ppa_state = state['ppa_state']
        self.data_source.current_scenario = ScenarioType(state['metadata']['scenario'])
        self.data_source.quality = DataQuality(state['metadata']['quality'])
        
        logger.info(f"State loaded from {filepath}")
        return True
    
    def list_saved_states(self) -> List[str]:
        """List all saved states"""
        states = []
        for f in os.listdir(self.save_dir):
            if f.endswith('.json') and not f.endswith('.checksum'):
                states.append(f.replace('.json', ''))
        return states


# ============================================================
# ENHANCEMENT 4: Historical Replay Manager
# ============================================================

class HistoricalReplay:
    """
    Record and replay historical data sequences.
    Enables reproducible testing and bug reproduction.
    """
    
    def __init__(self, data_source: 'SyntheticDataSource', max_history: int = 10000):
        self.data_source = data_source
        self.max_history = max_history
        self.history: List[Dict] = []
        self.replay_index = 0
        self.is_replaying = False
        self._recording = True
    
    def record(self) -> int:
        """Record current state to history"""
        if not self._recording:
            return len(self.history)
        
        snapshot = {
            'timestamp': time.time(),
            'datetime': datetime.now().isoformat(),
            'temperature': self.data_source._temperature_state.copy(),
            'grid': self.data_source._grid_state.copy(),
            'helium': self.data_source._helium_state.copy(),
            'recovery': self.data_source._recovery_state.copy(),
            'scenario': self.data_source.current_scenario.value,
            'quality': self.data_source.quality.value
        }
        
        self.history.append(snapshot)
        
        # Trim history if needed
        if len(self.history) > self.max_history:
            self.history = self.history[-self.max_history:]
        
        return len(self.history)
    
    def start_replay(self) -> bool:
        """Start replay mode from beginning"""
        if not self.history:
            logger.warning("No history to replay")
            return False
        
        self.replay_index = 0
        self.is_replaying = True
        self._recording = False
        
        # Load first state
        self._apply_state(self.history[0])
        logger.info(f"Replay started with {len(self.history)} states")
        return True
    
    def replay_next(self) -> bool:
        """Advance to next state in replay"""
        if not self.is_replaying:
            return False
        
        self.replay_index += 1
        if self.replay_index >= len(self.history):
            self.stop_replay()
            return False
        
        self._apply_state(self.history[self.replay_index])
        return True
    
    def stop_replay(self):
        """Stop replay and resume normal operation"""
        self.is_replaying = False
        self._recording = True
        logger.info("Replay stopped")
    
    def _apply_state(self, snapshot: Dict):
        """Apply a snapshot state to the data source"""
        self.data_source._temperature_state = snapshot['temperature'].copy()
        self.data_source._grid_state = snapshot['grid'].copy()
        self.data_source._helium_state = snapshot['helium'].copy()
        self.data_source._recovery_state = snapshot['recovery'].copy()
        self.data_source.current_scenario = ScenarioType(snapshot['scenario'])
        self.data_source.quality = DataQuality(snapshot['quality'])
    
    def export_history(self, filepath: str):
        """Export history to JSON file"""
        with open(filepath, 'w') as f:
            json.dump(self.history, f, indent=2, default=str)
        logger.info(f"History exported to {filepath}")
    
    def import_history(self, filepath: str):
        """Import history from JSON file"""
        with open(filepath, 'r') as f:
            history = json.load(f)
        
        # Convert string dates back to datetime objects
        for snapshot in history:
            if 'datetime' in snapshot:
                snapshot['datetime'] = snapshot['datetime']
        
        self.history = history
        logger.info(f"Imported {len(history)} states from {filepath}")
    
    def get_replay_progress(self) -> float:
        """Get replay progress as percentage"""
        if not self.history:
            return 0.0
        return self.replay_index / len(self.history) * 100


# ============================================================
# ENHANCEMENT 5: Cross-Stream Correlation Engine
# ============================================================

class CorrelationEngine:
    """
    Model correlations between different data streams.
    
    Simulates realistic dependencies:
    - High temperature increases grid demand (AC usage)
    - High carbon prices affect helium markets
    - Economic conditions affect all markets
    """
    
    def __init__(self, data_source: 'SyntheticDataSource'):
        self.data_source = data_source
        self.correlation_matrix = {
            ('temperature', 'grid_demand'): 0.6,   # Moderate positive correlation
            ('temperature', 'helium_price'): 0.2,  # Weak positive correlation
            ('carbon_price', 'helium_price'): 0.4, # Moderate positive correlation
            ('inventory', 'helium_price'): -0.7   # Strong negative correlation
        }
        self.enabled = True
    
    def apply_correlations(self):
        """Apply cross-stream correlations to current state"""
        if not self.enabled:
            return
        
        # Temperature affects grid demand (AC usage)
        ambient_temp = self.data_source._temperature_state.get('ambient', 22)
        temp_factor = 1 + max(0, (ambient_temp - 25) / 50)
        
        for region in self.data_source._grid_state:
            base_demand = self.data_source._grid_state[region].get('demand', 50000)
            adjusted_demand = base_demand * temp_factor
            self.data_source._grid_state[region]['demand'] = adjusted_demand
        
        # Carbon price affects helium market (indirect via economic activity)
        carbon_price = self.data_source._grid_state.get('us-east', {}).get('carbon_price', 0)
        if carbon_price > 50:
            price_factor = 1 + (carbon_price - 50) / 500
            self.data_source._helium_state['spot_price'] *= price_factor
        
        # Inventory inversely affects price (already in helium dynamics)
        # This is handled in _update_helium
    
    def set_enabled(self, enabled: bool):
        """Enable or disable correlation modeling"""
        self.enabled = enabled
        logger.info(f"Correlation engine {'enabled' if enabled else 'disabled'}")


# ============================================================
# ENHANCEMENT 6: Custom Scenario Manager
# ============================================================

class CustomScenario:
    """User-defined custom scenario"""
    
    def __init__(self, name: str, description: str = ""):
        self.name = name
        self.description = description
        self.temperature_overrides: Dict[str, float] = {}
        self.grid_overrides: Dict[str, Dict[str, float]] = {}
        self.helium_overrides: Dict[str, float] = {}
        self.duration_seconds: float = 0
        self.transition_seconds: float = 0
    
    def set_temperature(self, cpu: float = None, gpu: float = None, ambient: float = None):
        """Set temperature overrides"""
        if cpu is not None:
            self.temperature_overrides['cpu_temp'] = cpu
        if gpu is not None:
            self.temperature_overrides['gpu_temp'] = gpu
        if ambient is not None:
            self.temperature_overrides['ambient'] = ambient
        return self
    
    def set_grid(self, region: str, average: float = None, renewable: float = None):
        """Set grid overrides for a region"""
        if region not in self.grid_overrides:
            self.grid_overrides[region] = {}
        if average is not None:
            self.grid_overrides[region]['average'] = average
        if renewable is not None:
            self.grid_overrides[region]['renewable'] = renewable
        return self
    
    def set_helium(self, price: float = None, inventory: int = None):
        """Set helium market overrides"""
        if price is not None:
            self.helium_overrides['spot_price'] = price
        if inventory is not None:
            self.helium_overrides['inventory'] = inventory
        return self
    
    def set_duration(self, seconds: float):
        """Set scenario duration"""
        self.duration_seconds = seconds
        return self
    
    def apply(self, data_source: 'SyntheticDataSource'):
        """Apply this scenario to the data source"""
        # Apply temperature overrides
        for key, value in self.temperature_overrides.items():
            if key in data_source._temperature_state:
                data_source._temperature_state[key] = value
        
        # Apply grid overrides
        for region, overrides in self.grid_overrides.items():
            if region in data_source._grid_state:
                for key, value in overrides.items():
                    data_source._grid_state[region][key] = value
        
        # Apply helium overrides
        for key, value in self.helium_overrides.items():
            if key in data_source._helium_state:
                data_source._helium_state[key] = value
        
        logger.info(f"Custom scenario '{self.name}' applied")


class CustomScenarioManager:
    """Manage custom scenarios"""
    
    def __init__(self):
        self.scenarios: Dict[str, CustomScenario] = {}
    
    def register(self, scenario: CustomScenario):
        """Register a custom scenario"""
        self.scenarios[scenario.name] = scenario
        logger.info(f"Registered custom scenario: {scenario.name}")
    
    def get(self, name: str) -> Optional[CustomScenario]:
        """Get a custom scenario by name"""
        return self.scenarios.get(name)
    
    def list_scenarios(self) -> List[str]:
        """List all registered scenarios"""
        return list(self.scenarios.keys())
    
    def save_to_file(self, filepath: str):
        """Save scenarios to file"""
        # Implementation would serialize scenarios
        pass
    
    def load_from_file(self, filepath: str):
        """Load scenarios from file"""
        # Implementation would deserialize scenarios
        pass


# ============================================================
# ENHANCEMENT 7: Enhanced Main Synthetic Data Source
# ============================================================

class SyntheticDataSource:
    """
    Enhanced synthetic data source for all enhancement modules.
    Provides realistic, time-varying data with configurable quality and scenarios.
    """
    
    def __init__(self, config: Optional[Dict] = None):
        self.config = config or {}
        self.seed = self.config.get('seed', 42)
        self.quality = DataQuality(self.config.get('quality', 'perfect'))
        self.current_scenario = ScenarioType.NORMAL
        self.update_interval_seconds = self.config.get('update_interval', 5)
        self.regions = self.config.get('regions', ['us-east', 'us-west', 'eu-north', 'asia-pacific'])
        self.simulate_latency = self.config.get('simulate_latency', False)
        self.base_latency_ms = self.config.get('base_latency_ms', 0)
        
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
        
        # Subscribers with backpressure
        self._subscribers: Dict[str, List[Callable]] = {}
        self._subscriber_backpressure: Dict[str, Dict[str, int]] = {}
        
        # New components
        self.persistence = StatePersistence(self)
        self.replay = HistoricalReplay(self)
        self.correlation = CorrelationEngine(self)
        self.custom_scenarios = CustomScenarioManager()
        
        # Set random seed
        np.random.seed(self.seed)
        random.seed(self.seed)
        
        # Background thread
        self._running = False
        self._thread = None
        self._latency_distribution = None
        
        # Initialize
        self._init_state()
        
        logger.info(f"Synthetic Data Source v2.0 initialized (seed={self.seed}, latency={self.simulate_latency})")
    
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
                       'coal': 0.40, 'gas': 0.30, 'nuclear': 0.05, 'carbon_price': 25.0}
            elif region == 'us-west':
                base = {'average': 250, 'marginal': 220, 'demand': 40000, 'renewable': 0.45,
                       'coal': 0.20, 'gas': 0.25, 'nuclear': 0.10, 'carbon_price': 30.0}
            elif region == 'eu-north':
                base = {'average': 80, 'marginal': 70, 'demand': 30000, 'renewable': 0.65,
                       'coal': 0.05, 'gas': 0.15, 'nuclear': 0.15, 'carbon_price': 50.0}
            else:  # asia-pacific
                base = {'average': 550, 'marginal': 520, 'demand': 60000, 'renewable': 0.15,
                       'coal': 0.60, 'gas': 0.20, 'nuclear': 0.05, 'carbon_price': 15.0}
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
            'uptime': 0.99,
            'maintenance_schedule': ['2024-06-01', '2024-12-01']
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
    
    def subscribe(self, data_type: str, callback: Callable, max_backlog: int = 100):
        """Subscribe to data updates with backpressure"""
        if data_type not in self._subscribers:
            self._subscribers[data_type] = []
            self._subscriber_backpressure[data_type] = {}
        
        self._subscribers[data_type].append(callback)
        self._subscriber_backpressure[data_type][callback.__name__] = 0
        logger.debug(f"Subscriber added for {data_type}")
    
    def _update_loop(self):
        """Main update loop for synthetic data generation"""
        while self._running:
            try:
                start_time = time.time()
                
                self._update_all()
                self.correlation.apply_correlations()
                self._notify_subscribers()
                self.replay.record()
                
                # Maintain consistent update interval
                elapsed = time.time() - start_time
                sleep_time = max(0, self.update_interval_seconds - elapsed)
                time.sleep(sleep_time)
                
            except Exception as e:
                logger.error(f"Update error: {e}")
                time.sleep(1)
    
    def _update_all(self):
        """Update all data sources"""
        self._update_temperature()
        self._update_grid()
        self._update_helium()
        self._update_recovery()
    
    def _simulate_latency(self) -> float:
        """Simulate network latency if enabled"""
        if not self.simulate_latency:
            return 0.0
        
        # Simulate latency with distribution
        if self.quality == DataQuality.PERFECT:
            latency = np.random.exponential(self.base_latency_ms / 1000)
        elif self.quality == DataQuality.NOISY:
            latency = np.random.gamma(2, self.base_latency_ms / 1000)
        elif self.quality == DataQuality.DEGRADED:
            latency = np.random.lognormal(0, self.base_latency_ms / 500)
        else:
            latency = self.base_latency_ms / 1000
        
        time.sleep(min(latency, 1.0))  # Cap at 1 second
        return latency * 1000  # Return in ms
    
    def _update_temperature(self):
        """Update thermal data with realistic dynamics"""
        dt = self.update_interval_seconds / 60.0  # minutes
        tau = 20.0  # thermal time constant
        
        # Apply scenario effects
        ambient_offset = self.current_scenario.temperature_offset
        if self.current_scenario == ScenarioType.POWER_OUTAGE:
            workload_multiplier = 0.5
        else:
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
        noise_scale = self.quality.noise_scale
        if noise_scale > 0:
            for key in ['cpu_temp', 'gpu_temp']:
                self._temperature_state[key] += np.random.normal(0, noise_scale * 10)
        
        # Cooling system responds to temperature
        temp_above_ambient = max(0, self._temperature_state['gpu_temp'] - self._temperature_state['ambient'])
        self._temperature_state['cooling_power'] = min(500, temp_above_ambient * 10 + np.random.normal(0, 10))
        self._temperature_state['fan_speed'] = min(100, temp_above_ambient * 2)
        
        # Store history
        self._history['temperature'].append({
            'timestamp': time.time(),
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
            # Demand pattern
            morning_peak = 1.3 if 9 <= hour <= 11 else 1.0
            evening_peak = 1.4 if 17 <= hour <= 19 else 1.0
            night_low = 0.6 if 0 <= hour <= 5 else 1.0
            weekday_factor = 1.2 if is_weekday else 0.8
            
            demand_factor = max(night_low, morning_peak, evening_peak) * weekday_factor
            
            # Apply scenario effects
            carbon_factor = self.current_scenario.carbon_multiplier
            helium_factor = self.current_scenario.helium_multiplier
            
            # Update state with dynamics
            target_demand = 50000 * demand_factor
            state['demand'] = state['demand'] * 0.9 + target_demand * 0.1 + np.random.normal(0, 500)
            state['average'] = max(10, min(1000, state['average'] * carbon_factor + np.random.normal(0, 5)))
            state['marginal'] = state['average'] * (0.8 + 0.4 * np.random.random())
            state['renewable'] = max(0.05, min(0.95, state['renewable'] + np.random.normal(0, 0.02)))
            
            # 6-hour forecast
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
        helium_factor = self.current_scenario.helium_multiplier
        if self.current_scenario == ScenarioType.HELIUM_CRISIS:
            inventory_delta = -1.0
        else:
            inventory_delta = 0.3
        
        price_change = reversion + random_walk
        self._helium_state['spot_price'] = max(2.0, min(15.0, 
            self._helium_state['spot_price'] * helium_factor + price_change))
        
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
    
    def _update_recovery(self):
        """Update recovery system data"""
        if self.current_scenario == ScenarioType.RECOVERY_SUCCESS:
            efficiency_target = 0.95
        else:
            efficiency_target = 0.75
        
        self._recovery_state['efficiency'] = (self._recovery_state['efficiency'] * 0.99 + 
                                               efficiency_target * 0.01 + np.random.normal(0, 0.01))
        self._recovery_state['efficiency'] = min(0.96, max(0.60, self._recovery_state['efficiency']))
        
        # Accumulate recovered helium
        recovery_rate = 0.1  # liters per second
        self._recovery_state['recovered_current'] += recovery_rate * self.update_interval_seconds * self._recovery_state['efficiency']
        self._recovery_state['recovered_ytd'] += recovery_rate * self.update_interval_seconds * self._recovery_state['efficiency']
    
    def _notify_subscribers(self):
        """Notify all subscribers with backpressure handling"""
        for data_type, callbacks in self._subscribers.items():
            for callback in callbacks:
                try:
                    # Check backpressure
                    callback_name = callback.__name__
                    backlog = self._subscriber_backpressure[data_type].get(callback_name, 0)
                    if backlog > 10:
                        logger.warning(f"Skipping subscriber {callback_name} due to backpressure")
                        continue
                    
                    # Get appropriate data
                    if data_type == 'temperature':
                        data = self.get_temperature_data()
                    elif data_type == 'grid':
                        data = self.get_grid_data()
                    elif data_type == 'helium':
                        data = self.get_helium_data()
                    elif data_type == 'recovery':
                        data = self.get_recovery_data()
                    else:
                        continue
                    
                    callback(data)
                    self._subscriber_backpressure[data_type][callback_name] = 0
                    
                except Exception as e:
                    logger.error(f"Subscriber notification failed: {e}")
                    self._subscriber_backpressure[data_type][callback_name] = \
                        self._subscriber_backpressure[data_type].get(callback_name, 0) + 1
    
    def _check_availability(self) -> bool:
        """Check if data is available based on quality level"""
        if self.quality == DataQuality.OFFLINE:
            return False
        if self.quality == DataQuality.RECOVERING and random.random() > 0.5:
            return False
        if self.quality == DataQuality.DEGRADED and random.random() < 0.2:
            return False
        return True
    
    def get_temperature_data(self, device: str = 'gpu') -> SyntheticTemperatureData:
        """Get current temperature data with quality handling and latency simulation"""
        # Simulate latency
        latency = self._simulate_latency()
        
        if not self._check_availability():
            raise ConnectionError(f"Temperature data source offline (quality={self.quality.value})")
        
        # Apply quality-based noise
        cpu_temp = self._temperature_state['cpu_temp']
        gpu_temp = self._temperature_state['gpu_temp']
        noise_scale = self.quality.noise_scale
        
        if noise_scale > 0:
            cpu_temp += np.random.normal(0, noise_scale * 10)
            gpu_temp += np.random.normal(0, noise_scale * 15)
        
        return SyntheticTemperatureData(
            cpu_temp_c=cpu_temp,
            gpu_temp_c=gpu_temp,
            memory_temp_c=self._temperature_state['memory_temp'],
            ambient_temp_c=self._temperature_state['ambient'],
            cooling_power_w=self._temperature_state['cooling_power'],
            fan_speed_percent=self._temperature_state['fan_speed'],
            power_draw_w=self._temperature_state['power_draw'],
            timestamp=datetime.now(),
            quality=self.quality,
            source="synthetic",
            confidence=self.quality.availability,
            latency_ms=latency
        )
    
    def get_grid_data(self, region: str = 'us-east') -> SyntheticGridData:
        """Get current grid data with quality handling"""
        latency = self._simulate_latency()
        
        if not self._check_availability():
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
            carbon_price_usd=state.get('carbon_price', 25.0),
            forecast_next_6h=state.get('forecast', []),
            timestamp=datetime.now(),
            quality=self.quality,
            source="synthetic"
        )
    
    def get_helium_data(self) -> SyntheticHeliumData:
        """Get current helium market data"""
        latency = self._simulate_latency()
        
        if not self._check_availability():
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
        latency = self._simulate_latency()
        
        if not self._check_availability():
            raise ConnectionError("Recovery system data offline")
        
        return SyntheticRecoveryData(
            recovery_efficiency=self._recovery_state['efficiency'],
            liters_recovered_ytd=self._recovery_state['recovered_ytd'],
            liters_recovered_current=self._recovery_state['recovered_current'],
            recovery_method=self._recovery_state['method'],
            energy_cost_kwh_per_liter=self._recovery_state['energy_cost'],
            capex_usd=self._recovery_state['capex'],
            opex_usd_per_year=self._recovery_state['opex'],
            uptime_percentage=self._recovery_state['uptime'],
            maintenance_schedule=self._recovery_state['maintenance_schedule'],
            timestamp=datetime.now(),
            quality=self.quality,
            source="synthetic"
        )
    
    def get_ppa_data(self) -> List[SyntheticPPAData]:
        """Get PPA contract data"""
        latency = self._simulate_latency()
        
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
    
    def set_scenario(self, scenario: Union[ScenarioType, str]):
        """Set test scenario"""
        if isinstance(scenario, str):
            # Check custom scenarios first
            custom = self.custom_scenarios.get(scenario)
            if custom:
                custom.apply(self)
                self.current_scenario = ScenarioType.NORMAL  # Use NORMAL as base
                logger.info(f"Custom scenario '{scenario}' applied")
                return
            
            # Try predefined scenario
            try:
                scenario = ScenarioType(scenario)
            except ValueError:
                logger.warning(f"Unknown scenario: {scenario}")
                return
        
        self.current_scenario = scenario
        logger.info(f"Scenario set to {scenario.value}")
        
        # Apply immediate scenario effects
        if scenario == ScenarioType.HEATWAVE:
            self._temperature_state['ambient'] = 35
        elif scenario == ScenarioType.HELIUM_CRISIS:
            self._helium_state['spot_price'] = 12.0
            self._helium_state['inventory'] = 8
        elif scenario == ScenarioType.RECOVERY_SUCCESS:
            self._recovery_state['efficiency'] = 0.95
        elif scenario == ScenarioType.EXTREME_WEATHER:
            self._temperature_state['ambient'] = 38
            self._temperature_state['gpu_temp'] = 95
        elif scenario == ScenarioType.MARKET_CRASH:
            self._helium_state['spot_price'] = 2.5
    
    def get_scenario_metrics(self) -> Dict:
        """Get current scenario metrics"""
        return {
            'scenario': self.current_scenario.value,
            'quality': self.quality.value,
            'temperature': {
                'gpu': self._temperature_state['gpu_temp'],
                'cpu': self._temperature_state['cpu_temp'],
                'ambient': self._temperature_state['ambient']
            },
            'helium': {
                'price': self._helium_state['spot_price'],
                'inventory': self._helium_state['inventory'],
                'risk': self._helium_state['risk']
            },
            'grid': {
                'us_east_average': self._grid_state['us-east']['average'],
                'renewable_percent': self._grid_state['us-east']['renewable'] * 100,
                'carbon_price': self._grid_state['us-east'].get('carbon_price', 25)
            },
            'recovery': {
                'efficiency': self._recovery_state['efficiency'],
                'recovered_ytd': self._recovery_state['recovered_ytd']
            },
            'correlation_enabled': self.correlation.enabled,
            'latency_simulation': self.simulate_latency
        }
    
    def generate_report(self) -> str:
        """Generate a detailed report of current synthetic data state"""
        report = {
            'timestamp': datetime.now().isoformat(),
            'config': {
                'seed': self.seed,
                'update_interval': self.update_interval_seconds,
                'regions': self.regions,
                'simulate_latency': self.simulate_latency
            },
            'current_state': self.get_scenario_metrics(),
            'history_sizes': {k: len(v) for k, v in self._history.items()},
            'subscribers': {k: len(v) for k, v in self._subscribers.items()},
            'saved_states': self.persistence.list_saved_states(),
            'custom_scenarios': self.custom_scenarios.list_scenarios()
        }
        return json.dumps(report, indent=2)
    
    def save_state(self, name: str = "latest") -> str:
        """Save current state to disk"""
        return self.persistence.save_state(name)
    
    def load_state(self, name: str = "latest") -> bool:
        """Load state from disk"""
        return self.persistence.load_state(name)
    
    def start_replay(self) -> bool:
        """Start replaying historical data"""
        return self.replay.start_replay()
    
    def replay_next(self) -> bool:
        """Advance to next replay state"""
        return self.replay.replay_next()
    
    def stop_replay(self):
        """Stop replay mode"""
        self.replay.stop_replay()
    
    def export_history(self, filepath: str):
        """Export history to file"""
        self.replay.export_history(filepath)
    
    def register_custom_scenario(self, scenario: CustomScenario):
        """Register a custom scenario"""
        self.custom_scenarios.register(scenario)
    
    def enable_correlations(self, enabled: bool = True):
        """Enable or disable cross-stream correlations"""
        self.correlation.set_enabled(enabled)
    
    def set_latency_simulation(self, enabled: bool, base_latency_ms: float = 0):
        """Enable or disable latency simulation"""
        self.simulate_latency = enabled
        self.base_latency_ms = base_latency_ms
        logger.info(f"Latency simulation {'enabled' if enabled else 'disabled'} (base={base_latency_ms}ms)")


# ============================================================
# Usage Example
# ============================================================

def main():
    """Enhanced usage example"""
    print("=== Enhanced Synthetic Data Manager Demo ===\n")
    
    # Initialize data source
    source = SyntheticDataSource({
        'seed': 42,
        'quality': 'perfect',
        'update_interval': 1,
        'simulate_latency': False
    })
    
    source.start()
    
    print("1. Basic data retrieval:")
    temp = source.get_temperature_data()
    print(f"   GPU Temp: {temp.gpu_temp_c:.1f}°C, CPU Temp: {temp.cpu_temp_c:.1f}°C")
    
    grid = source.get_grid_data('us-east')
    print(f"   Grid Intensity: {grid.average_intensity_gco2_per_kwh:.0f} gCO2/kWh")
    
    helium = source.get_helium_data()
    print(f"   Helium Price: ${helium.spot_price_usd_per_liter:.2f}/L")
    
    print("\n2. Scenario switching:")
    source.set_scenario(ScenarioType.HEATWAVE)
    temp2 = source.get_temperature_data()
    print(f"   Heatwave GPU Temp: {temp2.gpu_temp_c:.1f}°C (vs {temp.gpu_temp_c:.1f}°C)")
    
    source.set_scenario(ScenarioType.HELIUM_CRISIS)
    helium2 = source.get_helium_data()
    print(f"   Helium Crisis Price: ${helium2.spot_price_usd_per_liter:.2f}/L")
    
    print("\n3. Quality degradation:")
    source.set_quality(DataQuality.DEGRADED)
    try:
        temp3 = source.get_temperature_data()
        print(f"   Degraded mode temp: {temp3.gpu_temp_c:.1f}°C")
    except Exception as e:
        print(f"   Degraded mode error: {e}")
    
    print("\n4. State persistence:")
    source.save_state("demo_state")
    print(f"   State saved to synthetic_states/demo_state.json")
    
    print("\n5. Custom scenario:")
    custom = CustomScenario("demo_scenario", "Test custom scenario")
    custom.set_temperature(gpu=80, ambient=30).set_helium(price=10.0)
    source.register_custom_scenario(custom)
    source.set_scenario("demo_scenario")
    temp4 = source.get_temperature_data()
    helium4 = source.get_helium_data()
    print(f"   Custom scenario GPU Temp: {temp4.gpu_temp_c:.1f}°C")
    print(f"   Custom scenario Helium Price: ${helium4.spot_price_usd_per_liter:.2f}/L")
    
    source.stop()
    
    print("\n✅ Enhanced Synthetic Data Manager test complete")

if __name__ == "__main__":
    main()
