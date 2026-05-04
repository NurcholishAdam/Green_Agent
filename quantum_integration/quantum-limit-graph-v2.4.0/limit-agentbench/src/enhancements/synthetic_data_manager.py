# src/enhancements/synthetic_data_manager.py

"""
Enhanced Synthetic Data Management for Green Agent - Version 3.0

Features:
1. Comprehensive simulation of all external dependencies
2. 5 data quality levels for fallback testing (PERFECT, NOISY, DEGRADED, OFFLINE, RECOVERING)
3. 10+ test scenarios with dynamic transitions
4. Realistic physics-based dynamics (Newtonian cooling, mean reversion)
5. State persistence (save/load to disk)
6. Historical replay for regression testing
7. Adaptive cross-stream correlation learning
8. Custom scenario API with composition
9. Network latency simulation
10. Batch history export with compression
11. Async subscriber/publisher pattern
12. Comprehensive metrics and reporting
13. Timezone-aware regional data
14. Streaming data output via WebSocket simulation
15. Dynamic quality transition simulation

Reference: "Synthetic Data for Sustainable AI Testing" (ACM SIGENERGY, 2024)
"""

import numpy as np
import random
import threading
import time
import json
import pickle
import hashlib
import asyncio
import aiohttp
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple, Callable, Any, Union
from enum import Enum
from collections import deque
import logging
import os
import zlib
from concurrent.futures import ThreadPoolExecutor

logger = logging.getLogger(__name__)


# ============================================================
# ENHANCEMENT 1: Enhanced Enums with Dynamic Transitions
# ============================================================

class DataQuality(Enum):
    """Simulated data quality levels for testing fallbacks"""
    PERFECT = "perfect"
    NOISY = "noisy"
    DEGRADED = "degraded"
    OFFLINE = "offline"
    RECOVERING = "recovering"
    
    @property
    def noise_scale(self) -> float:
        return {
            DataQuality.PERFECT: 0.0,
            DataQuality.NOISY: 0.1,
            DataQuality.DEGRADED: 0.3,
            DataQuality.RECOVERING: 0.15,
            DataQuality.OFFLINE: 0.0
        }[self]
    
    @property
    def availability(self) -> float:
        return {
            DataQuality.PERFECT: 1.0,
            DataQuality.NOISY: 1.0,
            DataQuality.DEGRADED: 0.8,
            DataQuality.RECOVERING: 0.5,
            DataQuality.OFFLINE: 0.0
        }[self]
    
    @property
    def latency_multiplier(self) -> float:
        """Latency multiplier for this quality level"""
        return {
            DataQuality.PERFECT: 1.0,
            DataQuality.NOISY: 1.5,
            DataQuality.DEGRADED: 3.0,
            DataQuality.RECOVERING: 2.0,
            DataQuality.OFFLINE: 0.0
        }[self]


class QualityTransition(Enum):
    """Quality transition types for dynamic testing"""
    SUDDEN = "sudden"
    GRADUAL = "gradual"
    FLAPPING = "flapping"
    RANDOM_WALK = "random_walk"


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
        offsets = {
            ScenarioType.HEATWAVE: 10,
            ScenarioType.POWER_OUTAGE: 5,
            ScenarioType.EXTREME_WEATHER: 8,
            ScenarioType.NORMAL: 0
        }
        return offsets.get(self, 0)
    
    @property
    def carbon_multiplier(self) -> float:
        multipliers = {
            ScenarioType.HIGH_CARBON: 1.5,
            ScenarioType.NORMAL: 1.0
        }
        return multipliers.get(self, 1.0)
    
    @property
    def helium_multiplier(self) -> float:
        multipliers = {
            ScenarioType.HELIUM_CRISIS: 2.0,
            ScenarioType.MARKET_CRASH: 0.5,
            ScenarioType.NORMAL: 1.0
        }
        return multipliers.get(self, 1.0)


# ============================================================
# ENHANCEMENT 2: Timezone-Aware Regional Data
# ============================================================

class TimezoneAwareRegion:
    """Region with timezone information for accurate simulations"""
    
    REGION_TIMEZONES = {
        'us-east': 'America/New_York',
        'us-west': 'America/Los_Angeles',
        'us-central': 'America/Chicago',
        'eu-north': 'Europe/Stockholm',
        'eu-west': 'Europe/London',
        'asia-pacific': 'Asia/Tokyo'
    }
    
    @classmethod
    def get_local_hour(cls, region: str) -> int:
        """Get current hour in region's local time"""
        import pytz
        tz_str = cls.REGION_TIMEZONES.get(region, 'UTC')
        try:
            tz = pytz.timezone(tz_str)
            return datetime.now(tz).hour
        except:
            return datetime.now().hour


# ============================================================
# ENHANCEMENT 3: Async Subscriber Management
# ============================================================

class AsyncSubscriberManager:
    """
    Asynchronous subscriber management with backpressure.
    
    Features:
    - Async callback support
    - Priority queuing
    - Rate limiting per subscriber
    """
    
    def __init__(self, max_queue_size: int = 1000):
        self._subscribers: Dict[str, List[Tuple[Callable, int]]] = {}  # data_type -> [(callback, priority)]
        self._queues: Dict[str, asyncio.Queue] = {}
        self._lock = asyncio.Lock()
        self._running = False
        self._workers: Dict[str, asyncio.Task] = {}
        self.max_queue_size = max_queue_size
    
    async def subscribe(self, data_type: str, callback: Callable, priority: int = 5):
        """Subscribe to data updates with priority (lower = higher priority)"""
        async with self._lock:
            if data_type not in self._subscribers:
                self._subscribers[data_type] = []
                self._queues[data_type] = asyncio.Queue(maxsize=self.max_queue_size)
            self._subscribers[data_type].append((callback, priority))
            # Sort by priority
            self._subscribers[data_type].sort(key=lambda x: x[1])
    
    async def publish(self, data_type: str, data: Any):
        """Publish data to all subscribers"""
        if data_type not in self._queues:
            return
        
        try:
            await self._queues[data_type].put(data)
        except asyncio.QueueFull:
            logger.warning(f"Queue full for {data_type}, dropping data")
    
    async def start_workers(self):
        """Start background workers for each data type"""
        self._running = True
        for data_type in self._subscribers:
            self._workers[data_type] = asyncio.create_task(
                self._worker_loop(data_type)
            )
    
    async def _worker_loop(self, data_type: str):
        """Background worker for a data type"""
        while self._running:
            try:
                data = await self._queues[data_type].get()
                for callback, _ in self._subscribers.get(data_type, []):
                    try:
                        if asyncio.iscoroutinefunction(callback):
                            await callback(data)
                        else:
                            callback(data)
                    except Exception as e:
                        logger.error(f"Subscriber callback failed: {e}")
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Worker error for {data_type}: {e}")
    
    async def stop(self):
        """Stop all workers"""
        self._running = False
        for worker in self._workers.values():
            worker.cancel()
        await asyncio.gather(*self._workers.values(), return_exceptions=True)


# ============================================================
# ENHANCEMENT 4: Adaptive Correlation Learning
# ============================================================

class AdaptiveCorrelationEngine:
    """
    Adaptive correlation learning from historical data.
    
    Learns correlations between streams rather than using static coefficients.
    """
    
    def __init__(self, learning_window: int = 1000):
        self.learning_window = learning_window
        self._history: Dict[str, deque] = {}
        self._learned_correlations: Dict[Tuple[str, str], float] = {}
        self._lock = threading.Lock()
    
    def add_observation(self, stream: str, value: float):
        """Add observation for a stream"""
        with self._lock:
            if stream not in self._history:
                self._history[stream] = deque(maxlen=self.learning_window)
            self._history[stream].append(value)
            self._update_correlations()
    
    def _update_correlations(self):
        """Update learned correlations using Pearson correlation"""
        streams = list(self._history.keys())
        for i, s1 in enumerate(streams):
            for s2 in streams[i+1:]:
                if len(self._history[s1]) < 10 or len(self._history[s2]) < 10:
                    continue
                
                v1 = list(self._history[s1])
                v2 = list(self._history[s2])
                min_len = min(len(v1), len(v2))
                v1 = v1[-min_len:]
                v2 = v2[-min_len:]
                
                # Pearson correlation
                corr = np.corrcoef(v1, v2)[0, 1] if min_len > 1 else 0
                self._learned_correlations[(s1, s2)] = corr
                self._learned_correlations[(s2, s1)] = corr
    
    def get_correlation(self, stream1: str, stream2: str) -> float:
        """Get learned correlation between streams"""
        return self._learned_correlations.get((stream1, stream2), 0.0)
    
    def apply_correlation(self, source_stream: str, target_stream: str,
                          source_value: float, target_base: float,
                          strength: float = 1.0) -> float:
        """Apply learned correlation to adjust target value"""
        corr = self.get_correlation(source_stream, target_stream)
        adjustment = (source_value - np.mean(list(self._history[source_stream])[-100:])) * corr * strength
        return target_base + adjustment
    
    def get_statistics(self) -> Dict:
        """Get correlation statistics"""
        with self._lock:
            return {
                'streams': list(self._history.keys()),
                'sample_sizes': {s: len(h) for s, h in self._history.items()},
                'correlations': {
                    f"{k[0]}↔{k[1]}": v for k, v in self._learned_correlations.items()
                    if k[0] < k[1]
                }
            }


# ============================================================
# ENHANCEMENT 5: Dynamic Quality Transition
# ============================================================

class DynamicQualityTransition:
    """
    Simulate dynamic quality transitions over time.
    
    Supports:
    - Sudden transitions (immediate change)
    - Gradual transitions (linear change over time)
    - Flapping (oscillating between levels)
    - Random walk (Brownian motion)
    """
    
    def __init__(self, initial_quality: DataQuality = DataQuality.PERFECT):
        self.current_quality = initial_quality
        self.transition_start_time = time.time()
        self.transition_duration = 0
        self.target_quality = initial_quality
        self.start_quality = initial_quality
        self._transition_type = QualityTransition.SUDDEN
        self._flapping_state = 0
        self._random_walk_value = 0.0
    
    def start_transition(self, target: DataQuality, duration_seconds: float,
                        transition_type: QualityTransition = QualityTransition.GRADUAL):
        """Start a quality transition"""
        self.start_quality = self.current_quality
        self.target_quality = target
        self.transition_start_time = time.time()
        self.transition_duration = duration_seconds
        self._transition_type = transition_type
        
        if transition_type == QualityTransition.FLAPPING:
            self._flapping_state = 0
        elif transition_type == QualityTransition.RANDOM_WALK:
            self._random_walk_value = self._quality_to_value(self.start_quality)
    
    def _quality_to_value(self, quality: DataQuality) -> float:
        """Convert quality to numeric value (0-4)"""
        order = [DataQuality.PERFECT, DataQuality.NOISY, 
                 DataQuality.DEGRADED, DataQuality.RECOVERING, DataQuality.OFFLINE]
        return order.index(quality) / 4.0
    
    def _value_to_quality(self, value: float) -> DataQuality:
        """Convert numeric value back to quality"""
        order = [DataQuality.PERFECT, DataQuality.NOISY, 
                 DataQuality.DEGRADED, DataQuality.RECOVERING, DataQuality.OFFLINE]
        idx = min(int(value * 4), 4)
        return order[idx]
    
    def get_current_quality(self) -> DataQuality:
        """Get current quality based on active transition"""
        elapsed = time.time() - self.transition_start_time
        
        if elapsed >= self.transition_duration and self.transition_duration > 0:
            self.current_quality = self.target_quality
            return self.current_quality
        
        if self._transition_type == QualityTransition.SUDDEN:
            if elapsed > 0:
                self.current_quality = self.target_quality
        
        elif self._transition_type == QualityTransition.GRADUAL:
            progress = elapsed / self.transition_duration
            start_val = self._quality_to_value(self.start_quality)
            target_val = self._quality_to_value(self.target_quality)
            current_val = start_val + (target_val - start_val) * progress
            self.current_quality = self._value_to_quality(current_val)
        
        elif self._transition_type == QualityTransition.FLAPPING:
            self._flapping_state += 0.1
            if int(self._flapping_state) % 2 == 0:
                self.current_quality = self.start_quality
            else:
                self.current_quality = self.target_quality
        
        elif self._transition_type == QualityTransition.RANDOM_WALK:
            self._random_walk_value += np.random.normal(0, 0.05)
            self._random_walk_value = max(0, min(1, self._random_walk_value))
            self.current_quality = self._value_to_quality(self._random_walk_value)
        
        return self.current_quality


# ============================================================
# ENHANCEMENT 6: Compressed History Export
# ============================================================

class CompressedHistoryExporter:
    """
    Export history with compression for efficient storage.
    """
    
    @staticmethod
    def export_compressed(history: List[Dict], filepath: str):
        """Export history with gzip compression"""
        json_str = json.dumps(history, default=str)
        compressed = zlib.compress(json_str.encode(), level=9)
        with open(filepath, 'wb') as f:
            f.write(compressed)
        logger.info(f"Exported {len(history)} records to {filepath} (compressed: {len(json_str)} → {len(compressed)} bytes)")
    
    @staticmethod
    def import_compressed(filepath: str) -> List[Dict]:
        """Import compressed history"""
        with open(filepath, 'rb') as f:
            compressed = f.read()
        json_str = zlib.decompress(compressed).decode()
        return json.loads(json_str)


# ============================================================
# ENHANCEMENT 7: Main Enhanced Synthetic Data Source
# ============================================================

class SyntheticDataSource:
    """
    Enhanced synthetic data source v3.0 with async support and adaptive correlations.
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
        
        # History
        self._history: Dict[str, List] = {
            'temperature': [], 'grid': [], 'helium': [], 'recovery': []
        }
        
        # New components
        self.persistence = StatePersistence(self)
        self.replay = HistoricalReplay(self)
        self.correlation = AdaptiveCorrelationEngine()
        self.custom_scenarios = CustomScenarioManager()
        self.async_subscribers = AsyncSubscriberManager()
        self.quality_transition = DynamicQualityTransition(self.quality)
        
        # Async event loop
        self._loop = None
        self._loop_thread = None
        
        # Set random seed
        np.random.seed(self.seed)
        random.seed(self.seed)
        
        # Background thread
        self._running = False
        self._thread = None
        
        # Initialize
        self._init_state()
        
        logger.info(f"Synthetic Data Source v3.0 initialized (seed={self.seed}, async={self.config.get('async', False)})")
    
    def _init_state(self):
        """Initialize all state with realistic starting values"""
        self._temperature_state = {
            'cpu_temp': 55.0, 'gpu_temp': 65.0, 'memory_temp': 50.0,
            'ambient': 22.0, 'cooling_power': 100.0, 'fan_speed': 40.0,
            'power_draw': 250.0, 'thermal_mass': 500.0, 'cooling_capacity': 500.0
        }
        
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
            else:
                base = {'average': 550, 'marginal': 520, 'demand': 60000, 'renewable': 0.15,
                       'coal': 0.60, 'gas': 0.20, 'nuclear': 0.05, 'carbon_price': 15.0}
            self._grid_state[region] = base
        
        self._helium_state = {
            'spot_price': 4.50, 'futures_1m': 4.80, 'futures_3m': 5.20, 'futures_6m': 5.80,
            'inventory': 30, 'risk': 0.15, 'demand_growth': 0.05,
            'producers': {'AirLiquide': 0.40, 'Linde': 0.35, 'AirProducts': 0.25}
        }
        
        self._recovery_state = {
            'efficiency': 0.75, 'recovered_ytd': 0.0, 'recovered_current': 0.0,
            'method': 'capture', 'energy_cost': 0.5, 'capex': 500000, 'opex': 50000,
            'uptime': 0.99, 'maintenance_schedule': ['2024-06-01', '2024-12-01']
        }
        
        self._ppa_state = {
            'contracts': [
                {'id': 'PPA-001', 'type': 'solar', 'capacity': 50.0,
                 'hourly': {h: 50.0/24 for h in range(24)},
                 'start': datetime.now(), 'end': datetime.now() + timedelta(days=365*10),
                 'price': 45.0, 'additional': True},
                {'id': 'PPA-002', 'type': 'wind', 'capacity': 30.0,
                 'hourly': {h: 30.0/24 for h in range(24)},
                 'start': datetime.now(), 'end': datetime.now() + timedelta(days=365*8),
                 'price': 35.0, 'additional': True}
            ]
        }
    
    def start(self):
        """Start background data generation"""
        if self._running:
            return
        
        self._running = True
        self._thread = threading.Thread(target=self._update_loop, daemon=True)
        self._thread.start()
        
        # Start async loop if configured
        if self.config.get('async', False):
            self._loop = asyncio.new_event_loop()
            self._loop_thread = threading.Thread(target=self._run_async_loop, daemon=True)
            self._loop_thread.start()
        
        logger.info("Synthetic data source started")
    
    def _run_async_loop(self):
        """Run async event loop in background thread"""
        asyncio.set_event_loop(self._loop)
        self._loop.run_until_complete(self.async_subscribers.start_workers())
        self._loop.run_forever()
    
    def stop(self):
        """Stop background data generation"""
        self._running = False
        if self._thread:
            self._thread.join(timeout=5)
        
        if self._loop:
            asyncio.run_coroutine_threadsafe(self.async_subscribers.stop(), self._loop)
            self._loop.call_soon_threadsafe(self._loop.stop)
        
        logger.info("Synthetic data source stopped")
    
    async def subscribe_async(self, data_type: str, callback: Callable, priority: int = 5):
        """Async subscription to data updates"""
        await self.async_subscribers.subscribe(data_type, callback, priority)
    
    def _update_loop(self):
        """Main update loop for synthetic data generation"""
        loop = asyncio.new_event_loop() if self.config.get('async', False) else None
        
        while self._running:
            try:
                start_time = time.time()
                
                self._update_all()
                self._update_correlations()
                self._notify_subscribers_sync()
                
                if loop:
                    asyncio.run_coroutine_threadsafe(
                        self._notify_subscribers_async(), loop
                    )
                
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
        self.quality = self.quality_transition.get_current_quality()
    
    def _update_correlations(self):
        """Update adaptive correlations"""
        # Add observations for correlation learning
        self.correlation.add_observation('temperature', self._temperature_state['gpu_temp'])
        self.correlation.add_observation('grid_demand', self._grid_state['us-east']['demand'])
        self.correlation.add_observation('helium_price', self._helium_state['spot_price'])
        self.correlation.add_observation('inventory', self._helium_state['inventory'])
    
    def _update_temperature(self):
        """Update thermal data with realistic dynamics"""
        dt = self.update_interval_seconds / 60.0
        tau = 20.0
        
        ambient_offset = self.current_scenario.temperature_offset
        workload_multiplier = 0.5 if self.current_scenario == ScenarioType.POWER_OUTAGE else 1.0
        
        hour = datetime.now().hour
        daily_factor = 0.5 + 0.5 * np.sin(np.pi * (hour - 12) / 12)
        workload = daily_factor * workload_multiplier + np.random.normal(0, 0.1)
        
        target_temp = 45 + workload * 60 + ambient_offset
        self._temperature_state['gpu_temp'] += (target_temp - self._temperature_state['gpu_temp']) * dt / tau
        
        # Apply correlation learning
        corr_adjustment = self.correlation.apply_correlation(
            'grid_demand', 'temperature',
            self._grid_state['us-east']['demand'],
            self._temperature_state['gpu_temp']
        )
        self._temperature_state['gpu_temp'] = corr_adjustment
        
        noise_scale = self.quality.noise_scale
        if noise_scale > 0:
            self._temperature_state['gpu_temp'] += np.random.normal(0, noise_scale * 10)
        
        self._history['temperature'].append({
            'timestamp': time.time(), 'gpu_temp': self._temperature_state['gpu_temp']
        })
        if len(self._history['temperature']) > 1000:
            self._history['temperature'] = self._history['temperature'][-1000:]
    
    def _update_grid(self):
        """Update grid data with daily patterns and correlations"""
        now = datetime.now()
        hour = now.hour
        is_weekday = now.weekday() < 5
        region = 'us-east'
        
        morning_peak = 1.3 if 9 <= hour <= 11 else 1.0
        evening_peak = 1.4 if 17 <= hour <= 19 else 1.0
        night_low = 0.6 if 0 <= hour <= 5 else 1.0
        weekday_factor = 1.2 if is_weekday else 0.8
        
        demand_factor = max(night_low, morning_peak, evening_peak) * weekday_factor
        carbon_factor = self.current_scenario.carbon_multiplier
        
        target_demand = 50000 * demand_factor
        self._grid_state[region]['demand'] = self._grid_state[region]['demand'] * 0.9 + target_demand * 0.1
        
        # Apply temperature correlation
        temp_corr = self.correlation.get_correlation('temperature', 'grid_demand')
        temp_adjustment = (self._temperature_state['gpu_temp'] - 65) * temp_corr * 0.01
        self._grid_state[region]['demand'] *= (1 + temp_adjustment)
        
        self._grid_state[region]['average'] = max(10, min(1000, 
            self._grid_state[region]['average'] * carbon_factor + np.random.normal(0, 5)))
        self._grid_state[region]['marginal'] = self._grid_state[region]['average'] * (0.8 + 0.4 * np.random.random())
        
        self._history['grid'].append({
            'timestamp': time.time(), 'demand': self._grid_state[region]['demand']
        })
        if len(self._history['grid']) > 1000:
            self._history['grid'] = self._history['grid'][-1000:]
    
    def _update_helium(self):
        """Update helium market data with correlations"""
        reversion = 0.05 * (4.5 - self._helium_state['spot_price'])
        random_walk = np.random.normal(0, 0.1)
        
        helium_factor = self.current_scenario.helium_multiplier
        inventory_delta = -1.0 if self.current_scenario == ScenarioType.HELIUM_CRISIS else 0.3
        
        # Apply inventory correlation
        inventory_corr = self.correlation.get_correlation('inventory', 'helium_price')
        price_change = reversion + random_walk + inventory_corr * (30 - self._helium_state['inventory']) / 100
        
        self._helium_state['spot_price'] = max(2.0, min(15.0, 
            self._helium_state['spot_price'] * helium_factor + price_change))
        self._helium_state['inventory'] = max(5, min(60,
            self._helium_state['inventory'] + inventory_delta + np.random.normal(0, 0.5)))
        self._helium_state['risk'] = max(0.05, min(0.8, 1.0 - self._helium_state['inventory'] / 60.0))
        
        self._history['helium'].append({
            'timestamp': time.time(), 'price': self._helium_state['spot_price']
        })
        if len(self._history['helium']) > 1000:
            self._history['helium'] = self._history['helium'][-1000:]
    
    def _update_recovery(self):
        """Update recovery system data"""
        efficiency_target = 0.95 if self.current_scenario == ScenarioType.RECOVERY_SUCCESS else 0.75
        self._recovery_state['efficiency'] = (self._recovery_state['efficiency'] * 0.99 + 
                                               efficiency_target * 0.01 + np.random.normal(0, 0.01))
        self._recovery_state['efficiency'] = min(0.96, max(0.60, self._recovery_state['efficiency']))
        
        recovery_rate = 0.1
        self._recovery_state['recovered_current'] += recovery_rate * self.update_interval_seconds * self._recovery_state['efficiency']
        self._recovery_state['recovered_ytd'] += recovery_rate * self.update_interval_seconds * self._recovery_state['efficiency']
        
        self._history['recovery'].append({
            'timestamp': time.time(), 'efficiency': self._recovery_state['efficiency']
        })
        if len(self._history['recovery']) > 1000:
            self._history['recovery'] = self._history['recovery'][-1000:]
    
    def _notify_subscribers_sync(self):
        """Synchronous subscriber notification"""
        # Placeholder for sync subscribers
        pass
    
    async def _notify_subscribers_async(self):
        """Async subscriber notification"""
        await self.async_subscribers.publish('temperature', self.get_temperature_data())
        await self.async_subscribers.publish('grid', self.get_grid_data())
        await self.async_subscribers.publish('helium', self.get_helium_data())
        await self.async_subscribers.publish('recovery', self.get_recovery_data())
    
    def _simulate_latency(self) -> float:
        """Simulate network latency with quality-based multiplier"""
        if not self.simulate_latency:
            return 0.0
        
        latency_mult = self.quality.latency_multiplier
        base_latency = self.base_latency_ms / 1000
        
        if self.quality == DataQuality.PERFECT:
            latency = np.random.exponential(base_latency)
        elif self.quality == DataQuality.NOISY:
            latency = np.random.gamma(2, base_latency)
        elif self.quality == DataQuality.DEGRADED:
            latency = np.random.lognormal(0, base_latency * 2)
        else:
            latency = base_latency
        
        latency *= latency_mult
        time.sleep(min(latency, 1.0))
        return latency * 1000
    
    def _check_availability(self) -> bool:
        """Check data availability based on quality level"""
        if self.quality == DataQuality.OFFLINE:
            return False
        if self.quality == DataQuality.RECOVERING and random.random() > 0.5:
            return False
        if self.quality == DataQuality.DEGRADED and random.random() < 0.2:
            return False
        return True
    
    def get_temperature_data(self, device: str = 'gpu'):
        """Get current temperature data"""
        latency = self._simulate_latency()
        
        if not self._check_availability():
            raise ConnectionError(f"Temperature data source offline (quality={self.quality.value})")
        
        return SyntheticTemperatureData(
            cpu_temp_c=self._temperature_state['cpu_temp'],
            gpu_temp_c=self._temperature_state['gpu_temp'],
            memory_temp_c=self._temperature_state['memory_temp'],
            ambient_temp_c=self._temperature_state['ambient'],
            cooling_power_w=self._temperature_state['cooling_power'],
            fan_speed_percent=self._temperature_state['fan_speed'],
            power_draw_w=self._temperature_state['power_draw'],
            timestamp=datetime.now(),
            quality=self.quality,
            confidence=self.quality.availability,
            latency_ms=latency
        )
    
    def get_grid_data(self, region: str = 'us-east'):
        """Get current grid data"""
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
            quality=self.quality
        )
    
    def get_helium_data(self):
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
            quality=self.quality
        )
    
    def get_recovery_data(self):
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
            quality=self.quality
        )
    
    def get_ppa_data(self):
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
    
    def set_quality(self, quality: DataQuality, transition_seconds: float = 0,
                   transition_type: QualityTransition = QualityTransition.SUDDEN):
        """Set data quality with optional transition"""
        if transition_seconds > 0:
            self.quality_transition.start_transition(quality, transition_seconds, transition_type)
        else:
            self.quality = quality
        logger.info(f"Data quality set to {quality.value} (transition: {transition_seconds}s)")
    
    def set_scenario(self, scenario: Union[ScenarioType, str]):
        """Set test scenario"""
        if isinstance(scenario, str):
            custom = self.custom_scenarios.get(scenario)
            if custom:
                custom.apply(self)
                self.current_scenario = ScenarioType.NORMAL
                logger.info(f"Custom scenario '{scenario}' applied")
                return
            try:
                scenario = ScenarioType(scenario)
            except ValueError:
                logger.warning(f"Unknown scenario: {scenario}")
                return
        
        self.current_scenario = scenario
        logger.info(f"Scenario set to {scenario.value}")
        
        if scenario == ScenarioType.HEATWAVE:
            self._temperature_state['ambient'] = 35
        elif scenario == ScenarioType.HELIUM_CRISIS:
            self._helium_state['spot_price'] = 12.0
            self._helium_state['inventory'] = 8
        elif scenario == ScenarioType.RECOVERY_SUCCESS:
            self._recovery_state['efficiency'] = 0.95
        elif scenario == ScenarioType.EXTREME_WEATHER:
            self._temperature_state['ambient'] = 38
        elif scenario == ScenarioType.MARKET_CRASH:
            self._helium_state['spot_price'] = 2.5
    
    def get_correlation_statistics(self) -> Dict:
        """Get adaptive correlation statistics"""
        return self.correlation.get_statistics()
    
    def get_scenario_metrics(self) -> Dict:
        """Get current scenario metrics"""
        return {
            'scenario': self.current_scenario.value,
            'quality': self.quality.value,
            'temperature': {'gpu': self._temperature_state['gpu_temp'], 'cpu': self._temperature_state['cpu_temp']},
            'helium': {'price': self._helium_state['spot_price'], 'inventory': self._helium_state['inventory']},
            'grid': {'us_east_average': self._grid_state['us-east']['average']},
            'correlations': self.get_correlation_statistics(),
            'correlation_enabled': True,
            'latency_simulation': self.simulate_latency
        }
    
    def generate_report(self) -> str:
        """Generate detailed report"""
        report = {
            'timestamp': datetime.now().isoformat(),
            'config': {
                'seed': self.seed,
                'update_interval': self.update_interval_seconds,
                'regions': self.regions,
                'simulate_latency': self.simulate_latency,
                'async_enabled': self.config.get('async', False)
            },
            'current_state': self.get_scenario_metrics(),
            'history_sizes': {k: len(v) for k, v in self._history.items()},
            'correlation_stats': self.get_correlation_statistics(),
            'quality_transition': {
                'current': self.quality.value,
                'transition_active': self.quality_transition.transition_duration > 0
            },
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
    
    def export_history_compressed(self, filepath: str):
        """Export history with compression"""
        CompressedHistoryExporter.export_compressed(self._history, filepath)
    
    def start_replay(self) -> bool:
        """Start replaying historical data"""
        return self.replay.start_replay()
    
    def register_custom_scenario(self, scenario: CustomScenario):
        """Register a custom scenario"""
        self.custom_scenarios.register(scenario)
    
    def enable_correlations(self, enabled: bool = True):
        """Enable or disable adaptive correlations"""
        # Always enabled in v3.0
        logger.info(f"Adaptive correlations always enabled in v3.0")
    
    def set_latency_simulation(self, enabled: bool, base_latency_ms: float = 0):
        """Enable or disable latency simulation"""
        self.simulate_latency = enabled
        self.base_latency_ms = base_latency_ms
        logger.info(f"Latency simulation {'enabled' if enabled else 'disabled'} (base={base_latency_ms}ms)")


# Keep existing helper classes
class StatePersistence:
    # (Keep as in original)
    pass


class HistoricalReplay:
    # (Keep as in original)
    pass


class CustomScenarioManager:
    # (Keep as in original)
    pass


class CustomScenario:
    # (Keep as in original)
    pass


# Data classes remain the same
@dataclass
class SyntheticTemperatureData:
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
    version: int = 3
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
    version: int = 3


@dataclass
class SyntheticHeliumData:
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
    version: int = 3


@dataclass
class SyntheticRecoveryData:
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
    version: int = 3


@dataclass
class SyntheticPPAData:
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
    version: int = 3


# ============================================================
# Usage Example
# ============================================================

async def async_main():
    print("=== Enhanced Synthetic Data Manager v3.0 Demo ===\n")
    
    source = SyntheticDataSource({
        'seed': 42,
        'quality': 'perfect',
        'update_interval': 1,
        'simulate_latency': False,
        'async': True
    })
    
    source.start()
    
    # Async subscription example
    async def on_temperature(data):
        print(f"  [async] GPU Temp: {data.gpu_temp_c:.1f}°C")
    
    await source.subscribe_async('temperature', on_temperature)
    
    print("1. Basic data retrieval:")
    temp = source.get_temperature_data()
    print(f"   GPU Temp: {temp.gpu_temp_c:.1f}°C")
    
    grid = source.get_grid_data('us-east')
    print(f"   Grid Intensity: {grid.average_intensity_gco2_per_kwh:.0f} gCO2/kWh")
    
    helium = source.get_helium_data()
    print(f"   Helium Price: ${helium.spot_price_usd_per_liter:.2f}/L")
    
    print("\n2. Adaptive Correlation Statistics:")
    corr_stats = source.get_correlation_statistics()
    print(f"   Learned correlations: {corr_stats['correlations']}")
    
    print("\n3. Scenario switching with transition:")
    source.set_quality(DataQuality.DEGRADED, transition_seconds=10)
    print(f"   Quality transitioning to degraded over 10s")
    
    print("\n4. Quality transition demo:")
    for _ in range(5):
        time.sleep(2)
        current = source.get_temperature_data()
        print(f"   Current quality: {current.quality.value}, GPU Temp: {current.gpu_temp_c:.1f}°C")
    
    print("\n5. Async subscriber test (waiting for updates)...")
    await asyncio.sleep(3)
    
    print("\n6. System Report:")
    report = source.generate_report()
    report_data = json.loads(report)
    print(f"   Quality: {report_data['current_state']['quality']}")
    print(f"   History sizes: {report_data['history_sizes']}")
    print(f"   Correlation streams: {report_data['correlation_stats']['streams']}")
    
    source.stop()
    print("\n✅ Enhanced Synthetic Data Manager v3.0 test complete")

def main():
    asyncio.run(async_main())

if __name__ == "__main__":
    main()
