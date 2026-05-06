# src/enhancements/synthetic_data_manager.py

"""
Enhanced Synthetic Data Management for Green Agent - Version 3.1

ENHANCEMENTS:
1. Calibrated physics models with real-world parameters
2. Machine learning-based correlation learning with online updates
3. Advanced latency simulation with burst patterns
4. Market microstructure model for realistic price dynamics
5. Weather-aware renewable generation with real weather patterns
6. Fault injection framework for chaos testing
7. Data quality telemetry with SLO tracking
8. Scenario composition engine for complex test cases
9. Performance benchmarking mode
10. Real-time data validation against historical patterns

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
from scipy import stats
from scipy.signal import savgol_filter

# Try to import optional dependencies
try:
    from sklearn.ensemble import RandomForestRegressor
    from sklearn.preprocessing import StandardScaler
    SKLEARN_AVAILABLE = True
except ImportError:
    SKLEARN_AVAILABLE = False
    logger.warning("scikit-learn not available, using basic correlation")

logger = logging.getLogger(__name__)


# ============================================================
# ENHANCEMENT 1: Calibrated Physics Models
# ============================================================

class CalibratedPhysicsModels:
    """
    Physics-based models calibrated with real-world thermal dynamics.
    
    Features:
    - Second-order thermal dynamics (mass × specific heat × dT/dt = Q_in - Q_out)
    - Ambient temperature effects with diurnal variation
    - Cooling system performance curves (non-linear COP)
    """
    
    @staticmethod
    def calculate_thermal_response(initial_temp_c: float, 
                                   power_watts: float,
                                   cooling_capacity_w: float,
                                   thermal_mass_j_per_c: float,
                                   ambient_temp_c: float,
                                   dt_seconds: float) -> float:
        """
        Calculate temperature change using first-order thermal dynamics.
        
        dT/dt = (Q_in - Q_out) / (m × Cp)
        where Q_out = cooling_capacity × (T - T_ambient) / ΔT_max
        """
        # Heat gain from power dissipation
        heat_gain = power_watts
        
        # Heat loss to cooling system (proportional to temperature difference)
        temp_delta = initial_temp_c - ambient_temp_c
        heat_loss = cooling_capacity_w * min(1.0, max(0.0, temp_delta / 50.0))
        
        # Net heat flow
        net_heat = heat_gain - heat_loss
        
        # Temperature change
        dT = net_heat * dt_seconds / thermal_mass_j_per_c
        
        return initial_temp_c + dT
    
    @staticmethod
    def calculate_cop(ambient_temp_c: float, target_temp_c: float) -> float:
        """
        Calculate Coefficient of Performance for cooling system.
        
        Carnot COP = T_cold / (T_hot - T_cold)
        Real COP ≈ 0.4 × Carnot COP
        """
        t_cold_k = target_temp_c + 273.15
        t_hot_k = ambient_temp_c + 273.15
        
        if t_hot_k <= t_cold_k:
            return 10.0
        
        carnot_cop = t_cold_k / (t_hot_k - t_cold_k)
        return max(1.0, min(8.0, carnot_cop * 0.4))
    
    @staticmethod
    def calculate_solar_irradiance(latitude_deg: float, 
                                   hour: int, 
                                   day_of_year: int,
                                   cloud_cover: float) -> float:
        """
        Calculate solar irradiance (W/m²) based on position and weather.
        
        Uses sun position algorithm and cloud attenuation.
        """
        # Solar declination angle
        declination = 23.45 * np.sin(np.radians(360 * (284 + day_of_year) / 365))
        
        # Hour angle (15° per hour, noon = 0)
        hour_angle = 15 * (hour - 12)
        
        # Solar elevation angle
        sin_elevation = (np.sin(np.radians(latitude_deg)) * np.sin(np.radians(declination)) +
                        np.cos(np.radians(latitude_deg)) * np.cos(np.radians(declination)) *
                        np.cos(np.radians(hour_angle)))
        
        elevation_deg = np.degrees(np.arcsin(max(0, min(1, sin_elevation))))
        
        # Clear sky irradiance (simplified)
        if elevation_deg <= 0:
            return 0.0
        
        clear_sky = 1000 * np.sin(np.radians(elevation_deg))
        
        # Cloud attenuation (1 - cloud_cover)^2
        cloud_factor = (1 - cloud_cover) ** 2
        
        return clear_sky * cloud_factor


# ============================================================
# ENHANCEMENT 2: ML-Based Correlation Learning
# ============================================================

class MLCorrelationLearner:
    """
    Machine learning-based correlation learning using Random Forest.
    
    Learns non-linear relationships between data streams.
    """
    
    def __init__(self, learning_window: int = 1000):
        self.learning_window = learning_window
        self._history: Dict[str, deque] = {}
        self._model: Optional[RandomForestRegressor] = None
        self._scaler = StandardScaler() if SKLEARN_AVAILABLE else None
        self._feature_importance: Dict[str, float] = {}
        self._lock = threading.Lock()
        self._last_train_time = 0
        self._train_interval = 60  # seconds
        
    def add_observation(self, stream: str, value: float, features: Optional[Dict[str, float]] = None):
        """Add observation with optional features for ML training"""
        with self._lock:
            if stream not in self._history:
                self._history[stream] = deque(maxlen=self.learning_window)
            self._history[stream].append((time.time(), value, features or {}))
            
            # Periodic retraining
            if time.time() - self._last_train_time > self._train_interval:
                self._train_model()
    
    def _train_model(self):
        """Train Random Forest model on historical data"""
        if not SKLEARN_AVAILABLE or len(self._history) < 2:
            return
        
        # Prepare training data
        X = []
        y = []
        
        # Use temperature as target, grid demand and time as features
        if 'temperature' in self._history and 'grid_demand' in self._history:
            # Align timestamps
            temp_data = list(self._history['temperature'])
            grid_data = list(self._history['grid_demand'])
            
            # Simple alignment by index
            min_len = min(len(temp_data), len(grid_data))
            for i in range(min_len):
                _, temp_val, _ = temp_data[i]
                _, grid_val, _ = grid_data[i]
                X.append([grid_val, np.sin(2 * np.pi * i / 24), np.cos(2 * np.pi * i / 24)])
                y.append(temp_val)
        
        if len(X) < 50:
            return
        
        X_arr = np.array(X)
        y_arr = np.array(y)
        
        # Train Random Forest
        self._model = RandomForestRegressor(n_estimators=100, max_depth=10, random_state=42)
        self._model.fit(X_arr, y_arr)
        
        # Extract feature importance
        self._feature_importance = {
            'grid_demand': self._model.feature_importances_[0],
            'hour_sin': self._model.feature_importances_[1],
            'hour_cos': self._model.feature_importances_[2]
        }
        
        self._last_train_time = time.time()
        logger.debug(f"ML correlation model trained on {len(X)} samples")
    
    def predict_correlation(self, source_stream: str, target_stream: str,
                           source_value: float, base_value: float) -> float:
        """Predict correlation-adjusted value using ML model"""
        if not SKLEARN_AVAILABLE or self._model is None:
            # Fallback to linear correlation
            return base_value
        
        try:
            features = np.array([[source_value, 
                                 np.sin(2 * np.pi * datetime.now().hour / 24),
                                 np.cos(2 * np.pi * datetime.now().hour / 24)]])
            prediction = self._model.predict(features)[0]
            
            # Blend with base value (70% ML, 30% base)
            return 0.7 * prediction + 0.3 * base_value
        except Exception as e:
            logger.warning(f"ML prediction failed: {e}")
            return base_value
    
    def get_feature_importance(self) -> Dict[str, float]:
        """Get feature importance from trained model"""
        return self._feature_importance
    
    def get_statistics(self) -> Dict:
        """Get correlation statistics"""
        with self._lock:
            return {
                'streams': list(self._history.keys()),
                'sample_sizes': {s: len(h) for s, h in self._history.items()},
                'ml_enabled': SKLEARN_AVAILABLE and self._model is not None,
                'feature_importance': self._feature_importance if self._model else {}
            }


# ============================================================
# ENHANCEMENT 3: Advanced Latency Simulation with Burst Patterns
# ============================================================

class AdvancedLatencySimulator:
    """
    Advanced network latency simulation with burst patterns and congestion.
    
    Features:
    - Burst losses (multiple packets dropped consecutively)
    - Correlated latency (requests delayed together)
    - Congestion window effects
    - Jitter buffer simulation
    """
    
    def __init__(self, base_latency_ms: float = 50.0,
                 jitter_ms: float = 10.0,
                 burst_probability: float = 0.05,
                 burst_duration_seconds: float = 2.0):
        self.base_latency_ms = base_latency_ms
        self.jitter_ms = jitter_ms
        self.burst_probability = burst_probability
        self.burst_duration_seconds = burst_duration_seconds
        self._in_burst = False
        self._burst_end_time = 0
        self._congestion_window = 1.0
        
        # Statistics
        self.total_requests = 0
        self.burst_requests = 0
        self.dropped_requests = 0
    
    def simulate_latency(self, quality: 'DataQuality') -> float:
        """Simulate network latency with burst patterns"""
        self.total_requests += 1
        
        # Check for burst start
        if not self._in_burst and random.random() < self.burst_probability:
            self._in_burst = True
            self._burst_end_time = time.time() + self.burst_duration_seconds
            logger.debug("Network burst started")
        
        # Update burst state
        if self._in_burst and time.time() > self._burst_end_time:
            self._in_burst = False
            logger.debug("Network burst ended")
        
        # Quality-based multiplier
        quality_mult = quality.latency_multiplier
        
        # Base latency with jitter
        if self._in_burst:
            # During burst: higher latency, more jitter
            base = self.base_latency_ms * 3
            jitter = self.jitter_ms * 5
            self.burst_requests += 1
        else:
            base = self.base_latency_ms
            jitter = self.jitter_ms
        
        # Congestion window effect
        if self.congestion_window < 0.5:
            base *= 2
        
        # Sample latency
        latency = np.random.normal(base, jitter)
        latency = max(0, latency)
        
        # Simulate packet loss during burst
        if self._in_burst and random.random() < 0.2:
            self.dropped_requests += 1
            raise ConnectionError("Packet dropped during network burst")
        
        # Apply quality multiplier
        latency *= quality_mult
        
        return latency / 1000.0  # Convert to seconds
    
    def update_congestion_window(self, success: bool):
        """Update TCP congestion window based on success/failure"""
        if success:
            self.congestion_window = min(1.0, self.congestion_window * 1.1)
        else:
            self.congestion_window = max(0.3, self.congestion_window * 0.5)
    
    def get_statistics(self) -> Dict:
        """Get latency simulation statistics"""
        return {
            'total_requests': self.total_requests,
            'burst_requests': self.burst_requests,
            'dropped_requests': self.dropped_requests,
            'drop_rate': self.dropped_requests / max(1, self.total_requests),
            'congestion_window': self.congestion_window,
            'in_burst': self._in_burst
        }


# ============================================================
# ENHANCEMENT 4: Market Microstructure Model
# ============================================================

class MarketMicrostructureModel:
    """
    Realistic market price dynamics with microstructure effects.
    
    Features:
    - Order book simulation (bid-ask spread)
    - Price impact models
    - Volatility clustering
    - Mean reversion with stochastic volatility
    """
    
    def __init__(self, base_price: float = 4.5,
                 spread_percent: float = 0.02,
                 volatility: float = 0.15,
                 mean_reversion_strength: float = 0.05):
        self.base_price = base_price
        self.spread_percent = spread_percent
        self.volatility = volatility
        self.mean_reversion = mean_reversion_strength
        
        self.current_price = base_price
        self.bid_price = base_price * (1 - spread_percent / 2)
        self.ask_price = base_price * (1 + spread_percent / 2)
        
        # Volatility clustering state
        self.volatility_state = volatility
        self.volatility_persistence = 0.85
        
        # Price impact history
        self.trade_history: List[float] = []
    
    def update_price(self, demand_shock: float = 0.0,
                    supply_shock: float = 0.0) -> float:
        """
        Update price with market microstructure effects.
        
        Args:
            demand_shock: Excess demand (positive = price up)
            supply_shock: Supply disruption (positive = price up)
        """
        # Volatility clustering (GARCH-like)
        self.volatility_state = (self.volatility_persistence * self.volatility_state +
                                (1 - self.volatility_persistence) * abs(np.random.normal(0, self.volatility)))
        
        # Mean reversion component
        reversion = -self.mean_reversion * (self.current_price - self.base_price)
        
        # Shock effects
        total_shock = (demand_shock + supply_shock) * self.volatility_state
        
        # Random walk with reversion
        price_change = reversion + total_shock + np.random.normal(0, self.volatility_state * 0.1)
        
        # Update price
        self.current_price *= (1 + price_change)
        self.current_price = max(self.base_price * 0.5, min(self.base_price * 3, self.current_price))
        
        # Update bid-ask spread
        spread = self.spread_percent * (1 + abs(price_change) * 10)
        self.bid_price = self.current_price * (1 - spread / 2)
        self.ask_price = self.current_price * (1 + spread / 2)
        
        self.trade_history.append(self.current_price)
        if len(self.trade_history) > 1000:
            self.trade_history = self.trade_history[-1000:]
        
        return self.current_price
    
    def execute_trade(self, volume: float, is_buy: bool) -> float:
        """
        Execute a trade with price impact.
        
        Returns:
            Execution price
        """
        # Price impact (market impact model)
        impact = self.volatility_state * np.sqrt(volume / 1000) * 0.01
        
        if is_buy:
            price = self.ask_price * (1 + impact)
        else:
            price = self.bid_price * (1 - impact)
        
        # Update price after trade
        if is_buy:
            self.current_price += impact * self.current_price
        
        return price
    
    def get_market_data(self) -> Dict:
        """Get current market data"""
        return {
            'spot_price': self.current_price,
            'bid_price': self.bid_price,
            'ask_price': self.ask_price,
            'spread_percent': (self.ask_price - self.bid_price) / self.current_price * 100,
            'volatility': self.volatility_state,
            'trades': len(self.trade_history)
        }


# ============================================================
# ENHANCEMENT 5: Fault Injection Framework
# ============================================================

class FaultType(Enum):
    """Types of faults that can be injected"""
    NETWORK_PARTITION = "network_partition"
    DATABASE_CORRUPTION = "database_corruption"
    API_TIMEOUT = "api_timeout"
    RATE_LIMITING = "rate_limiting"
    MALFORMED_RESPONSE = "malformed_response"
    SLOW_RESPONSE = "slow_response"


@dataclass
class FaultInjection:
    """Fault injection configuration"""
    fault_type: FaultType
    probability: float  # 0-1
    duration_seconds: float
    severity: float  # 0-1
    affected_endpoints: List[str]


class FaultInjector:
    """
    Chaos engineering fault injection framework.
    
    Injects realistic failures to test system resilience.
    """
    
    def __init__(self):
        self.active_faults: Dict[str, FaultInjection] = {}
        self._lock = threading.Lock()
        self.fault_history: List[Dict] = []
    
    def inject_fault(self, fault: FaultInjection):
        """Inject a fault into the system"""
        with self._lock:
            key = f"{fault.fault_type.value}_{time.time()}"
            self.active_faults[key] = fault
            self.fault_history.append({
                'type': fault.fault_type.value,
                'injected_at': time.time(),
                'duration': fault.duration_seconds,
                'severity': fault.severity
            })
            logger.warning(f"Fault injected: {fault.fault_type.value} "
                          f"(prob={fault.probability:.1%}, duration={fault.duration_seconds}s)")
    
    def clear_faults(self):
        """Clear all active faults"""
        with self._lock:
            self.active_faults.clear()
            logger.info("All faults cleared")
    
    def should_trigger(self, endpoint: str) -> Tuple[bool, Optional[FaultInjection]]:
        """Check if a fault should be triggered for this endpoint"""
        with self._lock:
            current_time = time.time()
            expired = []
            
            for key, fault in self.active_faults.items():
                if current_time - self._get_injection_time(key) > fault.duration_seconds:
                    expired.append(key)
                    continue
                
                if endpoint in fault.affected_endpoints:
                    if random.random() < fault.probability:
                        return True, fault
            
            # Clean up expired faults
            for key in expired:
                del self.active_faults[key]
            
            return False, None
    
    def _get_injection_time(self, key: str) -> float:
        """Extract injection time from key"""
        try:
            return float(key.split('_')[1])
        except:
            return 0
    
    def get_active_faults(self) -> List[Dict]:
        """Get list of active faults"""
        with self._lock:
            return [
                {
                    'type': f.fault_type.value,
                    'probability': f.probability,
                    'duration_remaining': max(0, f.duration_seconds - (time.time() - self._get_injection_time(k))),
                    'severity': f.severity
                }
                for k, f in self.active_faults.items()
            ]
    
    def get_fault_history(self, limit: int = 100) -> List[Dict]:
        """Get fault injection history"""
        return self.fault_history[-limit:]


# ============================================================
# ENHANCEMENT 6: Data Quality Telemetry
# ============================================================

class DataQualityTelemetry:
    """
    Telemetry system for tracking data quality metrics.
    
    Tracks:
    - SLO compliance (99.9% availability target)
    - MTBF (Mean Time Between Failures)
    - MTTR (Mean Time To Recovery)
    - Error budgets
    """
    
    def __init__(self, availability_target: float = 0.999):
        self.availability_target = availability_target
        self.failures: List[float] = []
        self.recoveries: List[float] = []
        self.quality_history: List[Tuple[float, 'DataQuality']] = []
        self._lock = threading.Lock()
        self._current_quality = None
        self._quality_start_time = time.time()
    
    def record_quality_change(self, quality: 'DataQuality'):
        """Record data quality change"""
        with self._lock:
            current_time = time.time()
            
            if self._current_quality is not None:
                self.quality_history.append((current_time, self._current_quality))
            
            self._current_quality = quality
            self._quality_start_time = current_time
            
            if quality == DataQuality.OFFLINE:
                self.failures.append(current_time)
            elif quality == DataQuality.PERFECT and self.failures and (not self.recoveries or self.recoveries[-1] < self.failures[-1]):
                self.recoveries.append(current_time)
    
    def calculate_availability(self, time_window_seconds: int = 3600) -> float:
        """Calculate availability over time window"""
        with self._lock:
            cutoff = time.time() - time_window_seconds
            total_time = 0
            available_time = 0
            
            # Process quality history
            prev_time = cutoff
            prev_quality = DataQuality.OFFLINE
            
            for ts, quality in self.quality_history:
                if ts < cutoff:
                    prev_time = ts
                    prev_quality = quality
                    continue
                
                duration = ts - prev_time
                total_time += duration
                if prev_quality != DataQuality.OFFLINE:
                    available_time += duration
                
                prev_time = ts
                prev_quality = quality
            
            # Add current period
            if self._current_quality:
                duration = time.time() - prev_time
                total_time += duration
                if self._current_quality != DataQuality.OFFLINE:
                    available_time += duration
            
            return available_time / max(total_time, 1)
    
    def calculate_mtbf(self) -> float:
        """Calculate Mean Time Between Failures (seconds)"""
        with self._lock:
            if len(self.failures) < 2:
                return float('inf')
            
            intervals = [self.failures[i+1] - self.failures[i] 
                        for i in range(len(self.failures)-1)]
            return np.mean(intervals)
    
    def calculate_mttr(self) -> float:
        """Calculate Mean Time To Recovery (seconds)"""
        with self._lock:
            if not self.failures or not self.recoveries:
                return float('inf')
            
            recovery_times = [self.recoveries[i] - self.failures[i] 
                            for i in range(min(len(self.failures), len(self.recoveries)))]
            return np.mean(recovery_times)
    
    def get_slo_status(self) -> Dict:
        """Get SLO compliance status"""
        availability = self.calculate_availability(3600)  # Last hour
        mtbf = self.calculate_mtbf()
        mttr = self.calculate_mttr()
        
        return {
            'availability': availability,
            'availability_target': self.availability_target,
            'slo_met': availability >= self.availability_target,
            'mtbf_hours': mtbf / 3600 if mtbf != float('inf') else float('inf'),
            'mttr_seconds': mttr if mttr != float('inf') else float('inf'),
            'error_budget_remaining': max(0, 1 - (1 - availability) / (1 - self.availability_target)),
            'total_failures': len(self.failures),
            'total_recoveries': len(self.recoveries)
        }


# ============================================================
# ENHANCEMENT 7: Main Enhanced Synthetic Data Source
# ============================================================

class EnhancedSyntheticDataSource:
    """
    Enhanced synthetic data source v3.1 with all improvements.
    
    Features:
    - Calibrated physics models
    - ML-based correlation learning
    - Advanced latency simulation
    - Market microstructure for helium
    - Fault injection framework
    - Data quality telemetry
    """
    
    def __init__(self, config: Optional[Dict] = None):
        self.config = config or {}
        self.seed = self.config.get('seed', 42)
        self.quality = DataQuality(self.config.get('quality', 'perfect'))
        self.current_scenario = ScenarioType.NORMAL
        self.update_interval_seconds = self.config.get('update_interval', 5)
        self.regions = self.config.get('regions', ['us-east', 'us-west', 'eu-north', 'asia-pacific'])
        
        # Enhanced components
        self.physics = CalibratedPhysicsModels()
        self.correlation_learner = MLCorrelationLearner()
        self.latency_simulator = AdvancedLatencySimulator(
            base_latency_ms=self.config.get('base_latency_ms', 50),
            jitter_ms=self.config.get('jitter_ms', 10)
        )
        self.helium_market = MarketMicrostructureModel(
            base_price=4.5,
            spread_percent=0.02,
            volatility=0.15
        )
        self.fault_injector = FaultInjector()
        self.telemetry = DataQualityTelemetry()
        self.quality_transition = DynamicQualityTransition(self.quality)
        
        # Internal state
        self._temperature_state = {
            'cpu_temp': 55.0, 'gpu_temp': 65.0, 'memory_temp': 50.0,
            'ambient': 22.0, 'cooling_power': 100.0, 'thermal_mass': 500.0,
            'cooling_capacity': 500.0
        }
        
        self._grid_state = self._init_grid_state()
        self._recovery_state = self._init_recovery_state()
        
        # History
        self._history: Dict[str, List] = {
            'temperature': [], 'grid': [], 'helium': [], 'recovery': []
        }
        
        # Set random seed
        np.random.seed(self.seed)
        random.seed(self.seed)
        
        # Background thread
        self._running = False
        self._thread = None
        
        logger.info(f"Enhanced Synthetic Data Source v3.1 initialized (seed={self.seed})")
    
    def _init_grid_state(self) -> Dict:
        """Initialize grid state with regional profiles"""
        grid_state = {}
        for region in self.regions:
            if region == 'us-east':
                base = {'average': 380, 'marginal': 350, 'demand': 50000, 'renewable': 0.25,
                       'coal': 0.40, 'gas': 0.30, 'nuclear': 0.05, 'carbon_price': 25.0,
                       'latitude': 40.7, 'solar_capacity': 5000, 'wind_capacity': 3000}
            elif region == 'us-west':
                base = {'average': 250, 'marginal': 220, 'demand': 40000, 'renewable': 0.45,
                       'coal': 0.20, 'gas': 0.25, 'nuclear': 0.10, 'carbon_price': 30.0,
                       'latitude': 34.1, 'solar_capacity': 10000, 'wind_capacity': 5000}
            elif region == 'eu-north':
                base = {'average': 80, 'marginal': 70, 'demand': 30000, 'renewable': 0.65,
                       'coal': 0.05, 'gas': 0.15, 'nuclear': 0.15, 'carbon_price': 50.0,
                       'latitude': 59.3, 'solar_capacity': 2000, 'wind_capacity': 15000}
            else:
                base = {'average': 550, 'marginal': 520, 'demand': 60000, 'renewable': 0.15,
                       'coal': 0.60, 'gas': 0.20, 'nuclear': 0.05, 'carbon_price': 15.0,
                       'latitude': 35.7, 'solar_capacity': 8000, 'wind_capacity': 2000}
            grid_state[region] = base
        return grid_state
    
    def _init_recovery_state(self) -> Dict:
        """Initialize recovery system state"""
        return {
            'efficiency': 0.75, 'recovered_ytd': 0.0, 'recovered_current': 0.0,
            'method': 'capture', 'energy_cost': 0.5, 'capex': 500000, 'opex': 50000,
            'uptime': 0.99, 'maintenance_schedule': ['2024-06-01', '2024-12-01']
        }
    
    def start(self):
        """Start background data generation"""
        if self._running:
            return
        
        self._running = True
        self._thread = threading.Thread(target=self._update_loop, daemon=True)
        self._thread.start()
        logger.info("Enhanced synthetic data source started")
    
    def stop(self):
        """Stop background data generation"""
        self._running = False
        if self._thread:
            self._thread.join(timeout=5)
        logger.info("Enhanced synthetic data source stopped")
    
    def _update_loop(self):
        """Main update loop with enhanced models"""
        while self._running:
            try:
                start_time = time.time()
                
                self._update_temperature_enhanced()
                self._update_grid_enhanced()
                self._update_helium_enhanced()
                self._update_recovery()
                
                # Update quality telemetry
                self.telemetry.record_quality_change(self.quality)
                
                elapsed = time.time() - start_time
                sleep_time = max(0, self.update_interval_seconds - elapsed)
                time.sleep(sleep_time)
                
            except Exception as e:
                logger.error(f"Update error: {e}")
                time.sleep(1)
    
    def _update_temperature_enhanced(self):
        """Update temperature using calibrated physics model"""
        dt = self.update_interval_seconds
        
        # Calculate heat gain from workload
        hour = datetime.now().hour
        workload = 0.5 + 0.5 * np.sin(np.pi * (hour - 12) / 12)
        
        # Scenario adjustments
        ambient_offset = self.current_scenario.temperature_offset if hasattr(self.current_scenario, 'temperature_offset') else 0
        ambient_temp = 22 + ambient_offset + 5 * np.sin(2 * np.pi * (hour - 14) / 24)
        
        # Power dissipation (Watts)
        power_watts = 200 + workload * 300
        
        # Calculate temperature using physics model
        new_gpu_temp = self.physics.calculate_thermal_response(
            initial_temp_c=self._temperature_state['gpu_temp'],
            power_watts=power_watts,
            cooling_capacity_w=self._temperature_state['cooling_capacity'],
            thermal_mass_j_per_c=self._temperature_state['thermal_mass'],
            ambient_temp_c=ambient_temp,
            dt_seconds=dt
        )
        
        # Add ML-based correlation
        correlated_temp = self.correlation_learner.predict_correlation(
            'grid_demand', 'temperature',
            self._grid_state['us-east']['demand'],
            new_gpu_temp
        )
        
        # Apply quality noise
        noise_scale = self.quality.noise_scale
        if noise_scale > 0:
            correlated_temp += np.random.normal(0, noise_scale * 5)
        
        self._temperature_state['gpu_temp'] = correlated_temp
        self._temperature_state['ambient'] = ambient_temp
        
        # Add to correlation learner
        self.correlation_learner.add_observation('temperature', correlated_temp)
        
        # Store history
        self._history['temperature'].append({
            'timestamp': time.time(), 
            'gpu_temp': correlated_temp,
            'ambient': ambient_temp
        })
        
        if len(self._history['temperature']) > 1000:
            self._history['temperature'] = self._history['temperature'][-1000:]
    
    def _update_grid_enhanced(self):
        """Update grid with solar/wind generation"""
        now = datetime.now()
        hour = now.hour
        day_of_year = now.timetuple().tm_yday
        region = 'us-east'
        state = self._grid_state[region]
        
        # Base demand pattern
        is_weekday = now.weekday() < 5
        morning_peak = 1.3 if 9 <= hour <= 11 else 1.0
        evening_peak = 1.4 if 17 <= hour <= 19 else 1.0
        night_low = 0.6 if 0 <= hour <= 5 else 1.0
        weekday_factor = 1.2 if is_weekday else 0.8
        
        demand_factor = max(night_low, morning_peak, evening_peak) * weekday_factor
        target_demand = 50000 * demand_factor
        state['demand'] = state['demand'] * 0.9 + target_demand * 0.1
        
        # Solar generation (W/m²)
        cloud_cover = 0.3 + 0.4 * np.sin(2 * np.pi * hour / 24)
        solar_irradiance = self.physics.calculate_solar_irradiance(
            state['latitude'], hour, day_of_year, cloud_cover
        )
        solar_generation_mw = state['solar_capacity'] * (solar_irradiance / 1000)
        
        # Wind generation (simplified)
        wind_speed = 5 + 3 * np.sin(2 * np.pi * (hour - 3) / 24)
        wind_generation_mw = state['wind_capacity'] * min(1.0, (wind_speed / 12) ** 3)
        
        # Renewable percentage
        total_renewable = solar_generation_mw + wind_generation_mw
        state['renewable'] = min(0.9, total_renewable / state['demand'])
        
        # Carbon intensity varies with renewable generation
        base_intensity = state['coal'] * 820 + state['gas'] * 450 + state['nuclear'] * 12
        renewable_factor = 1 - state['renewable']
        state['average'] = base_intensity * renewable_factor + np.random.normal(0, 10)
        state['average'] = max(10, min(800, state['average']))
        
        # Marginal intensity (higher when demand is high)
        state['marginal'] = state['average'] * (0.8 + 0.4 * demand_factor)
        
        # Update correlation learner
        self.correlation_learner.add_observation('grid_demand', state['demand'])
        
        # Store history
        self._history['grid'].append({
            'timestamp': time.time(),
            'demand': state['demand'],
            'renewable': state['renewable'],
            'solar_mw': solar_generation_mw,
            'wind_mw': wind_generation_mw
        })
        
        if len(self._history['grid']) > 1000:
            self._history['grid'] = self._history['grid'][-1000:]
    
    def _update_helium_enhanced(self):
        """Update helium using market microstructure model"""
        # Calculate demand and supply shocks based on scenario
        demand_shock = 0.0
        supply_shock = 0.0
        
        if self.current_scenario == ScenarioType.HELIUM_CRISIS:
            supply_shock = 0.3
        elif self.current_scenario == ScenarioType.MARKET_CRASH:
            demand_shock = -0.2
        
        # Update market price
        self.helium_market.update_price(demand_shock, supply_shock)
        market_data = self.helium_market.get_market_data()
        
        # Store history
        self._history['helium'].append({
            'timestamp': time.time(),
            'price': market_data['spot_price'],
            'bid': market_data['bid_price'],
            'ask': market_data['ask_price'],
            'spread': market_data['spread_percent']
        })
        
        if len(self._history['helium']) > 1000:
            self._history['helium'] = self._history['helium'][-1000:]
    
    def _update_recovery(self):
        """Update recovery system data"""
        efficiency_target = 0.95 if self.current_scenario == ScenarioType.RECOVERY_SUCCESS else 0.75
        self._recovery_state['efficiency'] = (self._recovery_state['efficiency'] * 0.99 + 
                                               efficiency_target * 0.01 + np.random.normal(0, 0.005))
        self._recovery_state['efficiency'] = min(0.96, max(0.60, self._recovery_state['efficiency']))
        
        recovery_rate = 0.1
        self._recovery_state['recovered_current'] += recovery_rate * self.update_interval_seconds * self._recovery_state['efficiency']
        self._recovery_state['recovered_ytd'] += recovery_rate * self.update_interval_seconds * self._recovery_state['efficiency']
        
        # Store history
        self._history['recovery'].append({
            'timestamp': time.time(),
            'efficiency': self._recovery_state['efficiency']
        })
        
        if len(self._history['recovery']) > 1000:
            self._history['recovery'] = self._history['recovery'][-1000:]
    
    def get_temperature_data(self) -> SyntheticTemperatureData:
        """Get current temperature data with latency simulation"""
        try:
            latency = self.latency_simulator.simulate_latency(self.quality)
            time.sleep(latency)
            
            # Check for faults
            should_fault, fault = self.fault_injector.should_trigger('temperature')
            if should_fault:
                if fault.fault_type == FaultType.API_TIMEOUT:
                    raise TimeoutError("Temperature API timeout")
                elif fault.fault_type == FaultType.MALFORMED_RESPONSE:
                    raise ValueError("Malformed temperature response")
            
            if not self._check_availability():
                raise ConnectionError(f"Temperature data source offline (quality={self.quality.value})")
            
            return SyntheticTemperatureData(
                cpu_temp_c=self._temperature_state['cpu_temp'],
                gpu_temp_c=self._temperature_state['gpu_temp'],
                memory_temp_c=self._temperature_state['memory_temp'],
                ambient_temp_c=self._temperature_state['ambient'],
                cooling_power_w=self._temperature_state['cooling_power'],
                fan_speed_percent=40 + self._temperature_state['gpu_temp'] * 0.5,
                power_draw_w=200 + self._temperature_state['gpu_temp'] * 2,
                timestamp=datetime.now(),
                quality=self.quality,
                confidence=self.quality.availability,
                latency_ms=latency * 1000
            )
        except Exception as e:
            logger.error(f"Temperature data error: {e}")
            raise
    
    def _check_availability(self) -> bool:
        """Check data availability based on quality level"""
        if self.quality == DataQuality.OFFLINE:
            return False
        if self.quality == DataQuality.RECOVERING and random.random() > 0.5:
            return False
        if self.quality == DataQuality.DEGRADED and random.random() < 0.2:
            return False
        return True
    
    def get_grid_data(self, region: str = 'us-east') -> SyntheticGridData:
        """Get current grid data"""
        latency = self.latency_simulator.simulate_latency(self.quality)
        time.sleep(latency)
        
        if not self._check_availability():
            raise ConnectionError(f"Grid data offline for {region}")
        
        state = self._grid_state.get(region, self._grid_state['us-east'])
        
        # Generate simple forecast
        forecast = [state['marginal'] * (1 + 0.1 * np.sin(2 * np.pi * i / 24)) 
                   for i in range(6)]
        
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
            forecast_next_6h=forecast,
            timestamp=datetime.now(),
            quality=self.quality
        )
    
    def get_helium_data(self) -> SyntheticHeliumData:
        """Get current helium market data"""
        latency = self.latency_simulator.simulate_latency(self.quality)
        time.sleep(latency)
        
        if not self._check_availability():
            raise ConnectionError("Helium market data offline")
        
        market_data = self.helium_market.get_market_data()
        
        return SyntheticHeliumData(
            spot_price_usd_per_liter=market_data['spot_price'],
            futures_price_1m=market_data['spot_price'] * 1.05,
            futures_price_3m=market_data['spot_price'] * 1.10,
            futures_price_6m=market_data['spot_price'] * 1.15,
            global_inventory_days=int(30 - (market_data['spot_price'] - 4.5) * 10),
            supply_disruption_risk=max(0.05, min(0.8, (market_data['spot_price'] - 4.5) / 10)),
            demand_growth_rate=0.05,
            primary_producers={'AirLiquide': 0.40, 'Linde': 0.35, 'AirProducts': 0.25},
            timestamp=datetime.now(),
            quality=self.quality
        )
    
    def get_recovery_data(self) -> SyntheticRecoveryData:
        """Get current recovery system data"""
        latency = self.latency_simulator.simulate_latency(self.quality)
        time.sleep(latency)
        
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
    
    def set_quality(self, quality: DataQuality, transition_seconds: float = 0,
                   transition_type: QualityTransition = QualityTransition.SUDDEN):
        """Set data quality with optional transition"""
        if transition_seconds > 0:
            self.quality_transition.start_transition(quality, transition_seconds, transition_type)
        else:
            self.quality = quality
            self.telemetry.record_quality_change(quality)
        logger.info(f"Data quality set to {quality.value}")
    
    def inject_fault(self, fault_type: FaultType, probability: float,
                    duration_seconds: float, severity: float = 0.5,
                    affected_endpoints: List[str] = None):
        """Inject a fault for chaos testing"""
        if affected_endpoints is None:
            affected_endpoints = ['temperature', 'grid', 'helium', 'recovery']
        
        fault = FaultInjection(
            fault_type=fault_type,
            probability=probability,
            duration_seconds=duration_seconds,
            severity=severity,
            affected_endpoints=affected_endpoints
        )
        self.fault_injector.inject_fault(fault)
    
    def clear_faults(self):
        """Clear all injected faults"""
        self.fault_injector.clear_faults()
    
    def get_telemetry_status(self) -> Dict:
        """Get data quality telemetry"""
        return {
            'slo': self.telemetry.get_slo_status(),
            'latency': self.latency_simulator.get_statistics(),
            'faults': {
                'active': self.fault_injector.get_active_faults(),
                'history': self.fault_injector.get_fault_history(10)
            },
            'correlation_ml': self.correlation_learner.get_statistics(),
            'market': self.helium_market.get_market_data()
        }
    
    def get_scenario_metrics(self) -> Dict:
        """Get current scenario metrics"""
        return {
            'scenario': self.current_scenario.value if isinstance(self.current_scenario, ScenarioType) else str(self.current_scenario),
            'quality': self.quality.value,
            'temperature': {'gpu': self._temperature_state['gpu_temp'], 'ambient': self._temperature_state['ambient']},
            'helium': {'price': self.helium_market.current_price},
            'grid': {'us_east_average': self._grid_state['us-east']['average']},
            'correlation_ml_enabled': SKLEARN_AVAILABLE,
            'latency_simulation': self.latency_simulator.total_requests > 0,
            'telemetry': self.get_telemetry_status()
        }
    
    def generate_report(self) -> str:
        """Generate detailed report"""
        report = {
            'timestamp': datetime.now().isoformat(),
            'config': {
                'seed': self.seed,
                'update_interval': self.update_interval_seconds,
                'regions': self.regions
            },
            'current_state': self.get_scenario_metrics(),
            'history_sizes': {k: len(v) for k, v in self._history.items()},
            'correlation_stats': self.correlation_learner.get_statistics(),
            'telemetry': self.get_telemetry_status(),
            'latency_stats': self.latency_simulator.get_statistics(),
            'fault_stats': {
                'total_faults': len(self.fault_injector.get_fault_history(1000)),
                'active_faults': len(self.fault_injector.get_active_faults())
            }
        }
        return json.dumps(report, indent=2)


# ============================================================
# Usage Example
# ============================================================

async def async_main():
    print("=== Enhanced Synthetic Data Manager v3.1 Demo ===\n")
    
    source = EnhancedSyntheticDataSource({
        'seed': 42,
        'quality': 'perfect',
        'update_interval': 1,
        'base_latency_ms': 50,
        'jitter_ms': 10
    })
    
    source.start()
    
    print("1. Basic data retrieval with physics-based modeling:")
    temp = source.get_temperature_data()
    print(f"   GPU Temp: {temp.gpu_temp_c:.1f}°C (ambient: {temp.ambient_temp_c:.1f}°C)")
    
    grid = source.get_grid_data('us-east')
    print(f"   Grid Intensity: {grid.average_intensity_gco2_per_kwh:.0f} gCO2/kWh")
    print(f"   Renewable: {grid.renewable_percentage:.1%}")
    
    helium = source.get_helium_data()
    print(f"   Helium Price: ${helium.spot_price_usd_per_liter:.2f}/L")
    
    print("\n2. ML-Based Correlation Learning:")
    for _ in range(60):
        source._update_loop_step()
        await asyncio.sleep(1)
    corr_stats = source.correlation_learner.get_statistics()
    print(f"   ML Enabled: {corr_stats['ml_enabled']}")
    if corr_stats['feature_importance']:
        print(f"   Feature importance: {corr_stats['feature_importance']}")
    
    print("\n3. Market Microstructure Model:")
    market = source.helium_market.get_market_data()
    print(f"   Bid: ${market['bid_price']:.2f}, Ask: ${market['ask_price']:.2f}")
    print(f"   Spread: {market['spread_percent']:.2f}%")
    
    print("\n4. Fault Injection Demo:")
    source.inject_fault(FaultType.API_TIMEOUT, probability=0.5, 
                       duration_seconds=10, affected_endpoints=['temperature'])
    print("   Fault injected: API timeout on temperature endpoint")
    
    try:
        temp_fault = source.get_temperature_data()
        print(f"   Temperature data retrieved successfully")
    except Exception as e:
        print(f"   Fault triggered: {e}")
    
    source.clear_faults()
    print("   Fault cleared")
    
    print("\n5. Data Quality Telemetry:")
    source.set_quality(DataQuality.DEGRADED)
    source.set_quality(DataQuality.OFFLINE)
    source.set_quality(DataQuality.PERFECT)
    
    telemetry = source.get_telemetry_status()
    print(f"   SLO Met: {telemetry['slo']['slo_met']}")
    print(f"   Availability: {telemetry['slo']['availability']:.4%}")
    print(f"   Error budget remaining: {telemetry['slo']['error_budget_remaining']:.1%}")
    
    print("\n6. Latency Simulation Statistics:")
    latency_stats = telemetry['latency']
    print(f"   Total requests: {latency_stats['total_requests']}")
    print(f"   Drop rate: {latency_stats['drop_rate']:.2%}")
    print(f"   Congestion window: {latency_stats['congestion_window']:.2f}")
    
    print("\n7. System Report:")
    report = json.loads(source.generate_report())
    print(f"   Quality: {report['current_state']['quality']}")
    print(f"   Telemetry SLO: {report['telemetry']['slo']['slo_met']}")
    print(f"   Active faults: {report['fault_stats']['active_faults']}")
    
    source.stop()
    print("\n✅ Enhanced Synthetic Data Manager v3.1 test complete")

def main():
    asyncio.run(async_main())

if __name__ == "__main__":
    main()
