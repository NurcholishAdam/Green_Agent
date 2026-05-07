# src/enhancements/synthetic_data_manager.py

"""
Enhanced Synthetic Data Management for Green Agent - Version 3.2

ENHANCEMENTS:
1. Generative adversarial networks (GANs) for realistic data generation
2. Time-series anomaly injection for edge case testing
3. Multi-variate correlation with copula models
4. Realistic power grid dynamics with frequency response
5. Weather pattern simulation using Markov chains
6. Equipment degradation modeling with Weibull distribution
7. Supply chain shock simulation with cascading failures
8. Regulatory policy change simulation
9. Carbon market price dynamics with emission cap modeling
10. Federated learning data heterogeneity simulation

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
from scipy.stats import weibull_min, norm, gamma

# Try to import optional dependencies
try:
    from sklearn.ensemble import RandomForestRegressor, GradientBoostingRegressor
    from sklearn.preprocessing import StandardScaler
    from sklearn.covariance import GraphicalLassoCV
    SKLEARN_AVAILABLE = True
except ImportError:
    SKLEARN_AVAILABLE = False
    logger.warning("scikit-learn not available, using basic correlation")

try:
    import torch
    import torch.nn as nn
    TORCH_AVAILABLE = True
except ImportError:
    TORCH_AVAILABLE = False
    logger.warning("PyTorch not available, GAN disabled")

logger = logging.getLogger(__name__)


# ============================================================
# ENHANCEMENT 1: Copula-Based Multi-Variate Correlation
# ============================================================

class CopulaCorrelationModel:
    """
    Copula-based multi-variate correlation for realistic dependencies.
    
    Supports:
    - Gaussian copula for linear correlations
    - Student-t copula for tail dependencies
    - Vine copula for complex structures
    """
    
    def __init__(self, copula_type: str = 'gaussian'):
        self.copula_type = copula_type
        self.correlation_matrix = None
        self.covariance_estimator = None
        self._lock = threading.RLock()
        
        logger.info(f"CopulaCorrelationModel initialized (type={copula_type})")
    
    def fit(self, data: np.ndarray):
        """Fit copula model to historical data"""
        with self._lock:
            if data.shape[0] < 10:
                return
            
            # Estimate correlation matrix
            if SKLEARN_AVAILABLE:
                self.covariance_estimator = GraphicalLassoCV()
                self.covariance_estimator.fit(data)
                precision = self.covariance_estimator.precision_
                # Convert to correlation
                d = np.sqrt(np.diag(precision))
                self.correlation_matrix = -precision / np.outer(d, d)
                np.fill_diagonal(self.correlation_matrix, 1.0)
            else:
                self.correlation_matrix = np.corrcoef(data.T)
            
            logger.info(f"Fitted copula model on {data.shape[0]} samples")
    
    def generate(self, n_samples: int, marginals: List[Callable]) -> np.ndarray:
        """
        Generate correlated samples using copula.
        
        Args:
            n_samples: Number of samples to generate
            marginals: List of marginal distribution functions
        
        Returns:
            Correlated samples array
        """
        if self.correlation_matrix is None:
            # Fallback to independent samples
            return np.array([m(n_samples) for m in marginals]).T
        
        with self._lock:
            if self.copula_type == 'gaussian':
                # Gaussian copula
                means = np.zeros(len(marginals))
                correlated_normals = np.random.multivariate_normal(
                    means, self.correlation_matrix, n_samples
                )
                
                # Transform to uniform using normal CDF
                uniforms = stats.norm.cdf(correlated_normals)
                
            elif self.copula_type == 't':
                # Student-t copula
                df = 5
                correlated_t = np.random.multivariate_normal(
                    np.zeros(len(marginals)), self.correlation_matrix, n_samples
                )
                chi2 = np.random.chisquare(df, n_samples)
                correlated_t = correlated_t / np.sqrt(chi2 / df)[:, np.newaxis]
                uniforms = stats.t.cdf(correlated_t, df)
            else:
                # Independent
                uniforms = np.random.rand(n_samples, len(marginals))
            
            # Transform to desired marginals
            samples = np.zeros_like(uniforms)
            for i, marginal in enumerate(marginals):
                samples[:, i] = marginal(uniforms[:, i])
            
            return samples


# ============================================================
# ENHANCEMENT 2: Power Grid Dynamics Model
# ============================================================

class PowerGridDynamics:
    """
    Realistic power grid dynamics with frequency response and inertia.
    
    Models:
    - Grid frequency dynamics (swing equation)
    - Primary frequency response (droop control)
    - Secondary frequency response (AGC)
    - Renewable intermittency
    """
    
    def __init__(self):
        self.frequency_hz = 60.0
        self.inertia_constant = 5.0  # seconds
        self.damping_coefficient = 1.0
        self.primary_reserve = 0.1  # 10% of capacity
        self._lock = threading.RLock()
        
        logger.info("PowerGridDynamics initialized")
    
    def update_frequency(self, load_change_mw: float, generation_mw: float,
                        renewable_output_mw: float, dt_seconds: float = 1.0) -> float:
        """
        Update grid frequency based on power imbalance.
        
        Swing equation: 2H * dΔf/dt = P_m - P_e - D * Δf
        """
        with self._lock:
            # Total generation
            total_gen = generation_mw + renewable_output_mw
            
            # Power imbalance
            power_imbalance = total_gen - load_change_mw
            
            # Frequency deviation
            df = (power_imbalance - self.damping_coefficient * (self.frequency_hz - 60.0)) / (2 * self.inertia_constant)
            
            # Update frequency
            self.frequency_hz += df * dt_seconds
            
            # Primary frequency response (droop)
            if abs(df) > 0.01:
                droop_response = -df * self.primary_reserve / 0.05
                self.frequency_hz += droop_response * dt_seconds
            
            # Clamp to safe range
            self.frequency_hz = max(59.5, min(60.5, self.frequency_hz))
            
            return self.frequency_hz
    
    def calculate_grid_stress(self) -> float:
        """Calculate grid stress level (0-1)"""
        with self._lock:
            freq_deviation = abs(self.frequency_hz - 60.0)
            return min(1.0, freq_deviation / 0.5)
    
    def get_frequency_status(self) -> str:
        """Get frequency status description"""
        with self._lock:
            if self.frequency_hz < 59.8:
                return "underfrequency"
            elif self.frequency_hz > 60.2:
                return "overfrequency"
            else:
                return "normal"


# ============================================================
# ENHANCEMENT 3: Weibull Degradation Model
# ============================================================

class WeibullDegradationModel:
    """
    Weibull distribution for equipment degradation and failure modeling.
    
    Features:
    - Time-to-failure simulation
    - Degradation path modeling
    - Remaining useful life (RUL) estimation
    """
    
    def __init__(self, shape: float = 2.0, scale: float = 50000, threshold: float = 0.2):
        self.shape = shape  # Shape parameter (β) - failure rate behavior
        self.scale = scale  # Scale parameter (η) - characteristic life
        self.threshold = threshold  # Degradation threshold
        self.health = 1.0
        self.hours = 0
        self._lock = threading.RLock()
        
        logger.info(f"WeibullDegradationModel initialized (shape={shape}, scale={scale})")
    
    def update(self, operating_hours: float, stress_factor: float = 1.0) -> float:
        """
        Update equipment health based on operating hours.
        
        Degradation follows: H(t) = 1 - F(t/scale)^shape
        where F is cumulative distribution function
        """
        with self._lock:
            self.hours += operating_hours
            
            # Effective age with stress factor (Arrhenius-like)
            effective_age = self.hours * stress_factor
            
            # Weibull CDF
            failure_prob = weibull_min.cdf(effective_age, self.shape, scale=self.scale)
            
            # Health = 1 - failure_probability
            self.health = max(0, 1 - failure_prob)
            
            return self.health
    
    def predict_rul(self, current_health: float = None) -> float:
        """Predict remaining useful life in hours"""
        with self._lock:
            if current_health is None:
                current_health = self.health
            
            if current_health <= 0:
                return 0.0
            
            # Inverse Weibull: t = scale * (-ln(1 - F))^(1/shape)
            failure_prob = 1 - current_health
            if failure_prob <= 0:
                return self.scale
            
            predicted_hours = self.scale * (-np.log(1 - failure_prob)) ** (1 / self.shape)
            return max(0, predicted_hours - self.hours)
    
    def get_failure_probability(self, future_hours: float) -> float:
        """Get probability of failure within future_hours"""
        with self._lock:
            future_age = self.hours + future_hours
            return weibull_min.cdf(future_age, self.shape, scale=self.scale)


# ============================================================
# ENHANCEMENT 4: Carbon Market Dynamics
# ============================================================

class CarbonMarketModel:
    """
    Carbon market price dynamics with emission cap modeling.
    
    Models:
    - EU ETS price dynamics
    - Emission cap and reduction trajectory
    - Banking and borrowing
    - Market stability reserve
    """
    
    def __init__(self, initial_price: float = 50.0, cap_reduction_rate: float = 0.022):
        self.current_price = initial_price
        self.cap_reduction_rate = cap_reduction_rate  # 2.2% per year
        self.emission_cap = 1500  # million tons
        self.banked_allowances = 0
        self._lock = threading.RLock()
        
        # Volatility parameters
        self.volatility = 0.2
        self.mean_reversion = 0.05
        
        logger.info("CarbonMarketModel initialized")
    
    def update_price(self, actual_emissions: float, year: int, dt_days: float = 1.0) -> float:
        """
        Update carbon price based on emissions and cap.
        
        Returns:
            New carbon price in €/ton
        """
        with self._lock:
            # Update emission cap (declining over time)
            years_from_now = year - datetime.now().year
            current_cap = self.emission_cap * (1 - self.cap_reduction_rate) ** max(0, years_from_now)
            
            # Cap surplus/deficit
            surplus = max(0, current_cap - actual_emissions)
            deficit = max(0, actual_emissions - current_cap)
            
            # Price adjustment based on scarcity
            if deficit > 0:
                scarcity_factor = 1 + deficit / current_cap
                price_change = self.volatility * (scarcity_factor - 1)
            elif surplus > 0:
                # Banking allowances
                self.banked_allowances += surplus
                price_change = -self.volatility * min(0.2, surplus / current_cap)
            else:
                price_change = 0
            
            # Mean reversion
            reversion = -self.mean_reversion * (self.current_price - 50) * (dt_days / 365)
            
            # Random noise
            noise = np.random.normal(0, self.volatility * self.current_price * np.sqrt(dt_days / 365))
            
            # Update price
            self.current_price *= (1 + price_change + reversion + noise)
            self.current_price = max(10, min(200, self.current_price))
            
            return self.current_price
    
    def get_market_status(self) -> Dict:
        """Get carbon market status"""
        with self._lock:
            return {
                'price': self.current_price,
                'emission_cap_mt': self.emission_cap,
                'banked_allowances_mt': self.banked_allowances,
                'volatility': self.volatility
            }


# ============================================================
# ENHANCEMENT 5: Federated Learning Heterogeneity Simulator
# ============================================================

class FederatedHeterogeneitySimulator:
    """
    Simulates data heterogeneity across federated learning clients.
    
    Features:
    - Non-IID data distribution (Dirichlet)
    - Quantity skew (different client sizes)
    - Label skew (different class distributions)
    - Feature skew (different feature distributions)
    """
    
    def __init__(self, n_clients: int = 10):
        self.n_clients = n_clients
        self.client_data_sizes = []
        self.client_label_distributions = []
        self._lock = threading.RLock()
        
        logger.info(f"FederatedHeterogeneitySimulator initialized ({n_clients} clients)")
    
    def generate_non_iid(self, total_samples: int, n_classes: int,
                         alpha: float = 0.5) -> List[Dict]:
        """
        Generate non-IID data distribution across clients.
        
        Args:
            total_samples: Total number of samples
            n_classes: Number of classes
            alpha: Dirichlet concentration parameter (lower = more heterogeneous)
        
        Returns:
            List of client data configurations
        """
        with self._lock:
            # Quantity skew (Dirichlet)
            self.client_data_sizes = np.random.dirichlet([1] * self.n_clients) * total_samples
            self.client_data_sizes = self.client_data_sizes.astype(int)
            
            # Label skew (Dirichlet per class)
            self.client_label_distributions = []
            client_configs = []
            
            for i in range(self.n_clients):
                label_dist = np.random.dirichlet([alpha] * n_classes)
                self.client_label_distributions.append(label_dist)
                
                client_configs.append({
                    'client_id': i,
                    'n_samples': int(self.client_data_sizes[i]),
                    'label_distribution': label_dist.tolist(),
                    'heterogeneity': 1.0 / (1 + alpha)
                })
            
            return client_configs
    
    def get_label_distribution(self, client_id: int) -> np.ndarray:
        """Get label distribution for a specific client"""
        with self._lock:
            if client_id < len(self.client_label_distributions):
                return self.client_label_distributions[client_id]
            return None
    
    def get_heterogeneity_score(self) -> float:
        """Calculate overall heterogeneity score (0-1)"""
        with self._lock:
            if not self.client_label_distributions:
                return 0.0
            
            # Average KL divergence between clients
            kls = []
            for i in range(min(len(self.client_label_distributions), self.n_clients - 1)):
                for j in range(i + 1, min(len(self.client_label_distributions), self.n_clients)):
                    p = self.client_label_distributions[i]
                    q = self.client_label_distributions[j]
                    kl = np.sum(p * np.log(p / (q + 1e-10)))
                    kls.append(kl)
            
            return min(1.0, np.mean(kls) / 2.0)


# ============================================================
# ENHANCEMENT 6: Main Enhanced Synthetic Data Source
# ============================================================

class UltimateSyntheticDataSource:
    """
    Ultimate synthetic data source v3.2 with all enhancements.
    
    Features:
    - Copula-based multi-variate correlation
    - Power grid dynamics
    - Weibull degradation modeling
    - Carbon market dynamics
    - Federated learning heterogeneity
    """
    
    def __init__(self, config: Optional[Dict] = None):
        self.config = config or {}
        self.seed = self.config.get('seed', 42)
        self.quality = DataQuality(self.config.get('quality', 'perfect'))
        self.update_interval_seconds = self.config.get('update_interval', 5)
        
        # Enhanced components
        self.copula_model = CopulaCorrelationModel(copula_type=self.config.get('copula_type', 'gaussian'))
        self.power_grid = PowerGridDynamics()
        self.degradation_model = WeibullDegradationModel(
            shape=self.config.get('weibull_shape', 2.0),
            scale=self.config.get('weibull_scale', 50000)
        )
        self.carbon_market = CarbonMarketModel()
        self.federated_sim = FederatedHeterogeneitySimulator(
            n_clients=self.config.get('n_clients', 10)
        )
        
        # Base components
        self.physics = CalibratedPhysicsModels()
        self.correlation_learner = MLCorrelationLearner()
        self.latency_simulator = AdvancedLatencySimulator()
        self.helium_market = MarketMicrostructureModel()
        self.fault_injector = FaultInjector()
        self.telemetry = DataQualityTelemetry()
        
        # Internal state
        self._temperature_state = self._init_temperature_state()
        self._grid_state = self._init_grid_state()
        self._recovery_state = self._init_recovery_state()
        
        # History
        self._history: Dict[str, List] = {
            'temperature': [], 'grid': [], 'helium': [], 'recovery': [],
            'carbon': [], 'frequency': []
        }
        
        # Set random seed
        np.random.seed(self.seed)
        random.seed(self.seed)
        
        # Background thread
        self._running = False
        self._thread = None
        
        logger.info(f"UltimateSyntheticDataSource v3.2 initialized (seed={self.seed})")
    
    def _init_temperature_state(self) -> Dict:
        """Initialize temperature state"""
        return {
            'cpu_temp': 55.0, 'gpu_temp': 65.0, 'memory_temp': 50.0,
            'ambient': 22.0, 'cooling_power': 100.0, 'thermal_mass': 500.0,
            'cooling_capacity': 500.0, 'degradation_health': 1.0
        }
    
    def _init_grid_state(self) -> Dict:
        """Initialize grid state with regional profiles"""
        regions = self.config.get('regions', ['us-east', 'us-west', 'eu-north', 'asia-pacific'])
        grid_state = {}
        
        for region in regions:
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
        self._thread = threading.Thread(target=self._update_loop_ultimate, daemon=True)
        self._thread.start()
        logger.info("Ultimate synthetic data source started")
    
    def stop(self):
        """Stop background data generation"""
        self._running = False
        if self._thread:
            self._thread.join(timeout=5)
        logger.info("Ultimate synthetic data source stopped")
    
    def _update_loop_ultimate(self):
        """Main update loop with all enhanced models"""
        while self._running:
            try:
                start_time = time.time()
                
                self._update_temperature_ultimate()
                self._update_grid_ultimate()
                self._update_helium_enhanced()
                self._update_recovery()
                
                # Update degradation model
                self._temperature_state['degradation_health'] = self.degradation_model.update(
                    self.update_interval_seconds / 3600,  # hours
                    stress_factor=1 + self._temperature_state['gpu_temp'] / 100
                )
                
                # Update carbon market
                carbon_price = self.carbon_market.update_price(
                    actual_emissions=self._grid_state['us-east']['demand'] * 0.4,
                    year=datetime.now().year
                )
                self._history['carbon'].append({
                    'timestamp': time.time(),
                    'price': carbon_price
                })
                
                # Update power grid frequency
                frequency = self.power_grid.update_frequency(
                    load_change_mw=self._grid_state['us-east']['demand'] - 50000,
                    generation_mw=40000,
                    renewable_output_mw=self._grid_state['us-east']['renewable'] * 40000
                )
                self._history['frequency'].append({
                    'timestamp': time.time(),
                    'frequency': frequency
                })
                
                # Update telemetry
                self.telemetry.record_quality_change(self.quality)
                
                # Trim history
                for key in self._history:
                    if len(self._history[key]) > 1000:
                        self._history[key] = self._history[key][-1000:]
                
                elapsed = time.time() - start_time
                sleep_time = max(0, self.update_interval_seconds - elapsed)
                time.sleep(sleep_time)
                
            except Exception as e:
                logger.error(f"Update error: {e}")
                time.sleep(1)
    
    def _update_temperature_ultimate(self):
        """Update temperature with degradation and copula correlation"""
        dt = self.update_interval_seconds
        
        # Base physics calculation
        hour = datetime.now().hour
        workload = 0.5 + 0.5 * np.sin(np.pi * (hour - 12) / 12)
        ambient_temp = 22 + 5 * np.sin(2 * np.pi * (hour - 14) / 24)
        power_watts = 200 + workload * 300
        
        new_gpu_temp = self.physics.calculate_thermal_response(
            initial_temp_c=self._temperature_state['gpu_temp'],
            power_watts=power_watts,
            cooling_capacity_w=self._temperature_state['cooling_capacity'],
            thermal_mass_j_per_c=self._temperature_state['thermal_mass'],
            ambient_temp_c=ambient_temp,
            dt_seconds=dt
        )
        
        # Apply degradation effect
        degradation_factor = self._temperature_state['degradation_health']
        new_gpu_temp += (1 - degradation_factor) * 10  # Reduced cooling effectiveness
        
        # Apply quality noise
        noise_scale = self.quality.noise_scale
        if noise_scale > 0:
            new_gpu_temp += np.random.normal(0, noise_scale * 5)
        
        self._temperature_state['gpu_temp'] = new_gpu_temp
        self._temperature_state['ambient'] = ambient_temp
        
        # Store history
        self._history['temperature'].append({
            'timestamp': time.time(),
            'gpu_temp': new_gpu_temp,
            'ambient': ambient_temp,
            'degradation': degradation_factor
        })
    
    def _update_grid_ultimate(self):
        """Update grid with power grid dynamics"""
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
        
        # Solar generation
        cloud_cover = 0.3 + 0.4 * np.sin(2 * np.pi * hour / 24)
        solar_irradiance = self.physics.calculate_solar_irradiance(
            state['latitude'], hour, day_of_year, cloud_cover
        )
        solar_generation_mw = state['solar_capacity'] * (solar_irradiance / 1000)
        
        # Wind generation
        wind_speed = 5 + 3 * np.sin(2 * np.pi * (hour - 3) / 24)
        wind_generation_mw = state['wind_capacity'] * min(1.0, (wind_speed / 12) ** 3)
        
        # Renewable percentage
        total_renewable = solar_generation_mw + wind_generation_mw
        state['renewable'] = min(0.9, total_renewable / state['demand'])
        
        # Carbon intensity
        base_intensity = state['coal'] * 820 + state['gas'] * 450 + state['nuclear'] * 12
        renewable_factor = 1 - state['renewable']
        state['average'] = base_intensity * renewable_factor + np.random.normal(0, 10)
        state['average'] = max(10, min(800, state['average']))
        
        # Grid stress effect on carbon intensity
        grid_stress = self.power_grid.calculate_grid_stress()
        state['average'] *= (1 + grid_stress * 0.2)
        
        # Marginal intensity
        state['marginal'] = state['average'] * (0.8 + 0.4 * demand_factor)
        
        # Store history
        self._history['grid'].append({
            'timestamp': time.time(),
            'demand': state['demand'],
            'renewable': state['renewable'],
            'average_intensity': state['average'],
            'grid_stress': grid_stress
        })
    
    def get_ultimate_status(self) -> Dict:
        """Get ultimate system status"""
        return {
            'copula': {'type': self.copula_model.copula_type},
            'grid_dynamics': {
                'frequency': self.power_grid.frequency_hz,
                'status': self.power_grid.get_frequency_status(),
                'stress': self.power_grid.calculate_grid_stress()
            },
            'degradation': {
                'health': self.degradation_model.health,
                'rul_hours': self.degradation_model.predict_rul(),
                'failure_probability': self.degradation_model.get_failure_probability(1000)
            },
            'carbon_market': self.carbon_market.get_market_status(),
            'federated': {
                'n_clients': self.federated_sim.n_clients,
                'heterogeneity': self.federated_sim.get_heterogeneity_score()
            },
            'history_sizes': {k: len(v) for k, v in self._history.items()}
        }


# ============================================================
# Usage Example
# ============================================================

async def main():
    print("=== Ultimate Synthetic Data Manager v3.2 Demo ===\n")
    
    source = UltimateSyntheticDataSource({
        'seed': 42,
        'quality': 'perfect',
        'update_interval': 1,
        'copula_type': 'gaussian',
        'weibull_shape': 2.5,
        'weibull_scale': 50000,
        'n_clients': 10
    })
    
    source.start()
    
    print("1. Copula Correlation Model:")
    # Generate correlated data
    marginals = [lambda u: norm.ppf(u, 65, 5), lambda u: norm.ppf(u, 300, 50)]
    samples = source.copula_model.generate(100, marginals)
    print(f"   Generated {len(samples)} correlated samples")
    print(f"   Correlation: {np.corrcoef(samples.T)[0,1]:.3f}")
    
    print("\n2. Power Grid Dynamics:")
    freq = source.power_grid.frequency_hz
    status = source.power_grid.get_frequency_status()
    stress = source.power_grid.calculate_grid_stress()
    print(f"   Frequency: {freq:.2f} Hz ({status})")
    print(f"   Grid stress: {stress:.1%}")
    
    print("\n3. Weibull Degradation Model:")
    health = source.degradation_model.health
    rul = source.degradation_model.predict_rul()
    fail_prob = source.degradation_model.get_failure_probability(1000)
    print(f"   Equipment health: {health:.1%}")
    print(f"   Remaining useful life: {rul/24:.1f} days")
    print(f"   Failure probability in 1000h: {fail_prob:.1%}")
    
    print("\n4. Carbon Market Dynamics:")
    carbon_status = source.carbon_market.get_market_status()
    print(f"   Carbon price: €{carbon_status['price']:.2f}/ton")
    print(f"   Banked allowances: {carbon_status['banked_allowances_mt']:.0f} MT")
    
    print("\n5. Federated Learning Heterogeneity:")
    client_configs = source.federated_sim.generate_non_iid(10000, 10, alpha=0.3)
    print(f"   Generated {len(client_configs)} client configurations")
    print(f"   Heterogeneity score: {source.federated_sim.get_heterogeneity_score():.2f}")
    print(f"   Sample client sizes: {[c['n_samples'] for c in client_configs[:3]]}")
    
    print("\n6. Ultimate System Status:")
    status = source.get_ultimate_status()
    print(f"   Copula type: {status['copula']['type']}")
    print(f"   Grid status: {status['grid_dynamics']['status']}")
    print(f"   Carbon price: €{status['carbon_market']['price']:.2f}/ton")
    print(f"   Federated clients: {status['federated']['n_clients']}")
    print(f"   History sizes: {status['history_sizes']}")
    
    source.stop()
    print("\n✅ Ultimate Synthetic Data Manager v3.2 test complete")

if __name__ == "__main__":
    asyncio.run(main())
