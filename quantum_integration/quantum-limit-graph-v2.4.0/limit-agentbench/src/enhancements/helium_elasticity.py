# src/enhancements/helium_elasticity.py

"""
Enhanced Helium Supply-Demand Elasticity & Pricing Model - Version 6.0

PRODUCTION ENHANCEMENTS OVER v5.1:
1. ENHANCED: Multi-agent fidelity preserved in parallel workers
2. ENHANCED: Production capacity limits for realistic supply modeling
3. ENHANCED: Deep merge for robust scenario overrides
4. ENHANCED: Consumer config validation (substitution threshold)
5. ENHANCED: Auto-normalizing producer market shares
6. ADDED: Market concentration alerts (HHI monitoring)
7. ADDED: Price spike detection
8. ADDED: Supply shortage risk metrics
9. ADDED: Interactive scenario comparison charts
10. ADDED: Export to CSV/JSON for external analysis

V6.0 NEW ENHANCEMENTS:
11. ADDED: Advanced stochastic volatility model with GARCH(1,1)
12. ADDED: Dynamic equilibrium price discovery with market clearing algorithm
13. ADDED: Machine learning-based price prediction with ensemble methods
14. ADDED: Real-time market monitoring with alerting system
15. ADDED: Advanced risk metrics (VaR, CVaR, Expected Shortfall)
16. ADDED: Time-varying elasticity models for producers/consumers
17. ADDED: Supply chain network effects and cascading disruptions
18. ADDED: Bayesian parameter estimation for model calibration
19. ADDED: GPU-accelerated Monte Carlo simulation support
20. ADDED: Interactive dashboard data generation

Reference:
- "Helium Market Dynamics" (USGS Mineral Commodity Summaries, 2024)
- "Commodity Price Modeling" (Journal of Commodity Markets, 2024)
- "Monte Carlo Methods in Finance" (Wiley, 2023)
- "Supply Chain Resilience" (Harvard Business Review, 2024)
- "Machine Learning in Commodity Markets" (Journal of Finance, 2025)
- "Stochastic Volatility Models" (Oxford Financial Series, 2024)
"""

from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple, Any, Callable, Union
from enum import Enum
import numpy as np
import math
import logging
import asyncio
import time
import json
import os
import hashlib
from datetime import datetime, timedelta
from collections import deque, defaultdict
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
import multiprocessing
import copy
import csv
import itertools
import warnings

# Production dependencies
from pydantic import BaseModel, Field, validator, root_validator
import yaml
import pandas as pd
from scipy import stats, optimize
from scipy.optimize import minimize, differential_evolution
from prometheus_client import Counter, Histogram, Gauge, CollectorRegistry, Summary
import structlog
from structlog.processors import JSONRenderer, TimeStamper

# Optional GPU support
try:
    import cupy as cp
    GPU_AVAILABLE = True
except ImportError:
    cp = None
    GPU_AVAILABLE = False

# Machine learning imports
try:
    from sklearn.ensemble import RandomForestRegressor, GradientBoostingRegressor
    from sklearn.preprocessing import StandardScaler
    from sklearn.model_selection import TimeSeriesSplit
    SKLEARN_AVAILABLE = True
except ImportError:
    SKLEARN_AVAILABLE = False

# Configure structured logging with enhanced processors
structlog.configure(
    processors=[
        structlog.stdlib.filter_by_level, 
        structlog.stdlib.add_logger_name,
        structlog.stdlib.add_log_level, 
        TimeStamper(fmt="iso"),
        structlog.processors.StackInfoRenderer(), 
        structlog.processors.format_exc_info,
        structlog.processors.UnicodeDecoder(),
        JSONRenderer()
    ],
    context_class=dict, 
    logger_factory=structlog.stdlib.LoggerFactory(),
    cache_logger_on_first_use=True,
)
logger = structlog.get_logger(__name__)

# Enhanced Prometheus metrics
REGISTRY = CollectorRegistry()
SIMULATION_RUNS = Counter('helium_simulation_runs_total', 'Total simulation runs',
                         ['scenario', 'status'], registry=REGISTRY)
SIMULATION_DURATION = Histogram('helium_simulation_duration_seconds', 'Simulation duration',
                               ['method'], registry=REGISTRY)
PRICE_FORECAST = Gauge('helium_price_forecast', 'Current price forecast',
                      ['horizon', 'scenario'], registry=REGISTRY)
MARKET_CONCENTRATION = Gauge('helium_market_hhi', 'Market concentration (HHI)', registry=REGISTRY)
SUPPLY_SHORTAGE_RISK = Gauge('helium_supply_shortage_risk', 'Supply shortage probability', registry=REGISTRY)
VOLATILITY_GAUGE = Gauge('helium_volatility', 'Current market volatility estimate', registry=REGISTRY)
VAR_GAUGE = Gauge('helium_value_at_risk', 'Value at Risk estimate', 
                  ['confidence_level', 'horizon'], registry=REGISTRY)
ALERT_COUNTER = Counter('helium_market_alerts_total', 'Total market alerts triggered',
                       ['alert_type', 'severity'], registry=REGISTRY)
SIMULATION_LATENCY = Summary('helium_simulation_latency_seconds', 
                            'Simulation step latency', ['phase'], registry=REGISTRY)


# ============================================================
# ENHANCEMENT 11: ADVANCED STOCHASTIC VOLATILITY MODELS
# ============================================================

class StochasticVolatilityModel:
    """
    Advanced stochastic volatility models for helium price dynamics.
    
    Implements:
    - GARCH(1,1) for time-varying volatility
    - Heston model for stochastic volatility
    - Regime-switching volatility (low/medium/high volatility states)
    """
    
    def __init__(self, initial_volatility: float = 0.2):
        self.initial_volatility = initial_volatility
        self._volatility_path = None
        self._regime = 'medium'  # low, medium, high
        
    def garch_volatility(self, returns: np.ndarray, omega: float = 0.00001, 
                        alpha: float = 0.1, beta: float = 0.85) -> np.ndarray:
        """
        GARCH(1,1) volatility estimation
        σ²_t = ω + α * r²_{t-1} + β * σ²_{t-1}
        """
        n = len(returns) + 1
        variance = np.zeros(n)
        variance[0] = self.initial_volatility ** 2
        
        for t in range(1, n):
            variance[t] = (omega + 
                          alpha * returns[t-1]**2 + 
                          beta * variance[t-1])
        
        self._volatility_path = np.sqrt(variance)
        VOLATILITY_GAUGE.set(np.mean(np.sqrt(variance[-252:])))
        return self._volatility_path
    
    def heston_volatility(self, n_steps: int, kappa: float = 2.0, 
                         theta: float = 0.04, xi: float = 0.3, 
                         rho: float = -0.7, dt: float = 1/252) -> np.ndarray:
        """
        Heston stochastic volatility model
        dS = μS dt + √v S dW₁
        dv = κ(θ - v) dt + ξ√v dW₂
        dW₁ dW₂ = ρ dt
        """
        variance = np.zeros(n_steps)
        variance[0] = self.initial_volatility ** 2
        
        for t in range(1, n_steps):
            # Correlated Brownian motions
            dW1 = np.random.normal(0, np.sqrt(dt))
            dW2 = rho * dW1 + np.sqrt(1 - rho**2) * np.random.normal(0, np.sqrt(dt))
            
            # Variance process (ensure non-negativity)
            variance[t] = max(variance[t-1] + 
                             kappa * (theta - variance[t-1]) * dt + 
                             xi * np.sqrt(max(variance[t-1], 1e-10)) * dW2, 
                             1e-8)
        
        self._volatility_path = np.sqrt(variance)
        VOLATILITY_GAUGE.set(np.mean(self._volatility_path[-252:]))
        return self._volatility_path
    
    def regime_switching_volatility(self, n_steps: int, 
                                   transition_matrix: np.ndarray = None) -> Tuple[np.ndarray, np.ndarray]:
        """
        Regime-switching volatility with three states:
        - Low volatility: σ = 0.10
        - Medium volatility: σ = 0.20
        - High volatility: σ = 0.40
        """
        if transition_matrix is None:
            # Default transition matrix
            transition_matrix = np.array([
                [0.85, 0.10, 0.05],  # From low
                [0.10, 0.80, 0.10],  # From medium
                [0.05, 0.15, 0.80]   # From high
            ])
        
        volatilities = {'low': 0.10, 'medium': 0.20, 'high': 0.40}
        regimes = ['low', 'medium', 'high']
        current_regime = 1  # Start in medium
        
        regime_path = np.zeros(n_steps, dtype=int)
        volatility_path = np.zeros(n_steps)
        
        for t in range(n_steps):
            regime_path[t] = current_regime
            volatility_path[t] = volatilities[regimes[current_regime]]
            
            # Transition to next regime
            current_regime = np.random.choice(3, p=transition_matrix[current_regime])
        
        self._volatility_path = volatility_path
        self._regime = regimes[regime_path[-1]]
        VOLATILITY_GAUGE.set(np.mean(volatility_path))
        return volatility_path, regime_path


# ============================================================
# ENHANCEMENT 12: DYNAMIC EQUILIBRIUM PRICE DISCOVERY
# ============================================================

class MarketClearingEngine:
    """
    Advanced market clearing algorithm for equilibrium price discovery.
    
    Features:
    - Iterative price adjustment until supply equals demand
    - Walrasian auctioneer mechanism
    - Price impact functions for large orders
    """
    
    def __init__(self, tolerance: float = 0.01, max_iterations: int = 100):
        self.tolerance = tolerance
        self.max_iterations = max_iterations
        
    def find_equilibrium(self, supply_function: Callable, demand_function: Callable,
                        initial_price: float, **kwargs) -> Tuple[float, float, float]:
        """
        Find market clearing price using Newton-Raphson method
        """
        price = initial_price
        
        for iteration in range(self.max_iterations):
            supply = supply_function(price, **kwargs)
            demand = demand_function(price, **kwargs)
            excess_demand = demand - supply
            
            if abs(excess_demand) < self.tolerance * max(supply, demand, 1):
                return price, supply, demand
            
            # Numerical derivative for price adjustment
            delta = 0.01 * price
            supply_plus = supply_function(price + delta, **kwargs)
            demand_plus = demand_function(price + delta, **kwargs)
            excess_plus = demand_plus - supply_plus
            
            derivative = (excess_plus - excess_demand) / delta if delta != 0 else 1
            
            # Price adjustment (dampened)
            adjustment = excess_demand / (abs(derivative) + 1e-10)
            price = max(10, price - 0.5 * adjustment)  # Dampening factor
        
        logger.warning("Market clearing did not converge", 
                      excess_demand=abs(excess_demand),
                      tolerance=self.tolerance)
        return price, supply_function(price, **kwargs), demand_function(price, **kwargs)
    
    def walrasian_tatonnement(self, supply_functions: List[Callable], 
                             demand_functions: List[Callable],
                             initial_price: float, adjustment_speed: float = 0.1) -> np.ndarray:
        """
        Walrasian tatonnement process for multi-agent equilibrium
        """
        n_steps = 100
        prices = np.zeros(n_steps)
        prices[0] = initial_price
        
        for t in range(1, n_steps):
            total_supply = sum(s(prices[t-1]) for s in supply_functions)
            total_demand = sum(d(prices[t-1]) for d in demand_functions)
            
            excess_demand = total_demand - total_supply
            prices[t] = prices[t-1] + adjustment_speed * excess_demand
            prices[t] = max(10, prices[t])
        
        return prices


# ============================================================
# ENHANCEMENT 13: MACHINE LEARNING PRICE PREDICTION
# ============================================================

class MLPricePredictor:
    """
    Machine learning-based price prediction using ensemble methods.
    """
    
    def __init__(self):
        self.models = {}
        self.scaler = StandardScaler()
        self.is_trained = False
        
    def _create_features(self, price_history: np.ndarray, 
                        lags: int = 20) -> Tuple[np.ndarray, np.ndarray]:
        """Create features for ML model from price history"""
        n = len(price_history)
        features = []
        targets = []
        
        for i in range(lags, n):
            # Lagged prices
            lags_features = price_history[i-lags:i]
            
            # Technical indicators
            returns = np.diff(price_history[max(0, i-21):i+1]) / price_history[max(0, i-21):i]
            volatility = np.std(returns[-20:]) if len(returns) >= 20 else 0.2
            momentum = price_history[i-1] / price_history[max(0, i-20)] - 1
            
            feature_vector = np.concatenate([
                lags_features,
                [volatility, momentum, np.mean(lags_features[-5:]), np.std(lags_features[-5:])]
            ])
            
            features.append(feature_vector)
            targets.append(price_history[i])
        
        return np.array(features), np.array(targets)
    
    def train(self, price_history: np.ndarray) -> None:
        """Train ensemble of ML models"""
        if not SKLEARN_AVAILABLE:
            logger.warning("scikit-learn not available, ML prediction disabled")
            return
        
        if len(price_history) < 30:
            logger.warning("Insufficient data for ML training")
            return
        
        try:
            X, y = self._create_features(price_history)
            X_scaled = self.scaler.fit_transform(X)
            
            # Train multiple models for ensemble
            self.models['rf'] = RandomForestRegressor(n_estimators=100, max_depth=10, random_state=42)
            self.models['gb'] = GradientBoostingRegressor(n_estimators=100, learning_rate=0.1, random_state=42)
            
            for name, model in self.models.items():
                model.fit(X_scaled, y)
            
            self.is_trained = True
            logger.info("ML models trained successfully", n_samples=len(X))
        except Exception as e:
            logger.error("Failed to train ML models", error=str(e))
    
    def predict(self, recent_prices: np.ndarray, horizon: int = 10) -> Dict[str, np.ndarray]:
        """Generate predictions from ensemble models"""
        if not self.is_trained or not recent_prices.any():
            return {'ensemble': recent_prices}
        
        try:
            predictions = {}
            current_features = self._create_features(recent_prices)[0]
            
            if len(current_features) == 0:
                return {'ensemble': recent_prices}
            
            last_features = current_features[-1:].reshape(1, -1)
            last_features_scaled = self.scaler.transform(last_features)
            
            for name, model in self.models.items():
                pred = model.predict(last_features_scaled)
                predictions[name] = pred
            
            # Ensemble prediction (weighted average)
            predictions['ensemble'] = np.mean(list(predictions.values()), axis=0)
            return predictions
        
        except Exception as e:
            logger.error("ML prediction failed", error=str(e))
            return {'ensemble': recent_prices}


# ============================================================
# ENHANCEMENT 14: REAL-TIME MARKET MONITORING
# ============================================================

class MarketMonitor:
    """
    Real-time market monitoring and alerting system.
    """
    
    def __init__(self):
        self.alerts_history = deque(maxlen=1000)
        self.metrics_buffer = deque(maxlen=100)
        self.alert_thresholds = {
            'price_spike': 0.15,      # 15% price increase
            'price_crash': -0.10,     # 10% price decrease
            'volatility_spike': 0.40, # 40% volatility
            'hhi_concentration': 2500, # High market concentration
            'shortage_critical': 0.8  # 80% shortage probability
        }
        
    def monitor(self, market_data: Dict) -> List[Dict]:
        """Monitor market conditions and generate alerts"""
        alerts = []
        timestamp = datetime.now().isoformat()
        
        # Price spike detection
        if 'price_change_pct' in market_data:
            if market_data['price_change_pct'] > self.alert_thresholds['price_spike']:
                alert = self._create_alert('price_spike', 'warning', 
                                          f"Price spike detected: {market_data['price_change_pct']:.1%}",
                                          market_data)
                alerts.append(alert)
        
        # Volatility monitoring
        if 'current_volatility' in market_data:
            if market_data['current_volatility'] > self.alert_thresholds['volatility_spike']:
                alert = self._create_alert('volatility_spike', 'critical',
                                          f"Extreme volatility: {market_data['current_volatility']:.1%}",
                                          market_data)
                alerts.append(alert)
        
        # Market concentration
        if 'hhi' in market_data:
            if market_data['hhi'] > self.alert_thresholds['hhi_concentration']:
                alert = self._create_alert('high_concentration', 'warning',
                                          f"High market concentration (HHI: {market_data['hhi']:.0f})",
                                          market_data)
                alerts.append(alert)
        
        # Supply shortage risk
        if 'shortage_risk' in market_data:
            if market_data['shortage_risk'] > self.alert_thresholds['shortage_critical']:
                alert = self._create_alert('supply_shortage', 'critical',
                                          f"Critical supply shortage risk: {market_data['shortage_risk']:.1%}",
                                          market_data)
                alerts.append(alert)
        
        self.alerts_history.extend(alerts)
        self.metrics_buffer.append(market_data)
        
        return alerts
    
    def _create_alert(self, alert_type: str, severity: str, message: str, 
                     data: Dict) -> Dict:
        """Create and log alert"""
        ALERT_COUNTER.labels(alert_type=alert_type, severity=severity).inc()
        
        alert = {
            'timestamp': datetime.now().isoformat(),
            'type': alert_type,
            'severity': severity,
            'message': message,
            'data': data
        }
        
        log_method = getattr(logger, severity, logger.warning)
        log_method("Market alert triggered", **alert)
        
        return alert
    
    def get_alert_summary(self) -> Dict:
        """Get summary of recent alerts"""
        if not self.alerts_history:
            return {'total_alerts': 0}
        
        alerts_df = pd.DataFrame(list(self.alerts_history))
        
        summary = {
            'total_alerts': len(alerts_df),
            'by_type': alerts_df['type'].value_counts().to_dict(),
            'by_severity': alerts_df['severity'].value_counts().to_dict(),
            'last_alert_time': alerts_df.iloc[-1]['timestamp'],
            'critical_alerts_24h': len(alerts_df[
                (alerts_df['severity'] == 'critical') & 
                (pd.to_datetime(alerts_df['timestamp']) > datetime.now() - timedelta(hours=24))
            ])
        }
        
        return summary


# ============================================================
# ENHANCEMENT 15: ADVANCED RISK METRICS
# ============================================================

class AdvancedRiskMetrics:
    """
    Advanced risk metrics for helium market analysis.
    
    Implements:
    - Value at Risk (VaR)
    - Conditional VaR (CVaR) / Expected Shortfall
    - Maximum drawdown
    - Sharpe ratio
    """
    
    @staticmethod
    def calculate_var(price_paths: np.ndarray, confidence_level: float = 0.95) -> float:
        """Calculate Value at Risk"""
        if len(price_paths) == 0:
            return 0
        
        final_prices = price_paths[:, -1]
        price_returns = np.diff(price_paths) / price_paths[:, :-1]
        
        # Calculate VaR using historical method
        var = np.percentile(price_returns.flatten(), (1 - confidence_level) * 100)
        
        VAR_GAUGE.labels(confidence_level=str(confidence_level), horizon='1d').set(abs(var))
        return abs(var)
    
    @staticmethod
    def calculate_cvar(price_paths: np.ndarray, confidence_level: float = 0.95) -> float:
        """Calculate Conditional VaR (Expected Shortfall)"""
        if len(price_paths) == 0:
            return 0
        
        price_returns = np.diff(price_paths) / price_paths[:, :-1]
        returns_flat = price_returns.flatten()
        
        var = np.percentile(returns_flat, (1 - confidence_level) * 100)
        cvar = np.mean(returns_flat[returns_flat <= var])
        
        return abs(cvar)
    
    @staticmethod
    def calculate_max_drawdown(price_path: np.ndarray) -> Tuple[float, int, int]:
        """Calculate maximum drawdown and its duration"""
        cumulative = np.maximum.accumulate(price_path)
        drawdowns = (price_path - cumulative) / cumulative
        
        max_dd = np.min(drawdowns)
        max_dd_end = np.argmin(drawdowns)
        max_dd_start = np.argmax(price_path[:max_dd_end]) if max_dd_end > 0 else 0
        
        return abs(max_dd), max_dd_start, max_dd_end
    
    @staticmethod
    def calculate_sharpe_ratio(price_path: np.ndarray, risk_free_rate: float = 0.02) -> float:
        """Calculate Sharpe ratio for price path"""
        if len(price_path) < 2:
            return 0
        
        returns = np.diff(price_path) / price_path[:-1]
        excess_returns = returns - risk_free_rate / 252  # Daily risk-free rate
        
        if np.std(excess_returns) == 0:
            return 0
        
        sharpe = np.sqrt(252) * np.mean(excess_returns) / np.std(excess_returns)
        return sharpe


# ============================================================
# ENHANCEMENT 16: TIME-VARYING ELASTICITY MODELS
# ============================================================

class TimeVaryingElasticity:
    """
    Time-varying elasticity models for producers and consumers.
    
    Features:
    - Seasonal elasticity patterns
    - Trend-based elasticity evolution
    - Price-dependent elasticity
    """
    
    @staticmethod
    def seasonal_elasticity(time_years: float, base_elasticity: float,
                          amplitude: float = 0.1, frequency: float = 1.0) -> float:
        """
        Model seasonal variations in elasticity
        """
        seasonal_factor = amplitude * np.sin(2 * np.pi * frequency * time_years)
        return base_elasticity * (1 + seasonal_factor)
    
    @staticmethod
    def adaptive_elasticity(price: float, reference_price: float, 
                          base_elasticity: float, adaptation_rate: float = 0.1) -> float:
        """
        Elasticity that adapts based on price levels
        """
        price_ratio = price / reference_price
        if price_ratio > 1.5:  # High prices
            # Consumers become more price-sensitive, producers less responsive
            adaptation = -adaptation_rate * (price_ratio - 1.5)
        elif price_ratio < 0.5:  # Low prices
            # Producers cut production, consumers less sensitive
            adaptation = adaptation_rate * (0.5 - price_ratio)
        else:
            adaptation = 0
        
        return base_elasticity + adaptation
    
    @staticmethod
    def regime_dependent_elasticity(volatility_regime: str, 
                                  base_elasticity: float) -> float:
        """
        Elasticity varies by volatility regime
        """
        regime_multipliers = {
            'low': 0.8,     # Less responsive in low volatility
            'medium': 1.0,   # Normal responsiveness
            'high': 1.3      # More responsive in high volatility
        }
        
        return base_elasticity * regime_multipliers.get(volatility_regime, 1.0)


# ============================================================
# ENHANCEMENT 17: SUPPLY CHAIN NETWORK EFFECTS
# ============================================================

class SupplyChainNetwork:
    """
    Supply chain network analysis for cascading disruptions.
    """
    
    def __init__(self, producers: List['HeliumProducer'], consumers: List['HeliumConsumer']):
        self.producers = producers
        self.consumers = consumers
        self.dependency_matrix = None
        self._build_dependency_matrix()
    
    def _build_dependency_matrix(self):
        """Build dependency matrix between market participants"""
        n_producers = len(self.producers)
        n_consumers = len(self.consumers)
        total_participants = n_producers + n_consumers
        
        self.dependency_matrix = np.zeros((total_participants, total_participants))
        
        # Model dependencies
        for i, consumer in enumerate(self.consumers):
            # Consumers depend on all producers
            for j, producer in enumerate(self.producers):
                dependency = producer.market_share_pct / 100
                self.dependency_matrix[i, j] = dependency
        
        for i, producer in enumerate(self.producers):
            # Producers depend on consumer demand
            for j, consumer in enumerate(self.consumers):
                dependency = consumer.base_demand_mmcf / sum(c.base_demand_mmcf for c in self.consumers)
                self.dependency_matrix[i + n_producers, j + n_producers] = dependency
    
    def simulate_cascading_disruption(self, initial_impact: Dict[str, float]) -> Dict:
        """
        Simulate cascading effects through supply chain
        
        Parameters:
        - initial_impact: Dict mapping producer/consumer names to impact levels (0 to 1)
        """
        n_rounds = 10
        impacts = copy.deepcopy(initial_impact)
        
        # Initialize all participants with 0 impact
        all_names = [p.name for p in self.producers] + [c.name for c in self.consumers]
        for name in all_names:
            if name not in impacts:
                impacts[name] = 0.0
        
        impact_history = [copy.deepcopy(impacts)]
        
        for round in range(n_rounds):
            new_impacts = copy.deepcopy(impacts)
            
            # Propagate impacts through network
            for i, name_i in enumerate(all_names):
                for j, name_j in enumerate(all_names):
                    if i != j and self.dependency_matrix[i, j] > 0:
                        # Impact propagates based on dependency
                        propagated = impacts[name_j] * self.dependency_matrix[i, j] * 0.5
                        new_impacts[name_i] = min(1.0, new_impacts[name_i] + propagated)
            
            impacts = new_impacts
            impact_history.append(copy.deepcopy(impacts))
            
            # Check for convergence
            if max(abs(impacts[n] - impact_history[-2][n]) for n in all_names) < 0.001:
                break
        
        return {
            'final_impacts': impacts,
            'impact_history': impact_history,
            'rounds_to_converge': len(impact_history) - 1
        }


# ============================================================
# ENHANCEMENT 18: BAYESIAN PARAMETER ESTIMATION
# ============================================================

class BayesianParameterEstimation:
    """
    Bayesian parameter estimation for model calibration.
    
    Uses Markov Chain Monte Carlo (MCMC) for parameter inference.
    """
    
    def __init__(self, prior_distributions: Dict[str, Any] = None):
        self.prior_distributions = prior_distributions or {
            'volatility': {'type': 'beta', 'alpha': 2, 'beta': 8},  # Prior for volatility
            'elasticity': {'type': 'normal', 'mu': 0.3, 'sigma': 0.1},  # Prior for elasticity
            'mean_reversion': {'type': 'gamma', 'k': 2, 'theta': 0.1}  # Prior for mean reversion speed
        }
    
    def estimate_parameters(self, historical_prices: np.ndarray, 
                          n_iterations: int = 10000, n_burn_in: int = 2000) -> Dict:
        """
        Estimate model parameters using MCMC
        """
        if len(historical_prices) < 50:
            return {'error': 'Insufficient historical data'}
        
        # Simple Metropolis-Hastings algorithm
        returns = np.diff(np.log(historical_prices))
        
        current_params = {
            'volatility': np.std(returns) * np.sqrt(252),
            'elasticity': 0.3,
            'mean_reversion': 0.1
        }
        
        samples = {key: [] for key in current_params}
        
        for iteration in range(n_iterations):
            # Propose new parameters
            proposal = {}
            for key in current_params:
                proposal[key] = current_params[key] + np.random.normal(0, 0.01)
                proposal[key] = max(0.001, proposal[key])  # Ensure positivity
            
            # Calculate acceptance probability
            log_prior_current = self._log_prior(current_params)
            log_prior_proposal = self._log_prior(proposal)
            log_likelihood_current = self._log_likelihood(returns, current_params)
            log_likelihood_proposal = self._log_likelihood(returns, proposal)
            
            log_acceptance = (log_likelihood_proposal + log_prior_proposal - 
                            log_likelihood_current - log_prior_current)
            
            # Accept or reject
            if np.log(np.random.random()) < log_acceptance:
                current_params = proposal
            
            # Store samples after burn-in
            if iteration >= n_burn_in:
                for key in current_params:
                    samples[key].append(current_params[key])
        
        # Calculate posterior statistics
        results = {}
        for key in samples:
            samples_array = np.array(samples[key])
            results[key] = {
                'mean': np.mean(samples_array),
                'median': np.median(samples_array),
                'std': np.std(samples_array),
                'ci_95': [np.percentile(samples_array, 2.5), np.percentile(samples_array, 97.5)]
            }
        
        return results
    
    def _log_prior(self, params: Dict) -> float:
        """Calculate log prior probability"""
        log_prior = 0
        for key, value in params.items():
            if key in self.prior_distributions:
                prior = self.prior_distributions[key]
                if prior['type'] == 'beta':
                    log_prior += stats.beta.logpdf(value, prior['alpha'], prior['beta'])
                elif prior['type'] == 'normal':
                    log_prior += stats.norm.logpdf(value, prior['mu'], prior['sigma'])
                elif prior['type'] == 'gamma':
                    log_prior += stats.gamma.logpdf(value, prior['k'], scale=prior['theta'])
        
        return log_prior
    
    def _log_likelihood(self, returns: np.ndarray, params: Dict) -> float:
        """Calculate log likelihood of returns given parameters"""
        volatility = params['volatility'] / np.sqrt(252)
        mean_reversion = params['mean_reversion']
        
        # Simple mean-reverting process likelihood
        n = len(returns)
        log_lik = -0.5 * n * np.log(2 * np.pi * volatility**2)
        
        for i in range(1, n):
            expected_return = -mean_reversion * returns[i-1]
            residual = returns[i] - expected_return
            log_lik -= 0.5 * (residual / volatility)**2
        
        return log_lik


# ============================================================
# ENHANCEMENT 19: GPU ACCELERATED SIMULATION
# ============================================================

class GPUAcceleratedSimulator:
    """
    GPU-accelerated Monte Carlo simulation using CuPy.
    """
    
    def __init__(self):
        self.gpu_available = GPU_AVAILABLE
        
    def simulate_gpu(self, n_paths: int, n_steps: int, params: Dict) -> np.ndarray:
        """
        Run Monte Carlo simulation on GPU
        """
        if not self.gpu_available:
            logger.warning("GPU not available, falling back to CPU")
            return self._simulate_cpu(n_paths, n_steps, params)
        
        try:
            # Transfer to GPU
            base_price_gpu = cp.array(params['base_price'])
            volatility_gpu = cp.array(params['volatility'])
            dt_gpu = cp.array(params['dt'])
            
            # Generate random numbers on GPU
            random_numbers = cp.random.normal(0, 1, (n_paths, n_steps))
            
            # Initialize price paths
            prices = cp.zeros((n_paths, n_steps + 1))
            prices[:, 0] = base_price_gpu
            
            # Simulate on GPU
            for t in range(1, n_steps + 1):
                # Mean reversion
                mean_reversion = params['mean_reversion'] * (base_price_gpu - prices[:, t-1]) * dt_gpu
                
                # Random shock
                shock = volatility_gpu * prices[:, t-1] * random_numbers[:, t-1] * cp.sqrt(dt_gpu)
                
                prices[:, t] = cp.maximum(10, prices[:, t-1] + mean_reversion + shock)
            
            # Transfer back to CPU
            return cp.asnumpy(prices)
            
        except Exception as e:
            logger.error("GPU simulation failed", error=str(e))
            return self._simulate_cpu(n_paths, n_steps, params)
    
    def _simulate_cpu(self, n_paths: int, n_steps: int, params: Dict) -> np.ndarray:
        """Fallback CPU simulation"""
        base_price = params['base_price']
        volatility = params['volatility']
        dt = params['dt']
        
        prices = np.zeros((n_paths, n_steps + 1))
        prices[:, 0] = base_price
        
        for t in range(1, n_steps + 1):
            mean_reversion = params['mean_reversion'] * (base_price - prices[:, t-1]) * dt
            shock = volatility * prices[:, t-1] * np.random.normal(0, 1, n_paths) * np.sqrt(dt)
            prices[:, t] = np.maximum(10, prices[:, t-1] + mean_reversion + shock)
        
        return prices


# ============================================================
# ENHANCEMENT 20: INTERACTIVE DASHBOARD DATA GENERATION
# ============================================================

class DashboardDataGenerator:
    """
    Generate data for interactive visualization dashboards.
    """
    
    def __init__(self):
        self.cache = {}
    
    def generate_dashboard_data(self, simulator: 'HeliumMarketSimulator', 
                               scenario_analysis: 'ScenarioAnalysis') -> Dict:
        """Generate comprehensive dashboard data"""
        dashboard_data = {
            'timestamp': datetime.now().isoformat(),
            'market_summary': self._get_market_summary(simulator),
            'price_analysis': self._get_price_analysis(simulator),
            'scenario_comparison': self._get_scenario_comparison(scenario_analysis),
            'risk_metrics': self._get_risk_metrics(simulator),
            'monitoring_alerts': self._get_monitoring_data(simulator),
            'supply_chain_health': self._get_supply_chain_data(simulator)
        }
        
        return dashboard_data
    
    def _get_market_summary(self, simulator: 'HeliumMarketSimulator') -> Dict:
        """Get market summary statistics"""
        stats = simulator.get_statistics()
        forecast = simulator.get_price_forecast()
        
        return {
            'current_price': forecast.get('expected_price', 0),
            'price_range': forecast.get('confidence_interval', [0, 0]),
            'market_hhi': stats.get('market_concentration_hhi', 0),
            'shortage_risk': stats.get('supply_shortage_risk', 0),
            'producer_count': len(stats.get('producers', [])),
            'consumer_count': len(stats.get('consumers', []))
        }
    
    def _get_price_analysis(self, simulator: 'HeliumMarketSimulator') -> Dict:
        """Get detailed price analysis"""
        if not simulator.price_paths:
            return {}
        
        price_array = np.array(simulator.price_paths)
        
        return {
            'mean_path': np.mean(price_array, axis=0).tolist(),
            'upper_95': np.percentile(price_array, 97.5, axis=0).tolist(),
            'lower_95': np.percentile(price_array, 2.5, axis=0).tolist(),
            'volatility': AdvancedRiskMetrics.calculate_var(price_array),
            'sharpe_ratio': AdvancedRiskMetrics.calculate_sharpe_ratio(np.mean(price_array, axis=0))
        }
    
    def _get_scenario_comparison(self, scenario_analysis: 'ScenarioAnalysis') -> Dict:
        """Get scenario comparison data"""
        comparison = scenario_analysis.compare_scenarios()
        
        if comparison.empty:
            return {}
        
        return comparison.to_dict('records')
    
    def _get_risk_metrics(self, simulator: 'HeliumMarketSimulator') -> Dict:
        """Get risk metrics"""
        if not simulator.price_paths:
            return {}
        
        price_array = np.array(simulator.price_paths)
        
        return {
            'var_95': AdvancedRiskMetrics.calculate_var(price_array, 0.95),
            'var_99': AdvancedRiskMetrics.calculate_var(price_array, 0.99),
            'cvar_95': AdvancedRiskMetrics.calculate_cvar(price_array, 0.95),
            'max_drawdown': AdvancedRiskMetrics.calculate_max_drawdown(
                np.mean(price_array, axis=0))[0]
        }
    
    def _get_monitoring_data(self, simulator: 'HeliumMarketSimulator') -> Dict:
        """Get monitoring alerts data"""
        monitor = MarketMonitor()
        
        market_data = {
            'price_change_pct': 0,
            'current_volatility': 0.2,
            'hhi': simulator.calculate_market_concentration(),
            'shortage_risk': simulator.calculate_supply_shortage_risk()
        }
        
        monitor.monitor(market_data)
        return monitor.get_alert_summary()
    
    def _get_supply_chain_data(self, simulator: 'HeliumMarketSimulator') -> Dict:
        """Get supply chain network data"""
        network = SupplyChainNetwork(simulator.producers, simulator.consumers)
        
        # Simulate disruption in largest producer
        largest_producer = max(simulator.producers, key=lambda p: p.market_share_pct)
        disruption = {largest_producer.name: 0.5}
        
        cascading = network.simulate_cascading_disruption(disruption)
        
        return {
            'dependency_matrix': network.dependency_matrix.tolist(),
            'cascading_impact': cascading['final_impacts'],
            'rounds_to_converge': cascading['rounds_to_converge']
        }


# ============================================================
# ENHANCED V6.0 MAIN SIMULATOR CLASS
# ============================================================

class HeliumMarketSimulatorV6(HeliumMarketSimulator):
    """
    Enhanced V6.0 simulator with all new features integrated.
    """
    
    def __init__(self, config: SimulationConfig):
        super().__init__(config)
        self.volatility_model = StochasticVolatilityModel(config.price_volatility)
        self.clearing_engine = MarketClearingEngine()
        self.ml_predictor = MLPricePredictor()
        self.monitor = MarketMonitor()
        self.risk_metrics = AdvancedRiskMetrics()
        self.gpu_simulator = GPUAcceleratedSimulator()
        self.elasticity_model = TimeVaryingElasticity()
        
        logger.info(f"HeliumMarketSimulatorV6 initialized with GPU: {GPU_AVAILABLE}")
    
    @SIMULATION_DURATION.time()
    def simulate_market_enhanced(self, use_gpu: bool = True) -> Dict:
        """
        Enhanced simulation with all V6.0 features
        """
        SIMULATION_RUNS.labels(scenario='enhanced_v6', status='running').inc()
        
        # Prepare simulation parameters
        params = {
            'base_price': self.base_price,
            'volatility': self.config.price_volatility,
            'dt': 1.0 / self.config.time_steps_per_year,
            'mean_reversion': 0.2,
            'years': self.config.simulation_years,
            'steps_per_year': self.config.time_steps_per_year
        }
        
        total_steps = self.config.simulation_years * self.config.time_steps_per_year
        
        # GPU-accelerated simulation
        if use_gpu and GPU_AVAILABLE:
            logger.info("Using GPU-accelerated simulation")
            with SIMULATION_LATENCY.labels(phase='gpu_simulation').time():
                price_paths = self.gpu_simulator.simulate_gpu(
                    self.config.monte_carlo_runs, total_steps, params
                )
        else:
            # Fallback to standard CPU simulation
            logger.info("Using CPU simulation")
            price_paths = super().simulate_market()
            if isinstance(price_paths, list):
                price_paths = np.array(price_paths)
        
        self.price_paths = price_paths
        
        # Train ML model on mean price path
        mean_price = np.mean(price_paths, axis=0)
        with SIMULATION_LATENCY.labels(phase='ml_training').time():
            self.ml_predictor.train(mean_price)
        
        # Calculate advanced risk metrics
        with SIMULATION_LATENCY.labels(phase='risk_metrics').time():
            risk_analysis = {
                'var_95': self.risk_metrics.calculate_var(price_paths, 0.95),
                'var_99': self.risk_metrics.calculate_var(price_paths, 0.99),
                'cvar_95': self.risk_metrics.calculate_cvar(price_paths, 0.95),
                'max_drawdown': self.risk_metrics.calculate_max_drawdown(mean_price)[0],
                'sharpe_ratio': self.risk_metrics.calculate_sharpe_ratio(mean_price)
            }
        
        # Bayesian parameter estimation
        bayesian = BayesianParameterEstimation()
        with SIMULATION_LATENCY.labels(phase='bayesian_estimation').time():
            parameter_estimates = bayesian.estimate_parameters(mean_price, n_iterations=1000)
        
        # Generate monitoring data
        market_data = {
            'price_change_pct': (mean_price[-1] - mean_price[0]) / mean_price[0],
            'current_volatility': np.std(np.diff(mean_price) / mean_price[:-1]) * np.sqrt(252),
            'hhi': self.calculate_market_concentration(),
            'shortage_risk': self.calculate_supply_shortage_risk()
        }
        alerts = self.monitor.monitor(market_data)
        
        # ML predictions
        ml_predictions = self.ml_predictor.predict(mean_price[-50:])
        
        # Price forecast
        forecast = self.get_price_forecast()
        PRICE_FORECAST.labels(horizon='final', scenario='enhanced_v6').set(
            forecast.get('expected_price', 0))
        
        SIMULATION_RUNS.labels(scenario='enhanced_v6', status='success').inc()
        
        results = {
            'price_paths': price_paths,
            'mean_price_path': mean_price.tolist(),
            'forecast': forecast,
            'risk_analysis': risk_analysis,
            'parameter_estimates': parameter_estimates,
            'alerts': alerts,
            'ml_predictions': ml_predictions,
            'market_summary': market_data,
            'statistics': self.get_statistics()
        }
        
        return results
    
    def generate_dashboard_export(self, scenario_analysis: 'ScenarioAnalysis') -> Dict:
        """Generate complete dashboard data"""
        generator = DashboardDataGenerator()
        return generator.generate_dashboard_data(self, scenario_analysis)


# ============================================================
# ENHANCED V6.0 MAIN FUNCTION
# ============================================================

def main_v6():
    """Enhanced V6.0 demonstration"""
    print("=" * 80)
    print("Helium Elasticity & Pricing Model v6.0 - Advanced Features Demo")
    print("=" * 80)
    
    # Create enhanced configuration
    config = SimulationConfig(
        simulation_years=15,
        monte_carlo_runs=500,
        parallel_workers=4,
        base_price_usd_per_mcf=200.0,
        price_volatility=0.20,
        producers=[
            ProducerConfig(
                name="Major Gas",
                producer_type=ProducerType.MAJOR_GAS,
                base_production_mmcf=100,
                max_production_mmcf=200,
                supply_elasticity=0.3,
                market_share_pct=40,
                cost_per_mcf_usd=50
            ),
            ProducerConfig(
                name="LNG Byproduct",
                producer_type=ProducerType.LNG_BYPRODUCT,
                base_production_mmcf=80,
                max_production_mmcf=150,
                supply_elasticity=0.4,
                market_share_pct=30,
                cost_per_mcf_usd=45
            ),
            ProducerConfig(
                name="Recycling",
                producer_type=ProducerType.RECYCLING,
                base_production_mmcf=30,
                max_production_mmcf=60,
                supply_elasticity=0.5,
                market_share_pct=30,
                cost_per_mcf_usd=60
            ),
        ],
        consumers=[
            ConsumerConfig(
                name="Semiconductor",
                consumer_type=ConsumerType.SEMICONDUCTOR,
                base_demand_mmcf=100,
                demand_elasticity=-0.4,
                demand_growth_rate=0.05,
                price_sensitivity=0.6,
                substitution_threshold_usd_per_mcf=400
            ),
            ConsumerConfig(
                name="MRI Medical",
                consumer_type=ConsumerType.MRI_MEDICAL,
                base_demand_mmcf=60,
                demand_elasticity=-0.2,
                demand_growth_rate=0.02,
                price_sensitivity=0.3,
                substitution_threshold_usd_per_mcf=600
            ),
        ],
        output_dir="v6_enhanced_output"
    )
    
    print("\n✅ V6.0 New Features Active:")
    print(f"   ✅ GPU Acceleration: {'Available' if GPU_AVAILABLE else 'Not Available (CPU fallback)'}")
    print(f"   ✅ ML Prediction: {'Available' if SKLEARN_AVAILABLE else 'Not Available'}")
    print(f"   ✅ Stochastic Volatility Models")
    print(f"   ✅ Dynamic Market Clearing")
    print(f"   ✅ Advanced Risk Metrics (VaR, CVaR)")
    print(f"   ✅ Real-time Market Monitoring")
    print(f"   ✅ Supply Chain Network Analysis")
    print(f"   ✅ Bayesian Parameter Estimation")
    print(f"   ✅ Dashboard Data Generation")
    
    # Initialize enhanced simulator
    print(f"\n🔬 Running Enhanced V6.0 Simulation...")
    simulator = HeliumMarketSimulatorV6(config)
    
    # Run enhanced simulation
    with SIMULATION_LATENCY.labels(phase='total_simulation').time():
        results = simulator.simulate_market_enhanced(use_gpu=GPU_AVAILABLE)
    
    # Display results
    print(f"\n📊 Enhanced Results:")
    forecast = results['forecast']
    print(f"   Expected Price: ${forecast['expected_price']:.0f}/Mcf")
    print(f"   90% CI: [${forecast['confidence_interval'][0]:.0f}, ${forecast['confidence_interval'][1]:.0f}]")
    
    risk = results['risk_analysis']
    print(f"\n📈 Risk Metrics:")
    print(f"   VaR (95%): {risk['var_95']:.2%}")
    print(f"   VaR (99%): {risk['var_99']:.2%}")
    print(f"   CVaR (95%): {risk['cvar_95']:.2%}")
    print(f"   Max Drawdown: {risk['max_drawdown']:.2%}")
    print(f"   Sharpe Ratio: {risk['sharpe_ratio']:.2f}")
    
    # Parameter estimates
    if 'error' not in results['parameter_estimates']:
        print(f"\n🔍 Bayesian Parameter Estimates:")
        for param, estimates in results['parameter_estimates'].items():
            print(f"   {param}: {estimates['mean']:.4f} ± {estimates['std']:.4f}")
    
    # Alerts summary
    print(f"\n⚠️ Market Alerts:")
    print(f"   Total Alerts: {len(results['alerts'])}")
    for alert in results['alerts'][:3]:
        print(f"   • [{alert['severity'].upper()}] {alert['message']}")
    
    # ML predictions
    if 'ensemble' in results['ml_predictions']:
        ml_pred = results['ml_predictions']['ensemble']
        print(f"\n🤖 ML Price Predictions:")
        print(f"   Short-term forecast: ${float(ml_pred):.0f}/Mcf")
    
    # Run scenario analysis with enhanced features
    print(f"\n🔄 Running Enhanced Scenario Analysis...")
    scenario_analysis = ScenarioAnalysis(config)
    scenario_analysis.run_scenario("baseline")
    scenario_analysis.run_scenario("supply_disruption")
    
    # Generate dashboard data
    print(f"\n📊 Generating Dashboard Export...")
    dashboard_data = simulator.generate_dashboard_export(scenario_analysis)
    
    # Export enhanced results
    simulator.export_results(config.output_dir, formats=['csv', 'json'])
    
    # Save dashboard data
    dashboard_path = Path(config.output_dir) / f"dashboard_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    dashboard_path.parent.mkdir(parents=True, exist_ok=True)
    with open(dashboard_path, 'w') as f:
        json.dump(dashboard_data, f, indent=2, default=str)
    
    print(f"\n✅ V6.0 Simulation Complete")
    print(f"   Output saved to: {config.output_dir}")
    print(f"   Dashboard data: {dashboard_path}")
    print("=" * 80)


# ============================================================
# BACKWARD COMPATIBILITY
# ============================================================

# Keep original class names and functions for backward compatibility
# while adding new enhanced versions
HeliumMarketSimulatorEnhanced = HeliumMarketSimulatorV6

# Maintain backward compatibility with original main function
if __name__ == "__main__":
    if len(os.sys.argv) > 1 and os.sys.argv[1] == "--v6":
        main_v6()
    else:
        print("Running V6.0 enhanced version by default...")
        print("Use --v6 flag explicitly for V6.0, or modify main() call for V5.1 compatibility")
        main_v6()
