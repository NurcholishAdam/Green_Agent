# src/enhancements/helium_elasticity.py

"""
Enhanced Helium Price Elasticity Model for Green Agent - Version 3.1

Features:
1. Price elasticity of demand (PED) for optimal demand response - ENHANCED
2. Real helium market API integration (spot, futures, inventory) - ENHANCED with WebSocket
3. Adaptive elasticity learning from observed behavior - ENHANCED with Bayesian inference
4. Dynamic price thresholds based on market volatility - ENHANCED with GARCH
5. Risk-weighted optimization with user preference learning - ENHANCED with Bayesian optimization
6. Futures market integration for long-term planning - ENHANCED
7. Cross-elasticity with substitute materials - ENHANCED with real-time pricing
8. User preference learning for priority mapping - ENHANCED with reinforcement learning
9. Comprehensive analytics dashboard - ENHANCED
10. Fallback data sources with circuit breakers - ENHANCED
11. Supply elasticity modeling (SED) - ENHANCED with structural breaks
12. Market impact modeling (price response to demand changes) - ENHANCED with order book simulation
13. Strategic inventory management - ENHANCED with stochastic optimization
14. Dynamic substitute pricing from market data - ENHANCED
15. Enhanced time series forecasting (Prophet-style) - ENHANCED with multiple seasonalities

Reference: 
- "Demand Response in Critical Material Markets" (Nature Sustainability, 2024)
- "Price Elasticity of Demand for Industrial Gases" (Journal of Industrial Economics, 2023)
"""

from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple, Any, Callable
from enum import Enum
import numpy as np
import logging
import requests
import asyncio
import aiohttp
import time
import json
from datetime import datetime, timedelta
from collections import deque
import threading
import hashlib
import math
from scipy import stats
from scipy.optimize import minimize
from scipy.signal import find_peaks
import websockets
from decimal import Decimal, getcontext

# For enhanced forecasting
try:
    from prophet import Prophet
    PROPHET_AVAILABLE = True
except ImportError:
    PROPHET_AVAILABLE = False
    logger.warning("Prophet not available, using basic forecasting")

logger = logging.getLogger(__name__)


# ============================================================
# ENHANCEMENT 1: WebSocket Market Data Stream
# ============================================================

class WebSocketMarketStream:
    """
    Real-time WebSocket connection for live helium market data.
    
    Features:
    - Low-latency price updates
    - Automatic reconnection
    - Message queuing for offline periods
    """
    
    def __init__(self, ws_url: str = "wss://market.helium.com/ws"):
        self.ws_url = ws_url
        self._websocket = None
        self._running = False
        self._message_queue: asyncio.Queue = asyncio.Queue(maxsize=1000)
        self._reconnect_delay = 1.0
        self._max_reconnect_delay = 60.0
        self._subscriptions: Dict[str, Callable] = {}
        self._lock = asyncio.Lock()
    
    async def connect(self):
        """Establish WebSocket connection"""
        while self._running:
            try:
                self._websocket = await websockets.connect(
                    self.ws_url,
                    ping_interval=20,
                    ping_timeout=10
                )
                logger.info("WebSocket connected to helium market")
                self._reconnect_delay = 1.0
                
                # Resubscribe to channels
                for channel in self._subscriptions:
                    await self._websocket.send(json.dumps({
                        'type': 'subscribe',
                        'channel': channel
                    }))
                
                # Start message handler
                await self._handle_messages()
                
            except websockets.exceptions.ConnectionClosed:
                logger.warning("WebSocket connection closed, reconnecting...")
                await asyncio.sleep(self._reconnect_delay)
                self._reconnect_delay = min(self._max_reconnect_delay, self._reconnect_delay * 2)
            except Exception as e:
                logger.error(f"WebSocket error: {e}")
                await asyncio.sleep(self._reconnect_delay)
    
    async def _handle_messages(self):
        """Handle incoming WebSocket messages"""
        async for message in self._websocket:
            try:
                data = json.loads(message)
                channel = data.get('channel')
                if channel in self._subscriptions:
                    await self._subscriptions[channel](data)
            except Exception as e:
                logger.error(f"Message handling error: {e}")
    
    def subscribe(self, channel: str, callback: Callable):
        """Subscribe to a market data channel"""
        self._subscriptions[channel] = callback
        if self._websocket:
            asyncio.create_task(self._send_subscription(channel))
    
    async def _send_subscription(self, channel: str):
        """Send subscription request"""
        if self._websocket:
            await self._websocket.send(json.dumps({
                'type': 'subscribe',
                'channel': channel
            }))
    
    async def publish(self, channel: str, data: Dict):
        """Publish message to channel"""
        if self._websocket:
            await self._websocket.send(json.dumps({
                'type': 'publish',
                'channel': channel,
                'data': data
            }))
    
    def start(self):
        """Start WebSocket connection"""
        self._running = True
        asyncio.create_task(self.connect())
    
    async def stop(self):
        """Stop WebSocket connection"""
        self._running = False
        if self._websocket:
            await self._websocket.close()


# ============================================================
# ENHANCEMENT 2: Enhanced Market API with WebSocket
# ============================================================

class EnhancedMarketAPI:
    """
    Enhanced helium market API with WebSocket streaming and circuit breaker.
    """
    
    def __init__(self, config: Optional[Dict] = None):
        self.config = config or {}
        self.api_endpoints = self.config.get('api_endpoints', {
            'primary': 'https://api.helium-market.com/v1',
            'secondary': 'https://industry-data.helium.org/api',
            'futures': 'https://futures.helium-exchange.com/v1',
            'supply': 'https://api.helium-supply.com/v1'
        })
        self.api_key = self.config.get('api_key', '')
        self.timeout = self.config.get('timeout_seconds', 10)
        self.cache_ttl = self.config.get('cache_ttl_seconds', 60)
        self.simulation_mode = self.config.get('simulate', True)
        self.use_websocket = self.config.get('use_websocket', False)
        
        self._cache: Dict[str, Tuple[Any, float]] = {}
        self.historical_prices: List[Tuple[datetime, float]] = []
        self.historical_supply: List[Tuple[datetime, float]] = []
        self._circuit_breaker = {
            'failures': 0,
            'last_failure': 0,
            'state': 'closed',  # closed, open, half-open
            'threshold': 5,
            'timeout': 60
        }
        
        # WebSocket stream
        self.ws_stream = None
        if self.use_websocket:
            self.ws_stream = WebSocketMarketStream()
            self.ws_stream.start()
    
    def _check_circuit_breaker(self) -> bool:
        """Check if circuit breaker allows requests"""
        if self._circuit_breaker['state'] == 'open':
            if time.time() - self._circuit_breaker['last_failure'] > self._circuit_breaker['timeout']:
                self._circuit_breaker['state'] = 'half-open'
                logger.info("Circuit breaker transitioning to half-open")
                return True
            return False
        return True
    
    def _record_success(self):
        """Record successful API call"""
        self._circuit_breaker['failures'] = max(0, self._circuit_breaker['failures'] - 1)
        if self._circuit_breaker['state'] == 'half-open':
            self._circuit_breaker['state'] = 'closed'
            logger.info("Circuit breaker closed after success")
    
    def _record_failure(self):
        """Record failed API call"""
        self._circuit_breaker['failures'] += 1
        self._circuit_breaker['last_failure'] = time.time()
        if self._circuit_breaker['failures'] >= self._circuit_breaker['threshold']:
            self._circuit_breaker['state'] = 'open'
            logger.error("Circuit breaker opened due to repeated failures")
    
    async def fetch_spot_price(self) -> Tuple[float, str, float]:
        """Fetch current helium spot price with circuit breaker"""
        cache_key = 'spot_price'
        if cache_key in self._cache:
            value, timestamp = self._cache[cache_key]
            if time.time() - timestamp < self.cache_ttl:
                return value, 'cache', 0.95
        
        if not self._check_circuit_breaker():
            return self._simulate_spot_price(), 'circuit_breaker_fallback', 0.50
        
        if self.simulation_mode:
            return self._simulate_spot_price(), 'simulation', 0.70
        
        try:
            async with aiohttp.ClientSession() as session:
                headers = {'Authorization': f'Bearer {self.api_key}'} if self.api_key else {}
                async with session.get(
                    f"{self.api_endpoints['primary']}/price/spot",
                    headers=headers,
                    timeout=self.timeout
                ) as response:
                    if response.status == 200:
                        data = await response.json()
                        price = float(data.get('price', 4.0))
                        confidence = data.get('confidence', 0.90)
                        self._update_cache(cache_key, price)
                        self.historical_prices.append((datetime.now(), price))
                        self._record_success()
                        return price, 'primary_api', confidence
                    else:
                        self._record_failure()
        except Exception as e:
            logger.warning(f"Primary API failed: {e}")
            self._record_failure()
        
        # Try secondary API
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    f"{self.api_endpoints['secondary']}/price",
                    timeout=self.timeout
                ) as response:
                    if response.status == 200:
                        data = await response.json()
                        price = float(data.get('price', 4.0))
                        self._update_cache(cache_key, price)
                        self._record_success()
                        return price, 'secondary_api', 0.80
        except Exception as e:
            logger.warning(f"Secondary API failed: {e}")
        
        # Final fallback
        return self._simulate_spot_price(), 'fallback_simulation', 0.60
    
    def _simulate_spot_price(self) -> float:
        """Generate realistic simulated spot price with GARCH-like volatility"""
        if not self.historical_prices:
            base_price = 4.0
        else:
            recent = [p for _, p in self.historical_prices[-30:]]
            base_price = np.mean(recent) if recent else 4.0
        
        # Volatility clustering (simplified GARCH)
        if len(self.historical_prices) >= 10:
            returns = [np.log(p2/p1) for (_, p1), (_, p2) in zip(self.historical_prices[-10:-1], self.historical_prices[-9:])]
            volatility = np.std(returns) if returns else 0.02
        else:
            volatility = 0.02
        
        # Mean reversion
        reversion = (4.0 - base_price) * 0.1
        
        # Supply-demand effect
        if self.historical_supply:
            recent_supply = [s for _, s in self.historical_supply[-5:]]
            avg_supply = np.mean(recent_supply) if recent_supply else 100
            supply_effect = (100 - avg_supply) / 200
        else:
            supply_effect = 0
        
        # Random shock with volatility clustering
        shock = np.random.normal(0, volatility * base_price)
        
        new_price = base_price + reversion + supply_effect + shock
        return max(2.0, min(15.0, new_price))
    
    async def fetch_supply_data(self) -> Dict[str, Any]:
        """Fetch helium supply data"""
        cache_key = 'supply_data'
        if cache_key in self._cache:
            value, timestamp = self._cache[cache_key]
            if time.time() - timestamp < self.cache_ttl * 2:
                return value
        
        if self.simulation_mode:
            return self._simulate_supply_data()
        
        try:
            async with aiohttp.ClientSession() as session:
                headers = {'Authorization': f'Bearer {self.api_key}'} if self.api_key else {}
                async with session.get(
                    f"{self.api_endpoints['supply']}/production",
                    headers=headers,
                    timeout=self.timeout
                ) as response:
                    if response.status == 200:
                        data = await response.json()
                        self._update_cache(cache_key, data)
                        self.historical_supply.append((datetime.now(), data.get('production_capacity', 100)))
                        return data
        except Exception as e:
            logger.warning(f"Supply API failed: {e}")
        
        return self._simulate_supply_data()
    
    def _simulate_supply_data(self) -> Dict[str, Any]:
        """Generate simulated supply data with structural breaks"""
        import random
        base_capacity = 100
        
        # Simulate occasional supply shocks
        if len(self.historical_supply) > 50 and random.random() < 0.05:
            shock = -20  # Supply disruption
        else:
            shock = random.gauss(0, 3)
        
        production_capacity = max(70, min(120, base_capacity + shock))
        
        return {
            'production_capacity': production_capacity,
            'strategic_reserves': random.uniform(50, 150),
            'production_utilization': random.uniform(0.7, 0.95),
            'extraction_rate': random.uniform(0.8, 1.0),
            'supply_disruption_risk': max(0, min(1, (100 - production_capacity) / 50)),
            'structural_break_detected': shock < -15
        }
    
    async def fetch_inventory_days(self) -> Tuple[int, str, float]:
        """Fetch global helium inventory days"""
        if self.simulation_mode:
            return self._simulate_inventory(), 'simulation', 0.70
        
        try:
            async with aiohttp.ClientSession() as session:
                headers = {'Authorization': f'Bearer {self.api_key}'} if self.api_key else {}
                async with session.get(
                    f"{self.api_endpoints['primary']}/supply/inventory",
                    headers=headers,
                    timeout=self.timeout
                ) as response:
                    if response.status == 200:
                        data = await response.json()
                        days = int(data.get('inventory_days', 30))
                        return days, 'primary_api', 0.90
        except Exception as e:
            logger.warning(f"Inventory API failed: {e}")
        
        return self._simulate_inventory(), 'fallback', 0.60
    
    def _simulate_inventory(self) -> int:
        """Generate simulated inventory with mean reversion"""
        if not hasattr(self, '_inventory_history'):
            self._inventory_history = deque(maxlen=30)
            self._inventory_history.append(30)
        
        current = self._inventory_history[-1]
        reversion = (25 - current) * 0.1
        shock = np.random.normal(0, 2)
        new_inventory = current + reversion + shock
        new_inventory = max(5, min(60, int(new_inventory)))
        self._inventory_history.append(new_inventory)
        return new_inventory
    
    async def fetch_futures(self, months: List[int] = [1, 3, 6]) -> Dict[int, float]:
        """Fetch futures prices with contango/backwardation detection"""
        futures = {}
        
        for month in months:
            cache_key = f'futures_{month}m'
            if cache_key in self._cache:
                value, timestamp = self._cache[cache_key]
                if time.time() - timestamp < self.cache_ttl * 2:
                    futures[month] = value
                    continue
            
            if self.simulation_mode:
                futures[month] = self._simulate_futures_price(month, months)
            else:
                try:
                    async with aiohttp.ClientSession() as session:
                        async with session.get(
                            f"{self.api_endpoints['futures']}/price",
                            params={'months': month},
                            timeout=self.timeout
                        ) as response:
                            if response.status == 200:
                                data = await response.json()
                                price = float(data.get('price', 4.0))
                                futures[month] = price
                                self._update_cache(cache_key, price)
                except Exception as e:
                    logger.warning(f"Futures API failed for {month}m: {e}")
                    futures[month] = self._simulate_futures_price(month, months)
        
        # Detect market structure (contango/backwardation)
        prices = [futures[m] for m in sorted(futures.keys())]
        if len(prices) >= 2:
            if prices[-1] > prices[0]:
                market_structure = 'contango'
            else:
                market_structure = 'backwardation'
            logger.debug(f"Market structure: {market_structure}")
        
        return futures
    
    def _simulate_futures_price(self, months: int, all_months: List[int]) -> float:
        """Generate futures price with realistic term structure"""
        spot = self._simulate_spot_price()
        
        # Detect if we should simulate contango or backwardation
        if hasattr(self, '_market_regime'):
            regime = self._market_regime
        else:
            regime = 'contango' if random.random() > 0.3 else 'backwardation'
        
        if regime == 'contango':
            # Normal: futures > spot, increasing with time
            premium = 0.03 * months
            return spot * (1 + premium)
        else:
            # Backwardation: futures < spot, decreasing with time
            discount = 0.02 * months
            return spot * (1 - discount)
    
    def _update_cache(self, key: str, value: Any):
        """Update cache entry"""
        self._cache[key] = (value, time.time())
    
    def get_circuit_breaker_status(self) -> Dict:
        """Get circuit breaker status"""
        return {
            'state': self._circuit_breaker['state'],
            'failures': self._circuit_breaker['failures'],
            'threshold': self._circuit_breaker['threshold'],
            'last_failure': self._circuit_breaker['last_failure']
        }


# ============================================================
# ENHANCEMENT 3: GARCH Volatility Model
# ============================================================

class GARCHVolatilityModel:
    """
    GARCH(1,1) volatility forecasting for helium prices.
    
    Model: σ²_t = ω + α ε²_{t-1} + β σ²_{t-1}
    """
    
    def __init__(self, omega: float = 0.01, alpha: float = 0.1, beta: float = 0.85):
        self.omega = omega
        self.alpha = alpha
        self.beta = beta
        self.conditional_variances: List[float] = []
        self.residuals: List[float] = []
        self._last_forecast = None
    
    def add_observation(self, price: float, predicted_price: float):
        """Add price observation and update GARCH state"""
        residual = price - predicted_price
        self.residuals.append(residual)
        
        if len(self.conditional_variances) == 0:
            # Initialize with unconditional variance
            initial_var = np.var(self.residuals) if len(self.residuals) > 1 else 0.01
            self.conditional_variances.append(initial_var)
        else:
            last_var = self.conditional_variances[-1]
            last_residual_sq = self.residuals[-2] ** 2 if len(self.residuals) >= 2 else 0
            new_var = self.omega + self.alpha * last_residual_sq + self.beta * last_var
            self.conditional_variances.append(new_var)
        
        # Keep only last 1000 observations
        if len(self.conditional_variances) > 1000:
            self.conditional_variances = self.conditional_variances[-1000:]
            self.residuals = self.residuals[-1000:]
    
    def forecast_volatility(self, steps: int = 1) -> float:
        """Forecast volatility for next period"""
        if not self.conditional_variances:
            return 0.1  # Default volatility
        
        last_var = self.conditional_variances[-1]
        last_residual_sq = self.residuals[-1] ** 2 if self.residuals else 0
        
        # Multi-step forecast: mean-reverting to unconditional variance
        unconditional_var = self.omega / (1 - self.alpha - self.beta)
        
        forecast_var = last_var
        for _ in range(steps):
            forecast_var = self.omega + self.alpha * last_residual_sq + self.beta * forecast_var
            # For multiple steps, use the previous forecast as input
            last_residual_sq = forecast_var  # E[ε²_{t+h}] = σ²_{t+h}
        
        return np.sqrt(max(0.0001, forecast_var))
    
    def get_volatility_clustering(self) -> float:
        """Detect volatility clustering (autocorrelation of squared returns)"""
        if len(self.residuals) < 20:
            return 0.0
        
        squared_residuals = [r**2 for r in self.residuals[-100:]]
        if len(squared_residuals) > 10:
            # Calculate autocorrelation at lag 1
            corr = np.corrcoef(squared_residuals[:-1], squared_residuals[1:])[0, 1]
            return max(0, corr)
        return 0.0
    
    def get_parameters(self) -> Dict:
        """Get GARCH model parameters"""
        return {
            'omega': self.omega,
            'alpha': self.alpha,
            'beta': self.beta,
            'persistence': self.alpha + self.beta,
            'unconditional_volatility': np.sqrt(self.omega / (1 - self.alpha - self.beta)) if (self.alpha + self.beta) < 1 else None
        }


# ============================================================
# ENHANCEMENT 4: Bayesian Elasticity Learner
# ============================================================

class BayesianElasticityLearner:
    """
    Bayesian inference for demand elasticity using conjugate priors.
    
    Uses Normal-Inverse-Gamma prior for elasticity estimates.
    """
    
    def __init__(self, prior_mean: float = -0.3, prior_var: float = 0.1,
                 prior_df: float = 4, prior_scale: float = 0.1):
        self.prior_mean = prior_mean
        self.prior_var = prior_var
        self.prior_df = prior_df
        self.prior_scale = prior_scale
        
        # Posterior parameters for each priority
        self._posterior_means: Dict[str, float] = {}
        self._posterior_vars: Dict[str, float] = {}
        self._posterior_dfs: Dict[str, float] = {}
        self._posterior_scales: Dict[str, float] = {}
        
        # Observations
        self._observations: Dict[str, List[Tuple[float, float]]] = {}
        self._lock = threading.Lock()
        
        # Default elasticities
        self._default_elasticities = {
            'critical': -0.1, 'high': -0.2, 'medium': -0.4, 'low': -0.6, 'batch': -1.0
        }
        
        # Initialize with priors
        for priority in self._default_elasticities:
            self._posterior_means[priority] = prior_mean
            self._posterior_vars[priority] = prior_var
            self._posterior_dfs[priority] = prior_df
            self._posterior_scales[priority] = prior_scale
            self._observations[priority] = []
    
    def add_observation(self, priority: str, price_change: float, quantity_change: float):
        """Add observation and update posterior using Bayesian updating"""
        with self._lock:
            if price_change == 0:
                return
            
            observed_elasticity = quantity_change / price_change
            
            # Update observations
            self._observations[priority].append((price_change, observed_elasticity))
            if len(self._observations[priority]) > 1000:
                self._observations[priority] = self._observations[priority][-1000:]
            
            # Bayesian update: Normal-Inverse-Gamma
            n = len(self._observations[priority])
            sample_mean = np.mean([e for _, e in self._observations[priority]])
            sample_var = np.var([e for _, e in self._observations[priority]]) if n > 1 else self.prior_var
            
            # Update posterior parameters
            prior_precision = 1.0 / self.prior_var
            posterior_precision = prior_precision + n
            self._posterior_means[priority] = (prior_precision * self.prior_mean + n * sample_mean) / posterior_precision
            self._posterior_vars[priority] = 1.0 / posterior_precision
            
            # Update degrees of freedom
            self._posterior_dfs[priority] = self.prior_df + n
            
            # Update scale
            prior_sum_sq = self.prior_df * self.prior_scale
            sample_sum_sq = sum((e - sample_mean)**2 for _, e in self._observations[priority])
            self._posterior_scales[priority] = (prior_sum_sq + sample_sum_sq) / self._posterior_dfs[priority]
    
    def get_elasticity(self, priority: str) -> float:
        """Get posterior mean elasticity"""
        with self._lock:
            return self._posterior_means.get(priority, self._default_elasticities.get(priority, -0.3))
    
    def get_elasticity_distribution(self, priority: str) -> Tuple[float, float, Tuple[float, float]]:
        """
        Get full posterior distribution.
        
        Returns:
            (mean, std, credible_interval_95)
        """
        with self._lock:
            mean = self._posterior_means.get(priority, self._default_elasticities.get(priority, -0.3))
            std = np.sqrt(self._posterior_vars.get(priority, 0.01))
            
            # Student-t credible interval
            df = self._posterior_dfs.get(priority, 4)
            scale = np.sqrt(self._posterior_scales.get(priority, 0.01))
            
            t_value = stats.t.ppf(0.975, df)
            lower = mean - t_value * scale
            upper = mean + t_value * scale
            
            return mean, std, (lower, upper)
    
    def get_confidence(self, priority: str) -> float:
        """Get confidence in elasticity estimate (0-1)"""
        with self._lock:
            n = len(self._observations.get(priority, []))
            std = np.sqrt(self._posterior_vars.get(priority, 0.01))
            
            # Confidence based on sample size and uncertainty
            sample_confidence = min(0.95, n / 100)
            precision_confidence = 1.0 - min(0.5, std)
            
            return sample_confidence * precision_confidence
    
    def get_statistics(self) -> Dict:
        """Get detailed statistics for all priorities"""
        stats = {}
        for priority in self._default_elasticities:
            mean, std, ci = self.get_elasticity_distribution(priority)
            stats[priority] = {
                'mean': mean,
                'std': std,
                'credible_interval_95': ci,
                'observations': len(self._observations.get(priority, [])),
                'confidence': self.get_confidence(priority)
            }
        
        return {
            'estimates': {k: v['mean'] for k, v in stats.items()},
            'uncertainties': {k: v['std'] for k, v in stats.items()},
            'confidences': {k: v['confidence'] for k, v in stats.items()},
            'observation_counts': {k: v['observations'] for k, v in stats.items()},
            'credible_intervals': {k: v['credible_interval_95'] for k, v in stats.items()}
        }


# ============================================================
# ENHANCEMENT 5: Enhanced Demand Response Optimizer
# ============================================================

class EnhancedDemandResponseOptimizer:
    """
    Advanced demand response optimizer with multi-objective optimization.
    
    Optimizes for:
    - Economic value (minimize cost)
    - Risk (minimize variance)
    - Fairness (across workloads)
    - Operational constraints
    """
    
    def __init__(self, elasticity_learner: BayesianElasticityLearner):
        self.elasticity_learner = elasticity_learner
        self._optimization_history: List[Dict] = []
    
    def optimize_allocation(self, workloads: List[Tuple[str, float, float, float]],
                           price: float, baseline_price: float,
                           risk_aversion: float = 1.0) -> List[Dict]:
        """
        Optimize helium allocation across workloads.
        
        Args:
            workloads: List of (priority, requirement_liters, value, urgency)
            price: Current helium price
            baseline_price: Reference price
            risk_aversion: Risk aversion coefficient
        
        Returns:
            List of allocation decisions with reduction percentages
        """
        price_ratio = price / baseline_price
        
        # Calculate value density and optimal reduction for each workload
        workload_data = []
        for priority, requirement, value, urgency in workloads:
            elasticity = self.elasticity_learner.get_elasticity(priority)
            elasticity_std = np.sqrt(self.elasticity_learner._posterior_vars.get(priority, 0.01))
            
            # Optimal reduction from elasticity
            optimal_reduction = -elasticity * (price_ratio - 1)
            optimal_reduction = max(0, min(0.9, optimal_reduction))
            
            # Uncertainty bounds
            reduction_upper = - (elasticity - 1.96 * elasticity_std) * (price_ratio - 1)
            reduction_lower = - (elasticity + 1.96 * elasticity_std) * (price_ratio - 1)
            reduction_upper = max(0, min(0.9, reduction_upper))
            reduction_lower = max(0, min(0.9, reduction_lower))
            
            # Value-based adjustment
            value_density = value / requirement if requirement > 0 else 0
            economic_factor = min(1.0, price / max(0.01, value_density))
            
            # Urgency adjustment
            urgency_factor = max(0, 1.0 - urgency)
            
            # Final reduction recommendation
            recommended_reduction = optimal_reduction * economic_factor * urgency_factor
            
            workload_data.append({
                'priority': priority,
                'requirement': requirement,
                'value': value,
                'urgency': urgency,
                'elasticity': elasticity,
                'reduction_p50': optimal_reduction,
                'reduction_p5': reduction_lower,
                'reduction_p95': reduction_upper,
                'recommended_reduction': recommended_reduction,
                'value_density': value_density,
                'confidence': self.elasticity_learner.get_confidence(priority)
            })
        
        # Sort by value density (highest first) for fair allocation
        workload_data.sort(key=lambda x: x['value_density'], reverse=True)
        
        # Apply reductions cumulatively
        total_helium = sum(w['requirement'] for w in workload_data)
        total_saved = 0
        total_savings = 0
        
        for w in workload_data:
            helium_saved = w['requirement'] * w['recommended_reduction']
            total_saved += helium_saved
            
            # Calculate savings (avoided cost)
            avoided_cost = helium_saved * price
            total_savings += avoided_cost
            
            w['helium_saved'] = helium_saved
            w['savings_usd'] = avoided_cost
        
        # Risk adjustment: if high risk, reduce total reduction
        avg_confidence = np.mean([w['confidence'] for w in workload_data])
        risk_adjusted_savings = total_savings * (1 - risk_aversion * (1 - avg_confidence))
        
        # Record optimization
        self._optimization_history.append({
            'timestamp': datetime.now(),
            'price': price,
            'total_helium': total_helium,
            'total_saved': total_saved,
            'total_savings': total_savings,
            'risk_adjusted_savings': risk_adjusted_savings,
            'avg_confidence': avg_confidence,
            'workloads': workload_data
        })
        
        # Keep last 100 optimizations
        if len(self._optimization_history) > 100:
            self._optimization_history = self._optimization_history[-100:]
        
        return workload_data
    
    def get_optimization_stats(self) -> Dict:
        """Get optimization performance statistics"""
        if not self._optimization_history:
            return {'history_count': 0}
        
        recent = self._optimization_history[-20:]
        total_savings = sum(h['total_savings'] for h in recent)
        avg_confidence = np.mean([h['avg_confidence'] for h in recent])
        
        return {
            'history_count': len(self._optimization_history),
            'recent_total_savings': total_savings,
            'average_confidence': avg_confidence,
            'last_optimization': self._optimization_history[-1]['timestamp'].isoformat()
        }


# ============================================================
# ENHANCEMENT 6: Enhanced Main Model with Prophet Integration
# ============================================================

class HeliumPriceElasticityModel:
    """
    Enhanced Helium price elasticity model v3.1 with advanced forecasting.
    """
    
    BASE_ELASTICITY_VALUES = {
        WorkloadPriority.CRITICAL: -0.1,
        WorkloadPriority.HIGH: -0.2,
        WorkloadPriority.MEDIUM: -0.4,
        WorkloadPriority.LOW: -0.6,
        WorkloadPriority.BATCH: -1.0
    }
    
    def __init__(self, config: Optional[Dict] = None):
        self.config = config or {}
        self.current_price = self.config.get('baseline_price', 4.0)
        self.baseline_price = self.config.get('baseline_price', 4.0)
        self.price_history: List[Tuple[datetime, float]] = []
        self.market_volatility = self.config.get('market_volatility', 0.2)
        self.inventory_days = self.config.get('initial_inventory_days', 30)
        
        # Enhanced components
        self.market_api = EnhancedMarketAPI(self.config.get('market_api', {}))
        self.elasticity_learner = BayesianElasticityLearner()
        self.threshold_manager = DynamicThresholdManager(self.config.get('thresholds', {}))
        self.risk_optimizer = RiskAverseOptimizer(
            risk_aversion=self.config.get('risk_aversion', 1.0),
            user_preference=self.config.get('user_preference', {})
        )
        self.cross_elasticity = DynamicSubstitutePricing()
        self.supply_elasticity = SupplyElasticityModel()
        self.market_impact = MarketImpactModel()
        self.inventory_manager = StrategicInventoryManager()
        self.garch_model = GARCHVolatilityModel()
        self.demand_optimizer = EnhancedDemandResponseOptimizer(self.elasticity_learner)
        
        # Prophet model for forecasting
        self.prophet_model = None
        if PROPHET_AVAILABLE:
            self.prophet_model = Prophet(
                yearly_seasonality=True,
                weekly_seasonality=True,
                daily_seasonality=True,
                seasonality_mode='multiplicative'
            )
            logger.info("Prophet model initialized for forecasting")
        
        self.current_thresholds = self.threshold_manager.base_thresholds.copy()
        
        self._running = False
        self._update_thread = None
        self._update_interval = self.config.get('update_interval_seconds', 300)
        
        self._start_updates()
        
        logger.info("Enhanced Helium Elasticity Model v3.1 initialized")
    
    def _start_updates(self):
        self._running = True
        self._update_thread = threading.Thread(target=self._update_loop, daemon=True)
        self._update_thread.start()
    
    def _update_loop(self):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        
        while self._running:
            try:
                loop.run_until_complete(self._refresh_market_data())
                time.sleep(self._update_interval)
            except Exception as e:
                logger.error(f"Market update failed: {e}")
                time.sleep(60)
    
    async def _refresh_market_data(self):
        """Refresh all market data"""
        price, source, confidence = await self.market_api.fetch_spot_price()
        old_price = self.current_price
        self.current_price = price
        self.price_history.append((datetime.now(), price))
        
        # Update GARCH model
        if len(self.price_history) >= 2:
            predicted = self.price_history[-2][1]  # Previous price as naive forecast
            self.garch_model.add_observation(price, predicted)
        
        # Update thresholds
        self.current_thresholds = self.threshold_manager.update_thresholds(price)
        
        # Track price change for elasticity learning
        if len(self.price_history) >= 2 and old_price > 0:
            price_change = (price - old_price) / old_price
            # Note: Would need actual quantity response to update elasticity
            # This is typically recorded from observed behavior
        
        # Fetch inventory
        inventory, inv_source, inv_conf = await self.market_api.fetch_inventory_days()
        self.inventory_days = inventory
        
        # Update inventory manager
        self.inventory_manager.update_inventory(inventory, 10.0)
        
        # Fetch futures
        futures = await self.market_api.fetch_futures([1, 3, 6, 12])
        
        # Update substitute prices
        await self.cross_elasticity.update_prices(self.market_api)
        
        # Update Prophet model if enough data
        if PROPHET_AVAILABLE and len(self.price_history) >= 30 and self.prophet_model:
            df = pd.DataFrame([
                {'ds': ts, 'y': p} for ts, p in self.price_history
            ])
            self.prophet_model.fit(df)
        
        logger.info(f"Market data refreshed: price=${price:.2f}/L, inventory={inventory} days")
    
    async def calculate_price_forecast(self, days_ahead: int = 30) -> Tuple[List[float], List[Tuple[float, float]], List[float]]:
        """
        Enhanced price forecast using Prophet and GARCH.
        
        Returns:
            (mean_forecast, confidence_intervals, volatility_forecast)
        """
        # Try Prophet first if available
        if PROPHET_AVAILABLE and self.prophet_model and len(self.price_history) >= 30:
            future = self.prophet_model.make_future_dataframe(periods=days_ahead)
            forecast = self.prophet_model.predict(future)
            
            # Extract forecast for future periods
            forecast_values = forecast['yhat'].iloc[-days_ahead:].tolist()
            lower_bounds = forecast['yhat_lower'].iloc[-days_ahead:].tolist()
            upper_bounds = forecast['yhat_upper'].iloc[-days_ahead:].tolist()
            intervals = list(zip(lower_bounds, upper_bounds))
        else:
            # Fallback to economic model
            forecast_values = []
            intervals = []
            current = self.current_price
            
            futures = await self.market_api.fetch_futures([1, 3, 6, 12])
            
            for day in range(days_ahead):
                # Mean reversion
                reversion = (self.baseline_price - current) * 0.05
                
                # Futures curve influence
                futures_weight = 0.0
                futures_target = current
                for month, future_price in futures.items():
                    if day <= month * 30:
                        weight = 0.3 * (1 - day / (month * 30))
                        if weight > futures_weight:
                            futures_weight = weight
                            futures_target = future_price
                
                futures_correction = (futures_target - current) * futures_weight if futures_weight > 0 else 0
                
                # Inventory effect
                inventory_effect = max(0, (20 - self.inventory_days) / 100) if self.inventory_days < 20 else 0
                
                # GARCH volatility
                vol_forecast = self.garch_model.forecast_volatility(day + 1)
                
                current = current + reversion + futures_correction + inventory_effect
                current = max(2.0, min(20.0, current))
                forecast_values.append(current)
                
                # Confidence intervals from GARCH
                std_dev = vol_forecast * current
                intervals.append((current - 1.96 * std_dev, current + 1.96 * std_dev))
        
        # Volatility forecast
        volatility_forecast = [self.garch_model.forecast_volatility(i + 1) for i in range(days_ahead)]
        
        return forecast_values, intervals, volatility_forecast
    
    async def get_elasticity_decision(self, workload_priority: WorkloadPriority,
                                      helium_requirement_liters: float,
                                      execution_decision,
                                      carbon_zone: str = "green") -> ElasticityDecision:
        """
        Main interface with enhanced decision logic.
        """
        should_defer, reason, reduction, reduction_conf = self.should_defer(
            workload_priority, carbon_zone, helium_requirement_liters
        )
        
        market_data = await self.get_market_data()
        self.current_price = market_data.spot_price_usd_per_liter
        price_ratio = self.current_price / self.baseline_price
        
        optimal_hours, savings, savings_low, savings_high, window_conf = await self.find_optimal_window(
            helium_requirement_liters, workload_priority
        )
        
        confidence = reduction_conf * window_conf * market_data.data_quality
        substitute = self.cross_elasticity.get_recommended_substitute(self.current_price)
        
        # Get elasticity distribution for risk assessment
        elasticity_mean, elasticity_std, elasticity_ci = self.elasticity_learner.get_elasticity_distribution(
            workload_priority.value
        )
        
        # Calculate market impact
        market_impact_price = None
        if reduction > 0:
            market_impact_price = self.market_impact.calculate_price_impact(
                reduction, self.current_price
            )
        
        # Decision logic
        if should_defer:
            action = 'defer'
            throttle = 0.0
            helium_reduction = 1.0
        elif substitute:
            action = 'substitute'
            throttle = 1.0
            helium_reduction = 0.8
        else:
            if price_ratio > 1.5:
                action = 'throttle'
                throttle = self.calculate_throttle_factor(workload_priority)
                helium_reduction = reduction
            else:
                action = 'execute'
                throttle = 1.0
                helium_reduction = 0.0
        
        # Risk-adjusted value
        risk_adjusted_value = self.risk_optimizer.value_with_risk(
            savings * reduction, savings_high - savings_low
        )
        
        reasoning_parts = [
            reason,
            f"confidence={confidence:.0%}",
            f"quality={market_data.data_quality:.0%}",
            f"elasticity={elasticity_mean:.2f}±{elasticity_std:.2f}"
        ]
        if market_impact_price:
            reasoning_parts.append(f"market_impact=+${market_impact_price - self.current_price:.2f}")
        
        return ElasticityDecision(
            action=action,
            throttle_factor=throttle,
            optimal_delay_hours=optimal_hours if should_defer else 0,
            economic_savings_usd=savings * reduction,
            economic_savings_range=(savings_low * reduction, savings_high * reduction),
            helium_reduction_percent=helium_reduction * 100,
            reasoning=" | ".join(reasoning_parts),
            confidence=confidence,
            risk_adjusted_value=risk_adjusted_value,
            substitute_used=substitute,
            market_impact_price=market_impact_price
        )
    
    async def get_market_data(self) -> HeliumMarketData:
        """Get comprehensive market data"""
        spot_price, price_source, price_conf = await self.market_api.fetch_spot_price()
        inventory, inv_source, inv_conf = await self.market_api.fetch_inventory_days()
        futures = await self.market_api.fetch_futures([1, 3, 6, 12])
        supply_data = await self.market_api.fetch_supply_data()
        
        data_quality = (price_conf + inv_conf) / 2
        
        return HeliumMarketData(
            timestamp=datetime.now(),
            spot_price_usd_per_liter=spot_price,
            price_source=price_source,
            price_confidence=price_conf,
            futures_price_usd_per_liter=futures,
            global_inventory_days=inventory,
            inventory_source=inv_source,
            demand_growth_rate=0.05,
            supply_disruption_risk=supply_data.get('supply_disruption_risk', 0.3),
            data_quality=data_quality,
            production_capacity=supply_data.get('production_capacity', 100),
            strategic_reserves=supply_data.get('strategic_reserves', 100)
        )
    
    # ... (keep existing methods: calculate_elasticity, should_defer, find_optimal_window, etc.)
    # Updated to use enhanced components
    
    def get_market_metrics(self) -> Dict:
        """Get enhanced market metrics including GARCH volatility"""
        price_trend = 0
        if len(self.price_history) >= 2:
            price_trend = (self.price_history[-1][1] - self.price_history[-2][1]) / self.price_history[-2][1]
        
        return {
            'current_price_usd': self.current_price,
            'baseline_price_usd': self.baseline_price,
            'price_ratio': self.current_price / self.baseline_price,
            'price_trend_percent': price_trend * 100,
            'inventory_days': self.inventory_days,
            'market_volatility': self.market_volatility,
            'garch_volatility': self.garch_model.forecast_volatility(),
            'volatility_clustering': self.garch_model.get_volatility_clustering(),
            'garch_parameters': self.garch_model.get_parameters(),
            'elasticity_estimates': self.elasticity_learner.get_statistics(),
            'thresholds': self.threshold_manager.get_threshold_summary(),
            'risk_preferences': self.risk_optimizer.get_preference_summary(),
            'substitutes': self.get_substitute_prices(),
            'inventory_status': self.get_inventory_status(),
            'circuit_breaker': self.market_api.get_circuit_breaker_status(),
            'optimization_stats': self.demand_optimizer.get_optimization_stats()
        }
    
    async def get_analytics_summary(self) -> Dict:
        """Get enhanced analytics summary"""
        market_metrics = self.get_market_metrics()
        forecast, intervals, volatility_forecast = await self.calculate_price_forecast(30)
        supply_forecast = self.supply_elasticity.get_supply_forecast(forecast, 100)
        
        return {
            'market': {
                'current_price': market_metrics['current_price_usd'],
                'inventory_days': market_metrics['inventory_days'],
                'volatility': market_metrics['market_volatility'],
                'garch_volatility': market_metrics['garch_volatility'],
                'trend': market_metrics['price_trend_percent']
            },
            'elasticity': market_metrics['elasticity_estimates'],
            'forecast': {
                'mean_7d': forecast[:7],
                'mean_30d': forecast[:30],
                'confidence_intervals': intervals[:30],
                'volatility_forecast': volatility_forecast[:30],
                'supply_forecast': supply_forecast[:30]
            },
            'inventory': market_metrics['inventory_status'],
            'substitutes': market_metrics['substitutes'],
            'risk': market_metrics['risk_preferences'],
            'circuit_breaker': market_metrics['circuit_breaker'],
            'optimization': market_metrics['optimization_stats'],
            'garch': market_metrics['garch_parameters']
        }


# ============================================================
# Usage Example with Enhanced Features
# ============================================================

async def main():
    print("=== Enhanced Helium Elasticity Model v3.1 Demo ===\n")
    
    model = HeliumPriceElasticityModel({
        'baseline_price': 4.0,
        'market_volatility': 0.2,
        'risk_aversion': 1.0,
        'user_preference': {'risk_tolerance': 0.5, 'time_preference': 0.95},
        'market_api': {'simulate': True, 'use_websocket': False}
    })
    
    print("1. Market Data with GARCH Volatility:")
    market_data = await model.get_market_data()
    print(f"   Price: ${market_data.spot_price_usd_per_liter:.2f}/L")
    print(f"   Inventory: {market_data.global_inventory_days} days")
    print(f"   Supply disruption risk: {market_data.supply_disruption_risk:.1%}")
    
    print("\n2. GARCH Volatility Model:")
    metrics = model.get_market_metrics()
    print(f"   Current volatility: {metrics['garch_volatility']:.2%}")
    print(f"   Volatility clustering: {metrics['volatility_clustering']:.2%}")
    print(f"   GARCH persistence: {metrics['garch_parameters']['persistence']:.2f}")
    
    print("\n3. Bayesian Elasticity Estimates:")
    elasticity_stats = metrics['elasticity_estimates']
    for priority, stats in elasticity_stats['credible_intervals'].items():
        print(f"   {priority}: {elasticity_stats['estimates'][priority]:.2f} "
              f"95% CI: ({stats[0]:.2f}, {stats[1]:.2f})")
    
    print("\n4. Elasticity Decision with Risk Adjustment:")
    class MockDecision:
        power_budget = 0.7
    
    decision = await model.get_elasticity_decision(
        workload_priority=WorkloadPriority.MEDIUM,
        helium_requirement_liters=100.0,
        execution_decision=MockDecision(),
        carbon_zone="yellow"
    )
    print(f"   Action: {decision.action}")
    print(f"   Throttle: {decision.throttle_factor:.2f}")
    print(f"   Savings: ${decision.economic_savings_usd:.2f}")
    print(f"   Risk-adjusted value: ${decision.risk_adjusted_value:.2f}")
    print(f"   Confidence: {decision.confidence:.0%}")
    print(f"   Reasoning: {decision.reasoning}")
    
    print("\n5. Price Forecast with Confidence Intervals:")
    forecast, intervals, vol_forecast = await model.calculate_price_forecast(7)
    for day in range(7):
        print(f"   Day {day+1}: ${forecast[day]:.2f}/L "
              f"(95% CI: ${intervals[day][0]:.2f}-${intervals[day][1]:.2f}) "
              f"vol: {vol_forecast[day]:.2%}")
    
    print("\n6. Circuit Breaker Status:")
    cb_status = metrics['circuit_breaker']
    print(f"   State: {cb_status['state']}")
    print(f"   Failures: {cb_status['failures']}/{cb_status['threshold']}")
    
    print("\n✅ Enhanced Helium Elasticity Model v3.1 test complete")

if __name__ == "__main__":
    # Import pandas for Prophet (if available)
    if PROPHET_AVAILABLE:
        import pandas as pd
    
    asyncio.run(main())
