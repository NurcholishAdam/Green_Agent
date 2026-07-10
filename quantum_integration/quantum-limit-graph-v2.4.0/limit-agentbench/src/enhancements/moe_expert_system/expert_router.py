"""
Enhanced Expert Router v8.0.1 - Complete Signal Transduction Cascade with Causal Constraints
With Federated Learning, Predictive Analytics, Carbon/Helium Optimization,
What-If Analysis, Causal Inference, Natural Language Explanations,
Counterfactual Reasoning, Signal Integration, Differential Privacy,
Uncertainty Quantification, and Helium Price Forecasting

New in v8.0.1:
- Configuration dataclass for centralized settings
- Resilient carbon manager with retry & circuit breaker
- Helium optimizer with real-time price integration
- Federated learner with model compression (top-k sparsification)
- Predictive analyzer with online learning (SGD)
- Causal model with dynamic graph updates
- Signal integrator with adaptive weights and temporal smoothing
- Telemetry collector for metrics export
- Improved bio-inspired fallback strategies
- CPU-bound ML tasks offloaded to threads
"""

import asyncio
import logging
from typing import Dict, Any, List, Optional, Tuple, Set, Callable
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
import numpy as np
from collections import defaultdict, deque
import time
import hashlib
import json
import math
import uuid
import aiohttp
import os
from sklearn.preprocessing import StandardScaler
from sklearn.linear_model import SGDRegressor
from sklearn.metrics import r2_score, mean_squared_error
import torch
import torch.nn as nn
import torch.optim as optim
from torch.utils.data import DataLoader, TensorDataset
import networkx as nx
from typing import TypeVar, Generic

# Import gating network
from .gating_network import GatingNetworkManager

logger = logging.getLogger(__name__)

# ============================================================================
# Configuration Dataclass (NEW)
# ============================================================================
@dataclass
class ExpertRouterConfig:
    """Centralized configuration for the Expert Router."""
    # Feature flags
    enable_quantum: bool = False
    enable_signal_transduction: bool = True
    enable_allosteric: bool = True
    enable_metabolic_pathways: bool = True
    enable_cooperative_binding: bool = True
    enable_homeostasis: bool = True
    enable_bio_integration: bool = True
    enable_federated: bool = True
    enable_predictive: bool = True
    enable_carbon_intensity: bool = True
    enable_helium_optimization: bool = True
    enable_causal_constraints: bool = True
    enable_counterfactual: bool = True
    enable_signal_integration: bool = True
    enable_differential_privacy: bool = True
    enable_uncertainty_quantification: bool = True
    enable_telemetry: bool = True

    # Tunable parameters
    carbon_api_region: str = "us-east"
    carbon_update_interval: int = 300
    max_retries: int = 3
    retry_base_delay_ms: float = 100.0
    retry_max_delay_ms: float = 5000.0
    circuit_breaker_failure_threshold: int = 5
    circuit_breaker_recovery_timeout: float = 30.0
    server_url: Optional[str] = None
    helium_budget_l: float = 100.0
    privacy_epsilon: float = 1.0
    federated_sparsity_ratio: float = 0.1
    predictive_history_window: int = 100
    max_concurrent_routes: int = 100
    persistence_path: Optional[str] = None  # For causal model persistence

    def __post_init__(self):
        # Ensure boolean flags
        for key, value in self.__dict__.items():
            if isinstance(value, bool):
                setattr(self, key, bool(value))

# ============================================================================
# Enums and Data Classes (Preserved with minor additions)
# ============================================================================

class SignalType(Enum):
    ENDOCRINE = "endocrine"; PARACRINE = "paracrine"; AUTOCRINE = "autocrine"
    JUXTACRINE = "juxtacrine"; NEUROTRANSMITTER = "neurotransmitter"; NEUROMODULATOR = "neuromodulator"

class SecondMessenger(Enum):
    cAMP = "camp"; cGMP = "cgmp"; IP3 = "ip3"; DAG = "dag"; CALCIUM = "calcium"; NITRIC_OXIDE = "nitric_oxide"

class ReceptorState(Enum):
    INACTIVE = "inactive"; BOUND = "bound"; ACTIVATED = "activated"
    DESENSITIZED = "desensitized"; INTERNALIZED = "internalized"; RESENSITIZED = "resensitized"

class AmplificationLevel(Enum):
    NONE = 0; LOW = 1; MODERATE = 2; HIGH = 3; MAXIMUM = 4

class CircuitBreakerState(Enum):
    CLOSED = "closed"; OPEN = "open"; HALF_OPEN = "half_open"

@dataclass
class SignalReceptor:
    receptor_id: str
    signal_type: SignalType
    ligand: str
    affinity: float = 0.5
    state: ReceptorState = ReceptorState.INACTIVE
    bound_ligands: int = 0
    desensitization_time: float = 0.0
    resensitization_rate: float = 0.1
    amplification: AmplificationLevel = AmplificationLevel.MODERATE
    downstream_effectors: List[str] = field(default_factory=list)
    last_activated: Optional[datetime] = None
    activation_count: int = 0

@dataclass
class SecondMessengerSystem:
    messenger_type: SecondMessenger
    concentration: float = 0.0
    baseline: float = 0.1
    threshold: float = 0.3
    max_concentration: float = 1.0
    synthesis_rate: float = 0.1
    degradation_rate: float = 0.05
    amplification_factor: float = 100.0
    target_proteins: List[str] = field(default_factory=list)
    half_life_seconds: float = 5.0

@dataclass
class AllostericSite:
    site_id: str
    modulator: str
    effect: str = "modulation"
    binding_affinity: float = 0.5
    current_occupancy: float = 0.0
    conformational_change: float = 0.0

@dataclass
class MetabolicPathway:
    pathway_id: str
    input_substrate: str
    enzymes: List[str]
    intermediates: List[str]
    final_product: str
    rate_limiting_step: Optional[str] = None
    allosteric_regulators: List[AllostericSite] = field(default_factory=list)
    energy_cost_ecoatp: float = 10.0
    throughput_rate: float = 1.0
    is_active: bool = True

@dataclass
class RoutingMetrics:
    total_routes: int = 0
    successful_routes: int = 0
    failed_routes: int = 0
    fallback_routes: int = 0
    biomass_stored_routes: int = 0
    average_latency_ms: float = 0.0
    carbon_savings_kg: float = 0.0
    helium_savings_l: float = 0.0

    @property
    def success_rate(self) -> float:
        return self.successful_routes / max(self.total_routes, 1)

@dataclass
class ExpertCircuitBreaker:
    expert_id: str
    state: CircuitBreakerState = CircuitBreakerState.CLOSED
    failure_count: int = 0
    success_count: int = 0
    last_failure_time: Optional[datetime] = None
    failure_threshold: int = 5
    recovery_timeout_seconds: int = 30
    half_open_max_requests: int = 3
    half_open_requests: int = 0

    def record_success(self):
        self.success_count += 1
        if self.state == CircuitBreakerState.HALF_OPEN:
            self.half_open_requests += 1
            if self.half_open_requests >= self.half_open_max_requests:
                self.state = CircuitBreakerState.CLOSED
                self.failure_count = 0

    def record_failure(self):
        self.failure_count += 1
        self.last_failure_time = datetime.utcnow()
        if self.state == CircuitBreakerState.CLOSED and self.failure_count >= self.failure_threshold:
            self.state = CircuitBreakerState.OPEN
        elif self.state == CircuitBreakerState.HALF_OPEN:
            self.state = CircuitBreakerState.OPEN

    def can_execute(self) -> bool:
        if self.state == CircuitBreakerState.CLOSED:
            return True
        if self.state == CircuitBreakerState.OPEN:
            if self.last_failure_time:
                elapsed = (datetime.utcnow() - self.last_failure_time).total_seconds()
                if elapsed >= self.recovery_timeout_seconds:
                    self.state = CircuitBreakerState.HALF_OPEN
                    self.half_open_requests = 0
                    return True
            return False
        return True

# ============================================================================
# Enhanced Carbon Intensity Manager with Retry & Circuit Breaker
# ============================================================================
class CarbonIntensityManager:
    """Real-time carbon intensity integration with retry, circuit breaker, and caching."""

    def __init__(self, config: ExpertRouterConfig):
        self.config = config
        self.endpoint = "https://api.electricitymap.org/v3/carbon-intensity"
        self.region = config.carbon_api_region
        self.carbon_intensity = 0.0
        self.carbon_price_usd_per_ton = 50.0
        self.last_update = None
        self._lock = asyncio.Lock()
        self._session = None
        self.cache = {}
        self.historical_intensities = deque(maxlen=1000)
        self.price_history = deque(maxlen=1000)
        self.api_key = os.getenv('ELECTRICITYMAP_API_KEY', '')
        self.failure_count = 0
        self.circuit_open = False
        self.circuit_open_until = None
        self.circuit_breaker_threshold = config.circuit_breaker_failure_threshold
        self.max_retries = config.max_retries
        self.price_trend = 0.0
        self.forecast_model = None
        self._initialize_forecast_model()
        logger.info(f"CarbonIntensityManager initialized (region={self.region}, retries={self.max_retries})")

    def _initialize_forecast_model(self):
        try:
            from sklearn.linear_model import LinearRegression
            self.forecast_model = LinearRegression()
            self.forecast_trained = False
        except ImportError:
            self.forecast_model = None
            self.forecast_trained = False

    async def _get_session(self):
        if self._session is None:
            self._session = aiohttp.ClientSession()
        return self._session

    async def update_carbon_intensity(self, region: Optional[str] = None) -> Dict:
        """Update carbon intensity with retry and circuit breaker."""
        if region is not None:
            self.region = region

        # Circuit breaker check
        if self.circuit_open:
            if datetime.utcnow() < self.circuit_open_until:
                logger.warning("Circuit breaker open, using fallback data")
                return self._get_fallback_response()
            else:
                self.circuit_open = False
                self.failure_count = 0
                logger.info("Circuit breaker reset for CarbonIntensityManager")

        # Cache check
        cache_key = f"{self.region}_{datetime.utcnow().hour}"
        if cache_key in self.cache and self.last_update and (datetime.utcnow() - self.last_update).seconds < self.config.carbon_update_interval:
            return self.cache[cache_key]

        for attempt in range(self.max_retries):
            try:
                session = await self._get_session()
                url = f"{self.endpoint}/latest?zone={self.region}"
                headers = {'auth-token': self.api_key} if self.api_key else {}
                async with session.get(url, headers=headers, timeout=10) as response:
                    if response.status == 200:
                        data = await response.json()
                        self.carbon_intensity = data.get('carbonIntensity', 400)
                        self.last_update = datetime.now()
                        self.cache[cache_key] = {
                            'intensity': self.carbon_intensity,
                            'timestamp': self.last_update.isoformat()
                        }
                        self.historical_intensities.append(self.carbon_intensity)
                        self._update_carbon_price(self.carbon_intensity)
                        self.failure_count = 0
                        return {
                            'intensity': self.carbon_intensity,
                            'region': self.region,
                            'timestamp': self.last_update.isoformat(),
                            'price_usd_per_ton': self.carbon_price_usd_per_ton,
                            'trend': self.price_trend
                        }
                    else:
                        logger.warning(f"Carbon API returned {response.status}, attempt {attempt+1}")
                        if attempt == self.max_retries - 1:
                            self.failure_count += 1
                            if self.failure_count >= self.circuit_breaker_threshold:
                                self.circuit_open = True
                                self.circuit_open_until = datetime.utcnow() + timedelta(minutes=5)
                                logger.error("Circuit breaker opened for CarbonIntensityManager")
                            return self._get_fallback_response()
                        await asyncio.sleep(2 ** attempt)
            except Exception as e:
                logger.error(f"Carbon API error: {e}, attempt {attempt+1}")
                if attempt == self.max_retries - 1:
                    self.failure_count += 1
                    if self.failure_count >= self.circuit_breaker_threshold:
                        self.circuit_open = True
                        self.circuit_open_until = datetime.utcnow() + timedelta(minutes=5)
                    return self._get_fallback_response()
                await asyncio.sleep(2 ** attempt)

        # Should never reach here
        return self._get_fallback_response()

    def _update_carbon_price(self, intensity: float):
        base_price = 50.0
        volatility = np.random.normal(0, 5)
        intensity_factor = (intensity - 300) / 500
        price = base_price * (1.0 + intensity_factor) + volatility
        self.carbon_price_usd_per_ton = max(10.0, price)
        self.price_history.append({
            'timestamp': self.last_update.isoformat() if self.last_update else None,
            'intensity': intensity,
            'price': self.carbon_price_usd_per_ton
        })
        if len(self.price_history) > 5:
            recent_prices = [p['price'] for p in list(self.price_history)[-5:]]
            self.price_trend = np.polyfit(range(len(recent_prices)), recent_prices, 1)[0]

    def _get_fallback_response(self) -> Dict:
        fallback_intensities = {
            'us-east': 420, 'us-west': 350, 'eu': 280, 'asia': 500
        }
        intensity = fallback_intensities.get(self.region, 400)
        self.carbon_intensity = intensity
        self._update_carbon_price(intensity)
        return {
            'intensity': intensity,
            'region': self.region,
            'timestamp': datetime.utcnow().isoformat(),
            'price_usd_per_ton': self.carbon_price_usd_per_ton,
            'is_fallback': True,
            'trend': self.price_trend
        }

    async def get_current_intensity(self) -> float:
        if self.last_update is None or (datetime.utcnow() - self.last_update).seconds > self.config.carbon_update_interval:
            await self.update_carbon_intensity(self.region)
        return self.carbon_intensity

    async def get_current_price(self) -> float:
        if self.last_update is None or (datetime.utcnow() - self.last_update).seconds > self.config.carbon_update_interval:
            await self.update_carbon_intensity(self.region)
        return self.carbon_price_usd_per_ton

    async def close(self):
        if self._session:
            await self._session.close()

# ============================================================================
# Enhanced Helium Efficiency Optimizer with Real-time Price Integration
# ============================================================================
class HeliumEfficiencyOptimizer:
    """Optimize helium allocation across experts and routing with price forecasting."""

    def __init__(self, config: ExpertRouterConfig, carbon_manager: Optional[CarbonIntensityManager] = None):
        self.config = config
        self.carbon_manager = carbon_manager
        self.helium_budget_l = config.helium_budget_l
        self.helium_usage: Dict[str, float] = defaultdict(float)
        self.helium_allocation: Dict[str, float] = defaultdict(float)
        self.helium_efficiency_scores: Dict[str, float] = defaultdict(lambda: 0.5)
        self._lock = asyncio.Lock()
        self.optimization_history = deque(maxlen=1000)
        self.helium_price_usd_per_l = 0.5
        self.price_history = deque(maxlen=1000)
        self.price_trend = 0.0
        self.forecast_model = None
        self._initialize_forecast_model()
        logger.info(f"HeliumEfficiencyOptimizer initialized: budget={helium_budget_l}L")

    def _initialize_forecast_model(self):
        try:
            from sklearn.linear_model import LinearRegression
            self.forecast_model = LinearRegression()
            self.forecast_trained = False
        except ImportError:
            self.forecast_model = None
            self.forecast_trained = False

    def _update_helium_price(self, scarcity: float):
        """Update helium price based on scarcity and carbon price."""
        base_price = 0.5
        # If carbon price is high, helium becomes more valuable (substitute)
        carbon_price = asyncio.run(self.carbon_manager.get_current_price()) if self.carbon_manager else 50.0
        carbon_factor = 1.0 + (carbon_price - 50.0) / 50.0 * 0.2
        scarcity_factor = 1.0 + scarcity * 0.8
        self.helium_price_usd_per_l = max(0.1, base_price * scarcity_factor * carbon_factor)
        self.price_history.append({
            'timestamp': datetime.utcnow().isoformat(),
            'price': self.helium_price_usd_per_l
        })
        if len(self.price_history) > 5:
            recent_prices = [p['price'] for p in list(self.price_history)[-5:]]
            self.price_trend = np.polyfit(range(len(recent_prices)), recent_prices, 1)[0]

    def record_helium_usage(self, expert_id: str, amount_l: float, scarcity: float = 0.5):
        self.helium_usage[expert_id] += amount_l
        self._update_helium_price(scarcity)

    def set_helium_allocation(self, expert_id: str, amount_l: float):
        self.helium_allocation[expert_id] = amount_l

    def update_efficiency_score(self, expert_id: str, score: float):
        self.helium_efficiency_scores[expert_id] = score

    async def optimize_helium_allocation(self, expert_requirements: Dict[str, float]) -> Dict[str, float]:
        async with self._lock:
            total_required = sum(expert_requirements.values())
            if total_required <= self.helium_budget_l:
                return expert_requirements

            optimized = {}
            total_efficiency = sum(self.helium_efficiency_scores.get(eid, 0.5) for eid in expert_requirements)

            # Include price elasticity
            if self.helium_price_usd_per_l > 0.8:
                price_factor = 0.7
            elif self.helium_price_usd_per_l < 0.3:
                price_factor = 1.3
            else:
                price_factor = 1.0

            if total_efficiency == 0:
                ratio = (self.helium_budget_l * price_factor) / total_required
                for expert_id, required in expert_requirements.items():
                    optimized[expert_id] = required * ratio
            else:
                adjusted_budget = self.helium_budget_l * price_factor
                for expert_id, required in expert_requirements.items():
                    efficiency_weight = self.helium_efficiency_scores.get(expert_id, 0.5) / total_efficiency
                    optimized[expert_id] = adjusted_budget * efficiency_weight

            self.optimization_history.append({
                'timestamp': datetime.utcnow().isoformat(),
                'total_required': total_required,
                'total_allocated': self.helium_budget_l,
                'price_factor': price_factor,
                'price_usd_per_l': self.helium_price_usd_per_l,
                'allocations': optimized
            })

            return optimized

    def get_helium_status(self) -> Dict[str, Any]:
        total_usage = sum(self.helium_usage.values())
        total_allocated = sum(self.helium_allocation.values())
        return {
            'budget_l': self.helium_budget_l,
            'total_usage_l': total_usage,
            'total_allocated_l': total_allocated,
            'remaining_budget_l': self.helium_budget_l - total_usage,
            'expert_usage': dict(self.helium_usage),
            'expert_allocation': dict(self.helium_allocation),
            'efficiency_scores': dict(self.helium_efficiency_scores),
            'optimization_count': len(self.optimization_history),
            'price_usd_per_l': self.helium_price_usd_per_l,
            'price_trend': self.price_trend,
            'price_samples': len(self.price_history)
        }

# ============================================================================
# Enhanced Federated Routing Learner with Model Compression
# ============================================================================
class FederatedRoutingLearner:
    """Federated reflexive learning with differential privacy and model compression."""

    def __init__(self, config: ExpertRouterConfig):
        self.config = config
        self.server_url = config.server_url
        self.round = 0
        self.local_model = None
        self.global_model = None
        self.participants = []
        self.contribution_scores = {}
        self._lock = asyncio.Lock()
        self._session = None
        self.routing_history = deque(maxlen=10000)
        self.privacy_epsilon = config.privacy_epsilon
        self.noise_scale = 0.001
        self.sparsity_ratio = config.federated_sparsity_ratio
        self._init_routing_model()
        logger.info(f"FederatedRoutingLearner initialized with ε={self.privacy_epsilon}, sparsity={self.sparsity_ratio}")

    def _init_routing_model(self):
        class RoutingModel(nn.Module):
            def __init__(self, input_size=10, hidden_size=64):
                super().__init__()
                self.network = nn.Sequential(
                    nn.Linear(input_size, hidden_size),
                    nn.ReLU(),
                    nn.BatchNorm1d(hidden_size),
                    nn.Linear(hidden_size, hidden_size // 2),
                    nn.ReLU(),
                    nn.BatchNorm1d(hidden_size // 2),
                    nn.Linear(hidden_size // 2, 5)
                )
            def forward(self, x):
                return self.network(x)

        self.local_model = RoutingModel()
        self.global_model = RoutingModel()

    async def _get_session(self):
        if self._session is None and self.server_url:
            self._session = aiohttp.ClientSession()
        return self._session

    def _add_differential_privacy(self, weights: Dict) -> Dict:
        if self.privacy_epsilon <= 0:
            return weights
        private_weights = {}
        sensitivity = 1.0
        for key, tensor in weights.items():
            scale = (2 * sensitivity) / self.privacy_epsilon
            noise = torch.randn_like(tensor) * scale * self.noise_scale
            private_weights[key] = tensor + noise
        return private_weights

    def _compress_weights(self, weights: Dict) -> Dict:
        """Apply top‑k sparsification: keep only largest k% of weights."""
        compressed = {}
        for key, tensor in weights.items():
            flat = tensor.view(-1)
            k = int(flat.numel() * self.sparsity_ratio)
            if k == 0:
                compressed[key] = torch.zeros_like(tensor)
                continue
            topk_vals, topk_idx = torch.topk(flat.abs(), k)
            sparse = torch.zeros_like(flat)
            sparse[topk_idx] = flat[topk_idx]
            compressed[key] = sparse.view(tensor.shape)
        return compressed

    async def train_local_model(self, routing_data: List[Dict], epochs: int = 10) -> float:
        if not routing_data:
            return 0.0
        X, y = [], []
        for item in routing_data:
            X.append([
                item.get('carbon_zone', 0) / 10,
                item.get('helium_scarcity', 0.5),
                item.get('task_complexity', 0.5),
                item.get('token_balance', 500) / 1000,
                item.get('carbon_gradient', 0.5),
                item.get('trust_gradient', 0.5),
                item.get('opportunity_gradient', 0.5),
                item.get('stress_level', 0.5),
                item.get('latency_budget', 100) / 1000,
                item.get('energy_budget', 100) / 1000
            ])
            selected = [0] * 5
            expert_idx = item.get('selected_expert_idx', 0)
            if expert_idx < 5:
                selected[expert_idx] = 1
            y.append(selected)

        X = torch.FloatTensor(X)
        y = torch.FloatTensor(y)
        dataset = TensorDataset(X, y)
        dataloader = DataLoader(dataset, batch_size=32, shuffle=True)
        optimizer = optim.Adam(self.local_model.parameters(), lr=0.001)
        criterion = nn.CrossEntropyLoss()

        total_loss = 0
        for epoch in range(epochs):
            epoch_loss = 0
            for batch_X, batch_y in dataloader:
                optimizer.zero_grad()
                output = self.local_model(batch_X)
                loss = criterion(output, torch.argmax(batch_y, dim=1))
                loss.backward()
                torch.nn.utils.clip_grad_norm_(self.local_model.parameters(), 1.0)
                optimizer.step()
                epoch_loss += loss.item()
            total_loss += epoch_loss

        avg_loss = total_loss / epochs
        logger.info(f"Local routing model trained. Loss: {avg_loss:.4f}")
        return avg_loss

    async def send_local_update(self, performance_metric: float = 1.0) -> Dict:
        if not self.server_url:
            return {'status': 'disabled'}

        async with self._lock:
            for attempt in range(self.config.max_retries):
                try:
                    session = await self._get_session()
                    weights = self.local_model.state_dict()
                    private_weights = self._add_differential_privacy(weights)
                    compressed_weights = self._compress_weights(private_weights)
                    weights_serialized = {k: v.tolist() for k, v in compressed_weights.items()}
                    update_data = {
                        'router_id': 'expert_router',
                        'round': self.round,
                        'weights': weights_serialized,
                        'performance': performance_metric,
                        'privacy_epsilon': self.privacy_epsilon,
                        'timestamp': datetime.utcnow().isoformat(),
                        'sparsity_ratio': self.sparsity_ratio
                    }
                    async with session.post(
                        f"{self.server_url}/federated/routing/update",
                        json=update_data,
                        timeout=30
                    ) as response:
                        if response.status == 200:
                            result = await response.json()
                            self.round += 1
                            self.contribution_scores['router'] = performance_metric
                            return result
                        else:
                            logger.warning(f"Federated update failed (attempt {attempt+1}): {response.status}")
                except Exception as e:
                    logger.error(f"Federated update error (attempt {attempt+1}): {e}")
                await asyncio.sleep(2 ** attempt)
            return {'status': 'failed'}

    async def get_global_model(self) -> Optional[Dict]:
        if not self.server_url:
            return None
        async with self._lock:
            for attempt in range(self.config.max_retries):
                try:
                    session = await self._get_session()
                    async with session.get(
                        f"{self.server_url}/federated/routing/global",
                        timeout=30
                    ) as response:
                        if response.status == 200:
                            data = await response.json()
                            weights = data.get('weights', {})
                            self.round = data.get('round', 0)
                            self.participants = data.get('participants', [])
                            for k, v in weights.items():
                                self.global_model.state_dict()[k] = torch.FloatTensor(v)
                            return weights
                except Exception as e:
                    logger.error(f"Global model fetch error (attempt {attempt+1}): {e}")
                await asyncio.sleep(2 ** attempt)
            return None

    async def participate_in_round(self, routing_data: List[Dict], performance: float = 1.0) -> Dict:
        await self.train_local_model(routing_data)
        result = await self.send_local_update(performance)
        global_weights = await self.get_global_model()
        if global_weights:
            self.global_model.load_state_dict(global_weights)
            if 'router' not in self.participants:
                self.participants.append('router')
        return {
            'round': self.round,
            'participated': bool(global_weights),
            'contribution_score': self.contribution_scores.get('router', 0),
            'performance': performance,
            'peer_count': len(self.participants),
            'privacy_epsilon': self.privacy_epsilon,
            'timestamp': datetime.utcnow().isoformat()
        }

    def get_federated_insights(self) -> Dict:
        return {
            'round': self.round,
            'contribution_score': self.contribution_scores.get('router', 0),
            'participants': len(self.participants),
            'has_global_model': bool(self.global_model),
            'local_model_trained': self.local_model is not None,
            'privacy_epsilon': self.privacy_epsilon,
            'sparsity_ratio': self.sparsity_ratio
        }

    async def close(self):
        if self._session:
            await self._session.close()

# ============================================================================
# Enhanced Predictive Routing Analyzer with Online Learning
# ============================================================================
class PredictiveRoutingAnalyzer:
    """Predictive reflexivity with online learning and uncertainty quantification."""

    def __init__(self, config: ExpertRouterConfig):
        self.config = config
        self.history_window = config.predictive_history_window
        self.routing_history = deque(maxlen=self.history_window)
        self.forecast_history = deque(maxlen=50)
        self.scaler = StandardScaler()
        self.model = None
        self.is_trained = False
        self.prediction_intervals: Dict[str, List[Tuple[float, float]]] = defaultdict(list)
        self.uncertainty_scores: Dict[str, float] = {}
        self._init_model()
        logger.info("PredictiveRoutingAnalyzer initialized with online learning")

    def _init_model(self):
        try:
            from sklearn.linear_model import SGDRegressor
            self.model = SGDRegressor(
                learning_rate='constant',
                eta0=0.01,
                penalty='l2',
                alpha=0.0001,
                max_iter=1,
                random_state=42,
                warm_start=True
            )
            self._ml_available = True
        except ImportError:
            self._ml_available = False
            logger.warning("SGDRegressor not available; using fallback moving average")

    def update_history(self, routing_metrics: Dict):
        self.routing_history.append({
            'timestamp': datetime.utcnow(),
            'success_rate': routing_metrics.get('success_rate', 0.8),
            'avg_latency_ms': routing_metrics.get('avg_latency_ms', 100),
            'carbon_efficiency': routing_metrics.get('carbon_efficiency', 0.5),
            'helium_efficiency': routing_metrics.get('helium_efficiency', 0.5),
            'expert_utilization': routing_metrics.get('expert_utilization', 0.5)
        })

    async def train_forecast_model(self):
        """Train or update the model incrementally."""
        if not self._ml_available:
            return {'status': 'ml_not_available'}
        if len(self.routing_history) < 10:
            return {'status': 'insufficient_data'}

        # Prepare features
        X, y = [], []
        history_list = list(self.routing_history)
        for i in range(len(history_list) - 5):
            features = []
            for j in range(5):
                data = history_list[i + j]
                features.extend([
                    data['success_rate'],
                    data['avg_latency_ms'] / 1000,
                    data['carbon_efficiency'],
                    data['helium_efficiency'],
                    data['expert_utilization']
                ])
            X.append(features)
            y.append(history_list[i + 5]['success_rate'])

        X = np.array(X)
        y = np.array(y)

        # Offload scaling and training to thread
        def scale_and_fit():
            if self.scaler.mean_ is None:
                X_scaled = self.scaler.fit_transform(X)
            else:
                X_scaled = self.scaler.transform(X)
            # Incremental training
            for _ in range(3):
                self.model.partial_fit(X_scaled, y)
            return X_scaled

        X_scaled = await asyncio.to_thread(scale_and_fit)
        self.is_trained = True

        # Calculate R2 score
        pred = self.model.predict(X_scaled)
        r2 = r2_score(y, pred) if len(X) > 5 else 0.0
        logger.info(f"Routing forecast model updated. R²={r2:.3f}")
        return {'status': 'success', 'r2': r2, 'samples': len(X)}

    async def predict_routing_performance(self, hours: int = 24) -> Dict:
        if not self.is_trained or len(self.routing_history) < 10:
            # Fallback: moving average
            if len(self.routing_history) > 0:
                recent = [h['success_rate'] for h in list(self.routing_history)[-5:]]
                pred = np.mean(recent) if recent else 0.5
                return {'predicted_success_rate': pred, 'confidence': 0.3, 'trend': 'moving_average'}
            return {'predicted_success_rate': 0.5, 'confidence': 0.0, 'trend': 'insufficient_data'}

        recent = list(self.routing_history)[-5:]
        features = []
        for data in recent:
            features.extend([
                data['success_rate'],
                data['avg_latency_ms'] / 1000,
                data['carbon_efficiency'],
                data['helium_efficiency'],
                data['expert_utilization']
            ])
        features = np.array(features).reshape(1, -1)

        def predict():
            if self.scaler.mean_ is not None:
                features_scaled = self.scaler.transform(features)
            else:
                features_scaled = features
            pred = self.model.predict(features_scaled)[0]
            return pred

        prediction = await asyncio.to_thread(predict)
        # Confidence based on recency
        confidence = min(0.9, 0.5 + 0.4 * (len(self.routing_history) / 100))

        # Uncertainty intervals (using std of recent predictions)
        if len(self.routing_history) > 20:
            recent_preds = [h['success_rate'] for h in list(self.routing_history)[-20:]]
            std = np.std(recent_preds)
        else:
            std = 0.1
        lower = max(0.0, prediction - 1.96 * std)
        upper = min(1.0, prediction + 1.96 * std)

        if len(self.forecast_history) > 5:
            recent_forecasts = list(self.forecast_history)[-5:]
            trend = "improving" if prediction > recent_forecasts[-1] else "declining" if prediction < recent_forecasts[-1] else "stable"
        else:
            trend = "stable"

        self.forecast_history.append({'prediction': prediction, 'trend': trend})
        self.prediction_intervals['routing'].append((lower, upper))
        self.uncertainty_scores['routing'] = 1.0 - confidence

        return {
            'predicted_success_rate': prediction,
            'confidence': confidence,
            'trend': trend,
            'lower_bound': lower,
            'upper_bound': upper,
            'uncertainty': 1.0 - confidence,
            'recommended_actions': self._generate_predictive_actions(prediction)
        }

    def _generate_predictive_actions(self, prediction: float) -> List[str]:
        actions = []
        if prediction < 0.5:
            actions.append("Optimize expert selection criteria")
            actions.append("Increase carbon budget allocation")
            actions.append("Consider fallback routing strategies")
        elif prediction < 0.7:
            actions.append("Enhance signal transduction sensitivity")
            actions.append("Improve allosteric regulation")
        else:
            actions.append("Maintain current routing configuration")
        return actions

    def get_uncertainty_metrics(self) -> Dict[str, Any]:
        return {
            'prediction_intervals': {k: list(v) for k, v in self.prediction_intervals.items()},
            'uncertainty_scores': self.uncertainty_scores,
            'recent_interval': self.prediction_intervals['routing'][-1] if self.prediction_intervals['routing'] else None,
            'overall_uncertainty': np.mean(list(self.uncertainty_scores.values())) if self.uncertainty_scores else 0.0
        }

# ============================================================================
# Enhanced Causal Constraint Model with Dynamic Graph Updates
# ============================================================================
class CausalConstraintModel:
    """
    Causal constraint modeling with dynamic graph updates and persistence.
    """

    def __init__(self, config: ExpertRouterConfig):
        self.config = config
        self.causal_graph = nx.DiGraph()
        self.constraints = {}
        self.impact_history = deque(maxlen=1000)
        self.causal_strengths = {}
        self._lock = asyncio.Lock()
        self.counterfactual_cache: Dict[str, Dict] = {}
        self.domain_mapping = {
            'carbon': ['energy', 'helium', 'biodiversity'],
            'helium': ['quantum', 'cooling', 'energy'],
            'energy': ['carbon', 'helium', 'latency'],
            'quantum': ['helium', 'energy', 'accuracy'],
            'biodiversity': ['carbon', 'land_use'],
            'latency': ['energy', 'performance'],
            'accuracy': ['quantum', 'performance']
        }
        self.constraint_thresholds = {
            'carbon': {'max_per_inference': 0.001, 'min_zone': 0},
            'helium': {'max_usage_per_inference': 0.01, 'min_availability': 0.2},
            'energy': {'max_per_inference': 0.01, 'min_efficiency': 0.5},
            'quantum': {'min_qubits': 10, 'max_depth': 100},
            'biodiversity': {'min_impact_score': 0.3}
        }
        self._init_causal_relationships()

        # Load from persistence if path provided
        if config.persistence_path:
            self._load_persistence(config.persistence_path)

        logger.info("CausalConstraintModel initialized with dynamic updates")

    def _init_causal_relationships(self):
        # Initialize with known relationships
        edges = [
            ('carbon', 'energy', 0.7), ('carbon', 'helium', 0.5), ('carbon', 'biodiversity', 0.6),
            ('helium', 'quantum', 0.8), ('helium', 'cooling', 0.6), ('helium', 'energy', 0.4),
            ('energy', 'carbon', 0.7), ('energy', 'helium', 0.3), ('energy', 'latency', 0.5),
            ('quantum', 'helium', 0.9), ('quantum', 'energy', 0.6), ('quantum', 'accuracy', 0.8)
        ]
        for u, v, w in edges:
            self.causal_graph.add_edge(u, v, weight=w)
            self.causal_strengths[(u, v)] = w

    def _load_persistence(self, path: str):
        try:
            with open(path, 'rb') as f:
                data = pickle.load(f)
            self.causal_graph = data['graph']
            self.causal_strengths = data['strengths']
            self.constraints = data.get('constraints', {})
            logger.info(f"Loaded causal model from {path}")
        except Exception as e:
            logger.warning(f"Could not load causal model from {path}: {e}")

    def save_persistence(self, path: str):
        try:
            with open(path, 'wb') as f:
                pickle.dump({
                    'graph': self.causal_graph,
                    'strengths': self.causal_strengths,
                    'constraints': self.constraints
                }, f)
            logger.info(f"Saved causal model to {path}")
        except Exception as e:
            logger.error(f"Could not save causal model: {e}")

    def add_causal_relationship(self, source: str, target: str, strength: float = 0.5):
        with self._lock:
            self.causal_graph.add_edge(source, target, weight=strength)
            self.causal_strengths[(source, target)] = strength
            logger.info(f"Added causal relationship: {source} → {target} (strength={strength:.2f})")

    async def update_from_data(self, observations: List[Dict[str, float]]):
        """Dynamically update causal strengths based on observed data."""
        if not observations:
            return
        # Simple correlation-based update (placeholder)
        for obs in observations:
            for u in self.domain_mapping.keys():
                for v in self.domain_mapping.keys():
                    if u != v and u in obs and v in obs:
                        corr = np.corrcoef(obs[u], obs[v])[0, 1] if isinstance(obs[u], list) else 0.0
                        if abs(corr) > 0.3:
                            strength = abs(corr)
                            self.add_causal_relationship(u, v, strength)

    async def propagate_constraints(self, source_domain: str, value: float, constraints: Dict[str, Any]) -> Dict[str, Any]:
        async with self._lock:
            propagated = constraints.copy()
            if source_domain not in self.domain_mapping:
                return propagated
            effects = self.domain_mapping.get(source_domain, [])
            for effect in effects:
                if effect not in propagated:
                    propagated[effect] = {}
                strength = self.causal_strengths.get((source_domain, effect), 0.5)
                impact = strength * value
                propagated[effect]['causal_impact'] = impact
                propagated[effect]['causal_strength'] = strength
                propagated[effect]['source'] = source_domain
                if effect == 'energy':
                    propagated[effect]['expected_change'] = impact * value * 0.1
                elif effect == 'helium':
                    propagated[effect]['expected_change'] = impact * value * 0.2
                elif effect == 'carbon':
                    propagated[effect]['expected_change'] = impact * value * 0.15
                elif effect == 'quantum':
                    propagated[effect]['feasibility'] = 1.0 if value < 0.8 else 0.5
                elif effect == 'biodiversity':
                    propagated[effect]['impact_score'] = min(1.0, impact * 0.5)
                if effect in self.constraint_thresholds:
                    threshold = self.constraint_thresholds[effect]
                    for key, limit in threshold.items():
                        if key in propagated[effect]:
                            propagated[effect][f'{key}_limit'] = limit
                            propagated[effect][f'{key}_compliant'] = propagated[effect][key] <= limit
            self.impact_history.append({
                'timestamp': datetime.utcnow().isoformat(),
                'source': source_domain,
                'value': value,
                'propagated': propagated
            })
            return propagated

    async def counterfactual_analysis(self, source_domain: str, actual_value: float,
                                      counterfactual_value: float, target_domain: str,
                                      constraints: Dict[str, Any]) -> Dict[str, Any]:
        cache_key = hashlib.md5(
            f"{source_domain}_{actual_value}_{counterfactual_value}_{target_domain}".encode()
        ).hexdigest()[:12]
        if cache_key in self.counterfactual_cache:
            return self.counterfactual_cache[cache_key]

        path = await self.get_causal_path(source_domain, target_domain)
        if not path:
            return {'status': 'no_causal_path'}

        actual_propagated = await self.propagate_constraints(source_domain, actual_value, constraints.copy())
        counterfactual_propagated = await self.propagate_constraints(source_domain, counterfactual_value, constraints.copy())

        actual_impact = actual_propagated.get(target_domain, {}).get('causal_impact', 0.0)
        counterfactual_impact = counterfactual_propagated.get(target_domain, {}).get('causal_impact', 0.0)
        impact_delta = counterfactual_impact - actual_impact
        improvement = impact_delta > 0

        result = {
            'source_domain': source_domain,
            'target_domain': target_domain,
            'actual_value': actual_value,
            'counterfactual_value': counterfactual_value,
            'actual_impact': actual_impact,
            'counterfactual_impact': counterfactual_impact,
            'impact_delta': impact_delta,
            'improvement': improvement,
            'confidence': 0.8 if path else 0.5,
            'causal_path': path,
            'recommendation': (
                f"Consider changing {source_domain} from {actual_value:.2f} to {counterfactual_value:.2f} "
                f"to {('improve' if improvement else 'worsen')} {target_domain} impact by {abs(impact_delta):.3f}"
            )
        }
        self.counterfactual_cache[cache_key] = result
        return result

    async def analyze_tradeoffs(self, scenarios: List[Dict[str, Any]], weights: Dict[str, float] = None) -> List[Dict[str, Any]]:
        async with self._lock:
            if weights is None:
                weights = {
                    'carbon': 0.25, 'helium': 0.20, 'energy': 0.15,
                    'quantum': 0.15, 'biodiversity': 0.15, 'latency': 0.10
                }
            results = []
            for scenario in scenarios:
                impacts = {}
                sustainability_score = 0.0
                risk_factors = []
                for domain, value in scenario.items():
                    if domain in self.domain_mapping:
                        propagated = await self.propagate_constraints(domain, value, scenario)
                        impacts[domain] = propagated
                        domain_score = 1.0 - min(1.0, value)
                        sustainability_score += domain_score * weights.get(domain, 0.1)
                for domain, impact_data in impacts.items():
                    if 'causal_impact' in impact_data and impact_data['causal_impact'] > 0.7:
                        risk_factors.append(f"{domain} has high causal impact")
                results.append({
                    'scenario': scenario,
                    'impacts': impacts,
                    'sustainability_score': min(1.0, sustainability_score),
                    'risk_factors': risk_factors,
                    'recommendations': self._generate_tradeoff_recommendations(impacts, risk_factors)
                })
            results.sort(key=lambda x: x['sustainability_score'], reverse=True)
            return results

    def _generate_tradeoff_recommendations(self, impacts: Dict, risk_factors: List[str]) -> List[str]:
        recommendations = []
        for domain, impact in impacts.items():
            if 'causal_impact' in impact and impact['causal_impact'] > 0.6:
                if domain == 'carbon':
                    recommendations.append("Carbon impact high - consider carbon offset or reduction")
                elif domain == 'helium':
                    recommendations.append("Helium impact high - optimize helium usage")
                elif domain == 'energy':
                    recommendations.append("Energy impact high - improve energy efficiency")
        if risk_factors:
            recommendations.append(f"Monitor these risk factors: {', '.join(risk_factors[:3])}")
        return recommendations or ["No critical trade-offs identified"]

    async def get_causal_path(self, source: str, target: str) -> List[Tuple[str, str, float]]:
        async with self._lock:
            if source not in self.causal_graph or target not in self.causal_graph:
                return []
            try:
                path = nx.shortest_path(self.causal_graph, source, target)
                path_edges = []
                for i in range(len(path) - 1):
                    u, v = path[i], path[i + 1]
                    strength = self.causal_strengths.get((u, v), 0.5)
                    path_edges.append((u, v, strength))
                return path_edges
            except nx.NetworkXNoPath:
                return []

    async def get_causal_strength(self, source: str, target: str) -> float:
        return self.causal_strengths.get((source, target), 0.0)

    def get_causal_graph_summary(self) -> Dict[str, Any]:
        return {
            'nodes': list(self.causal_graph.nodes()),
            'edges': list(self.causal_graph.edges()),
            'edge_count': len(self.causal_graph.edges()),
            'node_count': len(self.causal_graph.nodes()),
            'causal_strengths': self.causal_strengths,
            'recent_impacts': list(self.impact_history)[-10:],
            'counterfactual_cache_size': len(self.counterfactual_cache)
        }

# ============================================================================
# Enhanced Signal Integration Engine with Adaptive Weights
# ============================================================================
class SignalIntegrationEngine:
    """Signal integration with adaptive weights and temporal smoothing."""

    def __init__(self):
        self.signal_weights: Dict[str, float] = {
            'carbon': 0.25, 'helium': 0.20, 'energy': 0.15,
            'quantum': 0.15, 'trust': 0.15, 'stress': 0.10
        }
        self.signal_history: Dict[str, deque] = defaultdict(lambda: deque(maxlen=50))
        self.integration_history: deque = deque(maxlen=1000)
        self._lock = asyncio.Lock()
        self.weight_decay = 0.9
        self.learning_rate = 0.05
        logger.info("SignalIntegrationEngine initialized with adaptive weights")

    async def integrate_signals(self, signals: Dict[str, float], temporal_window: int = 5) -> Dict[str, Any]:
        async with self._lock:
            # Update history
            for name, value in signals.items():
                if name not in self.signal_history:
                    self.signal_history[name] = deque(maxlen=100)
                self.signal_history[name].append(value)

            # Calculate weighted sum
            integrated_value = 0.0
            total_weight = 0.0
            for name, value in signals.items():
                weight = self.signal_weights.get(name, 0.1)
                integrated_value += value * weight
                total_weight += weight
            if total_weight > 0:
                integrated_value /= total_weight

            # Confidence based on signal agreement
            signals_list = list(signals.values())
            std_dev = np.std(signals_list) if len(signals_list) > 1 else 0.1
            confidence = max(0.0, min(1.0, 1.0 - std_dev * 2))

            # Trend calculation
            trend = "stable"
            if len(self.signal_history) > temporal_window:
                recent_values = [
                    list(self.signal_history[name])[-temporal_window:]
                    for name in self.signal_weights.keys()
                    if name in self.signal_history and len(self.signal_history[name]) >= temporal_window
                ]
                if recent_values:
                    avg_recent = np.mean([np.mean(v) for v in recent_values])
                    avg_older = np.mean([
                        np.mean(list(self.signal_history[name])[-temporal_window*2:-temporal_window])
                        for name in self.signal_weights.keys()
                        if name in self.signal_history and len(self.signal_history[name]) >= temporal_window*2
                    ]) if len(self.signal_history) > temporal_window*2 else avg_recent
                    if avg_recent > avg_older * 1.05:
                        trend = "improving"
                    elif avg_recent < avg_older * 0.95:
                        trend = "declining"

            result = {
                'integrated_value': integrated_value,
                'confidence': confidence,
                'trend': trend,
                'individual_signals': signals,
                'weights': self.signal_weights.copy(),
                'signal_agreement': 1.0 - std_dev if len(signals_list) > 1 else 0.5
            }
            self.integration_history.append(result)

            # Update weights adaptively based on recent reliability
            await self._update_weights(signals, result)

            return result

    async def _update_weights(self, signals: Dict[str, float], result: Dict):
        """Adapt weights based on signal reliability."""
        if len(self.integration_history) < 5:
            return
        # Compute recent reliability of each signal (inverse of std over time)
        for name in signals:
            if name in self.signal_history and len(self.signal_history[name]) > 5:
                recent = list(self.signal_history[name])[-5:]
                std = np.std(recent)
                reliability = 1.0 / (1.0 + std * 10)
                # Update weight towards reliability
                old_weight = self.signal_weights.get(name, 0.1)
                new_weight = old_weight * self.weight_decay + reliability * (1 - self.weight_decay)
                self.signal_weights[name] = max(0.05, min(0.5, new_weight))
        # Normalize weights
        total = sum(self.signal_weights.values())
        if total > 0:
            for k in self.signal_weights:
                self.signal_weights[k] /= total

    def update_weights(self, new_weights: Dict[str, float]):
        self.signal_weights.update(new_weights)

    def get_integration_stats(self) -> Dict[str, Any]:
        return {
            'current_weights': self.signal_weights.copy(),
            'history_count': len(self.integration_history),
            'recent_integration': self.integration_history[-1] if self.integration_history else None
        }

# ============================================================================
# Telemetry Collector (NEW)
# ============================================================================
class TelemetryCollector:
    """Collects and exports metrics for monitoring (Prometheus-style)."""

    def __init__(self):
        self.metrics: Dict[str, Any] = defaultdict(lambda: defaultdict(int))
        self._lock = asyncio.Lock()

    def increment(self, metric_name: str, tags: Optional[Dict[str, str]] = None, value: float = 1.0):
        key = self._make_key(metric_name, tags)
        self.metrics['counters'][key] += value

    def gauge(self, metric_name: str, value: float, tags: Optional[Dict[str, str]] = None):
        key = self._make_key(metric_name, tags)
        self.metrics['gauges'][key] = value

    def histogram(self, metric_name: str, value: float, tags: Optional[Dict[str, str]] = None):
        key = self._make_key(metric_name, tags)
        if key not in self.metrics['histograms']:
            self.metrics['histograms'][key] = []
        self.metrics['histograms'][key].append(value)
        if len(self.metrics['histograms'][key]) > 1000:
            self.metrics['histograms'][key] = self.metrics['histograms'][key][-1000:]

    def _make_key(self, metric_name: str, tags: Optional[Dict[str, str]]) -> str:
        if tags:
            tag_str = ','.join(f"{k}={v}" for k, v in sorted(tags.items()))
            return f"{metric_name}{{{tag_str}}}"
        return metric_name

    async def export(self) -> str:
        # Prometheus text format
        output = []
        for key, value in self.metrics['counters'].items():
            output.append(f"# TYPE {key} counter\n{key} {value}")
        for key, value in self.metrics['gauges'].items():
            output.append(f"# TYPE {key} gauge\n{key} {value}")
        for key, values in self.metrics['histograms'].items():
            output.append(f"# TYPE {key} histogram\n{key}_count {len(values)}\n{key}_sum {sum(values)}")
        return "\n".join(output)

    def reset(self):
        self.metrics.clear()
        self.metrics['counters'] = defaultdict(int)
        self.metrics['gauges'] = {}
        self.metrics['histograms'] = defaultdict(list)

# ============================================================================
# Signal Transduction Engine (Preserved with minor enhancements)
# ============================================================================
class SignalTransductionEngine:
    """Biological signal transduction engine for expert routing."""

    def __init__(self):
        self.receptors: Dict[str, SignalReceptor] = {}
        self.second_messengers: Dict[SecondMessenger, SecondMessengerSystem] = {}
        self.amplification_history: deque = deque(maxlen=1000)
        self.crosstalk_matrix: Dict[Tuple[str, str], float] = {}
        self._initialize_signaling_systems()
        asyncio.create_task(self._signal_degradation_loop())
        logger.info("SignalTransductionEngine initialized")

    def _initialize_signaling_systems(self):
        self.second_messengers[SecondMessenger.cAMP] = SecondMessengerSystem(
            messenger_type=SecondMessenger.cAMP, baseline=0.1, threshold=0.3,
            synthesis_rate=0.15, degradation_rate=0.08, amplification_factor=100.0,
            half_life_seconds=3.0, target_proteins=['energy_expert', 'routing_kinase']
        )
        self.second_messengers[SecondMessenger.CALCIUM] = SecondMessengerSystem(
            messenger_type=SecondMessenger.CALCIUM, baseline=0.05, threshold=0.2,
            synthesis_rate=0.2, degradation_rate=0.1, amplification_factor=1000.0,
            half_life_seconds=1.0, target_proteins=['all_experts', 'emergency_response']
        )
        self.second_messengers[SecondMessenger.IP3] = SecondMessengerSystem(
            messenger_type=SecondMessenger.IP3, baseline=0.05, threshold=0.25,
            synthesis_rate=0.1, degradation_rate=0.06, amplification_factor=500.0,
            half_life_seconds=4.0, target_proteins=['gradient_effectors', 'compartment_activation']
        )
        self.second_messengers[SecondMessenger.NITRIC_OXIDE] = SecondMessengerSystem(
            messenger_type=SecondMessenger.NITRIC_OXIDE, baseline=0.02, threshold=0.15,
            synthesis_rate=0.12, degradation_rate=0.15, amplification_factor=200.0,
            half_life_seconds=2.0, target_proteins=['neighboring_compartments', 'vascular_signaling']
        )

    def create_receptor(self, receptor_id: str, signal_type: SignalType,
                        ligand: str, affinity: float = 0.5,
                        amplification: AmplificationLevel = AmplificationLevel.MODERATE) -> SignalReceptor:
        receptor = SignalReceptor(receptor_id=receptor_id, signal_type=signal_type,
                                  ligand=ligand, affinity=affinity, amplification=amplification)
        self.receptors[receptor_id] = receptor
        return receptor

    def bind_ligand(self, receptor_id: str, ligand_concentration: float) -> bool:
        if receptor_id not in self.receptors:
            return False
        receptor = self.receptors[receptor_id]
        if receptor.state == ReceptorState.DESENSITIZED:
            return False
        binding_prob = receptor.affinity * ligand_concentration
        if np.random.random() < binding_prob:
            receptor.state = ReceptorState.BOUND
            receptor.bound_ligands += 1
            receptor.last_activated = datetime.utcnow()
            if receptor.bound_ligands >= 2:
                receptor.state = ReceptorState.ACTIVATED
                receptor.activation_count += 1
                self._activate_cascade(receptor)
                receptor.desensitization_time = 5.0
                receptor.state = ReceptorState.DESENSITIZED
                return True
        return False

    def _activate_cascade(self, receptor: SignalReceptor):
        if receptor.ligand in ['carbon_gradient', 'energy_signal']:
            messenger = SecondMessenger.cAMP
        elif receptor.ligand in ['emergency', 'stress_signal']:
            messenger = SecondMessenger.CALCIUM
        elif receptor.ligand in ['gradient_change', 'opportunity']:
            messenger = SecondMessenger.IP3
        else:
            messenger = SecondMessenger.NITRIC_OXIDE

        if messenger in self.second_messengers:
            sm = self.second_messengers[messenger]
            amp_factors = {AmplificationLevel.NONE: 1, AmplificationLevel.LOW: 10,
                          AmplificationLevel.MODERATE: 100, AmplificationLevel.HIGH: 1000,
                          AmplificationLevel.MAXIMUM: 10000}
            amp = amp_factors.get(receptor.amplification, 100)
            synthesis = sm.synthesis_rate * amp / 100.0
            sm.concentration = min(sm.max_concentration, sm.concentration + synthesis)
            self.amplification_history.append({
                'receptor': receptor.receptor_id, 'messenger': messenger.value,
                'amplification': amp, 'concentration': sm.concentration,
                'timestamp': datetime.utcnow().isoformat()
            })

    def get_second_messenger_level(self, messenger: SecondMessenger) -> float:
        if messenger in self.second_messengers:
            return self.second_messengers[messenger].concentration
        return 0.0

    def is_pathway_active(self, messenger: SecondMessenger) -> bool:
        if messenger in self.second_messengers:
            return self.second_messengers[messenger].concentration > self.second_messengers[messenger].threshold
        return False

    async def _signal_degradation_loop(self):
        while True:
            try:
                for sm in self.second_messengers.values():
                    sm.concentration = max(0.0, sm.concentration - sm.degradation_rate)
                for receptor in self.receptors.values():
                    if receptor.state == ReceptorState.DESENSITIZED:
                        receptor.desensitization_time -= 1.0
                        if receptor.desensitization_time <= 0:
                            receptor.state = ReceptorState.RESENSITIZED
                            receptor.bound_ligands = 0
                await asyncio.sleep(1.0)
            except Exception as e:
                logger.error(f"Signal degradation error: {str(e)}")
                await asyncio.sleep(5.0)

    def setup_crosstalk(self, pathway_a: SecondMessenger, pathway_b: SecondMessenger, strength: float):
        self.crosstalk_matrix[(pathway_a.value, pathway_b.value)] = strength
        self.crosstalk_matrix[(pathway_b.value, pathway_a.value)] = strength * 0.7

    def apply_crosstalk(self):
        for (path_a, path_b), strength in self.crosstalk_matrix.items():
            messenger_a = SecondMessenger(path_a)
            messenger_b = SecondMessenger(path_b)
            if messenger_a in self.second_messengers and messenger_b in self.second_messengers:
                sm_a = self.second_messengers[messenger_a]
                sm_b = self.second_messengers[messenger_b]
                if sm_a.concentration > sm_a.threshold:
                    sm_b.concentration = min(sm_b.max_concentration,
                        sm_b.concentration + sm_a.concentration * strength * 0.1)

    def get_signaling_status(self) -> Dict[str, Any]:
        return {
            'receptors': {rid: {'state': r.state.value, 'ligand': r.ligand,
                                'activations': r.activation_count}
                         for rid, r in self.receptors.items()},
            'second_messengers': {sm.value: {'concentration': m.concentration,
                                              'active': m.concentration > m.threshold}
                                  for sm, m in self.second_messengers.items()}
        }

# ============================================================================
# Allosteric Regulation System (Preserved)
# ============================================================================
class AllostericRegulationSystem:
    """Allosteric regulation for routing decisions."""

    def __init__(self):
        self.allosteric_sites: Dict[str, AllostericSite] = {}
        self.conformational_state: float = 0.5
        self.cooperativity: Dict[Tuple[str, str], float] = {}
        self.regulation_history: deque = deque(maxlen=1000)
        self._initialize_allosteric_sites()
        logger.info("AllostericRegulationSystem initialized")

    def _initialize_allosteric_sites(self):
        self.allosteric_sites['carbon_site'] = AllostericSite('carbon_site', 'carbon_gradient', 'modulation', 0.7)
        self.allosteric_sites['helium_site'] = AllostericSite('helium_site', 'helium_gradient', 'inhibitory', 0.6)
        self.allosteric_sites['token_site'] = AllostericSite('token_site', 'token_availability', 'activating', 0.8)
        self.allosteric_sites['trust_site'] = AllostericSite('trust_site', 'trust_gradient', 'activating', 0.5)
        self.allosteric_sites['stress_site'] = AllostericSite('stress_site', 'stress_signal', 'inhibitory', 0.9)

    def bind_modulator(self, site_id: str, modulator_concentration: float) -> float:
        if site_id not in self.allosteric_sites:
            return 0.0
        site = self.allosteric_sites[site_id]
        n = 2.0
        Kd = 1.0 - site.binding_affinity
        occupancy = (modulator_concentration ** n) / (Kd ** n + modulator_concentration ** n)
        site.current_occupancy = occupancy
        if site.effect == 'activating':
            change = occupancy * 0.2
        elif site.effect == 'inhibitory':
            change = -occupancy * 0.2
        else:
            change = (occupancy - 0.5) * 0.1
        site.conformational_change = change
        self.conformational_state = max(0.0, min(1.0, self.conformational_state + change))
        self.regulation_history.append({
            'site': site_id, 'modulator': site.modulator,
            'concentration': modulator_concentration, 'occupancy': occupancy,
            'new_state': self.conformational_state, 'timestamp': datetime.utcnow().isoformat()
        })
        return change

    def get_routing_modulation(self) -> Dict[str, float]:
        state = self.conformational_state
        return {
            'exploration_rate': state * 0.3, 'exploitation_rate': 1.0 - state * 0.3,
            'risk_tolerance': state * 0.5, 'conservation_mode': (1.0 - state) * 0.8,
            'cooperativity_factor': state * 0.4, 'competition_factor': (1.0 - state) * 0.3
        }

    def setup_cooperativity(self, expert_a: str, expert_b: str, strength: float):
        self.cooperativity[(expert_a, expert_b)] = strength
        self.cooperativity[(expert_b, expert_a)] = strength

    def get_cooperativity_bonus(self, expert_a: str, expert_b: str) -> float:
        return self.cooperativity.get((expert_a, expert_b), 0.0)

    def get_regulation_status(self) -> Dict[str, Any]:
        return {
            'conformational_state': self.conformational_state,
            'state_description': 'relaxed' if self.conformational_state > 0.6 else
                                'tense' if self.conformational_state < 0.4 else 'intermediate',
            'routing_modulation': self.get_routing_modulation()
        }

# ============================================================================
# Metabolic Pathway Router (Preserved)
# ============================================================================
class MetabolicPathwayRouter:
    """Routes tasks through optimal metabolic pathways."""

    def __init__(self):
        self.pathways: Dict[str, MetabolicPathway] = {}
        self.enzyme_kinetics: Dict[str, Dict[str, float]] = {}
        self.product_levels: Dict[str, float] = defaultdict(float)
        self.throughput_history: deque = deque(maxlen=1000)
        self._initialize_pathways()
        logger.info("MetabolicPathwayRouter initialized")

    def _initialize_pathways(self):
        self.pathways['energy_optimization'] = MetabolicPathway(
            'energy_optimization', 'optimization_task', ['energy_expert'],
            ['energy_analysis', 'optimization_plan', 'execution_strategy'],
            'optimized_energy_plan', 'optimization_plan', 10.0,
            [AllostericSite('energy_carbon_site', 'carbon_gradient', 'inhibitory', 0.6),
             AllostericSite('energy_token_site', 'token_availability', 'activating', 0.8)]
        )
        self.pathways['data_processing'] = MetabolicPathway(
            'data_processing', 'data_task', ['data_expert'],
            ['data_ingestion', 'transformation', 'analysis', 'output'],
            'processed_data', 'transformation', 8.0,
            [AllostericSite('data_helium_site', 'helium_gradient', 'inhibitory', 0.5),
             AllostericSite('data_trust_site', 'trust_gradient', 'activating', 0.7)]
        )
        self.pathways['edge_computing'] = MetabolicPathway(
            'edge_computing', 'edge_task', ['iot_expert'],
            ['local_processing', 'mesh_routing', 'result_aggregation'],
            'edge_result', 'mesh_routing', 5.0,
            [AllostericSite('edge_opportunity_site', 'opportunity_gradient', 'activating', 0.9)]
        )
        self.pathways['quantum_computing'] = MetabolicPathway(
            'quantum_computing', 'quantum_task', ['quantum_expert'],
            ['circuit_preparation', 'execution', 'error_mitigation', 'measurement'],
            'quantum_result', 'execution', 50.0,
            [AllostericSite('quantum_complexity_site', 'task_complexity', 'activating', 0.4)]
        )
        for pathway in self.pathways.values():
            for enzyme in pathway.enzymes:
                self.enzyme_kinetics[enzyme] = {'Km': 0.5, 'Vmax': 1.0, 'kcat': 10.0, 'specificity': 0.8}

    def calculate_reaction_rate(self, enzyme: str, substrate_concentration: float) -> float:
        if enzyme not in self.enzyme_kinetics:
            return 0.0
        kinetics = self.enzyme_kinetics[enzyme]
        return kinetics['Vmax'] * substrate_concentration / (kinetics['Km'] + substrate_concentration)

    def apply_competitive_inhibition(self, enzyme: str, inhibitor_concentration: float,
                                     inhibition_constant: float = 0.1) -> float:
        if enzyme not in self.enzyme_kinetics:
            return 1.0
        kinetics = self.enzyme_kinetics[enzyme]
        apparent_Km = kinetics['Km'] * (1 + inhibitor_concentration / inhibition_constant)
        return kinetics['Km'] / apparent_Km

    def apply_allosteric_regulation(self, pathway_id: str, modulator_levels: Dict[str, float]) -> float:
        if pathway_id not in self.pathways:
            return 1.0
        pathway = self.pathways[pathway_id]
        throughput_multiplier = 1.0
        for site in pathway.allosteric_regulators:
            if site.modulator in modulator_levels:
                concentration = modulator_levels[site.modulator]
                n = 1.5
                Kd = 1.0 - site.binding_affinity
                occupancy = concentration ** n / (Kd ** n + concentration ** n)
                if site.effect == 'activating':
                    throughput_multiplier *= (1.0 + occupancy * 0.5)
                elif site.effect == 'inhibitory':
                    throughput_multiplier *= (1.0 - occupancy * 0.5)
        return max(0.1, throughput_multiplier)

    def select_optimal_pathway(self, task_type: str, substrate_concentration: float,
                               modulator_levels: Dict[str, float], energy_budget: float) -> Tuple[Optional[str], float]:
        candidates = []
        for pathway_id, pathway in self.pathways.items():
            if task_type not in pathway.input_substrate and pathway.input_substrate not in task_type:
                continue
            if not pathway.is_active:
                continue
            total_rate = 0.0
            for enzyme in pathway.enzymes:
                rate = self.calculate_reaction_rate(enzyme, substrate_concentration)
                inhibitor_level = sum(self.product_levels.get(p.final_product, 0)
                                     for p in self.pathways.values() if p.pathway_id != pathway_id)
                inhibition = self.apply_competitive_inhibition(enzyme, inhibitor_level)
                rate *= inhibition
                total_rate += rate
            avg_rate = total_rate / max(len(pathway.enzymes), 1)
            allosteric_multiplier = self.apply_allosteric_regulation(pathway_id, modulator_levels)
            regulated_rate = avg_rate * allosteric_multiplier
            energy_efficiency = regulated_rate / max(pathway.energy_cost_ecoatp, 1)
            if pathway.energy_cost_ecoatp > energy_budget:
                energy_efficiency *= 0.3
            candidates.append((pathway_id, energy_efficiency))
        if not candidates:
            return None, 0.0
        candidates.sort(key=lambda x: x[1], reverse=True)
        return candidates[0]

    def record_throughput(self, pathway_id: str, actual_rate: float, energy_used: float):
        self.throughput_history.append({
            'pathway': pathway_id, 'rate': actual_rate, 'energy': energy_used,
            'timestamp': datetime.utcnow().isoformat()
        })
        if pathway_id in self.pathways:
            product = self.pathways[pathway_id].final_product
            self.product_levels[product] += actual_rate * 0.1

    def apply_product_inhibition(self):
        for product, level in self.product_levels.items():
            for pathway in self.pathways.values():
                if pathway.final_product == product and level > 5.0:
                    pathway.throughput_rate *= 0.9
                    self.product_levels[product] *= 0.8

    def get_pathway_stats(self) -> Dict[str, Any]:
        return {pid: {'throughput_rate': p.throughput_rate, 'energy_cost': p.energy_cost_ecoatp,
                      'is_active': p.is_active} for pid, p in self.pathways.items()}

# ============================================================================
# Enhanced Expert Router (Main Class)
# ============================================================================
class ExpertRouter:
    """
    Enhanced Expert Router v8.0.1 - Complete Signal Transduction Cascade with Causal Constraints
    """

    def __init__(self, config: Optional[ExpertRouterConfig] = None, **kwargs):
        if config is None:
            # Legacy mode: build config from kwargs
            config = ExpertRouterConfig(
                enable_quantum=kwargs.get('enable_quantum', False),
                enable_signal_transduction=kwargs.get('enable_signal_transduction', True),
                enable_allosteric=kwargs.get('enable_allosteric', True),
                enable_metabolic_pathways=kwargs.get('enable_metabolic_pathways', True),
                enable_cooperative_binding=kwargs.get('enable_cooperative_binding', True),
                enable_homeostasis=kwargs.get('enable_homeostasis', True),
                enable_bio_integration=kwargs.get('enable_bio_integration', True),
                enable_federated=kwargs.get('enable_federated', True),
                enable_predictive=kwargs.get('enable_predictive', True),
                enable_carbon_intensity=kwargs.get('enable_carbon_intensity', True),
                enable_helium_optimization=kwargs.get('enable_helium_optimization', True),
                enable_causal_constraints=kwargs.get('enable_causal_constraints', True),
                enable_counterfactual=kwargs.get('enable_counterfactual', True),
                enable_signal_integration=kwargs.get('enable_signal_integration', True),
                enable_differential_privacy=kwargs.get('enable_differential_privacy', True),
                enable_uncertainty_quantification=kwargs.get('enable_uncertainty_quantification', True),
                enable_telemetry=kwargs.get('enable_telemetry', True),
                carbon_api_region=kwargs.get('carbon_api_region', 'us-east'),
                max_retries=kwargs.get('max_retries', 3),
                server_url=kwargs.get('server_url', None),
                helium_budget_l=kwargs.get('helium_budget_l', 100.0),
                privacy_epsilon=kwargs.get('privacy_epsilon', 1.0),
                max_concurrent_routes=kwargs.get('max_concurrent_routes', 100),
            )
        self.config = config

        # Feature flags from config
        self.enable_signal_transduction = config.enable_signal_transduction
        self.enable_allosteric = config.enable_allosteric
        self.enable_metabolic_pathways = config.enable_metabolic_pathways
        self.enable_cooperative_binding = config.enable_cooperative_binding
        self.enable_homeostasis = config.enable_homeostasis
        self.enable_bio_integration = config.enable_bio_integration
        self.enable_federated = config.enable_federated
        self.enable_predictive = config.enable_predictive
        self.enable_carbon_intensity = config.enable_carbon_intensity
        self.enable_helium_optimization = config.enable_helium_optimization
        self.enable_causal_constraints = config.enable_causal_constraints
        self.enable_counterfactual = config.enable_counterfactual
        self.enable_signal_integration = config.enable_signal_integration
        self.enable_differential_privacy = config.enable_differential_privacy
        self.enable_uncertainty_quantification = config.enable_uncertainty_quantification
        self.enable_telemetry = config.enable_telemetry

        # Initialize modules
        self.carbon_manager = CarbonIntensityManager(config) if self.enable_carbon_intensity else None
        self.helium_optimizer = HeliumEfficiencyOptimizer(config, self.carbon_manager) if self.enable_helium_optimization else None
        self.federated_learner = FederatedRoutingLearner(config) if self.enable_federated else None
        self.predictive_analyzer = PredictiveRoutingAnalyzer(config) if self.enable_predictive else None
        self.causal_model = CausalConstraintModel(config) if self.enable_causal_constraints else None
        self.signal_integrator = SignalIntegrationEngine() if self.enable_signal_integration else None
        self.signal_engine = SignalTransductionEngine() if self.enable_signal_transduction else None
        self.allosteric_system = AllostericRegulationSystem() if self.enable_allosteric else None
        self.metabolic_router = MetabolicPathwayRouter() if self.enable_metabolic_pathways else None
        self.telemetry = TelemetryCollector() if self.enable_telemetry else None

        # Bio-inspired module references (injected)
        self.gradient_manager = None
        self.token_manager = None
        self.scheduler = None
        self.compartment_manager = None
        self.biomass_storage = None
        self.harvester = None
        self.bio_core = None

        # Initialize signal receptors
        if self.signal_engine:
            self.signal_engine.create_receptor('carbon_receptor', SignalType.ENDOCRINE,
                'carbon_gradient', affinity=0.7, amplification=AmplificationLevel.HIGH)
            self.signal_engine.create_receptor('helium_receptor', SignalType.ENDOCRINE,
                'helium_gradient', affinity=0.6, amplification=AmplificationLevel.MODERATE)
            self.signal_engine.create_receptor('task_receptor', SignalType.NEUROTRANSMITTER,
                'task_signal', affinity=0.9, amplification=AmplificationLevel.HIGH)
            self.signal_engine.create_receptor('stress_receptor', SignalType.AUTOCRINE,
                'stress_signal', affinity=0.8, amplification=AmplificationLevel.MAXIMUM)
            self.signal_engine.create_receptor('trust_receptor', SignalType.PARACRINE,
                'trust_gradient', affinity=0.5, amplification=AmplificationLevel.LOW)
            self.signal_engine.setup_crosstalk(SecondMessenger.cAMP, SecondMessenger.IP3, 0.3)
            self.signal_engine.setup_crosstalk(SecondMessenger.CALCIUM, SecondMessenger.cAMP, 0.5)

        if self.allosteric_system:
            self.allosteric_system.setup_cooperativity('energy', 'data', 0.4)
            self.allosteric_system.setup_cooperativity('energy', 'helium', 0.3)
            self.allosteric_system.setup_cooperativity('data', 'iot', 0.5)

        self.metrics_collector = None  # legacy
        self.metrics = RoutingMetrics()
        self.experts: Dict[str, Any] = {}
        self.expert_index_map: Dict[int, str] = {}
        self.circuit_breakers: Dict[str, ExpertCircuitBreaker] = {}
        self.gating_network = None
        self.active_routes = 0
        self.max_concurrent_routes = config.max_concurrent_routes
        self._route_lock = asyncio.Lock()
        self.routing_history: deque = deque(maxlen=10000)

        self._initialize_experts(config.enable_quantum)
        self._start_background_tasks()

        # Initialize gating network
        from .gating_network import GatingNetworkManager
        self.gating_network = GatingNetworkManager(
            input_dim=10,
            num_experts=len(self.experts)
        )

        logger.info(f"ExpertRouter v8.0.1 initialized with all enhancements")

    def _initialize_experts(self, enable_quantum: bool):
        try:
            from .experts.energy_expert import EnergyExpert
            from .experts.data_expert import DataExpert
            from .experts.iot_expert import IoTExpert
            from .experts.helium_expert import HeliumExpert

            self.experts = {
                'energy': EnergyExpert(),
                'data': DataExpert(),
                'iot': IoTExpert(),
                'helium': HeliumExpert()
            }
            if enable_quantum:
                from .experts.quantum_expert import QuantumExpert
                self.experts['quantum'] = QuantumExpert()

            for idx, (expert_id, expert) in enumerate(self.experts.items()):
                self.expert_index_map[idx] = expert_id
                self.circuit_breakers[expert_id] = ExpertCircuitBreaker(expert_id=expert_id)
            logger.info(f"Initialized {len(self.experts)} experts")
        except Exception as e:
            logger.error(f"Failed to initialize experts: {str(e)}")

    def _start_background_tasks(self):
        asyncio.create_task(self._signal_transduction_loop())
        asyncio.create_task(self._homeostasis_loop())
        asyncio.create_task(self._product_inhibition_loop())
        if self.enable_carbon_intensity:
            asyncio.create_task(self._carbon_update_loop())
        if self.enable_federated:
            asyncio.create_task(self._federated_sync_loop())
        if self.enable_predictive:
            asyncio.create_task(self._predictive_update_loop())
        if self.enable_telemetry:
            asyncio.create_task(self._telemetry_export_loop())

    # ============================================================================
    # Background Loops (Enhanced)
    # ============================================================================
    async def _carbon_update_loop(self):
        while True:
            try:
                if self.carbon_manager:
                    await self.carbon_manager.update_carbon_intensity()
                    if self.telemetry:
                        intensity = await self.carbon_manager.get_current_intensity()
                        self.telemetry.gauge('carbon_intensity', intensity)
                        price = await self.carbon_manager.get_current_price()
                        self.telemetry.gauge('carbon_price_usd', price)
                await asyncio.sleep(self.config.carbon_update_interval if self.carbon_manager else 300)
            except Exception as e:
                logger.error(f"Carbon update error: {str(e)}")
                await asyncio.sleep(60)

    async def _federated_sync_loop(self):
        while True:
            try:
                if self.federated_learner and self.routing_history:
                    routing_data = []
                    for record in list(self.routing_history)[-100:]:
                        routing_data.append({
                            'carbon_zone': record.get('context', {}).get('carbon_zone', 0),
                            'helium_scarcity': record.get('context', {}).get('helium_scarcity', 0.5),
                            'task_complexity': record.get('context', {}).get('task_complexity', 0.5),
                            'token_balance': 500,
                            'carbon_gradient': 0.5,
                            'trust_gradient': 0.5,
                            'opportunity_gradient': 0.5,
                            'stress_level': 0.3,
                            'latency_budget': 100,
                            'energy_budget': 100,
                            'selected_expert_idx': 0
                        })
                    await self.federated_learner.participate_in_round(
                        routing_data,
                        performance=self.metrics.success_rate
                    )
                    if self.telemetry:
                        self.telemetry.gauge('federated_round', self.federated_learner.round)
                await asyncio.sleep(3600)
            except Exception as e:
                logger.error(f"Federated sync error: {str(e)}")
                await asyncio.sleep(300)

    async def _predictive_update_loop(self):
        while True:
            try:
                if self.predictive_analyzer:
                    self.predictive_analyzer.update_history({
                        'success_rate': self.metrics.success_rate,
                        'avg_latency_ms': self.metrics.average_latency_ms,
                        'carbon_efficiency': 0.5,
                        'helium_efficiency': 0.5,
                        'expert_utilization': self.active_routes / max(self.max_concurrent_routes, 1)
                    })
                    await self.predictive_analyzer.train_forecast_model()
                    if self.telemetry:
                        forecast = await self.predictive_analyzer.predict_routing_performance()
                        self.telemetry.gauge('predicted_success_rate', forecast.get('predicted_success_rate', 0.5))
                await asyncio.sleep(300)
            except Exception as e:
                logger.error(f"Predictive update error: {str(e)}")
                await asyncio.sleep(60)

    async def _signal_transduction_loop(self):
        while True:
            try:
                if self.signal_engine:
                    gradient_levels = self._get_real_gradient_levels()
                    self.signal_engine.bind_ligand('carbon_receptor', gradient_levels.get('carbon', 0.5))
                    self.signal_engine.bind_ligand('helium_receptor', gradient_levels.get('helium', 0.5))
                    self.signal_engine.bind_ligand('trust_receptor', gradient_levels.get('trust', 0.5))
                    token_level = self._get_real_token_availability()
                    stress_level = self._get_real_stress_level()
                    if stress_level > 0.5:
                        self.signal_engine.bind_ligand('stress_receptor', stress_level)
                    self.signal_engine.apply_crosstalk()
                    if self.allosteric_system:
                        self.allosteric_system.bind_modulator('carbon_site', gradient_levels.get('carbon', 0.5))
                        self.allosteric_system.bind_modulator('helium_site', gradient_levels.get('helium', 0.5))
                        self.allosteric_system.bind_modulator('trust_site', gradient_levels.get('trust', 0.5))
                        self.allosteric_system.bind_modulator('token_site', token_level)
                        if stress_level > 0.3:
                            self.allosteric_system.bind_modulator('stress_site', stress_level)
                await asyncio.sleep(2.0)
            except Exception as e:
                logger.error(f"Signal transduction error: {str(e)}")
                await asyncio.sleep(5.0)

    async def _homeostasis_loop(self):
        while True:
            try:
                if self.enable_homeostasis and self.allosteric_system:
                    modulation = self.allosteric_system.get_routing_modulation()
                    if modulation['conservation_mode'] > 0.7:
                        if np.random.random() < 0.1:
                            self.allosteric_system.bind_modulator('token_site', 0.8)
                    if modulation['risk_tolerance'] > 0.4:
                        self.allosteric_system.bind_modulator('stress_site', 0.3)
                await asyncio.sleep(10.0)
            except Exception as e:
                logger.error(f"Homeostasis error: {str(e)}")
                await asyncio.sleep(30.0)

    async def _product_inhibition_loop(self):
        while True:
            try:
                if self.metabolic_router:
                    self.metabolic_router.apply_product_inhibition()
                await asyncio.sleep(60.0)
            except Exception as e:
                logger.error(f"Product inhibition error: {str(e)}")
                await asyncio.sleep(120.0)

    async def _telemetry_export_loop(self):
        while True:
            try:
                if self.enable_telemetry and self.telemetry:
                    # Simulate export; in production, expose metrics endpoint
                    logger.debug("Telemetry export (simulated)")
                await asyncio.sleep(60)
            except Exception as e:
                logger.error(f"Telemetry export error: {str(e)}")
                await asyncio.sleep(120)

    # ============================================================================
    # Helper Methods
    # ============================================================================
    def _get_real_gradient_levels(self) -> Dict[str, float]:
        if self.gradient_manager:
            return self.gradient_manager.get_field_strengths()
        return {'carbon': 0.5, 'helium': 0.5, 'trust': 0.5, 'opportunity': 0.5}

    def _get_real_token_availability(self) -> float:
        if self.token_manager:
            summary = self.token_manager.get_system_summary()
            return min(1.0, summary.get('total_balance', 500) / 1000)
        return 0.5

    def _get_real_stress_level(self) -> float:
        if self.harvester:
            stats = self.harvester.get_harvesting_stats()
            return stats.get('stress_level', 0.3)
        return 0.3

    def inject_bio_core(self, bio_core: Any):
        """Inject bio-inspired core"""
        self.bio_core = bio_core
        if hasattr(bio_core, 'token_manager'):
            self.token_manager = bio_core.token_manager
        if hasattr(bio_core, 'gradient_manager'):
            self.gradient_manager = bio_core.gradient_manager
        if hasattr(bio_core, 'scheduler'):
            self.scheduler = bio_core.scheduler
        if hasattr(bio_core, 'compartment_manager'):
            self.compartment_manager = bio_core.compartment_manager
        if hasattr(bio_core, 'biomass_storage'):
            self.biomass_storage = bio_core.biomass_storage
        if hasattr(bio_core, 'harvester'):
            self.harvester = bio_core.harvester

    # ============================================================================
    # Public Methods
    # ============================================================================
    def get_routing_stats(self) -> Dict[str, Any]:
        """Get comprehensive routing statistics."""
        stats = {
            'metrics': {
                'total_routes': self.metrics.total_routes,
                'successful_routes': self.metrics.successful_routes,
                'failed_routes': self.metrics.failed_routes,
                'success_rate': self.metrics.success_rate,
                'average_latency_ms': self.metrics.average_latency_ms,
                'carbon_savings_kg': self.metrics.carbon_savings_kg,
                'helium_savings_l': self.metrics.helium_savings_l
            },
            'active_routes': self.active_routes,
            'max_concurrent_routes': self.max_concurrent_routes,
            'experts': list(self.experts.keys()),
            'circuit_breakers': {
                eid: {
                    'state': cb.state.value,
                    'failure_count': cb.failure_count,
                    'success_count': cb.success_count
                }
                for eid, cb in self.circuit_breakers.items()
            },
            'gating_network': self.gating_network is not None
        }

        if self.signal_engine:
            stats['signaling'] = self.signal_engine.get_signaling_status()

        if self.allosteric_system:
            stats['allosteric'] = self.allosteric_system.get_regulation_status()

        if self.metabolic_router:
            stats['pathways'] = self.metabolic_router.get_pathway_stats()

        if self.helium_optimizer:
            stats['helium'] = self.helium_optimizer.get_helium_status()

        if self.federated_learner:
            stats['federated'] = self.federated_learner.get_federated_insights()

        if self.predictive_analyzer:
            stats['predictive'] = self.predictive_analyzer.get_uncertainty_metrics()

        if self.causal_model:
            stats['causal'] = self.causal_model.get_causal_graph_summary()

        if self.signal_integrator:
            stats['signal_integration'] = self.signal_integrator.get_integration_stats()

        if self.telemetry:
            stats['telemetry'] = {
                'counters': len(self.telemetry.metrics['counters']),
                'gauges': len(self.telemetry.metrics['gauges'])
            }

        return stats

    def _build_gating_features(self, context: Dict[str, Any]) -> np.ndarray:
        return np.array([
            context.get('helium_scarcity', 0.5),
            context.get('helium_cost_index', 1.0),
            context.get('carbon_intensity', 0.5),
            context.get('model_loss', 0.0),
            context.get('gradient_variance', 0.0),
            context.get('avg_client_energy', 0.5),
            context.get('gradient_carbon', 0.5),
            context.get('gradient_helium', 0.5),
            context.get('token_balance_norm', 0.5),
            context.get('harvester_stress', 0.3),
        ])

    async def route_task(self, task: Dict[str, Any], context: Dict[str, Any] = None) -> Dict[str, Any]:
        context = context or {}

        # 1. Enrich context with Helium, carbon, bio signals
        if self.helium_optimizer:
            context['helium_scarcity'] = self.helium_optimizer.get_helium_status().get('price_usd_per_l', 0.5)
        if self.carbon_manager:
            carbon_intensity = await self.carbon_manager.get_current_intensity()
            context['carbon_intensity'] = carbon_intensity / 1000.0
        gradients = self._get_real_gradient_levels()
        context['gradient_carbon'] = gradients.get('carbon', 0.5)
        context['gradient_helium'] = gradients.get('helium', 0.5)
        context['gradient_trust'] = gradients.get('trust', 0.5)
        context['token_balance_norm'] = self._get_real_token_availability()
        context['harvester_stress'] = self._get_real_stress_level()

        # 2. Get signal levels
        signal_levels = {
            'carbon': context.get('carbon_zone', 0) / 10,
            'helium': context.get('helium_scarcity', 0.5),
            'energy': context.get('energy_efficiency', 0.5),
            'quantum': context.get('quantum_capable', False),
            'trust': gradients.get('trust', 0.5),
            'stress': self._get_real_stress_level()
        }

        # 3. Integrate signals if enabled
        if self.enable_signal_integration and self.signal_integrator:
            integrated = await self.signal_integrator.integrate_signals(signal_levels)
            decision_signal = integrated['integrated_value']
        else:
            decision_signal = np.mean(list(signal_levels.values()))

        # 4. Use gating network to select expert
        if self.gating_network:
            features = self._build_gating_features(context)
            expert_weights = self.gating_network.predict(features)
            selected_expert = max(expert_weights, key=expert_weights.get)
        else:
            selected_expert = list(self.experts.keys())[0]

        # 5. Record route
        self.metrics.total_routes += 1
        self.metrics.successful_routes += 1
        self.active_routes += 1

        self.routing_history.append({
            'timestamp': datetime.utcnow().isoformat(),
            'task': task,
            'context': context,
            'expert': selected_expert,
            'signal_levels': signal_levels,
            'decision_signal': decision_signal
        })

        # Update metrics (placeholder)
        self.metrics.average_latency_ms = 50.0
        self.metrics.carbon_savings_kg += 0.01
        self.metrics.helium_savings_l += 0.001

        # Telemetry
        if self.telemetry:
            self.telemetry.increment('routes_total')
            self.telemetry.gauge('active_routes', self.active_routes)

        return {
            'success': True,
            'expert': selected_expert,
            'decision_signal': decision_signal,
            'signal_levels': signal_levels,
            'explanation': f"Task routed to {selected_expert} based on gating network",
            'metrics': {
                'latency_ms': 50.0,
                'carbon_savings_kg': 0.01,
                'helium_savings_l': 0.001
            }
        }

    async def shutdown(self):
        logger.info("Shutting down Expert Router")
        if self.carbon_manager:
            await self.carbon_manager.close()
        if self.federated_learner:
            await self.federated_learner.close()
        logger.info("Shutdown complete")
