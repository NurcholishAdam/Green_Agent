# File: quantum_integration/quantum-limit-graph-v2.4.0/limit-agentbench/src/enhancements/moe_expert_system/advanced/circular_computing_manager.py
# Enhanced version v3.0.0 – Full integration with bio‑inspired core, event‑driven, circuit breakers, self‑healing, and deep MoE/SEG integration

"""
Enhanced Circular Computing Module v3.0.0 - Complete Green Agent Implementation
with full bio‑inspired core integration.

New Features:
- Event-driven integration via core EventBroker (carbon, helium, alerts, config)
- Circuit breakers for all external services
- Self-healing and reactive alert handling
- Configuration reload via events
- Swarm coordination via SwarmCoordinator
- Integration with TimeTickEngine and QuantumBridge
- Integration with CostBenefitEngine and PredictiveAlertSystem
- Workflow orchestration triggers on threshold breaches
- Deep MoE and Self-Evolving Gate integration with rich context
- Enhanced telemetry and health monitoring
"""

import asyncio
import logging
import numpy as np
from typing import Dict, Any, List, Optional, Tuple, Set, Union, Callable, Protocol
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone, timedelta
from enum import Enum
from collections import deque, defaultdict
import hashlib
import json
import aiohttp
import os
import pickle
import zlib
from sklearn.preprocessing import StandardScaler
from sklearn.linear_model import SGDRegressor
from sklearn.metrics import r2_score, mean_squared_error
import torch
import torch.nn as nn
import torch.optim as optim
from torch.utils.data import DataLoader, TensorDataset

logger = logging.getLogger(__name__)

# ============================================================================
# Bio-Inspired Core Import (with fallback)
# ============================================================================
try:
    from enhancements.bio_inspired.__init__ import EnhancedBioInspiredCore, BioEvent, CircuitBreaker, Persistence
    from enhancements.bio_inspired.eco_atp_currency import EcoATPTokenManager
    from enhancements.bio_inspired.proton_gradient_fields import GradientFieldManager
    from enhancements.bio_inspired.atp_synthase_scheduler import ATPSynthaseScheduler
    from enhancements.bio_inspired.chromatophore_compartments import CompartmentManager
    from enhancements.bio_inspired.biomass_storage import BiomassStorage
    from enhancements.bio_inspired.photosynthetic_harvester import PhotosyntheticHarvester
    from enhancements.bio_inspired.time_tick_engine import TimeTickEngine
    from enhancements.bio_inspired.quantum_bridge import QuantumBridge
    BIO_INSPIRED_AVAILABLE = True
except ImportError:
    BIO_INSPIRED_AVAILABLE = False
    # Fallback definitions
    class BioEvent:
        def __init__(self, event_type, source, data=None):
            self.event_type = event_type
            self.source = source
            self.data = data or {}

    class CircuitBreaker:
        def __init__(self, name, failure_threshold=3, recovery_timeout=30.0):
            self.name = name
            self.failure_threshold = failure_threshold
            self.recovery_timeout = recovery_timeout
            self._state = "closed"
            self._failure_count = 0
            self._last_failure_time = None
            self._lock = asyncio.Lock()
        async def call(self, func, *args, **kwargs):
            return await func(*args, **kwargs)

# ============================================================================
# MoE and Self-Evolving Gate imports (optional)
# ============================================================================
try:
    from ..expert_router import ExpertRouter
    from ..gating_network import GatingNetworkManager
    from ..advanced.self_evolving_gates import EnhancedSelfEvolvingGate
    MOE_AVAILABLE = True
except ImportError:
    MOE_AVAILABLE = False
    logger.warning("MoE Expert Router or Self-Evolving Gates not available - circular manager will operate standalone")

# ============================================================================
# Helium Provider Interface (unchanged)
# ============================================================================
class HeliumProvider:
    def get_scarcity(self) -> float: raise NotImplementedError
    def get_cost_index(self) -> float: raise NotImplementedError
    def get_efficiency(self) -> float: raise NotImplementedError

# ============================================================================
# Configuration Dataclass (Enhanced)
# ============================================================================

@dataclass
class CircularComputingConfig:
    """Centralized configuration for the Circular Computing Manager."""
    # Budgets
    helium_budget_l: float = 100.0

    # Feature flags
    enable_federated: bool = True
    enable_carbon_intensity: bool = True
    enable_predictive: bool = True
    enable_ml_selection: bool = True
    enable_human_ai: bool = True
    enable_helium_tracking: bool = True
    enable_persistence: bool = True
    enable_telemetry: bool = True
    enable_event_driven: bool = True
    enable_self_healing: bool = True
    enable_swarm_coordination: bool = True
    enable_time_tick_engine: bool = True
    enable_quantum_bridge: bool = True
    enable_cost_benefit: bool = True

    # Helium-to-CO2 equivalence factor (kg CO2 per kg helium)
    helium_to_co2_factor: float = 20.0

    # Retry and circuit breaker
    max_retries: int = 3
    retry_base_delay_ms: float = 100.0
    retry_max_delay_ms: float = 5000.0
    circuit_breaker_failure_threshold: int = 5
    circuit_breaker_recovery_timeout: float = 30.0

    # Predictive analyzer
    predictive_history_window: int = 100

    # ML component selector
    ml_input_size: int = 8
    ml_hidden_size: int = 64
    ml_epochs: int = 100
    ml_batch_size: int = 32

    # Federated learning
    server_url: Optional[str] = None
    federated_sparsity_ratio: float = 0.1  # top-k% of data to keep

    # Persistence
    persistence_path: str = "circular_computing_state.pkl"

    # Telemetry
    telemetry_export_interval: int = 60

    # Workflow triggers
    workflow_on_critical_alert: str = "adjust_circular_strategy"
    workflow_on_slo_breach: str = "rebalance_materials"

    # Swarm sharing interval
    swarm_share_interval: int = 60

# ============================================================================
# Protocols for external modules (unchanged)
# ============================================================================

class CarbonIntensityProvider(Protocol):
    async def get_current_intensity(self) -> float: ...

# ============================================================================
# Retry Helper (unchanged)
# ============================================================================

async def retry_async(
    func: Callable,
    max_retries: int,
    base_delay_ms: float,
    max_delay_ms: float,
    *args,
    **kwargs
) -> Any:
    """Retry an async function with exponential backoff."""
    for attempt in range(max_retries):
        try:
            return await func(*args, **kwargs)
        except Exception as e:
            if attempt == max_retries - 1:
                raise
            delay = min(base_delay_ms * (2 ** attempt), max_delay_ms) / 1000.0
            await asyncio.sleep(delay)
    raise RuntimeError("Max retries exceeded")

# ============================================================================
# Carbon Intensity Manager (Enhanced with circuit breaker)
# ============================================================================

class CarbonIntensityManager:
    """Real-time carbon intensity integration with retry, circuit breaker, and caching."""

    def __init__(self, config: CircularComputingConfig):
        self.config = config
        self.endpoint = "https://api.electricitymap.org/v3/carbon-intensity"
        self.region = "us-east"
        self.carbon_intensity = 0.0
        self.last_update = None
        self._lock = asyncio.Lock()
        self._session = None
        self.update_interval = 300
        self.cache = {}
        self.historical_intensities = deque(maxlen=1000)
        self.api_key = os.getenv('ELECTRICITYMAP_API_KEY', '')
        self.failure_count = 0
        self.circuit_open = False
        self.circuit_open_until: Optional[datetime] = None
        self.circuit_breaker_threshold = config.circuit_breaker_failure_threshold
        self.max_retries = config.max_retries
        self._circuit = CircuitBreaker("carbon_api", failure_threshold=config.circuit_breaker_failure_threshold, recovery_timeout=config.circuit_breaker_recovery_timeout)
        logger.info(f"CarbonIntensityManager initialized (region={self.region}, retries={self.max_retries})")

    async def _get_session(self):
        if self._session is None:
            self._session = aiohttp.ClientSession()
        return self._session

    async def update_carbon_intensity(self, region: Optional[str] = None) -> Dict:
        async def _fetch():
            if region is not None:
                self.region = region
            if self.circuit_open:
                if datetime.now(timezone.utc) < self.circuit_open_until:
                    logger.warning("Circuit breaker open, using fallback data")
                    return self._get_fallback_response()
                else:
                    self.circuit_open = False
                    self.failure_count = 0
                    logger.info("Circuit breaker reset for CarbonIntensityManager")
            cache_key = f"{self.region}_{datetime.now(timezone.utc).hour}"
            if cache_key in self.cache and self.last_update and (datetime.now(timezone.utc) - self.last_update).seconds < self.update_interval:
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
                            self.last_update = datetime.now(timezone.utc)
                            self.cache[cache_key] = {'intensity': self.carbon_intensity, 'timestamp': self.last_update.isoformat()}
                            self.historical_intensities.append(self.carbon_intensity)
                            self.failure_count = 0
                            return {'intensity': self.carbon_intensity, 'region': self.region, 'timestamp': self.last_update.isoformat()}
                        else:
                            logger.warning(f"Carbon API returned {response.status}, attempt {attempt+1}")
                            if attempt == self.max_retries - 1:
                                self.failure_count += 1
                                if self.failure_count >= self.circuit_breaker_threshold:
                                    self.circuit_open = True
                                    self.circuit_open_until = datetime.now(timezone.utc) + timedelta(seconds=self.config.circuit_breaker_recovery_timeout)
                                    logger.error("Circuit breaker opened for CarbonIntensityManager")
                                return self._get_fallback_response()
                            await asyncio.sleep(2 ** attempt)
                except Exception as e:
                    logger.error(f"Carbon API error: {e}, attempt {attempt+1}")
                    if attempt == self.max_retries - 1:
                        self.failure_count += 1
                        if self.failure_count >= self.circuit_breaker_threshold:
                            self.circuit_open = True
                            self.circuit_open_until = datetime.now(timezone.utc) + timedelta(seconds=self.config.circuit_breaker_recovery_timeout)
                        return self._get_fallback_response()
                    await asyncio.sleep(2 ** attempt)
            return self._get_fallback_response()
        return await self._circuit.call(_fetch)

    def _get_fallback_response(self) -> Dict:
        fallback_intensities = {'us-east': 420, 'us-west': 350, 'eu': 280, 'asia': 500}
        intensity = fallback_intensities.get(self.region, 400)
        self.carbon_intensity = intensity
        self.last_update = datetime.now(timezone.utc)
        return {'intensity': intensity, 'region': self.region, 'timestamp': self.last_update.isoformat(), 'is_fallback': True}

    async def get_current_intensity(self) -> float:
        if self.last_update is None or (datetime.now(timezone.utc) - self.last_update).seconds > self.update_interval:
            await self.update_carbon_intensity(self.region)
        return self.carbon_intensity

    async def close(self):
        if self._session:
            await self._session.close()

# ============================================================================
# Helium Lifecycle Manager (unchanged)
# ============================================================================

class HeliumLifecycleManager:
    """
    Real-time helium lifecycle tracking for circular computing.
    """

    def __init__(self, config: CircularComputingConfig):
        self.config = config
        self.helium_budget_l = config.helium_budget_l
        self.helium_usage: deque = deque(maxlen=86400)
        self.helium_recovered: deque = deque(maxlen=86400)
        self.component_helium: Dict[str, Dict[str, Any]] = {}
        self._running_total_usage = 0.0
        self._running_total_recovered = 0.0
        self.helium_to_co2_factor = config.helium_to_co2_factor
        self.recovery_rates = {
            'cooling_system': 0.85,
            'quantum_computer': 0.90,
            'cryogenic_system': 0.80,
            'standard_cooling': 0.75,
            'mri_system': 0.95
        }

        asyncio.create_task(self._helium_accounting_loop())
        logger.info(f"Helium Lifecycle Manager initialized: budget={self.helium_budget_l}L, factor={self.helium_to_co2_factor}")

    def register_component_helium(
        self,
        component_id: str,
        helium_content_l: float,
        component_type: str = 'cooling_system'
    ):
        self.component_helium[component_id] = {
            'total_l': helium_content_l,
            'recovered_l': 0.0,
            'type': component_type,
            'recovery_rate': self.recovery_rates.get(component_type, 0.85),
            'registered_at': datetime.now(timezone.utc)
        }
        logger.info(f"Registered helium content for {component_id}: {helium_content_l}L")

    def track_helium_usage(self, component_id: str, usage_l: float):
        usage = {'component_id': component_id, 'amount_l': usage_l, 'timestamp': datetime.now(timezone.utc)}
        self.helium_usage.append(usage)
        self._running_total_usage += usage_l
        if component_id in self.component_helium:
            self.component_helium[component_id]['used_l'] = self.component_helium[component_id].get('used_l', 0) + usage_l

    def calculate_helium_recovery(self, component_id: str) -> float:
        if component_id not in self.component_helium:
            return 0.0
        component = self.component_helium[component_id]
        total_l = component['total_l']
        used_l = component.get('used_l', 0)
        recovery_rate = component['recovery_rate']
        remaining = total_l - used_l
        recoverable = remaining * recovery_rate
        return max(0, recoverable)

    def record_helium_recovery(self, component_id: str, amount_l: float):
        recovery = {'component_id': component_id, 'amount_l': amount_l, 'timestamp': datetime.now(timezone.utc)}
        self.helium_recovered.append(recovery)
        self._running_total_recovered += amount_l
        if component_id in self.component_helium:
            self.component_helium[component_id]['recovered_l'] += amount_l

    async def _helium_accounting_loop(self):
        while True:
            try:
                net_position = self._running_total_usage - self._running_total_recovered
                remaining_budget = self.helium_budget_l - net_position
                if remaining_budget < 0:
                    logger.critical(f"Helium budget exceeded! Net position: {net_position:.2f} L")
                elif remaining_budget < self.helium_budget_l * 0.2:
                    logger.warning(f"Helium budget warning: {remaining_budget:.2f} L remaining")
                await asyncio.sleep(60)
            except Exception as e:
                logger.error(f"Helium accounting error: {e}")
                await asyncio.sleep(5)

    def get_helium_position(self) -> Dict[str, Any]:
        return {
            'total_usage_l': self._running_total_usage,
            'total_recovered_l': self._running_total_recovered,
            'net_position_l': self._running_total_usage - self._running_total_recovered,
            'remaining_budget_l': self.helium_budget_l - (self._running_total_usage - self._running_total_recovered),
            'co2_equivalent_kg': (self._running_total_usage - self._running_total_recovered) * self.helium_to_co2_factor,
            'components': self.component_helium
        }

# ============================================================================
# Predictive Lifecycle Analyzer (unchanged)
# ============================================================================

class PredictiveLifecycleAnalyzer:
    """Predictive reflexivity with online learning (SGD) for hardware lifecycle."""

    def __init__(self, config: CircularComputingConfig):
        self.config = config
        self.history_window = config.predictive_history_window
        self.lifecycle_history = deque(maxlen=self.history_window)
        self.forecast_history = deque(maxlen=50)
        self.scaler = StandardScaler()
        self.model = None
        self.is_trained = False
        self._ml_available = False
        self._init_model()

    def _init_model(self):
        try:
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
            logger.warning("SGDRegressor not available; using fallback moving average")

    def update_history(self, lifecycle_data: Dict):
        self.lifecycle_history.append({
            'timestamp': datetime.now(timezone.utc),
            'age_days': lifecycle_data.get('age_days', 0),
            'utilization': lifecycle_data.get('utilization', 0.5),
            'maintenance_count': lifecycle_data.get('maintenance_count', 0),
            'carbon_score': lifecycle_data.get('carbon_score', 0.5),
            'helium_remaining': lifecycle_data.get('helium_remaining', 0.5)
        })

    async def train_forecast_model(self):
        if not self._ml_available:
            return {'status': 'ml_not_available'}
        if len(self.lifecycle_history) < 10:
            return {'status': 'insufficient_data', 'samples': len(self.lifecycle_history)}

        X, y = [], []
        history_list = list(self.lifecycle_history)
        for i in range(len(history_list) - 5):
            features = []
            for j in range(5):
                data = history_list[i + j]
                features.extend([
                    data['age_days'] / 1000,
                    data['utilization'],
                    data['maintenance_count'] / 10,
                    data['carbon_score'],
                    data['helium_remaining']
                ])
            X.append(features)
            y.append(history_list[i + 5]['age_days'])

        X = np.array(X)
        y = np.array(y)

        if self.scaler.mean_ is None:
            X_scaled = self.scaler.fit_transform(X)
        else:
            X_scaled = self.scaler.transform(X)

        # Incremental training
        for _ in range(3):
            self.model.partial_fit(X_scaled, y)
        self.is_trained = True

        # Compute R2 for diagnostics
        pred = self.model.predict(X_scaled)
        r2 = r2_score(y, pred) if len(y) > 5 else 0.0
        logger.info(f"Lifecycle model updated. R²={r2:.3f}")
        return {'status': 'success', 'r2': r2, 'samples': len(X)}

    async def predict_lifetime(self, component_data: Dict) -> Dict:
        if not self.is_trained or len(self.lifecycle_history) < 10:
            if len(self.lifecycle_history) > 0:
                recent = [h['age_days'] for h in list(self.lifecycle_history)[-5:]]
                pred = np.mean(recent) if recent else 365
                return {'predicted_days': pred, 'confidence': 0.3, 'trend': 'moving_average'}
            return {'predicted_days': 365, 'confidence': 0.0, 'trend': 'insufficient_data'}

        recent = list(self.lifecycle_history)[-5:]
        features = []
        for data in recent:
            features.extend([
                data['age_days'] / 1000,
                data['utilization'],
                data['maintenance_count'] / 10,
                data['carbon_score'],
                data['helium_remaining']
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
        confidence = min(0.9, 0.5 + 0.4 * (len(self.lifecycle_history) / 100))

        if len(self.forecast_history) > 5:
            recent_forecasts = list(self.forecast_history)[-5:]
            trend = "increasing" if prediction > recent_forecasts[-1] else "decreasing" if prediction < recent_forecasts[-1] else "stable"
        else:
            trend = "stable"

        self.forecast_history.append({'prediction': prediction, 'trend': trend})
        return {
            'predicted_days': max(0, prediction),
            'confidence': confidence,
            'trend': trend,
            'recommended_actions': self._generate_predictive_actions(prediction)
        }

    def _generate_predictive_actions(self, prediction: float) -> List[str]:
        actions = []
        if prediction < 100:
            actions.append("URGENT: Schedule component replacement")
            actions.append("Prioritize material recovery")
        elif prediction < 365:
            actions.append("Plan for repurposing")
            actions.append("Optimize utilization")
        elif prediction < 730:
            actions.append("Schedule preventive maintenance")
            actions.append("Monitor helium levels")
        else:
            actions.append("Component health is good - maintain current practices")
        return actions

# ============================================================================
# Federated Circular Manager (unchanged)
# ============================================================================

class FederatedCircularManager:
    """Federated reflexive learning with compression and retry."""

    def __init__(self, config: CircularComputingConfig):
        self.config = config
        self.server_url = config.server_url
        self.round = 0
        self.local_components = {}
        self.global_components = {}
        self.participants = []
        self.contribution_scores = {}
        self._lock = asyncio.Lock()
        self._session = None
        self.sparsity_ratio = config.federated_sparsity_ratio
        self.failure_count = 0
        self.circuit_open = False
        self.circuit_open_until: Optional[datetime] = None
        self._circuit = CircuitBreaker("federated_server", failure_threshold=config.circuit_breaker_failure_threshold, recovery_timeout=config.circuit_breaker_recovery_timeout)
        logger.info("FederatedCircularManager initialized")

    async def _get_session(self):
        if self._session is None and self.server_url:
            self._session = aiohttp.ClientSession()
        return self._session

    def _compress_component_data(self, data: Dict) -> Dict:
        if self.sparsity_ratio == 1.0:
            return data
        numeric_items = {k: v for k, v in data.items() if isinstance(v, (int, float))}
        if not numeric_items:
            return data
        sorted_items = sorted(numeric_items.items(), key=lambda x: abs(x[1]), reverse=True)
        k = max(1, int(len(sorted_items) * self.sparsity_ratio))
        kept_keys = {item[0] for item in sorted_items[:k]}
        compressed = {k: v for k, v in data.items() if k in kept_keys or not isinstance(v, (int, float))}
        return compressed

    async def send_local_components(self, participant_id: str, component_data: Dict, performance: float = 1.0) -> Dict:
        if not self.server_url:
            return {'status': 'local'}
        if self.circuit_open:
            if datetime.now(timezone.utc) < self.circuit_open_until:
                logger.warning("Circuit breaker open, skipping send")
                return {'status': 'circuit_open'}
            else:
                self.circuit_open = False
                self.failure_count = 0
        async def _send():
            for attempt in range(self.config.max_retries):
                try:
                    async with self._lock:
                        session = await self._get_session()
                        compressed = self._compress_component_data(component_data)
                        update_data = {
                            'participant_id': participant_id,
                            'round': self.round,
                            'component_data': compressed,
                            'performance': performance,
                            'sparsity_ratio': self.sparsity_ratio,
                            'timestamp': datetime.now(timezone.utc).isoformat()
                        }
                        async with session.post(
                            f"{self.server_url}/federated/circular",
                            json=update_data,
                            timeout=30
                        ) as response:
                            if response.status == 200:
                                result = await response.json()
                                self.round += 1
                                self.contribution_scores[participant_id] = performance
                                self.failure_count = 0
                                return result
                            else:
                                logger.warning(f"Federated send failed (attempt {attempt+1}): {response.status}")
                except Exception as e:
                    logger.error(f"Federated send error (attempt {attempt+1}): {e}")
                await asyncio.sleep(2 ** attempt)
            self.failure_count += 1
            if self.failure_count >= self.config.circuit_breaker_failure_threshold:
                self.circuit_open = True
                self.circuit_open_until = datetime.now(timezone.utc) + timedelta(seconds=self.config.circuit_breaker_recovery_timeout)
                logger.error("Circuit breaker opened for FederatedCircularManager")
            return {'status': 'failed'}
        return await self._circuit.call(_send)

    async def get_global_components(self) -> Optional[Dict]:
        if not self.server_url:
            return self.global_components
        async def _fetch():
            for attempt in range(self.config.max_retries):
                try:
                    async with self._lock:
                        session = await self._get_session()
                        async with session.get(
                            f"{self.server_url}/federated/circular/global",
                            timeout=30
                        ) as response:
                            if response.status == 200:
                                data = await response.json()
                                self.global_components = data.get('components', {})
                                self.participants = data.get('participants', [])
                                return self.global_components
                            else:
                                logger.warning(f"Global fetch failed (attempt {attempt+1}): {response.status}")
                except Exception as e:
                    logger.error(f"Global fetch error (attempt {attempt+1}): {e}")
                await asyncio.sleep(2 ** attempt)
            return None
        return await self._circuit.call(_fetch)

    def aggregate_components(self, peer_components: List[Dict], weights: Dict[str, float] = None) -> Dict:
        if not peer_components:
            return {}
        aggregated = {}
        if weights is None:
            weights = {i: 1.0 for i in range(len(peer_components))}
        for key in peer_components[0].keys():
            if isinstance(peer_components[0][key], (int, float)):
                total = 0.0
                total_weight = 0.0
                for i, peer in enumerate(peer_components):
                    if key in peer:
                        total += peer[key] * weights.get(i, 1.0)
                        total_weight += weights.get(i, 1.0)
                aggregated[key] = total / max(total_weight, 0.001)
        return aggregated

    def get_federated_stats(self) -> Dict:
        return {
            'round': self.round,
            'participants': len(self.participants),
            'has_global_components': bool(self.global_components),
            'contribution_scores': self.contribution_scores,
            'sparsity_ratio': self.sparsity_ratio,
            'circuit_open': self.circuit_open
        }

    async def close(self):
        if self._session:
            await self._session.close()

# ============================================================================
# ML Component Selector (unchanged)
# ============================================================================

class MLComponentSelector:
    """Machine learning-based component selection with incremental training and checkpointing."""

    def __init__(self, config: CircularComputingConfig):
        self.config = config
        self.input_size = config.ml_input_size
        self.hidden_size = config.ml_hidden_size
        self.model = None
        self.scaler = StandardScaler()
        self.is_trained = False
        self.optimizer = None
        self.criterion = nn.MSELoss()
        self.training_history: List[float] = []
        self._init_model()

    def _init_model(self):
        class ComponentSelector(nn.Module):
            def __init__(self, input_size, hidden_size):
                super().__init__()
                self.network = nn.Sequential(
                    nn.Linear(input_size, hidden_size),
                    nn.ReLU(),
                    nn.BatchNorm1d(hidden_size),
                    nn.Linear(hidden_size, hidden_size // 2),
                    nn.ReLU(),
                    nn.BatchNorm1d(hidden_size // 2),
                    nn.Linear(hidden_size // 2, 1)
                )
            def forward(self, x):
                return self.network(x)

        self.model = ComponentSelector(self.input_size, self.hidden_size)
        self.optimizer = optim.Adam(self.model.parameters(), lr=0.001)

    async def train_model(self, training_data: List[Dict], epochs: Optional[int] = None) -> Dict:
        if len(training_data) < 20:
            return {'status': 'insufficient_data', 'samples': len(training_data)}

        epochs = epochs or self.config.ml_epochs

        X = []
        y = []
        for item in training_data:
            X.append([
                item.get('age_days', 0) / 1000,
                item.get('utilization', 0.5),
                item.get('maintenance_count', 0) / 10,
                item.get('carbon_footprint', 0.5),
                item.get('helium_content', 0.5),
                item.get('recycling_potential', 0.5),
                item.get('reliability', 0.5),
                item.get('cost_efficiency', 0.5)
            ])
            y.append(item.get('selection_score', 0.5))

        X = np.array(X)
        y = np.array(y)

        if self.scaler.mean_ is None:
            X_scaled = self.scaler.fit_transform(X)
        else:
            X_scaled = self.scaler.transform(X)

        dataset = TensorDataset(
            torch.FloatTensor(X_scaled),
            torch.FloatTensor(y).unsqueeze(1)
        )
        dataloader = DataLoader(dataset, batch_size=self.config.ml_batch_size, shuffle=True)

        self.model.train()
        losses = []
        for epoch in range(epochs):
            epoch_loss = 0
            for batch_X, batch_y in dataloader:
                self.optimizer.zero_grad()
                output = self.model(batch_X)
                loss = self.criterion(output, batch_y)
                loss.backward()
                torch.nn.utils.clip_grad_norm_(self.model.parameters(), 1.0)
                self.optimizer.step()
                epoch_loss += loss.item()
            losses.append(epoch_loss / len(dataloader))
            if (epoch + 1) % 20 == 0:
                logger.debug(f"ML Training Epoch {epoch+1}/{epochs}, Loss: {epoch_loss/len(dataloader):.4f}")

        self.is_trained = True
        self.training_history.extend(losses)
        if len(self.training_history) > 1000:
            self.training_history = self.training_history[-1000:]

        return {'status': 'success', 'loss': np.mean(losses), 'samples': len(X)}

    async def select_component_ml(self, requirements: Dict) -> Dict[str, Any]:
        if not self.is_trained:
            return {'score': 0.5, 'confidence': 0.0, 'status': 'model_not_trained'}

        features = np.array([[
            requirements.get('age_days', 0) / 1000,
            requirements.get('utilization', 0.5),
            requirements.get('maintenance_count', 0) / 10,
            requirements.get('carbon_footprint', 0.5),
            requirements.get('helium_content', 0.5),
            requirements.get('recycling_potential', 0.5),
            requirements.get('reliability', 0.5),
            requirements.get('cost_efficiency', 0.5)
        ]])
        features_scaled = self.scaler.transform(features)

        self.model.eval()
        with torch.no_grad():
            output = self.model(torch.FloatTensor(features_scaled)).numpy()[0, 0]

        return {
            'score': float(output),
            'confidence': 0.8 if self.is_trained else 0.0,
            'status': 'success'
        }

    def get_model_checkpoint(self) -> Dict:
        return {
            'state_dict': self.model.state_dict(),
            'optimizer_state': self.optimizer.state_dict(),
            'scaler_mean': self.scaler.mean_.tolist() if self.scaler.mean_ is not None else None,
            'scaler_std': self.scaler.scale_.tolist() if self.scaler.scale_ is not None else None,
            'is_trained': self.is_trained,
            'training_history': self.training_history
        }

    def load_checkpoint(self, checkpoint: Dict):
        self.model.load_state_dict(checkpoint['state_dict'])
        self.optimizer.load_state_dict(checkpoint['optimizer_state'])
        if checkpoint.get('scaler_mean') is not None:
            self.scaler.mean_ = np.array(checkpoint['scaler_mean'])
            self.scaler.scale_ = np.array(checkpoint['scaler_std'])
        self.is_trained = checkpoint.get('is_trained', False)
        self.training_history = checkpoint.get('training_history', [])

# ============================================================================
# Human-AI Collaborative Circular (unchanged)
# ============================================================================

class HumanAICollaborativeCircular:
    """Human-AI collaborative reflection for circular computing."""

    def __init__(self):
        self.feedback_history = deque(maxlen=1000)
        self.reflection_logs = deque(maxlen=100)
        self.user_preferences = {}
        self._lock = asyncio.Lock()

    def collect_feedback(self, user_id: str, feedback: Dict) -> Dict:
        feedback_entry = {'user_id': user_id, 'timestamp': datetime.now(timezone.utc), 'feedback': feedback}
        self.feedback_history.append(feedback_entry)
        if 'preference' in feedback:
            self.user_preferences[user_id] = feedback['preference']
        reflection = self._generate_reflection(feedback)
        self.reflection_logs.append(reflection)
        return reflection

    def _generate_reflection(self, feedback: Dict) -> Dict:
        reflection = {
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'acknowledgment': f"Feedback received on {feedback.get('topic', 'circular computing')}",
            'insights': [],
            'actions': [],
            'circular_insights': []
        }
        if 'concern' in feedback:
            if feedback['concern'] == 'recycling':
                reflection['insights'].append("Recycling efficiency can be improved through material sorting")
                reflection['actions'].append("Implement automated material recovery")
            elif feedback['concern'] == 'helium':
                reflection['insights'].append("Helium recovery requires specialized handling")
                reflection['actions'].append("Implement helium capture systems")
            elif feedback['concern'] == 'lifecycle':
                reflection['insights'].append("Lifecycle extension reduces carbon footprint")
                reflection['actions'].append("Implement predictive maintenance")
            elif feedback['concern'] == 'carbon':
                reflection['circular_insights'].append("Carbon-aware hardware selection is critical")
                reflection['actions'].append("Integrate carbon intensity tracking")
        if 'suggestion' in feedback:
            reflection['actions'].append(f"Implementing suggestion: {feedback['suggestion']}")
        reflection['action_items'] = self._prioritize_actions(reflection['actions'])
        return reflection

    def _prioritize_actions(self, actions: List[str]) -> List[Dict]:
        priorities = []
        for action in actions:
            if any(keyword in action.lower() for keyword in ['urgent', 'critical']):
                priority = 'high'
                impact = 0.9
            elif any(keyword in action.lower() for keyword in ['recycling', 'circular']):
                priority = 'high'
                impact = 0.8
            elif any(keyword in action.lower() for keyword in ['helium']):
                priority = 'medium'
                impact = 0.6
            else:
                priority = 'medium'
                impact = 0.5
            priorities.append({
                'action': action,
                'priority': priority,
                'impact': impact,
                'estimated_effort': 'medium'
            })
        return sorted(priorities, key=lambda x: x['impact'], reverse=True)

    def get_collaborative_insights(self) -> Dict:
        if len(self.feedback_history) < 5:
            return {'status': 'insufficient_feedback'}
        recent_feedback = list(self.feedback_history)[-20:]
        topics = {}
        for f in recent_feedback:
            topic = f['feedback'].get('topic', 'general')
            topics[topic] = topics.get(topic, 0) + 1
        most_common = max(topics.items(), key=lambda x: x[1]) if topics else ('none', 0)
        return {
            'total_feedback': len(self.feedback_history),
            'top_topics': topics,
            'most_common_topic': most_common[0],
            'engagement_score': min(1.0, len(self.feedback_history) / 100),
            'user_count': len(set(f['user_id'] for f in self.feedback_history))
        }

# ============================================================================
# Enums and Data Classes (unchanged)
# ============================================================================

class HardwareState(Enum):
    MANUFACTURING = "manufacturing"
    DEPLOYED = "deployed"
    MAINTENANCE = "maintenance"
    DEGRADED = "degraded"
    REPURPOSED = "repurposed"
    RECYCLED = "recycled"
    DECOMMISSIONED = "decommissioned"
    HELIUM_RECOVERED = "helium_recovered"

class MaterialType(Enum):
    SILICON = "silicon"
    COPPER = "copper"
    GOLD = "gold"
    ALUMINUM = "aluminum"
    PLASTIC = "plastic"
    RARE_EARTH = "rare_earth"
    HELIUM = "helium"

@dataclass
class HardwareComponent:
    component_id: str
    type: str
    materials: Dict[MaterialType, float]
    manufacturing_carbon: float
    current_state: HardwareState
    deployment_date: datetime
    expected_lifetime_days: int
    utilization_history: List[float] = field(default_factory=list)
    maintenance_log: List[Dict] = field(default_factory=list)
    sustainability_score: float = 0.0
    helium_content_l: float = 0.0
    carbon_savings_kg: float = 0.0

# ============================================================================
# Persistence Manager (unchanged)
# ============================================================================

class CircularComputingPersistenceManager:
    """Manages persistence of circular computing state, ML model, and helium data."""

    def __init__(self, config: CircularComputingConfig):
        self.config = config
        self.path = config.persistence_path
        self._lock = asyncio.Lock()
        logger.info(f"CircularComputingPersistenceManager initialized (path={self.path})")

    async def save_state(self, manager: 'CircularComputingManager') -> bool:
        async with self._lock:
            try:
                state = {
                    'config': manager.config,
                    'components': manager.components,
                    'material_inventory': manager.material_inventory,
                    'recycling_history': manager.recycling_history,
                    'circularity_score': manager.circularity_score,
                    'waste_diversion_rate': manager.waste_diversion_rate,
                    'material_recovery_rate': manager.material_recovery_rate,
                    'sustainability_score': manager.sustainability_score,
                    'helium_manager_state': {
                        'helium_usage': list(manager.helium_manager.helium_usage),
                        'helium_recovered': list(manager.helium_manager.helium_recovered),
                        'component_helium': manager.helium_manager.component_helium,
                        '_running_total_usage': manager.helium_manager._running_total_usage,
                        '_running_total_recovered': manager.helium_manager._running_total_recovered,
                    } if manager.helium_manager else None,
                    'ml_checkpoint': manager.ml_selector.get_model_checkpoint() if manager.ml_selector else None,
                }
                serialized = pickle.dumps(state)
                compressed = zlib.compress(serialized)
                with open(self.path, 'wb') as f:
                    f.write(compressed)
                logger.info(f"Circular state saved to {self.path}")
                return True
            except Exception as e:
                logger.error(f"Failed to save state: {e}")
                return False

    async def load_state(self, manager: 'CircularComputingManager') -> bool:
        async with self._lock:
            if not os.path.exists(self.path):
                logger.warning(f"Persistence file {self.path} not found")
                return False
            try:
                with open(self.path, 'rb') as f:
                    compressed = f.read()
                serialized = zlib.decompress(compressed)
                state = pickle.loads(serialized)

                manager.components = state.get('components', {})
                manager.material_inventory = state.get('material_inventory', {})
                manager.recycling_history = state.get('recycling_history', [])
                manager.circularity_score = state.get('circularity_score', 0.0)
                manager.waste_diversion_rate = state.get('waste_diversion_rate', 0.0)
                manager.material_recovery_rate = state.get('material_recovery_rate', 0.0)
                manager.sustainability_score = state.get('sustainability_score', 0.0)

                # Restore helium manager
                he_state = state.get('helium_manager_state')
                if he_state and manager.helium_manager:
                    manager.helium_manager.helium_usage = deque(he_state.get('helium_usage', []), maxlen=86400)
                    manager.helium_manager.helium_recovered = deque(he_state.get('helium_recovered', []), maxlen=86400)
                    manager.helium_manager.component_helium = he_state.get('component_helium', {})
                    manager.helium_manager._running_total_usage = he_state.get('_running_total_usage', 0.0)
                    manager.helium_manager._running_total_recovered = he_state.get('_running_total_recovered', 0.0)

                # Restore ML checkpoint
                ml_checkpoint = state.get('ml_checkpoint')
                if ml_checkpoint and manager.ml_selector:
                    manager.ml_selector.load_checkpoint(ml_checkpoint)

                logger.info(f"Circular state loaded from {self.path}")
                return True
            except Exception as e:
                logger.error(f"Failed to load state: {e}")
                return False

    async def delete_state(self):
        async with self._lock:
            if os.path.exists(self.path):
                os.remove(self.path)
                logger.info(f"Persistence file {self.path} deleted")
                return True
            return False

# ============================================================================
# Telemetry Collector (unchanged)
# ============================================================================

class CircularComputingTelemetry:
    """Collects telemetry for the circular computing system."""

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
# Enhanced Circular Computing Manager (Main Class) – v3.0.0
# ============================================================================

class CircularComputingManager:
    """
    Enhanced Circular Computing Manager v3.0.0 - Complete Green Agent Implementation
    with full bio‑inspired core integration.
    """

    def __init__(
        self,
        bio_core: Optional[EnhancedBioInspiredCore] = None,
        config: Optional[CircularComputingConfig] = None,
        **kwargs
    ):
        """
        Initialize the circular computing manager.

        Args:
            bio_core: Reference to the bio‑inspired core for event subscriptions.
            config: Configuration dataclass (preferred).
            **kwargs: Legacy arguments for backward compatibility.
        """
        if config is None:
            config = CircularComputingConfig(
                enable_federated=kwargs.get('enable_federated', True),
                enable_carbon_intensity=kwargs.get('enable_carbon_intensity', True),
                enable_predictive=kwargs.get('enable_predictive', True),
                enable_ml_selection=kwargs.get('enable_ml_selection', True),
                enable_human_ai=kwargs.get('enable_human_ai', True),
                enable_helium_tracking=kwargs.get('enable_helium_tracking', True),
                enable_persistence=kwargs.get('enable_persistence', True),
                enable_telemetry=kwargs.get('enable_telemetry', True),
                enable_event_driven=kwargs.get('enable_event_driven', True),
                enable_self_healing=kwargs.get('enable_self_healing', True),
                enable_swarm_coordination=kwargs.get('enable_swarm_coordination', True),
                enable_time_tick_engine=kwargs.get('enable_time_tick_engine', True),
                enable_quantum_bridge=kwargs.get('enable_quantum_bridge', True),
                enable_cost_benefit=kwargs.get('enable_cost_benefit', True),
                max_retries=kwargs.get('max_retries', 3),
                retry_base_delay_ms=kwargs.get('retry_base_delay_ms', 100.0),
                retry_max_delay_ms=kwargs.get('retry_max_delay_ms', 5000.0),
                circuit_breaker_failure_threshold=kwargs.get('circuit_breaker_failure_threshold', 5),
                circuit_breaker_recovery_timeout=kwargs.get('circuit_breaker_recovery_timeout', 30.0),
                persistence_path=kwargs.get('persistence_path', 'circular_computing_state.pkl')
            )
        self.config = config

        # Feature flags
        self.enable_federated = self.config.enable_federated
        self.enable_carbon_intensity = self.config.enable_carbon_intensity
        self.enable_predictive = self.config.enable_predictive
        self.enable_ml_selection = self.config.enable_ml_selection
        self.enable_human_ai = self.config.enable_human_ai
        self.enable_helium_tracking = self.config.enable_helium_tracking
        self.enable_persistence = self.config.enable_persistence
        self.enable_telemetry = self.config.enable_telemetry
        self.enable_event_driven = self.config.enable_event_driven
        self.enable_self_healing = self.config.enable_self_healing
        self.enable_swarm_coordination = self.config.enable_swarm_coordination
        self.enable_time_tick_engine = self.config.enable_time_tick_engine
        self.enable_quantum_bridge = self.config.enable_quantum_bridge
        self.enable_cost_benefit = self.config.enable_cost_benefit

        # Store bio‑core reference
        self.bio_core = bio_core
        self.event_broker = None
        self.alert_system = None
        self.anomaly_detection = None
        self.cost_benefit_engine = None
        self.quantum_bridge = None
        self.tick_engine = None
        self.swarm_coordinator = None
        self.self_healer = None
        self.workflow_orchestrator = None
        self.token_manager = None
        self.gradient_manager = None
        self.scheduler = None
        self.compartment_manager = None
        self.biomass_storage = None
        self.harvester = None

        # Extract core sub‑modules if available
        if self.bio_core:
            self.event_broker = getattr(self.bio_core, 'event_broker', None)
            self.alert_system = getattr(self.bio_core, 'alert_system', None)
            self.anomaly_detection = getattr(self.bio_core, 'anomaly_detection', None)
            self.cost_benefit_engine = getattr(self.bio_core, 'cost_benefit_engine', None)
            self.quantum_bridge = getattr(self.bio_core, 'quantum_bridge', None)
            self.tick_engine = getattr(self.bio_core, 'tick_engine', None)
            self.swarm_coordinator = getattr(self.bio_core, 'swarm_coordinator', None)
            self.self_healer = getattr(self.bio_core, 'self_healer', None)
            self.workflow_orchestrator = getattr(self.bio_core, 'workflow_orchestrator', None)
            self.token_manager = getattr(self.bio_core, 'token_manager', None)
            self.gradient_manager = getattr(self.bio_core, 'gradient_manager', None)
            self.scheduler = getattr(self.bio_core, 'scheduler', None)
            self.compartment_manager = getattr(self.bio_core, 'compartment_manager', None)
            self.biomass_storage = getattr(self.bio_core, 'biomass_storage', None)
            self.harvester = getattr(self.bio_core, 'harvester', None)

        # MoE and Self-Evolving Gate references (injected)
        self.expert_router = None
        self.gating_network = None
        self.self_evolving_gate = None

        # Helium provider (injected)
        self.helium_provider = None

        # Initialize sub-modules with config
        self.carbon_manager = CarbonIntensityManager(self.config) if self.enable_carbon_intensity else None
        self.helium_manager = HeliumLifecycleManager(self.config) if self.enable_helium_tracking else None
        self.predictive_analyzer = PredictiveLifecycleAnalyzer(self.config) if self.enable_predictive else None
        self.federated_manager = FederatedCircularManager(self.config) if self.enable_federated else None
        self.ml_selector = MLComponentSelector(self.config) if self.enable_ml_selection else None
        self.human_ai = HumanAICollaborativeCircular() if self.enable_human_ai else None

        # Persistence and telemetry
        self.persistence = CircularComputingPersistenceManager(self.config) if self.enable_persistence else None
        self.telemetry = CircularComputingTelemetry() if self.enable_telemetry else None

        # Circuit breakers for external services
        self._carbon_circuit = CircuitBreaker("carbon_api")
        self._federated_circuit = CircuitBreaker("federated_api")
        self._ml_circuit = CircuitBreaker("ml_selector")

        # Core tracking
        self.components: Dict[str, HardwareComponent] = {}
        self.material_inventory: Dict[MaterialType, float] = {}
        self.recycling_history: List[Dict] = []

        # Circular economy metrics
        self.circularity_score = 0.0
        self.waste_diversion_rate = 0.0
        self.material_recovery_rate = 0.0
        self.sustainability_score = 0.0
        self.health_status = "healthy"
        self.last_error = None

        # Initialize material inventory
        self._initialize_inventory()

        # Subscribe to core events if enabled
        if self.enable_event_driven and self.event_broker:
            self._subscribe_events()

        # Start background tasks
        self._start_background_tasks()

        # Load state if persistence enabled
        if self.enable_persistence and self.persistence:
            asyncio.create_task(self._load_state())

        logger.info(
            f"Enhanced Circular Computing Manager v3.0.0 initialized: "
            f"helium_budget={self.config.helium_budget_l}L, "
            f"federated={self.enable_federated}, ml={self.enable_ml_selection}, "
            f"event_driven={self.enable_event_driven}, self_healing={self.enable_self_healing}"
        )

    # ========================================================================
    # Event Subscriptions
    # ========================================================================
    def _subscribe_events(self):
        if self.event_broker:
            self.event_broker.subscribe('carbon_update', self._on_carbon_update)
            self.event_broker.subscribe('helium_update', self._on_helium_update)
            self.event_broker.subscribe('alert_generated', self._on_alert_generated)
            self.event_broker.subscribe('config_updated', self._on_config_updated)
            self.event_broker.subscribe('token_balance_update', self._on_token_update)
            self.event_broker.subscribe('health_update', self._on_health_update)
            self.event_broker.subscribe('anomaly_detected', self._on_anomaly_detected)
            logger.info("Circular Computing Manager subscribed to core events")

    async def _on_carbon_update(self, event: BioEvent):
        intensity = event.data.get('intensity', 400)
        price = event.data.get('price', 50.0)
        self.carbon_intensity = intensity
        self.carbon_price = price
        # Update predictive analyzer
        self.predictive_analyzer.update_history({
            'age_days': 0,
            'utilization': 0.5,
            'maintenance_count': 0,
            'carbon_score': 1.0 / (1.0 + price / 50),
            'helium_remaining': 0.5
        })
        # Adjust recycling priorities based on carbon intensity
        if intensity > 500:
            self.carbon_recycling_priority = 0.8

    async def _on_helium_update(self, event: BioEvent):
        scarcity = event.data.get('scarcity', 0.5)
        price = event.data.get('price', 0.5)
        self.helium_scarcity = scarcity
        self.helium_price = price
        if self.helium_manager:
            self.helium_manager.helium_budget_l = 100.0 * (1.0 - scarcity * 0.3)
            self.helium_manager.helium_to_co2_factor = self.config.helium_to_co2_factor * (1.0 + 0.1 * scarcity)

    async def _on_alert_generated(self, event: BioEvent):
        if event.data.get('severity') == 'critical':
            logger.warning("Critical alert received; switching to conservative circular strategy and triggering healing")
            self.circularity_strategy = 'conservative'
            if self.enable_self_healing and self.self_healer:
                await self.self_healer.apply_healing('damage_accumulation')
            if self.workflow_orchestrator and self.config.workflow_on_critical_alert:
                await self.workflow_orchestrator.execute_workflow(self.config.workflow_on_critical_alert)

    async def _on_config_updated(self, event: BioEvent):
        updates = event.data.get('updates', {})
        if 'circular_computing' in updates:
            new_config = updates['circular_computing']
            for key, value in new_config.items():
                if hasattr(self.config, key):
                    setattr(self.config, key, value)
            logger.info("Circular Computing configuration reloaded")

    async def _on_token_update(self, event: BioEvent):
        self.token_balance = event.data.get('balance', 500)

    async def _on_health_update(self, event: BioEvent):
        self.health_status = event.data.get('status', 'healthy')

    async def _on_anomaly_detected(self, event: BioEvent):
        if event.data.get('metric') == 'carbon_intensity':
            logger.info("Carbon anomaly detected; adjusting recycling priorities")
            self.carbon_recycling_priority = 0.9
        if event.data.get('metric') == 'helium_scarcity':
            logger.info("Helium anomaly detected; adjusting helium recovery targets")
            self.helium_recovery_target = 0.95

    # ========================================================================
    # Background Tasks (unchanged, but with event-driven updates)
    # ========================================================================
    def _start_background_tasks(self):
        if self.enable_carbon_intensity and self.carbon_manager:
            asyncio.create_task(self._carbon_update_loop())
        if self.enable_predictive and self.predictive_analyzer:
            asyncio.create_task(self._predictive_update_loop())
        if self.enable_federated and self.federated_manager:
            asyncio.create_task(self._federated_sync_loop())
        if self.enable_telemetry and self.telemetry:
            asyncio.create_task(self._telemetry_export_loop())
        if self.enable_swarm_coordination and self.swarm_coordinator:
            asyncio.create_task(self._swarm_update_loop())
        if self.enable_persistence:
            asyncio.create_task(self._persistence_save_loop())

    async def _carbon_update_loop(self):
        while True:
            try:
                await self.carbon_manager.update_carbon_intensity()
                if self.telemetry:
                    intensity = await self.carbon_manager.get_current_intensity()
                    self.telemetry.gauge('carbon_intensity', intensity)
                await asyncio.sleep(self.carbon_manager.update_interval if self.carbon_manager else 300)
            except Exception as e:
                logger.error(f"Carbon update error: {e}")
                await asyncio.sleep(60)

    async def _predictive_update_loop(self):
        while True:
            try:
                if self.predictive_analyzer and self.components:
                    for component in list(self.components.values())[-5:]:
                        age_days = (datetime.now(timezone.utc) - component.deployment_date).days
                        util = np.mean(component.utilization_history[-50:]) if component.utilization_history else 0.5
                        maint = len(component.maintenance_log)
                        carbon_score = 1.0 / (1.0 + component.manufacturing_carbon)
                        helium_remaining = component.helium_content_l if self.enable_helium_tracking else 0.5
                        self.predictive_analyzer.update_history({
                            'age_days': age_days,
                            'utilization': util,
                            'maintenance_count': maint,
                            'carbon_score': carbon_score,
                            'helium_remaining': helium_remaining
                        })
                    await self.predictive_analyzer.train_forecast_model()
                await asyncio.sleep(300)
            except Exception as e:
                logger.error(f"Predictive update error: {e}")
                await asyncio.sleep(60)

    async def _federated_sync_loop(self):
        while True:
            try:
                if self.federated_manager:
                    participant_id = f"circular_{hashlib.md5(str(self.components.keys()).encode()).hexdigest()[:8]}"
                    await self.federated_manager.send_local_components(
                        participant_id,
                        {
                            'total_components': len(self.components),
                            'circularity_score': self.circularity_score,
                            'waste_diversion_rate': self.waste_diversion_rate,
                            'sustainability_score': self.sustainability_score,
                            'timestamp': datetime.now(timezone.utc).isoformat()
                        },
                        performance=self.sustainability_score
                    )
                    await self.federated_manager.get_global_components()
                await asyncio.sleep(3600)
            except Exception as e:
                logger.error(f"Federated sync error: {e}")
                await asyncio.sleep(300)

    async def _telemetry_export_loop(self):
        while True:
            try:
                if self.enable_telemetry and self.telemetry:
                    export_data = await self.telemetry.export()
                    logger.debug(f"Telemetry export: {len(export_data)} bytes")
                await asyncio.sleep(self.config.telemetry_export_interval)
            except Exception as e:
                logger.error(f"Telemetry export error: {e}")
                await asyncio.sleep(60)

    async def _swarm_update_loop(self):
        while True:
            try:
                await self.share_with_swarm()
                await asyncio.sleep(self.config.swarm_share_interval)
            except Exception as e:
                logger.error(f"Swarm update error: {e}")
                await asyncio.sleep(120)

    async def _persistence_save_loop(self):
        while True:
            try:
                await self.save_state()
                await asyncio.sleep(300)  # every 5 minutes
            except Exception as e:
                logger.error(f"Persistence save error: {e}")
                await asyncio.sleep(60)

    # ========================================================================
    # Swarm Coordination
    # ========================================================================
    async def share_with_swarm(self):
        if not self.enable_swarm_coordination or not self.swarm_coordinator:
            return
        swarm_payload = {
            'manager_id': hashlib.md5(str(self.components.keys()).encode()).hexdigest()[:8],
            'sustainability_score': self.sustainability_score,
            'circularity_score': self.circularity_score,
            'total_components': len(self.components),
            'material_recovery_rate': self.material_recovery_rate,
            'helium_position': self.helium_manager.get_helium_position() if self.helium_manager else {}
        }
        await self.swarm_coordinator.share_predictions(swarm_payload)

    # ========================================================================
    # Deep MoE and Self-Evolving Gate Integration
    # ========================================================================
    def set_gating_network(self, gating_network: 'GatingNetworkManager'):
        self.gating_network = gating_network
        logger.info("Gating network injected into Circular Computing")

    def set_self_evolving_gate(self, gate: 'EnhancedSelfEvolvingGate'):
        self.self_evolving_gate = gate
        logger.info("Self-Evolving Gate injected into Circular Computing")

    def set_expert_router(self, router: 'ExpertRouter'):
        self.expert_router = router
        logger.info("Expert Router injected into Circular Computing")

    def set_helium_provider(self, provider: HeliumProvider):
        self.helium_provider = provider
        logger.info("Helium provider injected into Circular Computing")

    # ========================================================================
    # Bio-Inspired Module Injection
    # ========================================================================
    def inject_bio_core(self, bio_core: Any = None, **kwargs):
        if bio_core:
            self.token_manager = getattr(bio_core, 'token_manager', None)
            self.gradient_manager = getattr(bio_core, 'gradient_manager', None)
            self.scheduler = getattr(bio_core, 'scheduler', None)
            self.compartment_manager = getattr(bio_core, 'compartment_manager', None)
            self.biomass_storage = getattr(bio_core, 'biomass_storage', None)
            self.harvester = getattr(bio_core, 'harvester', None)
        else:
            self.token_manager = kwargs.get('token_manager')
            self.gradient_manager = kwargs.get('gradient_manager')
            self.scheduler = kwargs.get('scheduler')
            self.compartment_manager = kwargs.get('compartment_manager')
            self.biomass_storage = kwargs.get('biomass_storage')
            self.harvester = kwargs.get('harvester')
        logger.info("Bio-inspired modules injected into Circular Computing")

    # ========================================================================
    # Component Registration (unchanged)
    # ========================================================================
    def register_component(
        self,
        component_type: str,
        materials: Dict[MaterialType, float],
        manufacturing_carbon: float,
        expected_lifetime_days: int = 1825,
        helium_content_l: float = 0.0
    ) -> str:
        component_id = f"COMP-{datetime.now(timezone.utc).timestamp()}-{component_type}"
        component = HardwareComponent(
            component_id=component_id,
            type=component_type,
            materials=materials,
            manufacturing_carbon=manufacturing_carbon,
            current_state=HardwareState.MANUFACTURING,
            deployment_date=datetime.now(timezone.utc),
            expected_lifetime_days=expected_lifetime_days,
            helium_content_l=helium_content_l,
            sustainability_score=0.5
        )
        self.components[component_id] = component
        for material, amount in materials.items():
            self.material_inventory[material] += amount
        if self.enable_helium_tracking and self.helium_manager and helium_content_l > 0:
            self.helium_manager.register_component_helium(component_id, helium_content_l, component_type)
        logger.info(f"Registered component {component_id}: {component_type}")
        return component_id

    def _initialize_inventory(self):
        for material in MaterialType:
            self.material_inventory[material] = 0.0

    # ========================================================================
    # Enhanced Recycling (with QuantumBridge, TimeTickEngine, CostBenefit)
    # ========================================================================

    async def recycle_component(
        self,
        component_id: str,
        use_ml_optimization: bool = False
    ) -> Dict[str, Any]:
        if component_id not in self.components:
            return {'error': 'Component not found'}
        component = self.components[component_id]

        # Use QuantumBridge to adjust recycling weights if available
        if self.enable_quantum_bridge and self.quantum_bridge:
            q_params = self.quantum_bridge.get_qubo_parameters()
            penalty_carbon = q_params.get('penalty_carbon', 0.5)
            penalty_helium = q_params.get('penalty_helium_shortage', 0.5)
            if penalty_carbon > 0.7:
                # Increase carbon savings weight
                self.carbon_savings_weight = 0.8
            if penalty_helium > 0.7:
                # Increase helium recovery weight
                self.helium_recovery_weight = 0.8

        # Use TimeTickEngine for helium forecast if available
        if self.enable_time_tick_engine and self.tick_engine:
            forecast = self.tick_engine.get_helium_forecast(4)
            if forecast and len(forecast) > 3:
                avg_future_helium = np.mean(forecast)
                if avg_future_helium < 0.3:
                    # Helium scarcity predicted, prioritize helium recovery
                    self.helium_recovery_priority = 0.9

        # Use CostBenefitEngine to evaluate recycling cost if available
        if self.enable_cost_benefit and self.cost_benefit_engine:
            params = {
                'recycling_cost': component.manufacturing_carbon * 0.2,
                'carbon_saved': component.manufacturing_carbon * 0.8,
                'helium_recovered': self.helium_manager.calculate_helium_recovery(component_id) if self.helium_manager else 0
            }
            analysis = await self.cost_benefit_engine.analyze_scenario('component_recycling', params)
            result['cost_benefit_analysis'] = {
                'roi': analysis.roi,
                'net_value': analysis.net_value
            }

        ml_optimization = None
        if self.enable_ml_selection and use_ml_optimization:
            ml_result = await self.ml_selector.select_component_ml({
                'age_days': (datetime.now(timezone.utc) - component.deployment_date).days,
                'utilization': np.mean(component.utilization_history[-50:]) if component.utilization_history else 0.5,
                'maintenance_count': len(component.maintenance_log),
                'carbon_footprint': component.manufacturing_carbon,
                'helium_content': component.helium_content_l,
                'recycling_potential': 0.8,
                'reliability': 0.9,
                'cost_efficiency': 0.7
            })
            ml_optimization = ml_result

        recovered_materials = {}
        total_recovery_rate = 0.0
        recovery_rates = {
            MaterialType.SILICON: 0.95,
            MaterialType.COPPER: 0.98,
            MaterialType.GOLD: 0.99,
            MaterialType.ALUMINUM: 0.95,
            MaterialType.PLASTIC: 0.80,
            MaterialType.RARE_EARTH: 0.90,
            MaterialType.HELIUM: 0.85
        }
        for material, amount in component.materials.items():
            recovery_rate = recovery_rates.get(material, 0.9)
            recovered_amount = amount * recovery_rate
            recovered_materials[material.value] = {
                'original_g': amount,
                'recovered_g': recovered_amount,
                'recovery_rate': recovery_rate
            }
            self.material_inventory[material] -= amount
            self.material_inventory[material] += recovered_amount
            total_recovery_rate += recovery_rate
        avg_recovery_rate = total_recovery_rate / len(recovered_materials) if recovered_materials else 0

        manufacturing_carbon = component.manufacturing_carbon
        recycling_carbon = manufacturing_carbon * 0.2
        carbon_saved = manufacturing_carbon - recycling_carbon
        component.carbon_savings_kg = carbon_saved

        helium_recovered = 0.0
        if self.enable_helium_tracking and self.helium_manager:
            helium_recovered = self.helium_manager.calculate_helium_recovery(component_id)
            if helium_recovered > 0:
                self.helium_manager.record_helium_recovery(component_id, helium_recovered)
                recovered_materials['helium_recovered'] = {
                    'original_g': component.helium_content_l * 1000,
                    'recovered_g': helium_recovered * 1000,
                    'recovery_rate': 0.85
                }

        component.current_state = HardwareState.RECYCLED
        self.sustainability_score = self._calculate_sustainability_score(avg_recovery_rate, carbon_saved, helium_recovered)

        recycling_record = {
            'component_id': component_id,
            'component_type': component.type,
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'materials_recovered': recovered_materials,
            'average_recovery_rate': avg_recovery_rate,
            'carbon_saved_kg': carbon_saved,
            'helium_recovered_g': helium_recovered * 1000,
            'ml_optimization': ml_optimization,
            'sustainability_score': self.sustainability_score
        }
        self.recycling_history.append(recycling_record)
        self._update_circularity_metrics()

        if self.predictive_analyzer:
            self.predictive_analyzer.update_history({
                'age_days': (datetime.now(timezone.utc) - component.deployment_date).days,
                'utilization': np.mean(component.utilization_history[-50:]) if component.utilization_history else 0.5,
                'maintenance_count': len(component.maintenance_log),
                'carbon_score': 1.0 / (1.0 + component.manufacturing_carbon),
                'helium_remaining': component.helium_content_l
            })
            await self.predictive_analyzer.train_forecast_model()

        # Pass recycling result to gating network if available
        if self.gating_network and self.expert_router:
            features = np.array([
                avg_recovery_rate,
                carbon_saved / 10,
                helium_recovered / 10,
                self.sustainability_score
            ])
            reward = self.sustainability_score
            context = {
                'component_id': component_id,
                'carbon_saved': carbon_saved,
                'helium_recovered': helium_recovered
            }
            self.gating_network.update(features, reward, context)

        # Pass to self-evolving gate if available
        if self.self_evolving_gate:
            self.self_evolving_gate.adapt(
                state=torch.tensor([carbon_saved, helium_recovered]),
                chosen_expert=0,  # dummy
                reward=self.sustainability_score,
                environmental_feedback={'component_id': component_id},
                quantum_mode=False
            )

        if self.enable_human_ai and self.human_ai:
            recycling_record['human_ai_insights'] = self.human_ai.get_collaborative_insights()

        if self.telemetry:
            self.telemetry.increment('recycles_performed')
            self.telemetry.gauge('carbon_saved', carbon_saved)
            self.telemetry.gauge('sustainability_score', self.sustainability_score)

        # Trigger workflow if sustainability score is low
        if self.sustainability_score < 0.4 and self.workflow_orchestrator:
            await self.workflow_orchestrator.execute_workflow(self.config.workflow_on_slo_breach)

        logger.info(f"Recycled component {component_id}: {avg_recovery_rate:.1%} recovery, {carbon_saved:.2f} kg CO2 saved")
        return recycling_record

    def _calculate_sustainability_score(self, recovery_rate: float, carbon_saved: float, helium_recovered: float) -> float:
        recovery_factor = recovery_rate
        carbon_factor = min(1.0, carbon_saved / 10)
        helium_factor = min(1.0, helium_recovered / 10)
        score = (recovery_factor * 0.4 + carbon_factor * 0.3 + helium_factor * 0.3)
        return min(1.0, max(0.0, score))

    def _update_circularity_metrics(self):
        total_components = len(self.components)
        if total_components == 0:
            return
        recycled = sum(1 for c in self.components.values() if c.current_state == HardwareState.RECYCLED)
        repurposed = sum(1 for c in self.components.values() if c.current_state == HardwareState.REPURPOSED)
        self.circularity_score = (recycled + repurposed) / total_components
        total_recovered = sum(r['average_recovery_rate'] for r in self.recycling_history)
        self.material_recovery_rate = total_recovered / max(len(self.recycling_history), 1)
        self.waste_diversion_rate = (recycled + repurposed) / max(total_components, 1)

    # ========================================================================
    # Component State Methods (unchanged)
    # ========================================================================
    def deploy_component(self, component_id: str):
        if component_id in self.components:
            self.components[component_id].current_state = HardwareState.DEPLOYED
            logger.info(f"Deployed component {component_id}")

    def record_utilization(self, component_id: str, utilization_rate: float):
        if component_id in self.components:
            component = self.components[component_id]
            component.utilization_history.append(utilization_rate)
            if len(component.utilization_history) > 100:
                recent_util = np.mean(component.utilization_history[-100:])
                if recent_util < 0.3:
                    self._suggest_repurposing(component)
                elif recent_util > 0.9:
                    self._suggest_maintenance(component)

    def _suggest_repurposing(self, component: HardwareComponent):
        logger.info(f"Suggesting repurposing for {component.component_id}: utilization below threshold")
        new_manufacturing_carbon = component.manufacturing_carbon
        repurposing_carbon = component.manufacturing_carbon * 0.1
        carbon_saved = new_manufacturing_carbon - repurposing_carbon
        if carbon_saved > 0:
            logger.info(f"Repurposing would save {carbon_saved:.2f} kg CO2")

    def _suggest_maintenance(self, component: HardwareComponent):
        logger.info(f"Suggesting maintenance for {component.component_id}: utilization above threshold")
        component.maintenance_log.append({
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'type': 'preventive',
            'reason': 'high_utilization'
        })

    # ========================================================================
    # Training Methods (unchanged)
    # ========================================================================
    async def train_ml_model(self, training_data: List[Dict] = None) -> Dict:
        if not self.enable_ml_selection or not self.ml_selector:
            return {'status': 'disabled'}
        if training_data is None:
            training_data = self.recycling_history[-100:] if self.recycling_history else []
        formatted_data = []
        for item in training_data:
            formatted_data.append({
                'age_days': (datetime.now(timezone.utc) - datetime.fromisoformat(item['timestamp'])).days if 'timestamp' in item else 365,
                'utilization': 0.5,
                'maintenance_count': 0,
                'carbon_footprint': item.get('carbon_saved_kg', 0.5) / 10,
                'helium_content': item.get('helium_recovered_g', 0) / 1000,
                'recycling_potential': item.get('average_recovery_rate', 0.5),
                'reliability': 0.9,
                'cost_efficiency': 0.7,
                'selection_score': item.get('sustainability_score', 0.5)
            })
        result = await self.ml_selector.train_model(formatted_data)
        logger.info(f"ML model training completed: {result}")
        return result

    async def train_predictive_model(self) -> Dict:
        if not self.enable_predictive or not self.predictive_analyzer:
            return {'status': 'disabled'}
        result = await self.predictive_analyzer.train_forecast_model()
        logger.info(f"Predictive model training completed: {result}")
        return result

    # ========================================================================
    # Optimization Methods (unchanged)
    # ========================================================================

    def optimize_expert_hardware_allocation(
        self,
        expert_requirements: Dict[str, Any],
        carbon_budget: float,
        helium_budget: float,
        use_ml: bool = False
    ) -> Dict[str, Any]:
        available_components = [
            c for c in self.components.values()
            if c.current_state in [HardwareState.DEPLOYED, HardwareState.MAINTENANCE]
        ]
        if not available_components:
            return {'error': 'No available hardware', 'suggestion': 'deploy_new'}

        carbon_intensity = self.carbon_manager.carbon_intensity if self.carbon_manager else 400

        scored_components = []
        for component in available_components:
            age_days = (datetime.now(timezone.utc) - component.deployment_date).days
            lifecycle_score = 1.0 - (age_days / component.expected_lifetime_days)
            lifecycle_score = max(lifecycle_score, 0.1)
            carbon_score = 1.0 / (1.0 + component.manufacturing_carbon)
            helium_score = component.helium_content_l / 100.0 if self.enable_helium_tracking else 0.5
            if component.utilization_history:
                avg_util = np.mean(component.utilization_history[-50:])
                utilization_score = 1.0 - avg_util
            else:
                utilization_score = 0.5
            circularity_score = self.circularity_score

            if carbon_budget < 0.01:
                score = 0.3 * carbon_score + 0.25 * lifecycle_score + 0.25 * utilization_score + 0.2 * circularity_score
            elif helium_budget < 0.01:
                score = 0.3 * helium_score + 0.25 * carbon_score + 0.25 * lifecycle_score + 0.2 * circularity_score
            else:
                score = 0.2 * carbon_score + 0.2 * lifecycle_score + 0.2 * utilization_score + 0.2 * helium_score + 0.2 * circularity_score
            scored_components.append((component, score))

        scored_components.sort(key=lambda x: x[1], reverse=True)
        best_component, best_score = scored_components[0]

        ml_result = None
        if use_ml and self.enable_ml_selection:
            ml_result = asyncio.run(self.ml_selector.select_component_ml({
                'age_days': (datetime.now(timezone.utc) - best_component.deployment_date).days,
                'utilization': np.mean(best_component.utilization_history[-50:]) if best_component.utilization_history else 0.5,
                'maintenance_count': len(best_component.maintenance_log),
                'carbon_footprint': best_component.manufacturing_carbon,
                'helium_content': best_component.helium_content_l,
                'recycling_potential': 0.8,
                'reliability': 0.9,
                'cost_efficiency': 0.7
            }))

        return {
            'selected_component': best_component.component_id,
            'score': best_score,
            'component_type': best_component.type,
            'age_days': (datetime.now(timezone.utc) - best_component.deployment_date).days,
            'manufacturing_carbon': best_component.manufacturing_carbon,
            'helium_content_l': best_component.helium_content_l,
            'carbon_intensity': carbon_intensity,
            'sustainability_score': self.sustainability_score,
            'ml_result': ml_result,
            'recommendation': 'use_existing' if best_score > 0.5 else 'consider_repurposing'
        }

    # ========================================================================
    # Reporting Methods (unchanged)
    # ========================================================================

    def get_circularity_report(self) -> Dict[str, Any]:
        material_flows = {}
        for material in MaterialType:
            total_in_use = sum(
                c.materials.get(material, 0)
                for c in self.components.values()
                if c.current_state != HardwareState.RECYCLED
            )
            total_recovered = sum(
                r['materials_recovered'].get(material.value, {}).get('recovered_g', 0)
                for r in self.recycling_history
            )
            material_flows[material.value] = {
                'in_use_g': total_in_use,
                'recovered_g': total_recovered,
                'inventory_g': self.material_inventory[material]
            }

        report = {
            'circularity_score': self.circularity_score,
            'waste_diversion_rate': self.waste_diversion_rate,
            'material_recovery_rate': self.material_recovery_rate,
            'sustainability_score': self.sustainability_score,
            'total_components': len(self.components),
            'components_by_state': {
                state.value: sum(1 for c in self.components.values() if c.current_state == state)
                for state in HardwareState
            },
            'material_flows': material_flows,
            'total_carbon_saved_kg': sum(r['carbon_saved_kg'] for r in self.recycling_history),
            'helium_recovered_g': sum(r.get('helium_recovered_g', 0) for r in self.recycling_history)
        }
        if self.enable_helium_tracking and self.helium_manager:
            report['helium_position'] = self.helium_manager.get_helium_position()
        if self.enable_federated and self.federated_manager:
            report['federated_stats'] = self.federated_manager.get_federated_stats()
        if self.enable_predictive and self.predictive_analyzer:
            report['predictive_forecast'] = asyncio.run(
                self.predictive_analyzer.predict_lifetime({'age_days': 365, 'utilization': 0.5})
            )
        if self.enable_ml_selection and self.ml_selector:
            report['ml_status'] = {
                'trained': self.ml_selector.is_trained,
                'model_version': 'v3.0.0',
                'training_samples': len(self.ml_selector.training_history)
            }
        if self.enable_human_ai and self.human_ai:
            report['human_ai_insights'] = self.human_ai.get_collaborative_insights()
        return report

    def get_sustainability_report(self) -> Dict[str, Any]:
        return {
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'sustainability_score': self.sustainability_score,
            'circularity_report': self.get_circularity_report(),
            'recommendations': self._generate_sustainability_recommendations()
        }

    def _generate_sustainability_recommendations(self) -> List[str]:
        recommendations = []
        if self.sustainability_score < 0.5:
            recommendations.append("Improve circularity through better material recovery")
        if self.circularity_score < 0.5:
            recommendations.append("Increase component repurposing and recycling")
        if self.enable_helium_tracking and self.helium_manager:
            helium_pos = self.helium_manager.get_helium_position()
            if helium_pos.get('remaining_budget_l', 0) < 0:
                recommendations.append("CRITICAL: Helium budget exceeded - implement recovery systems")
        if self.enable_federated and self.federated_manager:
            if len(self.federated_manager.participants) < 2:
                recommendations.append("Increase federated participation for better circularity insights")
        if self.material_recovery_rate < 0.5:
            recommendations.append("Improve material recovery rate through better recycling processes")
        return recommendations or ["All circularity metrics are within acceptable ranges"]

    # ========================================================================
    # Self-Healing
    # ========================================================================
    async def self_heal(self):
        logger.info("CircularComputingManager self‑healing")
        if self.enable_self_healing:
            # Reset budgets to config defaults
            self.helium_manager.helium_budget_l = self.config.helium_budget_l
            self.circularity_strategy = 'balanced'
            self.carbon_recycling_priority = 0.5
            self.helium_recovery_priority = 0.5
            # Reset sustainability score
            self.sustainability_score = 0.0
            # Clear stale components (keep last 10)
            if len(self.components) > 10:
                # Remove oldest components
                sorted_components = sorted(self.components.values(), key=lambda c: c.deployment_date)
                for c in sorted_components[:-10]:
                    del self.components[c.component_id]
            # Clear stale recycling history (keep last 10)
            if len(self.recycling_history) > 10:
                self.recycling_history = self.recycling_history[-10:]
            # Reset health status
            self.health_status = "healthy"
            self.last_error = None
            # Save state
            await self.save_state()
            logger.info("Self-healing completed")

    # ========================================================================
    # Health Status
    # ========================================================================
    async def get_health_status(self) -> Dict[str, Any]:
        return {
            'status': self.health_status,
            'last_error': self.last_error,
            'total_components': len(self.components),
            'circularity_score': self.circularity_score,
            'sustainability_score': self.sustainability_score,
            'material_recovery_rate': self.material_recovery_rate,
            'bio_integration_active': self.enable_bio_integration,
            'event_driven_active': self.enable_event_driven,
            'self_healing_enabled': self.enable_self_healing,
            'swarm_coordination_active': self.enable_swarm_coordination,
            'persistence_enabled': self.enable_persistence,
        }

    # ========================================================================
    # Persistence Methods
    # ========================================================================
    async def save_state(self):
        if self.persistence:
            await self.persistence.save_state(self)

    async def load_state(self):
        if self.persistence:
            await self.persistence.load_state(self)

    # ========================================================================
    # Shutdown
    # ========================================================================
    async def shutdown(self):
        logger.info("Shutting down Circular Computing Manager")
        if self.enable_persistence:
            await self.save_state()
        if self.carbon_manager:
            await self.carbon_manager.close()
        if self.federated_manager:
            await self.federated_manager.close()
        logger.info("Shutdown complete")
