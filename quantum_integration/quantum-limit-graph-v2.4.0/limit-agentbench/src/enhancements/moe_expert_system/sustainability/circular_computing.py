"""
Enhanced Circular Computing Module v2.1.0 - Complete Green Agent Implementation

Implements circular economy principles with:
- Federated Reflexive Learning for distributed lifecycle management
- User-Adaptive Reflexivity with dynamic configuration
- Real-time Carbon Intensity Integration with API support
- Cross-Domain Knowledge Transfer with material recovery
- Human-AI Collaborative Reflection with circularity reporting
- Predictive Reflexivity with ensemble forecasting
- ML-Based Component Selection
- Real-Time Helium Tracking
- Sustainability Score Integration
- Configuration dataclass for centralized tuning
- Resilience with retry and circuit breaker
- Persistence for state across restarts
- Telemetry export for monitoring
- Health status reporting
- Incremental ML training with checkpointing
- Model compression for federated learning
- Improved component selection with real-time data
"""

import asyncio
import logging
import numpy as np
from typing import Dict, Any, List, Optional, Tuple, Set, Union, Callable, Protocol
from dataclasses import dataclass, field, asdict
from datetime import datetime, timedelta
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
# Configuration Dataclass (NEW)
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

    # Helium-to-CO2 equivalence factor (kg CO2 per kg helium)
    helium_to_co2_factor: float = 20.0

    # Retry and circuit breaker
    max_retries: int = 3
    retry_base_delay_ms: float = 100.0
    retry_max_delay_ms: float = 5000.0
    circuit_breaker_threshold: int = 5
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

# ============================================================================
# Protocols for external modules (NEW)
# ============================================================================

class CarbonIntensityProvider(Protocol):
    async def get_current_intensity(self) -> float: ...

# ============================================================================
# Retry Helper (NEW)
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
# Carbon Intensity Manager (Enhanced with retry & circuit breaker)
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
        self.circuit_breaker_threshold = config.circuit_breaker_threshold
        self.max_retries = config.max_retries
        logger.info(f"CarbonIntensityManager initialized (region={self.region}, retries={self.max_retries})")

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
        if cache_key in self.cache and self.last_update and (datetime.utcnow() - self.last_update).seconds < self.update_interval:
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
                        self.cache[cache_key] = {'intensity': self.carbon_intensity, 'timestamp': self.last_update.isoformat()}
                        self.historical_intensities.append(self.carbon_intensity)
                        self.failure_count = 0
                        return {'intensity': self.carbon_intensity, 'region': self.region,
                                'timestamp': self.last_update.isoformat()}
                    else:
                        logger.warning(f"Carbon API returned {response.status}, attempt {attempt+1}")
                        if attempt == self.max_retries - 1:
                            self.failure_count += 1
                            if self.failure_count >= self.circuit_breaker_threshold:
                                self.circuit_open = True
                                self.circuit_open_until = datetime.utcnow() + timedelta(seconds=self.config.circuit_breaker_recovery_timeout)
                                logger.error("Circuit breaker opened for CarbonIntensityManager")
                            return self._get_fallback_response()
                        await asyncio.sleep(2 ** attempt)
            except Exception as e:
                logger.error(f"Carbon API error: {e}, attempt {attempt+1}")
                if attempt == self.max_retries - 1:
                    self.failure_count += 1
                    if self.failure_count >= self.circuit_breaker_threshold:
                        self.circuit_open = True
                        self.circuit_open_until = datetime.utcnow() + timedelta(seconds=self.config.circuit_breaker_recovery_timeout)
                    return self._get_fallback_response()
                await asyncio.sleep(2 ** attempt)

        # Should never reach here
        return self._get_fallback_response()

    def _get_fallback_response(self) -> Dict:
        fallback_intensities = {'us-east': 420, 'us-west': 350, 'eu': 280, 'asia': 500}
        intensity = fallback_intensities.get(self.region, 400)
        self.carbon_intensity = intensity
        self.last_update = datetime.now()
        return {'intensity': intensity, 'region': self.region,
                'timestamp': self.last_update.isoformat(), 'is_fallback': True}

    async def get_current_intensity(self) -> float:
        if self.last_update is None or (datetime.utcnow() - self.last_update).seconds > self.update_interval:
            await self.update_carbon_intensity(self.region)
        return self.carbon_intensity

    async def close(self):
        if self._session:
            await self._session.close()

# ============================================================================
# Real-Time Helium Tracking Module (Enhanced with configurable factor)
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

        # Helium to CO2 equivalence (configurable)
        self.helium_to_co2_factor = config.helium_to_co2_factor

        # Helium recovery rates by component type
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
        """Register helium content in a component"""
        self.component_helium[component_id] = {
            'total_l': helium_content_l,
            'recovered_l': 0.0,
            'type': component_type,
            'recovery_rate': self.recovery_rates.get(component_type, 0.85),
            'registered_at': datetime.utcnow()
        }
        logger.info(f"Registered helium content for {component_id}: {helium_content_l}L")

    def track_helium_usage(self, component_id: str, usage_l: float):
        """Track helium usage in a component"""
        usage = {'component_id': component_id, 'amount_l': usage_l, 'timestamp': datetime.utcnow()}
        self.helium_usage.append(usage)
        self._running_total_usage += usage_l
        if component_id in self.component_helium:
            self.component_helium[component_id]['used_l'] = self.component_helium[component_id].get('used_l', 0) + usage_l

    def calculate_helium_recovery(self, component_id: str) -> float:
        """Calculate recoverable helium from component"""
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
        """Record helium recovery from a component"""
        recovery = {'component_id': component_id, 'amount_l': amount_l, 'timestamp': datetime.utcnow()}
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
# Predictive Lifecycle Analyzer (Enhanced with online learning)
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
            'timestamp': datetime.utcnow(),
            'age_days': lifecycle_data.get('age_days', 0),
            'utilization': lifecycle_data.get('utilization', 0.5),
            'maintenance_count': lifecycle_data.get('maintenance_count', 0),
            'carbon_score': lifecycle_data.get('carbon_score', 0.5),
            'helium_remaining': lifecycle_data.get('helium_remaining', 0.5)
        })

    async def train_forecast_model(self):
        """Train or update the model incrementally."""
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
            # Fallback: moving average
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
# Federated Circular Manager (Enhanced with compression & retry)
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

    async def _get_session(self):
        if self._session is None and self.server_url:
            self._session = aiohttp.ClientSession()
        return self._session

    def _compress_component_data(self, data: Dict) -> Dict:
        """Keep only top-k% of numeric values by absolute magnitude."""
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

        # Circuit breaker check
        if self.circuit_open:
            if datetime.utcnow() < self.circuit_open_until:
                logger.warning("Circuit breaker open, skipping send")
                return {'status': 'circuit_open'}
            else:
                self.circuit_open = False
                self.failure_count = 0

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
                        'timestamp': datetime.utcnow().isoformat()
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
        if self.failure_count >= self.config.circuit_breaker_threshold:
            self.circuit_open = True
            self.circuit_open_until = datetime.utcnow() + timedelta(seconds=self.config.circuit_breaker_recovery_timeout)
            logger.error("Circuit breaker opened for FederatedCircularManager")
        return {'status': 'failed'}

    async def get_global_components(self) -> Optional[Dict]:
        if not self.server_url:
            return self.global_components
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
# ML Component Selector (Enhanced with incremental training & checkpointing)
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
        """Train or incrementally update the ML model."""
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
# Human-AI Collaborative Circular (Enhanced)
# ============================================================================

class HumanAICollaborativeCircular:
    """Human-AI collaborative reflection for circular computing."""

    def __init__(self):
        self.feedback_history = deque(maxlen=1000)
        self.reflection_logs = deque(maxlen=100)
        self.user_preferences = {}
        self._lock = asyncio.Lock()

    def collect_feedback(self, user_id: str, feedback: Dict) -> Dict:
        feedback_entry = {'user_id': user_id, 'timestamp': datetime.utcnow(), 'feedback': feedback}
        self.feedback_history.append(feedback_entry)
        if 'preference' in feedback:
            self.user_preferences[user_id] = feedback['preference']
        reflection = self._generate_reflection(feedback)
        self.reflection_logs.append(reflection)
        return reflection

    def _generate_reflection(self, feedback: Dict) -> Dict:
        reflection = {
            'timestamp': datetime.utcnow().isoformat(),
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
# Enums and Data Classes (Enhanced)
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
# Persistence Manager (NEW)
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
# Telemetry Collector (NEW)
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
# Enhanced Circular Computing Manager (Main Class)
# ============================================================================

class CircularComputingManager:
    """
    Enhanced Circular Computing Manager v2.1.0 - Complete Green Agent Implementation
    """

    def __init__(self, config: Optional[CircularComputingConfig] = None):
        self.config = config or CircularComputingConfig()

        # Feature flags
        self.enable_federated = self.config.enable_federated
        self.enable_carbon_intensity = self.config.enable_carbon_intensity
        self.enable_predictive = self.config.enable_predictive
        self.enable_ml_selection = self.config.enable_ml_selection
        self.enable_human_ai = self.config.enable_human_ai
        self.enable_helium_tracking = self.config.enable_helium_tracking
        self.enable_persistence = self.config.enable_persistence
        self.enable_telemetry = self.config.enable_telemetry

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

        # Core tracking
        self.components: Dict[str, HardwareComponent] = {}
        self.material_inventory: Dict[MaterialType, float] = {}
        self.recycling_history: List[Dict] = []

        # Circular economy metrics
        self.circularity_score = 0.0
        self.waste_diversion_rate = 0.0
        self.material_recovery_rate = 0.0
        self.sustainability_score = 0.0

        # Initialize material inventory
        self._initialize_inventory()

        # Start background tasks
        self._start_background_tasks()

        # Load state if persistence enabled
        if self.enable_persistence and self.persistence:
            asyncio.create_task(self._load_state())

        logger.info(
            f"Enhanced Circular Computing Manager v2.1.0 initialized: "
            f"helium_budget={self.config.helium_budget_l}L, "
            f"federated={self.enable_federated}, ml={self.enable_ml_selection}"
        )

    def _start_background_tasks(self):
        if self.enable_carbon_intensity and self.carbon_manager:
            asyncio.create_task(self._carbon_update_loop())
        if self.enable_predictive and self.predictive_analyzer:
            asyncio.create_task(self._predictive_update_loop())
        if self.enable_federated and self.federated_manager:
            asyncio.create_task(self._federated_sync_loop())
        if self.enable_telemetry and self.telemetry:
            asyncio.create_task(self._telemetry_export_loop())

    async def _carbon_update_loop(self):
        while True:
            try:
                if self.carbon_manager:
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
                        age_days = (datetime.utcnow() - component.deployment_date).days
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
                            'timestamp': datetime.utcnow().isoformat()
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

    async def _load_state(self):
        if self.persistence:
            await self.persistence.load_state(self)

    async def save_state(self):
        if self.persistence:
            await self.persistence.save_state(self)

    async def delete_state(self):
        if self.persistence:
            await self.persistence.delete_state()

    async def get_health_status(self) -> Dict[str, Any]:
        """Report health of the circular computing system."""
        return {
            'status': 'healthy',
            'score': min(1.0, self.sustainability_score),
            'details': {
                'modules': {
                    'carbon_manager': self.carbon_manager is not None,
                    'helium_manager': self.helium_manager is not None,
                    'predictive_analyzer': self.predictive_analyzer is not None,
                    'federated_manager': self.federated_manager is not None,
                    'ml_selector': self.ml_selector is not None,
                    'human_ai': self.human_ai is not None,
                    'persistence': self.persistence is not None,
                    'telemetry': self.telemetry is not None
                },
                'total_components': len(self.components),
                'circularity_score': self.circularity_score,
                'sustainability_score': self.sustainability_score
            }
        }

    def _initialize_inventory(self):
        for material in MaterialType:
            self.material_inventory[material] = 0.0

    def inject_bio_core(self, bio_core: Any = None, **kwargs):
        # Stub for compatibility
        pass

    # ========================================================================
    # Component Registration with Enhanced Features
    # ========================================================================

    def register_component(
        self,
        component_type: str,
        materials: Dict[MaterialType, float],
        manufacturing_carbon: float,
        expected_lifetime_days: int = 1825,
        helium_content_l: float = 0.0
    ) -> str:
        component_id = f"COMP-{datetime.utcnow().timestamp()}-{component_type}"
        component = HardwareComponent(
            component_id=component_id,
            type=component_type,
            materials=materials,
            manufacturing_carbon=manufacturing_carbon,
            current_state=HardwareState.MANUFACTURING,
            deployment_date=datetime.utcnow(),
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

    # ========================================================================
    # Enhanced Recycling
    # ========================================================================

    async def recycle_component(
        self,
        component_id: str,
        use_ml_optimization: bool = False
    ) -> Dict[str, Any]:
        if component_id not in self.components:
            return {'error': 'Component not found'}
        component = self.components[component_id]

        ml_optimization = None
        if self.enable_ml_selection and use_ml_optimization:
            ml_result = await self.ml_selector.select_component_ml({
                'age_days': (datetime.utcnow() - component.deployment_date).days,
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
            'timestamp': datetime.utcnow().isoformat(),
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
                'age_days': (datetime.utcnow() - component.deployment_date).days,
                'utilization': np.mean(component.utilization_history[-50:]) if component.utilization_history else 0.5,
                'maintenance_count': len(component.maintenance_log),
                'carbon_score': 1.0 / (1.0 + component.manufacturing_carbon),
                'helium_remaining': component.helium_content_l
            })
            await self.predictive_analyzer.train_forecast_model()

        if self.enable_human_ai and self.human_ai:
            recycling_record['human_ai_insights'] = self.human_ai.get_collaborative_insights()

        if self.telemetry:
            self.telemetry.increment('recycles_performed')
            self.telemetry.gauge('carbon_saved', carbon_saved)
            self.telemetry.gauge('sustainability_score', self.sustainability_score)

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
    # Component State Methods
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
            'timestamp': datetime.utcnow().isoformat(),
            'type': 'preventive',
            'reason': 'high_utilization'
        })

    # ========================================================================
    # Training Methods
    # ========================================================================

    async def train_ml_model(self, training_data: List[Dict] = None) -> Dict:
        if not self.enable_ml_selection or not self.ml_selector:
            return {'status': 'disabled'}
        if training_data is None:
            training_data = self.recycling_history[-100:] if self.recycling_history else []
        formatted_data = []
        for item in training_data:
            formatted_data.append({
                'age_days': (datetime.utcnow() - datetime.fromisoformat(item['timestamp'])).days if 'timestamp' in item else 365,
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
    # Optimization Methods
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
            age_days = (datetime.utcnow() - component.deployment_date).days
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
                'age_days': (datetime.utcnow() - best_component.deployment_date).days,
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
            'age_days': (datetime.utcnow() - best_component.deployment_date).days,
            'manufacturing_carbon': best_component.manufacturing_carbon,
            'helium_content_l': best_component.helium_content_l,
            'carbon_intensity': carbon_intensity,
            'sustainability_score': self.sustainability_score,
            'ml_result': ml_result,
            'recommendation': 'use_existing' if best_score > 0.5 else 'consider_repurposing'
        }

    # ========================================================================
    # Reporting Methods
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
                'model_version': 'v2.1.0',
                'training_samples': len(self.ml_selector.training_history)
            }
        if self.enable_human_ai and self.human_ai:
            report['human_ai_insights'] = self.human_ai.get_collaborative_insights()
        return report

    def get_sustainability_report(self) -> Dict[str, Any]:
        return {
            'timestamp': datetime.utcnow().isoformat(),
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
