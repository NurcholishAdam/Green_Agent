"""
Enhanced Biodiversity Impact Assessment v2.1.0 - Complete Green Agent Implementation

Comprehensive biodiversity impact assessment with:
- Federated Reflexive Learning with distributed ecosystem tracking
- User-Adaptive Reflexivity with dynamic assessment parameters
- Real-time Carbon Intensity Integration with API support
- Cross-Domain Knowledge Transfer with ecosystem mapping
- Human-AI Collaborative Reflection with detailed reporting
- Predictive Reflexivity with ensemble forecasting
- Sustainability Score with multi-metric aggregation
- Enhanced Carbon/Helium Awareness with real-time tracking
- ML-Based Impact Prediction
- Ecosystem Trend Analysis
- Mitigation Strategy Optimization
- Configuration dataclass for centralized tuning
- Resilience with retry and circuit breaker
- Persistence for state across restarts
- Telemetry export for monitoring
- Health status reporting
- Incremental ML training with checkpointing
- Model compression for federated learning
- Extensible ecosystem configuration
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
# Bio-Inspired Import Check (unchanged)
# ============================================================================
try:
    from enhancements.bio_inspired.eco_atp_currency import EcoATPTokenManager
    from enhancements.bio_inspired.proton_gradient_fields import GradientFieldManager
    from enhancements.bio_inspired.atp_synthase_scheduler import ATPSynthaseScheduler
    from enhancements.bio_inspired.chromatophore_compartments import CompartmentManager
    from enhancements.bio_inspired.biomass_storage import BiomassStorage
    from enhancements.bio_inspired.photosynthetic_harvester import PhotosyntheticHarvester
    BIO_INSPIRED_AVAILABLE = True
except ImportError:
    BIO_INSPIRED_AVAILABLE = False

# ============================================================================
# Configuration Dataclass (NEW)
# ============================================================================

@dataclass
class BiodiversityConfig:
    """Centralized configuration for the Biodiversity Impact Assessor."""
    # Feature flags
    enable_federated: bool = True
    enable_carbon_intensity: bool = True
    enable_predictive: bool = True
    enable_ml_prediction: bool = True
    enable_human_ai: bool = True
    enable_persistence: bool = True
    enable_telemetry: bool = True
    enable_helium_tracking: bool = True  # NEW

    # Retry and circuit breaker
    max_retries: int = 3
    retry_base_delay_ms: float = 100.0
    retry_max_delay_ms: float = 5000.0
    circuit_breaker_threshold: int = 5
    circuit_breaker_recovery_timeout: float = 30.0

    # Predictive analyzer
    predictive_history_window: int = 100

    # ML impact predictor
    ml_input_size: int = 10
    ml_hidden_size: int = 64
    ml_epochs: int = 100
    ml_batch_size: int = 32

    # Federated learning
    server_url: Optional[str] = None
    federated_sparsity_ratio: float = 0.1  # top-k% of data to keep

    # Persistence
    persistence_path: str = "biodiversity_state.pkl"

    # Telemetry
    telemetry_export_interval: int = 60

    # Ecosystem configuration file (optional)
    ecosystems_config_path: Optional[str] = None

    # Helium-to-CO2 equivalence factor (kg CO2 per kg helium)
    helium_to_co2_factor: float = 20.0

# ============================================================================
# Protocols for external modules (NEW)
# ============================================================================

class CarbonIntensityProvider(Protocol):
    async def get_current_intensity(self) -> float: ...

class HeliumTrackerProvider(Protocol):
    def get_helium_position(self) -> Dict[str, Any]: ...

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

    def __init__(self, config: BiodiversityConfig):
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
# Helium Impact Tracker (NEW)
# ============================================================================

class HeliumImpactTracker:
    """Tracks helium usage and its biodiversity impact."""

    def __init__(self, config: BiodiversityConfig):
        self.config = config
        self.helium_budget_l = 100.0  # Placeholder; can be injected
        self.helium_usage: deque = deque(maxlen=86400)
        self.helium_recovered: deque = deque(maxlen=86400)
        self._running_total_usage = 0.0
        self._running_total_recovered = 0.0
        self.helium_to_co2_factor = config.helium_to_co2_factor

        asyncio.create_task(self._helium_accounting_loop())
        logger.info("HeliumImpactTracker initialized")

    def record_helium_usage(self, amount_l: float, source: str = "unknown"):
        usage = {'amount_l': amount_l, 'source': source, 'timestamp': datetime.utcnow()}
        self.helium_usage.append(usage)
        self._running_total_usage += amount_l

    def record_helium_recovery(self, amount_l: float, source: str = "unknown"):
        recovery = {'amount_l': amount_l, 'source': source, 'timestamp': datetime.utcnow()}
        self.helium_recovered.append(recovery)
        self._running_total_recovered += amount_l

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
            'co2_equivalent_kg': (self._running_total_usage - self._running_total_recovered) * self.helium_to_co2_factor
        }

# ============================================================================
# Predictive Biodiversity Analyzer (Enhanced with online learning)
# ============================================================================

class PredictiveBiodiversityAnalyzer:
    """Predictive reflexivity with online learning (SGD) for biodiversity impact."""

    def __init__(self, config: BiodiversityConfig):
        self.config = config
        self.history_window = config.predictive_history_window
        self.impact_history = deque(maxlen=self.history_window)
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

    def update_history(self, impact_data: Dict):
        self.impact_history.append({
            'timestamp': datetime.utcnow(),
            'total_impact': impact_data.get('total_impact', 0.5),
            'habitat_impact': impact_data.get('habitat_score', 0.5),
            'energy_impact': impact_data.get('energy_score', 0.5),
            'cooling_impact': impact_data.get('cooling_score', 0.5),
            'resource_impact': impact_data.get('resource_score', 0.5),
            'carbon_intensity': impact_data.get('carbon_intensity', 400),
            'ecosystem_sensitivity': impact_data.get('ecosystem_sensitivity', 0.5)
        })

    async def train_forecast_model(self):
        """Train or update the model incrementally."""
        if not self._ml_available:
            return {'status': 'ml_not_available'}
        if len(self.impact_history) < 10:
            return {'status': 'insufficient_data', 'samples': len(self.impact_history)}

        X, y = [], []
        history_list = list(self.impact_history)
        for i in range(len(history_list) - 5):
            features = []
            for j in range(5):
                data = history_list[i + j]
                features.extend([
                    data['total_impact'],
                    data['habitat_impact'],
                    data['energy_impact'],
                    data['cooling_impact'],
                    data['resource_impact'],
                    data['carbon_intensity'] / 100,
                    data['ecosystem_sensitivity']
                ])
            X.append(features)
            y.append(history_list[i + 5]['total_impact'])

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
        logger.info(f"Biodiversity model updated. R²={r2:.3f}")
        return {'status': 'success', 'r2': r2, 'samples': len(X)}

    async def predict_impact_trend(self, hours: int = 24) -> Dict:
        if not self.is_trained or len(self.impact_history) < 10:
            # Fallback: moving average
            if len(self.impact_history) > 0:
                recent = [h['total_impact'] for h in list(self.impact_history)[-5:]]
                pred = np.mean(recent) if recent else 0.5
                return {'predicted_impact': pred, 'confidence': 0.3, 'trend': 'moving_average'}
            return {'predicted_impact': 0.5, 'confidence': 0.0, 'trend': 'insufficient_data'}

        recent = list(self.impact_history)[-5:]
        features = []
        for data in recent:
            features.extend([
                data['total_impact'],
                data['habitat_impact'],
                data['energy_impact'],
                data['cooling_impact'],
                data['resource_impact'],
                data['carbon_intensity'] / 100,
                data['ecosystem_sensitivity']
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
        confidence = min(0.9, 0.5 + 0.4 * (len(self.impact_history) / 100))

        if len(self.forecast_history) > 5:
            recent_forecasts = list(self.forecast_history)[-5:]
            trend = "improving" if prediction < recent_forecasts[-1] else "declining" if prediction > recent_forecasts[-1] else "stable"
        else:
            trend = "stable"

        self.forecast_history.append({'prediction': prediction, 'trend': trend})
        return {
            'predicted_impact': prediction,
            'confidence': confidence,
            'trend': trend,
            'recommended_actions': self._generate_predictive_actions(prediction)
        }

    def _generate_predictive_actions(self, prediction: float) -> List[str]:
        actions = []
        if prediction > 0.7:
            actions.append("URGENT: Implement immediate biodiversity protection measures")
            actions.append("Relocate computation to lower-impact areas")
        elif prediction > 0.5:
            actions.append("Optimize energy and cooling strategies")
            actions.append("Invest in habitat restoration offsets")
        elif prediction > 0.3:
            actions.append("Monitor ecosystem health closely")
        else:
            actions.append("Current practices are sustainable - maintain standards")
        return actions

# ============================================================================
# Federated Biodiversity Assessor (Enhanced with compression & retry)
# ============================================================================

class FederatedBiodiversityAssessor:
    """Federated reflexive learning with compression and retry."""

    def __init__(self, config: BiodiversityConfig):
        self.config = config
        self.server_url = config.server_url
        self.round = 0
        self.local_impacts = {}
        self.global_impacts = {}
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

    def _compress_impact_data(self, data: Dict) -> Dict:
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

    async def send_local_impact(self, participant_id: str, impact_data: Dict, performance: float = 1.0) -> Dict:
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
                    compressed = self._compress_impact_data(impact_data)
                    update_data = {
                        'participant_id': participant_id,
                        'round': self.round,
                        'impact_data': compressed,
                        'performance': performance,
                        'sparsity_ratio': self.sparsity_ratio,
                        'timestamp': datetime.utcnow().isoformat()
                    }
                    async with session.post(
                        f"{self.server_url}/federated/biodiversity",
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
            logger.error("Circuit breaker opened for FederatedBiodiversityAssessor")
        return {'status': 'failed'}

    async def get_global_impacts(self) -> Optional[Dict]:
        if not self.server_url:
            return self.global_impacts
        for attempt in range(self.config.max_retries):
            try:
                async with self._lock:
                    session = await self._get_session()
                    async with session.get(
                        f"{self.server_url}/federated/biodiversity/global",
                        timeout=30
                    ) as response:
                        if response.status == 200:
                            data = await response.json()
                            self.global_impacts = data.get('impacts', {})
                            self.participants = data.get('participants', [])
                            return self.global_impacts
                        else:
                            logger.warning(f"Global fetch failed (attempt {attempt+1}): {response.status}")
            except Exception as e:
                logger.error(f"Global fetch error (attempt {attempt+1}): {e}")
            await asyncio.sleep(2 ** attempt)
        return None

    def aggregate_impacts(self, peer_impacts: List[Dict], weights: Dict[str, float] = None) -> Dict:
        if not peer_impacts:
            return {}
        aggregated = {}
        if weights is None:
            weights = {i: 1.0 for i in range(len(peer_impacts))}
        for key in peer_impacts[0].keys():
            if isinstance(peer_impacts[0][key], (int, float)):
                total = 0.0
                total_weight = 0.0
                for i, peer in enumerate(peer_impacts):
                    if key in peer:
                        total += peer[key] * weights.get(i, 1.0)
                        total_weight += weights.get(i, 1.0)
                aggregated[key] = total / max(total_weight, 0.001)
            else:
                values = [peer.get(key) for peer in peer_impacts if key in peer]
                if values:
                    aggregated[key] = max(set(values), key=values.count)
        return aggregated

    def get_federated_stats(self) -> Dict:
        return {
            'round': self.round,
            'participants': len(self.participants),
            'has_global_impacts': bool(self.global_impacts),
            'contribution_scores': self.contribution_scores,
            'sparsity_ratio': self.sparsity_ratio,
            'circuit_open': self.circuit_open
        }

    async def close(self):
        if self._session:
            await self._session.close()

# ============================================================================
# ML Impact Predictor (Enhanced with incremental training & checkpointing)
# ============================================================================

class MLImpactPredictor:
    """Machine learning-based impact prediction with incremental training and checkpointing."""

    def __init__(self, config: BiodiversityConfig):
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
        class ImpactPredictor(nn.Module):
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

        self.model = ImpactPredictor(self.input_size, self.hidden_size)
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
                item.get('carbon_intensity', 400) / 100,
                item.get('energy_intensity', 0.5),
                item.get('cooling_intensity', 0.5),
                item.get('resource_intensity', 0.5),
                item.get('ecosystem_sensitivity', 0.5),
                item.get('proximity_factor', 0.5),
                item.get('fragmentation_index', 0.5),
                item.get('species_density', 0.5),
                item.get('water_scarcity', 0.5),
                item.get('temperature_anomaly', 0.5)
            ])
            y.append(item.get('total_impact', 0.5))

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

    async def predict_impact(self, scenario: Dict) -> Dict:
        if not self.is_trained:
            return {'predicted_impact': 0.5, 'confidence': 0.0, 'status': 'model_not_trained'}

        features = np.array([[
            scenario.get('carbon_intensity', 400) / 100,
            scenario.get('energy_intensity', 0.5),
            scenario.get('cooling_intensity', 0.5),
            scenario.get('resource_intensity', 0.5),
            scenario.get('ecosystem_sensitivity', 0.5),
            scenario.get('proximity_factor', 0.5),
            scenario.get('fragmentation_index', 0.5),
            scenario.get('species_density', 0.5),
            scenario.get('water_scarcity', 0.5),
            scenario.get('temperature_anomaly', 0.5)
        ]])
        features_scaled = self.scaler.transform(features)

        self.model.eval()
        with torch.no_grad():
            prediction = self.model(torch.FloatTensor(features_scaled)).numpy()[0, 0]

        confidence = 0.8 if self.is_trained else 0.0
        return {
            'predicted_impact': float(prediction),
            'confidence': confidence,
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
# Human-AI Collaborative Biodiversity (Enhanced)
# ============================================================================

class HumanAICollaborativeBiodiversity:
    """Human-AI collaborative reflection for biodiversity impact."""

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
            'acknowledgment': f"Feedback received on {feedback.get('topic', 'biodiversity impact')}",
            'insights': [],
            'actions': [],
            'biodiversity_insights': []
        }
        if 'concern' in feedback:
            if feedback['concern'] == 'habitat':
                reflection['insights'].append("Habitat impact can be reduced through location optimization")
                reflection['actions'].append("Relocate computation to lower-impact areas")
            elif feedback['concern'] == 'energy':
                reflection['insights'].append("Energy source significantly affects biodiversity")
                reflection['actions'].append("Switch to renewable energy sources")
            elif feedback['concern'] == 'cooling':
                reflection['insights'].append("Cooling method impacts local water ecosystems")
                reflection['actions'].append("Implement water-free cooling solutions")
            elif feedback['concern'] == 'biodiversity':
                reflection['biodiversity_insights'].append("Biodiversity impact requires holistic assessment")
                reflection['actions'].append("Implement comprehensive biodiversity monitoring")
        if 'suggestion' in feedback:
            reflection['actions'].append(f"Implementing suggestion: {feedback['suggestion']}")
            reflection['insights'].append("User suggestion incorporated into improvement plan")
        reflection['action_items'] = self._prioritize_actions(reflection['actions'])
        return reflection

    def _prioritize_actions(self, actions: List[str]) -> List[Dict]:
        priorities = []
        for action in actions:
            if any(keyword in action.lower() for keyword in ['urgent', 'critical', 'immediate']):
                priority = 'high'
                impact = 0.9
                effort = 'high'
            elif any(keyword in action.lower() for keyword in ['biodiversity', 'habitat', 'ecosystem']):
                priority = 'high'
                impact = 0.8
                effort = 'medium'
            elif any(keyword in action.lower() for keyword in ['carbon', 'energy']):
                priority = 'medium'
                impact = 0.6
                effort = 'medium'
            else:
                priority = 'low'
                impact = 0.3
                effort = 'low'
            priorities.append({
                'action': action,
                'priority': priority,
                'impact': impact,
                'estimated_effort': effort,
                'biodiversity_weight': impact
            })
        return sorted(priorities, key=lambda x: (x['impact'], x['biodiversity_weight']), reverse=True)

    def get_collaborative_insights(self) -> Dict:
        if len(self.feedback_history) < 5:
            return {'status': 'insufficient_feedback'}
        recent_feedback = list(self.feedback_history)[-20:]
        topics = {}
        biodiversity_concerns = {}
        for f in recent_feedback:
            topic = f['feedback'].get('topic', 'general')
            topics[topic] = topics.get(topic, 0) + 1
            if 'concern' in f['feedback']:
                concern = f['feedback']['concern']
                biodiversity_concerns[concern] = biodiversity_concerns.get(concern, 0) + 1
        most_common = max(topics.items(), key=lambda x: x[1]) if topics else ('none', 0)
        top_concern = max(biodiversity_concerns.items(), key=lambda x: x[1]) if biodiversity_concerns else ('none', 0)
        return {
            'total_feedback': len(self.feedback_history),
            'top_topics': topics,
            'most_common_topic': most_common[0],
            'biodiversity_concerns': biodiversity_concerns,
            'top_biodiversity_concern': top_concern[0],
            'engagement_score': min(1.0, len(self.feedback_history) / 100),
            'user_count': len(set(f['user_id'] for f in self.feedback_history))
        }

# ============================================================================
# Enums and Data Classes (Enhanced)
# ============================================================================

class EcosystemType(Enum):
    TROPICAL_FOREST = "tropical_forest"
    TEMPERATE_FOREST = "temperate_forest"
    GRASSLAND = "grassland"
    WETLAND = "wetland"
    MARINE = "marine"
    FRESHWATER = "freshwater"
    URBAN = "urban"
    DESERT = "desert"

class ImpactCategory(Enum):
    HABITAT_LOSS = "habitat_loss"
    SPECIES_DISPLACEMENT = "species_displacement"
    WATER_POLLUTION = "water_pollution"
    AIR_POLLUTION = "air_pollution"
    NOISE_POLLUTION = "noise_pollution"
    LIGHT_POLLUTION = "light_pollution"
    THERMAL_POLLUTION = "thermal_pollution"
    RESOURCE_DEPLETION = "resource_depletion"
    CARBON_EMISSION = "carbon_emission"
    HELIUM_DEPLETION = "helium_depletion"

@dataclass
class BiodiversityMetric:
    ecosystem_type: EcosystemType
    species_richness: int
    endangered_species_count: int
    habitat_area_km2: float
    fragmentation_index: float
    ecological_connectivity: float
    last_assessment: datetime
    carbon_sensitivity: float = 0.5
    helium_sensitivity: float = 0.5
    sustainability_score: float = 0.0

@dataclass
class BiodiversityAssessment:
    assessment_id: str
    expert_type: str
    location: Dict[str, Any]
    total_impact: float
    impact_breakdown: Dict[str, Any]
    mitigation_strategies: List[Dict]
    recommendations: List[str]
    sustainability_score: float
    carbon_impact: float
    helium_impact: float
    timestamp: datetime

# ============================================================================
# Persistence Manager (NEW)
# ============================================================================

class BiodiversityPersistenceManager:
    """Manages persistence of biodiversity state, ML model, and ecosystem data."""

    def __init__(self, config: BiodiversityConfig):
        self.config = config
        self.path = config.persistence_path
        self._lock = asyncio.Lock()
        logger.info(f"BiodiversityPersistenceManager initialized (path={self.path})")

    async def save_state(self, assessor: 'BiodiversityImpactAssessor') -> bool:
        async with self._lock:
            try:
                state = {
                    'config': assessor.config,
                    'ecosystems': assessor.ecosystems,
                    'impact_history': assessor.impact_history,
                    'mitigation_strategies': assessor.mitigation_strategies,
                    'local_biodiversity_score': assessor.local_biodiversity_score,
                    'global_biodiversity_score': assessor.global_biodiversity_score,
                    'sustainability_score': assessor.sustainability_score,
                    'total_carbon_savings_kg': assessor.total_carbon_savings_kg,
                    'total_helium_savings_l': assessor.total_helium_savings_l,
                    'ml_checkpoint': assessor.ml_predictor.get_model_checkpoint() if assessor.ml_predictor else None,
                }
                serialized = pickle.dumps(state)
                compressed = zlib.compress(serialized)
                with open(self.path, 'wb') as f:
                    f.write(compressed)
                logger.info(f"Biodiversity state saved to {self.path}")
                return True
            except Exception as e:
                logger.error(f"Failed to save state: {e}")
                return False

    async def load_state(self, assessor: 'BiodiversityImpactAssessor') -> bool:
        async with self._lock:
            if not os.path.exists(self.path):
                logger.warning(f"Persistence file {self.path} not found")
                return False
            try:
                with open(self.path, 'rb') as f:
                    compressed = f.read()
                serialized = zlib.decompress(compressed)
                state = pickle.loads(serialized)

                assessor.ecosystems = state.get('ecosystems', {})
                assessor.impact_history = state.get('impact_history', [])
                assessor.mitigation_strategies = state.get('mitigation_strategies', {})
                assessor.local_biodiversity_score = state.get('local_biodiversity_score', 0.0)
                assessor.global_biodiversity_score = state.get('global_biodiversity_score', 0.0)
                assessor.sustainability_score = state.get('sustainability_score', 0.0)
                assessor.total_carbon_savings_kg = state.get('total_carbon_savings_kg', 0.0)
                assessor.total_helium_savings_l = state.get('total_helium_savings_l', 0.0)

                # Restore ML checkpoint
                ml_checkpoint = state.get('ml_checkpoint')
                if ml_checkpoint and assessor.ml_predictor:
                    assessor.ml_predictor.load_checkpoint(ml_checkpoint)

                logger.info(f"Biodiversity state loaded from {self.path}")
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

class BiodiversityTelemetry:
    """Collects telemetry for the biodiversity impact assessor."""

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
# Enhanced Biodiversity Impact Assessor (Main Class)
# ============================================================================

class BiodiversityImpactAssessor:
    """
    Enhanced Biodiversity Impact Assessor v2.1.0 - Complete Green Agent Implementation
    """

    def __init__(self, config: Optional[BiodiversityConfig] = None):
        self.config = config or BiodiversityConfig()

        # Feature flags
        self.enable_federated = self.config.enable_federated
        self.enable_carbon_intensity = self.config.enable_carbon_intensity
        self.enable_predictive = self.config.enable_predictive
        self.enable_ml_prediction = self.config.enable_ml_prediction
        self.enable_human_ai = self.config.enable_human_ai
        self.enable_persistence = self.config.enable_persistence
        self.enable_telemetry = self.config.enable_telemetry
        self.enable_helium_tracking = self.config.enable_helium_tracking

        # Initialize sub-modules with config
        self.carbon_manager = CarbonIntensityManager(self.config) if self.enable_carbon_intensity else None
        self.helium_tracker = HeliumImpactTracker(self.config) if self.enable_helium_tracking else None
        self.predictive_analyzer = PredictiveBiodiversityAnalyzer(self.config) if self.enable_predictive else None
        self.federated_assessor = FederatedBiodiversityAssessor(self.config) if self.enable_federated else None
        self.ml_predictor = MLImpactPredictor(self.config) if self.enable_ml_prediction else None
        self.human_ai = HumanAICollaborativeBiodiversity() if self.enable_human_ai else None

        # Persistence and telemetry
        self.persistence = BiodiversityPersistenceManager(self.config) if self.enable_persistence else None
        self.telemetry = BiodiversityTelemetry() if self.enable_telemetry else None

        # Bio-inspired modules (injected)
        self.token_manager = None
        self.gradient_manager = None
        self.scheduler = None
        self.compartment_manager = None
        self.biomass_storage = None
        self.harvester = None

        # Ecosystem tracking
        self.ecosystems: Dict[str, BiodiversityMetric] = {}
        self.impact_history: List[Dict] = []
        self.mitigation_strategies: Dict[str, List[Dict]] = {}

        # Biodiversity scores
        self.local_biodiversity_score = 0.0
        self.global_biodiversity_score = 0.0
        self.sustainability_score = 0.0
        self.total_carbon_savings_kg = 0.0
        self.total_helium_savings_l = 0.0

        # Initialize ecosystems (from config or default)
        self._initialize_ecosystems()

        # Start background tasks
        self._start_background_tasks()

        # Load state if persistence enabled
        if self.enable_persistence and self.persistence:
            asyncio.create_task(self._load_state())

        logger.info("Enhanced Biodiversity Impact Assessor v2.1.0 initialized")

    def _start_background_tasks(self):
        if self.enable_carbon_intensity and self.carbon_manager:
            asyncio.create_task(self._carbon_update_loop())
        if self.enable_predictive and self.predictive_analyzer:
            asyncio.create_task(self._predictive_update_loop())
        if self.enable_federated and self.federated_assessor:
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
                if self.predictive_analyzer and self.impact_history:
                    recent = self.impact_history[-5:] if self.impact_history else []
                    if recent:
                        total_impact = recent[-1].get('total_biodiversity_impact', 0.5)
                        breakdown = recent[-1].get('impact_breakdown', {})
                        habitat_score = breakdown.get('habitat', {}).get('score', 0.5)
                        energy_score = breakdown.get('energy', {}).get('score', 0.5)
                        cooling_score = breakdown.get('cooling', {}).get('score', 0.5)
                        resource_score = breakdown.get('resources', {}).get('score', 0.5)
                        carbon_intensity = self.carbon_manager.carbon_intensity if self.carbon_manager else 400
                        ecosystem_sensitivity = breakdown.get('habitat', {}).get('sensitivity', 0.5)
                        self.predictive_analyzer.update_history({
                            'total_impact': total_impact,
                            'habitat_score': habitat_score,
                            'energy_score': energy_score,
                            'cooling_score': cooling_score,
                            'resource_score': resource_score,
                            'carbon_intensity': carbon_intensity,
                            'ecosystem_sensitivity': ecosystem_sensitivity
                        })
                    await self.predictive_analyzer.train_forecast_model()
                await asyncio.sleep(300)
            except Exception as e:
                logger.error(f"Predictive update error: {e}")
                await asyncio.sleep(60)

    async def _federated_sync_loop(self):
        while True:
            try:
                if self.federated_assessor and self.impact_history:
                    latest = self.impact_history[-1] if self.impact_history else {}
                    participant_id = f"biodiversity_{hashlib.md5(str(self.ecosystems).encode()).hexdigest()[:8]}"
                    await self.federated_assessor.send_local_impact(
                        participant_id,
                        {
                            'local_score': self.local_biodiversity_score,
                            'global_score': self.global_biodiversity_score,
                            'total_impact': latest.get('total_biodiversity_impact', 0.5),
                            'timestamp': datetime.utcnow().isoformat()
                        },
                        performance=self.sustainability_score
                    )
                    await self.federated_assessor.get_global_impacts()
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
        """Report health of the biodiversity impact assessor."""
        return {
            'status': 'healthy',
            'score': min(1.0, self.sustainability_score),
            'details': {
                'modules': {
                    'carbon_manager': self.carbon_manager is not None,
                    'helium_tracker': self.helium_tracker is not None,
                    'predictive_analyzer': self.predictive_analyzer is not None,
                    'federated_assessor': self.federated_assessor is not None,
                    'ml_predictor': self.ml_predictor is not None,
                    'human_ai': self.human_ai is not None,
                    'persistence': self.persistence is not None,
                    'telemetry': self.telemetry is not None
                },
                'local_biodiversity_score': self.local_biodiversity_score,
                'global_biodiversity_score': self.global_biodiversity_score,
                'sustainability_score': self.sustainability_score,
                'ecosystems_tracked': len(self.ecosystems)
            }
        }

    def _initialize_ecosystems(self):
        """Initialize ecosystem tracking from config or default."""
        # If config provides a path, try to load from JSON
        if self.config.ecosystems_config_path and os.path.exists(self.config.ecosystems_config_path):
            try:
                with open(self.config.ecosystems_config_path, 'r') as f:
                    data = json.load(f)
                    for name, metrics in data.items():
                        self.ecosystems[name] = BiodiversityMetric(
                            ecosystem_type=EcosystemType(metrics['ecosystem_type']),
                            species_richness=metrics['species_richness'],
                            endangered_species_count=metrics['endangered_species_count'],
                            habitat_area_km2=metrics['habitat_area_km2'],
                            fragmentation_index=metrics['fragmentation_index'],
                            ecological_connectivity=metrics['ecological_connectivity'],
                            last_assessment=datetime.fromisoformat(metrics['last_assessment']),
                            carbon_sensitivity=metrics.get('carbon_sensitivity', 0.5),
                            helium_sensitivity=metrics.get('helium_sensitivity', 0.5),
                            sustainability_score=metrics.get('sustainability_score', 0.0)
                        )
                logger.info(f"Loaded ecosystems from {self.config.ecosystems_config_path}")
                return
            except Exception as e:
                logger.error(f"Failed to load ecosystems config: {e}")

        # Default ecosystems
        default_ecosystems = {
            'amazon_rainforest': BiodiversityMetric(
                ecosystem_type=EcosystemType.TROPICAL_FOREST,
                species_richness=16000,
                endangered_species_count=120,
                habitat_area_km2=5500000,
                fragmentation_index=0.15,
                ecological_connectivity=0.85,
                last_assessment=datetime.utcnow(),
                carbon_sensitivity=0.8,
                helium_sensitivity=0.3,
                sustainability_score=0.7
            ),
            'coral_reef_pacific': BiodiversityMetric(
                ecosystem_type=EcosystemType.MARINE,
                species_richness=4000,
                endangered_species_count=45,
                habitat_area_km2=50000,
                fragmentation_index=0.30,
                ecological_connectivity=0.70,
                last_assessment=datetime.utcnow(),
                carbon_sensitivity=0.6,
                helium_sensitivity=0.4,
                sustainability_score=0.6
            ),
            'european_wetlands': BiodiversityMetric(
                ecosystem_type=EcosystemType.WETLAND,
                species_richness=2500,
                endangered_species_count=30,
                habitat_area_km2=150000,
                fragmentation_index=0.25,
                ecological_connectivity=0.60,
                last_assessment=datetime.utcnow(),
                carbon_sensitivity=0.5,
                helium_sensitivity=0.5,
                sustainability_score=0.5
            )
        }
        self.ecosystems = default_ecosystems

    def inject_bio_core(self, bio_core: Any = None, **kwargs):
        """Inject bio-inspired modules for enhanced assessment."""
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
        logger.info("Bio-inspired modules injected into Biodiversity Impact Assessor")

    def _get_ecosystem_carbon_sensitivity(self, ecosystem_name: str) -> float:
        if ecosystem_name in self.ecosystems:
            return self.ecosystems[ecosystem_name].carbon_sensitivity
        return 0.5

    def _get_ecosystem_helium_sensitivity(self, ecosystem_name: str) -> float:
        if ecosystem_name in self.ecosystems:
            return self.ecosystems[ecosystem_name].helium_sensitivity
        return 0.5

    # ========================================================================
    # Enhanced Assessment Methods
    # ========================================================================

    async def assess_expert_impact(
        self,
        expert_type: str,
        location: Dict[str, Any],
        energy_source: str,
        cooling_method: str,
        use_ml_prediction: bool = False
    ) -> Dict[str, Any]:
        """
        Enhanced biodiversity impact assessment with ML prediction.
        """
        # Update carbon intensity
        carbon_intensity = 400
        if self.carbon_manager:
            carbon_intensity = await self.carbon_manager.get_current_intensity()

        # Perform standard assessment
        impact_scores = {}
        total_impact = 0.0

        habitat_impact = self._assess_habitat_impact(location)
        impact_scores['habitat'] = habitat_impact
        total_impact += habitat_impact['score']

        energy_impact = self._assess_energy_impact(energy_source, location)
        impact_scores['energy'] = energy_impact
        total_impact += energy_impact['score']

        cooling_impact = self._assess_cooling_impact(cooling_method, location)
        impact_scores['cooling'] = cooling_impact
        total_impact += cooling_impact['score']

        resource_impact = self._assess_resource_impact(expert_type)
        impact_scores['resources'] = resource_impact
        total_impact += resource_impact['score']

        # Carbon impact
        carbon_impact = self._assess_carbon_impact(energy_source, location, carbon_intensity)
        impact_scores['carbon'] = carbon_impact

        # Helium impact
        helium_impact = self._assess_helium_impact(cooling_method, location)
        impact_scores['helium'] = helium_impact

        total_impact += carbon_impact['score'] + helium_impact['score']
        total_impact = total_impact / 6.0

        # ML prediction if enabled
        ml_prediction = None
        if self.enable_ml_prediction and use_ml_prediction:
            ml_prediction = await self.ml_predictor.predict_impact({
                'carbon_intensity': carbon_intensity,
                'energy_intensity': energy_impact['score'],
                'cooling_intensity': cooling_impact['score'],
                'resource_intensity': resource_impact['score'],
                'ecosystem_sensitivity': habitat_impact.get('sensitivity', 0.5),
                'proximity_factor': habitat_impact.get('proximity_factor', 0.5),
                'fragmentation_index': habitat_impact.get('fragmentation_index', 0.5),
                'species_density': 0.5,
                'water_scarcity': location.get('water_scarcity_index', 0.5),
                'temperature_anomaly': 0.5
            })

        # Generate mitigation strategies
        mitigation = self._generate_mitigation_strategies(
            impact_scores, expert_type, location
        )

        # Calculate sustainability score
        sustainability_score = self._calculate_sustainability_score(
            impact_scores, total_impact, carbon_intensity
        )

        assessment = {
            'assessment_id': hashlib.md5(f"{expert_type}{location}{datetime.utcnow()}".encode()).hexdigest()[:12],
            'expert_type': expert_type,
            'location': location,
            'total_biodiversity_impact': total_impact,
            'impact_breakdown': impact_scores,
            'mitigation_strategies': mitigation,
            'recommendations': self._generate_recommendations(impact_scores),
            'sustainability_score': sustainability_score,
            'carbon_impact': carbon_impact,
            'helium_impact': helium_impact,
            'ml_prediction': ml_prediction,
            'timestamp': datetime.utcnow().isoformat()
        }

        self.impact_history.append(assessment)
        self._update_biodiversity_scores(assessment)

        # Update predictive analyzer
        if self.predictive_analyzer:
            self.predictive_analyzer.update_history({
                'total_impact': total_impact,
                'habitat_score': habitat_impact['score'],
                'energy_score': energy_impact['score'],
                'cooling_score': cooling_impact['score'],
                'resource_score': resource_impact['score'],
                'carbon_intensity': carbon_intensity,
                'ecosystem_sensitivity': habitat_impact.get('sensitivity', 0.5)
            })
            await self.predictive_analyzer.train_forecast_model()

        # Human-AI collaboration
        if self.enable_human_ai and self.human_ai:
            insights = self.human_ai.get_collaborative_insights()
            assessment['human_ai_insights'] = insights

        # Telemetry
        if self.telemetry:
            self.telemetry.increment('assessments_performed')
            self.telemetry.gauge('total_impact', total_impact)
            self.telemetry.gauge('sustainability_score', sustainability_score)

        # Helium tracking
        if self.enable_helium_tracking and self.helium_tracker:
            helium_usage_l = helium_impact.get('score', 0) * 10  # Placeholder calculation
            self.helium_tracker.record_helium_usage(helium_usage_l, expert_type)

        logger.info(
            f"Biodiversity assessment for {expert_type}: impact={total_impact:.2f}, "
            f"sustainability={sustainability_score:.2f}"
        )

        return assessment

    def _assess_carbon_impact(
        self,
        energy_source: str,
        location: Dict[str, Any],
        carbon_intensity: float
    ) -> Dict[str, Any]:
        """Assess carbon impact with real-time data."""
        energy_factors = {
            'solar': 0.02, 'wind': 0.03, 'hydroelectric': 0.05,
            'geothermal': 0.01, 'nuclear': 0.04, 'natural_gas': 0.35,
            'coal': 0.70, 'oil': 0.80, 'biomass': 0.25,
            'mixed_grid': 0.30
        }
        base_impact = energy_factors.get(energy_source, 0.3)
        carbon_factor = carbon_intensity / 400.0
        score = base_impact * carbon_factor
        if location.get('near_carbon_sensitive_ecosystem'):
            score *= 1.3
        return {
            'score': min(score, 1.0),
            'energy_source': energy_source,
            'carbon_intensity': carbon_intensity,
            'category': 'high' if score > 0.5 else 'moderate' if score > 0.2 else 'low'
        }

    def _assess_helium_impact(
        self,
        cooling_method: str,
        location: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Assess helium impact on biodiversity."""
        helium_factors = {
            'helium_cooling': 0.25,
            'water_cooling': 0.05,
            'air_cooling': 0.02,
            'evaporative_cooling': 0.08,
            'geothermal_cooling': 0.01,
            'liquid_immersion': 0.10,
            'free_cooling': 0.01
        }
        base_impact = helium_factors.get(cooling_method, 0.05)
        if location.get('near_helium_mining_region'):
            base_impact *= 2.0
        return {
            'score': min(base_impact, 1.0),
            'cooling_method': cooling_method,
            'category': 'high' if base_impact > 0.2 else 'moderate' if base_impact > 0.05 else 'low'
        }

    def _calculate_sustainability_score(
        self,
        impact_scores: Dict[str, Any],
        total_impact: float,
        carbon_intensity: float
    ) -> float:
        """Calculate overall sustainability score."""
        weights = {
            'habitat': 0.25,
            'energy': 0.20,
            'cooling': 0.15,
            'resources': 0.15,
            'carbon': 0.15,
            'helium': 0.10
        }
        score = 1.0
        for category, scores in impact_scores.items():
            if category in weights:
                score -= scores.get('score', 0) * weights[category]
        carbon_factor = 1.0 - (carbon_intensity / 800)
        score = score * 0.7 + carbon_factor * 0.3
        return max(0.0, min(1.0, score))

    # ========================================================================
    # Existing Assessment Methods (Preserved)
    # ========================================================================

    def _assess_habitat_impact(self, location: Dict[str, Any]) -> Dict[str, Any]:
        """Assess habitat impact of computing location."""
        nearest_ecosystem = self._find_nearest_ecosystem(location)
        if not nearest_ecosystem:
            return {'score': 0.1, 'category': 'minimal', 'ecosystem': None}
        distance_km = location.get('distance_to_ecosystem_km', 100)
        ecosystem = self.ecosystems[nearest_ecosystem]
        if distance_km < 1:
            proximity_factor = 1.0
        elif distance_km < 10:
            proximity_factor = 0.7
        elif distance_km < 50:
            proximity_factor = 0.3
        else:
            proximity_factor = 0.1
        sensitivity = ecosystem.endangered_species_count / 200.0
        sensitivity = min(sensitivity, 1.0)
        fragmentation_factor = ecosystem.fragmentation_index
        score = (proximity_factor * 0.4 + sensitivity * 0.4 + fragmentation_factor * 0.2)
        return {
            'score': score,
            'category': 'critical' if score > 0.7 else 'moderate' if score > 0.3 else 'low',
            'ecosystem': nearest_ecosystem,
            'proximity_factor': proximity_factor,
            'sensitivity': sensitivity,
            'fragmentation_index': fragmentation_factor
        }

    def _assess_energy_impact(self, energy_source: str, location: Dict[str, Any]) -> Dict[str, Any]:
        impact_factors = {
            'solar': 0.05, 'wind': 0.08, 'hydroelectric': 0.15,
            'geothermal': 0.03, 'nuclear': 0.10, 'natural_gas': 0.40,
            'coal': 0.80, 'oil': 0.90, 'biomass': 0.30,
            'mixed_grid': 0.35
        }
        base_impact = impact_factors.get(energy_source, 0.5)
        if location.get('near_water_body'):
            if energy_source in ['hydroelectric', 'nuclear']:
                base_impact *= 1.5
        if location.get('in_migration_corridor'):
            if energy_source in ['wind']:
                base_impact *= 1.3
        return {
            'score': base_impact,
            'energy_source': energy_source,
            'category': 'high' if base_impact > 0.5 else 'moderate' if base_impact > 0.2 else 'low'
        }

    def _assess_cooling_impact(self, cooling_method: str, location: Dict[str, Any]) -> Dict[str, Any]:
        impact_factors = {
            'air_cooling': 0.05, 'evaporative_cooling': 0.15,
            'water_cooling': 0.25, 'helium_cooling': 0.10,
            'geothermal_cooling': 0.03, 'liquid_immersion': 0.20,
            'free_cooling': 0.02
        }
        base_impact = impact_factors.get(cooling_method, 0.15)
        if cooling_method in ['water_cooling', 'evaporative_cooling']:
            if location.get('water_scarcity_index', 0) > 0.7:
                base_impact *= 2.0
            elif location.get('water_scarcity_index', 0) > 0.4:
                base_impact *= 1.5
        if cooling_method in ['water_cooling', 'liquid_immersion']:
            if location.get('near_water_body'):
                base_impact *= 1.3
        return {
            'score': base_impact,
            'cooling_method': cooling_method,
            'category': 'high' if base_impact > 0.5 else 'moderate' if base_impact > 0.2 else 'low'
        }

    def _assess_resource_impact(self, expert_type: str) -> Dict[str, Any]:
        resource_impacts = {
            'energy_expert': {'rare_earth': 0.1, 'copper': 0.05, 'overall': 0.08},
            'data_expert': {'rare_earth': 0.15, 'copper': 0.1, 'overall': 0.12},
            'iot_expert': {'rare_earth': 0.05, 'copper': 0.02, 'overall': 0.04},
            'quantum_expert': {'rare_earth': 0.3, 'copper': 0.2, 'overall': 0.25},
            'helium_expert': {'rare_earth': 0.08, 'copper': 0.05, 'overall': 0.06}
        }
        impact = resource_impacts.get(expert_type, {'overall': 0.1})
        return {
            'score': impact['overall'],
            'expert_type': expert_type,
            'category': 'high' if impact['overall'] > 0.2 else 'moderate' if impact['overall'] > 0.1 else 'low'
        }

    def _find_nearest_ecosystem(self, location: Dict[str, Any]) -> Optional[str]:
        if location.get('latitude', 0) < 0:
            return 'amazon_rainforest'
        elif location.get('latitude', 0) > 45:
            return 'european_wetlands'
        else:
            return 'coral_reef_pacific'

    def _generate_mitigation_strategies(
        self,
        impact_scores: Dict[str, Any],
        expert_type: str,
        location: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        strategies = []
        if impact_scores['habitat']['score'] > 0.5:
            strategies.append({
                'type': 'habitat_protection',
                'action': 'Relocate computation to lower-impact area',
                'impact_reduction': 0.6,
                'cost': 'medium',
                'implementation_time': 'short'
            })
            strategies.append({
                'type': 'habitat_restoration',
                'action': 'Invest in local habitat restoration project',
                'impact_reduction': 0.4,
                'cost': 'high',
                'implementation_time': 'long'
            })
        if impact_scores['energy']['score'] > 0.3:
            strategies.append({
                'type': 'renewable_energy',
                'action': 'Switch to renewable energy sources',
                'impact_reduction': 0.7,
                'cost': 'medium',
                'implementation_time': 'medium'
            })
        if impact_scores['cooling']['score'] > 0.3:
            strategies.append({
                'type': 'efficient_cooling',
                'action': 'Implement free cooling or geothermal cooling',
                'impact_reduction': 0.5,
                'cost': 'medium',
                'implementation_time': 'medium'
            })
        if impact_scores['resources']['score'] > 0.15:
            strategies.append({
                'type': 'circular_economy',
                'action': 'Use recycled materials and extend hardware life',
                'impact_reduction': 0.4,
                'cost': 'low',
                'implementation_time': 'short'
            })
        if 'carbon' in impact_scores and impact_scores['carbon']['score'] > 0.3:
            strategies.append({
                'type': 'carbon_offset',
                'action': 'Implement carbon offset program',
                'impact_reduction': 0.3,
                'cost': 'medium',
                'implementation_time': 'medium'
            })
        if 'helium' in impact_scores and impact_scores['helium']['score'] > 0.1:
            strategies.append({
                'type': 'helium_recovery',
                'action': 'Implement helium recovery and recycling',
                'impact_reduction': 0.5,
                'cost': 'high',
                'implementation_time': 'long'
            })
        return strategies

    def _generate_recommendations(self, impact_scores: Dict[str, Any]) -> List[str]:
        recommendations = []
        scores = {
            category: scores['score']
            for category, scores in impact_scores.items()
            if isinstance(scores, dict) and 'score' in scores
        }
        highest_impact = max(scores.items(), key=lambda x: x[1]) if scores else ('none', 0)
        if highest_impact[0] == 'habitat' and highest_impact[1] > 0.5:
            recommendations.append("HIGH PRIORITY: Relocate computation to avoid sensitive ecosystems")
        elif highest_impact[0] == 'energy' and highest_impact[1] > 0.5:
            recommendations.append("HIGH PRIORITY: Switch to renewable energy to reduce biodiversity impact")
        elif highest_impact[0] == 'cooling' and highest_impact[1] > 0.5:
            recommendations.append("HIGH PRIORITY: Implement water-free cooling to protect aquatic ecosystems")
        elif highest_impact[0] == 'carbon' and highest_impact[1] > 0.5:
            recommendations.append("HIGH PRIORITY: Reduce carbon emissions through efficiency improvements")
        elif highest_impact[0] == 'helium' and highest_impact[1] > 0.3:
            recommendations.append("HIGH PRIORITY: Implement helium recovery and recycling systems")
        if all(score < 0.2 for score in scores.values()):
            recommendations.append("Current setup has minimal biodiversity impact - maintain standards")
        else:
            recommendations.append("Consider biodiversity offsets equivalent to 110% of calculated impact")
        return recommendations

    def _update_biodiversity_scores(self, assessment: Dict[str, Any]):
        alpha = 0.1
        self.local_biodiversity_score = (
            (1 - alpha) * self.local_biodiversity_score +
            alpha * assessment['total_biodiversity_impact']
        )
        self.global_biodiversity_score = (
            (1 - alpha * 0.5) * self.global_biodiversity_score +
            alpha * 0.5 * assessment['total_biodiversity_impact']
        )
        self.sustainability_score = (
            (1 - alpha) * self.sustainability_score +
            alpha * assessment.get('sustainability_score', 0.5)
        )

    # ========================================================================
    # Report Generation
    # ========================================================================

    def get_biodiversity_report(self) -> Dict[str, Any]:
        recent_impacts = self.impact_history[-50:] if self.impact_history else []
        report = {
            'local_biodiversity_score': self.local_biodiversity_score,
            'global_biodiversity_score': self.global_biodiversity_score,
            'sustainability_score': self.sustainability_score,
            'total_carbon_savings_kg': self.total_carbon_savings_kg,
            'total_helium_savings_l': self.total_helium_savings_l,
            'ecosystems_tracked': len(self.ecosystems),
            'recent_impacts': [
                {
                    'expert_type': i['expert_type'],
                    'impact': i['total_biodiversity_impact'],
                    'sustainability_score': i.get('sustainability_score', 0.5),
                    'timestamp': i['timestamp']
                }
                for i in recent_impacts[-10:]
            ],
            'high_risk_ecosystems': [
                name for name, eco in self.ecosystems.items()
                if eco.endangered_species_count > 50
            ],
            'mitigation_effectiveness': self._calculate_mitigation_effectiveness(),
            'recommendations': self._generate_global_recommendations()
        }
        if self.enable_predictive and self.predictive_analyzer:
            report['predictive_forecast'] = asyncio.run(
                self.predictive_analyzer.predict_impact_trend()
            )
        if self.enable_federated and self.federated_assessor:
            report['federated_stats'] = self.federated_assessor.get_federated_stats()
        if self.enable_ml_prediction and self.ml_predictor:
            report['ml_status'] = {
                'trained': self.ml_predictor.is_trained,
                'model_version': 'v2.1.0',
                'training_samples': len(self.ml_predictor.training_history)
            }
        if self.enable_helium_tracking and self.helium_tracker:
            report['helium_position'] = self.helium_tracker.get_helium_position()
        return report

    def _calculate_mitigation_effectiveness(self) -> float:
        if not self.impact_history:
            return 0.0
        recent = self.impact_history[-20:]
        historical = self.impact_history[:-20]
        if not historical:
            return 0.5
        recent_avg = np.mean([i['total_biodiversity_impact'] for i in recent])
        historical_avg = np.mean([i['total_biodiversity_impact'] for i in historical])
        if historical_avg > 0:
            improvement = (historical_avg - recent_avg) / historical_avg
            return max(improvement, 0.0)
        return 0.0

    def _generate_global_recommendations(self) -> List[str]:
        recommendations = []
        if self.local_biodiversity_score > 0.5:
            recommendations.append("CRITICAL: Implement immediate biodiversity protection measures")
        if any(eco.endangered_species_count > 100 for eco in self.ecosystems.values()):
            recommendations.append("URGENT: Avoid computing operations near critical habitats")
        if self.sustainability_score < 0.5:
            recommendations.append("IMPROVE: Overall sustainability score needs improvement")
        recommendations.append("Implement helium recovery systems to reduce mining impact on biodiversity")
        recommendations.append("Monitor carbon intensity and optimize energy sources accordingly")
        return recommendations

    # ========================================================================
    # Expert Routing Guidance
    # ========================================================================

    def get_expert_routing_guidance(
        self,
        expert_options: List[str],
        location_options: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        impact_assessments = []
        for expert in expert_options:
            for location in location_options:
                assessment = {
                    'expert': expert,
                    'location': location.get('name', 'unknown'),
                    'estimated_impact': self._quick_impact_estimate(expert, location)
                }
                impact_assessments.append(assessment)
        impact_assessments.sort(key=lambda x: x['estimated_impact'])
        return {
            'best_option': impact_assessments[0] if impact_assessments else None,
            'worst_option': impact_assessments[-1] if impact_assessments else None,
            'all_options': impact_assessments,
            'recommendation': (
                f"Use {impact_assessments[0]['expert']} at {impact_assessments[0]['location']}"
                if impact_assessments else "No options available"
            ),
            'sustainability_score': self.sustainability_score,
            'biodiversity_impact_reduction': (
                (impact_assessments[-1]['estimated_impact'] - impact_assessments[0]['estimated_impact']) /
                max(impact_assessments[-1]['estimated_impact'], 0.001)
                if len(impact_assessments) > 1 else 0
            )
        }

    def _quick_impact_estimate(self, expert_type: str, location: Dict[str, Any]) -> float:
        location_sensitivity = location.get('biodiversity_sensitivity', 0.5)
        expert_intensity = {
            'energy': 0.3, 'data': 0.4, 'iot': 0.2,
            'quantum': 0.6, 'helium': 0.35
        }.get(expert_type, 0.4)
        return location_sensitivity * expert_intensity

    # ========================================================================
    # Training Methods
    # ========================================================================

    async def train_ml_model(self, training_data: List[Dict] = None) -> Dict:
        """Train ML model for impact prediction."""
        if not self.enable_ml_prediction or not self.ml_predictor:
            return {'status': 'disabled'}
        if training_data is None:
            training_data = self.impact_history[-100:] if self.impact_history else []
        formatted_data = []
        for item in training_data:
            breakdown = item.get('impact_breakdown', {})
            formatted_data.append({
                'carbon_intensity': item.get('carbon_impact', {}).get('carbon_intensity', 400),
                'energy_intensity': breakdown.get('energy', {}).get('score', 0.5),
                'cooling_intensity': breakdown.get('cooling', {}).get('score', 0.5),
                'resource_intensity': breakdown.get('resources', {}).get('score', 0.5),
                'ecosystem_sensitivity': breakdown.get('habitat', {}).get('sensitivity', 0.5),
                'proximity_factor': breakdown.get('habitat', {}).get('proximity_factor', 0.5),
                'fragmentation_index': breakdown.get('habitat', {}).get('fragmentation_index', 0.5),
                'species_density': 0.5,
                'water_scarcity': item.get('location', {}).get('water_scarcity_index', 0.5),
                'temperature_anomaly': 0.5,
                'total_impact': item.get('total_biodiversity_impact', 0.5)
            })
        result = await self.ml_predictor.train_model(formatted_data)
        logger.info(f"ML model training completed: {result}")
        return result

    async def train_predictive_model(self) -> Dict:
        """Train predictive model for trend analysis."""
        if not self.enable_predictive or not self.predictive_analyzer:
            return {'status': 'disabled'}
        result = await self.predictive_analyzer.train_forecast_model()
        logger.info(f"Predictive model training completed: {result}")
        return result

    # ========================================================================
    # Shutdown
    # ========================================================================

    async def shutdown(self):
        logger.info("Shutting down Biodiversity Impact Assessor")
        if self.enable_persistence:
            await self.save_state()
        if self.carbon_manager:
            await self.carbon_manager.close()
        if self.federated_assessor:
            await self.federated_assessor.close()
        logger.info("Shutdown complete")


# ============================================================================
# Legacy Compatibility
# ============================================================================

class LegacyBiodiversityImpactAssessor(BiodiversityImpactAssessor):
    """Legacy compatibility class."""
    def __init__(self):
        config = BiodiversityConfig(
            enable_federated=False,
            enable_carbon_intensity=False,
            enable_predictive=False,
            enable_ml_prediction=False,
            enable_human_ai=False,
            enable_helium_tracking=False,
            enable_persistence=False,
            enable_telemetry=False
        )
        super().__init__(config)
        logger.info("Legacy Biodiversity Impact Assessor initialized")
