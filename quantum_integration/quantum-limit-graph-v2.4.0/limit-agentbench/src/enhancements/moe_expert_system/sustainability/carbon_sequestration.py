"""
Enhanced Carbon Sequestration and Offset Integration v2.1.0 - Complete Green Agent Implementation

Enables active carbon removal strategies with:
- Federated Reflexive Learning with distributed project management
- User-Adaptive Reflexivity with dynamic strategies
- Real-time Carbon Intensity Integration with API support
- Cross-Domain Knowledge Transfer with project diversity
- Human-AI Collaborative Reflection with portfolio reporting
- Predictive Reflexivity with ensemble forecasting
- Helium Offset Integration
- ML-Based Project Selection
- Sustainability Score Integration
- Configuration dataclass for centralized tuning
- Resilience with retry and circuit breaker
- Persistence for state across restarts
- Telemetry export for monitoring
- Health status reporting
- Incremental ML training with checkpointing
- Model compression for federated learning
- Improved project selection with real-time data
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
class CarbonSequestrationConfig:
    """Centralized configuration for the Carbon Sequestration Manager."""
    # Budgets
    carbon_budget_kg: float = 1000.0
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

    # Offset strategy
    offset_strategy: str = 'proactive'  # 'proactive', 'reactive', 'conservative'

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

    # ML project selector
    ml_input_size: int = 8
    ml_hidden_size: int = 64
    ml_epochs: int = 100
    ml_batch_size: int = 32

    # Federated learning
    server_url: Optional[str] = None
    federated_sparsity_ratio: float = 0.1  # top-k% of data to keep

    # Persistence
    persistence_path: str = "carbon_sequestration_state.pkl"

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

    def __init__(self, config: CarbonSequestrationConfig):
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
# Helium Sequestration Manager (Enhanced with configurable factor)
# ============================================================================

class HeliumSequestrationManager:
    """
    Helium emission tracking and offset integration.
    """

    def __init__(self, config: CarbonSequestrationConfig):
        self.config = config
        self.helium_budget_l = config.helium_budget_l
        self.helium_emissions: deque = deque(maxlen=86400)
        self.helium_offsets: deque = deque(maxlen=86400)
        self._running_total_emissions = 0.0
        self._running_total_offsets = 0.0

        # Helium to CO2 equivalence (configurable)
        self.helium_to_co2_factor = config.helium_to_co2_factor

        # Helium sequestration projects
        self.helium_sequestration_projects = {
            'helium_recovery_advanced': {
                'type': 'helium_recovery',
                'capacity_l_per_year': 5000,
                'cost_per_l': 0.50,
                'efficiency': 0.95,
                'co_benefits': ['technology_development', 'resource_conservation']
            },
            'helium_capture_system': {
                'type': 'helium_capture',
                'capacity_l_per_year': 2000,
                'cost_per_l': 0.80,
                'efficiency': 0.85,
                'co_benefits': ['emissions_reduction', 'recycling']
            },
            'alternative_cooling': {
                'type': 'alternative_cooling',
                'capacity_l_per_year': 10000,
                'cost_per_l': 0.30,
                'efficiency': 0.70,
                'co_benefits': ['technology_diversification', 'cost_reduction']
            }
        }

        asyncio.create_task(self._helium_accounting_loop())
        logger.info(f"Helium Sequestration Manager initialized: budget={self.helium_budget_l}L, factor={self.helium_to_co2_factor}")

    def record_helium_emission(self, amount_l: float, source: str = "unknown"):
        emission = {'amount_l': amount_l, 'source': source, 'timestamp': datetime.utcnow()}
        self.helium_emissions.append(emission)
        self._running_total_emissions += amount_l

    def record_helium_offset(self, amount_l: float, project_id: str = None):
        offset = {'amount_l': amount_l, 'project_id': project_id or 'unknown', 'timestamp': datetime.utcnow()}
        self.helium_offsets.append(offset)
        self._running_total_offsets += amount_l

    async def _helium_accounting_loop(self):
        while True:
            try:
                net_position = self._running_total_emissions - self._running_total_offsets
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
            'total_emissions_l': self._running_total_emissions,
            'total_offsets_l': self._running_total_offsets,
            'net_position_l': self._running_total_emissions - self._running_total_offsets,
            'remaining_budget_l': self.helium_budget_l - (self._running_total_emissions - self._running_total_offsets),
            'co2_equivalent_kg': (self._running_total_emissions - self._running_total_offsets) * self.helium_to_co2_factor,
            'projects': self.helium_sequestration_projects
        }

    def calculate_helium_offset_from_carbon(self, carbon_credit_kg: float) -> float:
        return carbon_credit_kg * 0.05

    def select_helium_project(self, amount_l: float) -> Dict[str, Any]:
        scored_projects = []
        for project_id, project in self.helium_sequestration_projects.items():
            cost_score = 1.0 / (1.0 + project['cost_per_l'])
            capacity_score = min(project['capacity_l_per_year'] / max(amount_l, 1), 1.0)
            efficiency_score = project['efficiency']
            score = 0.4 * cost_score + 0.3 * capacity_score + 0.3 * efficiency_score
            scored_projects.append((project_id, score, project))
        scored_projects.sort(key=lambda x: x[1], reverse=True)
        if scored_projects:
            return {
                'project_id': scored_projects[0][0],
                'project': scored_projects[0][2],
                'score': scored_projects[0][1]
            }
        return {'project_id': None, 'project': None, 'score': 0.0}

# ============================================================================
# Predictive Sequestration Analyzer (Enhanced with online learning)
# ============================================================================

class PredictiveSequestrationAnalyzer:
    """Predictive reflexivity with online learning (SGD) for sequestration."""

    def __init__(self, config: CarbonSequestrationConfig):
        self.config = config
        self.history_window = config.predictive_history_window
        self.sequestration_history = deque(maxlen=self.history_window)
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

    def update_history(self, sequestration_data: Dict):
        self.sequestration_history.append({
            'timestamp': datetime.utcnow(),
            'offset_amount': sequestration_data.get('offset_amount', 0),
            'credit_price': sequestration_data.get('credit_price', 50),
            'project_success_rate': sequestration_data.get('project_success_rate', 0.9),
            'verification_confidence': sequestration_data.get('verification_confidence', 0.7),
            'carbon_intensity': sequestration_data.get('carbon_intensity', 400)
        })

    async def train_forecast_model(self):
        """Train or update the model incrementally."""
        if not self._ml_available:
            return {'status': 'ml_not_available'}
        if len(self.sequestration_history) < 10:
            return {'status': 'insufficient_data', 'samples': len(self.sequestration_history)}

        X, y = [], []
        history_list = list(self.sequestration_history)
        for i in range(len(history_list) - 5):
            features = []
            for j in range(5):
                data = history_list[i + j]
                features.extend([
                    data['offset_amount'] / 1000,
                    data['credit_price'] / 100,
                    data['project_success_rate'],
                    data['verification_confidence'],
                    data['carbon_intensity'] / 100
                ])
            X.append(features)
            y.append(history_list[i + 5]['offset_amount'])

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
        logger.info(f"Predictive model updated. R²={r2:.3f}")
        return {'status': 'success', 'r2': r2, 'samples': len(X)}

    async def predict_offset_demand(self) -> Dict:
        if not self.is_trained or len(self.sequestration_history) < 10:
            # Fallback: moving average
            if len(self.sequestration_history) > 0:
                recent = [h['offset_amount'] for h in list(self.sequestration_history)[-5:]]
                pred = np.mean(recent) if recent else 1000
                return {'predicted_demand': pred, 'confidence': 0.3, 'trend': 'moving_average'}
            return {'predicted_demand': 1000, 'confidence': 0.0, 'trend': 'insufficient_data'}

        recent = list(self.sequestration_history)[-5:]
        features = []
        for data in recent:
            features.extend([
                data['offset_amount'] / 1000,
                data['credit_price'] / 100,
                data['project_success_rate'],
                data['verification_confidence'],
                data['carbon_intensity'] / 100
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
        confidence = min(0.9, 0.5 + 0.4 * (len(self.sequestration_history) / 100))

        if len(self.forecast_history) > 5:
            recent_forecasts = list(self.forecast_history)[-5:]
            trend = "increasing" if prediction > recent_forecasts[-1] else "decreasing" if prediction < recent_forecasts[-1] else "stable"
        else:
            trend = "stable"

        self.forecast_history.append({'prediction': prediction, 'trend': trend})
        return {
            'predicted_demand': max(0, prediction),
            'confidence': confidence,
            'trend': trend,
            'recommended_actions': self._generate_predictive_actions(prediction)
        }

    def _generate_predictive_actions(self, prediction: float) -> List[str]:
        actions = []
        if prediction > 5000:
            actions.append("Increase sequestration project capacity")
            actions.append("Diversify project portfolio")
        elif prediction < 1000:
            actions.append("Reduce sequestration spending")
            actions.append("Focus on high-impact projects")
        else:
            actions.append("Maintain current sequestration strategy")
        return actions

# ============================================================================
# Federated Sequestration Manager (Enhanced with compression & retry)
# ============================================================================

class FederatedSequestrationManager:
    """Federated reflexive learning with compression and retry."""

    def __init__(self, config: CarbonSequestrationConfig):
        self.config = config
        self.server_url = config.server_url
        self.round = 0
        self.local_projects = {}
        self.global_projects = {}
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

    def _compress_project_data(self, data: Dict) -> Dict:
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

    async def send_local_projects(self, participant_id: str, project_data: Dict, performance: float = 1.0) -> Dict:
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
                    compressed = self._compress_project_data(project_data)
                    update_data = {
                        'participant_id': participant_id,
                        'round': self.round,
                        'project_data': compressed,
                        'performance': performance,
                        'sparsity_ratio': self.sparsity_ratio,
                        'timestamp': datetime.utcnow().isoformat()
                    }
                    async with session.post(
                        f"{self.server_url}/federated/sequestration",
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
            logger.error("Circuit breaker opened for FederatedSequestrationManager")
        return {'status': 'failed'}

    async def get_global_projects(self) -> Optional[Dict]:
        if not self.server_url:
            return self.global_projects
        for attempt in range(self.config.max_retries):
            try:
                async with self._lock:
                    session = await self._get_session()
                    async with session.get(
                        f"{self.server_url}/federated/sequestration/global",
                        timeout=30
                    ) as response:
                        if response.status == 200:
                            data = await response.json()
                            self.global_projects = data.get('projects', {})
                            self.participants = data.get('participants', [])
                            return self.global_projects
                        else:
                            logger.warning(f"Global fetch failed (attempt {attempt+1}): {response.status}")
            except Exception as e:
                logger.error(f"Global fetch error (attempt {attempt+1}): {e}")
            await asyncio.sleep(2 ** attempt)
        return None

    def aggregate_projects(self, peer_projects: List[Dict], weights: Dict[str, float] = None) -> Dict:
        if not peer_projects:
            return {}
        aggregated = {}
        if weights is None:
            weights = {i: 1.0 for i in range(len(peer_projects))}
        for project_id in peer_projects[0].keys():
            if isinstance(peer_projects[0][project_id], (int, float)):
                total = 0.0
                total_weight = 0.0
                for i, peer in enumerate(peer_projects):
                    if project_id in peer:
                        total += peer[project_id] * weights.get(i, 1.0)
                        total_weight += weights.get(i, 1.0)
                aggregated[project_id] = total / max(total_weight, 0.001)
        return aggregated

    def get_federated_stats(self) -> Dict:
        return {
            'round': self.round,
            'participants': len(self.participants),
            'has_global_projects': bool(self.global_projects),
            'contribution_scores': self.contribution_scores,
            'sparsity_ratio': self.sparsity_ratio,
            'circuit_open': self.circuit_open
        }

    async def close(self):
        if self._session:
            await self._session.close()

# ============================================================================
# ML Project Selector (Enhanced with incremental training & checkpointing)
# ============================================================================

class MLProjectSelector:
    """Machine learning-based project selection with incremental training and checkpointing."""

    def __init__(self, config: CarbonSequestrationConfig):
        self.config = config
        self.input_size = config.ml_input_size
        self.hidden_size = config.ml_hidden_size
        self.model = None
        self.scaler = StandardScaler()
        self.is_trained = False
        self.optimizer = None
        self.criterion = nn.CrossEntropyLoss()
        self.training_history: List[float] = []
        self._init_model()

    def _init_model(self):
        class ProjectSelector(nn.Module):
            def __init__(self, input_size, hidden_size):
                super().__init__()
                self.network = nn.Sequential(
                    nn.Linear(input_size, hidden_size),
                    nn.ReLU(),
                    nn.BatchNorm1d(hidden_size),
                    nn.Linear(hidden_size, hidden_size // 2),
                    nn.ReLU(),
                    nn.BatchNorm1d(hidden_size // 2),
                    nn.Linear(hidden_size // 2, len(self.get_project_types()))
                )

            def get_project_types(self):
                return ['reforestation', 'dac', 'biochar', 'ocean_based', 'helium_recovery']

        self.model = ProjectSelector(self.input_size, self.hidden_size)
        self.optimizer = optim.Adam(self.model.parameters(), lr=0.001)

    async def train_model(self, training_data: List[Dict], epochs: Optional[int] = None) -> Dict:
        """Train or incrementally update the ML model."""
        if len(training_data) < 20:
            return {'status': 'insufficient_data', 'samples': len(training_data)}

        epochs = epochs or self.config.ml_epochs
        project_types = self.model.get_project_types()

        X = []
        y = []
        for item in training_data:
            X.append([
                item.get('carbon_intensity', 400) / 100,
                item.get('cost_budget', 0.5),
                item.get('urgency', 0.5),
                item.get('permanence_requirement', 0.5),
                item.get('co_benefit_weight', 0.5),
                item.get('verification_confidence', 0.5),
                item.get('project_age_months', 1) / 12,
                item.get('historical_success', 0.8)
            ])
            # One-hot encode selected project
            selected = [0] * len(project_types)
            if 'selected_project' in item:
                idx = project_types.index(item['selected_project']) if item['selected_project'] in project_types else 0
                selected[idx] = 1
            y.append(selected)

        X = np.array(X)
        y = np.array(y)

        if self.scaler.mean_ is None:
            X_scaled = self.scaler.fit_transform(X)
        else:
            X_scaled = self.scaler.transform(X)

        dataset = TensorDataset(
            torch.FloatTensor(X_scaled),
            torch.FloatTensor(y)
        )
        dataloader = DataLoader(dataset, batch_size=self.config.ml_batch_size, shuffle=True)

        self.model.train()
        losses = []
        for epoch in range(epochs):
            epoch_loss = 0
            for batch_X, batch_y in dataloader:
                self.optimizer.zero_grad()
                output = self.model(batch_X)
                loss = self.criterion(output, torch.argmax(batch_y, dim=1))
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

    async def select_projects_ml(self, criteria: Dict) -> List[Dict[str, Any]]:
        if not self.is_trained:
            return []

        features = np.array([[
            criteria.get('carbon_intensity', 400) / 100,
            criteria.get('cost_budget', 0.5),
            criteria.get('urgency', 0.5),
            criteria.get('permanence_requirement', 0.5),
            criteria.get('co_benefit_weight', 0.5),
            criteria.get('verification_confidence', 0.5),
            criteria.get('project_age_months', 1) / 12,
            criteria.get('historical_success', 0.8)
        ]])
        features_scaled = self.scaler.transform(features)

        self.model.eval()
        with torch.no_grad():
            output = self.model(torch.FloatTensor(features_scaled)).numpy()[0]

        project_types = self.model.get_project_types()
        probabilities = [float(x) for x in output]

        recommendations = []
        for i, proj_type in enumerate(project_types):
            recommendations.append({
                'project_type': proj_type,
                'score': probabilities[i],
                'confidence': min(1.0, probabilities[i] * 1.5)
            })

        recommendations.sort(key=lambda x: x['score'], reverse=True)
        return recommendations

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
# Human-AI Collaborative Sequestration (Enhanced)
# ============================================================================

class HumanAICollaborativeSequestration:
    """Human-AI collaborative reflection for carbon sequestration."""

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
            'acknowledgment': f"Feedback received on {feedback.get('topic', 'carbon sequestration')}",
            'insights': [],
            'actions': [],
            'sequestration_insights': []
        }
        if 'concern' in feedback:
            if feedback['concern'] == 'cost':
                reflection['insights'].append("Cost optimization can be improved through project diversity")
                reflection['actions'].append("Implement cost-aware project selection")
            elif feedback['concern'] == 'permanence':
                reflection['insights'].append("Long-term permanence requires multi-decade planning")
                reflection['actions'].append("Prioritize high-permanence projects")
            elif feedback['concern'] == 'verification':
                reflection['insights'].append("Verification accuracy needs improvement")
                reflection['actions'].append("Implement enhanced verification methods")
            elif feedback['concern'] == 'helium':
                reflection['sequestration_insights'].append("Helium offset integration is needed")
                reflection['actions'].append("Implement helium sequestration projects")
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
            elif any(keyword in action.lower() for keyword in ['sequestration', 'carbon']):
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
# Persistence Manager (NEW)
# ============================================================================

class CarbonSequestrationPersistenceManager:
    """Manages persistence of sequestration state, ML model, and credits."""

    def __init__(self, config: CarbonSequestrationConfig):
        self.config = config
        self.path = config.persistence_path
        self._lock = asyncio.Lock()
        logger.info(f"CarbonSequestrationPersistenceManager initialized (path={self.path})")

    async def save_state(self, manager: 'CarbonSequestrationManager') -> bool:
        async with self._lock:
            try:
                state = {
                    'config': manager.config,
                    'credits': manager.credits,
                    'total_sequestered': manager.total_sequestered,
                    'total_offset': manager.total_offset,
                    'transaction_history': manager.transaction_history,
                    'sustainability_score': manager.sustainability_score,
                    'helium_manager_state': {
                        'helium_emissions': list(manager.helium_manager.helium_emissions),
                        'helium_offsets': list(manager.helium_manager.helium_offsets),
                        '_running_total_emissions': manager.helium_manager._running_total_emissions,
                        '_running_total_offsets': manager.helium_manager._running_total_offsets,
                    } if manager.helium_manager else None,
                    'ml_checkpoint': manager.ml_selector.get_model_checkpoint() if manager.ml_selector else None,
                }
                serialized = pickle.dumps(state)
                compressed = zlib.compress(serialized)
                with open(self.path, 'wb') as f:
                    f.write(compressed)
                logger.info(f"Sequestration state saved to {self.path}")
                return True
            except Exception as e:
                logger.error(f"Failed to save state: {e}")
                return False

    async def load_state(self, manager: 'CarbonSequestrationManager') -> bool:
        async with self._lock:
            if not os.path.exists(self.path):
                logger.warning(f"Persistence file {self.path} not found")
                return False
            try:
                with open(self.path, 'rb') as f:
                    compressed = f.read()
                serialized = zlib.decompress(compressed)
                state = pickle.loads(serialized)

                manager.credits = state.get('credits', [])
                manager.total_sequestered = state.get('total_sequestered', 0.0)
                manager.total_offset = state.get('total_offset', 0.0)
                manager.transaction_history = state.get('transaction_history', [])
                manager.sustainability_score = state.get('sustainability_score', 0.0)

                # Restore helium manager
                he_state = state.get('helium_manager_state')
                if he_state and manager.helium_manager:
                    manager.helium_manager.helium_emissions = deque(he_state.get('helium_emissions', []), maxlen=86400)
                    manager.helium_manager.helium_offsets = deque(he_state.get('helium_offsets', []), maxlen=86400)
                    manager.helium_manager._running_total_emissions = he_state.get('_running_total_emissions', 0.0)
                    manager.helium_manager._running_total_offsets = he_state.get('_running_total_offsets', 0.0)

                # Restore ML checkpoint
                ml_checkpoint = state.get('ml_checkpoint')
                if ml_checkpoint and manager.ml_selector:
                    manager.ml_selector.load_checkpoint(ml_checkpoint)

                logger.info(f"Sequestration state loaded from {self.path}")
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

class CarbonSequestrationTelemetry:
    """Collects telemetry for the carbon sequestration system."""

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
# Data Classes (Enhanced)
# ============================================================================

@dataclass
class CarbonCredit:
    credit_id: str
    amount_kg: float
    project_type: str
    verification_date: datetime
    expiry_date: datetime
    price_per_kg: float
    is_verified: bool = False
    permanence_years: float = 0.0
    co_benefits: List[str] = field(default_factory=list)
    sustainability_score: float = 0.0
    helium_offset_equivalent_l: float = 0.0

# ============================================================================
# Enhanced Carbon Sequestration Manager (Main Class)
# ============================================================================

class CarbonSequestrationManager:
    """
    Enhanced Carbon Sequestration Manager v2.1.0 - Complete Green Agent Implementation
    """

    def __init__(self, config: Optional[CarbonSequestrationConfig] = None):
        self.config = config or CarbonSequestrationConfig()

        # Feature flags
        self.enable_federated = self.config.enable_federated
        self.enable_carbon_intensity = self.config.enable_carbon_intensity
        self.enable_predictive = self.config.enable_predictive
        self.enable_ml_selection = self.config.enable_ml_selection
        self.enable_human_ai = self.config.enable_human_ai
        self.enable_helium_tracking = self.config.enable_helium_tracking
        self.enable_persistence = self.config.enable_persistence
        self.enable_telemetry = self.config.enable_telemetry

        # Core configuration
        self.offset_strategy = self.config.offset_strategy
        self.credits: List[CarbonCredit] = []
        self.sequestration_projects: Dict[str, Dict] = {}

        # Initialize sub-modules with config
        self.carbon_manager = CarbonIntensityManager(self.config) if self.enable_carbon_intensity else None
        self.helium_manager = HeliumSequestrationManager(self.config) if self.enable_helium_tracking else None
        self.predictive_analyzer = PredictiveSequestrationAnalyzer(self.config) if self.enable_predictive else None
        self.federated_manager = FederatedSequestrationManager(self.config) if self.enable_federated else None
        self.ml_selector = MLProjectSelector(self.config) if self.enable_ml_selection else None
        self.human_ai = HumanAICollaborativeSequestration() if self.enable_human_ai else None

        # Persistence and telemetry
        self.persistence = CarbonSequestrationPersistenceManager(self.config) if self.enable_persistence else None
        self.telemetry = CarbonSequestrationTelemetry() if self.enable_telemetry else None

        # Tracking
        self.total_sequestered = 0.0
        self.total_offset = 0.0
        self.transaction_history: List[Dict] = []
        self.sustainability_score = 0.0

        # Initialize projects
        self._initialize_projects()

        # Start background tasks
        self._start_background_tasks()

        # Load state if persistence enabled
        if self.enable_persistence and self.persistence:
            asyncio.create_task(self._load_state())

        logger.info(
            f"Enhanced Carbon Sequestration Manager v2.1.0 initialized with {len(self.credits)} credits, "
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
                if self.predictive_analyzer and self.transaction_history:
                    recent = self.transaction_history[-5:] if self.transaction_history else []
                    if recent:
                        offset_amount = recent[-1].get('offset_amount_kg', 0)
                        credit_price = np.mean([t.get('cost', 0) / max(t.get('offset_amount_kg', 1), 1) for t in recent[-10:]])
                        self.predictive_analyzer.update_history({
                            'offset_amount': offset_amount,
                            'credit_price': credit_price,
                            'project_success_rate': 0.9,
                            'verification_confidence': 0.7,
                            'carbon_intensity': self.carbon_manager.carbon_intensity if self.carbon_manager else 400
                        })
                    await self.predictive_analyzer.train_forecast_model()
                await asyncio.sleep(300)
            except Exception as e:
                logger.error(f"Predictive update error: {e}")
                await asyncio.sleep(60)

    async def _federated_sync_loop(self):
        while True:
            try:
                if self.federated_manager and self.transaction_history:
                    participant_id = f"sequestration_{hashlib.md5(str(self.sequestration_projects).encode()).hexdigest()[:8]}"
                    await self.federated_manager.send_local_projects(
                        participant_id,
                        {
                            'total_projects': len(self.sequestration_projects),
                            'total_sequestered': self.total_sequestered,
                            'total_offset': self.total_offset,
                            'sustainability_score': self.sustainability_score,
                            'timestamp': datetime.utcnow().isoformat()
                        },
                        performance=self.sustainability_score
                    )
                    await self.federated_manager.get_global_projects()
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
        """Report health of the sequestration system."""
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
                'total_offset': self.total_offset,
                'total_sequestered': self.total_sequestered,
                'credits_count': len(self.credits),
                'sustainability_score': self.sustainability_score
            }
        }

    def _initialize_projects(self):
        """Initialize default sequestration projects with helium integration."""
        self.sequestration_projects = {
            'reforestation_tropical': {
                'type': 'reforestation',
                'capacity_kg_per_year': 10000,
                'cost_per_kg': 0.01,
                'permanence_years': 50,
                'co_benefits': ['biodiversity', 'water_conservation'],
                'verification_method': 'satellite_imagery',
                'helium_offset_potential_l': 0.5
            },
            'direct_air_capture': {
                'type': 'dac',
                'capacity_kg_per_year': 5000,
                'cost_per_kg': 0.15,
                'permanence_years': 1000,
                'co_benefits': ['technology_development'],
                'verification_method': 'sensor_network',
                'helium_offset_potential_l': 2.0
            },
            'biochar_agriculture': {
                'type': 'biochar',
                'capacity_kg_per_year': 8000,
                'cost_per_kg': 0.05,
                'permanence_years': 100,
                'co_benefits': ['soil_health', 'crop_yield'],
                'verification_method': 'soil_sampling',
                'helium_offset_potential_l': 1.0
            },
            'ocean_alkalinization': {
                'type': 'ocean_based',
                'capacity_kg_per_year': 20000,
                'cost_per_kg': 0.08,
                'permanence_years': 10000,
                'co_benefits': ['ocean_health', 'marine_biodiversity'],
                'verification_method': 'water_sampling',
                'helium_offset_potential_l': 0.8
            },
            'helium_recovery_advanced': {
                'type': 'helium_recovery',
                'capacity_kg_per_year': 10000,
                'cost_per_kg': 0.12,
                'permanence_years': 500,
                'co_benefits': ['technology_development', 'resource_conservation'],
                'verification_method': 'sensor_network',
                'helium_offset_potential_l': 5.0
            }
        }

    def inject_bio_core(self, bio_core: Any = None, **kwargs):
        # Stub for compatibility
        pass

    # ========================================================================
    # Enhanced Offset Methods
    # ========================================================================

    async def offset_expert_emissions(
        self,
        expert_carbon_kg: float,
        budget_remaining: float,
        urgency: str = 'normal',
        use_ml_selection: bool = False
    ) -> Dict[str, Any]:
        """
        Enhanced offset emissions with ML-based project selection.
        """
        # Get carbon intensity
        carbon_intensity = 400
        if self.carbon_manager:
            carbon_intensity = await self.carbon_manager.get_current_intensity()

        # Determine offset amount based on strategy
        if self.offset_strategy == 'proactive':
            offset_amount = expert_carbon_kg * 1.2
        elif self.offset_strategy == 'reactive':
            offset_amount = expert_carbon_kg
        else:  # conservative
            offset_amount = expert_carbon_kg * 0.5

        # Select projects
        if use_ml_selection and self.enable_ml_selection and self.ml_selector:
            ml_results = await self.ml_selector.select_projects_ml({
                'carbon_intensity': carbon_intensity,
                'cost_budget': min(1.0, budget_remaining / 1000),
                'urgency': {'critical': 0.9, 'normal': 0.5, 'opportunistic': 0.2}.get(urgency, 0.5),
                'permanence_requirement': 0.6 if urgency == 'critical' else 0.4,
                'co_benefit_weight': 0.5,
                'verification_confidence': 0.7,
                'project_age_months': 12,
                'historical_success': 0.9
            })
            selected_projects = [r['project_type'] for r in ml_results[:3] if r['score'] > 0.3]
            # Map project types to project IDs
            project_map = {
                'reforestation': 'reforestation_tropical',
                'dac': 'direct_air_capture',
                'biochar': 'biochar_agriculture',
                'ocean_based': 'ocean_alkalinization',
                'helium_recovery': 'helium_recovery_advanced'
            }
            selected_projects = [project_map.get(p, p) for p in selected_projects if p in project_map]
            if not selected_projects:
                selected_projects = self._select_projects(offset_amount, urgency)
        else:
            selected_projects = self._select_projects(offset_amount, urgency)

        # Allocate offset across projects
        allocation = self._allocate_offset(offset_amount, selected_projects)

        # Execute offset
        offset_result = await self._execute_offset(allocation)

        # Create carbon credits with helium offsets
        new_credits = self._generate_credits(offset_result)
        self.credits.extend(new_credits)

        # Handle helium offsets
        if self.enable_helium_tracking and self.helium_manager:
            helium_offset = self.helium_manager.calculate_helium_offset_from_carbon(offset_amount)
            helium_project = self.helium_manager.select_helium_project(helium_offset)
            if helium_project['project_id']:
                self.helium_manager.record_helium_offset(helium_offset, helium_project['project_id'])

        # Update tracking
        self.total_offset += offset_amount
        self.total_sequestered += offset_amount * 0.1  # Simulated sequestration

        # Calculate sustainability score
        self.sustainability_score = self._calculate_sustainability_score(
            offset_amount, expert_carbon_kg, carbon_intensity
        )

        offset_plan = {
            'offset_amount_kg': offset_amount,
            'expert_emissions_kg': expert_carbon_kg,
            'over_offset_ratio': offset_amount / expert_carbon_kg if expert_carbon_kg > 0 else 0,
            'projects_used': selected_projects,
            'allocation': allocation,
            'credits_generated': len(new_credits),
            'cost': sum(p['cost'] for p in allocation.values()),
            'carbon_intensity': carbon_intensity,
            'sustainability_score': self.sustainability_score,
            'helium_offset_l': helium_offset if self.enable_helium_tracking else 0,
            'ml_used': use_ml_selection,
            'timestamp': datetime.utcnow().isoformat()
        }

        self.transaction_history.append(offset_plan)

        # Update predictive analyzer
        if self.predictive_analyzer:
            self.predictive_analyzer.update_history({
                'offset_amount': offset_amount,
                'credit_price': offset_plan['cost'] / max(offset_amount, 1),
                'project_success_rate': 0.9,
                'verification_confidence': 0.7,
                'carbon_intensity': carbon_intensity
            })
            await self.predictive_analyzer.train_forecast_model()

        # Human-AI collaboration
        if self.enable_human_ai and self.human_ai:
            insights = self.human_ai.get_collaborative_insights()
            offset_plan['human_ai_insights'] = insights

        # Telemetry
        if self.telemetry:
            self.telemetry.increment('offsets_performed')
            self.telemetry.gauge('offset_amount', offset_amount)
            self.telemetry.gauge('sustainability_score', self.sustainability_score)

        logger.info(
            f"Offset {expert_carbon_kg:.4f} kg CO2 with {offset_amount:.4f} kg "
            f"across {len(selected_projects)} projects, "
            f"sustainability_score={self.sustainability_score:.2f}"
        )

        return offset_plan

    def _calculate_sustainability_score(
        self,
        offset_amount: float,
        expert_carbon_kg: float,
        carbon_intensity: float
    ) -> float:
        offset_ratio = min(1.0, offset_amount / max(expert_carbon_kg, 1))
        carbon_factor = 1.0 - (carbon_intensity / 800)
        over_offset = min(1.0, (offset_amount - expert_carbon_kg) / max(expert_carbon_kg, 1) + 1)
        score = (offset_ratio * 0.3 + carbon_factor * 0.3 + over_offset * 0.4)
        return min(1.0, max(0.0, score))

    def _select_projects(self, amount_kg: float, urgency: str) -> List[str]:
        """Select sequestration projects based on amount and urgency."""
        scored_projects = []
        for project_id, project in self.sequestration_projects.items():
            cost_score = 1.0 / (1.0 + project['cost_per_kg'])
            capacity_score = min(project['capacity_kg_per_year'] / max(amount_kg, 1), 1.0)
            permanence_score = min(project['permanence_years'] / 1000, 1.0)
            helium_score = min(project.get('helium_offset_potential_l', 0) / 5, 1.0)

            if urgency == 'critical':
                score = 0.25 * cost_score + 0.15 * capacity_score + 0.35 * permanence_score + 0.25 * helium_score
            elif urgency == 'normal':
                score = 0.30 * cost_score + 0.25 * capacity_score + 0.25 * permanence_score + 0.20 * helium_score
            else:
                score = 0.40 * cost_score + 0.30 * capacity_score + 0.15 * permanence_score + 0.15 * helium_score

            scored_projects.append((project_id, score))

        scored_projects.sort(key=lambda x: x[1], reverse=True)

        selected = []
        total_capacity = 0
        for project_id, _ in scored_projects:
            selected.append(project_id)
            total_capacity += self.sequestration_projects[project_id]['capacity_kg_per_year']
            if total_capacity >= amount_kg:
                break

        return selected

    def _allocate_offset(self, amount_kg: float, projects: List[str]) -> Dict[str, Dict[str, Any]]:
        """Allocate offset amount across selected projects."""
        allocation = {}
        remaining = amount_kg
        sorted_projects = sorted(
            projects,
            key=lambda p: self.sequestration_projects[p]['cost_per_kg']
        )
        for project_id in sorted_projects:
            project = self.sequestration_projects[project_id]
            max_from_project = min(remaining, project['capacity_kg_per_year'] / 365)
            helium_potential = project.get('helium_offset_potential_l', 0) * max_from_project / 1000
            allocation[project_id] = {
                'amount_kg': max_from_project,
                'cost': max_from_project * project['cost_per_kg'],
                'project_type': project['type'],
                'helium_offset_potential_l': helium_potential,
                'permanence_years': project['permanence_years'],
                'co_benefits': project['co_benefits']
            }
            remaining -= max_from_project
            if remaining <= 0:
                break
        return allocation

    async def _execute_offset(self, allocation: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
        """Execute offset allocation with helium integration."""
        total_amount = sum(a['amount_kg'] for a in allocation.values())
        total_cost = sum(a['cost'] for a in allocation.values())
        total_helium = sum(a['helium_offset_potential_l'] for a in allocation.values())

        # Simulated execution with verification
        return {
            'total_amount_kg': total_amount,
            'total_cost': total_cost,
            'total_helium_offset_l': total_helium,
            'projects': allocation,
            'execution_time': datetime.utcnow().isoformat(),
            'verification_pending': True,
            'sustainability_score': self.sustainability_score
        }

    def _generate_credits(self, offset_result: Dict[str, Any]) -> List[CarbonCredit]:
        """Generate carbon credits from offset execution."""
        credits = []
        for project_id, allocation in offset_result['projects'].items():
            project = self.sequestration_projects[project_id]
            credit = CarbonCredit(
                credit_id=f"CRED-{datetime.utcnow().timestamp()}-{project_id}",
                amount_kg=allocation['amount_kg'],
                project_type=allocation['project_type'],
                verification_date=datetime.utcnow(),
                expiry_date=datetime.utcnow() + timedelta(days=365),
                price_per_kg=allocation['cost'] / allocation['amount_kg'] if allocation['amount_kg'] > 0 else 0,
                is_verified=False,
                permanence_years=project['permanence_years'],
                co_benefits=project['co_benefits'],
                sustainability_score=self.sustainability_score,
                helium_offset_equivalent_l=allocation.get('helium_offset_potential_l', 0)
            )
            credits.append(credit)
        return credits

    # ========================================================================
    # Verification Methods
    # ========================================================================

    def verify_credits(self) -> int:
        """Verify carbon credits through auditing."""
        verified_count = 0
        for credit in self.credits:
            if not credit.is_verified:
                if credit.amount_kg > 0 and credit.verification_date > datetime.utcnow() - timedelta(days=30):
                    credit.is_verified = True
                    verified_count += 1
                    if self.enable_helium_tracking and self.helium_manager:
                        self.helium_manager.record_helium_offset(
                            credit.helium_offset_equivalent_l,
                            f"credit_{credit.credit_id}"
                        )
        logger.info(f"Verified {verified_count} carbon credits")
        return verified_count

    # ========================================================================
    # Training Methods
    # ========================================================================

    async def train_ml_model(self, training_data: List[Dict] = None) -> Dict:
        """Train ML model for project selection."""
        if not self.enable_ml_selection or not self.ml_selector:
            return {'status': 'disabled'}
        if training_data is None:
            training_data = self.transaction_history[-100:] if self.transaction_history else []
        formatted_data = []
        for item in training_data:
            allocations = item.get('allocation', {})
            selected_project = list(allocations.keys())[0] if allocations else 'reforestation_tropical'
            project_map = {
                'reforestation_tropical': 'reforestation',
                'direct_air_capture': 'dac',
                'biochar_agriculture': 'biochar',
                'ocean_alkalinization': 'ocean_based',
                'helium_recovery_advanced': 'helium_recovery'
            }
            formatted_data.append({
                'carbon_intensity': item.get('carbon_intensity', 400),
                'cost_budget': min(1.0, item.get('cost', 100) / 1000),
                'urgency': 0.5,
                'permanence_requirement': 0.5,
                'co_benefit_weight': 0.5,
                'verification_confidence': 0.7,
                'project_age_months': 12,
                'historical_success': 0.9,
                'selected_project': project_map.get(selected_project, 'reforestation')
            })
        result = await self.ml_selector.train_model(formatted_data)
        logger.info(f"ML model training completed: {result}")
        return result

    async def train_predictive_model(self) -> Dict:
        """Train predictive model for offset demand."""
        if not self.enable_predictive or not self.predictive_analyzer:
            return {'status': 'disabled'}
        result = await self.predictive_analyzer.train_forecast_model()
        logger.info(f"Predictive model training completed: {result}")
        return result

    # ========================================================================
    # Reporting Methods
    # ========================================================================

    def get_carbon_portfolio(self) -> Dict[str, Any]:
        """Get comprehensive carbon portfolio status."""
        verified_credits = [c for c in self.credits if c.is_verified]
        unverified_credits = [c for c in self.credits if not c.is_verified]
        total_verified = sum(c.amount_kg for c in verified_credits)
        total_pending = sum(c.amount_kg for c in unverified_credits)

        portfolio = {
            'total_credits': len(self.credits),
            'verified_credits': len(verified_credits),
            'unverified_credits': len(unverified_credits),
            'total_verified_kg': total_verified,
            'total_pending_kg': total_pending,
            'total_offset_kg': self.total_offset,
            'total_sequestered_kg': self.total_sequestered,
            'sustainability_score': self.sustainability_score,
            'project_breakdown': {
                pid: {
                    'type': p['type'],
                    'capacity': p['capacity_kg_per_year'],
                    'cost': p['cost_per_kg'],
                    'permanence_years': p['permanence_years'],
                    'co_benefits': p['co_benefits'],
                    'helium_offset_potential_l': p.get('helium_offset_potential_l', 0)
                }
                for pid, p in self.sequestration_projects.items()
            },
            'net_carbon_impact_kg': self.total_sequestered - self.total_offset
        }

        if self.enable_helium_tracking and self.helium_manager:
            portfolio['helium_position'] = self.helium_manager.get_helium_position()

        if self.enable_federated and self.federated_manager:
            portfolio['federated_stats'] = self.federated_manager.get_federated_stats()

        if self.enable_predictive and self.predictive_analyzer:
            portfolio['predictive_forecast'] = asyncio.run(
                self.predictive_analyzer.predict_offset_demand()
            )

        if self.enable_ml_selection and self.ml_selector:
            portfolio['ml_status'] = {
                'trained': self.ml_selector.is_trained,
                'model_version': 'v2.1.0',
                'training_samples': len(self.ml_selector.training_history)
            }

        if self.enable_human_ai and self.human_ai:
            portfolio['human_ai_insights'] = self.human_ai.get_collaborative_insights()

        return portfolio

    def get_recommendation_for_expert(
        self,
        expert_carbon_per_inference: float,
        annual_inferences: int
    ) -> Dict[str, Any]:
        """Get carbon offset recommendation for specific expert."""
        annual_emissions = expert_carbon_per_inference * annual_inferences

        project_costs = []
        for pid, project in self.sequestration_projects.items():
            annual_cost = annual_emissions * project['cost_per_kg']
            helium_potential = project.get('helium_offset_potential_l', 0) * annual_emissions / 1000
            project_costs.append({
                'project_id': pid,
                'type': project['type'],
                'annual_cost': annual_cost,
                'cost_per_inference': annual_cost / annual_inferences if annual_inferences > 0 else 0,
                'co_benefits': project['co_benefits'],
                'permanence_years': project['permanence_years'],
                'helium_offset_potential_l': helium_potential,
                'sustainability_score': self.sustainability_score
            })

        project_costs.sort(key=lambda x: x['annual_cost'])

        recommendation = {
            'expert_annual_emissions_kg': annual_emissions,
            'recommended_project': project_costs[0] if project_costs else None,
            'all_options': project_costs,
            'offset_strategy': self.offset_strategy,
            'cost_effective': project_costs[0]['annual_cost'] < 100 if project_costs else False,
            'sustainability_score': self.sustainability_score,
            'recommended_helium_offset_l': project_costs[0]['helium_offset_potential_l'] if project_costs else 0
        }

        if self.enable_helium_tracking and self.helium_manager:
            helium_pos = self.helium_manager.get_helium_position()
            recommendation['helium_budget_status'] = {
                'remaining_l': helium_pos.get('remaining_budget_l', 0),
                'is_sufficient': helium_pos.get('remaining_budget_l', 0) > annual_emissions * 0.01
            }

        return recommendation

    def get_sustainability_report(self) -> Dict[str, Any]:
        """Generate comprehensive sustainability report."""
        report = {
            'timestamp': datetime.utcnow().isoformat(),
            'sustainability_score': self.sustainability_score,
            'carbon_portfolio': self.get_carbon_portfolio(),
            'helium_position': self.helium_manager.get_helium_position() if self.helium_manager else {},
            'recommendations': self._generate_sustainability_recommendations()
        }
        return report

    def _generate_sustainability_recommendations(self) -> List[str]:
        recommendations = []
        if self.sustainability_score < 0.5:
            recommendations.append("Improve carbon sequestration through project diversification")
        if self.total_offset < self.total_sequestered * 0.5:
            recommendations.append("Increase offset allocation to match sequestration capacity")
        if self.enable_helium_tracking and self.helium_manager:
            helium_pos = self.helium_manager.get_helium_position()
            if helium_pos.get('remaining_budget_l', 0) < 0:
                recommendations.append("CRITICAL: Helium budget exceeded - implement recovery systems")
        if self.enable_federated and self.federated_manager:
            if len(self.federated_manager.participants) < 2:
                recommendations.append("Increase federated participation for better project selection")
        return recommendations or ["All sustainability metrics are within acceptable ranges"]

    # ========================================================================
    # Shutdown
    # ========================================================================

    async def shutdown(self):
        """Graceful shutdown of all components."""
        logger.info("Shutting down Carbon Sequestration Manager")
        if self.enable_persistence:
            await self.save_state()
        if self.carbon_manager:
            await self.carbon_manager.close()
        if self.federated_manager:
            await self.federated_manager.close()
        logger.info("Shutdown complete")
