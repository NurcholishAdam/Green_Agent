# File: quantum_integration/quantum-limit-graph-v2.4.0/limit-agentbench/src/enhancements/moe_expert_system/advanced/biodiversity_impact_assessor.py
# Enhanced version v4.1.0 – Refactored for maintainability, concurrency, resilience, and MOPD support.

"""
Enhanced Biodiversity Impact Assessment v4.1.0 – Modular, event‑driven, robust, and MOPD‑aware.
"""

import asyncio
import logging
import json
import os
import math
import hashlib
import random
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone, timedelta
from enum import Enum
from typing import Dict, Any, List, Optional, Tuple, Deque, Callable
from collections import deque, defaultdict
import numpy as np
import aiohttp
import zlib

# Optional PyTorch
try:
    import torch
    import torch.nn as nn
    import torch.optim as optim
    from torch.utils.data import DataLoader, TensorDataset
    TORCH_AVAILABLE = True
except ImportError:
    TORCH_AVAILABLE = False
    logger = logging.getLogger(__name__)
    logger.warning("PyTorch not available; ML impact prediction will be disabled.")

# Optional sklearn
try:
    from sklearn.linear_model import SGDRegressor
    from sklearn.preprocessing import StandardScaler
    from sklearn.metrics import r2_score
    SKLEARN_AVAILABLE = True
except ImportError:
    SKLEARN_AVAILABLE = False

logger = logging.getLogger(__name__)

# ============================================================================
# Bio-Inspired Core Import (with fallback)
# ============================================================================
try:
    from enhancements.bio_inspired.__init__ import EnhancedBioInspiredCore, BioEvent, CircuitBreaker
    BIO_INSPIRED_AVAILABLE = True
except ImportError:
    BIO_INSPIRED_AVAILABLE = False
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
# MoE imports (optional)
# ============================================================================
try:
    from ..expert_router import ExpertRouter
    from ..gating_network import GatingNetworkManager
    from ..advanced.self_evolving_gates import EnhancedSelfEvolvingGate
    MOE_AVAILABLE = True
except ImportError:
    MOE_AVAILABLE = False

class HeliumProvider:
    def get_scarcity(self) -> float: raise NotImplementedError
    def get_cost_index(self) -> float: raise NotImplementedError
    def get_efficiency(self) -> float: raise NotImplementedError

# ============================================================================
# Enums and Data Classes (Enhanced with MOPD)
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
    HABITAT = "habitat"
    ENERGY = "energy"
    COOLING = "cooling"
    RESOURCES = "resources"
    CARBON = "carbon"
    HELIUM = "helium"

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
    carbon_impact: Dict[str, Any]
    helium_impact: Dict[str, Any]
    ml_prediction: Optional[Dict] = None
    timestamp: datetime

# ============================================================================
# MOPD Data Classes (NEW)
# ============================================================================
@dataclass
class MOPDPlan:
    """Represents a mitigation strategy with its objective vector."""
    # Decision variables (which mitigation strategies are chosen)
    strategy_ids: List[str]
    # Objectives (to be minimised/maximised)
    habitat_impact: float
    energy_impact: float
    cooling_impact: float
    resource_impact: float
    carbon_impact: float
    helium_impact: float
    total_impact: float
    cost: float
    implementation_time_days: int
    # Scalarised score (will be computed later)
    scalarised_score: float = 0.0

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'MOPDPlan':
        return cls(**data)

@dataclass
class MOPDConfig:
    """Configuration for MOPD analysis."""
    enabled: bool = True
    objective_weights: Dict[str, float] = field(default_factory=lambda: {
        'habitat_impact': 0.25,
        'energy_impact': 0.20,
        'cooling_impact': 0.15,
        'resource_impact': 0.15,
        'carbon_impact': 0.15,
        'helium_impact': 0.10,
    })
    grid_resolution: int = 5
    enable_cost_benefit: bool = True
    enable_predictive: bool = True
    enable_quantum: bool = True

# ============================================================================
# Enhanced Configuration with MOPD Sub‑Config
# ============================================================================
@dataclass
class CarbonConfig:
    enabled: bool = True
    region: str = "us-east"
    update_interval_seconds: int = 300
    max_retries: int = 3
    circuit_breaker_threshold: int = 5
    circuit_breaker_recovery_timeout: float = 30.0
    api_key_env: str = "ELECTRICITYMAP_API_KEY"

@dataclass
class HeliumConfig:
    enabled: bool = True
    budget_l: float = 100.0
    helium_to_co2_factor: float = 20.0
    accounting_interval_seconds: int = 60

@dataclass
class PredictiveConfig:
    enabled: bool = True
    history_window: int = 100
    update_interval_seconds: int = 300

@dataclass
class MLConfig:
    enabled: bool = True
    input_size: int = 10
    hidden_size: int = 64
    epochs: int = 100
    batch_size: int = 32
    train_interval_seconds: int = 600

@dataclass
class FederatedConfig:
    enabled: bool = True
    server_url: Optional[str] = None
    sparsity_ratio: float = 0.1
    sync_interval_seconds: int = 3600
    max_retries: int = 3

@dataclass
class TelemetryConfig:
    enabled: bool = True
    export_interval_seconds: int = 60

@dataclass
class PersistenceConfig:
    enabled: bool = True
    path: str = "biodiversity_state.json"
    save_interval_seconds: int = 300

@dataclass
class SelfHealingConfig:
    enabled: bool = True

@dataclass
class BiodiversityConfig:
    """Centralized configuration with sub‑configs."""
    # High‑level flags
    enable_bio_integration: bool = True
    enable_event_driven: bool = True
    enable_swarm_coordination: bool = True
    enable_human_ai: bool = True
    enable_cost_benefit: bool = True
    enable_time_tick_engine: bool = True
    enable_quantum_bridge: bool = True
    enable_mopd: bool = True               # NEW: MOPD feature flag

    # Sub‑configs
    carbon: CarbonConfig = field(default_factory=CarbonConfig)
    helium: HeliumConfig = field(default_factory=HeliumConfig)
    predictive: PredictiveConfig = field(default_factory=PredictiveConfig)
    ml: MLConfig = field(default_factory=MLConfig)
    federated: FederatedConfig = field(default_factory=FederatedConfig)
    telemetry: TelemetryConfig = field(default_factory=TelemetryConfig)
    persistence: PersistenceConfig = field(default_factory=PersistenceConfig)
    self_healing: SelfHealingConfig = field(default_factory=SelfHealingConfig)
    mopd: MOPDConfig = field(default_factory=MOPDConfig)      # NEW: MOPD sub‑config

    # Workflow triggers
    workflow_on_critical_impact: str = "adjust_mitigation_strategy"
    workflow_on_slo_breach: str = "relocate_computation"

    # Swarm sharing interval
    swarm_share_interval_seconds: int = 60

    # Token exchange rate (if bio‑integration)
    token_exchange_rate: float = 1000.0

# ============================================================================
# Carbon Intensity Manager (improved)
# ============================================================================
class CarbonIntensityManager:
    # ... (same as before) ...
    def __init__(self, config: CarbonConfig):
        self.config = config
        self.endpoint = "https://api.electricitymap.org/v3/carbon-intensity"
        self.region = config.region
        self.carbon_intensity = 0.0
        self.last_update: Optional[datetime] = None
        self._lock = asyncio.Lock()
        self._session: Optional[aiohttp.ClientSession] = None
        self.cache: Dict[str, Dict] = {}
        self.historical_intensities: Deque[float] = deque(maxlen=1000)
        self.api_key = os.getenv(config.api_key_env, '')
        self._circuit = CircuitBreaker(
            "carbon_api",
            failure_threshold=config.circuit_breaker_threshold,
            recovery_timeout=config.circuit_breaker_recovery_timeout
        )
        logger.info(f"CarbonIntensityManager initialized (region={self.region})")

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None:
            self._session = aiohttp.ClientSession()
        return self._session

    async def update_carbon_intensity(self, region: Optional[str] = None) -> Dict:
        if region is not None:
            self.region = region

        async def _fetch():
            # Cache check
            cache_key = f"{self.region}_{datetime.now(timezone.utc).hour}"
            if (self.last_update and
                (datetime.now(timezone.utc) - self.last_update).seconds < self.config.update_interval_seconds and
                cache_key in self.cache):
                return self.cache[cache_key]

            for attempt in range(self.config.max_retries):
                try:
                    session = await self._get_session()
                    url = f"{self.endpoint}/latest?zone={self.region}"
                    headers = {'auth-token': self.api_key} if self.api_key else {}
                    async with session.get(url, headers=headers, timeout=10) as resp:
                        if resp.status == 200:
                            data = await resp.json()
                            self.carbon_intensity = data.get('carbonIntensity', 400)
                            self.last_update = datetime.now(timezone.utc)
                            result = {
                                'intensity': self.carbon_intensity,
                                'region': self.region,
                                'timestamp': self.last_update.isoformat()
                            }
                            self.cache[cache_key] = result
                            self.historical_intensities.append(self.carbon_intensity)
                            return result
                        else:
                            logger.warning(f"Carbon API returned {resp.status}, attempt {attempt+1}")
                except Exception as e:
                    logger.error(f"Carbon API error: {e}, attempt {attempt+1}")
                await asyncio.sleep(2 ** attempt)

            # Fallback
            fallback_intensities = {'us-east': 420, 'us-west': 350, 'eu': 280, 'asia': 500}
            intensity = fallback_intensities.get(self.region, 400)
            self.carbon_intensity = intensity
            self.last_update = datetime.now(timezone.utc)
            return {'intensity': intensity, 'region': self.region, 'timestamp': self.last_update.isoformat(), 'is_fallback': True}

        return await self._circuit.call(_fetch)

    async def get_current_intensity(self) -> float:
        if self.last_update is None or (datetime.now(timezone.utc) - self.last_update).seconds > self.config.update_interval_seconds:
            await self.update_carbon_intensity(self.region)
        return self.carbon_intensity

    async def close(self):
        if self._session:
            await self._session.close()

# ============================================================================
# Helium Impact Tracker (improved)
# ============================================================================
class HeliumImpactTracker:
    # ... (same as before) ...
    def __init__(self, config: HeliumConfig):
        self.config = config
        self.budget_l = config.budget_l
        self.usage: Deque[Dict] = deque(maxlen=86400)
        self.recovered: Deque[Dict] = deque(maxlen=86400)
        self._total_usage = 0.0
        self._total_recovered = 0.0
        self._lock = asyncio.Lock()
        self._task: Optional[asyncio.Task] = None
        self._accounting_loop_running = False
        logger.info("HeliumImpactTracker initialized")

    def record_usage(self, amount_l: float, source: str = "unknown"):
        usage = {'amount_l': amount_l, 'source': source, 'timestamp': datetime.now(timezone.utc)}
        self.usage.append(usage)
        self._total_usage += amount_l

    def record_recovery(self, amount_l: float, source: str = "unknown"):
        recovery = {'amount_l': amount_l, 'source': source, 'timestamp': datetime.now(timezone.utc)}
        self.recovered.append(recovery)
        self._total_recovered += amount_l

    async def _accounting_loop(self):
        self._accounting_loop_running = True
        while self._accounting_loop_running:
            try:
                async with self._lock:
                    net = self._total_usage - self._total_recovered
                    remaining = self.budget_l - net
                    if remaining < 0:
                        logger.critical(f"Helium budget exceeded! Net: {net:.2f} L")
                    elif remaining < self.budget_l * 0.2:
                        logger.warning(f"Helium budget warning: {remaining:.2f} L remaining")
                await asyncio.sleep(self.config.accounting_interval_seconds)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Helium accounting error: {e}")
                await asyncio.sleep(5)

    def start(self):
        if not self._task:
            self._task = asyncio.create_task(self._accounting_loop())

    async def stop(self):
        if self._task:
            self._accounting_loop_running = False
            self._task.cancel()
            await self._task
            self._task = None

    def get_position(self) -> Dict[str, Any]:
        net = self._total_usage - self._total_recovered
        return {
            'total_usage_l': self._total_usage,
            'total_recovered_l': self._total_recovered,
            'net_position_l': net,
            'remaining_budget_l': self.budget_l - net,
            'co2_equivalent_kg': net * self.config.helium_to_co2_factor
        }

# ============================================================================
# Predictive Analyzer (improved)
# ============================================================================
class PredictiveBiodiversityAnalyzer:
    # ... (same as before) ...
    def __init__(self, config: PredictiveConfig):
        self.config = config
        self.history_window = config.history_window
        self.history: Deque[Dict] = deque(maxlen=config.history_window)
        self.forecasts: Deque[Dict] = deque(maxlen=50)
        self.scaler = StandardScaler() if SKLEARN_AVAILABLE else None
        self.model = None
        self.is_trained = False
        self._ml_available = SKLEARN_AVAILABLE
        self._lock = asyncio.Lock()
        if self._ml_available:
            self.model = SGDRegressor(
                learning_rate='constant',
                eta0=0.01,
                penalty='l2',
                alpha=0.0001,
                max_iter=1,
                random_state=42,
                warm_start=True
            )
            logger.info("PredictiveBiodiversityAnalyzer initialized with SGD")
        else:
            logger.warning("sklearn not available; using moving average fallback")

    def update_history(self, impact_data: Dict):
        self.history.append({
            'timestamp': datetime.now(timezone.utc),
            'total_impact': impact_data.get('total_impact', 0.5),
            'habitat_impact': impact_data.get('habitat_score', 0.5),
            'energy_impact': impact_data.get('energy_score', 0.5),
            'cooling_impact': impact_data.get('cooling_score', 0.5),
            'resource_impact': impact_data.get('resource_score', 0.5),
            'carbon_intensity': impact_data.get('carbon_intensity', 400),
            'ecosystem_sensitivity': impact_data.get('ecosystem_sensitivity', 0.5)
        })

    async def train(self) -> Dict:
        if not self._ml_available:
            return {'status': 'ml_not_available'}
        if len(self.history) < 10:
            return {'status': 'insufficient_data', 'samples': len(self.history)}

        async with self._lock:
            X, y = [], []
            hist_list = list(self.history)
            for i in range(len(hist_list) - 5):
                features = []
                for j in range(5):
                    data = hist_list[i + j]
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
                y.append(hist_list[i + 5]['total_impact'])

            X = np.array(X)
            y = np.array(y)
            if self.scaler.mean_ is None:
                X_scaled = self.scaler.fit_transform(X)
            else:
                X_scaled = self.scaler.transform(X)

            for _ in range(3):
                self.model.partial_fit(X_scaled, y)
            self.is_trained = True

            pred = self.model.predict(X_scaled)
            r2 = r2_score(y, pred) if len(y) > 5 else 0.0
            logger.info(f"Predictive model updated. R²={r2:.3f}")
            return {'status': 'success', 'r2': r2, 'samples': len(X)}

    async def predict_trend(self, hours: int = 24) -> Dict:
        if not self.is_trained or len(self.history) < 10:
            if self.history:
                recent = [h['total_impact'] for h in list(self.history)[-5:]]
                pred = np.mean(recent) if recent else 0.5
                return {'predicted_impact': pred, 'confidence': 0.3, 'trend': 'moving_average'}
            return {'predicted_impact': 0.5, 'confidence': 0.0, 'trend': 'insufficient_data'}

        recent = list(self.history)[-5:]
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
            return self.model.predict(features_scaled)[0]

        prediction = await asyncio.to_thread(predict)
        confidence = min(0.9, 0.5 + 0.4 * (len(self.history) / 100))

        if len(self.forecasts) > 5:
            recent_forecasts = list(self.forecasts)[-5:]
            trend = "improving" if prediction < recent_forecasts[-1] else "declining" if prediction > recent_forecasts[-1] else "stable"
        else:
            trend = "stable"

        self.forecasts.append({'prediction': prediction, 'trend': trend})
        return {
            'predicted_impact': prediction,
            'confidence': confidence,
            'trend': trend,
            'recommended_actions': self._generate_actions(prediction)
        }

    def _generate_actions(self, prediction: float) -> List[str]:
        if prediction > 0.7:
            return ["URGENT: Implement immediate biodiversity protection measures",
                    "Relocate computation to lower-impact areas"]
        elif prediction > 0.5:
            return ["Optimize energy and cooling strategies",
                    "Invest in habitat restoration offsets"]
        elif prediction > 0.3:
            return ["Monitor ecosystem health closely"]
        return ["Current practices are sustainable - maintain standards"]

# ============================================================================
# ML Impact Predictor (PyTorch, with thread offload)
# ============================================================================
class MLImpactPredictor:
    # ... (same as before) ...
    def __init__(self, config: MLConfig):
        self.config = config
        self.input_size = config.input_size
        self.hidden_size = config.hidden_size
        self.model = None
        self.scaler = StandardScaler() if SKLEARN_AVAILABLE else None
        self.is_trained = False
        self.optimizer = None
        self.criterion = None
        self.training_history: List[float] = []
        self._lock = asyncio.Lock()
        if TORCH_AVAILABLE:
            self._init_model()
            logger.info("MLImpactPredictor initialized with PyTorch")
        else:
            logger.warning("PyTorch not available; ML predictor disabled")

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
        self.criterion = nn.MSELoss()

    async def train(self, training_data: List[Dict], epochs: Optional[int] = None) -> Dict:
        if not TORCH_AVAILABLE or not self.model:
            return {'status': 'disabled'}
        if len(training_data) < 20:
            return {'status': 'insufficient_data', 'samples': len(training_data)}

        epochs = epochs or self.config.epochs

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

        if self.scaler is not None:
            if self.scaler.mean_ is None:
                X_scaled = self.scaler.fit_transform(X)
            else:
                X_scaled = self.scaler.transform(X)
        else:
            X_scaled = X

        dataset = TensorDataset(
            torch.FloatTensor(X_scaled),
            torch.FloatTensor(y).unsqueeze(1)
        )
        dataloader = DataLoader(dataset, batch_size=self.config.batch_size, shuffle=True)

        async with self._lock:
            # Offload training to a thread to avoid blocking event loop
            def train_sync():
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
                return losses

            losses = await asyncio.to_thread(train_sync)
            self.is_trained = True
            self.training_history.extend(losses)
            if len(self.training_history) > 1000:
                self.training_history = self.training_history[-1000:]
            return {'status': 'success', 'loss': np.mean(losses), 'samples': len(X)}

    async def predict(self, scenario: Dict) -> Dict:
        if not TORCH_AVAILABLE or not self.is_trained:
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
        if self.scaler is not None:
            features_scaled = self.scaler.transform(features)
        else:
            features_scaled = features

        self.model.eval()
        with torch.no_grad():
            prediction = self.model(torch.FloatTensor(features_scaled)).numpy()[0, 0]

        return {
            'predicted_impact': float(prediction),
            'confidence': 0.8 if self.is_trained else 0.0,
            'status': 'success'
        }

    def get_checkpoint(self) -> Dict:
        if not TORCH_AVAILABLE:
            return {}
        return {
            'state_dict': self.model.state_dict(),
            'optimizer_state': self.optimizer.state_dict(),
            'scaler_mean': self.scaler.mean_.tolist() if self.scaler.mean_ is not None else None,
            'scaler_std': self.scaler.scale_.tolist() if self.scaler.scale_ is not None else None,
            'is_trained': self.is_trained,
            'training_history': self.training_history
        }

    def load_checkpoint(self, checkpoint: Dict):
        if not TORCH_AVAILABLE or not checkpoint:
            return
        self.model.load_state_dict(checkpoint['state_dict'])
        self.optimizer.load_state_dict(checkpoint['optimizer_state'])
        if checkpoint.get('scaler_mean') is not None and self.scaler is not None:
            self.scaler.mean_ = np.array(checkpoint['scaler_mean'])
            self.scaler.scale_ = np.array(checkpoint['scaler_std'])
        self.is_trained = checkpoint.get('is_trained', False)
        self.training_history = checkpoint.get('training_history', [])

# ============================================================================
# Federated Assessor (improved)
# ============================================================================
class FederatedBiodiversityAssessor:
    # ... (same as before) ...
    def __init__(self, config: FederatedConfig):
        self.config = config
        self.server_url = config.server_url
        self.round = 0
        self.local_impacts = {}
        self.global_impacts = {}
        self.participants = []
        self.contribution_scores = {}
        self._lock = asyncio.Lock()
        self._session: Optional[aiohttp.ClientSession] = None
        self._circuit = CircuitBreaker(
            "federated_server",
            failure_threshold=3,
            recovery_timeout=30.0
        )
        logger.info("FederatedBiodiversityAssessor initialized")

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None and self.server_url:
            self._session = aiohttp.ClientSession()
        return self._session

    def _compress(self, data: Dict) -> Dict:
        if self.config.sparsity_ratio == 1.0:
            return data
        numeric = {k: v for k, v in data.items() if isinstance(v, (int, float))}
        if not numeric:
            return data
        sorted_items = sorted(numeric.items(), key=lambda x: abs(x[1]), reverse=True)
        k = max(1, int(len(sorted_items) * self.config.sparsity_ratio))
        kept = {item[0] for item in sorted_items[:k]}
        return {k: v for k, v in data.items() if k in kept or not isinstance(v, (int, float))}

    async def send_local_impact(self, participant_id: str, impact_data: Dict, performance: float = 1.0) -> Dict:
        if not self.server_url:
            return {'status': 'local'}

        async def _send():
            for attempt in range(self.config.max_retries):
                try:
                    async with self._lock:
                        session = await self._get_session()
                        compressed = self._compress(impact_data)
                        update = {
                            'participant_id': participant_id,
                            'round': self.round,
                            'impact_data': compressed,
                            'performance': performance,
                            'sparsity_ratio': self.config.sparsity_ratio,
                            'timestamp': datetime.now(timezone.utc).isoformat()
                        }
                        async with session.post(
                            f"{self.server_url}/federated/biodiversity",
                            json=update,
                            timeout=30
                        ) as resp:
                            if resp.status == 200:
                                result = await resp.json()
                                self.round += 1
                                self.contribution_scores[participant_id] = performance
                                return result
                            else:
                                logger.warning(f"Federated send failed (attempt {attempt+1}): {resp.status}")
                except Exception as e:
                    logger.error(f"Federated send error (attempt {attempt+1}): {e}")
                await asyncio.sleep(2 ** attempt)
            return {'status': 'failed'}
        return await self._circuit.call(_send)

    async def get_global_impacts(self) -> Optional[Dict]:
        if not self.server_url:
            return self.global_impacts

        async def _fetch():
            for attempt in range(self.config.max_retries):
                try:
                    async with self._lock:
                        session = await self._get_session()
                        async with session.get(
                            f"{self.server_url}/federated/biodiversity/global",
                            timeout=30
                        ) as resp:
                            if resp.status == 200:
                                data = await resp.json()
                                self.global_impacts = data.get('impacts', {})
                                self.participants = data.get('participants', [])
                                return self.global_impacts
                            else:
                                logger.warning(f"Global fetch failed (attempt {attempt+1}): {resp.status}")
                except Exception as e:
                    logger.error(f"Global fetch error (attempt {attempt+1}): {e}")
                await asyncio.sleep(2 ** attempt)
            return None
        return await self._circuit.call(_fetch)

    async def close(self):
        if self._session:
            await self._session.close()

# ============================================================================
# Human‑AI Collaborative Support (simplified)
# ============================================================================
class HumanAICollaborativeBiodiversity:
    # ... (same as before) ...
    def __init__(self):
        self.feedback_history: Deque[Dict] = deque(maxlen=1000)
        self.reflection_logs: Deque[Dict] = deque(maxlen=100)
        self.user_preferences: Dict[str, Any] = {}
        self._lock = asyncio.Lock()

    def collect_feedback(self, user_id: str, feedback: Dict) -> Dict:
        entry = {'user_id': user_id, 'timestamp': datetime.now(timezone.utc), 'feedback': feedback}
        self.feedback_history.append(entry)
        if 'preference' in feedback:
            self.user_preferences[user_id] = feedback['preference']
        reflection = self._generate_reflection(feedback)
        self.reflection_logs.append(reflection)
        return reflection

    def _generate_reflection(self, feedback: Dict) -> Dict:
        reflection = {
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'acknowledgment': f"Feedback received on {feedback.get('topic', 'biodiversity impact')}",
            'insights': [],
            'actions': [],
            'biodiversity_insights': []
        }
        concern = feedback.get('concern')
        if concern == 'habitat':
            reflection['insights'].append("Habitat impact can be reduced through location optimization")
            reflection['actions'].append("Relocate computation to lower-impact areas")
        elif concern == 'energy':
            reflection['insights'].append("Energy source significantly affects biodiversity")
            reflection['actions'].append("Switch to renewable energy sources")
        elif concern == 'cooling':
            reflection['insights'].append("Cooling method impacts local water ecosystems")
            reflection['actions'].append("Implement water-free cooling solutions")
        elif concern == 'biodiversity':
            reflection['biodiversity_insights'].append("Biodiversity impact requires holistic assessment")
            reflection['actions'].append("Implement comprehensive biodiversity monitoring")
        if 'suggestion' in feedback:
            reflection['actions'].append(f"Implementing suggestion: {feedback['suggestion']}")
        reflection['action_items'] = self._prioritize_actions(reflection['actions'])
        return reflection

    def _prioritize_actions(self, actions: List[str]) -> List[Dict]:
        priorities = []
        for action in actions:
            if any(kw in action.lower() for kw in ['urgent', 'critical', 'immediate']):
                priority, impact, effort = 'high', 0.9, 'high'
            elif any(kw in action.lower() for kw in ['biodiversity', 'habitat', 'ecosystem']):
                priority, impact, effort = 'high', 0.8, 'medium'
            elif any(kw in action.lower() for kw in ['carbon', 'energy']):
                priority, impact, effort = 'medium', 0.6, 'medium'
            else:
                priority, impact, effort = 'low', 0.3, 'low'
            priorities.append({
                'action': action,
                'priority': priority,
                'impact': impact,
                'estimated_effort': effort,
                'biodiversity_weight': impact
            })
        return sorted(priorities, key=lambda x: (x['impact'], x['biodiversity_weight']), reverse=True)

    async def get_insights(self) -> Dict:
        if len(self.feedback_history) < 5:
            return {'status': 'insufficient_feedback'}
        recent = list(self.feedback_history)[-20:]
        topics = defaultdict(int)
        concerns = defaultdict(int)
        for f in recent:
            topics[f['feedback'].get('topic', 'general')] += 1
            if 'concern' in f['feedback']:
                concerns[f['feedback']['concern']] += 1
        return {
            'total_feedback': len(self.feedback_history),
            'top_topics': dict(topics),
            'top_concerns': dict(concerns),
            'engagement_score': min(1.0, len(self.feedback_history) / 100),
            'user_count': len(set(f['user_id'] for f in self.feedback_history))
        }

# ============================================================================
# Persistence Manager (JSON with versioning)
# ============================================================================
class BiodiversityPersistenceManager:
    # ... (same as before) ...
    def __init__(self, config: PersistenceConfig):
        self.config = config
        self.path = config.path
        self._lock = asyncio.Lock()
        self._version = 2  # Bumped for MOPD
        logger.info(f"BiodiversityPersistenceManager initialized (path={self.path})")

    async def save_state(self, state: Dict[str, Any]) -> bool:
        async with self._lock:
            try:
                payload = {
                    'version': self._version,
                    'timestamp': datetime.now(timezone.utc).isoformat(),
                    'data': self._make_serializable(state)
                }
                with open(self.path, 'w') as f:
                    json.dump(payload, f, indent=2)
                logger.info(f"State saved to {self.path}")
                return True
            except Exception as e:
                logger.error(f"Failed to save state: {e}")
                return False

    async def load_state(self) -> Optional[Dict]:
        async with self._lock:
            if not os.path.exists(self.path):
                logger.warning(f"Persistence file {self.path} not found")
                return None
            try:
                with open(self.path, 'r') as f:
                    payload = json.load(f)
                if payload.get('version') != self._version:
                    logger.warning(f"State version mismatch; may be incompatible")
                return self._deserialize(payload.get('data', {}))
            except Exception as e:
                logger.error(f"Failed to load state: {e}")
                return None

    def _make_serializable(self, obj: Any) -> Any:
        if isinstance(obj, dict):
            return {k: self._make_serializable(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [self._make_serializable(v) for v in obj]
        elif isinstance(obj, datetime):
            return obj.isoformat()
        elif isinstance(obj, (deque, set)):
            return self._make_serializable(list(obj))
        elif hasattr(obj, '__dict__'):
            return self._make_serializable(obj.__dict__)
        else:
            return obj

    def _deserialize(self, obj: Any) -> Any:
        if isinstance(obj, dict):
            return {k: self._deserialize(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [self._deserialize(v) for v in obj]
        elif isinstance(obj, str):
            try:
                return datetime.fromisoformat(obj)
            except ValueError:
                return obj
        else:
            return obj

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
class BiodiversityTelemetry:
    # ... (same as before) ...
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
        async with self._lock:
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
# Biodiversity Storage (holds data)
# ============================================================================
class BiodiversityStorage:
    def __init__(self):
        self.ecosystems: Dict[str, BiodiversityMetric] = {}
        self.impact_history: List[BiodiversityAssessment] = []
        self.mitigation_strategies: Dict[str, List[Dict]] = {}
        self.local_score = 0.0
        self.global_score = 0.0
        self.sustainability_score = 0.0
        self.total_carbon_savings_kg = 0.0
        self.total_helium_savings_l = 0.0
        self._lock = asyncio.Lock()

    async def add_assessment(self, assessment: BiodiversityAssessment):
        async with self._lock:
            self.impact_history.append(assessment)
            if len(self.impact_history) > 1000:
                self.impact_history = self.impact_history[-1000:]

    async def update_scores(self, local: float, global_: float, sustainability: float):
        async with self._lock:
            self.local_score = local
            self.global_score = global_
            self.sustainability_score = sustainability

    async def get_ecosystems(self) -> Dict[str, BiodiversityMetric]:
        async with self._lock:
            return dict(self.ecosystems)

    async def set_ecosystem(self, name: str, metric: BiodiversityMetric):
        async with self._lock:
            self.ecosystems[name] = metric

    async def get_impact_history(self, limit: int = 50) -> List[BiodiversityAssessment]:
        async with self._lock:
            return self.impact_history[-limit:]

    async def get_scores(self) -> Dict[str, float]:
        async with self._lock:
            return {
                'local': self.local_score,
                'global': self.global_score,
                'sustainability': self.sustainability_score
            }

# ============================================================================
# Biodiversity Analyzer (performs assessments)
# ============================================================================
class BiodiversityAnalyzer:
    def __init__(self, config: BiodiversityConfig, storage: BiodiversityStorage,
                 carbon_manager: Optional[CarbonIntensityManager] = None,
                 helium_tracker: Optional[HeliumImpactTracker] = None,
                 predictive: Optional[PredictiveBiodiversityAnalyzer] = None,
                 ml_predictor: Optional[MLImpactPredictor] = None):
        self.config = config
        self.storage = storage
        self.carbon_manager = carbon_manager
        self.helium_tracker = helium_tracker
        self.predictive = predictive
        self.ml_predictor = ml_predictor
        self._lock = asyncio.Lock()

    async def assess_expert_impact(
        self,
        expert_type: str,
        location: Dict[str, Any],
        energy_source: str,
        cooling_method: str,
        use_ml_prediction: bool = False,
        return_mopd: bool = False           # NEW: if True, return Pareto front of mitigation strategies
    ) -> Dict[str, Any]:
        # Get carbon intensity
        carbon_intensity = 400
        if self.carbon_manager:
            carbon_intensity = await self.carbon_manager.get_current_intensity()

        # Perform sub‑assessments
        habitat = self._assess_habitat(location)
        energy = self._assess_energy(energy_source, location)
        cooling = self._assess_cooling(cooling_method, location)
        resources = self._assess_resources(expert_type)
        carbon = self._assess_carbon(energy_source, location, carbon_intensity)
        helium = self._assess_helium(cooling_method, location)

        # Aggregate
        breakdown = {
            'habitat': habitat,
            'energy': energy,
            'cooling': cooling,
            'resources': resources,
            'carbon': carbon,
            'helium': helium
        }
        total = (habitat['score'] + energy['score'] + cooling['score'] +
                 resources['score'] + carbon['score'] + helium['score']) / 6.0

        # ML prediction
        ml_pred = None
        if self.ml_predictor and use_ml_prediction:
            ml_pred = await self.ml_predictor.predict({
                'carbon_intensity': carbon_intensity,
                'energy_intensity': energy['score'],
                'cooling_intensity': cooling['score'],
                'resource_intensity': resources['score'],
                'ecosystem_sensitivity': habitat.get('sensitivity', 0.5),
                'proximity_factor': habitat.get('proximity_factor', 0.5),
                'fragmentation_index': habitat.get('fragmentation_index', 0.5),
                'species_density': 0.5,
                'water_scarcity': location.get('water_scarcity_index', 0.5),
                'temperature_anomaly': 0.5
            })

        # Mitigation and recommendations
        mitigation = self._generate_mitigation(breakdown, expert_type, location)
        recommendations = self._generate_recommendations(breakdown)

        # Sustainability score
        sustainability = self._calc_sustainability(breakdown, total, carbon_intensity)

        assessment = BiodiversityAssessment(
            assessment_id=hashlib.md5(f"{expert_type}{location}{datetime.now(timezone.utc)}".encode()).hexdigest()[:12],
            expert_type=expert_type,
            location=location,
            total_impact=total,
            impact_breakdown=breakdown,
            mitigation_strategies=mitigation,
            recommendations=recommendations,
            sustainability_score=sustainability,
            carbon_impact=carbon,
            helium_impact=helium,
            ml_prediction=ml_pred,
            timestamp=datetime.now(timezone.utc)
        )

        # Store and update scores
        await self.storage.add_assessment(assessment)
        await self._update_scores(assessment)

        # Update predictive history
        if self.predictive:
            self.predictive.update_history({
                'total_impact': total,
                'habitat_score': habitat['score'],
                'energy_score': energy['score'],
                'cooling_score': cooling['score'],
                'resource_score': resources['score'],
                'carbon_intensity': carbon_intensity,
                'ecosystem_sensitivity': habitat.get('sensitivity', 0.5)
            })

        # Track helium
        if self.helium_tracker:
            helium_usage_l = helium['score'] * 10  # simplistic
            self.helium_tracker.record_usage(helium_usage_l, expert_type)

        # MOPD: generate Pareto front of mitigation strategies if enabled
        mopd_result = None
        if self.config.enable_mopd and return_mopd:
            mopd_result = await self._generate_mitigation_pareto_front(breakdown, expert_type, location)

        result = asdict(assessment)
        if mopd_result:
            result['mopd_pareto_front'] = [p.to_dict() for p in mopd_result['pareto_front']]
            result['mopd_best_plan'] = mopd_result['best_plan'].to_dict() if mopd_result['best_plan'] else None
        return result

    # ============================================================================
    # MOPD Methods (NEW)
    # ============================================================================
    async def _generate_mitigation_pareto_front(
        self,
        breakdown: Dict,
        expert_type: str,
        location: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Generate Pareto front of mitigation strategy combinations."""
        # Define available mitigation strategies (each with effects on each objective)
        strategies = [
            {
                'id': 'habitat_relocation',
                'effect': {'habitat_impact': -0.6, 'energy_impact': 0.0, 'cooling_impact': 0.0, 'resource_impact': 0.0, 'carbon_impact': 0.0, 'helium_impact': 0.0},
                'cost': 5.0, 'implementation_time': 3
            },
            {
                'id': 'renewable_energy',
                'effect': {'habitat_impact': 0.0, 'energy_impact': -0.7, 'cooling_impact': 0.0, 'resource_impact': 0.0, 'carbon_impact': -0.5, 'helium_impact': 0.0},
                'cost': 3.0, 'implementation_time': 5
            },
            {
                'id': 'efficient_cooling',
                'effect': {'habitat_impact': 0.0, 'energy_impact': -0.2, 'cooling_impact': -0.5, 'resource_impact': 0.0, 'carbon_impact': -0.2, 'helium_impact': -0.3},
                'cost': 2.5, 'implementation_time': 4
            },
            {
                'id': 'circular_economy',
                'effect': {'habitat_impact': 0.0, 'energy_impact': 0.0, 'cooling_impact': 0.0, 'resource_impact': -0.4, 'carbon_impact': -0.1, 'helium_impact': 0.0},
                'cost': 1.5, 'implementation_time': 2
            },
            {
                'id': 'carbon_offset',
                'effect': {'habitat_impact': 0.0, 'energy_impact': 0.0, 'cooling_impact': 0.0, 'resource_impact': 0.0, 'carbon_impact': -0.3, 'helium_impact': 0.0},
                'cost': 2.0, 'implementation_time': 1
            },
            {
                'id': 'helium_recovery',
                'effect': {'habitat_impact': 0.0, 'energy_impact': 0.0, 'cooling_impact': 0.0, 'resource_impact': 0.0, 'carbon_impact': 0.0, 'helium_impact': -0.5},
                'cost': 4.0, 'implementation_time': 6
            }
        ]

        # Current scores for each objective
        base = {
            'habitat_impact': breakdown['habitat']['score'],
            'energy_impact': breakdown['energy']['score'],
            'cooling_impact': breakdown['cooling']['score'],
            'resource_impact': breakdown['resources']['score'],
            'carbon_impact': breakdown['carbon']['score'],
            'helium_impact': breakdown['helium']['score']
        }

        # Generate all combinations of strategies (subset of strategies)
        # For simplicity, we generate combinations of up to 3 strategies
        import itertools
        plans = []
        for r in range(0, min(4, len(strategies) + 1)):
            for combo in itertools.combinations(strategies, r):
                # Apply effects to base
                plan_vals = base.copy()
                total_cost = 0.0
                total_time = 0
                strategy_ids = []
                for strat in combo:
                    for key, effect in strat['effect'].items():
                        plan_vals[key] = max(0.0, plan_vals[key] + effect)
                    total_cost += strat['cost']
                    total_time = max(total_time, strat['implementation_time'])
                    strategy_ids.append(strat['id'])
                # Calculate total impact as average of objectives (for now)
                plan_total = np.mean(list(plan_vals.values()))
                plan = MOPDPlan(
                    strategy_ids=strategy_ids,
                    habitat_impact=plan_vals['habitat_impact'],
                    energy_impact=plan_vals['energy_impact'],
                    cooling_impact=plan_vals['cooling_impact'],
                    resource_impact=plan_vals['resource_impact'],
                    carbon_impact=plan_vals['carbon_impact'],
                    helium_impact=plan_vals['helium_impact'],
                    total_impact=plan_total,
                    cost=total_cost,
                    implementation_time_days=total_time
                )
                plans.append(plan)

        # Filter dominated plans using dominance check
        objective_names = ['habitat_impact', 'energy_impact', 'cooling_impact', 'resource_impact', 'carbon_impact', 'helium_impact']
        # We minimise all objectives (lower is better)
        pareto = []
        for i, p_i in enumerate(plans):
            dominated = False
            for j, p_j in enumerate(plans):
                if i == j:
                    continue
                a_vec = [getattr(p_i, key) for key in objective_names]
                b_vec = [getattr(p_j, key) for key in objective_names]
                if all(b <= a for a, b in zip(a_vec, b_vec)) and any(b < a for a, b in zip(a_vec, b_vec)):
                    dominated = True
                    break
            if not dominated:
                pareto.append(p_i)

        # Select best plan using scalarisation with weights
        best_plan = self._select_best_from_pareto(pareto)

        return {'pareto_front': pareto, 'best_plan': best_plan}

    def _select_best_from_pareto(self, pareto_front: List[MOPDPlan]) -> Optional[MOPDPlan]:
        if not pareto_front:
            return None
        weights = self.config.mopd.objective_weights
        # Normalise objectives across Pareto front
        objective_names = ['habitat_impact', 'energy_impact', 'cooling_impact', 'resource_impact', 'carbon_impact', 'helium_impact']
        max_vals = {key: max(getattr(p, key) for p in pareto_front) for key in objective_names}
        min_vals = {key: min(getattr(p, key) for p in pareto_front) for key in objective_names}
        ranges = {key: max_vals[key] - min_vals[key] if max_vals[key] != min_vals[key] else 1.0 for key in objective_names}

        best = None
        best_score = -float('inf')
        for plan in pareto_front:
            score = 0.0
            for key in objective_names:
                val = getattr(plan, key)
                norm = 1.0 - (val - min_vals[key]) / ranges[key] if ranges[key] > 0 else 1.0
                weight = weights.get(key, 1.0 / len(objective_names))
                score += weight * norm
            if score > best_score:
                best_score = score
                best = plan
        return best

    # ============================================================================
    # Assessment Sub‑functions (same as before)
    # ============================================================================
    def _assess_habitat(self, location: Dict[str, Any]) -> Dict[str, Any]:
        nearest = self._find_nearest_ecosystem(location)
        if not nearest:
            return {'score': 0.1, 'category': 'minimal', 'ecosystem': None}
        distance = location.get('distance_to_ecosystem_km', 100)
        if distance < 1:
            proximity = 1.0
        elif distance < 10:
            proximity = 0.7
        elif distance < 50:
            proximity = 0.3
        else:
            proximity = 0.1
        ecosystem = self.storage.ecosystems.get(nearest)
        if ecosystem:
            sensitivity = min(ecosystem.endangered_species_count / 200.0, 1.0)
            fragmentation = ecosystem.fragmentation_index
        else:
            sensitivity = 0.5
            fragmentation = 0.5
        score = proximity * 0.4 + sensitivity * 0.4 + fragmentation * 0.2
        return {
            'score': score,
            'category': 'critical' if score > 0.7 else 'moderate' if score > 0.3 else 'low',
            'ecosystem': nearest,
            'proximity_factor': proximity,
            'sensitivity': sensitivity,
            'fragmentation_index': fragmentation
        }

    def _assess_energy(self, energy_source: str, location: Dict[str, Any]) -> Dict[str, Any]:
        factors = {
            'solar': 0.05, 'wind': 0.08, 'hydroelectric': 0.15,
            'geothermal': 0.03, 'nuclear': 0.10, 'natural_gas': 0.40,
            'coal': 0.80, 'oil': 0.90, 'biomass': 0.30,
            'mixed_grid': 0.35
        }
        base = factors.get(energy_source, 0.5)
        if location.get('near_water_body'):
            if energy_source in ['hydroelectric', 'nuclear']:
                base *= 1.5
        if location.get('in_migration_corridor'):
            if energy_source in ['wind']:
                base *= 1.3
        return {'score': base, 'energy_source': energy_source,
                'category': 'high' if base > 0.5 else 'moderate' if base > 0.2 else 'low'}

    def _assess_cooling(self, cooling_method: str, location: Dict[str, Any]) -> Dict[str, Any]:
        factors = {
            'air_cooling': 0.05, 'evaporative_cooling': 0.15,
            'water_cooling': 0.25, 'helium_cooling': 0.10,
            'geothermal_cooling': 0.03, 'liquid_immersion': 0.20,
            'free_cooling': 0.02
        }
        base = factors.get(cooling_method, 0.15)
        if cooling_method in ['water_cooling', 'evaporative_cooling']:
            scarcity = location.get('water_scarcity_index', 0)
            if scarcity > 0.7:
                base *= 2.0
            elif scarcity > 0.4:
                base *= 1.5
        if cooling_method in ['water_cooling', 'liquid_immersion']:
            if location.get('near_water_body'):
                base *= 1.3
        return {'score': base, 'cooling_method': cooling_method,
                'category': 'high' if base > 0.5 else 'moderate' if base > 0.2 else 'low'}

    def _assess_resources(self, expert_type: str) -> Dict[str, Any]:
        impacts = {
            'energy_expert': {'rare_earth': 0.1, 'copper': 0.05, 'overall': 0.08},
            'data_expert': {'rare_earth': 0.15, 'copper': 0.1, 'overall': 0.12},
            'iot_expert': {'rare_earth': 0.05, 'copper': 0.02, 'overall': 0.04},
            'quantum_expert': {'rare_earth': 0.3, 'copper': 0.2, 'overall': 0.25},
            'helium_expert': {'rare_earth': 0.08, 'copper': 0.05, 'overall': 0.06}
        }
        overall = impacts.get(expert_type, {'overall': 0.1})['overall']
        return {'score': overall, 'expert_type': expert_type,
                'category': 'high' if overall > 0.2 else 'moderate' if overall > 0.1 else 'low'}

    def _assess_carbon(self, energy_source: str, location: Dict[str, Any], carbon_intensity: float) -> Dict[str, Any]:
        factors = {
            'solar': 0.02, 'wind': 0.03, 'hydroelectric': 0.05,
            'geothermal': 0.01, 'nuclear': 0.04, 'natural_gas': 0.35,
            'coal': 0.70, 'oil': 0.80, 'biomass': 0.25,
            'mixed_grid': 0.30
        }
        base = factors.get(energy_source, 0.3)
        carbon_factor = carbon_intensity / 400.0
        score = base * carbon_factor
        if location.get('near_carbon_sensitive_ecosystem'):
            score *= 1.3
        return {'score': min(score, 1.0), 'energy_source': energy_source,
                'carbon_intensity': carbon_intensity,
                'category': 'high' if score > 0.5 else 'moderate' if score > 0.2 else 'low'}

    def _assess_helium(self, cooling_method: str, location: Dict[str, Any]) -> Dict[str, Any]:
        factors = {
            'helium_cooling': 0.25,
            'water_cooling': 0.05,
            'air_cooling': 0.02,
            'evaporative_cooling': 0.08,
            'geothermal_cooling': 0.01,
            'liquid_immersion': 0.10,
            'free_cooling': 0.01
        }
        base = factors.get(cooling_method, 0.05)
        if location.get('near_helium_mining_region'):
            base *= 2.0
        return {'score': min(base, 1.0), 'cooling_method': cooling_method,
                'category': 'high' if base > 0.2 else 'moderate' if base > 0.05 else 'low'}

    def _find_nearest_ecosystem(self, location: Dict[str, Any]) -> Optional[str]:
        if location.get('latitude', 0) < 0:
            return 'amazon_rainforest'
        elif location.get('latitude', 0) > 45:
            return 'european_wetlands'
        else:
            return 'coral_reef_pacific'

    def _generate_mitigation(self, breakdown: Dict, expert_type: str, location: Dict) -> List[Dict]:
        strategies = []
        if breakdown['habitat']['score'] > 0.5:
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
        if breakdown['energy']['score'] > 0.3:
            strategies.append({
                'type': 'renewable_energy',
                'action': 'Switch to renewable energy sources',
                'impact_reduction': 0.7,
                'cost': 'medium',
                'implementation_time': 'medium'
            })
        if breakdown['cooling']['score'] > 0.3:
            strategies.append({
                'type': 'efficient_cooling',
                'action': 'Implement free cooling or geothermal cooling',
                'impact_reduction': 0.5,
                'cost': 'medium',
                'implementation_time': 'medium'
            })
        if breakdown['resources']['score'] > 0.15:
            strategies.append({
                'type': 'circular_economy',
                'action': 'Use recycled materials and extend hardware life',
                'impact_reduction': 0.4,
                'cost': 'low',
                'implementation_time': 'short'
            })
        if breakdown['carbon']['score'] > 0.3:
            strategies.append({
                'type': 'carbon_offset',
                'action': 'Implement carbon offset program',
                'impact_reduction': 0.3,
                'cost': 'medium',
                'implementation_time': 'medium'
            })
        if breakdown['helium']['score'] > 0.1:
            strategies.append({
                'type': 'helium_recovery',
                'action': 'Implement helium recovery and recycling',
                'impact_reduction': 0.5,
                'cost': 'high',
                'implementation_time': 'long'
            })
        return strategies

    def _generate_recommendations(self, breakdown: Dict) -> List[str]:
        recommendations = []
        scores = {cat: data['score'] for cat, data in breakdown.items() if 'score' in data}
        highest = max(scores.items(), key=lambda x: x[1]) if scores else ('none', 0)
        if highest[0] == 'habitat' and highest[1] > 0.5:
            recommendations.append("HIGH PRIORITY: Relocate computation to avoid sensitive ecosystems")
        elif highest[0] == 'energy' and highest[1] > 0.5:
            recommendations.append("HIGH PRIORITY: Switch to renewable energy")
        elif highest[0] == 'cooling' and highest[1] > 0.5:
            recommendations.append("HIGH PRIORITY: Implement water-free cooling")
        elif highest[0] == 'carbon' and highest[1] > 0.5:
            recommendations.append("HIGH PRIORITY: Reduce carbon emissions")
        elif highest[0] == 'helium' and highest[1] > 0.3:
            recommendations.append("HIGH PRIORITY: Implement helium recovery")
        if all(s < 0.2 for s in scores.values()):
            recommendations.append("Current setup has minimal biodiversity impact")
        else:
            recommendations.append("Consider biodiversity offsets equivalent to 110% of calculated impact")
        return recommendations

    def _calc_sustainability(self, breakdown: Dict, total: float, carbon_intensity: float) -> float:
        weights = {'habitat': 0.25, 'energy': 0.20, 'cooling': 0.15,
                   'resources': 0.15, 'carbon': 0.15, 'helium': 0.10}
        score = 1.0
        for cat, data in breakdown.items():
            if cat in weights:
                score -= data['score'] * weights[cat]
        carbon_factor = 1.0 - (carbon_intensity / 800)
        score = score * 0.7 + carbon_factor * 0.3
        return max(0.0, min(1.0, score))

    async def _update_scores(self, assessment: BiodiversityAssessment):
        alpha = 0.1
        async with self.storage._lock:
            self.storage.local_score = (1 - alpha) * self.storage.local_score + alpha * assessment.total_impact
            self.storage.global_score = (1 - alpha * 0.5) * self.storage.global_score + alpha * 0.5 * assessment.total_impact
            self.storage.sustainability_score = (1 - alpha) * self.storage.sustainability_score + alpha * assessment.sustainability_score

    async def get_routing_guidance(self, expert_options: List[str], location_options: List[Dict]) -> Dict:
        assessments = []
        for expert in expert_options:
            for loc in location_options:
                loc_sens = loc.get('biodiversity_sensitivity', 0.5)
                expert_int = {
                    'energy': 0.3, 'data': 0.4, 'iot': 0.2,
                    'quantum': 0.6, 'helium': 0.35
                }.get(expert, 0.4)
                impact = loc_sens * expert_int
                assessments.append({'expert': expert, 'location': loc.get('name', 'unknown'), 'estimated_impact': impact})
        assessments.sort(key=lambda x: x['estimated_impact'])
        best = assessments[0] if assessments else None
        worst = assessments[-1] if assessments else None
        return {
            'best_option': best,
            'worst_option': worst,
            'all_options': assessments,
            'recommendation': f"Use {best['expert']} at {best['location']}" if best else "No options",
            'sustainability_score': self.storage.sustainability_score,
            'biodiversity_impact_reduction': (worst['estimated_impact'] - best['estimated_impact']) / max(worst['estimated_impact'], 0.001) if best and worst else 0
        }

# ============================================================================
# Biodiversity Reporter (reporting, telemetry, persistence, and MOPD reporting)
# ============================================================================
class BiodiversityReporter:
    def __init__(self, config: BiodiversityConfig, storage: BiodiversityStorage, analyzer: BiodiversityAnalyzer,
                 telemetry: Optional[BiodiversityTelemetry] = None,
                 persistence: Optional[BiodiversityPersistenceManager] = None,
                 human_ai: Optional[HumanAICollaborativeBiodiversity] = None):
        self.config = config
        self.storage = storage
        self.analyzer = analyzer
        self.telemetry = telemetry
        self.persistence = persistence
        self.human_ai = human_ai
        self._lock = asyncio.Lock()

    async def generate_report(self) -> Dict[str, Any]:
        scores = await self.storage.get_scores()
        ecosystems = await self.storage.get_ecosystems()
        recent = await self.storage.get_impact_history(10)
        report = {
            'local_biodiversity_score': scores['local'],
            'global_biodiversity_score': scores['global'],
            'sustainability_score': scores['sustainability'],
            'total_carbon_savings_kg': self.storage.total_carbon_savings_kg,
            'total_helium_savings_l': self.storage.total_helium_savings_l,
            'ecosystems_tracked': len(ecosystems),
            'recent_impacts': [
                {
                    'expert_type': a.expert_type,
                    'impact': a.total_impact,
                    'sustainability_score': a.sustainability_score,
                    'timestamp': a.timestamp.isoformat()
                }
                for a in recent
            ],
            'high_risk_ecosystems': [
                name for name, eco in ecosystems.items()
                if eco.endangered_species_count > 50
            ],
            'mitigation_effectiveness': self._calc_mitigation_effectiveness(),
            'recommendations': self._generate_global_recommendations()
        }
        if self.analyzer.predictive:
            forecast = await self.analyzer.predictive.predict_trend()
            report['predictive_forecast'] = forecast
        if self.human_ai:
            report['human_ai_insights'] = await self.human_ai.get_insights()
        # MOPD summary
        if self.config.enable_mopd:
            # Use a sample assessment to generate Pareto front (if any)
            history = await self.storage.get_impact_history(1)
            if history:
                sample = history[0]
                mopd_result = await self.analyzer._generate_mitigation_pareto_front(
                    sample.impact_breakdown, sample.expert_type, sample.location
                )
                report['mopd_pareto_front_size'] = len(mopd_result['pareto_front'])
                if mopd_result['best_plan']:
                    report['mopd_best_plan'] = mopd_result['best_plan'].to_dict()
        return report

    def _calc_mitigation_effectiveness(self) -> float:
        history = self.storage.impact_history
        if len(history) < 20:
            return 0.5
        recent = history[-20:]
        historical = history[:-20]
        if not historical:
            return 0.5
        recent_avg = np.mean([a.total_impact for a in recent])
        historical_avg = np.mean([a.total_impact for a in historical])
        if historical_avg > 0:
            return max((historical_avg - recent_avg) / historical_avg, 0.0)
        return 0.0

    def _generate_global_recommendations(self) -> List[str]:
        recs = []
        if self.storage.local_score > 0.5:
            recs.append("CRITICAL: Implement immediate biodiversity protection measures")
        if any(eco.endangered_species_count > 100 for eco in self.storage.ecosystems.values()):
            recs.append("URGENT: Avoid computing operations near critical habitats")
        if self.storage.sustainability_score < 0.5:
            recs.append("IMPROVE: Overall sustainability score needs improvement")
        recs.append("Implement helium recovery systems to reduce mining impact")
        recs.append("Monitor carbon intensity and optimize energy sources")
        return recs

    async def export_telemetry(self):
        if self.telemetry:
            data = await self.telemetry.export()
            logger.debug(f"Telemetry export: {len(data)} bytes")

    async def save_state(self):
        if self.persistence:
            state = {
                'ecosystems': self.storage.ecosystems,
                'impact_history': self.storage.impact_history,
                'mitigation_strategies': self.storage.mitigation_strategies,
                'local_score': self.storage.local_score,
                'global_score': self.storage.global_score,
                'sustainability_score': self.storage.sustainability_score,
                'total_carbon_savings_kg': self.storage.total_carbon_savings_kg,
                'total_helium_savings_l': self.storage.total_helium_savings_l,
                'ml_checkpoint': self.analyzer.ml_predictor.get_checkpoint() if self.analyzer.ml_predictor else None,
                # MOPD not persisted yet (optional)
            }
            await self.persistence.save_state(state)

    async def load_state(self):
        if self.persistence:
            state = await self.persistence.load_state()
            if state:
                self.storage.ecosystems = state.get('ecosystems', {})
                self.storage.impact_history = state.get('impact_history', [])
                self.storage.mitigation_strategies = state.get('mitigation_strategies', {})
                self.storage.local_score = state.get('local_score', 0.0)
                self.storage.global_score = state.get('global_score', 0.0)
                self.storage.sustainability_score = state.get('sustainability_score', 0.0)
                self.storage.total_carbon_savings_kg = state.get('total_carbon_savings_kg', 0.0)
                self.storage.total_helium_savings_l = state.get('total_helium_savings_l', 0.0)
                ml_cp = state.get('ml_checkpoint')
                if ml_cp and self.analyzer.ml_predictor:
                    self.analyzer.ml_predictor.load_checkpoint(ml_cp)

# ============================================================================
# Main Controller
# ============================================================================
class BiodiversityImpactAssessor:
    """
    Enhanced Biodiversity Impact Assessor v4.1.0 – Controller that orchestrates
    storage, analysis, reporting, event handling, and MOPD support.
    """

    def __init__(
        self,
        bio_core: Optional[EnhancedBioInspiredCore] = None,
        config: Optional[BiodiversityConfig] = None,
        **kwargs
    ):
        if config is None:
            config = BiodiversityConfig(**{k: v for k, v in kwargs.items() if k in BiodiversityConfig.__annotations__})
        self.config = config

        # Bio‑core references
        self.bio_core = bio_core
        self.event_broker = getattr(bio_core, 'event_broker', None) if bio_core else None
        self.self_healer = getattr(bio_core, 'self_healer', None) if bio_core else None
        self.workflow_orchestrator = getattr(bio_core, 'workflow_orchestrator', None) if bio_core else None
        self.swarm_coordinator = getattr(bio_core, 'swarm_coordinator', None) if bio_core else None
        self.token_manager = getattr(bio_core, 'token_manager', None) if bio_core else None
        self.gradient_manager = getattr(bio_core, 'gradient_manager', None) if bio_core else None

        # Sub‑modules
        self.carbon_manager = CarbonIntensityManager(self.config.carbon) if self.config.carbon.enabled else None
        self.helium_tracker = HeliumImpactTracker(self.config.helium) if self.config.helium.enabled else None
        self.predictive = PredictiveBiodiversityAnalyzer(self.config.predictive) if self.config.predictive.enabled else None
        self.ml_predictor = MLImpactPredictor(self.config.ml) if self.config.ml.enabled else None
        self.federated = FederatedBiodiversityAssessor(self.config.federated) if self.config.federated.enabled else None
        self.human_ai = HumanAICollaborativeBiodiversity() if self.config.enable_human_ai else None
        self.telemetry = BiodiversityTelemetry() if self.config.telemetry.enabled else None
        self.persistence = BiodiversityPersistenceManager(self.config.persistence) if self.config.persistence.enabled else None

        # Storage, Analyzer, Reporter
        self.storage = BiodiversityStorage()
        self.analyzer = BiodiversityAnalyzer(
            config, self.storage, self.carbon_manager, self.helium_tracker,
            self.predictive, self.ml_predictor
        )
        self.reporter = BiodiversityReporter(
            config, self.storage, self.analyzer, self.telemetry,
            self.persistence, self.human_ai
        )

        # MoE injectables
        self.expert_router = None
        self.gating_network = None
        self.self_evolving_gate = None
        self.helium_provider = None

        # Event queue
        self._event_queue: asyncio.Queue = asyncio.Queue()
        self._event_consumer_task: Optional[asyncio.Task] = None

        # Background tasks
        self._background_tasks: List[asyncio.Task] = []

        # Health
        self.health_status = "healthy"
        self.last_error: Optional[str] = None

        # Initialize ecosystems (default or from config)
        self._initialize_ecosystems()

        # Subscribe to events
        if self.config.enable_event_driven and self.event_broker:
            self._subscribe_events()

        # Start background tasks
        self._start_background_tasks()

        # Load state
        if self.config.persistence.enabled:
            asyncio.create_task(self._load_state())

        logger.info("BiodiversityImpactAssessor v4.1.0 initialized with MOPD")

    # ============================================================================
    # Event Handling (via queue)
    # ============================================================================
    def _subscribe_events(self):
        if self.event_broker:
            self.event_broker.subscribe('carbon_update', self._enqueue_event)
            self.event_broker.subscribe('helium_update', self._enqueue_event)
            self.event_broker.subscribe('alert_generated', self._enqueue_event)
            self.event_broker.subscribe('config_updated', self._enqueue_event)
            self.event_broker.subscribe('token_balance_update', self._enqueue_event)
            self.event_broker.subscribe('health_update', self._enqueue_event)
            self.event_broker.subscribe('anomaly_detected', self._enqueue_event)
            logger.info("Subscribed to core events via queue")

    async def _enqueue_event(self, event: BioEvent):
        await self._event_queue.put(event)

    async def _event_consumer(self):
        while True:
            try:
                event = await self._event_queue.get()
                await self._handle_event(event)
                self._event_queue.task_done()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Event consumer error: {e}")

    async def _handle_event(self, event: BioEvent):
        handler = getattr(self, f"_on_{event.event_type}", None)
        if handler:
            try:
                await handler(event)
            except Exception as e:
                logger.error(f"Error handling event {event.event_type}: {e}")

    async def _on_carbon_update(self, event: BioEvent):
        intensity = event.data.get('intensity', 400)
        if self.carbon_manager:
            self.carbon_manager.carbon_intensity = intensity
        for eco in self.storage.ecosystems.values():
            eco.carbon_sensitivity = 0.5 + 0.5 * (intensity / 800)

    async def _on_helium_update(self, event: BioEvent):
        scarcity = event.data.get('scarcity', 0.5)
        if self.helium_tracker:
            self.helium_tracker.budget_l = 100.0 * (1.0 - scarcity * 0.3)
        for eco in self.storage.ecosystems.values():
            eco.helium_sensitivity = 0.5 + 0.5 * scarcity

    async def _on_alert_generated(self, event: BioEvent):
        if event.data.get('severity') == 'critical':
            logger.warning("Critical alert; triggering self‑healing")
            if self.config.self_healing.enabled and self.self_healer:
                await self.self_healer.apply_healing('damage_accumulation')
            if self.workflow_orchestrator and self.config.workflow_on_critical_impact:
                await self.workflow_orchestrator.execute_workflow(self.config.workflow_on_critical_impact)

    async def _on_config_updated(self, event: BioEvent):
        updates = event.data.get('updates', {})
        if 'biodiversity_assessor' in updates:
            new = updates['biodiversity_assessor']
            for key, value in new.items():
                if hasattr(self.config, key):
                    setattr(self.config, key, value)
            logger.info("Configuration reloaded")

    async def _on_token_update(self, event: BioEvent):
        pass

    async def _on_health_update(self, event: BioEvent):
        self.health_status = event.data.get('status', 'healthy')

    async def _on_anomaly_detected(self, event: BioEvent):
        if event.data.get('metric') == 'carbon_intensity':
            for eco in self.storage.ecosystems.values():
                eco.carbon_sensitivity = min(1.0, eco.carbon_sensitivity * 1.2)
        if event.data.get('metric') == 'helium_scarcity':
            for eco in self.storage.ecosystems.values():
                eco.helium_sensitivity = min(1.0, eco.helium_sensitivity * 1.2)

    # ============================================================================
    # Background Tasks (cancellable)
    # ============================================================================
    def _start_background_tasks(self):
        if self.config.enable_event_driven:
            self._event_consumer_task = asyncio.create_task(self._event_consumer())
            self._background_tasks.append(self._event_consumer_task)

        if self.carbon_manager:
            t = asyncio.create_task(self._carbon_update_loop())
            self._background_tasks.append(t)

        if self.helium_tracker:
            self.helium_tracker.start()

        if self.predictive:
            t = asyncio.create_task(self._predictive_update_loop())
            self._background_tasks.append(t)

        if self.ml_predictor:
            t = asyncio.create_task(self._ml_training_loop())
            self._background_tasks.append(t)

        if self.federated:
            t = asyncio.create_task(self._federated_sync_loop())
            self._background_tasks.append(t)

        if self.telemetry:
            t = asyncio.create_task(self._telemetry_export_loop())
            self._background_tasks.append(t)

        if self.persistence:
            t = asyncio.create_task(self._persistence_save_loop())
            self._background_tasks.append(t)

        if self.config.enable_swarm_coordination and self.swarm_coordinator:
            t = asyncio.create_task(self._swarm_update_loop())
            self._background_tasks.append(t)

    async def _carbon_update_loop(self):
        while True:
            try:
                if self.carbon_manager:
                    await self.carbon_manager.update_carbon_intensity()
                    if self.telemetry:
                        intensity = await self.carbon_manager.get_current_intensity()
                        self.telemetry.gauge('carbon_intensity', intensity)
                await asyncio.sleep(self.config.carbon.update_interval_seconds)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Carbon update error: {e}")
                await asyncio.sleep(60)

    async def _predictive_update_loop(self):
        while True:
            try:
                if self.predictive and self.storage.impact_history:
                    await self.predictive.train()
                await asyncio.sleep(self.config.predictive.update_interval_seconds)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Predictive update error: {e}")
                await asyncio.sleep(60)

    async def _ml_training_loop(self):
        while True:
            try:
                if self.ml_predictor:
                    history = await self.storage.get_impact_history(200)
                    if len(history) >= 20:
                        training_data = []
                        for a in history:
                            bd = a.impact_breakdown
                            training_data.append({
                                'carbon_intensity': a.carbon_impact.get('carbon_intensity', 400),
                                'energy_intensity': bd.get('energy', {}).get('score', 0.5),
                                'cooling_intensity': bd.get('cooling', {}).get('score', 0.5),
                                'resource_intensity': bd.get('resources', {}).get('score', 0.5),
                                'ecosystem_sensitivity': bd.get('habitat', {}).get('sensitivity', 0.5),
                                'proximity_factor': bd.get('habitat', {}).get('proximity_factor', 0.5),
                                'fragmentation_index': bd.get('habitat', {}).get('fragmentation_index', 0.5),
                                'species_density': 0.5,
                                'water_scarcity': a.location.get('water_scarcity_index', 0.5),
                                'temperature_anomaly': 0.5,
                                'total_impact': a.total_impact
                            })
                        await self.ml_predictor.train(training_data)
                await asyncio.sleep(self.config.ml.train_interval_seconds)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"ML training error: {e}")
                await asyncio.sleep(60)

    async def _federated_sync_loop(self):
        while True:
            try:
                if self.federated:
                    scores = await self.storage.get_scores()
                    pid = f"biodiversity_{hashlib.md5(str(self.storage.ecosystems).encode()).hexdigest()[:8]}"
                    await self.federated.send_local_impact(
                        pid,
                        {
                            'local_score': scores['local'],
                            'global_score': scores['global'],
                            'total_impact': self.storage.impact_history[-1].total_impact if self.storage.impact_history else 0.5,
                            'timestamp': datetime.now(timezone.utc).isoformat()
                        },
                        performance=scores['sustainability']
                    )
                    await self.federated.get_global_impacts()
                await asyncio.sleep(self.config.federated.sync_interval_seconds)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Federated sync error: {e}")
                await asyncio.sleep(300)

    async def _telemetry_export_loop(self):
        while True:
            try:
                await self.reporter.export_telemetry()
                await asyncio.sleep(self.config.telemetry.export_interval_seconds)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Telemetry export error: {e}")
                await asyncio.sleep(60)

    async def _persistence_save_loop(self):
        while True:
            try:
                await self.reporter.save_state()
                await asyncio.sleep(self.config.persistence.save_interval_seconds)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Persistence save error: {e}")
                await asyncio.sleep(60)

    async def _swarm_update_loop(self):
        while True:
            try:
                await self.share_with_swarm()
                await asyncio.sleep(self.config.swarm_share_interval_seconds)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Swarm update error: {e}")
                await asyncio.sleep(120)

    # ============================================================================
    # Public API
    # ============================================================================
    async def assess_expert_impact(
        self,
        expert_type: str,
        location: Dict[str, Any],
        energy_source: str,
        cooling_method: str,
        use_ml_prediction: bool = False,
        return_mopd: bool = False           # NEW: if True, return Pareto front
    ) -> Dict[str, Any]:
        assessment = await self.analyzer.assess_expert_impact(
            expert_type, location, energy_source, cooling_method, use_ml_prediction, return_mopd
        )
        # Trigger workflows if critical
        if assessment['total_impact'] > 0.8 and self.workflow_orchestrator:
            await self.workflow_orchestrator.execute_workflow(self.config.workflow_on_critical_impact)

        # Feed to gating network / self‑evolving gate
        if self.gating_network and self.expert_router:
            features = np.array([
                assessment['total_impact'],
                (assessment['carbon_impact'].get('carbon_intensity', 400) / 800),
                assessment['sustainability_score'],
                len(assessment['mitigation_strategies'])
            ])
            reward = 1.0 - assessment['total_impact']
            self.gating_network.update(features, reward, {'expert_type': expert_type})

        if self.self_evolving_gate and TORCH_AVAILABLE:
            state = torch.tensor([assessment['total_impact'], assessment['sustainability_score']], dtype=torch.float32)
            self.self_evolving_gate.adapt(
                state=state,
                chosen_expert=0,
                reward=1.0 - assessment['total_impact'],
                environmental_feedback={'expert_type': expert_type},
                quantum_mode=False
            )

        if self.telemetry:
            self.telemetry.increment('assessments_performed')
            self.telemetry.gauge('total_impact', assessment['total_impact'])
            self.telemetry.gauge('sustainability_score', assessment['sustainability_score'])
            if return_mopd and 'mopd_pareto_front' in assessment:
                self.telemetry.increment('mopd_generations')
                self.telemetry.histogram('mopd_pareto_front_size', len(assessment['mopd_pareto_front']))

        logger.info(f"Assessment for {expert_type}: impact={assessment['total_impact']:.2f}, sustainability={assessment['sustainability_score']:.2f}")
        return assessment

    async def get_biodiversity_report(self) -> Dict[str, Any]:
        return await self.reporter.generate_report()

    async def get_routing_guidance(self, expert_options: List[str], location_options: List[Dict]) -> Dict:
        return await self.analyzer.get_routing_guidance(expert_options, location_options)

    async def train_ml_model(self, training_data: Optional[List[Dict]] = None) -> Dict:
        if not self.ml_predictor:
            return {'status': 'disabled'}
        if training_data is None:
            history = await self.storage.get_impact_history(200)
            training_data = []
            for a in history:
                bd = a.impact_breakdown
                training_data.append({
                    'carbon_intensity': a.carbon_impact.get('carbon_intensity', 400),
                    'energy_intensity': bd.get('energy', {}).get('score', 0.5),
                    'cooling_intensity': bd.get('cooling', {}).get('score', 0.5),
                    'resource_intensity': bd.get('resources', {}).get('score', 0.5),
                    'ecosystem_sensitivity': bd.get('habitat', {}).get('sensitivity', 0.5),
                    'proximity_factor': bd.get('habitat', {}).get('proximity_factor', 0.5),
                    'fragmentation_index': bd.get('habitat', {}).get('fragmentation_index', 0.5),
                    'species_density': 0.5,
                    'water_scarcity': a.location.get('water_scarcity_index', 0.5),
                    'temperature_anomaly': 0.5,
                    'total_impact': a.total_impact
                })
        return await self.ml_predictor.train(training_data)

    async def train_predictive_model(self) -> Dict:
        if not self.predictive:
            return {'status': 'disabled'}
        return await self.predictive.train()

    # ============================================================================
    # MOPD Public Methods (NEW)
    # ============================================================================
    async def get_mitigation_pareto_front(
        self,
        expert_type: str,
        location: Dict[str, Any],
        energy_source: str,
        cooling_method: str,
        use_ml_prediction: bool = False
    ) -> List[MOPDPlan]:
        """
        Generate Pareto front of mitigation strategies for a hypothetical scenario.
        Returns a list of MOPDPlan objects.
        """
        if not self.config.enable_mopd:
            return []
        # Perform quick assessment to get breakdown
        assessment = await self.analyzer.assess_expert_impact(
            expert_type, location, energy_source, cooling_method, use_ml_prediction, return_mopd=True
        )
        if 'mopd_pareto_front' in assessment:
            return [MOPDPlan.from_dict(p) for p in assessment['mopd_pareto_front']]
        return []

    async def get_mopd_summary(self) -> Dict[str, Any]:
        """Return a summary of MOPD‑related metrics."""
        if not self.config.enable_mopd:
            return {'enabled': False}
        return {
            'enabled': True,
            'objective_weights': self.config.mopd.objective_weights,
            'grid_resolution': self.config.mopd.grid_resolution,
            'strategies_available': 6,  # hardcoded for now
        }

    # ============================================================================
    # Swarm Coordination
    # ============================================================================
    async def share_with_swarm(self):
        if not self.config.enable_swarm_coordination or not self.swarm_coordinator:
            return
        scores = await self.storage.get_scores()
        payload = {
            'assessor_id': hashlib.md5(str(self.storage.ecosystems).encode()).hexdigest()[:8],
            'local_biodiversity_score': scores['local'],
            'global_biodiversity_score': scores['global'],
            'sustainability_score': scores['sustainability'],
            'total_carbon_savings_kg': self.storage.total_carbon_savings_kg,
            'total_helium_savings_l': self.storage.total_helium_savings_l,
            'ecosystems_tracked': len(self.storage.ecosystems),
            'mopd_enabled': self.config.enable_mopd,
        }
        await self.swarm_coordinator.share_predictions(payload)

    # ============================================================================
    # Injection Methods
    # ============================================================================
    def set_gating_network(self, gating_network: 'GatingNetworkManager'):
        self.gating_network = gating_network

    def set_self_evolving_gate(self, gate: 'EnhancedSelfEvolvingGate'):
        self.self_evolving_gate = gate

    def set_expert_router(self, router: 'ExpertRouter'):
        self.expert_router = router

    def set_helium_provider(self, provider: HeliumProvider):
        self.helium_provider = provider

    def inject_bio_core(self, bio_core: Any = None, **kwargs):
        pass

    # ============================================================================
    # Self‑Healing
    # ============================================================================
    async def self_heal(self):
        logger.info("Self‑healing started")
        if not self.config.self_healing.enabled:
            logger.warning("Self‑healing disabled")
            return
        self._initialize_ecosystems()
        async with self.storage._lock:
            self.storage.local_score = 0.0
            self.storage.global_score = 0.0
            self.storage.sustainability_score = 0.0
            self.storage.total_carbon_savings_kg = 0.0
            self.storage.total_helium_savings_l = 0.0
            self.storage.impact_history.clear()
            self.storage.mitigation_strategies.clear()
        self.health_status = "healthy"
        self.last_error = None
        await self.reporter.save_state()
        logger.info("Self‑healing completed")

    # ============================================================================
    # Health Status
    # ============================================================================
    async def get_health_status(self) -> Dict[str, Any]:
        scores = await self.storage.get_scores()
        return {
            'status': self.health_status,
            'last_error': self.last_error,
            'local_biodiversity_score': scores['local'],
            'global_biodiversity_score': scores['global'],
            'sustainability_score': scores['sustainability'],
            'ecosystems_tracked': len(self.storage.ecosystems),
            'bio_integration_active': self.config.enable_bio_integration,
            'event_driven_active': self.config.enable_event_driven,
            'self_healing_enabled': self.config.self_healing.enabled,
            'persistence_enabled': self.config.persistence.enabled,
            'mopd_enabled': self.config.enable_mopd,
        }

    # ============================================================================
    # Helper
    # ============================================================================
    def _initialize_ecosystems(self):
        defaults = {
            'amazon_rainforest': BiodiversityMetric(
                ecosystem_type=EcosystemType.TROPICAL_FOREST,
                species_richness=10000,
                endangered_species_count=200,
                habitat_area_km2=5500000,
                fragmentation_index=0.3,
                ecological_connectivity=0.7,
                last_assessment=datetime.now(timezone.utc)
            ),
            'coral_reef_pacific': BiodiversityMetric(
                ecosystem_type=EcosystemType.MARINE,
                species_richness=5000,
                endangered_species_count=150,
                habitat_area_km2=200000,
                fragmentation_index=0.4,
                ecological_connectivity=0.5,
                last_assessment=datetime.now(timezone.utc)
            ),
            'european_wetlands': BiodiversityMetric(
                ecosystem_type=EcosystemType.WETLAND,
                species_richness=2000,
                endangered_species_count=80,
                habitat_area_km2=15000,
                fragmentation_index=0.6,
                ecological_connectivity=0.4,
                last_assessment=datetime.now(timezone.utc)
            )
        }
        self.storage.ecosystems = defaults

    async def _load_state(self):
        await self.reporter.load_state()

    # ============================================================================
    # Shutdown
    # ============================================================================
    async def shutdown(self):
        logger.info("Shutting down Biodiversity Impact Assessor")
        for task in self._background_tasks:
            task.cancel()
        await asyncio.gather(*self._background_tasks, return_exceptions=True)

        if self.helium_tracker:
            await self.helium_tracker.stop()

        if self.persistence:
            await self.reporter.save_state()

        if self.carbon_manager:
            await self.carbon_manager.close()
        if self.federated:
            await self.federated.close()

        logger.info("Shutdown complete")
