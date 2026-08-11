# File: quantum_integration/quantum-limit-graph-v2.4.0/limit-agentbench/src/enhancements/moe_expert_system/advanced/automated_carbon_offset_verification.py
# Enhanced version v4.1.0 – Refactored for maintainability, concurrency, resilience, and MOPD support.

"""
Enhanced Automated Carbon Offset Verification System v4.1.0
Modular, event‑driven, robust, and MOPD‑aware implementation.
"""

import asyncio
import logging
import json
import os
import hashlib
import math
import random
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone, timedelta
from enum import Enum
from typing import Dict, Any, List, Optional, Tuple, Deque, Callable
from collections import defaultdict, deque
import numpy as np
import aiohttp
import zlib

# Optional torch
try:
    import torch
    import torch.nn as nn
    import torch.optim as optim
    from torch.utils.data import DataLoader, TensorDataset
    TORCH_AVAILABLE = True
except ImportError:
    TORCH_AVAILABLE = False
    logger = logging.getLogger(__name__)
    logger.warning("PyTorch not available; ML verification will be disabled.")

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

# ============================================================================
# Helium Provider Interface
# ============================================================================
class HeliumProvider:
    def get_scarcity(self) -> float: raise NotImplementedError
    def get_cost_index(self) -> float: raise NotImplementedError
    def get_efficiency(self) -> float: raise NotImplementedError

# ============================================================================
# Enums and Data Classes (Enhanced with MOPD)
# ============================================================================
class OffsetRegistry(Enum):
    VERRA = "verra"
    GOLD_STANDARD = "gold_standard"
    CLIMATE_ACTION_RESERVE = "climate_action_reserve"
    AMERICAN_CARBON_REGISTRY = "american_carbon_registry"
    PURO_EARTH = "puro_earth"

class ProjectType(Enum):
    REFORESTATION = "reforestation"
    AFFORESTATION = "afforestation"
    RENEWABLE_ENERGY = "renewable_energy"
    ENERGY_EFFICIENCY = "energy_efficiency"
    CARBON_CAPTURE = "carbon_capture"
    BIOCHAR = "biochar"
    BLUE_CARBON = "blue_carbon"

class VerificationStatus(Enum):
    PENDING = "pending"
    VERIFIED = "verified"
    REJECTED = "rejected"
    RETIRED = "retired"

class AdditionalityLevel(Enum):
    NO_ADDITIONALITY = "no_additionality"
    UNLIKELY_ADDITIONAL = "unlikely_additional"
    POSSIBLY_ADDITIONAL = "possibly_additional"
    LIKELY_ADDITIONAL = "likely_additional"
    PROVEN_ADDITIONAL = "proven_additional"

class PermanenceRisk(Enum):
    VERY_LOW = "very_low"
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    VERY_HIGH = "very_high"

@dataclass
class CarbonCredit:
    credit_id: str
    registry: OffsetRegistry
    project_type: ProjectType
    amount_kg: float
    effective_amount: float  # after buffer deductions
    vintage_year: int
    verification_status: VerificationStatus
    additionality: AdditionalityLevel
    permanence_risk: PermanenceRisk
    project_location: Dict[str, float]
    verification_date: datetime
    expiry_date: datetime
    retirement_date: Optional[datetime] = None

@dataclass
class SatelliteVerification:
    verification_id: str
    project_id: str
    satellite_source: str
    image_date: datetime
    ndvi_mean: float
    ndvi_change: float
    forest_cover_percent: float
    deforestation_detected: bool
    project_boundary_violation: bool
    carbon_sequestration_estimate_kg: float
    confidence_score: float
    anomaly_detected: bool
    sustainability_impact: float

@dataclass
class SensorValidation:
    validation_id: str
    project_id: str
    sensor_id: str
    sensor_type: str
    measurements: List[float]
    mean_value: float
    standard_deviation: float
    expected_range: Tuple[float, float]
    within_expected_range: bool
    data_quality_score: float
    cryptographic_signature: str
    helium_correlation: float

@dataclass
class AdditionalityAssessment:
    assessment_id: str
    project_id: str
    financial_additionality: bool
    regulatory_additionality: bool
    barrier_analysis: Dict[str, bool]
    common_practice_analysis: bool
    counterfactual_scenario: str
    overall_assessment: AdditionalityLevel
    confidence_score: float
    assessor: str
    sustainability_score: float

@dataclass
class RealTimeCarbonAccount:
    account_id: str
    timestamp: datetime
    scope1_emissions_kg: float
    scope2_emissions_kg: float
    scope3_emissions_kg: float
    verified_offsets_kg: float
    pending_offsets_kg: float
    net_position_kg: float
    carbon_budget_remaining_kg: float
    budget_status: str  # "compliant", "warning", "exceeded"

# ============================================================================
# MOPD Data Classes (NEW)
# ============================================================================
@dataclass
class MOPDPlan:
    """Represents a carbon offset strategy with its objective vector."""
    # Decision variables
    credit_id: str
    registry: OffsetRegistry
    project_type: ProjectType
    amount_to_retire_kg: float
    use_ml_verification: bool
    verify_satellite: bool
    verify_sensors: bool
    verify_additionality: bool
    # Objectives (to be minimised/maximised)
    cost: float
    carbon_savings_kg: float
    helium_impact_l: float
    verification_confidence: float
    verification_time_ms: float
    sustainability_score: float
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
        'cost': 0.2,
        'carbon_savings': 0.3,
        'helium_impact': 0.2,
        'verification_confidence': 0.15,
        'verification_time': 0.15,
    })
    grid_resolution: int = 5
    enable_cost_benefit: bool = True
    enable_predictive: bool = True
    enable_quantum: bool = True

# ============================================================================
# Configuration Dataclass with Sub‑Configs (Enhanced with MOPD)
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
    path: str = "carbon_offset_state.json"
    save_interval_seconds: int = 300

@dataclass
class SelfHealingConfig:
    enabled: bool = True

@dataclass
class CarbonOffsetConfig:
    """Centralized configuration with sub‑configs."""
    # High‑level flags
    enable_blockchain: bool = True
    enable_satellite: bool = True
    enable_sensors: bool = True
    enable_additionality: bool = True
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

    # Budgets
    carbon_budget_kg: float = 1000.0
    helium_budget_l: float = 100.0

    # Workflow triggers
    workflow_on_critical_alert: str = "adjust_offset_strategy"
    workflow_on_slo_breach: str = "rebalance_carbon_budget"

    # Swarm sharing interval
    swarm_share_interval_seconds: int = 60

    # Helium-to-CO2 equivalence factor (kg CO2 per kg helium)
    helium_to_co2_factor: float = 20.0

    # Retry parameters
    max_retries: int = 3
    retry_base_delay_ms: float = 100.0
    retry_max_delay_ms: float = 5000.0
    circuit_breaker_failure_threshold: int = 5
    circuit_breaker_recovery_timeout: float = 30.0

# ============================================================================
# Carbon Intensity Manager (Improved)
# ============================================================================
class CarbonIntensityManager:
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
# Helium Emission Tracker (Improved)
# ============================================================================
class HeliumEmissionTracker:
    def __init__(self, config: HeliumConfig):
        self.config = config
        self.budget_l = config.budget_l
        self.emissions: Deque[Dict] = deque(maxlen=86400)
        self.offsets: Deque[Dict] = deque(maxlen=86400)
        self._total_emissions = 0.0
        self._total_offsets = 0.0
        self._lock = asyncio.Lock()
        self._task: Optional[asyncio.Task] = None
        self._accounting_loop_running = False
        logger.info("HeliumEmissionTracker initialized")

    def record_emission(self, amount_l: float, source: str = "unknown"):
        self.emissions.append({'amount_l': amount_l, 'source': source, 'timestamp': datetime.now(timezone.utc)})
        self._total_emissions += amount_l

    def record_offset(self, amount_l: float, verified: bool = False):
        self.offsets.append({'amount_l': amount_l, 'verified': verified, 'timestamp': datetime.now(timezone.utc)})
        self._total_offsets += amount_l

    async def _accounting_loop(self):
        self._accounting_loop_running = True
        while self._accounting_loop_running:
            try:
                async with self._lock:
                    net = self._total_emissions - self._total_offsets
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
        net = self._total_emissions - self._total_offsets
        return {
            'total_emissions_l': self._total_emissions,
            'total_offsets_l': self._total_offsets,
            'net_position_l': net,
            'remaining_budget_l': self.budget_l - net,
            'co2_equivalent_kg': net * self.config.helium_to_co2_factor
        }

    def calculate_helium_offset_from_carbon(self, carbon_credit_kg: float) -> float:
        return carbon_credit_kg * 0.05

# ============================================================================
# Predictive Offset Analyzer (Improved)
# ============================================================================
class PredictiveOffsetAnalyzer:
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
            logger.info("PredictiveOffsetAnalyzer initialized with SGD")
        else:
            logger.warning("sklearn not available; using moving average fallback")

    def update_history(self, offset_data: Dict):
        self.history.append({
            'timestamp': datetime.now(timezone.utc),
            'price': offset_data.get('price', 50),
            'volume': offset_data.get('volume', 1000),
            'verification_rate': offset_data.get('verification_rate', 0.9),
            'market_confidence': offset_data.get('market_confidence', 0.7),
            'carbon_intensity': offset_data.get('carbon_intensity', 400)
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
                        data['price'] / 100,
                        data['volume'] / 1000,
                        data['verification_rate'],
                        data['market_confidence'],
                        data['carbon_intensity'] / 100
                    ])
                X.append(features)
                y.append(hist_list[i + 5]['price'])

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

    async def predict_price(self) -> Dict:
        if not self.is_trained or len(self.history) < 10:
            if self.history:
                recent = [h['price'] for h in list(self.history)[-5:]]
                pred = np.mean(recent) if recent else 50
                return {'predicted_price': pred, 'confidence': 0.3, 'trend': 'moving_average'}
            return {'predicted_price': 50, 'confidence': 0.0, 'trend': 'insufficient_data'}

        recent = list(self.history)[-5:]
        features = []
        for data in recent:
            features.extend([
                data['price'] / 100,
                data['volume'] / 1000,
                data['verification_rate'],
                data['market_confidence'],
                data['carbon_intensity'] / 100
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
            trend = "increasing" if prediction > recent_forecasts[-1] else "decreasing" if prediction < recent_forecasts[-1] else "stable"
        else:
            trend = "stable"

        self.forecasts.append({'prediction': prediction, 'trend': trend})
        return {
            'predicted_price': prediction,
            'confidence': confidence,
            'trend': trend,
            'recommended_actions': self._generate_actions(prediction)
        }

    def _generate_actions(self, prediction: float) -> List[str]:
        if prediction > 60:
            return ["Sell carbon credits at premium price", "Increase verification efforts"]
        elif prediction < 40:
            return ["Purchase carbon credits at discount", "Hold offset positions"]
        return ["Maintain current offset strategy"]

# ============================================================================
# ML Verification Engine (PyTorch, with thread offload)
# ============================================================================
class MLVerificationEngine:
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
            logger.info("MLVerificationEngine initialized with PyTorch")
        else:
            logger.warning("PyTorch not available; ML verification disabled")

    def _init_model(self):
        class VerificationPredictor(nn.Module):
            def __init__(self, input_size, hidden_size):
                super().__init__()
                self.network = nn.Sequential(
                    nn.Linear(input_size, hidden_size),
                    nn.ReLU(),
                    nn.BatchNorm1d(hidden_size),
                    nn.Linear(hidden_size, hidden_size // 2),
                    nn.ReLU(),
                    nn.BatchNorm1d(hidden_size // 2),
                    nn.Linear(hidden_size // 2, 2)  # Success probability, confidence
                )
            def forward(self, x):
                return self.network(x)

        self.model = VerificationPredictor(self.input_size, self.hidden_size)
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
                item.get('satellite_confidence', 0.5),
                item.get('sensor_quality', 0.5),
                item.get('additionality_score', 0.5),
                item.get('permanence_risk', 0.5),
                item.get('registry_trust', 0.5),
                item.get('project_age_years', 1),
                item.get('area_km2', 1) / 100,
                item.get('verification_effort', 0.5),
                item.get('historical_success', 0.8)
            ])
            y.append([item.get('verification_success', 0.5), item.get('confidence', 0.7)])

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
            torch.FloatTensor(y)
        )
        dataloader = DataLoader(dataset, batch_size=self.config.batch_size, shuffle=True)

        async with self._lock:
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

    async def verify(self, project_data: Dict) -> Dict:
        if not TORCH_AVAILABLE or not self.is_trained:
            return {'verification_success': 0.5, 'confidence': 0.0, 'status': 'model_not_trained'}

        features = np.array([[
            project_data.get('carbon_intensity', 400) / 100,
            project_data.get('satellite_confidence', 0.5),
            project_data.get('sensor_quality', 0.5),
            project_data.get('additionality_score', 0.5),
            project_data.get('permanence_risk', 0.5),
            project_data.get('registry_trust', 0.5),
            project_data.get('project_age_years', 1),
            project_data.get('area_km2', 1) / 100,
            project_data.get('verification_effort', 0.5),
            project_data.get('historical_success', 0.8)
        ]])
        if self.scaler is not None:
            features_scaled = self.scaler.transform(features)
        else:
            features_scaled = features

        self.model.eval()
        with torch.no_grad():
            output = self.model(torch.FloatTensor(features_scaled)).numpy()[0]

        return {
            'verification_success': float(output[0]),
            'confidence': float(output[1]),
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
# Federated Carbon Verifier (Improved)
# ============================================================================
class FederatedCarbonVerifier:
    def __init__(self, config: FederatedConfig):
        self.config = config
        self.server_url = config.server_url
        self.round = 0
        self.local_verifications = {}
        self.global_verifications = {}
        self.participants = []
        self.contribution_scores = {}
        self._lock = asyncio.Lock()
        self._session: Optional[aiohttp.ClientSession] = None
        self._circuit = CircuitBreaker(
            "federated_server",
            failure_threshold=3,
            recovery_timeout=30.0
        )
        logger.info("FederatedCarbonVerifier initialized")

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

    async def send_local_verification(self, participant_id: str, verification_data: Dict, performance: float = 1.0) -> Dict:
        if not self.server_url:
            return {'status': 'local'}

        async def _send():
            for attempt in range(self.config.max_retries):
                try:
                    async with self._lock:
                        session = await self._get_session()
                        compressed = self._compress(verification_data)
                        update = {
                            'participant_id': participant_id,
                            'round': self.round,
                            'verification_data': compressed,
                            'performance': performance,
                            'sparsity_ratio': self.config.sparsity_ratio,
                            'timestamp': datetime.now(timezone.utc).isoformat()
                        }
                        async with session.post(
                            f"{self.server_url}/federated/carbon",
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

    async def get_global_verifications(self) -> Optional[Dict]:
        if not self.server_url:
            return self.global_verifications

        async def _fetch():
            for attempt in range(self.config.max_retries):
                try:
                    async with self._lock:
                        session = await self._get_session()
                        async with session.get(
                            f"{self.server_url}/federated/carbon/global",
                            timeout=30
                        ) as resp:
                            if resp.status == 200:
                                data = await resp.json()
                                self.global_verifications = data.get('verifications', {})
                                self.participants = data.get('participants', [])
                                return self.global_verifications
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
# Human-AI Collaborative Verification (Simplified)
# ============================================================================
class HumanAICollaborativeVerification:
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
            'acknowledgment': f"Feedback received on {feedback.get('topic', 'carbon verification')}",
            'insights': [],
            'actions': [],
            'carbon_insights': []
        }
        concern = feedback.get('concern')
        if concern == 'verification':
            reflection['insights'].append("Verification accuracy can be improved through ML")
            reflection['actions'].append("Implement ML verification engine")
        elif concern == 'additionality':
            reflection['insights'].append("Additionality assessment needs refinement")
            reflection['actions'].append("Enhance counterfactual analysis")
        elif concern == 'permanence':
            reflection['insights'].append("Permanence risk requires long-term monitoring")
            reflection['actions'].append("Implement satellite-based monitoring")
        if 'suggestion' in feedback:
            reflection['actions'].append(f"Implementing suggestion: {feedback['suggestion']}")
        reflection['action_items'] = self._prioritize_actions(reflection['actions'])
        return reflection

    def _prioritize_actions(self, actions: List[str]) -> List[Dict]:
        priorities = []
        for action in actions:
            if any(kw in action.lower() for kw in ['urgent', 'critical']):
                priority, impact = 'high', 0.9
            elif any(kw in action.lower() for kw in ['verification', 'carbon']):
                priority, impact = 'high', 0.8
            else:
                priority, impact = 'medium', 0.5
            priorities.append({
                'action': action,
                'priority': priority,
                'impact': impact,
                'estimated_effort': 'medium'
            })
        return sorted(priorities, key=lambda x: x['impact'], reverse=True)

    async def get_insights(self) -> Dict:
        if len(self.feedback_history) < 5:
            return {'status': 'insufficient_feedback'}
        recent = list(self.feedback_history)[-20:]
        topics = defaultdict(int)
        for f in recent:
            topics[f['feedback'].get('topic', 'general')] += 1
        most_common = max(topics.items(), key=lambda x: x[1]) if topics else ('none', 0)
        return {
            'total_feedback': len(self.feedback_history),
            'top_topics': dict(topics),
            'most_common_topic': most_common[0],
            'engagement_score': min(1.0, len(self.feedback_history) / 100),
            'user_count': len(set(f['user_id'] for f in self.feedback_history))
        }

# ============================================================================
# Legacy Sub‑Modules (Blockchain, Satellite, Sensors, Additionality, Accountant)
# ============================================================================
class BlockchainRegistryConnector:
    def __init__(self, config: CarbonOffsetConfig):
        self.config = config
        self.verified_credits: Dict[str, CarbonCredit] = {}
        self.retired_credits: Dict[str, CarbonCredit] = {}
        self.audit_chain: List[Dict] = []
        logger.info("Blockchain Registry Connector initialized")

    async def verify_credit(self, credit_id: str, registry: OffsetRegistry) -> Tuple[bool, Optional[CarbonCredit]]:
        if credit_id in self.verified_credits:
            return True, self.verified_credits[credit_id]
        # Simulate verification
        credit = CarbonCredit(
            credit_id=credit_id,
            registry=registry,
            project_type=ProjectType.REFORESTATION,
            amount_kg=1000.0,
            effective_amount=950.0,
            vintage_year=2023,
            verification_status=VerificationStatus.VERIFIED,
            additionality=AdditionalityLevel.PROVEN_ADDITIONAL,
            permanence_risk=PermanenceRisk.LOW,
            project_location={'lat': 0, 'lon': 0},
            verification_date=datetime.now(timezone.utc),
            expiry_date=datetime.now(timezone.utc) + timedelta(days=365)
        )
        self.verified_credits[credit_id] = credit
        return True, credit

    async def retire_credit(self, credit_id: str, amount_kg: Optional[float] = None) -> Tuple[bool, str]:
        if credit_id in self.verified_credits:
            credit = self.verified_credits[credit_id]
            if amount_kg is None:
                amount_kg = credit.amount_kg
            if amount_kg <= credit.amount_kg:
                credit.amount_kg -= amount_kg
                credit.retirement_date = datetime.now(timezone.utc)
                self.retired_credits[credit_id] = credit
                tx_hash = hashlib.sha256(f"{credit_id}_{datetime.now(timezone.utc).timestamp()}".encode()).hexdigest()
                return True, tx_hash
        return False, ""

    def get_retired_credits_summary(self) -> Dict[str, Any]:
        return {'total_retired': len(self.retired_credits), 'total_amount_kg': sum(c.amount_kg for c in self.retired_credits.values())}

    def verify_chain_integrity(self) -> bool:
        return True

class SatelliteVerificationEngine:
    def __init__(self, config: CarbonOffsetConfig):
        self.config = config
        self.verification_history: List[SatelliteVerification] = []
        logger.info("Satellite Verification Engine initialized")

    async def verify_project(self, project_id: str, project_location: Dict[str, float],
                            project_area_km2: float, baseline_year: int = 2020) -> SatelliteVerification:
        # Simulated verification
        ndvi_change = np.random.normal(0.02, 0.01)
        confidence = np.random.uniform(0.7, 0.95)
        verification = SatelliteVerification(
            verification_id=f"sat_{project_id}_{datetime.now(timezone.utc).timestamp()}",
            project_id=project_id,
            satellite_source='sentinel-2',
            image_date=datetime.now(timezone.utc),
            ndvi_mean=0.5 + np.random.normal(0, 0.1),
            ndvi_change=ndvi_change,
            forest_cover_percent=80 + np.random.normal(0, 5),
            deforestation_detected=ndvi_change < -0.05,
            project_boundary_violation=False,
            carbon_sequestration_estimate_kg=project_area_km2 * 1000 * np.random.uniform(0.8, 1.2),
            confidence_score=confidence,
            anomaly_detected=False,
            sustainability_impact=confidence * 0.8
        )
        self.verification_history.append(verification)
        return verification

    def get_verification_summary(self) -> Dict[str, Any]:
        if not self.verification_history:
            return {'status': 'no_verifications'}
        return {
            'total_verifications': len(self.verification_history),
            'avg_confidence': np.mean([v.confidence_score for v in self.verification_history]),
            'avg_ndvi_change': np.mean([v.ndvi_change for v in self.verification_history]),
            'anomalies_detected': sum(1 for v in self.verification_history if v.anomaly_detected)
        }

class IoTSensorValidator:
    def __init__(self, config: CarbonOffsetConfig):
        self.config = config
        self.registered_sensors: Dict[str, Dict] = {}
        self.validation_history: List[SensorValidation] = []
        logger.info("IoT Sensor Validator initialized")

    def register_sensor(self, sensor_id: str, sensor_type: str, location: Dict[str, float], public_key: str):
        self.registered_sensors[sensor_id] = {
            'sensor_type': sensor_type,
            'location': location,
            'public_key': public_key,
            'registered_at': datetime.now(timezone.utc)
        }
        logger.info(f"Sensor registered: {sensor_id}")

    async def validate_sensor_data(self, sensor_id: str, expected_range: Optional[Tuple[float, float]] = None) -> SensorValidation:
        if expected_range is None:
            expected_range = (0, 1)
        mean_value = np.random.uniform(0.3, 0.7)
        std_dev = np.random.uniform(0.01, 0.05)
        within_range = expected_range[0] <= mean_value <= expected_range[1]
        validation = SensorValidation(
            validation_id=f"sensor_{sensor_id}_{datetime.now(timezone.utc).timestamp()}",
            project_id="dummy_project",
            sensor_id=sensor_id,
            sensor_type=self.registered_sensors.get(sensor_id, {}).get('sensor_type', 'unknown'),
            measurements=[],
            mean_value=mean_value,
            standard_deviation=std_dev,
            expected_range=expected_range,
            within_expected_range=within_range,
            data_quality_score=np.random.uniform(0.7, 0.95),
            cryptographic_signature=hashlib.sha256(f"{sensor_id}_{datetime.now(timezone.utc).timestamp()}".encode()).hexdigest(),
            helium_correlation=np.random.uniform(0.1, 0.5)
        )
        self.validation_history.append(validation)
        return validation

    def get_sensor_status(self) -> Dict[str, Any]:
        return {'registered_sensors': len(self.registered_sensors), 'validations': len(self.validation_history)}

class AdditionalityAssessor:
    def __init__(self, config: CarbonOffsetConfig):
        self.config = config
        self.assessments: List[AdditionalityAssessment] = []
        logger.info("Additionality Assessor initialized")

    async def assess_project(self, project_id: str, project_type: ProjectType,
                            project_location: Dict[str, float], financial_data: Optional[Dict] = None,
                            regulatory_context: Optional[Dict] = None) -> AdditionalityAssessment:
        overall = np.random.choice(list(AdditionalityLevel), p=[0.1, 0.2, 0.5, 0.1, 0.1])
        assessment = AdditionalityAssessment(
            assessment_id=f"add_{project_id}_{datetime.now(timezone.utc).timestamp()}",
            project_id=project_id,
            financial_additionality=np.random.choice([True, False]),
            regulatory_additionality=np.random.choice([True, False]),
            barrier_analysis={'technical': True, 'financial': True, 'institutional': False},
            common_practice_analysis=np.random.choice([True, False]),
            counterfactual_scenario="Baseline scenario without project",
            overall_assessment=overall,
            confidence_score=np.random.uniform(0.6, 0.95),
            assessor="AI_Assessor_v2",
            sustainability_score=0.7 + np.random.normal(0, 0.1)
        )
        self.assessments.append(assessment)
        return assessment

    def get_additionality_summary(self) -> Dict[str, Any]:
        if not self.assessments:
            return {'status': 'no_assessments'}
        levels = {}
        for a in self.assessments:
            levels[a.overall_assessment.value] = levels.get(a.overall_assessment.value, 0) + 1
        return {
            'total_assessments': len(self.assessments),
            'level_distribution': levels,
            'avg_confidence': np.mean([a.confidence_score for a in self.assessments])
        }

class RealTimeCarbonAccountant:
    def __init__(self, carbon_budget_kg: float = 1000.0, accounting_interval_seconds: float = 1.0):
        self.carbon_budget_kg = carbon_budget_kg
        self.accounting_interval = accounting_interval_seconds
        self.scope1_emissions: Deque[Dict] = deque(maxlen=86400)
        self.scope2_emissions: Deque[Dict] = deque(maxlen=86400)
        self.scope3_emissions: Deque[Dict] = deque(maxlen=86400)
        self.verified_offsets: float = 0.0
        self.pending_offsets: float = 0.0
        self.account_history: Deque[RealTimeCarbonAccount] = deque(maxlen=10000)
        self._running_total_scope1 = 0.0
        self._running_total_scope2 = 0.0
        self._running_total_scope3 = 0.0
        self._lock = asyncio.Lock()
        self._task: Optional[asyncio.Task] = None
        self._accounting_loop_running = False
        logger.info(f"Real-Time Carbon Accountant initialized: budget={carbon_budget_kg}kg")

    def record_emission(self, scope: int, amount_kg: float, source: str = "unknown"):
        if scope == 1:
            self.scope1_emissions.append({'amount_kg': amount_kg, 'source': source, 'timestamp': datetime.now(timezone.utc)})
            self._running_total_scope1 += amount_kg
        elif scope == 2:
            self.scope2_emissions.append({'amount_kg': amount_kg, 'source': source, 'timestamp': datetime.now(timezone.utc)})
            self._running_total_scope2 += amount_kg
        elif scope == 3:
            self.scope3_emissions.append({'amount_kg': amount_kg, 'source': source, 'timestamp': datetime.now(timezone.utc)})
            self._running_total_scope3 += amount_kg

    def record_offset(self, amount_kg: float, verified: bool = False):
        if verified:
            self.verified_offsets += amount_kg
        else:
            self.pending_offsets += amount_kg

    def get_current_position(self) -> RealTimeCarbonAccount:
        net = (self._running_total_scope1 + self._running_total_scope2 + self._running_total_scope3) - self.verified_offsets
        remaining = self.carbon_budget_kg - net
        if remaining < 0:
            status = "exceeded"
        elif remaining < self.carbon_budget_kg * 0.2:
            status = "warning"
        else:
            status = "compliant"
        return RealTimeCarbonAccount(
            account_id=f"acc_{datetime.now(timezone.utc).timestamp()}",
            timestamp=datetime.now(timezone.utc),
            scope1_emissions_kg=self._running_total_scope1,
            scope2_emissions_kg=self._running_total_scope2,
            scope3_emissions_kg=self._running_total_scope3,
            verified_offsets_kg=self.verified_offsets,
            pending_offsets_kg=self.pending_offsets,
            net_position_kg=net,
            carbon_budget_remaining_kg=remaining,
            budget_status=status
        )

    def get_emissions_breakdown(self) -> Dict[str, float]:
        return {
            'scope1_kg': self._running_total_scope1,
            'scope2_kg': self._running_total_scope2,
            'scope3_kg': self._running_total_scope3,
            'total_emissions_kg': self._running_total_scope1 + self._running_total_scope2 + self._running_total_scope3
        }

    def start(self):
        if not self._task:
            self._task = asyncio.create_task(self._accounting_loop())

    async def stop(self):
        if self._task:
            self._accounting_loop_running = False
            self._task.cancel()
            await self._task
            self._task = None

    async def _accounting_loop(self):
        self._accounting_loop_running = True
        while self._accounting_loop_running:
            try:
                async with self._lock:
                    self.account_history.append(self.get_current_position())
                await asyncio.sleep(self.accounting_interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Accounting loop error: {e}")
                await asyncio.sleep(5)

# ============================================================================
# Persistence Manager (JSON with versioning)
# ============================================================================
class CarbonOffsetPersistenceManager:
    def __init__(self, config: PersistenceConfig):
        self.config = config
        self.path = config.path
        self._lock = asyncio.Lock()
        self._version = 2  # Bumped for MOPD
        logger.info(f"CarbonOffsetPersistenceManager initialized (path={self.path})")

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
class CarbonOffsetTelemetry:
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
# Storage Module
# ============================================================================
class CarbonOffsetStorage:
    def __init__(self):
        self.verification_records: List[Dict] = []
        self.sustainability_score = 0.0
        self.mopd_plans: List[MOPDPlan] = []  # NEW: store MOPD plans
        self._lock = asyncio.Lock()

    async def add_record(self, record: Dict):
        async with self._lock:
            self.verification_records.append(record)
            if len(self.verification_records) > 10000:
                self.verification_records = self.verification_records[-10000:]

    async def add_mopd_plan(self, plan: MOPDPlan):
        async with self._lock:
            self.mopd_plans.append(plan)
            if len(self.mopd_plans) > 10000:
                self.mopd_plans = self.mopd_plans[-10000:]

    async def get_mopd_plans(self, limit: Optional[int] = None) -> List[MOPDPlan]:
        async with self._lock:
            if limit is not None:
                return self.mopd_plans[-limit:]
            return self.mopd_plans.copy()

    async def get_records(self, limit: Optional[int] = None) -> List[Dict]:
        async with self._lock:
            if limit is not None:
                return self.verification_records[-limit:]
            return self.verification_records.copy()

    async def update_sustainability_score(self, score: float):
        async with self._lock:
            self.sustainability_score = score

    async def get_sustainability_score(self) -> float:
        async with self._lock:
            return self.sustainability_score

    async def get_stats(self) -> Dict[str, Any]:
        async with self._lock:
            total = len(self.verification_records)
            if total == 0:
                return {'total': 0, 'success_rate': 0.0}
            success = sum(1 for r in self.verification_records if r.get('overall_success', False))
            return {
                'total': total,
                'successful': success,
                'success_rate': success / total
            }

# ============================================================================
# Analyzer Module (Enhanced with MOPD)
# ============================================================================
class CarbonOffsetAnalyzer:
    def __init__(
        self,
        config: CarbonOffsetConfig,
        storage: CarbonOffsetStorage,
        blockchain: Optional[BlockchainRegistryConnector],
        satellite: Optional[SatelliteVerificationEngine],
        sensors: Optional[IoTSensorValidator],
        additionality: Optional[AdditionalityAssessor],
        carbon_manager: Optional[CarbonIntensityManager],
        helium_tracker: Optional[HeliumEmissionTracker],
        ml_verifier: Optional[MLVerificationEngine],
        predictive: Optional[PredictiveOffsetAnalyzer],
        accountant: RealTimeCarbonAccountant,
        human_ai: Optional[HumanAICollaborativeVerification]
    ):
        self.config = config
        self.storage = storage
        self.blockchain = blockchain
        self.satellite = satellite
        self.sensors = sensors
        self.additionality = additionality
        self.carbon_manager = carbon_manager
        self.helium_tracker = helium_tracker
        self.ml_verifier = ml_verifier
        self.predictive = predictive
        self.accountant = accountant
        self.human_ai = human_ai

        self._blockchain_circuit = CircuitBreaker("blockchain_api")
        self._satellite_circuit = CircuitBreaker("satellite_api")
        self._sensor_circuit = CircuitBreaker("sensor_api")
        self._additionality_circuit = CircuitBreaker("additionality_api")

        self._lock = asyncio.Lock()

    # ============================================================================
    # MOPD Methods (NEW)
    # ============================================================================
    async def _enumerate_strategies(
        self,
        credit_id: str,
        registry: OffsetRegistry,
        project_id: str,
        project_location: Dict[str, float],
        project_area_km2: float,
        project_type: Optional[ProjectType] = None
    ) -> List[MOPDPlan]:
        """Generate all feasible offset strategies."""
        # Decision variables:
        # - amount_to_retire_kg: sample a few values around available
        # - use_ml_verification: True/False
        # - verify_satellite: True/False
        # - verify_sensors: True/False
        # - verify_additionality: True/False

        available_amount = 1000.0  # assume from credit
        amount_options = [available_amount * 0.25, available_amount * 0.5, available_amount * 0.75, available_amount * 1.0]

        use_ml_options = [True, False]
        sat_options = [True, False]
        sensor_options = [True, False]
        add_options = [True, False]

        plans = []
        for amount in amount_options:
            for use_ml in use_ml_options:
                for sat in sat_options:
                    for sensor in sensor_options:
                        for add in add_options:
                            plan = MOPDPlan(
                                credit_id=credit_id,
                                registry=registry,
                                project_type=project_type or ProjectType.REFORESTATION,
                                amount_to_retire_kg=amount,
                                use_ml_verification=use_ml,
                                verify_satellite=sat,
                                verify_sensors=sensor,
                                verify_additionality=add,
                                cost=0.0,
                                carbon_savings_kg=0.0,
                                helium_impact_l=0.0,
                                verification_confidence=0.0,
                                verification_time_ms=0.0,
                                sustainability_score=0.0
                            )
                            plans.append(plan)
        return plans

    async def _compute_plan_objectives(self, plan: MOPDPlan) -> MOPDPlan:
        """Calculate cost, carbon savings, helium impact, confidence, time for a given plan."""
        # Base assumptions
        cost = 0.0
        carbon_savings = plan.amount_to_retire_kg
        helium_impact = -plan.amount_to_retire_kg * 0.05  # negative means offset (good)
        confidence = 0.5
        time_ms = 1000.0

        # Adjust based on verification methods
        if plan.verify_satellite:
            cost += 10.0
            confidence += 0.15
            time_ms += 2000.0
        if plan.verify_sensors:
            cost += 5.0
            confidence += 0.1
            time_ms += 1000.0
        if plan.verify_additionality:
            cost += 20.0
            confidence += 0.2
            time_ms += 3000.0
        if plan.use_ml_verification:
            cost += 2.0
            confidence += 0.1
            time_ms += 500.0

        # Clamp confidence
        confidence = min(1.0, confidence)

        # If no verification, confidence is low
        if not plan.verify_satellite and not plan.verify_sensors and not plan.verify_additionality and not plan.use_ml_verification:
            confidence = 0.3

        plan.cost = cost
        plan.carbon_savings_kg = carbon_savings
        plan.helium_impact_l = helium_impact
        plan.verification_confidence = confidence
        plan.verification_time_ms = time_ms
        plan.sustainability_score = confidence * 0.7 + (1.0 - helium_impact / 10) * 0.3
        return plan

    async def _generate_pareto_front_for_offset(
        self,
        credit_id: str,
        registry: OffsetRegistry,
        project_id: str,
        project_location: Dict[str, float],
        project_area_km2: float,
        project_type: Optional[ProjectType] = None
    ) -> List[MOPDPlan]:
        """Generate Pareto front of offset strategies."""
        plans = await self._enumerate_strategies(
            credit_id, registry, project_id, project_location, project_area_km2, project_type
        )
        computed_plans = []
        for plan in plans:
            computed = await self._compute_plan_objectives(plan)
            computed_plans.append(computed)

        # Filter dominated plans
        objective_names = ['cost', 'carbon_savings_kg', 'helium_impact_l', 'verification_confidence', 'verification_time_ms']
        # We minimise cost, helium_impact, verification_time; maximise carbon_savings, verification_confidence
        pareto = []
        for i, p_i in enumerate(computed_plans):
            dominated = False
            for j, p_j in enumerate(computed_plans):
                if i == j:
                    continue
                # Build vectors: for maximisation, we negate
                a_vec = [
                    p_i.cost,
                    -p_i.carbon_savings_kg,
                    p_i.helium_impact_l,
                    -p_i.verification_confidence,
                    p_i.verification_time_ms
                ]
                b_vec = [
                    p_j.cost,
                    -p_j.carbon_savings_kg,
                    p_j.helium_impact_l,
                    -p_j.verification_confidence,
                    p_j.verification_time_ms
                ]
                if all(b <= a for a, b in zip(a_vec, b_vec)) and any(b < a for a, b in zip(a_vec, b_vec)):
                    dominated = True
                    break
            if not dominated:
                pareto.append(p_i)
        return pareto

    def _select_best_from_pareto(self, pareto_front: List[MOPDPlan]) -> Optional[MOPDPlan]:
        if not pareto_front:
            return None
        weights = self.config.mopd.objective_weights
        objective_names = ['cost', 'carbon_savings_kg', 'helium_impact_l', 'verification_confidence', 'verification_time_ms']
        # Normalise across front
        max_vals = {}
        min_vals = {}
        for key in objective_names:
            vals = [getattr(p, key) for p in pareto_front]
            max_vals[key] = max(vals)
            min_vals[key] = min(vals)
        ranges = {k: max_vals[k] - min_vals[k] if max_vals[k] != min_vals[k] else 1.0 for k in objective_names}

        best = None
        best_score = -float('inf')
        for plan in pareto_front:
            score = 0.0
            for key in objective_names:
                val = getattr(plan, key)
                # For objectives to minimise, we invert: 1 - (val - min)/range
                if key in ['cost', 'helium_impact_l', 'verification_time_ms']:
                    norm = 1.0 - (val - min_vals[key]) / ranges[key] if ranges[key] > 0 else 1.0
                else:  # maximise
                    norm = (val - min_vals[key]) / ranges[key] if ranges[key] > 0 else 1.0
                weight = weights.get(key, 1.0 / len(objective_names))
                score += weight * norm
            if score > best_score:
                best_score = score
                best = plan
        return best

    # ============================================================================
    # Core Verification Method (Enhanced with MOPD)
    # ============================================================================
    async def verify_and_retire(
        self,
        credit_id: str,
        registry: OffsetRegistry,
        project_id: str,
        project_location: Dict[str, float],
        project_area_km2: float,
        amount_to_retire_kg: float,
        project_type: Optional[ProjectType] = None,
        use_ml_verification: bool = False,
        return_mopd: bool = False           # NEW: if True, return Pareto front
    ) -> Dict[str, Any]:
        result = {
            'credit_id': credit_id,
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'verification_steps': {},
            'overall_success': False,
            'sustainability_score': 0.0,
            'helium_impact': {}
        }

        # Get carbon intensity
        carbon_intensity = 400
        if self.carbon_manager:
            carbon_intensity = await self.carbon_manager.get_current_intensity()

        # Step 1: Blockchain verification
        if self.blockchain:
            try:
                async def _verify():
                    return await self.blockchain.verify_credit(credit_id, registry)
                is_valid, credit = await self._blockchain_circuit.call(_verify)
                result['verification_steps']['blockchain'] = {
                    'success': is_valid,
                    'amount_kg': credit.amount_kg if credit else 0,
                    'effective_amount_kg': credit.effective_amount if credit else 0
                }
                if not is_valid:
                    result['overall_success'] = False
                    return result
            except Exception as e:
                logger.error(f"Blockchain verification failed: {e}")
                result['verification_steps']['blockchain'] = {'success': False, 'error': str(e)}
                result['overall_success'] = False
                return result
        else:
            credit = None

        # Step 2: Satellite verification
        if self.satellite:
            try:
                async def _verify_sat():
                    return await self.satellite.verify_project(
                        project_id, project_location, project_area_km2
                    )
                sat_verification = await self._satellite_circuit.call(_verify_sat)
                result['verification_steps']['satellite'] = {
                    'success': not sat_verification.anomaly_detected,
                    'ndvi_change': sat_verification.ndvi_change,
                    'sequestration_estimate_kg': sat_verification.carbon_sequestration_estimate_kg,
                    'confidence': sat_verification.confidence_score,
                    'sustainability_impact': sat_verification.sustainability_impact
                }
            except Exception as e:
                logger.error(f"Satellite verification failed: {e}")
                result['verification_steps']['satellite'] = {'success': False, 'error': str(e)}

        # Step 3: IoT sensor validation
        if self.sensors:
            try:
                async def _validate_sensor():
                    return await self.sensors.validate_sensor_data(f"sensor_{project_id}")
                sensor_validation = await self._sensor_circuit.call(_validate_sensor)
                if sensor_validation:
                    result['verification_steps']['sensors'] = {
                        'success': sensor_validation.within_expected_range,
                        'data_quality': sensor_validation.data_quality_score,
                        'helium_correlation': sensor_validation.helium_correlation
                    }
            except Exception as e:
                logger.error(f"Sensor validation failed: {e}")
                result['verification_steps']['sensors'] = {'success': False, 'error': str(e)}

        # Step 4: Additionality assessment
        if self.additionality:
            try:
                async def _assess_add():
                    return await self.additionality.assess_project(
                        project_id,
                        project_type or ProjectType.REFORESTATION,
                        project_location
                    )
                assessment = await self._additionality_circuit.call(_assess_add)
                result['verification_steps']['additionality'] = {
                    'success': assessment.overall_assessment in [
                        AdditionalityLevel.PROVEN_ADDITIONAL,
                        AdditionalityLevel.LIKELY_ADDITIONAL
                    ],
                    'level': assessment.overall_assessment.value,
                    'confidence': assessment.confidence_score,
                    'sustainability_score': assessment.sustainability_score
                }
            except Exception as e:
                logger.error(f"Additionality assessment failed: {e}")
                result['verification_steps']['additionality'] = {'success': False, 'error': str(e)}

        # Step 5: ML verification
        if self.ml_verifier and use_ml_verification:
            try:
                ml_result = await self.ml_verifier.verify({
                    'carbon_intensity': carbon_intensity,
                    'satellite_confidence': sat_verification.confidence_score if sat_verification else 0.5,
                    'sensor_quality': sensor_validation.data_quality_score if sensor_validation else 0.5,
                    'additionality_score': assessment.confidence_score if assessment else 0.5,
                    'permanence_risk': 0.3,
                    'registry_trust': 0.9,
                    'project_age_years': 1,
                    'area_km2': project_area_km2,
                    'verification_effort': 0.8,
                    'historical_success': 0.9
                })
                result['verification_steps']['ml'] = {
                    'success': ml_result.get('verification_success', 0.5) > 0.7,
                    'verification_success': ml_result.get('verification_success', 0.5),
                    'confidence': ml_result.get('confidence', 0.5)
                }
            except Exception as e:
                logger.error(f"ML verification failed: {e}")
                result['verification_steps']['ml'] = {'success': False, 'error': str(e)}

        # Step 6: Helium impact
        if self.helium_tracker:
            helium_offset = self.helium_tracker.calculate_helium_offset_from_carbon(amount_to_retire_kg)
            self.helium_tracker.record_offset(helium_offset, verified=True)
            result['helium_impact'] = {
                'offset_l': helium_offset,
                'co2_equivalent_kg': helium_offset * self.helium_tracker.config.helium_to_co2_factor,
                'net_position_l': self.helium_tracker.get_position()['net_position_l']
            }

        # Step 7: Retire credit
        if self.blockchain and credit:
            try:
                async def _retire():
                    return await self.blockchain.retire_credit(credit_id, amount_to_retire_kg)
                success, tx_hash = await self._blockchain_circuit.call(_retire)
                result['verification_steps']['retirement'] = {
                    'success': success,
                    'transaction_hash': tx_hash,
                    'amount_retired_kg': amount_to_retire_kg
                }
                if success:
                    effective_amount = credit.effective_amount if credit else amount_to_retire_kg
                    self.accountant.record_offset(effective_amount, verified=True)
            except Exception as e:
                logger.error(f"Retirement failed: {e}")
                result['verification_steps']['retirement'] = {'success': False, 'error': str(e)}

        # Calculate sustainability score
        sustainability_score = self._calculate_sustainability_score(result)
        result['sustainability_score'] = sustainability_score
        await self.storage.update_sustainability_score(sustainability_score)

        # Update carbon position
        current_position = self.accountant.get_current_position()
        result['carbon_position'] = {
            'net_position_kg': current_position.net_position_kg,
            'carbon_budget_remaining_kg': current_position.carbon_budget_remaining_kg,
            'budget_status': current_position.budget_status
        }

        # Determine overall success
        steps = result['verification_steps']
        result['overall_success'] = all(
            step.get('success', False)
            for step in steps.values()
        )

        # Human‑AI insights
        if self.human_ai:
            insights = self.human_ai.get_insights()
            result['human_ai_insights'] = insights

        # MOPD: generate Pareto front if requested
        if self.config.enable_mopd and return_mopd:
            pareto_front = await self._generate_pareto_front_for_offset(
                credit_id, registry, project_id, project_location, project_area_km2, project_type
            )
            # Store MOPD plans
            for plan in pareto_front:
                await self.storage.add_mopd_plan(plan)
            result['mopd_pareto_front'] = [p.to_dict() for p in pareto_front]
            best_plan = self._select_best_from_pareto(pareto_front)
            if best_plan:
                result['mopd_best_plan'] = best_plan.to_dict()

        # Store record
        await self.storage.add_record(result)

        return result

    def _calculate_sustainability_score(self, result: Dict) -> float:
        scores = []
        if 'blockchain' in result.get('verification_steps', {}):
            scores.append(0.9 if result['verification_steps']['blockchain']['success'] else 0.3)
        if 'satellite' in result.get('verification_steps', {}):
            scores.append(result['verification_steps']['satellite'].get('confidence', 0.5))
        if 'additionality' in result.get('verification_steps', {}):
            scores.append(result['verification_steps']['additionality'].get('confidence', 0.5))
        if 'ml' in result.get('verification_steps', {}):
            scores.append(result['verification_steps']['ml'].get('verification_success', 0.5))
        if 'carbon_position' in result:
            status = result['carbon_position'].get('budget_status', 'compliant')
            if status == 'compliant':
                scores.append(0.9)
            elif status == 'warning':
                scores.append(0.5)
            else:
                scores.append(0.2)
        return np.mean(scores) if scores else 0.5

    async def train_ml_model(self, training_data: Optional[List[Dict]] = None) -> Dict:
        if not self.ml_verifier:
            return {'status': 'disabled'}
        if training_data is None:
            records = await self.storage.get_records(200)
            training_data = []
            for item in records:
                steps = item.get('verification_steps', {})
                training_data.append({
                    'carbon_intensity': self.carbon_manager.carbon_intensity if self.carbon_manager else 400,
                    'satellite_confidence': steps.get('satellite', {}).get('confidence', 0.5),
                    'sensor_quality': steps.get('sensors', {}).get('data_quality', 0.5),
                    'additionality_score': steps.get('additionality', {}).get('confidence', 0.5),
                    'permanence_risk': 0.3,
                    'registry_trust': 0.9,
                    'project_age_years': 1,
                    'area_km2': 100,
                    'verification_effort': 0.8,
                    'historical_success': 0.9,
                    'verification_success': 1.0 if item.get('overall_success', False) else 0.0,
                    'confidence': 0.7
                })
        return await self.ml_verifier.train(training_data)

    async def train_predictive_model(self) -> Dict:
        if not self.predictive:
            return {'status': 'disabled'}
        return await self.predictive.train()

# ============================================================================
# Reporter Module (Enhanced with MOPD)
# ============================================================================
class CarbonOffsetReporter:
    def __init__(
        self,
        config: CarbonOffsetConfig,
        storage: CarbonOffsetStorage,
        analyzer: CarbonOffsetAnalyzer,
        telemetry: Optional[CarbonOffsetTelemetry],
        persistence: Optional[CarbonOffsetPersistenceManager],
        human_ai: Optional[HumanAICollaborativeVerification],
        federated: Optional[FederatedCarbonVerifier],
        predictive: Optional[PredictiveOffsetAnalyzer],
        ml_verifier: Optional[MLVerificationEngine],
        helium_tracker: Optional[HeliumEmissionTracker],
        accountant: RealTimeCarbonAccountant,
        blockchain: Optional[BlockchainRegistryConnector],
        satellite: Optional[SatelliteVerificationEngine],
        sensors: Optional[IoTSensorValidator],
        additionality: Optional[AdditionalityAssessor]
    ):
        self.config = config
        self.storage = storage
        self.analyzer = analyzer
        self.telemetry = telemetry
        self.persistence = persistence
        self.human_ai = human_ai
        self.federated = federated
        self.predictive = predictive
        self.ml_verifier = ml_verifier
        self.helium_tracker = helium_tracker
        self.accountant = accountant
        self.blockchain = blockchain
        self.satellite = satellite
        self.sensors = sensors
        self.additionality = additionality
        self._lock = asyncio.Lock()

    async def get_verification_summary(self) -> Dict[str, Any]:
        stats = await self.storage.get_stats()
        sustainability = await self.storage.get_sustainability_score()
        summary = {
            'total_verifications': stats['total'],
            'successful_verifications': stats['successful'],
            'success_rate': stats['success_rate'],
            'carbon_position': self.accountant.get_current_position().__dict__,
            'emissions_breakdown': self.accountant.get_emissions_breakdown(),
            'sustainability_score': sustainability,
            'blockchain_summary': self.blockchain.get_retired_credits_summary() if self.blockchain else {},
            'satellite_summary': self.satellite.get_verification_summary() if self.satellite else {},
            'sensor_status': self.sensors.get_sensor_status() if self.sensors else {},
            'additionality_summary': self.additionality.get_additionality_summary() if self.additionality else {}
        }

        if self.helium_tracker:
            summary['helium_position'] = self.helium_tracker.get_position()

        if self.federated:
            summary['federated_stats'] = self.federated.get_federated_stats()

        if self.predictive:
            forecast = await self.predictive.predict_price()
            summary['predictive_forecast'] = forecast

        if self.ml_verifier:
            summary['ml_status'] = {
                'trained': self.ml_verifier.is_trained,
                'model_version': 'v4.0.0',
                'training_samples': len(self.ml_verifier.training_history)
            }

        if self.human_ai:
            summary['human_ai_insights'] = await self.human_ai.get_insights()

        # MOPD summary
        if self.config.enable_mopd:
            mopd_plans = await self.storage.get_mopd_plans(20)
            summary['mopd_plans'] = [p.to_dict() for p in mopd_plans]

        return summary

    async def get_sustainability_report(self) -> Dict[str, Any]:
        sustainability = await self.storage.get_sustainability_score()
        return {
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'sustainability_score': sustainability,
            'carbon_position': self.accountant.get_current_position().__dict__,
            'helium_position': self.helium_tracker.get_position() if self.helium_tracker else {},
            'total_verifications': (await self.storage.get_stats())['total'],
            'success_rate': (await self.storage.get_stats())['success_rate'],
            'recommendations': self._generate_recommendations()
        }

    def _generate_recommendations(self) -> List[str]:
        recs = []
        status = self.accountant.get_current_position().budget_status
        if status == 'exceeded':
            recs.append("CRITICAL: Carbon budget exceeded - reduce emissions immediately")
        elif status == 'warning':
            recs.append("Carbon budget warning - implement reduction measures")

        if self.helium_tracker:
            remaining = self.helium_tracker.get_position().get('remaining_budget_l', 0)
            if remaining < 0:
                recs.append("CRITICAL: Helium budget exceeded - implement recovery systems")

        if self.federated and len(self.federated.participants) < 2:
            recs.append("Increase federated participation for better verification")

        if self.config.enable_mopd:
            recs.append("Consider using MOPD to explore trade-offs in verification strategies")

        return recs or ["All sustainability metrics are within acceptable ranges"]

    async def export_telemetry(self):
        if self.telemetry:
            data = await self.telemetry.export()
            logger.debug(f"Telemetry export: {len(data)} bytes")

    async def save_state(self):
        if self.persistence:
            state = {
                'verification_records': await self.storage.get_records(),
                'sustainability_score': await self.storage.get_sustainability_score(),
                'carbon_accountant': {
                    'carbon_budget_kg': self.accountant.carbon_budget_kg,
                    'scope1_emissions': list(self.accountant.scope1_emissions),
                    'scope2_emissions': list(self.accountant.scope2_emissions),
                    'scope3_emissions': list(self.accountant.scope3_emissions),
                    'verified_offsets': self.accountant.verified_offsets,
                    'pending_offsets': self.accountant.pending_offsets,
                    'account_history': list(self.accountant.account_history),
                    '_running_total_scope1': self.accountant._running_total_scope1,
                    '_running_total_scope2': self.accountant._running_total_scope2,
                    '_running_total_scope3': self.accountant._running_total_scope3,
                },
                'helium_tracker': {
                    'emissions': list(self.helium_tracker.emissions) if self.helium_tracker else [],
                    'offsets': list(self.helium_tracker.offsets) if self.helium_tracker else [],
                    '_total_emissions': self.helium_tracker._total_emissions if self.helium_tracker else 0.0,
                    '_total_offsets': self.helium_tracker._total_offsets if self.helium_tracker else 0.0,
                },
                'ml_checkpoint': self.analyzer.ml_verifier.get_checkpoint() if self.analyzer.ml_verifier else None,
                'mopd_plans': [p.to_dict() for p in await self.storage.get_mopd_plans()],  # NEW
            }
            await self.persistence.save_state(state)

    async def load_state(self):
        if self.persistence:
            state = await self.persistence.load_state()
            if state:
                # Restore verification_records
                records = state.get('verification_records', [])
                for r in records:
                    await self.storage.add_record(r)
                await self.storage.update_sustainability_score(state.get('sustainability_score', 0.0))

                # Restore accountant
                acc_data = state.get('carbon_accountant', {})
                if acc_data:
                    self.accountant.carbon_budget_kg = acc_data.get('carbon_budget_kg', 1000.0)
                    self.accountant.scope1_emissions = deque(acc_data.get('scope1_emissions', []), maxlen=86400)
                    self.accountant.scope2_emissions = deque(acc_data.get('scope2_emissions', []), maxlen=86400)
                    self.accountant.scope3_emissions = deque(acc_data.get('scope3_emissions', []), maxlen=86400)
                    self.accountant.verified_offsets = acc_data.get('verified_offsets', 0.0)
                    self.accountant.pending_offsets = acc_data.get('pending_offsets', 0.0)
                    self.accountant.account_history = deque(acc_data.get('account_history', []), maxlen=10000)
                    self.accountant._running_total_scope1 = acc_data.get('_running_total_scope1', 0.0)
                    self.accountant._running_total_scope2 = acc_data.get('_running_total_scope2', 0.0)
                    self.accountant._running_total_scope3 = acc_data.get('_running_total_scope3', 0.0)

                # Restore helium tracker
                he_data = state.get('helium_tracker', {})
                if he_data and self.helium_tracker:
                    self.helium_tracker.emissions = deque(he_data.get('emissions', []), maxlen=86400)
                    self.helium_tracker.offsets = deque(he_data.get('offsets', []), maxlen=86400)
                    self.helium_tracker._total_emissions = he_data.get('_total_emissions', 0.0)
                    self.helium_tracker._total_offsets = he_data.get('_total_offsets', 0.0)

                # Restore ML checkpoint
                ml_cp = state.get('ml_checkpoint')
                if ml_cp and self.analyzer.ml_verifier:
                    self.analyzer.ml_verifier.load_checkpoint(ml_cp)

                # Restore MOPD plans
                mopd_plans = state.get('mopd_plans', [])
                for p_dict in mopd_plans:
                    await self.storage.add_mopd_plan(MOPDPlan.from_dict(p_dict))

# ============================================================================
# Main Controller (Enhanced with MOPD)
# ============================================================================
class AutomatedCarbonOffsetVerification:
    """
    Enhanced Automated Carbon Offset Verification System v4.1.0
    Controller that orchestrates storage, analysis, reporting, and MOPD support.
    """

    def __init__(
        self,
        bio_core: Optional[EnhancedBioInspiredCore] = None,
        config: Optional[CarbonOffsetConfig] = None,
        **kwargs
    ):
        if config is None:
            config = CarbonOffsetConfig(**{k: v for k, v in kwargs.items() if k in CarbonOffsetConfig.__annotations__})
        self.config = config

        # Bio‑core references
        self.bio_core = bio_core
        self.event_broker = getattr(bio_core, 'event_broker', None) if bio_core else None
        self.self_healer = getattr(bio_core, 'self_healer', None) if bio_core else None
        self.workflow_orchestrator = getattr(bio_core, 'workflow_orchestrator', None) if bio_core else None
        self.swarm_coordinator = getattr(bio_core, 'swarm_coordinator', None) if bio_core else None
        self.token_manager = getattr(bio_core, 'token_manager', None) if bio_core else None
        self.gradient_manager = getattr(bio_core, 'gradient_manager', None) if bio_core else None
        self.quantum_bridge = getattr(bio_core, 'quantum_bridge', None) if bio_core else None
        self.tick_engine = getattr(bio_core, 'tick_engine', None) if bio_core else None
        self.cost_benefit_engine = getattr(bio_core, 'cost_benefit_engine', None) if bio_core else None

        # Sub‑modules
        self.carbon_manager = CarbonIntensityManager(self.config.carbon) if self.config.carbon.enabled else None
        self.helium_tracker = HeliumEmissionTracker(self.config.helium) if self.config.helium.enabled else None
        self.predictive = PredictiveOffsetAnalyzer(self.config.predictive) if self.config.predictive.enabled else None
        self.ml_verifier = MLVerificationEngine(self.config.ml) if self.config.ml.enabled else None
        self.federated = FederatedCarbonVerifier(self.config.federated) if self.config.federated.enabled else None
        self.human_ai = HumanAICollaborativeVerification() if self.config.enable_human_ai else None
        self.telemetry = CarbonOffsetTelemetry() if self.config.telemetry.enabled else None
        self.persistence = CarbonOffsetPersistenceManager(self.config.persistence) if self.config.persistence.enabled else None

        # Legacy sub‑modules
        self.blockchain = BlockchainRegistryConnector(self.config) if self.config.enable_blockchain else None
        self.satellite = SatelliteVerificationEngine(self.config) if self.config.enable_satellite else None
        self.sensors = IoTSensorValidator(self.config) if self.config.enable_sensors else None
        self.additionality = AdditionalityAssessor(self.config) if self.config.enable_additionality else None

        # Core components
        self.accountant = RealTimeCarbonAccountant(self.config.carbon_budget_kg)

        # Storage, Analyzer, Reporter
        self.storage = CarbonOffsetStorage()
        self.analyzer = CarbonOffsetAnalyzer(
            self.config,
            self.storage,
            self.blockchain,
            self.satellite,
            self.sensors,
            self.additionality,
            self.carbon_manager,
            self.helium_tracker,
            self.ml_verifier,
            self.predictive,
            self.accountant,
            self.human_ai
        )
        self.reporter = CarbonOffsetReporter(
            self.config,
            self.storage,
            self.analyzer,
            self.telemetry,
            self.persistence,
            self.human_ai,
            self.federated,
            self.predictive,
            self.ml_verifier,
            self.helium_tracker,
            self.accountant,
            self.blockchain,
            self.satellite,
            self.sensors,
            self.additionality
        )

        # MoE injectables
        self.expert_router = None
        self.gating_network = None
        self.self_evolving_gate = None
        self.helium_provider = None

        # Health status
        self.health_status = "healthy"
        self.last_error: Optional[str] = None

        # Event queue
        self._event_queue: asyncio.Queue = asyncio.Queue()
        self._event_consumer_task: Optional[asyncio.Task] = None

        # Background tasks
        self._background_tasks: List[asyncio.Task] = []

        # Start sub‑module loops
        if self.helium_tracker:
            self.helium_tracker.start()
        if self.accountant:
            self.accountant.start()

        # Subscribe to events
        if self.config.enable_event_driven and self.event_broker:
            self._subscribe_events()

        # Start background tasks
        self._start_background_tasks()

        # Load state
        if self.config.persistence.enabled:
            asyncio.create_task(self.reporter.load_state())

        logger.info("Automated Carbon Offset Verification System v4.1.0 initialized with MOPD")

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
        # Update predictive history
        if self.predictive:
            self.predictive.update_history({
                'price': event.data.get('price', 50),
                'volume': 1000,
                'verification_rate': 0.9,
                'market_confidence': 0.7,
                'carbon_intensity': intensity
            })

    async def _on_helium_update(self, event: BioEvent):
        scarcity = event.data.get('scarcity', 0.5)
        if self.helium_tracker:
            self.helium_tracker.budget_l = self.config.helium_budget_l * (1.0 - scarcity * 0.3)
            self.helium_tracker.config.helium_to_co2_factor = self.config.helium_to_co2_factor * (1.0 + 0.1 * scarcity)

    async def _on_alert_generated(self, event: BioEvent):
        if event.data.get('severity') == 'critical':
            logger.warning("Critical alert; triggering self‑healing")
            if self.config.self_healing.enabled and self.self_healer:
                await self.self_healer.apply_healing('damage_accumulation')
            if self.workflow_orchestrator and self.config.workflow_on_critical_alert:
                await self.workflow_orchestrator.execute_workflow(self.config.workflow_on_critical_alert)

    async def _on_config_updated(self, event: BioEvent):
        updates = event.data.get('updates', {})
        if 'carbon_offset' in updates:
            new = updates['carbon_offset']
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
            pass

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

        if self.predictive:
            t = asyncio.create_task(self._predictive_update_loop())
            self._background_tasks.append(t)

        if self.ml_verifier:
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
                if self.predictive:
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
                if self.ml_verifier:
                    records = await self.storage.get_records(200)
                    if len(records) >= 20:
                        training_data = []
                        for item in records:
                            steps = item.get('verification_steps', {})
                            training_data.append({
                                'carbon_intensity': self.carbon_manager.carbon_intensity if self.carbon_manager else 400,
                                'satellite_confidence': steps.get('satellite', {}).get('confidence', 0.5),
                                'sensor_quality': steps.get('sensors', {}).get('data_quality', 0.5),
                                'additionality_score': steps.get('additionality', {}).get('confidence', 0.5),
                                'permanence_risk': 0.3,
                                'registry_trust': 0.9,
                                'project_age_years': 1,
                                'area_km2': 100,
                                'verification_effort': 0.8,
                                'historical_success': 0.9,
                                'verification_success': 1.0 if item.get('overall_success', False) else 0.0,
                                'confidence': 0.7
                            })
                        await self.ml_verifier.train(training_data)
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
                    stats = await self.storage.get_stats()
                    pid = f"carbon_verifier_{hashlib.md5(str(self.storage.verification_records).encode()).hexdigest()[:8]}"
                    await self.federated.send_local_verification(
                        pid,
                        {
                            'total_verifications': stats['total'],
                            'success_rate': stats['success_rate'],
                            'carbon_position': self.accountant.get_current_position().__dict__,
                            'timestamp': datetime.now(timezone.utc).isoformat()
                        },
                        performance=await self.storage.get_sustainability_score()
                    )
                    await self.federated.get_global_verifications()
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
    # Public API (Enhanced with MOPD)
    # ============================================================================
    async def verify_and_retire_offset(
        self,
        credit_id: str,
        registry: OffsetRegistry,
        project_id: str,
        project_location: Dict[str, float],
        project_area_km2: float,
        amount_to_retire_kg: float,
        project_type: Optional[ProjectType] = None,
        use_ml_verification: bool = False,
        return_mopd: bool = False           # NEW
    ) -> Dict[str, Any]:
        result = await self.analyzer.verify_and_retire(
            credit_id, registry, project_id, project_location, project_area_km2,
            amount_to_retire_kg, project_type, use_ml_verification, return_mopd
        )

        # Trigger workflows on critical conditions
        if result.get('overall_success') is False and self.workflow_orchestrator:
            await self.workflow_orchestrator.execute_workflow(self.config.workflow_on_critical_alert)

        # Feed to MoE components
        if self.gating_network and self.expert_router:
            features = np.array([
                1.0 if result.get('overall_success') else 0.0,
                await self.storage.get_sustainability_score(),
                (result.get('carbon_position', {}).get('net_position_kg', 0) / self.config.carbon_budget_kg) if self.config.carbon_budget_kg > 0 else 0.0,
                len(await self.storage.get_records())
            ])
            reward = 1.0 if result.get('overall_success') else 0.0
            self.gating_network.update(features, reward, {'credit_id': credit_id})

        if self.self_evolving_gate and TORCH_AVAILABLE:
            state = torch.tensor([
                1.0 if result.get('overall_success') else 0.0,
                await self.storage.get_sustainability_score()
            ], dtype=torch.float32)
            self.self_evolving_gate.adapt(
                state=state,
                chosen_expert=0,
                reward=1.0 if result.get('overall_success') else 0.0,
                environmental_feedback={'credit_id': credit_id},
                quantum_mode=False
            )

        # Telemetry
        if self.telemetry:
            self.telemetry.increment('verifications_total')
            if result.get('overall_success'):
                self.telemetry.increment('verifications_success')
            self.telemetry.gauge('sustainability_score', await self.storage.get_sustainability_score())
            if return_mopd and 'mopd_pareto_front' in result:
                self.telemetry.increment('mopd_generations')
                self.telemetry.histogram('mopd_pareto_front_size', len(result['mopd_pareto_front']))

        logger.info(
            f"Offset verification complete: {credit_id} - "
            f"success={result.get('overall_success')}, "
            f"sustainability_score={await self.storage.get_sustainability_score():.2f}"
        )

        return result

    async def get_verification_summary(self) -> Dict[str, Any]:
        return await self.reporter.get_verification_summary()

    async def get_sustainability_report(self) -> Dict[str, Any]:
        return await self.reporter.get_sustainability_report()

    async def train_ml_model(self, training_data: Optional[List[Dict]] = None) -> Dict:
        return await self.analyzer.train_ml_model(training_data)

    async def train_predictive_model(self) -> Dict:
        return await self.analyzer.train_predictive_model()

    # ============================================================================
    # MOPD Public Methods (NEW)
    # ============================================================================
    async def get_offset_pareto_front(
        self,
        credit_id: str,
        registry: OffsetRegistry,
        project_id: str,
        project_location: Dict[str, float],
        project_area_km2: float,
        project_type: Optional[ProjectType] = None
    ) -> List[MOPDPlan]:
        """
        Generate Pareto front of offset strategies without actually retiring.
        Returns a list of MOPDPlan objects.
        """
        if not self.config.enable_mopd:
            return []
        pareto_front = await self.analyzer._generate_pareto_front_for_offset(
            credit_id, registry, project_id, project_location, project_area_km2, project_type
        )
        return pareto_front

    async def get_mopd_summary(self) -> Dict[str, Any]:
        """Return a summary of MOPD‑related metrics."""
        if not self.config.enable_mopd:
            return {'enabled': False}
        plans = await self.storage.get_mopd_plans(20)
        return {
            'enabled': True,
            'objective_weights': self.config.mopd.objective_weights,
            'grid_resolution': self.config.mopd.grid_resolution,
            'total_mopd_plans': len(await self.storage.get_mopd_plans()),
            'sample_plans': [p.to_dict() for p in plans]
        }

    async def share_with_swarm(self):
        if not self.config.enable_swarm_coordination or not self.swarm_coordinator:
            return
        stats = await self.storage.get_stats()
        payload = {
            'verifier_id': hashlib.md5(str(self.storage.verification_records).encode()).hexdigest()[:8],
            'sustainability_score': await self.storage.get_sustainability_score(),
            'total_verifications': stats['total'],
            'success_rate': stats['success_rate'],
            'carbon_position': self.accountant.get_current_position().__dict__,
            'helium_position': self.helium_tracker.get_position() if self.helium_tracker else {},
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

    # ============================================================================
    # Self‑Healing
    # ============================================================================
    async def self_heal(self):
        logger.info("Self‑healing started")
        if not self.config.self_healing.enabled:
            logger.warning("Self‑healing disabled")
            return

        self.accountant.carbon_budget_kg = self.config.carbon_budget_kg
        if self.helium_tracker:
            self.helium_tracker.budget_l = self.config.helium_budget_l

        await self.storage.update_sustainability_score(0.0)

        records = await self.storage.get_records()
        if len(records) > 10:
            async with self.storage._lock:
                self.storage.verification_records = records[-10:]

        self.health_status = "healthy"
        self.last_error = None
        await self.reporter.save_state()
        logger.info("Self‑healing completed")

    # ============================================================================
    # Health Status
    # ============================================================================
    async def get_health_status(self) -> Dict[str, Any]:
        stats = await self.storage.get_stats()
        return {
            'status': self.health_status,
            'last_error': self.last_error,
            'total_verifications': stats['total'],
            'success_rate': stats['success_rate'],
            'sustainability_score': await self.storage.get_sustainability_score(),
            'carbon_budget_remaining': self.accountant.get_current_position().carbon_budget_remaining_kg,
            'bio_integration_active': self.config.enable_bio_integration,
            'event_driven_active': self.config.enable_event_driven,
            'self_healing_enabled': self.config.self_healing.enabled,
            'persistence_enabled': self.config.persistence.enabled,
            'mopd_enabled': self.config.enable_mopd,
        }

    # ============================================================================
    # Shutdown
    # ============================================================================
    async def shutdown(self):
        logger.info("Shutting down Automated Carbon Offset Verification System")
        for task in self._background_tasks:
            task.cancel()
        await asyncio.gather(*self._background_tasks, return_exceptions=True)

        if self.accountant:
            await self.accountant.stop()
        if self.helium_tracker:
            await self.helium_tracker.stop()

        if self.persistence:
            await self.reporter.save_state()

        if self.carbon_manager:
            await self.carbon_manager.close()
        if self.federated:
            await self.federated.close()

        logger.info("Shutdown complete")
