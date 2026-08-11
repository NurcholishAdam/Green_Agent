# File: quantum_integration/quantum-limit-graph-v2.4.0/limit-agentbench/src/enhancements/moe_expert_system/advanced/circular_computing_manager.py
# Enhanced version v4.1.0 – Refactored for maintainability, concurrency, resilience, and MOPD support.

"""
Enhanced Circular Computing Module v4.1.0
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
    logger.warning("PyTorch not available; ML component selection will be disabled.")

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
    materials: Dict[MaterialType, float]  # grams
    manufacturing_carbon: float  # kg CO2
    current_state: HardwareState
    deployment_date: datetime
    expected_lifetime_days: int
    utilization_history: List[float] = field(default_factory=list)
    maintenance_log: List[Dict] = field(default_factory=list)
    sustainability_score: float = 0.0
    helium_content_l: float = 0.0
    carbon_savings_kg: float = 0.0

# ============================================================================
# MOPD Data Classes (NEW)
# ============================================================================
@dataclass
class MOPDPlan:
    """Represents a recycling strategy with its objective vector."""
    # Decision variables
    recycling_method: str               # 'full_recycling', 'repurposing', 'material_recovery'
    helium_recovery: bool               # whether to attempt helium recovery
    material_recovery_target: float     # target recovery rate (0-1)
    use_ml_optimization: bool
    # Objectives (to be minimised/maximised)
    cost: float
    carbon_saved_kg: float
    helium_recovered_l: float
    material_recovery_rate: float
    time_days: float
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
        'carbon_saved': 0.3,
        'helium_recovered': 0.2,
        'material_recovery': 0.15,
        'time': 0.15,
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
    input_size: int = 8
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
    path: str = "circular_computing_state.json"
    save_interval_seconds: int = 300

@dataclass
class SelfHealingConfig:
    enabled: bool = True

@dataclass
class CircularComputingConfig:
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

    # Budgets
    helium_budget_l: float = 100.0

    # Helium-to-CO2 equivalence factor (kg CO2 per kg helium)
    helium_to_co2_factor: float = 20.0

    # Retry parameters
    max_retries: int = 3
    retry_base_delay_ms: float = 100.0
    retry_max_delay_ms: float = 5000.0
    circuit_breaker_failure_threshold: int = 5
    circuit_breaker_recovery_timeout: float = 30.0

    # Workflow triggers
    workflow_on_critical_alert: str = "adjust_circular_strategy"
    workflow_on_slo_breach: str = "rebalance_materials"

    # Swarm sharing interval
    swarm_share_interval_seconds: int = 60

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
# Helium Lifecycle Manager (Improved)
# ============================================================================
class HeliumLifecycleManager:
    def __init__(self, config: HeliumConfig):
        self.config = config
        self.budget_l = config.budget_l
        self.usage: Deque[Dict] = deque(maxlen=86400)
        self.recovered: Deque[Dict] = deque(maxlen=86400)
        self.component_helium: Dict[str, Dict[str, Any]] = {}
        self._total_usage = 0.0
        self._total_recovered = 0.0
        self._lock = asyncio.Lock()
        self._task: Optional[asyncio.Task] = None
        self._accounting_loop_running = False
        self.recovery_rates = {
            'cooling_system': 0.85,
            'quantum_computer': 0.90,
            'cryogenic_system': 0.80,
            'standard_cooling': 0.75,
            'mri_system': 0.95
        }
        logger.info(f"HeliumLifecycleManager initialized: budget={self.budget_l}L")

    def register_component_helium(self, component_id: str, helium_content_l: float, component_type: str = 'cooling_system'):
        self.component_helium[component_id] = {
            'total_l': helium_content_l,
            'recovered_l': 0.0,
            'type': component_type,
            'recovery_rate': self.recovery_rates.get(component_type, 0.85),
            'registered_at': datetime.now(timezone.utc)
        }
        logger.info(f"Registered helium content for {component_id}: {helium_content_l}L")

    def track_usage(self, component_id: str, usage_l: float):
        self.usage.append({'component_id': component_id, 'amount_l': usage_l, 'timestamp': datetime.now(timezone.utc)})
        self._total_usage += usage_l
        if component_id in self.component_helium:
            self.component_helium[component_id]['used_l'] = self.component_helium[component_id].get('used_l', 0) + usage_l

    def calculate_recovery(self, component_id: str) -> float:
        if component_id not in self.component_helium:
            return 0.0
        comp = self.component_helium[component_id]
        total = comp['total_l']
        used = comp.get('used_l', 0)
        recovery_rate = comp['recovery_rate']
        remaining = total - used
        return max(0, remaining * recovery_rate)

    def record_recovery(self, component_id: str, amount_l: float):
        self.recovered.append({'component_id': component_id, 'amount_l': amount_l, 'timestamp': datetime.now(timezone.utc)})
        self._total_recovered += amount_l
        if component_id in self.component_helium:
            self.component_helium[component_id]['recovered_l'] += amount_l

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
            'co2_equivalent_kg': net * self.config.helium_to_co2_factor,
            'components': self.component_helium
        }

# ============================================================================
# Predictive Lifecycle Analyzer (Improved)
# ============================================================================
class PredictiveLifecycleAnalyzer:
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
            logger.info("PredictiveLifecycleAnalyzer initialized with SGD")
        else:
            logger.warning("sklearn not available; using moving average fallback")

    def update_history(self, data: Dict):
        self.history.append({
            'timestamp': datetime.now(timezone.utc),
            'age_days': data.get('age_days', 0),
            'utilization': data.get('utilization', 0.5),
            'maintenance_count': data.get('maintenance_count', 0),
            'carbon_score': data.get('carbon_score', 0.5),
            'helium_remaining': data.get('helium_remaining', 0.5)
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
                        data['age_days'] / 1000,
                        data['utilization'],
                        data['maintenance_count'] / 10,
                        data['carbon_score'],
                        data['helium_remaining']
                    ])
                X.append(features)
                y.append(hist_list[i + 5]['age_days'])

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

    async def predict_lifetime(self, component_data: Dict) -> Dict:
        if not self.is_trained or len(self.history) < 10:
            if self.history:
                recent = [h['age_days'] for h in list(self.history)[-5:]]
                pred = np.mean(recent) if recent else 365
                return {'predicted_days': pred, 'confidence': 0.3, 'trend': 'moving_average'}
            return {'predicted_days': 365, 'confidence': 0.0, 'trend': 'insufficient_data'}

        recent = list(self.history)[-5:]
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
            'predicted_days': max(0, prediction),
            'confidence': confidence,
            'trend': trend,
            'recommended_actions': self._generate_actions(prediction)
        }

    def _generate_actions(self, prediction: float) -> List[str]:
        if prediction < 100:
            return ["URGENT: Schedule component replacement", "Prioritize material recovery"]
        elif prediction < 365:
            return ["Plan for repurposing", "Optimize utilization"]
        elif prediction < 730:
            return ["Schedule preventive maintenance", "Monitor helium levels"]
        return ["Component health is good - maintain current practices"]

# ============================================================================
# ML Component Selector (PyTorch, with thread offload)
# ============================================================================
class MLComponentSelector:
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
            logger.info("MLComponentSelector initialized with PyTorch")
        else:
            logger.warning("PyTorch not available; ML component selection disabled")

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

    async def select_component(self, requirements: Dict) -> Dict[str, Any]:
        if not TORCH_AVAILABLE or not self.is_trained:
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
        if self.scaler is not None:
            features_scaled = self.scaler.transform(features)
        else:
            features_scaled = features

        self.model.eval()
        with torch.no_grad():
            output = self.model(torch.FloatTensor(features_scaled)).numpy()[0, 0]

        return {
            'score': float(output),
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
# Federated Circular Manager (Improved)
# ============================================================================
class FederatedCircularManager:
    def __init__(self, config: FederatedConfig):
        self.config = config
        self.server_url = config.server_url
        self.round = 0
        self.local_components = {}
        self.global_components = {}
        self.participants = []
        self.contribution_scores = {}
        self._lock = asyncio.Lock()
        self._session: Optional[aiohttp.ClientSession] = None
        self._circuit = CircuitBreaker(
            "federated_server",
            failure_threshold=3,
            recovery_timeout=30.0
        )
        logger.info("FederatedCircularManager initialized")

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

    async def send_local_components(self, participant_id: str, component_data: Dict, performance: float = 1.0) -> Dict:
        if not self.server_url:
            return {'status': 'local'}

        async def _send():
            for attempt in range(self.config.max_retries):
                try:
                    async with self._lock:
                        session = await self._get_session()
                        compressed = self._compress(component_data)
                        update = {
                            'participant_id': participant_id,
                            'round': self.round,
                            'component_data': compressed,
                            'performance': performance,
                            'sparsity_ratio': self.config.sparsity_ratio,
                            'timestamp': datetime.now(timezone.utc).isoformat()
                        }
                        async with session.post(
                            f"{self.server_url}/federated/circular",
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
                        ) as resp:
                            if resp.status == 200:
                                data = await resp.json()
                                self.global_components = data.get('components', {})
                                self.participants = data.get('participants', [])
                                return self.global_components
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
# Human-AI Collaborative Circular (Simplified)
# ============================================================================
class HumanAICollaborativeCircular:
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
            'acknowledgment': f"Feedback received on {feedback.get('topic', 'circular computing')}",
            'insights': [],
            'actions': [],
            'circular_insights': []
        }
        concern = feedback.get('concern')
        if concern == 'recycling':
            reflection['insights'].append("Recycling efficiency can be improved through material sorting")
            reflection['actions'].append("Implement automated material recovery")
        elif concern == 'helium':
            reflection['insights'].append("Helium recovery requires specialized handling")
            reflection['actions'].append("Implement helium capture systems")
        elif concern == 'lifecycle':
            reflection['insights'].append("Lifecycle extension reduces carbon footprint")
            reflection['actions'].append("Implement predictive maintenance")
        elif concern == 'carbon':
            reflection['circular_insights'].append("Carbon-aware hardware selection is critical")
            reflection['actions'].append("Integrate carbon intensity tracking")
        if 'suggestion' in feedback:
            reflection['actions'].append(f"Implementing suggestion: {feedback['suggestion']}")
        reflection['action_items'] = self._prioritize_actions(reflection['actions'])
        return reflection

    def _prioritize_actions(self, actions: List[str]) -> List[Dict]:
        priorities = []
        for action in actions:
            if any(kw in action.lower() for kw in ['urgent', 'critical']):
                priority, impact = 'high', 0.9
            elif any(kw in action.lower() for kw in ['recycling', 'circular']):
                priority, impact = 'high', 0.8
            elif any(kw in action.lower() for kw in ['helium']):
                priority, impact = 'medium', 0.6
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
# Persistence Manager (JSON with versioning)
# ============================================================================
class CircularComputingPersistenceManager:
    def __init__(self, config: PersistenceConfig):
        self.config = config
        self.path = config.path
        self._lock = asyncio.Lock()
        self._version = 2  # Bumped for MOPD
        logger.info(f"CircularComputingPersistenceManager initialized (path={self.path})")

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
class CircularComputingTelemetry:
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
# Storage Module (Enhanced with MOPD)
# ============================================================================
class CircularStorage:
    def __init__(self):
        self.components: Dict[str, HardwareComponent] = {}
        self.material_inventory: Dict[MaterialType, float] = defaultdict(float)
        self.recycling_history: List[Dict] = []
        self.circularity_score = 0.0
        self.waste_diversion_rate = 0.0
        self.material_recovery_rate = 0.0
        self.sustainability_score = 0.0
        self.mopd_plans: List[MOPDPlan] = []  # NEW: store MOPD plans
        self._lock = asyncio.Lock()

    async def add_component(self, component: HardwareComponent):
        async with self._lock:
            self.components[component.component_id] = component
            for mat, amount in component.materials.items():
                self.material_inventory[mat] += amount

    async def get_component(self, component_id: str) -> Optional[HardwareComponent]:
        async with self._lock:
            return self.components.get(component_id)

    async def get_components(self, state: Optional[HardwareState] = None) -> List[HardwareComponent]:
        async with self._lock:
            if state is None:
                return list(self.components.values())
            return [c for c in self.components.values() if c.current_state == state]

    async def update_component_state(self, component_id: str, new_state: HardwareState):
        async with self._lock:
            if component_id in self.components:
                self.components[component_id].current_state = new_state

    async def add_recycling_record(self, record: Dict):
        async with self._lock:
            self.recycling_history.append(record)
            if len(self.recycling_history) > 10000:
                self.recycling_history = self.recycling_history[-10000:]

    async def add_mopd_plan(self, plan: MOPDPlan):
        async with self._lock:
            self.mopd_plans.append(plan)
            if len(self.mopd_plans) > 10000:
                self.mopd_plans = self.mopd_plans[-10000:]

    async def update_metrics(self, circularity: float, waste: float, recovery: float):
        async with self._lock:
            self.circularity_score = circularity
            self.waste_diversion_rate = waste
            self.material_recovery_rate = recovery

    async def update_sustainability_score(self, score: float):
        async with self._lock:
            self.sustainability_score = score

    async def get_metrics(self) -> Dict[str, float]:
        async with self._lock:
            return {
                'circularity_score': self.circularity_score,
                'waste_diversion_rate': self.waste_diversion_rate,
                'material_recovery_rate': self.material_recovery_rate,
                'sustainability_score': self.sustainability_score
            }

    async def get_stats(self) -> Dict[str, Any]:
        async with self._lock:
            total = len(self.components)
            states = {s.value: 0 for s in HardwareState}
            for c in self.components.values():
                states[c.current_state.value] += 1
            total_recycled = states.get(HardwareState.RECYCLED.value, 0) + states.get(HardwareState.REPURPOSED.value, 0)
            return {
                'total_components': total,
                'components_by_state': states,
                'recycled_or_repurposed': total_recycled,
                'material_inventory': dict(self.material_inventory),
                'sustainability_score': self.sustainability_score
            }

    async def get_recycling_history(self, limit: Optional[int] = None) -> List[Dict]:
        async with self._lock:
            if limit:
                return self.recycling_history[-limit:]
            return self.recycling_history.copy()

    async def get_mopd_plans(self, limit: Optional[int] = None) -> List[MOPDPlan]:
        async with self._lock:
            if limit is not None:
                return self.mopd_plans[-limit:]
            return self.mopd_plans.copy()

# ============================================================================
# Analyzer Module (Enhanced with MOPD)
# ============================================================================
class CircularAnalyzer:
    def __init__(
        self,
        config: CircularComputingConfig,
        storage: CircularStorage,
        carbon_manager: Optional[CarbonIntensityManager],
        helium_manager: Optional[HeliumLifecycleManager],
        predictive: Optional[PredictiveLifecycleAnalyzer],
        ml_selector: Optional[MLComponentSelector],
        human_ai: Optional[HumanAICollaborativeCircular]
    ):
        self.config = config
        self.storage = storage
        self.carbon_manager = carbon_manager
        self.helium_manager = helium_manager
        self.predictive = predictive
        self.ml_selector = ml_selector
        self.human_ai = human_ai
        self._lock = asyncio.Lock()

    # ============================================================================
    # MOPD Methods (NEW)
    # ============================================================================
    async def _enumerate_recycling_strategies(
        self,
        component: HardwareComponent
    ) -> List[MOPDPlan]:
        """Generate all feasible recycling strategies for a component."""
        # Decision variables:
        # - recycling_method: 'full_recycling', 'repurposing', 'material_recovery'
        # - helium_recovery: True/False (if helium content > 0)
        # - material_recovery_target: 0.5, 0.75, 0.95
        # - use_ml_optimization: True/False

        recycling_methods = ['full_recycling', 'repurposing', 'material_recovery']
        helium_recovery_options = [False]
        if component.helium_content_l > 0:
            helium_recovery_options = [False, True]
        material_targets = [0.5, 0.75, 0.95]
        use_ml_options = [True, False]

        plans = []
        for method in recycling_methods:
            for hr in helium_recovery_options:
                for target in material_targets:
                    for use_ml in use_ml_options:
                        plan = MOPDPlan(
                            recycling_method=method,
                            helium_recovery=hr,
                            material_recovery_target=target,
                            use_ml_optimization=use_ml,
                            cost=0.0,
                            carbon_saved_kg=0.0,
                            helium_recovered_l=0.0,
                            material_recovery_rate=0.0,
                            time_days=0.0,
                            sustainability_score=0.0
                        )
                        plans.append(plan)
        return plans

    async def _compute_plan_objectives(
        self,
        plan: MOPDPlan,
        component: HardwareComponent
    ) -> MOPDPlan:
        """Calculate cost, carbon saved, helium recovered, material recovery, time for a given plan."""
        # Base estimates
        cost = 0.0
        carbon_saved = component.manufacturing_carbon * 0.8  # baseline for recycling
        helium_recovered = 0.0
        material_recovery = 0.0
        time_days = 0.0
        sustainability = 0.5

        # Adjust based on decision variables
        if plan.recycling_method == 'full_recycling':
            cost = 5.0
            carbon_saved = component.manufacturing_carbon * 0.8
            material_recovery = 0.95
            time_days = 10
        elif plan.recycling_method == 'repurposing':
            cost = 2.0
            carbon_saved = component.manufacturing_carbon * 0.5
            material_recovery = 0.7
            time_days = 5
        elif plan.recycling_method == 'material_recovery':
            cost = 3.0
            carbon_saved = component.manufacturing_carbon * 0.6
            material_recovery = plan.material_recovery_target
            time_days = 7

        # Helium recovery
        if plan.helium_recovery and self.helium_manager:
            helium_recovered = self.helium_manager.calculate_recovery(component.component_id)
            if helium_recovered > 0:
                cost += 2.0
                time_days += 3
                # Carbon saving from helium recovery (avoided extraction)
                carbon_saved += helium_recovered * 5.0  # approximate

        # ML optimization might improve efficiency
        if plan.use_ml_optimization and self.ml_selector:
            ml_result = await self.ml_selector.select_component({
                'age_days': (datetime.now(timezone.utc) - component.deployment_date).days,
                'utilization': np.mean(component.utilization_history[-50:]) if component.utilization_history else 0.5,
                'maintenance_count': len(component.maintenance_log),
                'carbon_footprint': component.manufacturing_carbon,
                'helium_content': component.helium_content_l,
                'recycling_potential': 0.8,
                'reliability': 0.9,
                'cost_efficiency': 0.7
            })
            if ml_result and ml_result.get('score', 0) > 0.5:
                material_recovery *= 1.05
                carbon_saved *= 1.1
                cost *= 0.95

        # Sustainability score (simple calculation)
        sustainability = (material_recovery * 0.4 +
                         (carbon_saved / component.manufacturing_carbon) * 0.3 +
                         (helium_recovered / component.helium_content_l if component.helium_content_l > 0 else 0) * 0.3)

        plan.cost = cost
        plan.carbon_saved_kg = carbon_saved
        plan.helium_recovered_l = helium_recovered
        plan.material_recovery_rate = min(1.0, material_recovery)
        plan.time_days = time_days
        plan.sustainability_score = min(1.0, max(0.0, sustainability))
        return plan

    async def _generate_pareto_front_for_recycling(
        self,
        component_id: str
    ) -> List[MOPDPlan]:
        """Generate Pareto front of recycling strategies."""
        component = await self.storage.get_component(component_id)
        if not component:
            return []

        plans = await self._enumerate_recycling_strategies(component)
        computed_plans = []
        for plan in plans:
            computed = await self._compute_plan_objectives(plan, component)
            computed_plans.append(computed)

        # Filter dominated plans
        objective_names = ['cost', 'carbon_saved_kg', 'helium_recovered_l', 'material_recovery_rate', 'time_days']
        # We minimise cost and time; maximise carbon_saved, helium_recovered, material_recovery
        pareto = []
        for i, p_i in enumerate(computed_plans):
            dominated = False
            for j, p_j in enumerate(computed_plans):
                if i == j:
                    continue
                # Build vectors: for max objectives, negate
                a_vec = [
                    p_i.cost,
                    -p_i.carbon_saved_kg,
                    -p_i.helium_recovered_l,
                    -p_i.material_recovery_rate,
                    p_i.time_days
                ]
                b_vec = [
                    p_j.cost,
                    -p_j.carbon_saved_kg,
                    -p_j.helium_recovered_l,
                    -p_j.material_recovery_rate,
                    p_j.time_days
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
        objective_names = ['cost', 'carbon_saved_kg', 'helium_recovered_l', 'material_recovery_rate', 'time_days']
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
                if key in ['cost', 'time_days']:  # minimise
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
    # Core Recycling Method (Enhanced with MOPD)
    # ============================================================================
    async def recycle_component(
        self,
        component_id: str,
        use_ml_optimization: bool = False,
        return_mopd: bool = False           # NEW: if True, return Pareto front
    ) -> Dict[str, Any]:
        component = await self.storage.get_component(component_id)
        if not component:
            return {'error': 'Component not found'}

        # MOPD: generate Pareto front if requested
        pareto_front = None
        best_plan = None
        if self.config.enable_mopd and return_mopd:
            pareto_front = await self._generate_pareto_front_for_recycling(component_id)
            if pareto_front:
                # Store MOPD plans
                for plan in pareto_front:
                    await self.storage.add_mopd_plan(plan)
                best_plan = self._select_best_from_pareto(pareto_front)
                if best_plan:
                    # Override decision variables based on best plan
                    # Use the best plan's parameters for actual recycling
                    use_ml_optimization = best_plan.use_ml_optimization
                    # We could also adjust other parameters, but for simplicity we just note it.

        # ML optimization (if requested or from best plan)
        ml_result = None
        if use_ml_optimization and self.ml_selector:
            ml_result = await self.ml_selector.select_component({
                'age_days': (datetime.now(timezone.utc) - component.deployment_date).days,
                'utilization': np.mean(component.utilization_history[-50:]) if component.utilization_history else 0.5,
                'maintenance_count': len(component.maintenance_log),
                'carbon_footprint': component.manufacturing_carbon,
                'helium_content': component.helium_content_l,
                'recycling_potential': 0.8,
                'reliability': 0.9,
                'cost_efficiency': 0.7
            })

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
            rate = recovery_rates.get(material, 0.9)
            recovered = amount * rate
            recovered_materials[material.value] = {
                'original_g': amount,
                'recovered_g': recovered,
                'recovery_rate': rate
            }
            async with self.storage._lock:
                self.storage.material_inventory[material] -= amount
                self.storage.material_inventory[material] += recovered
            total_recovery_rate += rate
        avg_recovery = total_recovery_rate / len(recovered_materials) if recovered_materials else 0

        carbon_saved = component.manufacturing_carbon * 0.8
        component.carbon_savings_kg = carbon_saved

        helium_recovered = 0.0
        if self.helium_manager:
            helium_recovered = self.helium_manager.calculate_recovery(component_id)
            if helium_recovered > 0:
                self.helium_manager.record_recovery(component_id, helium_recovered)
                recovered_materials['helium_recovered'] = {
                    'original_g': component.helium_content_l * 1000,
                    'recovered_g': helium_recovered * 1000,
                    'recovery_rate': 0.85
                }

        await self.storage.update_component_state(component_id, HardwareState.RECYCLED)

        sustainability = self._calc_sustainability(avg_recovery, carbon_saved, helium_recovered)
        await self.storage.update_sustainability_score(sustainability)

        record = {
            'component_id': component_id,
            'component_type': component.type,
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'materials_recovered': recovered_materials,
            'average_recovery_rate': avg_recovery,
            'carbon_saved_kg': carbon_saved,
            'helium_recovered_g': helium_recovered * 1000,
            'ml_optimization': ml_result,
            'sustainability_score': sustainability
        }
        await self.storage.add_recycling_record(record)
        await self._update_circularity_metrics()

        # Update predictive history
        if self.predictive:
            self.predictive.update_history({
                'age_days': (datetime.now(timezone.utc) - component.deployment_date).days,
                'utilization': np.mean(component.utilization_history[-50:]) if component.utilization_history else 0.5,
                'maintenance_count': len(component.maintenance_log),
                'carbon_score': 1.0 / (1.0 + component.manufacturing_carbon),
                'helium_remaining': component.helium_content_l
            })
            await self.predictive.train()

        if self.human_ai:
            record['human_ai_insights'] = await self.human_ai.get_insights()

        # Add MOPD info to record
        if self.config.enable_mopd and return_mopd:
            if pareto_front:
                record['mopd_pareto_front'] = [p.to_dict() for p in pareto_front]
            if best_plan:
                record['mopd_best_plan'] = best_plan.to_dict()

        logger.info(f"Recycled {component_id}: {avg_recovery:.1%} recovery, {carbon_saved:.2f} kg CO2 saved")
        return record

    def _calc_sustainability(self, recovery_rate: float, carbon_saved: float, helium_recovered: float) -> float:
        recovery_factor = recovery_rate
        carbon_factor = min(1.0, carbon_saved / 10)
        helium_factor = min(1.0, helium_recovered / 10)
        return min(1.0, max(0.0, recovery_factor * 0.4 + carbon_factor * 0.3 + helium_factor * 0.3))

    async def _update_circularity_metrics(self):
        stats = await self.storage.get_stats()
        total = stats['total_components']
        if total == 0:
            return
        recycled = stats['components_by_state'].get(HardwareState.RECYCLED.value, 0)
        repurposed = stats['components_by_state'].get(HardwareState.REPURPOSED.value, 0)
        circularity = (recycled + repurposed) / total
        history = await self.storage.get_recycling_history()
        if history:
            recovery = np.mean([h['average_recovery_rate'] for h in history])
        else:
            recovery = 0
        waste_diversion = circularity  # simplification
        await self.storage.update_metrics(circularity, waste_diversion, recovery)

    async def train_ml_model(self, training_data: Optional[List[Dict]] = None) -> Dict:
        if not self.ml_selector:
            return {'status': 'disabled'}
        if training_data is None:
            history = await self.storage.get_recycling_history(100)
            training_data = []
            for item in history:
                training_data.append({
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
        return await self.ml_selector.train(training_data)

    async def train_predictive_model(self) -> Dict:
        if not self.predictive:
            return {'status': 'disabled'}
        return await self.predictive.train()

    async def optimize_hardware_allocation(
        self,
        expert_requirements: Dict[str, Any],
        carbon_budget: float,
        helium_budget: float,
        use_ml: bool = False
    ) -> Dict[str, Any]:
        available = await self.storage.get_components(HardwareState.DEPLOYED)
        available += await self.storage.get_components(HardwareState.MAINTENANCE)
        if not available:
            return {'error': 'No available hardware', 'suggestion': 'deploy_new'}

        carbon_intensity = self.carbon_manager.carbon_intensity if self.carbon_manager else 400
        metrics = await self.storage.get_metrics()
        sustainability = metrics['sustainability_score']

        scored = []
        for comp in available:
            age = (datetime.now(timezone.utc) - comp.deployment_date).days
            lifecycle = 1.0 - (age / comp.expected_lifetime_days)
            lifecycle = max(lifecycle, 0.1)
            carbon_score = 1.0 / (1.0 + comp.manufacturing_carbon)
            helium_score = comp.helium_content_l / 100.0 if self.helium_manager else 0.5
            if comp.utilization_history:
                avg_util = np.mean(comp.utilization_history[-50:])
                util_score = 1.0 - avg_util
            else:
                util_score = 0.5

            if carbon_budget < 0.01:
                score = 0.3 * carbon_score + 0.25 * lifecycle + 0.25 * util_score + 0.2 * sustainability
            elif helium_budget < 0.01:
                score = 0.3 * helium_score + 0.25 * carbon_score + 0.25 * lifecycle + 0.2 * sustainability
            else:
                score = 0.2 * carbon_score + 0.2 * lifecycle + 0.2 * util_score + 0.2 * helium_score + 0.2 * sustainability
            scored.append((comp, score))

        scored.sort(key=lambda x: x[1], reverse=True)
        best = scored[0][0] if scored else None

        ml_result = None
        if best and use_ml and self.ml_selector:
            ml_result = await self.ml_selector.select_component({
                'age_days': (datetime.now(timezone.utc) - best.deployment_date).days,
                'utilization': np.mean(best.utilization_history[-50:]) if best.utilization_history else 0.5,
                'maintenance_count': len(best.maintenance_log),
                'carbon_footprint': best.manufacturing_carbon,
                'helium_content': best.helium_content_l,
                'recycling_potential': 0.8,
                'reliability': 0.9,
                'cost_efficiency': 0.7
            })

        return {
            'selected_component': best.component_id if best else None,
            'score': scored[0][1] if scored else 0,
            'component_type': best.type if best else None,
            'age_days': (datetime.now(timezone.utc) - best.deployment_date).days if best else 0,
            'manufacturing_carbon': best.manufacturing_carbon if best else 0,
            'helium_content_l': best.helium_content_l if best else 0,
            'carbon_intensity': carbon_intensity,
            'sustainability_score': sustainability,
            'ml_result': ml_result,
            'recommendation': 'use_existing' if scored and scored[0][1] > 0.5 else 'consider_repurposing'
        }

# ============================================================================
# Reporter Module (Enhanced with MOPD)
# ============================================================================
class CircularReporter:
    def __init__(
        self,
        config: CircularComputingConfig,
        storage: CircularStorage,
        analyzer: CircularAnalyzer,
        telemetry: Optional[CircularComputingTelemetry],
        persistence: Optional[CircularComputingPersistenceManager],
        human_ai: Optional[HumanAICollaborativeCircular],
        federated: Optional[FederatedCircularManager],
        predictive: Optional[PredictiveLifecycleAnalyzer],
        ml_selector: Optional[MLComponentSelector],
        helium_manager: Optional[HeliumLifecycleManager]
    ):
        self.config = config
        self.storage = storage
        self.analyzer = analyzer
        self.telemetry = telemetry
        self.persistence = persistence
        self.human_ai = human_ai
        self.federated = federated
        self.predictive = predictive
        self.ml_selector = ml_selector
        self.helium_manager = helium_manager
        self._lock = asyncio.Lock()

    async def get_circularity_report(self) -> Dict[str, Any]:
        stats = await self.storage.get_stats()
        metrics = await self.storage.get_metrics()
        history = await self.storage.get_recycling_history()

        material_flows = {}
        for material in MaterialType:
            total_in_use = sum(
                c.materials.get(material, 0)
                for c in (await self.storage.get_components())
                if c.current_state != HardwareState.RECYCLED
            )
            total_recovered = sum(
                r['materials_recovered'].get(material.value, {}).get('recovered_g', 0)
                for r in history
            )
            material_flows[material.value] = {
                'in_use_g': total_in_use,
                'recovered_g': total_recovered,
                'inventory_g': stats['material_inventory'].get(material.value, 0)
            }

        report = {
            'circularity_score': metrics['circularity_score'],
            'waste_diversion_rate': metrics['waste_diversion_rate'],
            'material_recovery_rate': metrics['material_recovery_rate'],
            'sustainability_score': metrics['sustainability_score'],
            'total_components': stats['total_components'],
            'components_by_state': stats['components_by_state'],
            'material_flows': material_flows,
            'total_carbon_saved_kg': sum(r['carbon_saved_kg'] for r in history),
            'helium_recovered_g': sum(r.get('helium_recovered_g', 0) for r in history)
        }

        if self.helium_manager:
            report['helium_position'] = self.helium_manager.get_position()

        if self.federated:
            report['federated_stats'] = self.federated.get_federated_stats()

        if self.predictive:
            forecast = await self.predictive.predict_lifetime({'age_days': 365, 'utilization': 0.5})
            report['predictive_forecast'] = forecast

        if self.ml_selector:
            report['ml_status'] = {
                'trained': self.ml_selector.is_trained,
                'model_version': 'v4.0.0',
                'training_samples': len(self.ml_selector.training_history)
            }

        if self.human_ai:
            report['human_ai_insights'] = await self.human_ai.get_insights()

        # MOPD summary
        if self.config.enable_mopd:
            mopd_plans = await self.storage.get_mopd_plans(20)
            report['mopd_plans'] = [p.to_dict() for p in mopd_plans]

        return report

    async def get_sustainability_report(self) -> Dict[str, Any]:
        metrics = await self.storage.get_metrics()
        return {
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'sustainability_score': metrics['sustainability_score'],
            'circularity_report': await self.get_circularity_report(),
            'recommendations': self._generate_recommendations()
        }

    def _generate_recommendations(self) -> List[str]:
        metrics = asyncio.run(self.storage.get_metrics())
        recs = []
        if metrics['sustainability_score'] < 0.5:
            recs.append("Improve circularity through better material recovery")
        if metrics['circularity_score'] < 0.5:
            recs.append("Increase component repurposing and recycling")
        if self.helium_manager:
            pos = self.helium_manager.get_position()
            if pos.get('remaining_budget_l', 0) < 0:
                recs.append("CRITICAL: Helium budget exceeded - implement recovery systems")
        if self.federated and len(self.federated.participants) < 2:
            recs.append("Increase federated participation for better circularity insights")
        if metrics['material_recovery_rate'] < 0.5:
            recs.append("Improve material recovery rate through better recycling processes")
        if self.config.enable_mopd:
            recs.append("Consider using MOPD to explore trade-offs among recycling strategies")
        return recs or ["All circularity metrics are within acceptable ranges"]

    async def export_telemetry(self):
        if self.telemetry:
            data = await self.telemetry.export()
            logger.debug(f"Telemetry export: {len(data)} bytes")

    async def save_state(self):
        if self.persistence:
            stats = await self.storage.get_stats()
            metrics = await self.storage.get_metrics()
            state = {
                'components': {
                    cid: asdict(c) for cid, c in (await self.storage.get_components()).items()
                },
                'material_inventory': stats['material_inventory'],
                'recycling_history': await self.storage.get_recycling_history(),
                'circularity_score': metrics['circularity_score'],
                'waste_diversion_rate': metrics['waste_diversion_rate'],
                'material_recovery_rate': metrics['material_recovery_rate'],
                'sustainability_score': metrics['sustainability_score'],
                'helium_manager_state': {
                    'usage': list(self.helium_manager.usage) if self.helium_manager else [],
                    'recovered': list(self.helium_manager.recovered) if self.helium_manager else [],
                    'component_helium': self.helium_manager.component_helium if self.helium_manager else {},
                    '_total_usage': self.helium_manager._total_usage if self.helium_manager else 0.0,
                    '_total_recovered': self.helium_manager._total_recovered if self.helium_manager else 0.0,
                } if self.helium_manager else None,
                'ml_checkpoint': self.analyzer.ml_selector.get_checkpoint() if self.analyzer.ml_selector else None,
                'mopd_plans': [p.to_dict() for p in await self.storage.get_mopd_plans()],  # NEW
            }
            await self.persistence.save_state(state)

    async def load_state(self):
        if self.persistence:
            state = await self.persistence.load_state()
            if state:
                # Restore components
                for cid, cdict in state.get('components', {}).items():
                    comp = HardwareComponent(**cdict)
                    await self.storage.add_component(comp)
                # Restore inventory
                async with self.storage._lock:
                    for mat, amount in state.get('material_inventory', {}).items():
                        self.storage.material_inventory[MaterialType(mat)] = amount
                # Restore recycling history
                for r in state.get('recycling_history', []):
                    await self.storage.add_recycling_record(r)
                # Restore metrics
                await self.storage.update_metrics(
                    state.get('circularity_score', 0.0),
                    state.get('waste_diversion_rate', 0.0),
                    state.get('material_recovery_rate', 0.0)
                )
                await self.storage.update_sustainability_score(state.get('sustainability_score', 0.0))
                # Restore helium manager
                he_state = state.get('helium_manager_state')
                if he_state and self.helium_manager:
                    self.helium_manager.usage = deque(he_state.get('usage', []), maxlen=86400)
                    self.helium_manager.recovered = deque(he_state.get('recovered', []), maxlen=86400)
                    self.helium_manager.component_helium = he_state.get('component_helium', {})
                    self.helium_manager._total_usage = he_state.get('_total_usage', 0.0)
                    self.helium_manager._total_recovered = he_state.get('_total_recovered', 0.0)
                # Restore ML checkpoint
                ml_cp = state.get('ml_checkpoint')
                if ml_cp and self.analyzer.ml_selector:
                    self.analyzer.ml_selector.load_checkpoint(ml_cp)
                # Restore MOPD plans
                mopd_plans = state.get('mopd_plans', [])
                for p_dict in mopd_plans:
                    await self.storage.add_mopd_plan(MOPDPlan.from_dict(p_dict))

# ============================================================================
# Main Controller (Enhanced with MOPD)
# ============================================================================
class CircularComputingManager:
    """
    Enhanced Circular Computing Manager v4.1.0
    Controller that orchestrates storage, analysis, reporting, and MOPD support.
    """

    def __init__(
        self,
        bio_core: Optional[EnhancedBioInspiredCore] = None,
        config: Optional[CircularComputingConfig] = None,
        **kwargs
    ):
        if config is None:
            config = CircularComputingConfig(**{k: v for k, v in kwargs.items() if k in CircularComputingConfig.__annotations__})
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
        self.helium_manager = HeliumLifecycleManager(self.config.helium) if self.config.helium.enabled else None
        self.predictive = PredictiveLifecycleAnalyzer(self.config.predictive) if self.config.predictive.enabled else None
        self.ml_selector = MLComponentSelector(self.config.ml) if self.config.ml.enabled else None
        self.federated = FederatedCircularManager(self.config.federated) if self.config.federated.enabled else None
        self.human_ai = HumanAICollaborativeCircular() if self.config.enable_human_ai else None
        self.telemetry = CircularComputingTelemetry() if self.config.telemetry.enabled else None
        self.persistence = CircularComputingPersistenceManager(self.config.persistence) if self.config.persistence.enabled else None

        # Storage, Analyzer, Reporter
        self.storage = CircularStorage()
        self.analyzer = CircularAnalyzer(
            self.config,
            self.storage,
            self.carbon_manager,
            self.helium_manager,
            self.predictive,
            self.ml_selector,
            self.human_ai
        )
        self.reporter = CircularReporter(
            self.config,
            self.storage,
            self.analyzer,
            self.telemetry,
            self.persistence,
            self.human_ai,
            self.federated,
            self.predictive,
            self.ml_selector,
            self.helium_manager
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
        if self.helium_manager:
            self.helium_manager.start()

        # Initialize material inventory (defaults are zero, handled in storage)
        # No explicit initialization needed; storage initializes with defaultdict

        # Subscribe to events
        if self.config.enable_event_driven and self.event_broker:
            self._subscribe_events()

        # Start background tasks
        self._start_background_tasks()

        # Load state
        if self.config.persistence.enabled:
            asyncio.create_task(self.reporter.load_state())

        logger.info("Circular Computing Manager v4.1.0 initialized with MOPD")

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
        if self.predictive:
            self.predictive.update_history({
                'age_days': 0,
                'utilization': 0.5,
                'maintenance_count': 0,
                'carbon_score': 1.0 / (1.0 + event.data.get('price', 50) / 50),
                'helium_remaining': 0.5
            })
        if intensity > 500:
            self.config.carbon_recycling_priority = 0.8  # custom attribute

    async def _on_helium_update(self, event: BioEvent):
        scarcity = event.data.get('scarcity', 0.5)
        if self.helium_manager:
            self.helium_manager.budget_l = self.config.helium_budget_l * (1.0 - scarcity * 0.3)
            self.helium_manager.config.helium_to_co2_factor = self.config.helium_to_co2_factor * (1.0 + 0.1 * scarcity)

    async def _on_alert_generated(self, event: BioEvent):
        if event.data.get('severity') == 'critical':
            logger.warning("Critical alert; triggering self‑healing")
            self.config.circularity_strategy = 'conservative'  # custom attribute
            if self.config.self_healing.enabled and self.self_healer:
                await self.self_healer.apply_healing('damage_accumulation')
            if self.workflow_orchestrator and self.config.workflow_on_critical_alert:
                await self.workflow_orchestrator.execute_workflow(self.config.workflow_on_critical_alert)

    async def _on_config_updated(self, event: BioEvent):
        updates = event.data.get('updates', {})
        if 'circular_computing' in updates:
            new = updates['circular_computing']
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
            self.config.carbon_recycling_priority = 0.9
        if event.data.get('metric') == 'helium_scarcity':
            if self.helium_manager:
                self.helium_manager.budget_l *= 0.8

    # ============================================================================
    # Background Tasks (cancellable)
    # ============================================================================
    def _start_background_tasks(self):
        # Event consumer
        if self.config.enable_event_driven:
            self._event_consumer_task = asyncio.create_task(self._event_consumer())
            self._background_tasks.append(self._event_consumer_task)

        # Carbon update loop
        if self.carbon_manager:
            t = asyncio.create_task(self._carbon_update_loop())
            self._background_tasks.append(t)

        # Predictive training loop
        if self.predictive:
            t = asyncio.create_task(self._predictive_update_loop())
            self._background_tasks.append(t)

        # ML training loop
        if self.ml_selector:
            t = asyncio.create_task(self._ml_training_loop())
            self._background_tasks.append(t)

        # Federated sync
        if self.federated:
            t = asyncio.create_task(self._federated_sync_loop())
            self._background_tasks.append(t)

        # Telemetry export
        if self.telemetry:
            t = asyncio.create_task(self._telemetry_export_loop())
            self._background_tasks.append(t)

        # Persistence save
        if self.persistence:
            t = asyncio.create_task(self._persistence_save_loop())
            self._background_tasks.append(t)

        # Swarm update
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
                if self.ml_selector:
                    history = await self.storage.get_recycling_history(100)
                    if len(history) >= 20:
                        training_data = []
                        for item in history:
                            training_data.append({
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
                        await self.ml_selector.train(training_data)
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
                    metrics = await self.storage.get_metrics()
                    pid = f"circular_{hashlib.md5(str(self.storage.components).encode()).hexdigest()[:8]}"
                    await self.federated.send_local_components(
                        pid,
                        {
                            'total_components': stats['total_components'],
                            'circularity_score': metrics['circularity_score'],
                            'waste_diversion_rate': metrics['waste_diversion_rate'],
                            'sustainability_score': metrics['sustainability_score'],
                            'timestamp': datetime.now(timezone.utc).isoformat()
                        },
                        performance=metrics['sustainability_score']
                    )
                    await self.federated.get_global_components()
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
    # Public API – Delegated to Analyzer and Reporter (Enhanced with MOPD)
    # ============================================================================
    async def register_component(
        self,
        component_type: str,
        materials: Dict[MaterialType, float],
        manufacturing_carbon: float,
        expected_lifetime_days: int = 1825,
        helium_content_l: float = 0.0
    ) -> str:
        return await self.analyzer.register_component(
            component_type, materials, manufacturing_carbon,
            expected_lifetime_days, helium_content_l
        )

    async def deploy_component(self, component_id: str):
        await self.analyzer.deploy_component(component_id)

    async def record_utilization(self, component_id: str, utilization_rate: float):
        await self.analyzer.record_utilization(component_id, utilization_rate)

    async def recycle_component(
        self,
        component_id: str,
        use_ml_optimization: bool = False,
        return_mopd: bool = False           # NEW
    ) -> Dict[str, Any]:
        result = await self.analyzer.recycle_component(component_id, use_ml_optimization, return_mopd)

        # Feed to MoE components
        if self.gating_network and self.expert_router:
            metrics = await self.storage.get_metrics()
            features = np.array([
                result.get('average_recovery_rate', 0),
                result.get('carbon_saved_kg', 0) / 10,
                result.get('helium_recovered_g', 0) / 1000,
                metrics['sustainability_score']
            ])
            reward = metrics['sustainability_score']
            self.gating_network.update(features, reward, {'component_id': component_id})

        if self.self_evolving_gate and TORCH_AVAILABLE:
            state = torch.tensor([
                result.get('carbon_saved_kg', 0),
                result.get('helium_recovered_g', 0) / 1000
            ], dtype=torch.float32)
            self.self_evolving_gate.adapt(
                state=state,
                chosen_expert=0,
                reward=await self.storage.get_metrics()['sustainability_score'],
                environmental_feedback={'component_id': component_id},
                quantum_mode=False
            )

        # Telemetry
        if self.telemetry:
            self.telemetry.increment('recycles_performed')
            self.telemetry.gauge('carbon_saved', result.get('carbon_saved_kg', 0))
            self.telemetry.gauge('sustainability_score', await self.storage.get_metrics()['sustainability_score'])
            if return_mopd and 'mopd_pareto_front' in result:
                self.telemetry.increment('mopd_generations')
                self.telemetry.histogram('mopd_pareto_front_size', len(result['mopd_pareto_front']))

        # Trigger workflow if sustainability score is low
        if (await self.storage.get_metrics())['sustainability_score'] < 0.4 and self.workflow_orchestrator:
            await self.workflow_orchestrator.execute_workflow(self.config.workflow_on_slo_breach)

        logger.info(f"Recycled component {component_id}: {result.get('average_recovery_rate', 0):.1%} recovery, {result.get('carbon_saved_kg', 0):.2f} kg CO2 saved")
        return result

    async def get_circularity_report(self) -> Dict[str, Any]:
        return await self.reporter.get_circularity_report()

    async def get_sustainability_report(self) -> Dict[str, Any]:
        return await self.reporter.get_sustainability_report()

    async def optimize_hardware_allocation(
        self,
        expert_requirements: Dict[str, Any],
        carbon_budget: float,
        helium_budget: float,
        use_ml: bool = False
    ) -> Dict[str, Any]:
        return await self.analyzer.optimize_hardware_allocation(
            expert_requirements, carbon_budget, helium_budget, use_ml
        )

    async def train_ml_model(self, training_data: Optional[List[Dict]] = None) -> Dict:
        return await self.analyzer.train_ml_model(training_data)

    async def train_predictive_model(self) -> Dict:
        return await self.analyzer.train_predictive_model()

    # ============================================================================
    # MOPD Public Methods (NEW)
    # ============================================================================
    async def get_recycling_pareto_front(
        self,
        component_id: str
    ) -> List[MOPDPlan]:
        """
        Generate Pareto front of recycling strategies for a given component.
        Returns a list of MOPDPlan objects.
        """
        if not self.config.enable_mopd:
            return []
        pareto_front = await self.analyzer._generate_pareto_front_for_recycling(component_id)
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
        metrics = await self.storage.get_metrics()
        payload = {
            'manager_id': hashlib.md5(str(self.storage.components).encode()).hexdigest()[:8],
            'sustainability_score': metrics['sustainability_score'],
            'circularity_score': metrics['circularity_score'],
            'total_components': stats['total_components'],
            'material_recovery_rate': metrics['material_recovery_rate'],
            'helium_position': self.helium_manager.get_position() if self.helium_manager else {},
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

        # Reset helium budget
        if self.helium_manager:
            self.helium_manager.budget_l = self.config.helium_budget_l
        self.config.circularity_strategy = 'balanced'

        # Reset sustainability score
        await self.storage.update_sustainability_score(0.0)

        # Trim components and recycling history
        components = await self.storage.get_components()
        if len(components) > 10:
            # Keep newest components
            sorted_comp = sorted(components, key=lambda c: c.deployment_date)
            for c in sorted_comp[:-10]:
                async with self.storage._lock:
                    del self.storage.components[c.component_id]
        history = await self.storage.get_recycling_history()
        if len(history) > 10:
            async with self.storage._lock:
                self.storage.recycling_history = history[-10:]

        # Reset health status
        self.health_status = "healthy"
        self.last_error = None

        # Save state
        await self.reporter.save_state()
        logger.info("Self‑healing completed")

    # ============================================================================
    # Health Status
    # ============================================================================
    async def get_health_status(self) -> Dict[str, Any]:
        stats = await self.storage.get_stats()
        metrics = await self.storage.get_metrics()
        return {
            'status': self.health_status,
            'last_error': self.last_error,
            'total_components': stats['total_components'],
            'circularity_score': metrics['circularity_score'],
            'sustainability_score': metrics['sustainability_score'],
            'material_recovery_rate': metrics['material_recovery_rate'],
            'bio_integration_active': self.config.enable_bio_integration,
            'event_driven_active': self.config.enable_event_driven,
            'self_healing_enabled': self.config.self_healing.enabled,
            'swarm_coordination_active': self.config.enable_swarm_coordination,
            'persistence_enabled': self.config.persistence.enabled,
            'mopd_enabled': self.config.enable_mopd,
        }

    # ============================================================================
    # Shutdown
    # ============================================================================
    async def shutdown(self):
        logger.info("Shutting down Circular Computing Manager")
        # Cancel background tasks
        for task in self._background_tasks:
            task.cancel()
        await asyncio.gather(*self._background_tasks, return_exceptions=True)

        # Stop helium accounting
        if self.helium_manager:
            await self.helium_manager.stop()

        # Save final state
        if self.persistence:
            await self.reporter.save_state()

        # Close external sessions
        if self.carbon_manager:
            await self.carbon_manager.close()
        if self.federated:
            await self.federated.close()

        logger.info("Shutdown complete")
