# File: quantum_integration/quantum-limit-graph-v2.4.0/limit-agentbench/src/enhancements/moe_expert_system/advanced/carbon_sequestration_manager.py
# Enhanced version v4.1.0 – Refactored for maintainability, concurrency, resilience, and MOPD support.

"""
Enhanced Carbon Sequestration and Offset Integration v4.1.0
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
    logger.warning("PyTorch not available; ML project selection will be disabled.")

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
# Enums and Data Classes
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
# MOPD Data Classes (NEW)
# ============================================================================
@dataclass
class MOPDPlan:
    """Represents a sequestration strategy with its objective vector."""
    # Decision variables
    offset_strategy: str                 # 'proactive', 'reactive', 'conservative'
    use_ml_selection: bool
    selected_projects: List[str]
    urgency: str                         # 'critical', 'normal', 'opportunistic'
    # Objectives (to be minimised/maximised)
    cost: float
    carbon_offset_kg: float
    helium_impact_l: float               # positive means offset (good)
    permanence_years: float
    verification_confidence: float
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
        'carbon_offset': 0.3,
        'helium_impact': 0.2,
        'permanence': 0.15,
        'verification_confidence': 0.15,
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
    path: str = "carbon_sequestration_state.json"
    save_interval_seconds: int = 300

@dataclass
class SelfHealingConfig:
    enabled: bool = True

@dataclass
class CarbonSequestrationConfig:
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
    carbon_budget_kg: float = 1000.0
    helium_budget_l: float = 100.0

    # Offset strategy
    offset_strategy: str = 'proactive'  # 'proactive', 'reactive', 'conservative'

    # Helium-to-CO2 equivalence factor (kg CO2 per kg helium)
    helium_to_co2_factor: float = 20.0

    # Retry parameters
    max_retries: int = 3
    retry_base_delay_ms: float = 100.0
    retry_max_delay_ms: float = 5000.0
    circuit_breaker_failure_threshold: int = 5
    circuit_breaker_recovery_timeout: float = 30.0

    # Workflow triggers
    workflow_on_critical_alert: str = "adjust_offset_strategy"
    workflow_on_slo_breach: str = "rebalance_carbon_budget"

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
# Helium Sequestration Manager (Improved)
# ============================================================================
class HeliumSequestrationManager:
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
        self.sequestration_projects = {
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
        logger.info(f"HeliumSequestrationManager initialized: budget={self.budget_l}L")

    def record_emission(self, amount_l: float, source: str = "unknown"):
        self.emissions.append({'amount_l': amount_l, 'source': source, 'timestamp': datetime.now(timezone.utc)})
        self._total_emissions += amount_l

    def record_offset(self, amount_l: float, project_id: str = None):
        self.offsets.append({'amount_l': amount_l, 'project_id': project_id or 'unknown', 'timestamp': datetime.now(timezone.utc)})
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
            'co2_equivalent_kg': net * self.config.helium_to_co2_factor,
            'projects': self.sequestration_projects
        }

    def calculate_helium_offset_from_carbon(self, carbon_credit_kg: float) -> float:
        return carbon_credit_kg * 0.05

    def select_helium_project(self, amount_l: float) -> Dict[str, Any]:
        scored_projects = []
        for project_id, project in self.sequestration_projects.items():
            cost_score = 1.0 / (1.0 + project['cost_per_l'])
            capacity_score = min(project['capacity_l_per_year'] / max(amount_l, 1), 1.0)
            efficiency_score = project['efficiency']
            score = 0.4 * cost_score + 0.3 * capacity_score + 0.3 * efficiency_score
            scored_projects.append((project_id, score, project))
        scored_projects.sort(key=lambda x: x[1], reverse=True)
        if scored_projects:
            return {'project_id': scored_projects[0][0], 'project': scored_projects[0][2], 'score': scored_projects[0][1]}
        return {'project_id': None, 'project': None, 'score': 0.0}

# ============================================================================
# Predictive Sequestration Analyzer (Improved)
# ============================================================================
class PredictiveSequestrationAnalyzer:
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
            logger.info("PredictiveSequestrationAnalyzer initialized with SGD")
        else:
            logger.warning("sklearn not available; using moving average fallback")

    def update_history(self, data: Dict):
        self.history.append({
            'timestamp': datetime.now(timezone.utc),
            'offset_amount': data.get('offset_amount', 0),
            'credit_price': data.get('credit_price', 50),
            'project_success_rate': data.get('project_success_rate', 0.9),
            'verification_confidence': data.get('verification_confidence', 0.7),
            'carbon_intensity': data.get('carbon_intensity', 400)
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
                        data['offset_amount'] / 1000,
                        data['credit_price'] / 100,
                        data['project_success_rate'],
                        data['verification_confidence'],
                        data['carbon_intensity'] / 100
                    ])
                X.append(features)
                y.append(hist_list[i + 5]['offset_amount'])

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

    async def predict_demand(self) -> Dict:
        if not self.is_trained or len(self.history) < 10:
            if self.history:
                recent = [h['offset_amount'] for h in list(self.history)[-5:]]
                pred = np.mean(recent) if recent else 1000
                return {'predicted_demand': pred, 'confidence': 0.3, 'trend': 'moving_average'}
            return {'predicted_demand': 1000, 'confidence': 0.0, 'trend': 'insufficient_data'}

        recent = list(self.history)[-5:]
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
            'predicted_demand': max(0, prediction),
            'confidence': confidence,
            'trend': trend,
            'recommended_actions': self._generate_actions(prediction)
        }

    def _generate_actions(self, prediction: float) -> List[str]:
        if prediction > 5000:
            return ["Increase sequestration project capacity", "Diversify project portfolio"]
        elif prediction < 1000:
            return ["Reduce sequestration spending", "Focus on high-impact projects"]
        return ["Maintain current sequestration strategy"]

# ============================================================================
# ML Project Selector (PyTorch, with thread offload)
# ============================================================================
class MLProjectSelector:
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
            logger.info("MLProjectSelector initialized with PyTorch")
        else:
            logger.warning("PyTorch not available; ML project selection disabled")

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
                    nn.Linear(hidden_size // 2, 5)  # 5 project types
                )

            def forward(self, x):
                return self.network(x)

        self.model = ProjectSelector(self.input_size, self.hidden_size)
        self.optimizer = optim.Adam(self.model.parameters(), lr=0.001)
        self.criterion = nn.CrossEntropyLoss()

    async def train(self, training_data: List[Dict], epochs: Optional[int] = None) -> Dict:
        if not TORCH_AVAILABLE or not self.model:
            return {'status': 'disabled'}
        if len(training_data) < 20:
            return {'status': 'insufficient_data', 'samples': len(training_data)}

        epochs = epochs or self.config.epochs
        project_types = ['reforestation', 'dac', 'biochar', 'ocean_based', 'helium_recovery']

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
            selected = item.get('selected_project', 'reforestation')
            idx = project_types.index(selected) if selected in project_types else 0
            y.append(idx)

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
            torch.LongTensor(y)
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

    async def select_projects(self, criteria: Dict) -> List[Dict[str, Any]]:
        if not TORCH_AVAILABLE or not self.is_trained:
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
        if self.scaler is not None:
            features_scaled = self.scaler.transform(features)
        else:
            features_scaled = features

        self.model.eval()
        with torch.no_grad():
            output = self.model(torch.FloatTensor(features_scaled)).numpy()[0]

        project_types = ['reforestation', 'dac', 'biochar', 'ocean_based', 'helium_recovery']
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
# Federated Sequestration Manager (Improved)
# ============================================================================
class FederatedSequestrationManager:
    def __init__(self, config: FederatedConfig):
        self.config = config
        self.server_url = config.server_url
        self.round = 0
        self.local_projects = {}
        self.global_projects = {}
        self.participants = []
        self.contribution_scores = {}
        self._lock = asyncio.Lock()
        self._session: Optional[aiohttp.ClientSession] = None
        self._circuit = CircuitBreaker(
            "federated_server",
            failure_threshold=3,
            recovery_timeout=30.0
        )
        logger.info("FederatedSequestrationManager initialized")

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

    async def send_local_projects(self, participant_id: str, project_data: Dict, performance: float = 1.0) -> Dict:
        if not self.server_url:
            return {'status': 'local'}

        async def _send():
            for attempt in range(self.config.max_retries):
                try:
                    async with self._lock:
                        session = await self._get_session()
                        compressed = self._compress(project_data)
                        update = {
                            'participant_id': participant_id,
                            'round': self.round,
                            'project_data': compressed,
                            'performance': performance,
                            'sparsity_ratio': self.config.sparsity_ratio,
                            'timestamp': datetime.now(timezone.utc).isoformat()
                        }
                        async with session.post(
                            f"{self.server_url}/federated/sequestration",
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

    async def get_global_projects(self) -> Optional[Dict]:
        if not self.server_url:
            return self.global_projects

        async def _fetch():
            for attempt in range(self.config.max_retries):
                try:
                    async with self._lock:
                        session = await self._get_session()
                        async with session.get(
                            f"{self.server_url}/federated/sequestration/global",
                            timeout=30
                        ) as resp:
                            if resp.status == 200:
                                data = await resp.json()
                                self.global_projects = data.get('projects', {})
                                self.participants = data.get('participants', [])
                                return self.global_projects
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
# Human-AI Collaborative Sequestration (Simplified)
# ============================================================================
class HumanAICollaborativeSequestration:
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
            'acknowledgment': f"Feedback received on {feedback.get('topic', 'carbon sequestration')}",
            'insights': [],
            'actions': [],
            'sequestration_insights': []
        }
        concern = feedback.get('concern')
        if concern == 'cost':
            reflection['insights'].append("Cost optimization can be improved through project diversity")
            reflection['actions'].append("Implement cost-aware project selection")
        elif concern == 'permanence':
            reflection['insights'].append("Long-term permanence requires multi-decade planning")
            reflection['actions'].append("Prioritize high-permanence projects")
        elif concern == 'verification':
            reflection['insights'].append("Verification accuracy needs improvement")
            reflection['actions'].append("Implement enhanced verification methods")
        elif concern == 'helium':
            reflection['sequestration_insights'].append("Helium offset integration is needed")
            reflection['actions'].append("Implement helium sequestration projects")
        if 'suggestion' in feedback:
            reflection['actions'].append(f"Implementing suggestion: {feedback['suggestion']}")
        reflection['action_items'] = self._prioritize_actions(reflection['actions'])
        return reflection

    def _prioritize_actions(self, actions: List[str]) -> List[Dict]:
        priorities = []
        for action in actions:
            if any(kw in action.lower() for kw in ['urgent', 'critical']):
                priority, impact = 'high', 0.9
            elif any(kw in action.lower() for kw in ['sequestration', 'carbon']):
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
class CarbonSequestrationPersistenceManager:
    def __init__(self, config: PersistenceConfig):
        self.config = config
        self.path = config.path
        self._lock = asyncio.Lock()
        self._version = 2  # Bumped for MOPD
        logger.info(f"CarbonSequestrationPersistenceManager initialized (path={self.path})")

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
class CarbonSequestrationTelemetry:
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
class CarbonSequestrationStorage:
    def __init__(self):
        self.credits: List[CarbonCredit] = []
        self.transaction_history: List[Dict] = []
        self.sequestration_projects: Dict[str, Dict] = {}
        self.sustainability_score = 0.0
        self.total_sequestered = 0.0
        self.total_offset = 0.0
        self.mopd_plans: List[MOPDPlan] = []  # NEW: store MOPD plans
        self._lock = asyncio.Lock()

    async def add_credit(self, credit: CarbonCredit):
        async with self._lock:
            self.credits.append(credit)
            if len(self.credits) > 10000:
                self.credits = self.credits[-10000:]

    async def add_transaction(self, transaction: Dict):
        async with self._lock:
            self.transaction_history.append(transaction)
            if len(self.transaction_history) > 10000:
                self.transaction_history = self.transaction_history[-10000:]

    async def add_mopd_plan(self, plan: MOPDPlan):
        async with self._lock:
            self.mopd_plans.append(plan)
            if len(self.mopd_plans) > 10000:
                self.mopd_plans = self.mopd_plans[-10000:]

    async def update_sustainability_score(self, score: float):
        async with self._lock:
            self.sustainability_score = score

    async def update_totals(self, offset: float, sequestered: float):
        async with self._lock:
            self.total_offset += offset
            self.total_sequestered += sequestered

    async def get_credits(self, limit: Optional[int] = None) -> List[CarbonCredit]:
        async with self._lock:
            if limit is not None:
                return self.credits[-limit:]
            return self.credits.copy()

    async def get_transaction_history(self, limit: Optional[int] = None) -> List[Dict]:
        async with self._lock:
            if limit is not None:
                return self.transaction_history[-limit:]
            return self.transaction_history.copy()

    async def get_sustainability_score(self) -> float:
        async with self._lock:
            return self.sustainability_score

    async def get_totals(self) -> Dict[str, float]:
        async with self._lock:
            return {'offset': self.total_offset, 'sequestered': self.total_sequestered}

    async def get_projects(self) -> Dict[str, Dict]:
        async with self._lock:
            return dict(self.sequestration_projects)

    async def set_projects(self, projects: Dict[str, Dict]):
        async with self._lock:
            self.sequestration_projects = projects

    async def get_stats(self) -> Dict[str, Any]:
        async with self._lock:
            return {
                'total_credits': len(self.credits),
                'verified_credits': sum(1 for c in self.credits if c.is_verified),
                'total_offset_kg': self.total_offset,
                'total_sequestered_kg': self.total_sequestered,
                'sustainability_score': self.sustainability_score
            }

    async def get_mopd_plans(self, limit: Optional[int] = None) -> List[MOPDPlan]:
        async with self._lock:
            if limit is not None:
                return self.mopd_plans[-limit:]
            return self.mopd_plans.copy()

# ============================================================================
# Analyzer Module (Enhanced with MOPD)
# ============================================================================
class CarbonSequestrationAnalyzer:
    def __init__(
        self,
        config: CarbonSequestrationConfig,
        storage: CarbonSequestrationStorage,
        carbon_manager: Optional[CarbonIntensityManager],
        helium_manager: Optional[HeliumSequestrationManager],
        predictive: Optional[PredictiveSequestrationAnalyzer],
        ml_selector: Optional[MLProjectSelector],
        human_ai: Optional[HumanAICollaborativeSequestration]
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
    async def _enumerate_strategies(
        self,
        expert_carbon_kg: float,
        budget_remaining: float,
        urgency: str = 'normal'
    ) -> List[MOPDPlan]:
        """Generate all feasible sequestration strategies."""
        # Decision variables:
        # - offset_strategy: proactive, reactive, conservative
        # - use_ml_selection: True/False
        # - urgency: critical, normal, opportunistic (from input)

        strategy_options = ['proactive', 'reactive', 'conservative']
        use_ml_options = [True, False]

        # For simplicity, we also vary urgency in the plan generation, but the caller passes it
        # We'll keep urgency as a decision variable for exploration.
        urgency_options = ['critical', 'normal', 'opportunistic']

        plans = []
        for strategy in strategy_options:
            for use_ml in use_ml_options:
                for ur in urgency_options:
                    plan = MOPDPlan(
                        offset_strategy=strategy,
                        use_ml_selection=use_ml,
                        selected_projects=[],  # will be computed later
                        urgency=ur,
                        cost=0.0,
                        carbon_offset_kg=0.0,
                        helium_impact_l=0.0,
                        permanence_years=0.0,
                        verification_confidence=0.0,
                        sustainability_score=0.0
                    )
                    plans.append(plan)
        return plans

    async def _compute_plan_objectives(
        self,
        plan: MOPDPlan,
        expert_carbon_kg: float,
        budget_remaining: float
    ) -> MOPDPlan:
        """Calculate cost, carbon offset, helium impact, permanence, confidence for a given plan."""
        # Determine offset amount based on strategy
        if plan.offset_strategy == 'proactive':
            offset_amount = max(expert_carbon_kg, expert_carbon_kg * 1.2)
        elif plan.offset_strategy == 'reactive':
            offset_amount = max(expert_carbon_kg, expert_carbon_kg)
        else:  # conservative
            offset_amount = min(expert_carbon_kg, expert_carbon_kg * 0.5)

        # Select projects based on urgency and ML usage
        if plan.use_ml_selection and self.ml_selector:
            ml_results = await self.ml_selector.select_projects({
                'carbon_intensity': self.carbon_manager.carbon_intensity if self.carbon_manager else 400,
                'cost_budget': min(1.0, budget_remaining / 1000),
                'urgency': {'critical': 0.9, 'normal': 0.5, 'opportunistic': 0.2}.get(plan.urgency, 0.5),
                'permanence_requirement': 0.6 if plan.urgency == 'critical' else 0.4,
                'co_benefit_weight': 0.5,
                'verification_confidence': 0.7,
                'project_age_months': 12,
                'historical_success': 0.9
            })
            selected_projects = [r['project_type'] for r in ml_results[:3] if r['score'] > 0.3]
            project_map = {
                'reforestation': 'reforestation_tropical',
                'dac': 'direct_air_capture',
                'biochar': 'biochar_agriculture',
                'ocean_based': 'ocean_alkalinization',
                'helium_recovery': 'helium_recovery_advanced'
            }
            plan.selected_projects = [project_map.get(p, p) for p in selected_projects if p in project_map]
        else:
            plan.selected_projects = self._select_projects(offset_amount, plan.urgency)

        # Allocate offset across projects
        allocation = self._allocate_offset(offset_amount, plan.selected_projects)
        total_cost = sum(a['cost'] for a in allocation.values())
        total_permanence = np.mean([a['permanence_years'] for a in allocation.values()]) if allocation else 0
        total_confidence = 0.7  # placeholder, could be derived from verification

        # Helium impact: positive means offset (good)
        helium_offset = 0
        if self.helium_manager:
            helium_offset = self.helium_manager.calculate_helium_offset_from_carbon(offset_amount)

        # Sustainability score (simplified)
        sustainability = self._calculate_sustainability_score(offset_amount, expert_carbon_kg, 400)

        plan.cost = total_cost
        plan.carbon_offset_kg = offset_amount
        plan.helium_impact_l = helium_offset
        plan.permanence_years = total_permanence
        plan.verification_confidence = total_confidence
        plan.sustainability_score = sustainability
        return plan

    async def _generate_pareto_front_for_offset(
        self,
        expert_carbon_kg: float,
        budget_remaining: float,
        urgency: str = 'normal'
    ) -> List[MOPDPlan]:
        """Generate Pareto front of sequestration strategies."""
        plans = await self._enumerate_strategies(expert_carbon_kg, budget_remaining, urgency)
        computed_plans = []
        for plan in plans:
            computed = await self._compute_plan_objectives(plan, expert_carbon_kg, budget_remaining)
            computed_plans.append(computed)

        # Filter dominated plans
        objective_names = ['cost', 'carbon_offset_kg', 'helium_impact_l', 'permanence_years', 'verification_confidence']
        # We minimise cost; maximise carbon_offset, helium_impact, permanence, confidence
        pareto = []
        for i, p_i in enumerate(computed_plans):
            dominated = False
            for j, p_j in enumerate(computed_plans):
                if i == j:
                    continue
                # Build vectors: for max objectives, we negate
                a_vec = [
                    p_i.cost,
                    -p_i.carbon_offset_kg,
                    -p_i.helium_impact_l,
                    -p_i.permanence_years,
                    -p_i.verification_confidence
                ]
                b_vec = [
                    p_j.cost,
                    -p_j.carbon_offset_kg,
                    -p_j.helium_impact_l,
                    -p_j.permanence_years,
                    -p_j.verification_confidence
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
        objective_names = ['cost', 'carbon_offset_kg', 'helium_impact_l', 'permanence_years', 'verification_confidence']
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
                # For objectives to minimise (cost): invert
                if key == 'cost':
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
    # Core Offset Method (Enhanced with MOPD)
    # ============================================================================
    async def offset_expert_emissions(
        self,
        expert_carbon_kg: float,
        budget_remaining: float,
        urgency: str = 'normal',
        use_ml_selection: bool = False,
        return_mopd: bool = False           # NEW: if True, return Pareto front
    ) -> Dict[str, Any]:
        # Get carbon intensity
        carbon_intensity = 400
        if self.carbon_manager:
            carbon_intensity = await self.carbon_manager.get_current_intensity()

        # Determine offset amount based on strategy
        offset_amount = expert_carbon_kg * 1.0
        if self.config.offset_strategy == 'proactive':
            offset_amount = max(expert_carbon_kg, expert_carbon_kg * 1.2)
        elif self.config.offset_strategy == 'reactive':
            offset_amount = max(expert_carbon_kg, expert_carbon_kg)
        else:  # conservative
            offset_amount = min(expert_carbon_kg, expert_carbon_kg * 0.5)

        # Select projects
        if use_ml_selection and self.ml_selector:
            ml_results = await self.ml_selector.select_projects({
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
            project_map = {
                'reforestation': 'reforestation_tropical',
                'dac': 'direct_air_capture',
                'biochar': 'biochar_agriculture',
                'ocean_based': 'ocean_alkalinization',
                'helium_recovery': 'helium_recovery_advanced'
            }
            selected_projects = [project_map.get(p, p) for p in selected_projects if p in project_map]
        else:
            selected_projects = self._select_projects(offset_amount, urgency)

        # Allocate offset across projects
        allocation = self._allocate_offset(offset_amount, selected_projects)

        # Execute offset
        offset_result = await self._execute_offset(allocation)

        # Create carbon credits
        new_credits = self._generate_credits(offset_result)
        for credit in new_credits:
            await self.storage.add_credit(credit)

        # Handle helium offsets
        if self.helium_manager:
            helium_offset = self.helium_manager.calculate_helium_offset_from_carbon(offset_amount)
            helium_project = self.helium_manager.select_helium_project(helium_offset)
            if helium_project['project_id']:
                self.helium_manager.record_offset(helium_offset, helium_project['project_id'])

        # Update totals
        await self.storage.update_totals(offset_amount, offset_amount * 0.1)

        # Calculate sustainability score
        sustainability_score = self._calculate_sustainability_score(
            offset_amount, expert_carbon_kg, carbon_intensity
        )
        await self.storage.update_sustainability_score(sustainability_score)

        offset_plan = {
            'offset_amount_kg': offset_amount,
            'expert_emissions_kg': expert_carbon_kg,
            'over_offset_ratio': offset_amount / expert_carbon_kg if expert_carbon_kg > 0 else 0,
            'projects_used': selected_projects,
            'allocation': allocation,
            'credits_generated': len(new_credits),
            'cost': sum(p['cost'] for p in allocation.values()),
            'carbon_intensity': carbon_intensity,
            'sustainability_score': sustainability_score,
            'helium_offset_l': helium_offset if self.helium_manager else 0,
            'ml_used': use_ml_selection,
            'timestamp': datetime.now(timezone.utc).isoformat()
        }
        await self.storage.add_transaction(offset_plan)

        # Update predictive analyzer
        if self.predictive:
            self.predictive.update_history({
                'offset_amount': offset_amount,
                'credit_price': offset_plan['cost'] / max(offset_amount, 1),
                'project_success_rate': 0.9,
                'verification_confidence': 0.7,
                'carbon_intensity': carbon_intensity
            })
            await self.predictive.train()

        # Human‑AI insights
        if self.human_ai:
            offset_plan['human_ai_insights'] = await self.human_ai.get_insights()

        # MOPD: generate Pareto front if requested
        if self.config.enable_mopd and return_mopd:
            pareto_front = await self._generate_pareto_front_for_offset(
                expert_carbon_kg, budget_remaining, urgency
            )
            for plan in pareto_front:
                await self.storage.add_mopd_plan(plan)
            offset_plan['mopd_pareto_front'] = [p.to_dict() for p in pareto_front]
            best_plan = self._select_best_from_pareto(pareto_front)
            if best_plan:
                offset_plan['mopd_best_plan'] = best_plan.to_dict()

        logger.info(
            f"Offset {expert_carbon_kg:.4f} kg CO2 with {offset_amount:.4f} kg "
            f"across {len(selected_projects)} projects, "
            f"sustainability_score={sustainability_score:.2f}"
        )
        return offset_plan

    def _calculate_sustainability_score(
        self, offset_amount: float, expert_carbon_kg: float, carbon_intensity: float
    ) -> float:
        offset_ratio = min(1.0, offset_amount / max(expert_carbon_kg, 1))
        carbon_factor = 1.0 - (carbon_intensity / 800)
        over_offset = min(1.0, (offset_amount - expert_carbon_kg) / max(expert_carbon_kg, 1) + 1)
        score = (offset_ratio * 0.3 + carbon_factor * 0.3 + over_offset * 0.4)
        return min(1.0, max(0.0, score))

    def _select_projects(self, amount_kg: float, urgency: str) -> List[str]:
        projects = asyncio.run(self.storage.get_projects())
        scored_projects = []
        for project_id, project in projects.items():
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
            total_capacity += projects[project_id]['capacity_kg_per_year']
            if total_capacity >= amount_kg:
                break
        return selected

    def _allocate_offset(self, amount_kg: float, projects: List[str]) -> Dict[str, Dict[str, Any]]:
        projects_dict = asyncio.run(self.storage.get_projects())
        allocation = {}
        remaining = amount_kg
        sorted_projects = sorted(projects, key=lambda p: projects_dict[p]['cost_per_kg'])
        for project_id in sorted_projects:
            project = projects_dict[project_id]
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
        total_amount = sum(a['amount_kg'] for a in allocation.values())
        total_cost = sum(a['cost'] for a in allocation.values())
        total_helium = sum(a['helium_offset_potential_l'] for a in allocation.values())
        return {
            'total_amount_kg': total_amount,
            'total_cost': total_cost,
            'total_helium_offset_l': total_helium,
            'projects': allocation,
            'execution_time': datetime.now(timezone.utc).isoformat(),
            'verification_pending': True,
            'sustainability_score': await self.storage.get_sustainability_score()
        }

    def _generate_credits(self, offset_result: Dict[str, Any]) -> List[CarbonCredit]:
        credits = []
        projects_dict = asyncio.run(self.storage.get_projects())
        for project_id, allocation in offset_result['projects'].items():
            project = projects_dict.get(project_id, {})
            credit = CarbonCredit(
                credit_id=f"CRED-{datetime.now(timezone.utc).timestamp()}-{project_id}",
                amount_kg=allocation['amount_kg'],
                project_type=allocation['project_type'],
                verification_date=datetime.now(timezone.utc),
                expiry_date=datetime.now(timezone.utc) + timedelta(days=365),
                price_per_kg=allocation['cost'] / allocation['amount_kg'] if allocation['amount_kg'] > 0 else 0,
                is_verified=False,
                permanence_years=project.get('permanence_years', 0),
                co_benefits=project.get('co_benefits', []),
                sustainability_score=offset_result['sustainability_score'],
                helium_offset_equivalent_l=allocation.get('helium_offset_potential_l', 0)
            )
            credits.append(credit)
        return credits

    async def verify_credits(self) -> int:
        credits = await self.storage.get_credits()
        verified_count = 0
        for credit in credits:
            if not credit.is_verified and credit.amount_kg > 0 and credit.verification_date > datetime.now(timezone.utc) - timedelta(days=30):
                credit.is_verified = True
                verified_count += 1
                if self.helium_manager:
                    self.helium_manager.record_offset(
                        credit.helium_offset_equivalent_l,
                        f"credit_{credit.credit_id}"
                    )
        logger.info(f"Verified {verified_count} carbon credits")
        return verified_count

    async def train_ml_model(self, training_data: Optional[List[Dict]] = None) -> Dict:
        if not self.ml_selector:
            return {'status': 'disabled'}
        if training_data is None:
            history = await self.storage.get_transaction_history(100)
            training_data = []
            for item in history:
                allocations = item.get('allocation', {})
                selected_project = list(allocations.keys())[0] if allocations else 'reforestation_tropical'
                project_map = {
                    'reforestation_tropical': 'reforestation',
                    'direct_air_capture': 'dac',
                    'biochar_agriculture': 'biochar',
                    'ocean_alkalinization': 'ocean_based',
                    'helium_recovery_advanced': 'helium_recovery'
                }
                training_data.append({
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
        return await self.ml_selector.train(training_data)

    async def train_predictive_model(self) -> Dict:
        if not self.predictive:
            return {'status': 'disabled'}
        return await self.predictive.train()

    async def get_recommendation_for_expert(
        self, expert_carbon_per_inference: float, annual_inferences: int
    ) -> Dict[str, Any]:
        annual_emissions = expert_carbon_per_inference * annual_inferences
        projects = await self.storage.get_projects()
        project_costs = []
        for pid, project in projects.items():
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
                'sustainability_score': await self.storage.get_sustainability_score()
            })
        project_costs.sort(key=lambda x: x['annual_cost'])
        return {
            'expert_annual_emissions_kg': annual_emissions,
            'recommended_project': project_costs[0] if project_costs else None,
            'all_options': project_costs,
            'offset_strategy': self.config.offset_strategy,
            'cost_effective': project_costs[0]['annual_cost'] < 100 if project_costs else False,
            'sustainability_score': await self.storage.get_sustainability_score(),
            'recommended_helium_offset_l': project_costs[0]['helium_offset_potential_l'] if project_costs else 0
        }

# ============================================================================
# Reporter Module (Enhanced with MOPD)
# ============================================================================
class CarbonSequestrationReporter:
    def __init__(
        self,
        config: CarbonSequestrationConfig,
        storage: CarbonSequestrationStorage,
        analyzer: CarbonSequestrationAnalyzer,
        telemetry: Optional[CarbonSequestrationTelemetry],
        persistence: Optional[CarbonSequestrationPersistenceManager],
        human_ai: Optional[HumanAICollaborativeSequestration],
        federated: Optional[FederatedSequestrationManager],
        predictive: Optional[PredictiveSequestrationAnalyzer],
        ml_selector: Optional[MLProjectSelector],
        helium_manager: Optional[HeliumSequestrationManager]
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

    async def get_carbon_portfolio(self) -> Dict[str, Any]:
        stats = await self.storage.get_stats()
        projects = await self.storage.get_projects()
        portfolio = {
            'total_credits': stats['total_credits'],
            'verified_credits': stats['verified_credits'],
            'total_offset_kg': stats['total_offset_kg'],
            'total_sequestered_kg': stats['total_sequestered_kg'],
            'sustainability_score': stats['sustainability_score'],
            'project_breakdown': {
                pid: {
                    'type': p['type'],
                    'capacity': p['capacity_kg_per_year'],
                    'cost': p['cost_per_kg'],
                    'permanence_years': p['permanence_years'],
                    'co_benefits': p['co_benefits'],
                    'helium_offset_potential_l': p.get('helium_offset_potential_l', 0)
                }
                for pid, p in projects.items()
            },
            'net_carbon_impact_kg': stats['total_sequestered_kg'] - stats['total_offset_kg']
        }

        if self.helium_manager:
            portfolio['helium_position'] = self.helium_manager.get_position()

        if self.federated:
            portfolio['federated_stats'] = self.federated.get_federated_stats()

        if self.predictive:
            forecast = await self.predictive.predict_demand()
            portfolio['predictive_forecast'] = forecast

        if self.ml_selector:
            portfolio['ml_status'] = {
                'trained': self.ml_selector.is_trained,
                'model_version': 'v4.0.0',
                'training_samples': len(self.ml_selector.training_history)
            }

        if self.human_ai:
            portfolio['human_ai_insights'] = await self.human_ai.get_insights()

        # MOPD summary
        if self.config.enable_mopd:
            mopd_plans = await self.storage.get_mopd_plans(20)
            portfolio['mopd_plans'] = [p.to_dict() for p in mopd_plans]

        return portfolio

    async def get_sustainability_report(self) -> Dict[str, Any]:
        stats = await self.storage.get_stats()
        return {
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'sustainability_score': stats['sustainability_score'],
            'carbon_portfolio': await self.get_carbon_portfolio(),
            'helium_position': self.helium_manager.get_position() if self.helium_manager else {},
            'recommendations': self._generate_recommendations()
        }

    def _generate_recommendations(self) -> List[str]:
        recs = []
        stats = asyncio.run(self.storage.get_stats())
        if stats['sustainability_score'] < 0.5:
            recs.append("Improve carbon sequestration through project diversification")
        if stats['total_offset_kg'] < stats['total_sequestered_kg'] * 0.5:
            recs.append("Increase offset allocation to match sequestration capacity")
        if self.helium_manager:
            remaining = self.helium_manager.get_position().get('remaining_budget_l', 0)
            if remaining < 0:
                recs.append("CRITICAL: Helium budget exceeded - implement recovery systems")
        if self.federated and len(self.federated.participants) < 2:
            recs.append("Increase federated participation for better project selection")
        if self.config.enable_mopd:
            recs.append("Consider using MOPD to explore trade-offs among offset strategies")
        return recs or ["All sustainability metrics are within acceptable ranges"]

    async def export_telemetry(self):
        if self.telemetry:
            data = await self.telemetry.export()
            logger.debug(f"Telemetry export: {len(data)} bytes")

    async def save_state(self):
        if self.persistence:
            state = {
                'credits': await self.storage.get_credits(),
                'transaction_history': await self.storage.get_transaction_history(),
                'sequestration_projects': await self.storage.get_projects(),
                'sustainability_score': await self.storage.get_sustainability_score(),
                'total_sequestered': (await self.storage.get_totals())['sequestered'],
                'total_offset': (await self.storage.get_totals())['offset'],
                'helium_manager_state': {
                    'emissions': list(self.helium_manager.emissions) if self.helium_manager else [],
                    'offsets': list(self.helium_manager.offsets) if self.helium_manager else [],
                    '_total_emissions': self.helium_manager._total_emissions if self.helium_manager else 0.0,
                    '_total_offsets': self.helium_manager._total_offsets if self.helium_manager else 0.0,
                } if self.helium_manager else None,
                'ml_checkpoint': self.analyzer.ml_selector.get_checkpoint() if self.analyzer.ml_selector else None,
                'mopd_plans': [p.to_dict() for p in await self.storage.get_mopd_plans()],  # NEW
            }
            await self.persistence.save_state(state)

    async def load_state(self):
        if self.persistence:
            state = await self.persistence.load_state()
            if state:
                # Restore credits
                credits = state.get('credits', [])
                for c in credits:
                    await self.storage.add_credit(c)
                # Restore transaction history
                for t in state.get('transaction_history', []):
                    await self.storage.add_transaction(t)
                # Restore projects
                await self.storage.set_projects(state.get('sequestration_projects', {}))
                await self.storage.update_sustainability_score(state.get('sustainability_score', 0.0))
                # Restore totals
                await self.storage.update_totals(
                    state.get('total_offset', 0.0),
                    state.get('total_sequestered', 0.0)
                )
                # Restore helium manager
                he_state = state.get('helium_manager_state')
                if he_state and self.helium_manager:
                    self.helium_manager.emissions = deque(he_state.get('emissions', []), maxlen=86400)
                    self.helium_manager.offsets = deque(he_state.get('offsets', []), maxlen=86400)
                    self.helium_manager._total_emissions = he_state.get('_total_emissions', 0.0)
                    self.helium_manager._total_offsets = he_state.get('_total_offsets', 0.0)
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
class CarbonSequestrationManager:
    """
    Enhanced Carbon Sequestration Manager v4.1.0
    Controller that orchestrates storage, analysis, reporting, and MOPD support.
    """

    def __init__(
        self,
        bio_core: Optional[EnhancedBioInspiredCore] = None,
        config: Optional[CarbonSequestrationConfig] = None,
        **kwargs
    ):
        if config is None:
            config = CarbonSequestrationConfig(**{k: v for k, v in kwargs.items() if k in CarbonSequestrationConfig.__annotations__})
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
        self.helium_manager = HeliumSequestrationManager(self.config.helium) if self.config.helium.enabled else None
        self.predictive = PredictiveSequestrationAnalyzer(self.config.predictive) if self.config.predictive.enabled else None
        self.ml_selector = MLProjectSelector(self.config.ml) if self.config.ml.enabled else None
        self.federated = FederatedSequestrationManager(self.config.federated) if self.config.federated.enabled else None
        self.human_ai = HumanAICollaborativeSequestration() if self.config.enable_human_ai else None
        self.telemetry = CarbonSequestrationTelemetry() if self.config.telemetry.enabled else None
        self.persistence = CarbonSequestrationPersistenceManager(self.config.persistence) if self.config.persistence.enabled else None

        # Storage, Analyzer, Reporter
        self.storage = CarbonSequestrationStorage()
        self.analyzer = CarbonSequestrationAnalyzer(
            self.config,
            self.storage,
            self.carbon_manager,
            self.helium_manager,
            self.predictive,
            self.ml_selector,
            self.human_ai
        )
        self.reporter = CarbonSequestrationReporter(
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

        # Initialize projects
        asyncio.create_task(self._initialize_projects())

        # Subscribe to events
        if self.config.enable_event_driven and self.event_broker:
            self._subscribe_events()

        # Start background tasks
        self._start_background_tasks()

        # Load state
        if self.config.persistence.enabled:
            asyncio.create_task(self.reporter.load_state())

        logger.info("Carbon Sequestration Manager v4.1.0 initialized with MOPD")

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
                'offset_amount': 0,
                'credit_price': event.data.get('price', 50),
                'project_success_rate': 0.9,
                'verification_confidence': 0.7,
                'carbon_intensity': intensity
            })
        if intensity > 500:
            self.config.offset_strategy = 'proactive'
        elif intensity < 300:
            self.config.offset_strategy = 'conservative'

    async def _on_helium_update(self, event: BioEvent):
        scarcity = event.data.get('scarcity', 0.5)
        if self.helium_manager:
            self.helium_manager.budget_l = self.config.helium_budget_l * (1.0 - scarcity * 0.3)
            self.helium_manager.config.helium_to_co2_factor = self.config.helium_to_co2_factor * (1.0 + 0.1 * scarcity)
        if scarcity > 0.7:
            projects = await self.storage.get_projects()
            for project in projects.values():
                project['cost_per_kg'] *= (1.0 - 0.1 * scarcity)

    async def _on_alert_generated(self, event: BioEvent):
        if event.data.get('severity') == 'critical':
            logger.warning("Critical alert; triggering self‑healing")
            self.config.offset_strategy = 'conservative'
            if self.config.self_healing.enabled and self.self_healer:
                await self.self_healer.apply_healing('damage_accumulation')
            if self.workflow_orchestrator and self.config.workflow_on_critical_alert:
                await self.workflow_orchestrator.execute_workflow(self.config.workflow_on_critical_alert)

    async def _on_config_updated(self, event: BioEvent):
        updates = event.data.get('updates', {})
        if 'carbon_sequestration' in updates:
            new = updates['carbon_sequestration']
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
            self.config.offset_strategy = 'proactive'
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
                    history = await self.storage.get_transaction_history(100)
                    if len(history) >= 20:
                        training_data = []
                        for item in history:
                            allocations = item.get('allocation', {})
                            selected_project = list(allocations.keys())[0] if allocations else 'reforestation_tropical'
                            project_map = {
                                'reforestation_tropical': 'reforestation',
                                'direct_air_capture': 'dac',
                                'biochar_agriculture': 'biochar',
                                'ocean_alkalinization': 'ocean_based',
                                'helium_recovery_advanced': 'helium_recovery'
                            }
                            training_data.append({
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
                    pid = f"sequestration_{hashlib.md5(str(self.storage.sequestration_projects).encode()).hexdigest()[:8]}"
                    await self.federated.send_local_projects(
                        pid,
                        {
                            'total_projects': len(await self.storage.get_projects()),
                            'total_sequestered': (await self.storage.get_totals())['sequestered'],
                            'total_offset': (await self.storage.get_totals())['offset'],
                            'sustainability_score': await self.storage.get_sustainability_score(),
                            'timestamp': datetime.now(timezone.utc).isoformat()
                        },
                        performance=await self.storage.get_sustainability_score()
                    )
                    await self.federated.get_global_projects()
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
    async def offset_expert_emissions(
        self,
        expert_carbon_kg: float,
        budget_remaining: float,
        urgency: str = 'normal',
        use_ml_selection: bool = False,
        return_mopd: bool = False           # NEW
    ) -> Dict[str, Any]:
        result = await self.analyzer.offset_expert_emissions(
            expert_carbon_kg, budget_remaining, urgency, use_ml_selection, return_mopd
        )

        # Trigger workflows if critical
        if await self.storage.get_sustainability_score() < 0.4 and self.workflow_orchestrator:
            await self.workflow_orchestrator.execute_workflow(self.config.workflow_on_slo_breach)

        # Feed to MoE components
        if self.gating_network and self.expert_router:
            features = np.array([
                result['offset_amount_kg'] / 1000,
                await self.storage.get_sustainability_score(),
                (result.get('carbon_intensity', 400) / 800),
                len(result.get('projects_used', []))
            ])
            reward = 1.0 - (expert_carbon_kg - result['offset_amount_kg']) / max(expert_carbon_kg, 1)
            self.gating_network.update(features, reward, {'strategy': self.config.offset_strategy})

        if self.self_evolving_gate and TORCH_AVAILABLE:
            state = torch.tensor([
                result['offset_amount_kg'],
                await self.storage.get_sustainability_score()
            ], dtype=torch.float32)
            self.self_evolving_gate.adapt(
                state=state,
                chosen_expert=0,
                reward=1.0 - (expert_carbon_kg - result['offset_amount_kg']) / max(expert_carbon_kg, 1),
                environmental_feedback={'strategy': self.config.offset_strategy},
                quantum_mode=False
            )

        # Telemetry
        if self.telemetry:
            self.telemetry.increment('offsets_performed')
            self.telemetry.gauge('offset_amount', result['offset_amount_kg'])
            self.telemetry.gauge('sustainability_score', await self.storage.get_sustainability_score())
            if return_mopd and 'mopd_pareto_front' in result:
                self.telemetry.increment('mopd_generations')
                self.telemetry.histogram('mopd_pareto_front_size', len(result['mopd_pareto_front']))

        logger.info(
            f"Offset {expert_carbon_kg:.4f} kg CO2 with {result['offset_amount_kg']:.4f} kg "
            f"across {len(result.get('projects_used', []))} projects, "
            f"sustainability_score={await self.storage.get_sustainability_score():.2f}"
        )
        return result

    async def verify_credits(self) -> int:
        return await self.analyzer.verify_credits()

    async def get_carbon_portfolio(self) -> Dict[str, Any]:
        return await self.reporter.get_carbon_portfolio()

    async def get_sustainability_report(self) -> Dict[str, Any]:
        return await self.reporter.get_sustainability_report()

    async def get_recommendation_for_expert(
        self, expert_carbon_per_inference: float, annual_inferences: int
    ) -> Dict[str, Any]:
        return await self.analyzer.get_recommendation_for_expert(expert_carbon_per_inference, annual_inferences)

    async def train_ml_model(self, training_data: Optional[List[Dict]] = None) -> Dict:
        return await self.analyzer.train_ml_model(training_data)

    async def train_predictive_model(self) -> Dict:
        return await self.analyzer.train_predictive_model()

    # ============================================================================
    # MOPD Public Methods (NEW)
    # ============================================================================
    async def get_sequestration_pareto_front(
        self,
        expert_carbon_kg: float,
        budget_remaining: float,
        urgency: str = 'normal'
    ) -> List[MOPDPlan]:
        """
        Generate Pareto front of sequestration strategies without actually offsetting.
        Returns a list of MOPDPlan objects.
        """
        if not self.config.enable_mopd:
            return []
        pareto_front = await self.analyzer._generate_pareto_front_for_offset(
            expert_carbon_kg, budget_remaining, urgency
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
            'manager_id': hashlib.md5(str(self.storage.sequestration_projects).encode()).hexdigest()[:8],
            'sustainability_score': stats['sustainability_score'],
            'total_offset': stats['total_offset_kg'],
            'total_sequestered': stats['total_sequestered_kg'],
            'credits_count': stats['total_credits'],
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

        # Reset budgets
        if self.helium_manager:
            self.helium_manager.budget_l = self.config.helium_budget_l
        self.config.offset_strategy = 'proactive'

        # Reset sustainability score
        await self.storage.update_sustainability_score(0.0)

        # Trim credits and transaction history
        credits = await self.storage.get_credits()
        if len(credits) > 10:
            async with self.storage._lock:
                self.storage.credits = credits[-10:]
        history = await self.storage.get_transaction_history()
        if len(history) > 10:
            async with self.storage._lock:
                self.storage.transaction_history = history[-10:]

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
        return {
            'status': self.health_status,
            'last_error': self.last_error,
            'total_offset': stats['total_offset_kg'],
            'total_sequestered': stats['total_sequestered_kg'],
            'credits_count': stats['total_credits'],
            'sustainability_score': stats['sustainability_score'],
            'bio_integration_active': self.config.enable_bio_integration,
            'event_driven_active': self.config.enable_event_driven,
            'self_healing_enabled': self.config.self_healing.enabled,
            'persistence_enabled': self.config.persistence.enabled,
            'mopd_enabled': self.config.enable_mopd,
        }

    # ============================================================================
    # Helper Methods
    # ============================================================================
    async def _initialize_projects(self):
        default_projects = {
            'reforestation_tropical': {
                'type': 'reforestation',
                'capacity_kg_per_year': 10000,
                'cost_per_kg': 0.05,
                'permanence_years': 100,
                'co_benefits': ['biodiversity', 'water_cycle'],
                'helium_offset_potential_l': 5
            },
            'direct_air_capture': {
                'type': 'dac',
                'capacity_kg_per_year': 5000,
                'cost_per_kg': 0.20,
                'permanence_years': 10000,
                'co_benefits': ['technology', 'employment'],
                'helium_offset_potential_l': 2
            },
            'biochar_agriculture': {
                'type': 'biochar',
                'capacity_kg_per_year': 8000,
                'cost_per_kg': 0.08,
                'permanence_years': 1000,
                'co_benefits': ['soil_health', 'crop_yield'],
                'helium_offset_potential_l': 3
            },
            'ocean_alkalinization': {
                'type': 'ocean_based',
                'capacity_kg_per_year': 20000,
                'cost_per_kg': 0.15,
                'permanence_years': 10000,
                'co_benefits': ['ocean_health', 'carbon_sink'],
                'helium_offset_potential_l': 1
            },
            'helium_recovery_advanced': {
                'type': 'helium_recovery',
                'capacity_kg_per_year': 1000,
                'cost_per_kg': 0.50,
                'permanence_years': 50,
                'co_benefits': ['resource_conservation', 'technology'],
                'helium_offset_potential_l': 50
            }
        }
        await self.storage.set_projects(default_projects)

    # ============================================================================
    # Shutdown
    # ============================================================================
    async def shutdown(self):
        logger.info("Shutting down Carbon Sequestration Manager")
        # Cancel background tasks
        for task in self._background_tasks:
            task.cancel()
        await asyncio.gather(*self._background_tasks, return_exceptions=True)

        # Stop loops
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
