"""
Enhanced Layer Integrator v6.1.1 - Complete Green Agent Implementation

Complete bio-inspired integration with:
- Federated Reflexive Learning with distributed layer health
- User-Adaptive Reflexivity with dynamic configuration
- Real-time Carbon Intensity Integration with API support
- Cross-Domain Knowledge Transfer with event-driven communication
- Human-AI Collaborative Reflection with comprehensive monitoring
- Predictive Reflexivity with ensemble forecasting
- Sustainability Score with multi-metric aggregation
- Enhanced Carbon/Helium Awareness with real-time tracking
- Gradient-based layer health (trust gradient as health indicator)
- Membrane permeability mapping (compartment membrane states)
- Second messenger event communication (signal transduction)
- Token-backed cache TTL (dynamic cache expiration)
- Entangled layer dependencies (biomass resource coupling)
- Token recovery on transaction rollback
- Gradient-modulated retry timing
- Harvester-aware layer vitality
- Dynamic layer discovery for runtime registration
- Health-based circuit reset using gradient fields
- Event correlation for complex workflow orchestration
- Gradient-aware cache invalidation
- Distributed transaction support across integrators
- Context builder for MoE expert system
- Helium and Federated Learning telemetry integration

New in v6.1.1:
- Configuration dataclass for centralized settings
- Resilient carbon intensity manager with retry & circuit breaker
- Online predictive layer analyzer with incremental learning
- Robust dynamic discovery with retry & health checks
- Advanced event correlation with temporal and causal patterns
- Adaptive gradient-aware cache with weighted eviction
- Enhanced distributed transactions with timeout and participant failure
- Dedicated sustainability score calculator
- Pluggable telemetry collector for Prometheus export
- Improved bio-inspired fallback strategies
"""

import asyncio
import logging
from typing import Dict, Any, List, Optional, Tuple, Set, Callable, Union
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
import numpy as np
import hashlib
import json
import time
import inspect
import functools
from collections import defaultdict, deque
from concurrent.futures import ThreadPoolExecutor
import aiohttp
import os
import uuid
import zlib
import pickle

logger = logging.getLogger(__name__)

# ============================================================================
# Bio-Inspired Import Check
# ============================================================================
try:
    from enhancements.bio_inspired.eco_atp_currency import (
        EcoATPTokenManager, DynamicExchangeRate, EcoATPSource, EcoATPConsumer,
        TokenState, EcoATPToken, EcoATPAccount
    )
    from enhancements.bio_inspired.proton_gradient_fields import (
        GradientFieldManager, GradientField
    )
    from enhancements.bio_inspired.atp_synthase_scheduler import (
        ATPSynthaseScheduler, SynthaseConfig
    )
    from enhancements.bio_inspired.chromatophore_compartments import (
        CompartmentManager, ChromatophoreCompartment, CompartmentState,
        MembranePermeability
    )
    from enhancements.bio_inspired.biomass_storage import (
        BiomassStorage, StorageTier, GuaranteeLevel
    )
    from enhancements.bio_inspired.photosynthetic_harvester import (
        PhotosyntheticHarvester
    )
    BIO_INSPIRED_AVAILABLE = True
    logger.info("Bio-inspired modules loaded for Layer Integrator")
except ImportError as e:
    BIO_INSPIRED_AVAILABLE = False
    logger.warning(f"Bio-inspired modules not available: {str(e)} - using standard integration")

# ============================================================================
# MoE Expert Router Import
# ============================================================================
try:
    from ..expert_router import ExpertRouter
    MOE_AVAILABLE = True
except ImportError:
    MOE_AVAILABLE = False
    logger.warning("MoE Expert Router not available - context building will be limited")

# ============================================================================
# Configuration Dataclass (NEW)
# ============================================================================
@dataclass
class LayerIntegratorConfig:
    """Centralized configuration for the Layer Integrator."""
    # Feature flags
    enable_cache: bool = True
    enable_circuit_breaker: bool = True
    enable_retry: bool = True
    enable_events: bool = True
    enable_transactions: bool = True
    enable_monitoring: bool = True
    enable_bio_integration: bool = True
    enable_carbon_intensity: bool = True
    enable_predictive: bool = True
    enable_cross_domain: bool = True
    enable_sustainability_scoring: bool = True
    enable_dynamic_discovery: bool = True
    enable_event_correlation: bool = True
    enable_gradient_cache: bool = True
    enable_distributed_txns: bool = True

    # Tunable parameters
    cache_ttl_seconds: float = 60.0
    max_cache_size: int = 1000
    coordinator_id: str = "main_coordinator"
    carbon_api_region: str = "us-east"
    carbon_update_interval: int = 300
    discovery_interval: int = 60
    health_check_interval: int = 10
    max_retries: int = 3
    retry_base_delay_ms: float = 100.0
    retry_max_delay_ms: float = 5000.0
    circuit_breaker_failure_threshold: int = 5
    circuit_breaker_recovery_timeout: float = 30.0
    half_open_max_requests: int = 3
    transaction_timeout_seconds: float = 60.0
    token_reserve_factor: float = 10.0
    gradient_health_threshold: float = 0.6
    sustainability_weights: Dict[str, float] = field(default_factory=lambda: {
        'carbon_savings': 0.3,
        'helium_efficiency': 0.2,
        'renewable_usage': 0.2,
        'token_efficiency': 0.15,
        'layer_health': 0.15
    })

    def __post_init__(self):
        # Ensure all flags are booleans
        for key, value in self.__dict__.items():
            if isinstance(value, bool):
                setattr(self, key, bool(value))

# ============================================================================
# Enums and Data Classes (Enhanced)
# ============================================================================
class LayerStatus(Enum):
    HEALTHY = "healthy"
    DEGRADED = "degraded"
    UNHEALTHY = "unhealthy"
    RECOVERING = "recovering"
    OFFLINE = "offline"
    MAINTENANCE = "maintenance"
    DISCOVERED = "discovered"

    def to_membrane_state(self):
        if not BIO_INSPIRED_AVAILABLE:
            return None
        mapping = {
            LayerStatus.HEALTHY: MembranePermeability.PERMEABLE,
            LayerStatus.DEGRADED: MembranePermeability.SELECTIVE,
            LayerStatus.UNHEALTHY: MembranePermeability.RESTRICTIVE,
            LayerStatus.RECOVERING: MembranePermeability.SELECTIVE,
            LayerStatus.OFFLINE: MembranePermeability.IMPERMEABLE,
            LayerStatus.MAINTENANCE: MembranePermeability.RESTRICTIVE,
            LayerStatus.DISCOVERED: MembranePermeability.SELECTIVE
        }
        return mapping.get(self)

class IntegrationMode(Enum):
    SYNCHRONOUS = "synchronous"
    ASYNCHRONOUS = "asynchronous"
    EVENT_DRIVEN = "event_driven"
    BATCH = "batch"
    STREAMING = "streaming"

class CircuitState(Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"
    RECOVERING = "recovering"

@dataclass
class LayerInfo:
    layer_number: int
    layer_name: str
    version: str
    status: LayerStatus = LayerStatus.HEALTHY
    integration_mode: IntegrationMode = IntegrationMode.SYNCHRONOUS
    last_heartbeat: datetime = field(default_factory=datetime.utcnow)
    dependencies: List[int] = field(default_factory=list)
    capabilities: List[str] = field(default_factory=list)
    endpoints: Dict[str, str] = field(default_factory=dict)
    metrics: Dict[str, Any] = field(default_factory=dict)
    config: Dict[str, Any] = field(default_factory=dict)
    circuit_breaker: 'LayerCircuitBreaker' = None
    gradient_health: float = 0.7
    membrane_permeability: str = "selective"
    token_balance: float = 0.0
    harvester_vitality: float = 0.5
    entangled_layers: List[int] = field(default_factory=list)
    sustainability_score: float = 0.0
    carbon_savings_kg: float = 0.0
    discovery_timestamp: Optional[datetime] = None
    health_history: List[Dict] = field(default_factory=list)
    recovery_attempts: int = 0
    max_recovery_attempts: int = 5

    def __post_init__(self):
        if self.circuit_breaker is None:
            self.circuit_breaker = LayerCircuitBreaker(f"layer_{self.layer_number}")

@dataclass
class LayerCircuitBreaker:
    layer_id: str
    state: CircuitState = CircuitState.CLOSED
    failure_count: int = 0
    success_count: int = 0
    last_failure_time: Optional[datetime] = None
    last_success_time: Optional[datetime] = None
    failure_threshold: int = 5
    recovery_timeout_seconds: float = 30.0
    half_open_max_requests: int = 3
    half_open_requests: int = 0
    gradient_health_threshold: float = 0.6
    recovery_attempts: int = 0
    recovery_progress: float = 0.0

    def record_success(self):
        self.success_count += 1
        self.last_success_time = datetime.utcnow()
        if self.state == CircuitState.HALF_OPEN:
            self.half_open_requests += 1
            if self.half_open_requests >= self.half_open_max_requests:
                self.state = CircuitState.CLOSED
                self.failure_count = 0
                self.half_open_requests = 0
                self.recovery_attempts = 0
                self.recovery_progress = 1.0

    def record_failure(self):
        self.failure_count += 1
        self.last_failure_time = datetime.utcnow()
        if self.state == CircuitState.CLOSED and self.failure_count >= self.failure_threshold:
            self.state = CircuitState.OPEN
        elif self.state == CircuitState.HALF_OPEN:
            self.state = CircuitState.OPEN
            self.recovery_attempts = 0

    def record_recovery_attempt(self):
        self.recovery_attempts += 1
        self.recovery_progress = min(1.0, self.recovery_progress + 0.1)

    def can_execute(self) -> bool:
        if self.state == CircuitState.CLOSED:
            return True
        if self.state == CircuitState.OPEN:
            if self.last_failure_time:
                elapsed = (datetime.utcnow() - self.last_failure_time).total_seconds()
                if elapsed >= self.recovery_timeout_seconds:
                    self.state = CircuitState.HALF_OPEN
                    self.half_open_requests = 0
                    self.recovery_attempts = 0
                    return True
            return False
        if self.state == CircuitState.RECOVERING:
            return False
        return True

@dataclass
class LayerEvent:
    event_id: str
    event_type: str
    source_layer: int
    target_layer: Optional[int]
    payload: Dict[str, Any]
    timestamp: datetime = field(default_factory=datetime.utcnow)
    correlation_id: Optional[str] = None
    priority: int = 0
    second_messenger_type: Optional[str] = None
    gradient_level: float = 0.0
    token_cost: float = 0.0
    carbon_impact: float = 0.0
    parent_event_id: Optional[str] = None
    child_event_ids: List[str] = field(default_factory=list)
    workflow_phase: Optional[str] = None

@dataclass
class CacheEntry:
    key: str
    value: Any
    created_at: datetime
    expires_at: datetime
    layer_number: int
    access_count: int = 0
    last_accessed: datetime = field(default_factory=datetime.utcnow)
    token_backed: bool = False
    gradient_level_at_creation: float = 0.5
    gradient_threshold: float = 0.3
    invalidated_by_gradient: bool = False
    weight: float = 1.0  # For weighted eviction

@dataclass
class RetryConfig:
    max_retries: int = 3
    base_delay_ms: float = 100.0
    max_delay_ms: float = 5000.0
    exponential_base: float = 2.0
    jitter: bool = True
    retryable_exceptions: Tuple[type, ...] = (Exception,)

    def get_delay(self, attempt: int, gradient_modulation: float = 1.0) -> float:
        delay = min(self.base_delay_ms * (self.exponential_base ** attempt), self.max_delay_ms)
        delay *= gradient_modulation
        if self.jitter:
            delay *= (0.5 + np.random.random())
        return delay / 1000.0

@dataclass
class TransactionContext:
    transaction_id: str
    started_at: datetime
    layers_involved: List[int]
    operations: List[Dict[str, Any]] = field(default_factory=list)
    compensation_actions: List[Dict[str, Any]] = field(default_factory=list)
    status: str = "active"
    timeout_seconds: float = 60.0
    tokens_allocated: float = 0.0
    tokens_consumed: float = 0.0
    tokens_recovered: float = 0.0
    carbon_impact: float = 0.0
    coordinator_id: Optional[str] = None
    participants: List[str] = field(default_factory=list)
    distributed_status: Dict[str, str] = field(default_factory=dict)
    # NEW: Timeout tracking
    last_heartbeat: datetime = field(default_factory=datetime.utcnow)
    heartbeat_interval: float = 10.0

# ============================================================================
# Enhanced Carbon Intensity Manager with Retry & Circuit Breaker
# ============================================================================
class CarbonIntensityManager:
    """Real-time carbon intensity integration with retry, circuit breaker, and caching."""

    def __init__(self, config: LayerIntegratorConfig):
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
        self.circuit_breaker_threshold = 5
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
                            'price_usd_per_ton': self.carbon_price_usd_per_ton
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
        intensity_factor = (intensity - 300) / 500
        self.carbon_price_usd_per_ton = max(10.0, base_price * (1.0 + intensity_factor))
        self.price_history.append({
            'timestamp': self.last_update.isoformat() if self.last_update else None,
            'price': self.carbon_price_usd_per_ton
        })

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
            'is_fallback': True
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
# Enhanced Predictive Layer Analyzer with Online Learning
# ============================================================================
class PredictiveLayerAnalyzer:
    """Predictive layer health with online learning using incremental updates."""

    def __init__(self, config: LayerIntegratorConfig, history_window: int = 100):
        self.config = config
        self.history_window = history_window
        self.layer_history = deque(maxlen=history_window)
        self.forecast_history = deque(maxlen=50)
        self.is_trained = False
        self._ml_available = False
        self.model = None
        self.scaler = None
        self.feature_means = None
        self.feature_stds = None
        self.last_training_time = None
        self.training_interval = 300  # seconds

        try:
            from sklearn.preprocessing import StandardScaler
            from sklearn.linear_model import SGDRegressor
            self.scaler = StandardScaler()
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
            logger.warning("Sklearn not available; using fallback moving average")

    def update_history(self, layer_metrics: Dict):
        self.layer_history.append({
            'timestamp': datetime.utcnow(),
            'health_score': layer_metrics.get('health_score', 0.8),
            'gradient_health': layer_metrics.get('gradient_health', 0.5),
            'token_balance': layer_metrics.get('token_balance', 0.5),
            'carbon_intensity': layer_metrics.get('carbon_intensity', 400),
            'active_layers': layer_metrics.get('active_layers', 6),
            'carbon_price': layer_metrics.get('carbon_price', 50.0),
            'resource_scarcity': layer_metrics.get('resource_scarcity', 0.5)
        })

    async def train_forecast_model(self):
        """Train or update the model incrementally."""
        if not self._ml_available:
            return {'status': 'ml_not_available'}

        if len(self.layer_history) < 10:
            return {'status': 'insufficient_data'}

        # Prepare features
        X, y = [], []
        history_list = list(self.layer_history)
        for i in range(len(history_list) - 5):
            features = []
            for j in range(5):
                data = history_list[i + j]
                features.extend([
                    data['health_score'],
                    data['gradient_health'],
                    data['token_balance'],
                    data['carbon_intensity'] / 100,
                    data['active_layers'] / 12,
                    data.get('carbon_price', 50.0) / 100,
                    data.get('resource_scarcity', 0.5)
                ])
            X.append(features)
            y.append(history_list[i + 5]['health_score'])

        X = np.array(X)
        y = np.array(y)

        if self.scaler.mean_ is None:
            X_scaled = self.scaler.fit_transform(X)
        else:
            X_scaled = self.scaler.transform(X)

        # Incremental training
        if self.model is not None:
            for _ in range(3):  # multiple passes for better fit
                self.model.partial_fit(X_scaled, y)
            self.is_trained = True
            self.last_training_time = datetime.utcnow()

        # Calculate R2 score for diagnostics
        if len(X) > 5:
            pred = self.model.predict(X_scaled)
            from sklearn.metrics import r2_score
            r2 = r2_score(y, pred)
        else:
            r2 = 0.0

        return {'status': 'success', 'r2': r2, 'samples': len(X)}

    async def predict_layer_health(self) -> Dict:
        if not self.is_trained or len(self.layer_history) < 10:
            # Fallback: moving average
            if len(self.layer_history) > 0:
                recent = [h['health_score'] for h in list(self.layer_history)[-5:]]
                pred = np.mean(recent) if recent else 0.5
                return {'predicted_health': pred, 'confidence': 0.3, 'trend': 'moving_average'}
            return {'predicted_health': 0.5, 'confidence': 0.0, 'trend': 'insufficient_data'}

        recent = list(self.layer_history)[-5:]
        features = []
        for data in recent:
            features.extend([
                data['health_score'],
                data['gradient_health'],
                data['token_balance'],
                data['carbon_intensity'] / 100,
                data['active_layers'] / 12,
                data.get('carbon_price', 50.0) / 100,
                data.get('resource_scarcity', 0.5)
            ])

        features = np.array(features).reshape(1, -1)
        if self.scaler.mean_ is not None:
            features_scaled = self.scaler.transform(features)
        else:
            features_scaled = features

        prediction = self.model.predict(features_scaled)[0]
        # Confidence based on recency and model stability
        confidence = min(0.9, 0.5 + 0.4 * (len(self.layer_history) / 100))

        if len(self.forecast_history) > 5:
            recent_forecasts = list(self.forecast_history)[-5:]
            trend = "improving" if prediction > recent_forecasts[-1] else "declining" if prediction < recent_forecasts[-1] else "stable"
        else:
            trend = "stable"

        self.forecast_history.append({'prediction': prediction, 'trend': trend})
        return {'predicted_health': prediction, 'confidence': confidence, 'trend': trend,
                'recommended_actions': self._generate_actions(prediction)}

    def _generate_actions(self, prediction: float) -> List[str]:
        actions = []
        if prediction < 0.4:
            actions.append("Increase token allocation for critical layers")
            actions.append("Optimize carbon-aware layer scheduling")
            actions.append("Trigger health recovery protocols")
        elif prediction < 0.6:
            actions.append("Enhance gradient health monitoring")
            actions.append("Improve membrane permeability")
            actions.append("Activate secondary backup layers")
        return actions or ["Layer health is on track"]

# ============================================================================
# Enhanced Layer Cross-Domain Transfer with Effectiveness Tracking
# ============================================================================
class LayerCrossDomainTransfer:
    """Cross-domain knowledge transfer with effectiveness tracking and pruning."""

    def __init__(self):
        self.knowledge_base: Dict[str, Dict[str, Dict]] = {}
        self.transfer_logs = deque(maxlen=1000)
        self.effectiveness_history = deque(maxlen=100)
        self.domain_mappings = {
            'layer→energy': {
                'efficiency_strategies': ['token-based', 'gradient-driven'],
                'resource_allocation': ['dynamic', 'adaptive']
            },
            'layer→carbon': {
                'optimization_strategies': ['load-shifting', 'efficiency-first']
            },
            'layer→helium': {
                'scarcity_strategies': ['efficiency-first', 'conservation']
            },
            'layer→quantum': {
                'circuit_optimization': ['depth-reduction', 'qubit-saving'],
                'scheduling_strategies': ['carbon-aware', 'helium-efficient']
            }
        }

    def transfer_knowledge(self, source_domain: str, target_domain: str,
                          knowledge_type: str, data: Dict[str, Any]) -> Dict:
        key = f"{source_domain}→{target_domain}"
        if key not in self.knowledge_base:
            self.knowledge_base[key] = {}
        if knowledge_type not in self.knowledge_base[key]:
            self.knowledge_base[key][knowledge_type] = {
                'data': data,
                'transfer_count': 1,
                'effectiveness_score': 0.5,
                'last_used': datetime.utcnow()
            }
        else:
            existing = self.knowledge_base[key][knowledge_type]
            existing['data'].update(data)
            existing['transfer_count'] += 1
            existing['last_used'] = datetime.utcnow()

        self.transfer_logs.append({
            'timestamp': datetime.utcnow(),
            'source': source_domain,
            'target': target_domain,
            'type': knowledge_type,
            'effectiveness': self.knowledge_base[key][knowledge_type]['effectiveness_score']
        })

        # Prune stale knowledge (older than 7 days)
        self._prune_stale()
        return self.knowledge_base[key][knowledge_type]

    def update_effectiveness(self, source_domain: str, target_domain: str,
                            knowledge_type: str, effectiveness: float):
        key = f"{source_domain}→{target_domain}"
        if key in self.knowledge_base and knowledge_type in self.knowledge_base[key]:
            entry = self.knowledge_base[key][knowledge_type]
            old_score = entry['effectiveness_score']
            transfer_count = entry['transfer_count']
            # Weighted moving average
            entry['effectiveness_score'] = (old_score * transfer_count + effectiveness) / (transfer_count + 1)
            self.effectiveness_history.append({
                'timestamp': datetime.utcnow(),
                'transfer': key,
                'type': knowledge_type,
                'effectiveness': entry['effectiveness_score']
            })

    def _prune_stale(self, max_age_days: int = 7):
        now = datetime.utcnow()
        for key, domain_data in list(self.knowledge_base.items()):
            for ktype, entry in list(domain_data.items()):
                age = (now - entry['last_used']).days
                if age > max_age_days:
                    del self.knowledge_base[key][ktype]
            if not self.knowledge_base[key]:
                del self.knowledge_base[key]

    def get_transfer_statistics(self) -> Dict:
        total_transfers = len(self.transfer_logs)
        domain_pairs = {}
        for log in self.transfer_logs:
            key = f"{log['source']}→{log['target']}"
            domain_pairs[key] = domain_pairs.get(key, 0) + 1
        avg_effectiveness = np.mean([l.get('effectiveness', 0.5) for l in self.transfer_logs[-50:]]) if self.transfer_logs else 0.5
        return {
            'total_transfers': total_transfers,
            'domain_pairs': domain_pairs,
            'knowledge_types': list(self.knowledge_base.keys()),
            'average_effectiveness': avg_effectiveness,
            'stored_entries': sum(len(d) for d in self.knowledge_base.values())
        }

# ============================================================================
# Enhanced Dynamic Layer Discovery with Retry & Health Checks
# ============================================================================
class DynamicLayerDiscoveryManager:
    """Dynamic layer discovery with retry, circuit breaker, and health monitoring."""

    def __init__(self, config: LayerIntegratorConfig):
        self.config = config
        self.discovered_layers: Dict[int, Dict[str, Any]] = {}
        self.discovery_registry: Dict[str, Dict] = {}
        self._lock = asyncio.Lock()
        self.max_retries = config.max_retries
        self.health_failure_counts: Dict[int, int] = defaultdict(int)
        self.circuit_breaker_threshold = config.circuit_breaker_failure_threshold
        self.circuit_open_layers: Set[int] = set()
        self.circuit_open_until: Dict[int, datetime] = {}

        logger.info(f"DynamicLayerDiscoveryManager initialized (max_retries={self.max_retries})")

    async def discover_layer(self, layer_number: int, service_url: str) -> bool:
        """Discover a layer with retry and circuit breaker."""
        # Circuit breaker check
        if layer_number in self.circuit_open_layers:
            if datetime.utcnow() < self.circuit_open_until.get(layer_number, datetime.min):
                logger.warning(f"Discovery circuit breaker open for layer {layer_number}")
                return False
            else:
                self.circuit_open_layers.remove(layer_number)
                self.health_failure_counts[layer_number] = 0
                logger.info(f"Discovery circuit breaker reset for layer {layer_number}")

        for attempt in range(self.max_retries):
            try:
                async with self._lock:
                    # Simulate discovery (replace with real service discovery)
                    capabilities = self._get_layer_capabilities(layer_number)
                    health = await self._check_layer_health(service_url)

                    if health:
                        self.discovered_layers[layer_number] = {
                            'url': service_url,
                            'capabilities': capabilities,
                            'health': health,
                            'discovered_at': datetime.utcnow().isoformat(),
                            'status': 'active'
                        }
                        self.health_failure_counts[layer_number] = 0
                        logger.info(f"Discovered layer {layer_number} at {service_url}")
                        return True
                    else:
                        logger.warning(f"Layer {layer_number} health check failed, attempt {attempt+1}")
                        if attempt == self.max_retries - 1:
                            self._handle_discovery_failure(layer_number)
                            return False
                        await asyncio.sleep(2 ** attempt)
            except Exception as e:
                logger.error(f"Layer discovery error for {layer_number}: {e}, attempt {attempt+1}")
                if attempt == self.max_retries - 1:
                    self._handle_discovery_failure(layer_number)
                    return False
                await asyncio.sleep(2 ** attempt)

        return False

    def _handle_discovery_failure(self, layer_number: int):
        self.health_failure_counts[layer_number] += 1
        if self.health_failure_counts[layer_number] >= self.circuit_breaker_threshold:
            self.circuit_open_layers.add(layer_number)
            self.circuit_open_until[layer_number] = datetime.utcnow() + timedelta(minutes=5)
            logger.error(f"Discovery circuit breaker opened for layer {layer_number}")
        if layer_number in self.discovered_layers:
            self.discovered_layers[layer_number]['status'] = 'unreachable'

    async def _check_layer_health(self, service_url: str) -> bool:
        """Health check for a discovered layer."""
        try:
            # Simulate health check (replace with actual HTTP health endpoint)
            async with aiohttp.ClientSession() as session:
                async with session.get(f"{service_url}/health", timeout=5) as resp:
                    return resp.status == 200
        except Exception:
            return False

    def _get_layer_capabilities(self, layer_number: int) -> List[str]:
        capabilities = {
            0: ["workload_classification", "helium_profiling"],
            1: ["meta_cognition", "reflection", "budget_management"],
            2: ["symbolic_validation", "graph_reasoning"],
            3: ["dual_axis_scoring", "zone_mapping"],
            4: ["model_quantization", "helium_aware_training"],
            5: ["data_compression", "batching", "caching"],
            6: ["distributed_execution", "load_balancing"],
            7: ["carbon_monitoring", "helium_monitoring"],
            8: ["immutable_logging", "audit_trail"],
            9: ["pareto_analysis", "3d_benchmarking"],
            10: ["quantum_circuits", "quantum_scheduling"],
            11: ["visualization", "dashboards", "alerting"]
        }
        return capabilities.get(layer_number, [])

    def get_discovered_layers(self) -> Dict[int, Dict[str, Any]]:
        return self.discovered_layers.copy()

    def get_layer_status(self, layer_number: int) -> Optional[Dict]:
        return self.discovered_layers.get(layer_number)

# ============================================================================
# Enhanced Event Correlation Engine with Temporal & Causal Patterns
# ============================================================================
class EventCorrelationEngine:
    """Event correlation with temporal windows and causal pattern detection."""

    def __init__(self, config: LayerIntegratorConfig):
        self.config = config
        self.event_graph: Dict[str, List[str]] = defaultdict(list)
        self.event_metadata: Dict[str, Dict] = {}
        self.correlation_patterns: Dict[str, List[str]] = defaultdict(list)
        self._lock = asyncio.Lock()
        self.temporal_window_seconds = 60
        self.pattern_keywords = {
            'workflow_start': ['initialize', 'start', 'begin'],
            'workflow_end': ['complete', 'finish', 'end'],
            'workflow_error': ['error', 'fail', 'exception'],
            'workflow_retry': ['retry', 'recover', 'resume'],
            'workflow_health': ['health', 'status', 'heartbeat']
        }
        self.recent_events = deque(maxlen=1000)
        logger.info("EventCorrelationEngine initialized")

    async def correlate_event(self, event: LayerEvent) -> Optional[str]:
        """Correlate event with existing events using temporal and causal patterns."""
        async with self._lock:
            self.recent_events.append(event)

            # 1. Temporal correlation: check if event happens within a window of a parent
            for other in reversed(list(self.recent_events)[-20:]):  # look at last 20
                if other.event_id == event.event_id:
                    continue
                time_diff = (event.timestamp - other.timestamp).total_seconds()
                if 0 < time_diff < self.temporal_window_seconds:
                    # If event type is related, consider it a child
                    if self._is_related(other.event_type, event.event_type):
                        event.parent_event_id = other.event_id
                        self.event_graph[other.event_id].append(event.event_id)
                        return other.correlation_id or other.event_id

            # 2. Pattern-based correlation
            pattern = self._detect_pattern(event)
            if pattern:
                correlation_id = f"corr_{datetime.utcnow().timestamp()}_{pattern}"
                self.correlation_patterns[correlation_id].append(event.event_id)
                event.correlation_id = correlation_id
                self.event_metadata[correlation_id] = {'pattern': pattern, 'timestamp': event.timestamp.isoformat()}
                return correlation_id

            return None

    def _is_related(self, type1: str, type2: str) -> bool:
        """Check if two event types are semantically related."""
        # Simple keyword overlap
        words1 = set(type1.lower().split('_'))
        words2 = set(type2.lower().split('_'))
        return bool(words1 & words2)

    def _detect_pattern(self, event: LayerEvent) -> Optional[str]:
        for pattern, keywords in self.pattern_keywords.items():
            if any(kw in event.event_type.lower() for kw in keywords):
                return pattern
        return None

    def get_event_chain(self, event_id: str) -> List[str]:
        chain = [event_id]
        children = self.event_graph.get(event_id, [])
        for child in children:
            chain.extend(self.get_event_chain(child))
        return chain

    def get_correlation_stats(self) -> Dict[str, Any]:
        return {
            'total_events': len(self.recent_events),
            'correlation_patterns': len(self.correlation_patterns),
            'event_graph_edges': sum(len(children) for children in self.event_graph.values()),
            'total_metadata': len(self.event_metadata),
            'temporal_window_seconds': self.temporal_window_seconds
        }

# ============================================================================
# Enhanced Gradient-Aware Cache Manager with Weighted Eviction
# ============================================================================
class GradientAwareCacheManager:
    """Gradient-aware cache with weighted eviction and dynamic TTL."""

    def __init__(self, config: LayerIntegratorConfig):
        self.config = config
        self.cache: Dict[str, CacheEntry] = {}
        self._lock = asyncio.Lock()
        self.base_ttl = config.cache_ttl_seconds
        self.max_size = config.max_cache_size
        self.gradient_threshold = 0.3
        logger.info(f"GradientAwareCacheManager initialized (max_size={self.max_size})")

    async def get(self, key: str, gradient_level: float = 0.5) -> Optional[Any]:
        async with self._lock:
            if key not in self.cache:
                return None
            entry = self.cache[key]

            # Gradient-based invalidation
            if abs(gradient_level - entry.gradient_level_at_creation) > entry.gradient_threshold:
                entry.invalidated_by_gradient = True
                del self.cache[key]
                return None

            if datetime.utcnow() > entry.expires_at:
                del self.cache[key]
                return None

            entry.access_count += 1
            entry.last_accessed = datetime.utcnow()
            return entry.value

    async def set(self, key: str, value: Any, layer_number: int, gradient_level: float = 0.5,
                  token_balance: float = 0.5):
        async with self._lock:
            if len(self.cache) >= self.max_size:
                await self._evict_weighted()

            # Dynamic TTL based on token balance and gradient
            ttl = self.base_ttl
            if token_balance > 0.5:
                ttl *= 1.5
            elif token_balance < 0.2:
                ttl *= 0.5
            ttl *= (1.0 + gradient_level * 0.5)

            entry = CacheEntry(
                key=key,
                value=value,
                created_at=datetime.utcnow(),
                expires_at=datetime.utcnow() + timedelta(seconds=ttl),
                layer_number=layer_number,
                gradient_level_at_creation=gradient_level,
                token_backed=token_balance > 0.2,
                weight=1.0  # initial weight
            )
            self.cache[key] = entry

    async def _evict_weighted(self):
        """Evict using a weighted score: higher weight = more important."""
        if not self.cache:
            return
        # Score = access_count / (age + 1) * weight
        now = datetime.utcnow()
        min_score = float('inf')
        evict_key = None
        for key, entry in self.cache.items():
            age = (now - entry.last_accessed).total_seconds()
            score = entry.access_count / (age + 1) * entry.weight
            if score < min_score:
                min_score = score
                evict_key = key
        if evict_key:
            del self.cache[evict_key]

    async def invalidate_by_gradient(self, gradient_level: float):
        async with self._lock:
            to_remove = [k for k, e in self.cache.items()
                         if abs(gradient_level - e.gradient_level_at_creation) > e.gradient_threshold]
            for k in to_remove:
                self.cache[k].invalidated_by_gradient = True
                del self.cache[k]
            if to_remove:
                logger.info(f"Invalidated {len(to_remove)} cache entries by gradient")

    def get_stats(self) -> Dict[str, Any]:
        return {
            'size': len(self.cache),
            'max_size': self.max_size,
            'base_ttl': self.base_ttl,
            'gradient_threshold': self.gradient_threshold,
            'entries': [{
                'key': e.key,
                'layer_number': e.layer_number,
                'access_count': e.access_count,
                'expires_at': e.expires_at.isoformat(),
                'invalidated_by_gradient': e.invalidated_by_gradient
            } for e in list(self.cache.values())[-10:]]
        }

# ============================================================================
# Enhanced Distributed Transaction Coordinator with Timeout & Participant Failures
# ============================================================================
class DistributedTransactionCoordinator:
    """Distributed transaction coordinator with timeout handling and participant failure detection."""

    def __init__(self, config: LayerIntegratorConfig):
        self.config = config
        self.coordinator_id = config.coordinator_id
        self.active_transactions: Dict[str, TransactionContext] = {}
        self._lock = asyncio.Lock()
        self.participant_timeout = 30.0
        self.heartbeat_interval = 5.0
        logger.info(f"DistributedTransactionCoordinator initialized: {self.coordinator_id}")

    async def begin_distributed_transaction(
        self,
        layers_involved: List[int],
        participants: List[str],
        timeout_seconds: float = 60.0
    ) -> TransactionContext:
        async with self._lock:
            txn = TransactionContext(
                transaction_id=f"dist_txn_{datetime.utcnow().timestamp()}_{uuid.uuid4().hex[:8]}",
                started_at=datetime.utcnow(),
                layers_involved=layers_involved,
                timeout_seconds=timeout_seconds,
                coordinator_id=self.coordinator_id,
                participants=participants,
                distributed_status={p: 'pending' for p in participants}
            )
            self.active_transactions[txn.transaction_id] = txn
            logger.info(f"Started distributed transaction: {txn.transaction_id}")
            return txn

    async def prepare_participant(self, transaction_id: str, participant: str) -> bool:
        async with self._lock:
            if transaction_id not in self.active_transactions:
                return False
            txn = self.active_transactions[transaction_id]
            if participant not in txn.participants:
                return False

            # Simulate prepare phase with timeout
            try:
                prepared = await asyncio.wait_for(
                    self._simulate_prepare(participant),
                    timeout=self.participant_timeout
                )
                txn.distributed_status[participant] = 'prepared' if prepared else 'failed'
                if prepared:
                    logger.info(f"Participant {participant} prepared for {transaction_id}")
                else:
                    logger.warning(f"Participant {participant} failed to prepare for {transaction_id}")
                return prepared
            except asyncio.TimeoutError:
                logger.error(f"Participant {participant} prepare timeout for {transaction_id}")
                txn.distributed_status[participant] = 'timeout'
                return False

    async def _simulate_prepare(self, participant: str) -> bool:
        # Simulate prepare; replace with actual RPC
        await asyncio.sleep(0.1)  # realistic delay
        return np.random.random() > 0.1

    async def commit_distributed_transaction(self, transaction_id: str) -> bool:
        async with self._lock:
            if transaction_id not in self.active_transactions:
                return False
            txn = self.active_transactions[transaction_id]

            # Check if all participants are prepared
            all_prepared = all(
                status == 'prepared' for status in txn.distributed_status.values()
            )
            if not all_prepared:
                logger.warning(f"Not all participants prepared for {transaction_id}")
                await self.rollback_distributed_transaction(transaction_id)
                return False

            # Commit all participants
            for participant in txn.participants:
                txn.distributed_status[participant] = 'committed'
            txn.status = 'committed'
            del self.active_transactions[transaction_id]
            logger.info(f"Distributed transaction committed: {transaction_id}")
            return True

    async def rollback_distributed_transaction(self, transaction_id: str) -> bool:
        async with self._lock:
            if transaction_id not in self.active_transactions:
                return False
            txn = self.active_transactions[transaction_id]
            for participant in txn.participants:
                txn.distributed_status[participant] = 'rolled_back'
            txn.status = 'rolled_back'
            del self.active_transactions[transaction_id]
            logger.info(f"Distributed transaction rolled back: {transaction_id}")
            return True

    async def heartbeat(self, transaction_id: str):
        """Send heartbeat to keep transaction alive."""
        async with self._lock:
            if transaction_id in self.active_transactions:
                txn = self.active_transactions[transaction_id]
                txn.last_heartbeat = datetime.utcnow()
                # Check timeout
                if (datetime.utcnow() - txn.started_at).total_seconds() > txn.timeout_seconds:
                    await self.rollback_distributed_transaction(transaction_id)
                    logger.warning(f"Transaction {transaction_id} timed out and rolled back")

    def get_transaction_status(self, transaction_id: str) -> Optional[Dict]:
        if transaction_id in self.active_transactions:
            txn = self.active_transactions[transaction_id]
            return {
                'transaction_id': txn.transaction_id,
                'status': txn.status,
                'participants': txn.distributed_status,
                'layers_involved': txn.layers_involved,
                'started_at': txn.started_at.isoformat(),
                'last_heartbeat': txn.last_heartbeat.isoformat()
            }
        return None

# ============================================================================
# Sustainability Score Calculator (NEW)
# ============================================================================
class SustainabilityScoreCalculator:
    """Calculates a composite sustainability score from multiple metrics."""

    def __init__(self, config: LayerIntegratorConfig):
        self.config = config
        self.weights = config.sustainability_weights

    def calculate(self,
                 carbon_savings_kg: float,
                 helium_saved_l: float,
                 renewable_usage_percent: float,
                 token_efficiency: float,
                 layer_health_avg: float,
                 total_energy_saved_kwh: float = 0.0) -> float:
        """
        Compute sustainability score as weighted sum of normalized components.
        """
        # Normalize each component to [0,1]
        norm_carbon = min(1.0, carbon_savings_kg / 1000)  # assume max 1000 kg
        norm_helium = min(1.0, helium_saved_l / 100)     # assume max 100 L
        norm_renewable = min(1.0, renewable_usage_percent / 100)
        norm_tokens = min(1.0, token_efficiency)
        norm_health = min(1.0, layer_health_avg)

        score = (
            self.weights.get('carbon_savings', 0.3) * norm_carbon +
            self.weights.get('helium_efficiency', 0.2) * norm_helium +
            self.weights.get('renewable_usage', 0.2) * norm_renewable +
            self.weights.get('token_efficiency', 0.15) * norm_tokens +
            self.weights.get('layer_health', 0.15) * norm_health
        )
        return max(0.0, min(1.0, score))

    def get_weighted_breakdown(self, components: Dict[str, float]) -> Dict[str, float]:
        """Return individual weighted contributions for diagnostics."""
        breakdown = {}
        for key, value in components.items():
            weight = self.weights.get(key, 0.0)
            breakdown[key] = value * weight
        return breakdown

# ============================================================================
# Telemetry Collector (NEW)
# ============================================================================
class TelemetryCollector:
    """Collects and exports telemetry metrics for monitoring."""

    def __init__(self, exporter: Optional[str] = None):
        self.exporter = exporter  # e.g., 'prometheus'
        self.metrics: Dict[str, Any] = defaultdict(lambda: defaultdict(int))
        self._lock = asyncio.Lock()
        logger.info(f"TelemetryCollector initialized (exporter={exporter})")

    def increment(self, metric_name: str, tags: Optional[Dict[str, str]] = None, value: float = 1.0):
        """Increment a counter metric."""
        key = self._make_key(metric_name, tags)
        self.metrics['counters'][key] += value

    def gauge(self, metric_name: str, value: float, tags: Optional[Dict[str, str]] = None):
        """Set a gauge metric."""
        key = self._make_key(metric_name, tags)
        self.metrics['gauges'][key] = value

    def histogram(self, metric_name: str, value: float, tags: Optional[Dict[str, str]] = None):
        """Record a histogram observation."""
        key = self._make_key(metric_name, tags)
        if key not in self.metrics['histograms']:
            self.metrics['histograms'][key] = []
        self.metrics['histograms'][key].append(value)
        # Keep last 1000 observations
        if len(self.metrics['histograms'][key]) > 1000:
            self.metrics['histograms'][key] = self.metrics['histograms'][key][-1000:]

    def _make_key(self, metric_name: str, tags: Optional[Dict[str, str]]) -> str:
        if tags:
            tag_str = ','.join(f"{k}={v}" for k, v in sorted(tags.items()))
            return f"{metric_name}{{{tag_str}}}"
        return metric_name

    async def export(self):
        """Export metrics to the configured backend."""
        if self.exporter == 'prometheus':
            # Prometheus text format export
            output = []
            for key, value in self.metrics['counters'].items():
                output.append(f"# TYPE {key} counter\n{key} {value}")
            for key, value in self.metrics['gauges'].items():
                output.append(f"# TYPE {key} gauge\n{key} {value}")
            for key, values in self.metrics['histograms'].items():
                output.append(f"# TYPE {key} histogram\n{key}_count {len(values)}\n{key}_sum {sum(values)}")
            return "\n".join(output)
        else:
            return None

    def reset(self):
        self.metrics.clear()
        self.metrics['counters'] = defaultdict(int)
        self.metrics['gauges'] = {}
        self.metrics['histograms'] = defaultdict(list)

# ============================================================================
# Enhanced Layer Integrator (Main Class)
# ============================================================================
class EnhancedLayerIntegrator:
    """
    Enhanced Layer Integrator v6.1.1 - Complete Green Agent Implementation
    """

    def __init__(self, config: Optional[LayerIntegratorConfig] = None):
        if config is None:
            config = LayerIntegratorConfig()
        self.config = config

        # Feature flags from config
        self.enable_cache = config.enable_cache
        self.enable_circuit_breaker = config.enable_circuit_breaker
        self.enable_retry = config.enable_retry
        self.enable_events = config.enable_events
        self.enable_transactions = config.enable_transactions
        self.enable_monitoring = config.enable_monitoring
        self.enable_bio_integration = config.enable_bio_integration and BIO_INSPIRED_AVAILABLE
        self.enable_carbon_intensity = config.enable_carbon_intensity
        self.enable_predictive = config.enable_predictive
        self.enable_cross_domain = config.enable_cross_domain
        self.enable_sustainability_scoring = config.enable_sustainability_scoring
        self.enable_dynamic_discovery = config.enable_dynamic_discovery
        self.enable_event_correlation = config.enable_event_correlation
        self.enable_gradient_cache = config.enable_gradient_cache
        self.enable_distributed_txns = config.enable_distributed_txns

        # Bio-inspired modules
        self.token_manager = None
        self.gradient_manager = None
        self.scheduler = None
        self.compartment_manager = None
        self.biomass_storage = None
        self.harvester = None

        # Initialize components
        self.carbon_manager = CarbonIntensityManager(config) if self.enable_carbon_intensity else None
        self.predictive_analyzer = PredictiveLayerAnalyzer(config) if self.enable_predictive else None
        self.cross_domain_transfer = LayerCrossDomainTransfer() if self.enable_cross_domain else None
        self.discovery_manager = DynamicLayerDiscoveryManager(config) if self.enable_dynamic_discovery else None
        self.event_correlation = EventCorrelationEngine(config) if self.enable_event_correlation else None
        self.gradient_cache = GradientAwareCacheManager(config) if self.enable_gradient_cache else None
        self.distributed_coordinator = DistributedTransactionCoordinator(config) if self.enable_distributed_txns else None
        self.sustainability_calculator = SustainabilityScoreCalculator(config) if self.enable_sustainability_scoring else None
        self.telemetry = TelemetryCollector() if self.enable_monitoring else None

        # MoE integration
        self.expert_router = None
        self.helium_provider = None
        self.fl_monitor = None

        # Layer registry
        self.layers: Dict[int, LayerInfo] = {}
        self.layer_modules: Dict[int, Any] = {}
        self.integration_status: Dict[int, bool] = {i: False for i in range(12)}

        # Cache (fallback)
        self.cache: Dict[str, CacheEntry] = {}
        self.cache_ttl = config.cache_ttl_seconds
        self.max_cache_size = config.max_cache_size

        # Event system
        self.event_subscribers: Dict[str, List[Callable]] = defaultdict(list)
        self.event_queue: asyncio.Queue = asyncio.Queue(maxsize=10000)

        # Retry config
        self.retry_config = RetryConfig(
            max_retries=config.max_retries,
            base_delay_ms=config.retry_base_delay_ms,
            max_delay_ms=config.retry_max_delay_ms
        )

        # Transactions
        self.active_transactions: Dict[str, TransactionContext] = {}

        # Performance metrics
        self.layer_latency: Dict[int, List[float]] = defaultdict(list)
        self.layer_errors: Dict[int, int] = defaultdict(int)
        self.layer_calls: Dict[int, int] = defaultdict(int)

        # Sustainability tracking
        self.total_carbon_savings_kg = 0.0
        self.total_helium_saved_l = 0.0
        self.total_energy_saved_kwh = 0.0
        self.sustainability_score = 0.0

        # Thread pool
        self.executor = ThreadPoolExecutor(max_workers=4)

        # Initialize all 12 layers
        self._initialize_all_layers()

        # Start background tasks
        self._start_background_tasks()

        logger.info(
            f"EnhancedLayerIntegrator v6.1.1 initialized: "
            f"layers={len(self.layers)}/12, "
            f"bio_integration={self.enable_bio_integration}, "
            f"carbon_intensity={self.enable_carbon_intensity}, "
            f"predictive={self.enable_predictive}, "
            f"dynamic_discovery={self.enable_dynamic_discovery}, "
            f"event_correlation={self.enable_event_correlation}, "
            f"gradient_cache={self.enable_gradient_cache}, "
            f"distributed_txns={self.enable_distributed_txns}, "
            f"sustainability_scoring={self.enable_sustainability_scoring}"
        )

    def _initialize_all_layers(self):
        layer_definitions = {
            0: ("Workload + Helium Profile", "2.4.0", [1, 2]),
            1: ("Meta-Cognition + Helium Adapter", "2.4.0", [0, 2, 3]),
            2: ("Neuro-Symbolic + Graph Reasoning", "2.4.0", [1, 3]),
            3: ("Dual-Axis Decision Core", "2.4.0", [1, 2, 4, 5]),
            4: ("Helium-Aware ML", "2.4.0", [3, 5]),
            5: ("Data Optimization", "2.4.0", [3, 4]),
            6: ("Distributed Execution", "2.4.0", [3, 7]),
            7: ("Dual Monitoring (C + H)", "2.4.0", [6, 8]),
            8: ("Immutable Dual Ledger", "2.4.0", [7, 9]),
            9: ("3D Pareto Benchmarking", "2.4.0", [8, 10]),
            10: ("Quantum Integration (beta)", "2.4.0-beta", [9, 11]),
            11: ("Dashboard & Visualization", "2.4.0", [10])
        }
        entangled_pairs = [(0, 1), (1, 2), (2, 3), (3, 4), (4, 5),
                          (6, 7), (7, 8), (8, 9), (9, 10), (10, 11)]

        for layer_num, (name, version, deps) in layer_definitions.items():
            entangled = []
            for a, b in entangled_pairs:
                if layer_num == a and b not in entangled:
                    entangled.append(b)
                elif layer_num == b and a not in entangled:
                    entangled.append(a)

            self.layers[layer_num] = LayerInfo(
                layer_number=layer_num,
                layer_name=name,
                version=version,
                dependencies=deps,
                capabilities=self._get_layer_capabilities(layer_num),
                entangled_layers=entangled,
                sustainability_score=0.5,
                discovery_timestamp=datetime.utcnow() if self.enable_dynamic_discovery else None
            )

    def _get_layer_capabilities(self, layer_num: int) -> List[str]:
        capabilities = {
            0: ["workload_classification", "helium_profiling", "task_embedding"],
            1: ["meta_cognition", "reflection", "budget_management", "adaptive_learning"],
            2: ["symbolic_validation", "graph_reasoning", "policy_enforcement"],
            3: ["dual_axis_scoring", "zone_mapping", "action_classification"],
            4: ["model_quantization", "helium_aware_training", "pruning"],
            5: ["data_compression", "batching", "streaming", "caching"],
            6: ["distributed_execution", "load_balancing", "fault_tolerance"],
            7: ["carbon_monitoring", "helium_monitoring", "prometheus_export"],
            8: ["immutable_logging", "audit_trail", "iso_compliance"],
            9: ["pareto_analysis", "3d_benchmarking", "tradeoff_optimization"],
            10: ["quantum_circuits", "quantum_scheduling", "error_mitigation"],
            11: ["visualization", "dashboards", "alerting", "reporting"]
        }
        return capabilities.get(layer_num, [])

    def _start_background_tasks(self):
        asyncio.create_task(self._health_check_loop())
        asyncio.create_task(self._event_processing_loop())
        asyncio.create_task(self._cache_cleanup_loop())
        asyncio.create_task(self._transaction_timeout_loop())
        if self.enable_bio_integration:
            asyncio.create_task(self._bio_sync_loop())
        if self.enable_carbon_intensity:
            asyncio.create_task(self._carbon_update_loop())
        if self.enable_dynamic_discovery:
            asyncio.create_task(self._discovery_loop())

    # ==========================================================================
    # Injection Methods
    # ==========================================================================
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
        if any([self.token_manager, self.gradient_manager, self.compartment_manager]):
            self.enable_bio_integration = True

    def set_expert_router(self, router: 'ExpertRouter'):
        self.expert_router = router
        logger.info("Expert Router injected into Layer Integrator")

    def set_helium_provider(self, provider):
        self.helium_provider = provider
        logger.info("Helium provider injected into Layer Integrator")

    def set_fl_monitor(self, fl_monitor):
        self.fl_monitor = fl_monitor
        logger.info("FL monitor injected into Layer Integrator")

    # ==========================================================================
    # Context Builder for MoE Expert System
    # ==========================================================================
    async def build_context(self) -> Dict[str, Any]:
        """Build a comprehensive context for MoE expert router."""
        context = {}

        # 1. Helium telemetry
        if self.helium_provider:
            context['helium_scarcity'] = self.helium_provider.get_scarcity()
            context['helium_cost_index'] = self.helium_provider.get_cost_index()
            context['avg_client_energy'] = self.helium_provider.get_avg_client_energy()
        else:
            context['helium_scarcity'] = 0.5
            context['helium_cost_index'] = 1.0
            context['avg_client_energy'] = 0.5

        # 2. Carbon intensity
        if self.enable_carbon_intensity and self.carbon_manager:
            carbon_intensity = await self.carbon_manager.get_current_intensity()
            carbon_price = await self.carbon_manager.get_current_price()
            context['carbon_intensity'] = carbon_intensity / 1000.0
            context['carbon_price_usd'] = carbon_price
        else:
            context['carbon_intensity'] = 0.5
            context['carbon_price_usd'] = 50.0

        # 3. Bio-inspired signals
        gradients = self._get_real_gradient_levels()
        context['gradient_carbon'] = gradients.get('carbon', 0.5)
        context['gradient_helium'] = gradients.get('helium', 0.5)
        context['gradient_trust'] = gradients.get('trust', 0.5)
        context['gradient_opportunity'] = gradients.get('opportunity', 0.5)
        context['token_balance_norm'] = self._get_real_token_availability()
        context['harvester_stress'] = self._get_harvester_vitality()
        context['avg_layer_health'] = np.mean([info.gradient_health for info in self.layers.values()])

        # 4. Federated Learning metrics
        if self.fl_monitor:
            context['model_loss'] = self.fl_monitor.get_loss()
            context['gradient_variance'] = self.fl_monitor.get_gradient_variance()
            context['accuracy'] = self.fl_monitor.get_accuracy()
        else:
            context['model_loss'] = 0.0
            context['gradient_variance'] = 0.0
            context['accuracy'] = 0.0

        # 5. Sustainability
        context['sustainability_score'] = self.sustainability_score
        context['carbon_savings_kg'] = self.total_carbon_savings_kg
        context['helium_saved_l'] = self.total_helium_saved_l
        context['energy_saved_kwh'] = self.total_energy_saved_kwh

        # 6. Predictions
        if self.enable_predictive and self.predictive_analyzer:
            forecast = await self.predictive_analyzer.predict_layer_health()
            context['predicted_layer_health'] = forecast.get('predicted_health', 0.5)
            context['prediction_confidence'] = forecast.get('confidence', 0.0)

        return context

    def _get_real_gradient_levels(self) -> Dict[str, float]:
        if self.gradient_manager:
            return self.gradient_manager.get_field_strengths()
        return {'carbon': 0.5, 'helium': 0.5, 'trust': 0.5, 'opportunity': 0.5}

    def _get_real_token_availability(self) -> float:
        if self.token_manager:
            summary = self.token_manager.get_system_summary()
            return min(1.0, summary.get('total_balance', 500) / 1000)
        return 0.5

    def _get_harvester_vitality(self) -> float:
        if self.harvester:
            stats = self.harvester.get_harvesting_stats()
            total = stats.get('total_harvested', 0)
            return min(1.0, total / max(total + 100, 1))
        return 0.5

    # ==========================================================================
    # Bio-Inspired Helper Methods (with fallback)
    # ==========================================================================
    def _get_gradient_health(self, layer_number: int) -> float:
        if self.gradient_manager:
            trust = self.gradient_manager.fields.get('trust')
            if trust:
                return trust.gradient_strength
        return 0.7

    def _get_membrane_permeability(self, layer_number: int) -> str:
        if self.compartment_manager:
            layer_types = {0: 'energy', 1: 'energy', 2: 'data', 3: 'data',
                          4: 'energy', 5: 'data', 6: 'iot', 7: 'data',
                          8: 'data', 9: 'energy', 10: 'quantum', 11: 'data'}
            expert_type = layer_types.get(layer_number, 'data')
            compartment = self.compartment_manager.find_best_compartment(expert_type)
            if compartment:
                return compartment.membrane.permeability.value
        return 'selective'

    def _get_token_backed_cache_ttl(self) -> float:
        if self.token_manager:
            summary = self.token_manager.get_system_summary()
            balance = summary.get('total_balance', 500)
            if balance > 500:
                return 120.0
            elif balance < 100:
                return 30.0
        return self.cache_ttl

    def _recover_tokens_on_rollback(self, transaction_id: str, amount: float) -> float:
        if self.token_manager:
            return self.token_manager.recover_tokens(
                token_ids=[f"txn_{transaction_id}"],
                completion_percentage=0.5
            )
        return 0.0

    def _get_gradient_modulated_retry_delay(self, base_delay: float) -> float:
        if self.gradient_manager:
            carbon = self.gradient_manager.fields.get('carbon')
            if carbon and carbon.gradient_strength > 0.7:
                return base_delay * 2.0
            elif carbon and carbon.gradient_strength < 0.3:
                return base_delay * 0.5
        return base_delay

    def _get_entangled_resources(self, layer_number: int) -> List[str]:
        entangled = []
        if layer_number in self.layers:
            for other_layer in self.layers[layer_number].entangled_layers:
                entangled.append(f"layer_{other_layer}")
        if self.biomass_storage:
            stats = self.biomass_storage.get_storage_stats()
            if stats.get('collateral_pool', 0) > 0:
                entangled.append('biomass_collateral')
        return entangled

    def _get_circuit_recovery_delay(self, layer_number: int) -> float:
        if self.gradient_manager:
            trust = self.gradient_manager.fields.get('trust')
            if trust and trust.gradient_strength > self.layers[layer_number].circuit_breaker.gradient_health_threshold:
                return 15.0
            return 45.0
        return 30.0

    # ==========================================================================
    # Background Loops
    # ==========================================================================
    async def _bio_sync_loop(self):
        while True:
            try:
                if not self.enable_bio_integration:
                    await asyncio.sleep(60)
                    continue
                for layer_num, layer_info in self.layers.items():
                    layer_info.gradient_health = self._get_gradient_health(layer_num)
                    layer_info.membrane_permeability = self._get_membrane_permeability(layer_num)
                    layer_info.harvester_vitality = self._get_harvester_vitality()
                    if self.token_manager:
                        account = self.token_manager.get_account_summary(f"layer_{layer_num}")
                        if account:
                            layer_info.token_balance = account.get('balance', 0)

                    # Health-based circuit reset
                    if self.enable_circuit_breaker and layer_info.gradient_health > 0.6:
                        if layer_info.circuit_breaker.state == CircuitState.OPEN:
                            layer_info.circuit_breaker.state = CircuitState.RECOVERING
                            layer_info.circuit_breaker.record_recovery_attempt()
                            recovery_delay = self._get_circuit_recovery_delay(layer_num)
                            if layer_info.circuit_breaker.recovery_attempts > 2:
                                layer_info.circuit_breaker.state = CircuitState.CLOSED
                                layer_info.circuit_breaker.failure_count = 0
                                logger.info(f"Circuit breaker reset for layer {layer_num}")

                # Gradient-aware cache invalidation
                if self.enable_gradient_cache and self.gradient_cache:
                    gradients = self._get_real_gradient_levels()
                    await self.gradient_cache.invalidate_by_gradient(gradients.get('trust', 0.5))

                if self.enable_cache:
                    self.cache_ttl = self._get_token_backed_cache_ttl()

                # Telemetry
                if self.enable_monitoring and self.telemetry:
                    self.telemetry.gauge('layer_gradient_health', np.mean([l.gradient_health for l in self.layers.values()]))
                    self.telemetry.gauge('layer_token_balance', np.mean([l.token_balance for l in self.layers.values()]))

                await asyncio.sleep(30)
            except Exception as e:
                logger.error(f"Bio sync error: {str(e)}")
                await asyncio.sleep(60)

    async def _carbon_update_loop(self):
        while True:
            try:
                if self.enable_carbon_intensity and self.carbon_manager:
                    await self.carbon_manager.update_carbon_intensity()
                    if self.enable_monitoring and self.telemetry:
                        self.telemetry.gauge('carbon_intensity', await self.carbon_manager.get_current_intensity())
                        self.telemetry.gauge('carbon_price_usd', await self.carbon_manager.get_current_price())
                await asyncio.sleep(self.config.carbon_update_interval)
            except Exception as e:
                logger.error(f"Carbon update error: {str(e)}")
                await asyncio.sleep(60)

    async def _health_check_loop(self):
        while True:
            try:
                for layer_num, layer_info in self.layers.items():
                    if layer_num not in self.layer_modules:
                        continue
                    if hasattr(self.layer_modules[layer_num], 'health_check'):
                        try:
                            is_healthy = await self.call_layer(layer_num, 'health_check', timeout=5.0, retry=False)
                            if is_healthy:
                                layer_info.status = LayerStatus.HEALTHY
                                layer_info.last_heartbeat = datetime.utcnow()
                            else:
                                layer_info.status = LayerStatus.UNHEALTHY
                        except Exception:
                            layer_info.status = LayerStatus.UNHEALTHY
                    if self.enable_bio_integration:
                        layer_info.gradient_health = self._get_gradient_health(layer_num)
                        layer_info.membrane_permeability = self._get_membrane_permeability(layer_num)
                    heartbeat_age = (datetime.utcnow() - layer_info.last_heartbeat).total_seconds()
                    if heartbeat_age > 60 and layer_info.status == LayerStatus.HEALTHY:
                        layer_info.status = LayerStatus.DEGRADED

                # Update predictive analyzer
                if self.enable_predictive and self.predictive_analyzer:
                    active_layers = sum(1 for info in self.layers.values() if info.status == LayerStatus.HEALTHY)
                    carbon_price = await self.carbon_manager.get_current_price() if self.enable_carbon_intensity else 50.0
                    self.predictive_analyzer.update_history({
                        'health_score': active_layers / 12,
                        'gradient_health': np.mean([info.gradient_health for info in self.layers.values()]),
                        'token_balance': np.mean([info.token_balance for info in self.layers.values()]),
                        'carbon_intensity': await self.carbon_manager.get_current_intensity() if self.enable_carbon_intensity else 400,
                        'active_layers': active_layers,
                        'carbon_price': carbon_price,
                        'resource_scarcity': 1.0 - (active_layers / 12)
                    })
                    await self.predictive_analyzer.train_forecast_model()

                await asyncio.sleep(self.config.health_check_interval)
            except Exception as e:
                logger.error(f"Health check error: {str(e)}")
                await asyncio.sleep(30)

    async def _discovery_loop(self):
        while True:
            try:
                if self.enable_dynamic_discovery and self.discovery_manager:
                    for layer_num in range(12):
                        if layer_num not in self.layer_modules:
                            service_url = f"http://layer-{layer_num}:8080"
                            await self.discovery_manager.discover_layer(layer_num, service_url)
                await asyncio.sleep(self.config.discovery_interval)
            except Exception as e:
                logger.error(f"Discovery loop error: {str(e)}")
                await asyncio.sleep(120)

    async def _event_processing_loop(self):
        while True:
            try:
                event = await self.event_queue.get()
                subscribers = self.event_subscribers.get(event.event_type, [])

                # Event correlation if enabled
                if self.enable_event_correlation and self.event_correlation:
                    correlation_id = await self.event_correlation.correlate_event(event)
                    if correlation_id:
                        event.correlation_id = correlation_id

                for callback in subscribers:
                    try:
                        if asyncio.iscoroutinefunction(callback):
                            await callback(event)
                        else:
                            callback(event)
                    except Exception as e:
                        logger.error(f"Event callback error: {str(e)}")
                self.event_queue.task_done()
            except Exception as e:
                logger.error(f"Event processing error: {str(e)}")
                await asyncio.sleep(1)

    async def _cache_cleanup_loop(self):
        while True:
            try:
                now = datetime.utcnow()
                expired = [key for key, entry in self.cache.items() if now > entry.expires_at]
                for key in expired:
                    del self.cache[key]
                await asyncio.sleep(30)
            except Exception as e:
                logger.error(f"Cache cleanup error: {str(e)}")
                await asyncio.sleep(60)

    async def _transaction_timeout_loop(self):
        while True:
            try:
                now = datetime.utcnow()
                timed_out = []
                for txn_id, txn in self.active_transactions.items():
                    elapsed = (now - txn.started_at).total_seconds()
                    if elapsed > txn.timeout_seconds:
                        timed_out.append(txn_id)
                for txn_id in timed_out:
                    await self.rollback_transaction(txn_id)
                await asyncio.sleep(5)
            except Exception as e:
                logger.error(f"Transaction timeout error: {str(e)}")
                await asyncio.sleep(30)

    # ==========================================================================
    # Layer Communication (Enhanced)
    # ==========================================================================
    async def call_layer(
        self,
        layer_number: int,
        method: str,
        *args,
        timeout: float = 30.0,
        retry: Optional[bool] = None,
        cache_key: Optional[str] = None,
        **kwargs
    ) -> Any:
        if layer_number not in self.layer_modules:
            raise Exception(f"Layer {layer_number} not registered")

        if self.enable_bio_integration:
            permeability = self._get_membrane_permeability(layer_number)
            if permeability == 'impermeable':
                raise Exception(f"Layer {layer_number} membrane is impermeable")

        # Check gradient-aware cache
        if self.enable_gradient_cache and self.gradient_cache and cache_key:
            gradients = self._get_real_gradient_levels()
            cached_value = await self.gradient_cache.get(cache_key, gradients.get('trust', 0.5))
            if cached_value is not None:
                return cached_value
        elif self.enable_cache and cache_key:
            cached = self._get_from_cache(cache_key)
            if cached is not None:
                return cached

        layer_info = self.layers[layer_number]
        module = self.layer_modules[layer_number]

        if self.enable_circuit_breaker:
            if not layer_info.circuit_breaker.can_execute():
                # Check if recovery is possible with gradient health
                if self.enable_bio_integration:
                    gradient_health = self._get_gradient_health(layer_number)
                    if gradient_health > 0.6 and layer_info.circuit_breaker.state == CircuitState.OPEN:
                        layer_info.circuit_breaker.state = CircuitState.RECOVERING
                        layer_info.circuit_breaker.record_recovery_attempt()
                        if layer_info.circuit_breaker.recovery_attempts > 2:
                            layer_info.circuit_breaker.state = CircuitState.CLOSED
                            layer_info.circuit_breaker.failure_count = 0
                            logger.info(f"Circuit breaker reset for layer {layer_number}")
                    else:
                        raise Exception(f"Circuit breaker open for layer {layer_number}")
                else:
                    raise Exception(f"Circuit breaker open for layer {layer_number}")

        should_retry = retry if retry is not None else self.enable_retry
        max_attempts = self.retry_config.max_retries if should_retry else 1

        last_exception = None
        for attempt in range(max_attempts):
            try:
                start_time = time.time()
                result = await asyncio.wait_for(
                    self._execute_layer_method(module, method, *args, **kwargs),
                    timeout=timeout
                )
                execution_time = (time.time() - start_time) * 1000
                self._record_layer_success(layer_number, execution_time)
                if self.enable_circuit_breaker:
                    layer_info.circuit_breaker.record_success()

                # Store in cache
                if cache_key:
                    if self.enable_gradient_cache and self.gradient_cache:
                        gradients = self._get_real_gradient_levels()
                        token_balance = self._get_real_token_availability()
                        await self.gradient_cache.set(cache_key, result, layer_number, gradients.get('trust', 0.5), token_balance)
                    elif self.enable_cache:
                        self._set_cache(cache_key, result, layer_number)

                # Telemetry
                if self.enable_monitoring and self.telemetry:
                    self.telemetry.increment('layer_calls', {'layer': str(layer_number)})
                    self.telemetry.histogram('layer_latency_ms', execution_time, {'layer': str(layer_number)})

                return result
            except asyncio.TimeoutError:
                last_exception = Exception(f"Layer {layer_number} timeout after {timeout}s")
            except Exception as e:
                last_exception = e

            self._record_layer_error(layer_number)
            if self.enable_circuit_breaker:
                layer_info.circuit_breaker.record_failure()

            if attempt < max_attempts - 1:
                base_delay = self.retry_config.get_delay(attempt)
                if self.enable_bio_integration:
                    base_delay = self._get_gradient_modulated_retry_delay(base_delay)
                await asyncio.sleep(base_delay)

        raise last_exception or Exception(f"Layer {layer_number}.{method} failed")

    async def _execute_layer_method(self, module: Any, method: str, *args, **kwargs) -> Any:
        if not hasattr(module, method):
            raise Exception(f"Method {method} not found on layer module")
        method_func = getattr(module, method)
        if asyncio.iscoroutinefunction(method_func):
            return await method_func(*args, **kwargs)
        else:
            loop = asyncio.get_event_loop()
            return await loop.run_in_executor(self.executor, lambda: method_func(*args, **kwargs))

    # ==========================================================================
    # Event System
    # ==========================================================================
    def subscribe_to_event(self, event_type: str, callback: Callable):
        self.event_subscribers[event_type].append(callback)

    def unsubscribe_from_event(self, event_type: str, callback: Callable):
        if event_type in self.event_subscribers:
            self.event_subscribers[event_type].remove(callback)

    async def publish_event(self, event: LayerEvent):
        if not self.enable_events:
            return
        if self.enable_bio_integration and self.gradient_manager:
            gradients = self._get_real_gradient_levels()
            event.gradient_level = gradients.get('trust', 0.5)
            if 'error' in event.event_type.lower():
                event.second_messenger_type = 'calcium'
            elif 'update' in event.event_type.lower():
                event.second_messenger_type = 'cAMP'
            elif 'gradient' in event.event_type.lower():
                event.second_messenger_type = 'IP3'
            else:
                event.second_messenger_type = 'nitric_oxide'

        if self.enable_carbon_intensity:
            event.carbon_impact = await self.carbon_manager.get_current_intensity() / 1000

        # Event correlation
        if self.enable_event_correlation and self.event_correlation:
            await self.event_correlation.correlate_event(event)

        try:
            self.event_queue.put_nowait(event)
        except asyncio.QueueFull:
            logger.warning("Event queue full, dropping event")

    # ==========================================================================
    # Cache Management (Enhanced)
    # ==========================================================================
    def _get_from_cache(self, key: str) -> Optional[Any]:
        if key not in self.cache:
            return None
        entry = self.cache[key]
        if datetime.utcnow() > entry.expires_at:
            del self.cache[key]
            return None
        entry.access_count += 1
        entry.last_accessed = datetime.utcnow()
        return entry.value

    def _set_cache(self, key: str, value: Any, layer_number: int):
        if len(self.cache) >= self.max_cache_size:
            self._evict_cache_entry()
        ttl = self._get_token_backed_cache_ttl() if self.enable_bio_integration else self.cache_ttl
        gradient_level = 0.5
        if self.enable_bio_integration and self.gradient_manager:
            gradients = self._get_real_gradient_levels()
            gradient_level = gradients.get('trust', 0.5)
        entry = CacheEntry(
            key=key, value=value, created_at=datetime.utcnow(),
            expires_at=datetime.utcnow() + timedelta(seconds=ttl),
            layer_number=layer_number,
            token_backed=self.enable_bio_integration and self.token_manager is not None,
            gradient_level_at_creation=gradient_level
        )
        self.cache[key] = entry

    def _invalidate_layer_cache(self, layer_number: int):
        keys_to_remove = [key for key, entry in self.cache.items() if entry.layer_number == layer_number]
        for key in keys_to_remove:
            del self.cache[key]

    def _evict_cache_entry(self):
        if not self.cache:
            return
        lru_key = min(self.cache.keys(), key=lambda k: self.cache[k].last_accessed)
        del self.cache[lru_key]

    # ==========================================================================
    # Transaction Support (Enhanced)
    # ==========================================================================
    async def begin_transaction(
        self,
        layers_involved: List[int],
        timeout_seconds: float = 60.0,
        distributed: bool = False,
        participants: List[str] = None
    ) -> TransactionContext:
        if distributed and self.enable_distributed_txns and self.distributed_coordinator:
            return await self.distributed_coordinator.begin_distributed_transaction(
                layers_involved,
                participants or [],
                timeout_seconds
            )

        transaction = TransactionContext(
            transaction_id=f"txn_{datetime.utcnow().timestamp()}_{np.random.randint(10000)}",
            started_at=datetime.utcnow(),
            layers_involved=layers_involved,
            timeout_seconds=timeout_seconds
        )
        if self.enable_bio_integration and self.token_manager:
            ecoatp_cost = len(layers_involved) * self.config.token_reserve_factor
            success, _ = self.token_manager.reserve_tokens(
                account_id=f"txn_{transaction.transaction_id}",
                amount=ecoatp_cost,
                consumer=EcoATPConsumer.EXPERT_EXECUTION
            )
            if success:
                transaction.tokens_allocated = ecoatp_cost
        self.active_transactions[transaction.transaction_id] = transaction
        return transaction

    async def rollback_transaction(self, transaction_id: str):
        if transaction_id in self.active_transactions:
            transaction = self.active_transactions[transaction_id]
            if self.enable_bio_integration and transaction.tokens_allocated > 0:
                recovered = self._recover_tokens_on_rollback(transaction_id, transaction.tokens_allocated)
                transaction.tokens_recovered = recovered
            await self._compensate_transaction(transaction_id)
            transaction.status = "rolled_back"
            del self.active_transactions[transaction_id]
        elif self.enable_distributed_txns and self.distributed_coordinator:
            await self.distributed_coordinator.rollback_distributed_transaction(transaction_id)

    async def commit_transaction(self, transaction_id: str) -> bool:
        if transaction_id in self.active_transactions:
            transaction = self.active_transactions[transaction_id]
            transaction.status = "committed"
            del self.active_transactions[transaction_id]
            return True
        elif self.enable_distributed_txns and self.distributed_coordinator:
            return await self.distributed_coordinator.commit_distributed_transaction(transaction_id)
        return False

    async def _compensate_transaction(self, transaction_id: str):
        if transaction_id not in self.active_transactions:
            return
        transaction = self.active_transactions[transaction_id]
        for compensation in reversed(transaction.compensation_actions):
            try:
                await self.call_layer(compensation['layer'], compensation['method'], *compensation['args'])
            except Exception as e:
                logger.error(f"Compensation failed: {str(e)}")

    # ==========================================================================
    # Layer Registration (Enhanced)
    # ==========================================================================
    def register_layer_module(
        self,
        layer_number: int,
        module: Any,
        version: Optional[str] = None,
        endpoints: Optional[Dict[str, str]] = None
    ) -> bool:
        if layer_number not in self.layers:
            logger.error(f"Invalid layer number: {layer_number}")
            return False
        layer_info = self.layers[layer_number]
        self.layer_modules[layer_number] = module
        if endpoints:
            layer_info.endpoints.update(endpoints)
        self.integration_status[layer_number] = True
        layer_info.status = LayerStatus.HEALTHY
        layer_info.last_heartbeat = datetime.utcnow()
        if self.enable_bio_integration and self.token_manager:
            self.token_manager.create_account(f"layer_{layer_number}")
        self._subscribe_layer_to_events(layer_number)

        # Dynamic discovery
        if self.enable_dynamic_discovery and self.discovery_manager:
            service_url = endpoints.get('primary', f"http://layer-{layer_number}:8080") if endpoints else f"http://layer-{layer_number}:8080"
            asyncio.create_task(self.discovery_manager.discover_layer(layer_number, service_url))

        logger.info(f"Layer {layer_number} ({layer_info.layer_name}) registered")
        return True

    def _subscribe_layer_to_events(self, layer_number: int):
        layer_info = self.layers[layer_number]
        for dep_num in layer_info.dependencies:
            event_type = f"layer_{dep_num}_update"
            self.subscribe_to_event(event_type, lambda event, ln=layer_number:
                asyncio.create_task(self._handle_dependency_update(ln, event)))

    async def _handle_dependency_update(self, layer_number: int, event: LayerEvent):
        self._invalidate_layer_cache(layer_number)
        if self.enable_gradient_cache and self.gradient_cache:
            gradients = self._get_real_gradient_levels()
            await self.gradient_cache.invalidate_by_gradient(gradients.get('trust', 0.5))
        if layer_number in self.layer_modules:
            module = self.layer_modules[layer_number]
            if hasattr(module, 'on_dependency_update'):
                await module.on_dependency_update(event)

    # ==========================================================================
    # Metrics and Recording
    # ==========================================================================
    def _record_layer_success(self, layer_number: int, execution_time_ms: float):
        self.layer_latency[layer_number].append(execution_time_ms)
        self.layer_calls[layer_number] += 1
        if len(self.layer_latency[layer_number]) > 1000:
            self.layer_latency[layer_number] = self.layer_latency[layer_number][-1000:]

    def _record_layer_error(self, layer_number: int):
        self.layer_errors[layer_number] += 1
        self.layer_calls[layer_number] += 1

    # ==========================================================================
    # Status Methods (Enhanced)
    # ==========================================================================
    def get_integration_status(self) -> Dict[str, Any]:
        status = {
            'total_layers': 12,
            'integrated_layers': sum(self.integration_status.values()),
            'bio_integration_active': self.enable_bio_integration,
            'carbon_intensity_active': self.enable_carbon_intensity,
            'predictive_active': self.enable_predictive,
            'cross_domain_active': self.enable_cross_domain,
            'sustainability_scoring_active': self.enable_sustainability_scoring,
            'dynamic_discovery_active': self.enable_dynamic_discovery,
            'event_correlation_active': self.enable_event_correlation,
            'gradient_cache_active': self.enable_gradient_cache,
            'distributed_txns_active': self.enable_distributed_txns,
            'moe_router_injected': self.expert_router is not None,
            'helium_provider_injected': self.helium_provider is not None,
            'fl_monitor_injected': self.fl_monitor is not None,
            'version': '6.1.1',
            'config': self.config.__dict__,
            'layer_details': {}
        }

        for num, info in self.layers.items():
            status['layer_details'][num] = {
                'name': info.layer_name,
                'version': info.version,
                'status': info.status.value,
                'integrated': self.integration_status.get(num, False),
                'circuit_breaker': info.circuit_breaker.state.value,
                'dependencies': info.dependencies,
                'capabilities': info.capabilities,
                'gradient_health': info.gradient_health,
                'membrane_permeability': info.membrane_permeability,
                'token_balance': info.token_balance,
                'harvester_vitality': info.harvester_vitality,
                'entangled_layers': info.entangled_layers,
                'sustainability_score': info.sustainability_score,
                'recovery_attempts': info.recovery_attempts
            }

        # Cache stats
        if self.enable_gradient_cache and self.gradient_cache:
            status['cache_stats'] = self.gradient_cache.get_stats()
        else:
            status['cache_stats'] = {
                'entries': len(self.cache),
                'max_size': self.max_cache_size,
                'ttl_seconds': self._get_token_backed_cache_ttl() if self.enable_bio_integration else self.cache_ttl,
                'token_backed': self.enable_bio_integration and self.token_manager is not None
            }

        status['event_stats'] = {
            'queue_size': self.event_queue.qsize(),
            'subscribers': sum(len(v) for v in self.event_subscribers.values()),
            'correlation_enabled': self.enable_event_correlation
        }

        if self.enable_event_correlation and self.event_correlation:
            status['correlation_stats'] = self.event_correlation.get_correlation_stats()

        status['transaction_stats'] = {
            'active': len(self.active_transactions),
            'distributed_enabled': self.enable_distributed_txns
        }

        status['performance'] = {
            str(num): {
                'calls': self.layer_calls.get(num, 0),
                'errors': self.layer_errors.get(num, 0),
                'error_rate': self.layer_errors[num] / max(self.layer_calls[num], 1),
                'avg_latency_ms': np.mean(self.layer_latency[num]) if self.layer_latency.get(num) else 0
            }
            for num in range(12)
        }

        if self.enable_dynamic_discovery and self.discovery_manager:
            status['discovery_stats'] = {
                'discovered_layers': len(self.discovery_manager.discovered_layers),
                'active_layers': sum(1 for l in self.discovery_manager.discovered_layers.values() if l.get('status') == 'active')
            }

        if self.enable_bio_integration:
            status['gradient_levels'] = self._get_real_gradient_levels()
            status['harvester_vitality'] = self._get_harvester_vitality()

        if self.enable_predictive and self.predictive_analyzer:
            status['predictive_forecast'] = asyncio.run(self.predictive_analyzer.predict_layer_health())

        if self.enable_cross_domain and self.cross_domain_transfer:
            status['cross_domain_stats'] = self.cross_domain_transfer.get_transfer_statistics()

        if self.enable_sustainability_scoring and self.sustainability_calculator:
            status['sustainability'] = {
                'score': self.sustainability_score,
                'carbon_savings_kg': self.total_carbon_savings_kg,
                'helium_saved_l': self.total_helium_saved_l,
                'energy_saved_kwh': self.total_energy_saved_kwh,
                'breakdown': self.sustainability_calculator.get_weighted_breakdown({
                    'carbon_savings': min(1.0, self.total_carbon_savings_kg / 1000),
                    'helium_efficiency': min(1.0, self.total_helium_saved_l / 100),
                    'renewable_usage': 0.0,  # placeholder
                    'token_efficiency': self._get_real_token_availability(),
                    'layer_health': np.mean([info.gradient_health for info in self.layers.values()])
                })
            }

        return status

    def get_layer_health(self) -> Dict[int, Dict[str, Any]]:
        health = {}
        for layer_num in range(12):
            health[layer_num] = {
                'gradient_health': self._get_gradient_health(layer_num),
                'membrane_permeability': self._get_membrane_permeability(layer_num),
                'harvester_vitality': self._get_harvester_vitality(),
                'entangled_resources': self._get_entangled_resources(layer_num)
            }
        return health

    def get_bio_cache_config(self) -> Dict[str, Any]:
        return {
            'ttl_seconds': self._get_token_backed_cache_ttl() if self.enable_bio_integration else self.cache_ttl,
            'gradient_modulated': self.gradient_manager is not None,
            'token_backed': self.token_manager is not None,
            'bio_integration_active': self.enable_bio_integration,
            'gradient_cache_active': self.enable_gradient_cache
        }

    def get_sustainability_report(self) -> Dict[str, Any]:
        return {
            'timestamp': datetime.utcnow().isoformat(),
            'sustainability_score': self.sustainability_score,
            'total_carbon_savings_kg': self.total_carbon_savings_kg,
            'total_helium_saved_l': self.total_helium_saved_l,
            'total_energy_saved_kwh': self.total_energy_saved_kwh,
            'bio_integration_active': self.enable_bio_integration,
            'predictive_forecast': asyncio.run(self.predictive_analyzer.predict_layer_health()) if self.enable_predictive else {},
            'recommendations': self._generate_sustainability_recommendations()
        }

    def _generate_sustainability_recommendations(self) -> List[str]:
        recommendations = []
        if self.sustainability_score < 0.5:
            recommendations.append("Increase token allocation for critical layers")
            recommendations.append("Optimize carbon-aware layer scheduling")
        if self.total_carbon_savings_kg < 10:
            recommendations.append("Implement more aggressive carbon reduction strategies")
        if self.enable_bio_integration and np.mean([info.gradient_health for info in self.layers.values()]) < 0.5:
            recommendations.append("Improve gradient health through better trust management")
        if self.enable_dynamic_discovery and self.discovery_manager:
            discovered = len(self.discovery_manager.discovered_layers)
            if discovered < 6:
                recommendations.append("Increase layer discovery to ensure all layers are available")
        return recommendations or ["Layer integration sustainability is on track"]

    def clear_cache(self):
        self.cache.clear()
        if self.enable_gradient_cache and self.gradient_cache:
            self.gradient_cache.cache.clear()

    def reset_circuit_breaker(self, layer_number: int):
        if layer_number in self.layers:
            self.layers[layer_number].circuit_breaker = LayerCircuitBreaker(f"layer_{layer_number}")

    async def shutdown(self):
        logger.info("Shutting down Enhanced Layer Integrator")
        await self.carbon_manager.close() if self.carbon_manager else None
        logger.info("Shutdown complete")


# ============================================================================
# Legacy Compatibility Class
# ============================================================================
class LayerIntegrator(EnhancedLayerIntegrator):
    """
    Legacy LayerIntegrator for backward compatibility.
    """

    def __init__(self, expert_router=None):
        config = LayerIntegratorConfig()
        super().__init__(config)
        self.router = expert_router
        self.layer_integration_status = {f'layer_{i}': False for i in range(12)}
        logger.info("Layer Integrator initialized (compatibility mode)")

    def get_integration_status(self) -> Dict[str, bool]:
        return self.layer_integration_status.copy()
