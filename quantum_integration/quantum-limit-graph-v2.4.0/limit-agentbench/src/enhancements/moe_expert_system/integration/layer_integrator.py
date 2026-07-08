# File: quantum_integration/quantum-limit-graph-v2.4.0/limit-agentbench/src/enhancements/moe_expert_system/integration/layer_integrator.py
"""
Enhanced Layer Integrator v6.1.0 - Complete Green Agent Implementation

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
- Context builder for MoE expert system (NEW)
- Helium and Federated Learning telemetry integration (NEW)
"""

import asyncio
import logging
from typing import Dict, Any, List, Optional, Tuple, Set, Callable
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
# NEW: Import MoE Expert Router
# ============================================================================
try:
    from ..expert_router import ExpertRouter
    MOE_AVAILABLE = True
except ImportError:
    MOE_AVAILABLE = False
    logger.warning("MoE Expert Router not available - context building will be limited")

# ============================================================================
# Carbon Intensity Integration Module (unchanged)
# ============================================================================

class CarbonIntensityManager:
    """Real-time carbon intensity integration with API support and dynamic pricing"""
    
    def __init__(self, endpoint: str = "https://api.electricitymap.org/v3/carbon-intensity"):
        self.endpoint = endpoint
        self.carbon_intensity = 0.0
        self.region = "us-east"
        self.last_update = None
        self._lock = asyncio.Lock()
        self._session = None
        self.update_interval = 300
        self.cache = {}
        self.historical_intensities = deque(maxlen=1000)
        self.api_key = os.getenv('ELECTRICITYMAP_API_KEY', '')
        # NEW: Dynamic pricing
        self.carbon_price_usd_per_ton = 50.0
        self.price_history = deque(maxlen=1000)
    
    async def _get_session(self):
        if self._session is None:
            self._session = aiohttp.ClientSession()
        return self._session
    
    async def update_carbon_intensity(self, region: str = "us-east") -> Dict:
        async with self._lock:
            session = await self._get_session()
            try:
                url = f"{self.endpoint}/latest?zone={region}"
                headers = {'auth-token': self.api_key} if self.api_key else {}
                async with session.get(url, headers=headers, timeout=10) as response:
                    if response.status == 200:
                        data = await response.json()
                        self.carbon_intensity = data.get('carbonIntensity', 400)
                        self.region = region
                        self.last_update = datetime.now()
                        self.cache[region] = {'intensity': self.carbon_intensity, 'timestamp': self.last_update}
                        self.historical_intensities.append(self.carbon_intensity)
                        self._update_carbon_price(self.carbon_intensity)
                    else:
                        self.carbon_intensity = self._get_fallback_intensity(region)
                        self.last_update = datetime.now()
                        self._update_carbon_price(self.carbon_intensity)
            except Exception as e:
                logger.error(f"Carbon intensity fetch error: {e}")
                self.carbon_intensity = self._get_fallback_intensity(region)
                self.last_update = datetime.now()
                self._update_carbon_price(self.carbon_intensity)
            return {'intensity': self.carbon_intensity, 'region': self.region,
                    'timestamp': self.last_update.isoformat() if self.last_update else None,
                    'price_usd_per_ton': self.carbon_price_usd_per_ton}
    
    def _update_carbon_price(self, intensity: float):
        base_price = 50.0
        intensity_factor = (intensity - 300) / 500
        self.carbon_price_usd_per_ton = max(10.0, base_price * (1.0 + intensity_factor))
        self.price_history.append({
            'timestamp': self.last_update.isoformat() if self.last_update else None,
            'price': self.carbon_price_usd_per_ton
        })
    
    def _get_fallback_intensity(self, region: str) -> float:
        fallback_values = {'us-east': 420, 'us-west': 350, 'eu': 280, 'asia': 500, 'default': 400}
        return fallback_values.get(region, 400)
    
    async def get_current_intensity(self) -> float:
        if self.last_update is None or (datetime.now() - self.last_update).seconds > self.update_interval:
            await self.update_carbon_intensity(self.region)
        return self.carbon_intensity
    
    async def get_current_price(self) -> float:
        if self.last_update is None or (datetime.now() - self.last_update).seconds > self.update_interval:
            await self.update_carbon_intensity(self.region)
        return self.carbon_price_usd_per_ton
    
    async def close(self):
        if self._session:
            await self._session.close()

# ============================================================================
# Predictive Reflexivity Module (Enhanced)
# ============================================================================

class PredictiveLayerAnalyzer:
    """Predictive reflexivity with ensemble forecasting for layer health"""
    
    def __init__(self, history_window: int = 100):
        self.history_window = history_window
        self.layer_history = deque(maxlen=history_window)
        self.forecast_history = deque(maxlen=50)
        self.models = {}
        self.scaler = None
        self.is_trained = False
        
        try:
            from sklearn.preprocessing import StandardScaler
            from sklearn.ensemble import RandomForestRegressor, GradientBoostingRegressor
            from sklearn.linear_model import LinearRegression
            from sklearn.metrics import r2_score
            self.scaler = StandardScaler()
            self.models['random_forest'] = RandomForestRegressor(n_estimators=100, random_state=42)
            self.models['gradient_boosting'] = GradientBoostingRegressor(n_estimators=100, random_state=42)
            self.models['linear'] = LinearRegression()
            self._ml_available = True
        except ImportError:
            self._ml_available = False
    
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
        if not self._ml_available or len(self.layer_history) < 10:
            return {'status': 'insufficient_data'}
        
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
        
        X = np.array(X); y = np.array(y)
        X_scaled = self.scaler.fit_transform(X)
        results = {}
        for name, model in self.models.items():
            if model is not None:
                model.fit(X_scaled, y)
                predictions = model.predict(X_scaled)
                from sklearn.metrics import r2_score
                results[name] = r2_score(y, predictions)
        self.is_trained = True
        return {'status': 'success', 'results': results}
    
    async def predict_layer_health(self) -> Dict:
        if not self.is_trained or len(self.layer_history) < 10:
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
        features_scaled = self.scaler.transform(features)
        predictions = []
        for name, model in self.models.items():
            if model is not None:
                predictions.append(model.predict(features_scaled)[0])
        if not predictions:
            return {'predicted_health': 0.5, 'confidence': 0.0, 'trend': 'no_models'}
        prediction = np.mean(predictions)
        confidence = min(0.9, np.std(predictions) / 0.2) if len(predictions) > 1 else 0.5
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
# Cross-Domain Knowledge Transfer Module (unchanged)
# ============================================================================

class LayerCrossDomainTransfer:
    """Cross-domain knowledge transfer for layer integration"""
    
    def __init__(self):
        self.knowledge_base: Dict[str, Dict[str, Dict]] = {}
        self.transfer_logs = deque(maxlen=1000)
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
            self.knowledge_base[key][knowledge_type] = {'data': data, 'transfer_count': 1,
                'effectiveness_score': 0.5, 'last_used': datetime.utcnow()}
        else:
            existing = self.knowledge_base[key][knowledge_type]
            existing['data'].update(data); existing['transfer_count'] += 1
            existing['last_used'] = datetime.utcnow()
        self.transfer_logs.append({'timestamp': datetime.utcnow(), 'source': source_domain,
                                   'target': target_domain, 'type': knowledge_type})
        return self.knowledge_base[key][knowledge_type]
    
    def get_transfer_statistics(self) -> Dict:
        total_transfers = len(self.transfer_logs)
        domain_pairs = {}
        for log in self.transfer_logs:
            key = f"{log['source']}→{log['target']}"
            domain_pairs[key] = domain_pairs.get(key, 0) + 1
        return {'total_transfers': total_transfers, 'domain_pairs': domain_pairs,
                'knowledge_types': list(self.knowledge_base.keys())}

# ============================================================================
# Layer Status and Integration Enums (Enhanced)
# ============================================================================

class LayerStatus(Enum):
    HEALTHY = "healthy"; DEGRADED = "degraded"; UNHEALTHY = "unhealthy"
    RECOVERING = "recovering"; OFFLINE = "offline"; MAINTENANCE = "maintenance"
    DISCOVERED = "discovered"  # NEW
    
    def to_membrane_state(self) -> 'MembranePermeability':
        if not BIO_INSPIRED_AVAILABLE: return None
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
    SYNCHRONOUS = "synchronous"; ASYNCHRONOUS = "asynchronous"
    EVENT_DRIVEN = "event_driven"; BATCH = "batch"; STREAMING = "streaming"

class CircuitState(Enum):
    CLOSED = "closed"; OPEN = "open"; HALF_OPEN = "half_open"
    RECOVERING = "recovering"  # NEW

# ============================================================================
# Data Classes (Enhanced)
# ============================================================================

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
    # NEW: Discovery and health tracking
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
    # NEW: Gradient-based recovery
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
    # NEW: Event correlation
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
    # NEW: Gradient-aware invalidation
    gradient_threshold: float = 0.3
    invalidated_by_gradient: bool = False

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
    # NEW: Distributed transaction support
    coordinator_id: Optional[str] = None
    participants: List[str] = field(default_factory=list)
    distributed_status: Dict[str, str] = field(default_factory=dict)

# ============================================================================
# Dynamic Layer Discovery Manager (NEW)
# ============================================================================

class DynamicLayerDiscoveryManager:
    """
    Dynamic layer discovery for runtime registration.
    
    Features:
    - Runtime layer registration
    - Service discovery
    - Capability exchange
    - Health-based discovery
    """
    
    def __init__(self):
        self.discovered_layers: Dict[int, Dict[str, Any]] = {}
        self.discovery_registry: Dict[str, Dict] = {}
        self._lock = asyncio.Lock()
        self.discovery_interval = 60
        self.health_interval = 30
        self.max_discovery_attempts = 3
        
        logger.info("Dynamic Layer Discovery Manager initialized")
    
    async def discover_layer(self, layer_number: int, service_url: str) -> bool:
        """Discover a layer at runtime"""
        async with self._lock:
            try:
                # Simulate service discovery
                capabilities = self._get_layer_capabilities(layer_number)
                health = await self._check_layer_health(service_url)
                
                self.discovered_layers[layer_number] = {
                    'url': service_url,
                    'capabilities': capabilities,
                    'health': health,
                    'discovered_at': datetime.utcnow().isoformat(),
                    'status': 'active' if health else 'degraded'
                }
                
                logger.info(f"Discovered layer {layer_number} at {service_url}")
                return True
                
            except Exception as e:
                logger.error(f"Layer discovery error for {layer_number}: {e}")
                return False
    
    async def _check_layer_health(self, service_url: str) -> bool:
        """Check health of a discovered layer"""
        try:
            # Simulate health check
            return np.random.random() > 0.1
        except Exception:
            return False
    
    def _get_layer_capabilities(self, layer_number: int) -> List[str]:
        """Get capabilities for a layer"""
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
        """Get all discovered layers"""
        return self.discovered_layers.copy()
    
    def get_layer_status(self, layer_number: int) -> Optional[Dict]:
        """Get status of a specific discovered layer"""
        return self.discovered_layers.get(layer_number)

# ============================================================================
# Event Correlation Engine (NEW)
# ============================================================================

class EventCorrelationEngine:
    """
    Event correlation for complex workflow orchestration.
    
    Features:
    - Parent-child event tracking
    - Workflow phase tracking
    - Event causality detection
    - Pattern recognition
    """
    
    def __init__(self):
        self.event_graph: Dict[str, List[str]] = defaultdict(list)  # parent -> children
        self.event_metadata: Dict[str, Dict] = {}
        self.correlation_patterns: Dict[str, List[str]] = defaultdict(list)
        self._lock = asyncio.Lock()
        
        logger.info("Event Correlation Engine initialized")
    
    async def correlate_event(self, event: LayerEvent) -> Optional[str]:
        """Correlate event with existing events"""
        async with self._lock:
            # Check for correlation patterns
            pattern = self._detect_pattern(event)
            if pattern:
                correlation_id = f"corr_{datetime.utcnow().timestamp()}_{pattern}"
                self.correlation_patterns[correlation_id].append(event.event_id)
                event.correlation_id = correlation_id
                return correlation_id
            
            # Check if event is child of existing event
            for parent_id, children in self.event_graph.items():
                if event.event_type.startswith(self.event_metadata.get(parent_id, {}).get('pattern', '')):
                    children.append(event.event_id)
                    event.parent_event_id = parent_id
                    return parent_id
            
            return None
    
    def _detect_pattern(self, event: LayerEvent) -> Optional[str]:
        """Detect event pattern"""
        patterns = {
            'workflow_start': ['initialize', 'start', 'begin'],
            'workflow_end': ['complete', 'finish', 'end'],
            'workflow_error': ['error', 'fail', 'exception'],
            'workflow_retry': ['retry', 'recover', 'resume']
        }
        
        for pattern, keywords in patterns.items():
            if any(kw in event.event_type.lower() for kw in keywords):
                return pattern
        
        return None
    
    def get_event_chain(self, event_id: str) -> List[str]:
        """Get complete event chain"""
        chain = [event_id]
        
        # Get children
        children = self.event_graph.get(event_id, [])
        for child in children:
            chain.extend(self.get_event_chain(child))
        
        return chain
    
    def get_correlation_stats(self) -> Dict[str, Any]:
        """Get event correlation statistics"""
        return {
            'total_events': sum(len(children) for children in self.event_graph.values()),
            'correlation_patterns': len(self.correlation_patterns),
            'event_graph_edges': sum(len(children) for children in self.event_graph.values()),
            'total_metadata': len(self.event_metadata)
        }

# ============================================================================
# Gradient-Aware Cache Manager (NEW)
# ============================================================================

class GradientAwareCacheManager:
    """
    Gradient-aware cache invalidation.
    
    Features:
    - Gradient-based TTL adjustment
    - Adaptive cache policies
    - Health-based invalidation
    - Token-backed retention
    """
    
    def __init__(self, base_ttl: float = 60.0):
        self.cache: Dict[str, CacheEntry] = {}
        self.base_ttl = base_ttl
        self._lock = asyncio.Lock()
        self.gradient_threshold = 0.3
        self.max_cache_size = 1000
        
        logger.info("Gradient-Aware Cache Manager initialized")
    
    async def get(self, key: str, gradient_level: float = 0.5) -> Optional[Any]:
        """Get from cache with gradient awareness"""
        async with self._lock:
            if key not in self.cache:
                return None
            
            entry = self.cache[key]
            
            # Check gradient-based invalidation
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
    
    async def set(self, key: str, value: Any, layer_number: int, gradient_level: float = 0.5):
        """Set cache entry with gradient awareness"""
        async with self._lock:
            if len(self.cache) >= self.max_cache_size:
                await self._evict_lru()
            
            # Adjust TTL based on gradient
            ttl = self.base_ttl * (1.0 + gradient_level * 0.5)
            
            entry = CacheEntry(
                key=key,
                value=value,
                created_at=datetime.utcnow(),
                expires_at=datetime.utcnow() + timedelta(seconds=ttl),
                layer_number=layer_number,
                gradient_level_at_creation=gradient_level,
                gradient_threshold=self.gradient_threshold
            )
            self.cache[key] = entry
    
    async def invalidate_by_gradient(self, gradient_level: float):
        """Invalidate cache entries based on gradient change"""
        async with self._lock:
            to_remove = []
            for key, entry in self.cache.items():
                if abs(gradient_level - entry.gradient_level_at_creation) > entry.gradient_threshold:
                    entry.invalidated_by_gradient = True
                    to_remove.append(key)
            
            for key in to_remove:
                del self.cache[key]
            
            if to_remove:
                logger.info(f"Invalidated {len(to_remove)} cache entries due to gradient change")
    
    async def _evict_lru(self):
        """Evict least recently used cache entry"""
        if not self.cache:
            return
        
        lru_key = min(self.cache.keys(), key=lambda k: self.cache[k].last_accessed)
        del self.cache[lru_key]
    
    def get_stats(self) -> Dict[str, Any]:
        """Get cache statistics"""
        return {
            'size': len(self.cache),
            'max_size': self.max_cache_size,
            'base_ttl': self.base_ttl,
            'gradient_threshold': self.gradient_threshold,
            'entries': [
                {
                    'key': entry.key,
                    'layer_number': entry.layer_number,
                    'access_count': entry.access_count,
                    'expires_at': entry.expires_at.isoformat(),
                    'invalidated_by_gradient': entry.invalidated_by_gradient
                }
                for entry in self.cache.values()
            ][-10:]  # Last 10 entries for display
        }

# ============================================================================
# Distributed Transaction Coordinator (NEW)
# ============================================================================

class DistributedTransactionCoordinator:
    """
    Distributed transaction support across multiple integrators.
    
    Features:
    - Two-phase commit simulation
    - Participant coordination
    - Transaction recovery
    - Distributed rollback
    """
    
    def __init__(self, coordinator_id: str):
        self.coordinator_id = coordinator_id
        self.active_transactions: Dict[str, TransactionContext] = {}
        self._lock = asyncio.Lock()
        self.participant_timeout = 30.0
        
        logger.info(f"Distributed Transaction Coordinator initialized: {coordinator_id}")
    
    async def begin_distributed_transaction(
        self,
        layers_involved: List[int],
        participants: List[str],
        timeout_seconds: float = 60.0
    ) -> TransactionContext:
        """Begin a distributed transaction"""
        async with self._lock:
            transaction = TransactionContext(
                transaction_id=f"dist_txn_{datetime.utcnow().timestamp()}_{uuid.uuid4().hex[:8]}",
                started_at=datetime.utcnow(),
                layers_involved=layers_involved,
                timeout_seconds=timeout_seconds,
                coordinator_id=self.coordinator_id,
                participants=participants,
                distributed_status={p: 'pending' for p in participants}
            )
            
            self.active_transactions[transaction.transaction_id] = transaction
            logger.info(f"Started distributed transaction: {transaction.transaction_id}")
            return transaction
    
    async def prepare_participant(self, transaction_id: str, participant: str) -> bool:
        """Prepare a participant for commit"""
        async with self._lock:
            if transaction_id not in self.active_transactions:
                return False
            
            txn = self.active_transactions[transaction_id]
            if participant not in txn.participants:
                return False
            
            # Simulate prepare phase
            prepared = np.random.random() > 0.1
            txn.distributed_status[participant] = 'prepared' if prepared else 'failed'
            
            if prepared:
                logger.info(f"Participant {participant} prepared for {transaction_id}")
            else:
                logger.warning(f"Participant {participant} failed to prepare for {transaction_id}")
            
            return prepared
    
    async def commit_distributed_transaction(self, transaction_id: str) -> bool:
        """Commit a distributed transaction"""
        async with self._lock:
            if transaction_id not in self.active_transactions:
                return False
            
            txn = self.active_transactions[transaction_id]
            
            # Check all participants are prepared
            all_prepared = all(
                status == 'prepared' 
                for status in txn.distributed_status.values()
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
        """Rollback a distributed transaction"""
        async with self._lock:
            if transaction_id not in self.active_transactions:
                return False
            
            txn = self.active_transactions[transaction_id]
            
            # Rollback all participants
            for participant in txn.participants:
                txn.distributed_status[participant] = 'rolled_back'
            
            txn.status = 'rolled_back'
            del self.active_transactions[transaction_id]
            
            logger.info(f"Distributed transaction rolled back: {transaction_id}")
            return True
    
    def get_transaction_status(self, transaction_id: str) -> Optional[Dict]:
        """Get status of a distributed transaction"""
        if transaction_id in self.active_transactions:
            txn = self.active_transactions[transaction_id]
            return {
                'transaction_id': txn.transaction_id,
                'status': txn.status,
                'participants': txn.distributed_status,
                'layers_involved': txn.layers_involved,
                'started_at': txn.started_at.isoformat()
            }
        return None

# ============================================================================
# Enhanced Layer Integrator (with new context builder and MoE integration)
# ============================================================================

class EnhancedLayerIntegrator:
    """
    Enhanced Layer Integrator v6.1.0 - Complete Green Agent Implementation
    
    New Features:
    - Dynamic layer discovery for runtime registration
    - Health-based circuit reset using gradient fields
    - Event correlation for complex workflow orchestration
    - Gradient-aware cache invalidation
    - Distributed transaction support across integrators
    - Context builder for MoE expert system (NEW)
    - Helium and Federated Learning telemetry integration (NEW)
    """
    
    def __init__(
        self,
        enable_cache: bool = True,
        enable_circuit_breaker: bool = True,
        enable_retry: bool = True,
        enable_events: bool = True,
        enable_transactions: bool = True,
        enable_monitoring: bool = True,
        enable_bio_integration: bool = True,
        enable_carbon_intensity: bool = True,
        enable_predictive: bool = True,
        enable_cross_domain: bool = True,
        enable_sustainability_scoring: bool = True,
        enable_dynamic_discovery: bool = True,  # NEW
        enable_event_correlation: bool = True,  # NEW
        enable_gradient_cache: bool = True,  # NEW
        enable_distributed_txns: bool = True,  # NEW
        cache_ttl_seconds: float = 60.0,
        max_cache_size: int = 1000,
        coordinator_id: str = "main_coordinator"
    ):
        # Feature flags
        self.enable_cache = enable_cache
        self.enable_circuit_breaker = enable_circuit_breaker
        self.enable_retry = enable_retry
        self.enable_events = enable_events
        self.enable_transactions = enable_transactions
        self.enable_monitoring = enable_monitoring
        self.enable_bio_integration = enable_bio_integration and BIO_INSPIRED_AVAILABLE
        self.enable_carbon_intensity = enable_carbon_intensity
        self.enable_predictive = enable_predictive
        self.enable_cross_domain = enable_cross_domain
        self.enable_sustainability_scoring = enable_sustainability_scoring
        
        # NEW feature flags
        self.enable_dynamic_discovery = enable_dynamic_discovery
        self.enable_event_correlation = enable_event_correlation
        self.enable_gradient_cache = enable_gradient_cache
        self.enable_distributed_txns = enable_distributed_txns
        
        # Bio-inspired modules
        self.token_manager = None
        self.gradient_manager = None
        self.scheduler = None
        self.compartment_manager = None
        self.biomass_storage = None
        self.harvester = None
        
        # Existing modules
        self.carbon_manager = CarbonIntensityManager()
        self.predictive_analyzer = PredictiveLayerAnalyzer()
        self.cross_domain_transfer = LayerCrossDomainTransfer()
        
        # NEW modules
        self.discovery_manager = DynamicLayerDiscoveryManager() if enable_dynamic_discovery else None
        self.event_correlation = EventCorrelationEngine() if enable_event_correlation else None
        self.gradient_cache = GradientAwareCacheManager(cache_ttl_seconds) if enable_gradient_cache else None
        self.distributed_coordinator = DistributedTransactionCoordinator(coordinator_id) if enable_distributed_txns else None
        
        # NEW: MoE Expert Router reference (injected)
        self.expert_router = None
        self.helium_provider = None   # To be injected
        self.fl_monitor = None        # To be injected (for FL metrics)
        
        # Layer registry
        self.layers: Dict[int, LayerInfo] = {}
        self.layer_modules: Dict[int, Any] = {}
        
        # Cache
        self.cache: Dict[str, CacheEntry] = {}
        self.cache_ttl = cache_ttl_seconds
        self.max_cache_size = max_cache_size
        
        # Event system
        self.event_subscribers: Dict[str, List[Callable]] = defaultdict(list)
        self.event_queue: asyncio.Queue = asyncio.Queue(maxsize=10000)
        
        # Retry config
        self.retry_config = RetryConfig()
        
        # Transactions
        self.active_transactions: Dict[str, TransactionContext] = {}
        
        # Integration status
        self.integration_status: Dict[int, bool] = {i: False for i in range(12)}
        
        # Performance metrics
        self.layer_latency: Dict[int, List[float]] = defaultdict(list)
        self.layer_errors: Dict[int, int] = defaultdict(int)
        self.layer_calls: Dict[int, int] = defaultdict(int)
        
        # Sustainability tracking
        self.total_carbon_savings_kg = 0.0
        self.sustainability_score = 0.0
        
        # Thread pool
        self.executor = ThreadPoolExecutor(max_workers=4)
        
        # Initialize all 12 layers
        self._initialize_all_layers()
        
        # Start background tasks
        self._start_background_tasks()
        
        logger.info(
            f"Enhanced Layer Integrator v6.1.0 initialized: "
            f"layers={len(self.layers)}/12, "
            f"bio_integration={self.enable_bio_integration}, "
            f"carbon_intensity={self.enable_carbon_intensity}, "
            f"predictive={self.enable_predictive}, "
            f"dynamic_discovery={self.enable_dynamic_discovery}, "
            f"event_correlation={self.enable_event_correlation}, "
            f"gradient_cache={self.enable_gradient_cache}, "
            f"distributed_txns={self.enable_distributed_txns}"
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
        if any([self.token_manager, self.gradient_manager, self.compartment_manager]):
            self.enable_bio_integration = True
    
    # ========================================================================
    # NEW: Inject Expert Router, Helium Provider, and FL Monitor
    # ========================================================================
    
    def set_expert_router(self, router: 'ExpertRouter'):
        """Inject the MoE expert router."""
        self.expert_router = router
        logger.info("Expert Router injected into Layer Integrator")
    
    def set_helium_provider(self, provider):
        """Inject the Helium provider for telemetry."""
        self.helium_provider = provider
        logger.info("Helium provider injected into Layer Integrator")
    
    def set_fl_monitor(self, fl_monitor):
        """Inject the Federated Learning monitor for metrics."""
        self.fl_monitor = fl_monitor
        logger.info("FL monitor injected into Layer Integrator")
    
    # ========================================================================
    # NEW: Context Builder for MoE Expert System
    # ========================================================================
    
    async def build_context(self) -> Dict[str, Any]:
        """
        Build a comprehensive context dict for the MoE expert router.
        Gathers:
        - Helium telemetry (scarcity, cost, client energy)
        - Carbon intensity and price
        - Bio-inspired signals (gradients, token balance, stress)
        - Federated Learning metrics (if available)
        - Layer health and sustainability scores
        """
        context = {}
        
        # 1. Helium telemetry
        if self.helium_provider:
            context['helium_scarcity'] = self.helium_provider.get_scarcity()
            context['helium_cost_index'] = self.helium_provider.get_cost_index()
            context['avg_client_energy'] = self.helium_provider.get_avg_client_energy()
        else:
            # fallback
            context['helium_scarcity'] = 0.5
            context['helium_cost_index'] = 1.0
            context['avg_client_energy'] = 0.5
        
        # 2. Carbon intensity
        if self.enable_carbon_intensity:
            carbon_intensity = await self.carbon_manager.get_current_intensity()
            carbon_price = await self.carbon_manager.get_current_price()
            context['carbon_intensity'] = carbon_intensity / 1000.0  # normalize
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
        context['harvester_stress'] = self._get_harvester_vitality()  # use vitality as proxy
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
        
        # 6. Predictions (optional)
        if self.enable_predictive:
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
    
    # ========================================================================
    # Bio-Inspired Methods (Existing - unchanged)
    # ========================================================================
    
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
        """Get gradient-modulated circuit recovery delay"""
        if self.gradient_manager:
            trust = self.gradient_manager.fields.get('trust')
            if trust and trust.gradient_strength > self.layers[layer_number].circuit_breaker.gradient_health_threshold:
                return 15.0  # Faster recovery with high trust
            return 45.0
        return 30.0
    
    # ========================================================================
    # Background Loops (Existing - with minor updates)
    # ========================================================================
    
    async def _bio_sync_loop(self):
        while True:
            try:
                if not self.enable_bio_integration:
                    await asyncio.sleep(60); continue
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
                            # Reduce recovery timeout based on gradient health
                            recovery_delay = self._get_circuit_recovery_delay(layer_num)
                            if layer_info.circuit_breaker.recovery_attempts > 2:
                                layer_info.circuit_breaker.state = CircuitState.CLOSED
                                layer_info.circuit_breaker.failure_count = 0
                                logger.info(f"Circuit breaker reset for layer {layer_num}")
                
                # Update gradient-aware cache
                if self.enable_gradient_cache and self.gradient_cache:
                    gradients = self._get_real_gradient_levels()
                    await self.gradient_cache.invalidate_by_gradient(gradients.get('trust', 0.5))
                
                if self.enable_cache:
                    self.cache_ttl = self._get_token_backed_cache_ttl()
                await asyncio.sleep(30)
            except Exception as e:
                logger.error(f"Bio sync error: {str(e)}")
                await asyncio.sleep(60)
    
    async def _carbon_update_loop(self):
        while True:
            try:
                await self.carbon_manager.update_carbon_intensity()
                await asyncio.sleep(self.carbon_manager.update_interval)
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
                if self.enable_predictive:
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
                
                await asyncio.sleep(10)
            except Exception as e:
                logger.error(f"Health check error: {str(e)}")
                await asyncio.sleep(30)
    
    async def _discovery_loop(self):
        """Background loop for dynamic layer discovery"""
        while True:
            try:
                if self.enable_dynamic_discovery and self.discovery_manager:
                    # Simulate discovering new layers
                    for layer_num in range(12):
                        if layer_num not in self.layer_modules:
                            service_url = f"http://layer-{layer_num}:8080"
                            await self.discovery_manager.discover_layer(layer_num, service_url)
                await asyncio.sleep(60)
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
    
    # ========================================================================
    # Enhanced Layer Communication (unchanged)
    # ========================================================================
    
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
                        await self.gradient_cache.set(cache_key, result, layer_number, gradients.get('trust', 0.5))
                    elif self.enable_cache:
                        self._set_cache(cache_key, result, layer_number)
                
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
    
    # ========================================================================
    # Event System (Enhanced) – unchanged
    # ========================================================================
    
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
    
    # ========================================================================
    # Cache Management (Enhanced) – unchanged
    # ========================================================================
    
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
    
    # ========================================================================
    # Transaction Support (Enhanced) – unchanged
    # ========================================================================
    
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
            ecoatp_cost = len(layers_involved) * 10.0
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
    
    # ========================================================================
    # Layer Registration (Enhanced) – unchanged
    # ========================================================================
    
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
    
    # ========================================================================
    # Metrics (unchanged)
    # ========================================================================
    
    def _record_layer_success(self, layer_number: int, execution_time_ms: float):
        self.layer_latency[layer_number].append(execution_time_ms)
        self.layer_calls[layer_number] += 1
        if len(self.layer_latency[layer_number]) > 1000:
            self.layer_latency[layer_number] = self.layer_latency[layer_number][-1000:]
    
    def _record_layer_error(self, layer_number: int):
        self.layer_errors[layer_number] += 1
        self.layer_calls[layer_number] += 1
    
    # ========================================================================
    # Status Methods (Enhanced)
    # ========================================================================
    
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
        
        # Event correlation stats
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
        
        # Discovery stats
        if self.enable_dynamic_discovery and self.discovery_manager:
            status['discovery_stats'] = {
                'discovered_layers': len(self.discovery_manager.discovered_layers),
                'active_layers': sum(1 for l in self.discovery_manager.discovered_layers.values() if l.get('status') == 'active')
            }
        
        if self.enable_bio_integration:
            status['gradient_levels'] = self._get_real_gradient_levels()
            status['harvester_vitality'] = self._get_harvester_vitality()
        
        if self.enable_predictive:
            status['predictive_forecast'] = asyncio.run(self.predictive_analyzer.predict_layer_health())
        
        if self.enable_cross_domain:
            status['cross_domain_stats'] = self.cross_domain_transfer.get_transfer_statistics()
        
        status['sustainability'] = {
            'score': self.sustainability_score,
            'carbon_savings_kg': self.total_carbon_savings_kg
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
        await self.carbon_manager.close()
        logger.info("Shutdown complete")


# ============================================================================
# Legacy Compatibility Class
# ============================================================================

class LayerIntegrator(EnhancedLayerIntegrator):
    """
    Legacy LayerIntegrator for backward compatibility.
    """
    
    def __init__(self, expert_router=None):
        super().__init__()
        self.router = expert_router
        self.layer_integration_status = {f'layer_{i}': False for i in range(12)}
        logger.info("Layer Integrator initialized (compatibility mode)")
    
    def get_integration_status(self) -> Dict[str, bool]:
        return self.layer_integration_status.copy()
