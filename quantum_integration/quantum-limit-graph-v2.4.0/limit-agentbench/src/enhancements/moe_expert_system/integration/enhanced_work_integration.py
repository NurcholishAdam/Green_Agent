# File: quantum_integration/quantum-limit-graph-v2.4.0/limit-agentbench/src/enhancements/moe_expert_system/integration/enhanced_work_integration.py
"""
Enhanced Work Integration v6.0.0 - Complete Green Agent Implementation

Complete bio-inspired integration with:
- Federated Reflexive Learning with meta-cognitive state sharing
- User-Adaptive Reflexivity with dynamic thresholds
- Real-time Carbon Intensity Integration with API support
- Cross-Domain Knowledge Transfer with domain mapping
- Human-AI Collaborative Reflection with feedback loops
- Predictive Reflexivity with ensemble forecasting
- Sustainability Score with multi-metric aggregation
- Enhanced Carbon/Helium Awareness with real-time tracking
- Eco-ATP token allocation for work execution
- Biomass storage for deferred task queuing
- Gradient-aware priority scheduling
- Compartment-aware work routing
- ATP synthase-driven work dispatching
- Photosynthetic opportunity detection
- Token recovery on work failure
- Token expiration handling for stale work
- Gradient-modulated SLA management
- Biomass mobilization for backlog processing
- State persistence for recovery across restarts (NEW)
- Dynamic token pricing based on priority and scarcity (NEW)
- Quantum-classical hybrid pipeline for adaptive routing (NEW)
- SLA prediction for proactive scheduling (NEW)
- Sustainability dashboard for work items (NEW)
"""

import asyncio
import logging
from typing import Dict, Any, List, Optional, Tuple, Set, Callable
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
import numpy as np
from collections import defaultdict, deque
import hashlib
import json
import uuid
import networkx as nx
from concurrent.futures import ThreadPoolExecutor
import aiohttp
import os
import pickle
import zlib

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
        BiomassStorage, StorageTier, GuaranteeLevel, StoredTask, StorageToken
    )
    from enhancements.bio_inspired.photosynthetic_harvester import (
        PhotosyntheticHarvester
    )
    BIO_INSPIRED_AVAILABLE = True
    logger.info("Bio-inspired modules loaded for Enhanced Work Integration")
except ImportError as e:
    BIO_INSPIRED_AVAILABLE = False
    logger.warning(f"Bio-inspired modules not available: {str(e)} - using standard work processing")

# ============================================================================
# Work State Machine
# ============================================================================

class WorkState(Enum):
    CREATED = "created"; VALIDATED = "validated"; QUEUED = "queued"; SCHEDULED = "scheduled"
    RESOURCES_RESERVED = "resources_reserved"; TOKENS_ALLOCATED = "tokens_allocated"
    EXECUTING = "executing"; CHECKPOINTED = "checkpointed"; COMPLETED = "completed"
    FAILED = "failed"; ROLLING_BACK = "rolling_back"; ROLLED_BACK = "rolled_back"
    CANCELLED = "cancelled"; STORED_AS_BIOMASS = "stored_as_biomass"; SUSPENDED = "suspended"
    RESUMED = "resumed"; MIGRATED = "migrated"; ARCHIVED = "archived"
    
    def is_terminal(self) -> bool:
        return self in [WorkState.COMPLETED, WorkState.FAILED,
                       WorkState.ROLLED_BACK, WorkState.CANCELLED, WorkState.ARCHIVED]
    
    def is_active(self) -> bool:
        return self in [WorkState.EXECUTING, WorkState.ROLLING_BACK, WorkState.CHECKPOINTED]
    
    def can_transition_to(self, target: 'WorkState') -> bool:
        valid_transitions = {
            WorkState.CREATED: [WorkState.VALIDATED, WorkState.CANCELLED],
            WorkState.VALIDATED: [WorkState.QUEUED, WorkState.CANCELLED, WorkState.STORED_AS_BIOMASS],
            WorkState.QUEUED: [WorkState.SCHEDULED, WorkState.CANCELLED, WorkState.STORED_AS_BIOMASS],
            WorkState.SCHEDULED: [WorkState.RESOURCES_RESERVED, WorkState.TOKENS_ALLOCATED, WorkState.CANCELLED],
            WorkState.RESOURCES_RESERVED: [WorkState.EXECUTING, WorkState.CANCELLED],
            WorkState.TOKENS_ALLOCATED: [WorkState.EXECUTING, WorkState.CANCELLED],
            WorkState.EXECUTING: [WorkState.COMPLETED, WorkState.FAILED, WorkState.CHECKPOINTED,
                                 WorkState.SUSPENDED, WorkState.MIGRATED],
            WorkState.CHECKPOINTED: [WorkState.EXECUTING, WorkState.RESUMED, WorkState.FAILED],
            WorkState.FAILED: [WorkState.ROLLING_BACK, WorkState.QUEUED, WorkState.STORED_AS_BIOMASS],
            WorkState.ROLLING_BACK: [WorkState.ROLLED_BACK, WorkState.FAILED],
            WorkState.ROLLED_BACK: [WorkState.QUEUED, WorkState.ARCHIVED, WorkState.STORED_AS_BIOMASS],
            WorkState.SUSPENDED: [WorkState.RESUMED, WorkState.CANCELLED],
            WorkState.RESUMED: [WorkState.EXECUTING],
            WorkState.STORED_AS_BIOMASS: [WorkState.QUEUED, WorkState.EXECUTING, WorkState.ARCHIVED],
            WorkState.COMPLETED: [WorkState.ARCHIVED]
        }
        return target in valid_transitions.get(self, [])

class WorkPriority(Enum):
    CRITICAL = 0; HIGH = 1; MEDIUM = 2; LOW = 3; BACKGROUND = 4; DEFERRABLE = 5
    
    @property
    def weight(self) -> float:
        weights = {WorkPriority.CRITICAL: 10.0, WorkPriority.HIGH: 5.0, WorkPriority.MEDIUM: 2.0,
                   WorkPriority.LOW: 1.0, WorkPriority.BACKGROUND: 0.5, WorkPriority.DEFERRABLE: 0.2}
        return weights.get(self, 1.0)

class SLALevel(Enum):
    PLATINUM = "platinum"; GOLD = "gold"; SILVER = "silver"; BRONZE = "bronze"; BEST_EFFORT = "best_effort"

# ============================================================================
# Carbon Intensity Integration Module
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
        self.price_trend = 0.0
    
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
        """Update carbon price based on intensity"""
        base_price = 50.0
        intensity_factor = (intensity - 300) / 500
        self.carbon_price_usd_per_ton = max(10.0, base_price * (1.0 + intensity_factor))
        self.price_history.append({
            'timestamp': self.last_update.isoformat() if self.last_update else None,
            'price': self.carbon_price_usd_per_ton
        })
        if len(self.price_history) > 5:
            recent_prices = [p['price'] for p in list(self.price_history)[-5:]]
            self.price_trend = np.polyfit(range(len(recent_prices)), recent_prices, 1)[0]
    
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
# Predictive Reflexivity Module
# ============================================================================

class PredictiveWorkAnalyzer:
    """Predictive reflexivity with ensemble forecasting for work processing"""
    
    def __init__(self, history_window: int = 100):
        self.history_window = history_window
        self.work_history = deque(maxlen=history_window)
        self.forecast_history = deque(maxlen=50)
        self.models = {}
        self.scaler = None
        self.is_trained = False
        
        try:
            from sklearn.preprocessing import StandardScaler
            from sklearn.ensemble import RandomForestRegressor, GradientBoostingRegressor
            from sklearn.metrics import r2_score
            self.scaler = StandardScaler()
            self.models['random_forest'] = RandomForestRegressor(n_estimators=100, random_state=42)
            self.models['gradient_boosting'] = GradientBoostingRegressor(n_estimators=100, random_state=42)
            self._ml_available = True
        except ImportError:
            self._ml_available = False
    
    def update_history(self, work_metrics: Dict):
        self.work_history.append({
            'timestamp': datetime.utcnow(),
            'success_rate': work_metrics.get('success_rate', 0.8),
            'avg_latency_ms': work_metrics.get('avg_latency_ms', 100),
            'carbon_intensity': work_metrics.get('carbon_intensity', 400),
            'token_efficiency': work_metrics.get('token_efficiency', 0.5),
            'workload': work_metrics.get('workload', 0.5)
        })
    
    async def train_forecast_model(self):
        if not self._ml_available or len(self.work_history) < 10:
            return {'status': 'insufficient_data'}
        
        X, y = [], []
        history_list = list(self.work_history)
        for i in range(len(history_list) - 5):
            features = []
            for j in range(5):
                data = history_list[i + j]
                features.extend([data['success_rate'], data['avg_latency_ms'] / 1000,
                               data['carbon_intensity'] / 100, data['token_efficiency'],
                               data['workload']])
            X.append(features)
            y.append(history_list[i + 5]['success_rate'])
        
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
    
    async def predict_work_trend(self) -> Dict:
        if not self.is_trained or len(self.work_history) < 10:
            return {'predicted_success': 0.5, 'confidence': 0.0, 'trend': 'insufficient_data'}
        
        recent = list(self.work_history)[-5:]
        features = []
        for data in recent:
            features.extend([data['success_rate'], data['avg_latency_ms'] / 1000,
                           data['carbon_intensity'] / 100, data['token_efficiency'],
                           data['workload']])
        features = np.array(features).reshape(1, -1)
        features_scaled = self.scaler.transform(features)
        predictions = []
        for name, model in self.models.items():
            if model is not None:
                predictions.append(model.predict(features_scaled)[0])
        if not predictions:
            return {'predicted_success': 0.5, 'confidence': 0.0, 'trend': 'no_models'}
        prediction = np.mean(predictions)
        confidence = min(0.9, np.std(predictions) / 0.2) if len(predictions) > 1 else 0.5
        if len(self.forecast_history) > 5:
            recent_forecasts = list(self.forecast_history)[-5:]
            trend = "improving" if prediction > recent_forecasts[-1] else "declining" if prediction < recent_forecasts[-1] else "stable"
        else:
            trend = "stable"
        self.forecast_history.append({'prediction': prediction, 'trend': trend})
        return {'predicted_success': prediction, 'confidence': confidence, 'trend': trend,
                'recommended_actions': self._generate_actions(prediction)}
    
    def _generate_actions(self, prediction: float) -> List[str]:
        actions = []
        if prediction < 0.4:
            actions.append("Increase carbon budget allocation")
            actions.append("Optimize token efficiency")
        elif prediction < 0.6:
            actions.append("Enhance resource reservation strategy")
            actions.append("Improve SLA compliance")
        return actions or ["Work processing is on track"]

# ============================================================================
# Cross-Domain Knowledge Transfer Module
# ============================================================================

class WorkCrossDomainTransfer:
    """Cross-domain knowledge transfer for work processing"""
    
    def __init__(self):
        self.knowledge_base: Dict[str, Dict[str, Dict]] = {}
        self.transfer_logs = deque(maxlen=1000)
        self.domain_mappings = {
            'work→energy': {
                'efficiency_strategies': ['token-based', 'gradient-driven', 'ATP-aware'],
                'resource_allocation': ['dynamic', 'adaptive', 'predictive']
            },
            'work→carbon': {
                'optimization_strategies': ['load-shifting', 'efficiency-first', 'renewable-tracking']
            },
            'work→helium': {
                'scarcity_strategies': ['efficiency-first', 'conservation', 'recovery']
            },
            'work→data': {
                'compression_strategies': ['lossy', 'lossless', 'adaptive']
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
# Data Classes (Enhanced)
# ============================================================================

@dataclass
class WorkSLA:
    level: SLALevel
    max_latency_ms: float
    min_availability: float
    max_carbon_kg: Optional[float] = None
    max_helium_units: Optional[float] = None
    max_ecoatp_cost: Optional[float] = None
    deadline: Optional[datetime] = None
    penalty_per_violation: float = 0.0
    violations: int = 0
    # NEW: SLA prediction
    predicted_violation_probability: float = 0.0
    sla_health_score: float = 1.0
    
    def is_violated(self, actual_latency_ms: float) -> bool:
        return actual_latency_ms > self.max_latency_ms
    
    def time_until_deadline(self) -> Optional[float]:
        if self.deadline:
            return (self.deadline - datetime.utcnow()).total_seconds()
        return None
    
    def is_deadline_critical(self) -> bool:
        remaining = self.time_until_deadline()
        return remaining is not None and remaining < 60
    
    def update_health(self, predicted_probability: float):
        """Update SLA health score based on prediction"""
        self.predicted_violation_probability = predicted_probability
        self.sla_health_score = 1.0 - predicted_probability

@dataclass
class ResourceReservation:
    reservation_id: str
    work_id: str
    resources: Dict[str, float]
    reserved_at: datetime
    expires_at: datetime
    carbon_budget_kg: float
    helium_budget: float
    ecoatp_budget: float = 0.0
    is_active: bool = True
    # NEW: Dynamic pricing
    carbon_price_at_reservation: float = 50.0
    helium_price_at_reservation: float = 0.5

@dataclass
class WorkCheckpoint:
    checkpoint_id: str
    work_id: str
    state: WorkState
    progress: float
    intermediate_results: Dict[str, Any]
    resource_usage: Dict[str, float]
    ecoatp_consumed: float = 0.0
    created_at: datetime = field(default_factory=datetime.utcnow)
    pipeline_state: Dict[str, Any] = field(default_factory=dict)
    # NEW: Sustainability metrics at checkpoint
    carbon_footprint_at_checkpoint: float = 0.0
    helium_usage_at_checkpoint: float = 0.0

@dataclass
class EnhancedWorkContext:
    task_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    work_type: str = "general"
    priority: WorkPriority = WorkPriority.MEDIUM
    state: WorkState = WorkState.CREATED
    state_history: List[Tuple[WorkState, datetime]] = field(default_factory=list)
    sla: Optional[WorkSLA] = None
    complexity: float = 0.5
    estimated_duration_ms: float = 100.0
    helium_dependency: float = 0.0
    helium_profile: Dict[str, Any] = field(default_factory=dict)
    meta_cognitive_state: Dict[str, Any] = field(default_factory=dict)
    reflection_notes: List[str] = field(default_factory=list)
    symbolic_rules: Dict[str, Any] = field(default_factory=dict)
    knowledge_graph_nodes: List[str] = field(default_factory=list)
    carbon_zone: int = 0
    helium_zone: int = 0
    dual_axis_score: float = 0.0
    quantum_capable: bool = False
    quantum_circuit_required: bool = False
    quantum_backend_type: Optional[str] = None
    max_carbon_budget: float = float('inf')
    max_helium_budget: float = float('inf')
    max_latency_ms: float = 1000.0
    max_ecoatp_budget: float = float('inf')
    min_accuracy: float = 0.0
    batch_group: Optional[str] = None
    can_batch: bool = True
    batch_priority: int = 0
    depends_on: List[str] = field(default_factory=list)
    dependents: List[str] = field(default_factory=list)
    checkpoints: List[WorkCheckpoint] = field(default_factory=list)
    resume_from_checkpoint: Optional[str] = None
    tenant_id: str = "default"
    isolation_level: str = "shared"
    reservation: Optional[ResourceReservation] = None
    tokens_allocated: float = 0.0
    tokens_consumed: float = 0.0
    tokens_recovered: float = 0.0
    biomass_storage_token: Optional[str] = None
    compartment_id: Optional[str] = None
    created_at: datetime = field(default_factory=datetime.utcnow)
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    execution_attempts: int = 0
    max_attempts: int = 3
    rollback_actions: List[Callable] = field(default_factory=list)
    compensation_actions: List[Callable] = field(default_factory=list)
    metrics: Dict[str, Any] = field(default_factory=dict)
    events: List[Dict[str, Any]] = field(default_factory=list)
    sustainability_score: float = 0.0
    carbon_savings_kg: float = 0.0
    # NEW: Enhanced tracking
    predicted_completion_time: Optional[datetime] = None
    deadline_risk_score: float = 0.0
    resource_efficiency_score: float = 0.0
    dynamic_token_price: float = 1.0
    
    def transition_to(self, new_state: WorkState) -> bool:
        if not self.state.can_transition_to(new_state):
            logger.warning(f"Invalid state transition: {self.state.value} -> {new_state.value}")
            return False
        old_state = self.state
        self.state = new_state
        self.state_history.append((new_state, datetime.utcnow()))
        return True
    
    def add_checkpoint(self, checkpoint: WorkCheckpoint):
        self.checkpoints.append(checkpoint)
        if len(self.checkpoints) > 5:
            self.checkpoints = self.checkpoints[-5:]
    
    def add_event(self, event_type: str, details: Dict[str, Any]):
        self.events.append({'type': event_type, 'details': details,
                           'timestamp': datetime.utcnow().isoformat()})
        if len(self.events) > 1000:
            self.events = self.events[-1000:]
    
    def can_retry(self) -> bool:
        return self.execution_attempts < self.max_attempts
    
    def to_routing_context(self) -> Dict[str, Any]:
        return {
            'task_id': self.task_id,
            'task_type': self.work_type,
            'priority': self.priority.value,
            'complexity': self.complexity,
            'estimated_duration_ms': self.estimated_duration_ms,
            'carbon_zone': self.carbon_zone,
            'helium_dependency': self.helium_dependency,
            'quantum_capable': self.quantum_capable,
            'max_latency_ms': self.max_latency_ms,
            'max_carbon_budget': self.max_carbon_budget,
            'max_helium_budget': self.max_helium_budget,
            'max_ecoatp_budget': self.max_ecoatp_budget,
            'tenant_id': self.tenant_id
        }

# ============================================================================
# State Persistence Manager (NEW)
# ============================================================================

class StatePersistenceManager:
    """
    State persistence for recovery across restarts.
    
    Features:
    - Checkpoint serialization
    - Recovery from persistent storage
    - State versioning
    - Compression support
    """
    
    def __init__(self, storage_path: str = "work_states"):
        self.storage_path = storage_path
        self._lock = asyncio.Lock()
        os.makedirs(storage_path, exist_ok=True)
        logger.info(f"State Persistence Manager initialized at {storage_path}")
    
    async def save_work_state(self, work_id: str, context: EnhancedWorkContext):
        """Save work state to persistent storage"""
        async with self._lock:
            try:
                filename = f"{self.storage_path}/{work_id}.pkl"
                
                # Create serializable copy
                state_data = {
                    'task_id': context.task_id,
                    'work_type': context.work_type,
                    'priority': context.priority.value,
                    'state': context.state.value,
                    'state_history': [(s.value, t.isoformat()) for s, t in context.state_history],
                    'complexity': context.complexity,
                    'estimated_duration_ms': context.estimated_duration_ms,
                    'helium_dependency': context.helium_dependency,
                    'carbon_zone': context.carbon_zone,
                    'quantum_capable': context.quantum_capable,
                    'max_carbon_budget': context.max_carbon_budget,
                    'max_helium_budget': context.max_helium_budget,
                    'max_latency_ms': context.max_latency_ms,
                    'max_ecoatp_budget': context.max_ecoatp_budget,
                    'tokens_allocated': context.tokens_allocated,
                    'tokens_consumed': context.tokens_consumed,
                    'tokens_recovered': context.tokens_recovered,
                    'execution_attempts': context.execution_attempts,
                    'max_attempts': context.max_attempts,
                    'sustainability_score': context.sustainability_score,
                    'carbon_savings_kg': context.carbon_savings_kg,
                    'created_at': context.created_at.isoformat(),
                    'started_at': context.started_at.isoformat() if context.started_at else None,
                    'completed_at': context.completed_at.isoformat() if context.completed_at else None,
                    'checkpoints': [
                        {
                            'checkpoint_id': cp.checkpoint_id,
                            'progress': cp.progress,
                            'created_at': cp.created_at.isoformat(),
                            'ecoatp_consumed': cp.ecoatp_consumed
                        }
                        for cp in context.checkpoints
                    ],
                    'metrics': context.metrics,
                    'events': context.events
                }
                
                # Compress and save
                serialized = pickle.dumps(state_data)
                compressed = zlib.compress(serialized)
                
                with open(filename, 'wb') as f:
                    f.write(compressed)
                
                logger.debug(f"Saved work state for {work_id}")
                return True
                
            except Exception as e:
                logger.error(f"Error saving work state for {work_id}: {e}")
                return False
    
    async def load_work_state(self, work_id: str) -> Optional[Dict]:
        """Load work state from persistent storage"""
        async with self._lock:
            try:
                filename = f"{self.storage_path}/{work_id}.pkl"
                if not os.path.exists(filename):
                    return None
                
                with open(filename, 'rb') as f:
                    compressed = f.read()
                
                serialized = zlib.decompress(compressed)
                state_data = pickle.loads(serialized)
                
                # Reconstruct state
                state_data['state'] = WorkState(state_data['state'])
                state_data['priority'] = WorkPriority(state_data['priority'])
                state_data['created_at'] = datetime.fromisoformat(state_data['created_at'])
                if state_data['started_at']:
                    state_data['started_at'] = datetime.fromisoformat(state_data['started_at'])
                if state_data['completed_at']:
                    state_data['completed_at'] = datetime.fromisoformat(state_data['completed_at'])
                
                logger.debug(f"Loaded work state for {work_id}")
                return state_data
                
            except Exception as e:
                logger.error(f"Error loading work state for {work_id}: {e}")
                return None
    
    async def delete_work_state(self, work_id: str):
        """Delete work state from persistent storage"""
        async with self._lock:
            try:
                filename = f"{self.storage_path}/{work_id}.pkl"
                if os.path.exists(filename):
                    os.remove(filename)
                    logger.debug(f"Deleted work state for {work_id}")
                    return True
                return False
            except Exception as e:
                logger.error(f"Error deleting work state for {work_id}: {e}")
                return False

# ============================================================================
# Dynamic Token Pricing Manager (NEW)
# ============================================================================

class DynamicTokenPricingManager:
    """
    Dynamic token pricing based on priority and resource scarcity.
    
    Features:
    - Priority-based pricing
    - Scarcity-aware pricing
    - Demand forecasting
    - Price elasticity modeling
    """
    
    def __init__(self):
        self.base_price = 1.0
        self.priority_multipliers = {
            WorkPriority.CRITICAL: 2.0,
            WorkPriority.HIGH: 1.5,
            WorkPriority.MEDIUM: 1.0,
            WorkPriority.LOW: 0.7,
            WorkPriority.BACKGROUND: 0.5,
            WorkPriority.DEFERRABLE: 0.3
        }
        self.scarcity_factors = {
            'carbon': 1.0,
            'helium': 1.0,
            'energy': 1.0
        }
        self.demand_history: deque = deque(maxlen=1000)
        self.price_history: deque = deque(maxlen=1000)
        self._lock = asyncio.Lock()
        
        logger.info("Dynamic Token Pricing Manager initialized")
    
    async def get_price(
        self,
        priority: WorkPriority,
        resource_type: str = 'general'
    ) -> float:
        """Get dynamic token price for a work item"""
        async with self._lock:
            base_price = self.base_price
            priority_multiplier = self.priority_multipliers.get(priority, 1.0)
            scarcity_multiplier = self.scarcity_factors.get(resource_type, 1.0)
            
            # Demand adjustment
            if len(self.demand_history) > 10:
                recent_demand = np.mean(list(self.demand_history)[-10:])
                demand_adjustment = 1.0 + (recent_demand - 0.5) * 0.5
            else:
                demand_adjustment = 1.0
            
            price = base_price * priority_multiplier * scarcity_multiplier * demand_adjustment
            return max(0.1, min(10.0, price))
    
    async def update_scarcity(self, resource_type: str, scarcity: float):
        """Update scarcity factor for a resource"""
        async with self._lock:
            self.scarcity_factors[resource_type] = 1.0 + scarcity * 0.5
    
    async def record_demand(self, demand_level: float):
        """Record demand level for price forecasting"""
        async with self._lock:
            self.demand_history.append(demand_level)
    
    def get_pricing_stats(self) -> Dict[str, Any]:
        """Get pricing statistics"""
        return {
            'base_price': self.base_price,
            'priority_multipliers': {k.name: v for k, v in self.priority_multipliers.items()},
            'scarcity_factors': self.scarcity_factors.copy(),
            'demand_samples': len(self.demand_history),
            'price_samples': len(self.price_history)
        }

# ============================================================================
# Quantum-Classical Hybrid Pipeline (NEW)
# ============================================================================

class QuantumClassicalHybridPipeline:
    """
    Quantum-classical hybrid pipeline for adaptive workload routing.
    
    Features:
    - Quantum circuit optimization
    - Classical fallback
    - Hybrid execution modes
    - Adaptive routing based on work characteristics
    """
    
    def __init__(self, quantum_module=None):
        self.quantum_module = quantum_module
        self.hybrid_stats = {
            'quantum_executions': 0,
            'classical_executions': 0,
            'hybrid_executions': 0,
            'quantum_success_rate': 0.0,
            'classical_success_rate': 0.0,
            'savings_by_mode': defaultdict(float)
        }
        self._lock = asyncio.Lock()
        logger.info("Quantum-Classical Hybrid Pipeline initialized")
    
    async def execute(
        self,
        context: EnhancedWorkContext,
        work_fn: Callable,
        quantum_threshold: float = 0.7
    ) -> Dict[str, Any]:
        """
        Execute work with adaptive quantum-classical routing.
        
        Args:
            context: Work context
            work_fn: Work function to execute
            quantum_threshold: Threshold for quantum execution (0-1)
        """
        async with self._lock:
            # Determine execution mode
            should_use_quantum = (
                context.quantum_capable and
                context.complexity > quantum_threshold and
                self.quantum_module is not None
            )
            
            if should_use_quantum:
                # Try quantum execution
                try:
                    result = await self._execute_quantum(context, work_fn)
                    self.hybrid_stats['quantum_executions'] += 1
                    if result.get('success', False):
                        self.hybrid_stats['quantum_success_rate'] = (
                            (self.hybrid_stats['quantum_success_rate'] * 0.9) + 0.1
                        )
                        result['execution_mode'] = 'quantum'
                        return result
                    else:
                        # Fallback to classical
                        logger.info(f"Quantum execution failed, falling back to classical for {context.task_id}")
                except Exception as e:
                    logger.warning(f"Quantum execution error: {e}, falling back to classical")
            
            # Classical execution
            result = await self._execute_classical(context, work_fn)
            self.hybrid_stats['classical_executions'] += 1
            if result.get('success', False):
                self.hybrid_stats['classical_success_rate'] = (
                    (self.hybrid_stats['classical_success_rate'] * 0.9) + 0.1
                )
            result['execution_mode'] = 'classical'
            return result
    
    async def _execute_quantum(self, context: EnhancedWorkContext, work_fn: Callable) -> Dict:
        """Execute work using quantum module"""
        if not self.quantum_module:
            return {'success': False, 'error': 'Quantum module not available'}
        
        # Prepare quantum circuit
        circuit_params = {
            'qubits': context.quantum_capable if hasattr(context, 'quantum_capable') else 4,
            'depth': min(10, int(context.complexity * 20)),
            'backend': context.quantum_backend_type or 'simulator'
        }
        
        # Execute quantum work
        result = await work_fn(context, quantum_mode=True, circuit_params=circuit_params)
        
        # Calculate quantum savings
        self.hybrid_stats['savings_by_mode']['quantum'] += result.get('carbon_savings_kg', 0)
        
        return result
    
    async def _execute_classical(self, context: EnhancedWorkContext, work_fn: Callable) -> Dict:
        """Execute work classically"""
        result = await work_fn(context, quantum_mode=False)
        self.hybrid_stats['savings_by_mode']['classical'] += result.get('carbon_savings_kg', 0)
        return result
    
    def get_hybrid_stats(self) -> Dict[str, Any]:
        """Get hybrid execution statistics"""
        total = (self.hybrid_stats['quantum_executions'] + 
                self.hybrid_stats['classical_executions'] + 
                self.hybrid_stats['hybrid_executions'])
        
        return {
            'quantum_executions': self.hybrid_stats['quantum_executions'],
            'classical_executions': self.hybrid_stats['classical_executions'],
            'hybrid_executions': self.hybrid_stats['hybrid_executions'],
            'total_executions': total,
            'quantum_success_rate': self.hybrid_stats['quantum_success_rate'],
            'classical_success_rate': self.hybrid_stats['classical_success_rate'],
            'carbon_savings_by_mode': dict(self.hybrid_stats['savings_by_mode'])
        }

# ============================================================================
# Sustainability Dashboard (NEW)
# ============================================================================

class WorkSustainabilityDashboard:
    """
    Sustainability dashboard for work items.
    
    Features:
    - Real-time sustainability metrics
    - Historical trend analysis
    - Work-level sustainability scoring
    - Recommendations generation
    """
    
    def __init__(self):
        self.metrics: Dict[str, deque] = {}
        self.scores: Dict[str, float] = {}
        self.history = deque(maxlen=10000)
        self._lock = asyncio.Lock()
        
        # Initialize metric streams
        self.metrics['carbon_intensity'] = deque(maxlen=1000)
        self.metrics['helium_usage'] = deque(maxlen=1000)
        self.metrics['token_efficiency'] = deque(maxlen=1000)
        self.metrics['success_rate'] = deque(maxlen=1000)
        self.metrics['sustainability_score'] = deque(maxlen=1000)
        
        logger.info("Work Sustainability Dashboard initialized")
    
    async def update_metrics(self, work_id: str, metrics: Dict[str, float]):
        """Update sustainability metrics for a work item"""
        async with self._lock:
            self.scores[work_id] = metrics.get('sustainability_score', 0.0)
            
            # Update metric streams
            for key, value in metrics.items():
                if key in self.metrics:
                    self.metrics[key].append(value)
            
            self.history.append({
                'timestamp': datetime.utcnow().isoformat(),
                'work_id': work_id,
                'metrics': metrics
            })
    
    async def get_dashboard_data(self, work_id: Optional[str] = None) -> Dict[str, Any]:
        """Get dashboard data for a work item or overall"""
        async with self._lock:
            if work_id:
                return {
                    'work_id': work_id,
                    'sustainability_score': self.scores.get(work_id, 0.0),
                    'work_history': [h for h in self.history if h['work_id'] == work_id][-50:]
                }
            
            # Overall dashboard
            recent = list(self.history)[-100:]
            return {
                'current_metrics': {
                    'avg_sustainability_score': np.mean([h['metrics'].get('sustainability_score', 0) for h in recent]) if recent else 0,
                    'avg_carbon_intensity': np.mean([h['metrics'].get('carbon_intensity', 400) for h in recent]) if recent else 400,
                    'avg_helium_usage': np.mean([h['metrics'].get('helium_usage', 0.5) for h in recent]) if recent else 0.5,
                    'avg_token_efficiency': np.mean([h['metrics'].get('token_efficiency', 0.5) for h in recent]) if recent else 0.5
                },
                'trends': {
                    'sustainability_trend': self._calculate_trend('sustainability_score'),
                    'carbon_trend': self._calculate_trend('carbon_intensity'),
                    'success_trend': self._calculate_trend('success_rate')
                },
                'total_works_tracked': len(self.scores),
                'recommendations': await self._generate_recommendations()
            }
    
    def _calculate_trend(self, metric_key: str) -> str:
        """Calculate trend for a metric"""
        if metric_key not in self.metrics or len(self.metrics[metric_key]) < 10:
            return 'insufficient_data'
        
        values = list(self.metrics[metric_key])[-10:]
        if len(values) < 3:
            return 'stable'
        
        slope = np.polyfit(range(len(values)), values, 1)[0]
        if abs(slope) < 0.01:
            return 'stable'
        elif slope > 0:
            return 'improving'
        else:
            return 'declining'
    
    async def _generate_recommendations(self) -> List[str]:
        """Generate sustainability recommendations"""
        recommendations = []
        
        if len(self.metrics['sustainability_score']) > 10:
            avg_score = np.mean(list(self.metrics['sustainability_score'])[-10:])
            if avg_score < 0.5:
                recommendations.append("Overall sustainability is below target - consider optimization")
        
        if len(self.metrics['carbon_intensity']) > 10:
            avg_carbon = np.mean(list(self.metrics['carbon_intensity'])[-10:])
            if avg_carbon > 500:
                recommendations.append("High carbon intensity detected - consider scheduling during off-peak hours")
        
        if len(self.metrics['success_rate']) > 10:
            avg_success = np.mean(list(self.metrics['success_rate'])[-10:])
            if avg_success < 0.8:
                recommendations.append("Low success rate - consider increasing retry limits or adjusting timeouts")
        
        return recommendations or ["All sustainability metrics are within acceptable ranges"]

# ========================================================================
# Resource Reservation Manager
# ========================================================================

class ResourceReservationManager:
    def __init__(self):
        self.reservations: Dict[str, ResourceReservation] = {}
        self.total_carbon_allocated: float = 0.0
        self.total_helium_allocated: float = 0.0
        self.total_ecoatp_allocated: float = 0.0
    
    def reserve(self, work_id: str, resources: Dict[str, float],
                carbon_budget: float, helium_budget: float,
                ecoatp_budget: float = 0.0, duration_seconds: float = 300,
                carbon_price: float = 50.0, helium_price: float = 0.5) -> Optional[ResourceReservation]:
        reservation = ResourceReservation(
            reservation_id=f"res_{work_id}_{datetime.utcnow().timestamp()}",
            work_id=work_id, resources=resources,
            reserved_at=datetime.utcnow(),
            expires_at=datetime.utcnow() + timedelta(seconds=duration_seconds),
            carbon_budget_kg=carbon_budget, helium_budget=helium_budget,
            ecoatp_budget=ecoatp_budget,
            carbon_price_at_reservation=carbon_price,
            helium_price_at_reservation=helium_price
        )
        self.reservations[reservation.reservation_id] = reservation
        self.total_carbon_allocated += carbon_budget
        self.total_helium_allocated += helium_budget
        self.total_ecoatp_allocated += ecoatp_budget
        return reservation
    
    def release(self, reservation_id: str):
        if reservation_id in self.reservations:
            reservation = self.reservations.pop(reservation_id)
            self.total_carbon_allocated -= reservation.carbon_budget_kg
            self.total_helium_allocated -= reservation.helium_budget
            self.total_ecoatp_allocated -= reservation.ecoatp_budget

# ============================================================================
# Enhanced Work Integrator
# ============================================================================

class EnhancedWorkIntegrator:
    """
    Enhanced Work Integrator v6.0.0 - Complete Green Agent Implementation
    
    New Features:
    - State persistence for recovery across restarts
    - Dynamic token pricing based on priority and scarcity
    - Quantum-classical hybrid pipeline for adaptive routing
    - SLA prediction for proactive scheduling
    - Sustainability dashboard for work items
    """
    
    def __init__(
        self,
        expert_router=None,
        meta_cognitive_module=None,
        neuro_symbolic_module=None,
        quantum_module=None,
        enable_batching: bool = True,
        enable_checkpointing: bool = True,
        enable_rollback: bool = True,
        enable_sla_tracking: bool = True,
        enable_resource_reservation: bool = True,
        enable_bio_integration: bool = True,
        enable_carbon_intensity: bool = True,
        enable_predictive: bool = True,
        enable_cross_domain: bool = True,
        enable_sustainability_scoring: bool = True,
        enable_state_persistence: bool = True,  # NEW
        enable_dynamic_pricing: bool = True,  # NEW
        enable_hybrid_pipeline: bool = True,  # NEW
        enable_sustainability_dashboard: bool = True  # NEW
    ):
        # Core modules
        self.router = expert_router
        self.meta_cognitive = meta_cognitive_module
        self.neuro_symbolic = neuro_symbolic_module
        self.quantum_module = quantum_module
        
        # Feature flags
        self.enable_batching = enable_batching
        self.enable_checkpointing = enable_checkpointing
        self.enable_rollback = enable_rollback
        self.enable_sla_tracking = enable_sla_tracking
        self.enable_resource_reservation = enable_resource_reservation
        self.enable_bio_integration = enable_bio_integration and BIO_INSPIRED_AVAILABLE
        self.enable_carbon_intensity = enable_carbon_intensity
        self.enable_predictive = enable_predictive
        self.enable_cross_domain = enable_cross_domain
        self.enable_sustainability_scoring = enable_sustainability_scoring
        
        # NEW feature flags
        self.enable_state_persistence = enable_state_persistence
        self.enable_dynamic_pricing = enable_dynamic_pricing
        self.enable_hybrid_pipeline = enable_hybrid_pipeline
        self.enable_sustainability_dashboard = enable_sustainability_dashboard
        
        # Bio-inspired modules
        self.token_manager = None
        self.gradient_manager = None
        self.scheduler = None
        self.compartment_manager = None
        self.biomass_storage = None
        self.harvester = None
        
        # Existing modules
        self.carbon_manager = CarbonIntensityManager()
        self.predictive_analyzer = PredictiveWorkAnalyzer()
        self.cross_domain_transfer = WorkCrossDomainTransfer()
        
        # NEW modules
        self.state_persistence = StatePersistenceManager() if enable_state_persistence else None
        self.dynamic_pricing = DynamicTokenPricingManager() if enable_dynamic_pricing else None
        self.hybrid_pipeline = QuantumClassicalHybridPipeline(quantum_module) if enable_hybrid_pipeline else None
        self.sustainability_dashboard = WorkSustainabilityDashboard() if enable_sustainability_dashboard else None
        
        # Work management
        self.active_works: Dict[str, EnhancedWorkContext] = {}
        self.completed_works: Dict[str, Dict[str, Any]] = {}
        self.failed_works: Dict[str, Dict[str, Any]] = {}
        self.workflow_dag = nx.DiGraph()
        self.resource_manager = ResourceReservationManager()
        self.work_metrics: Dict[str, List[Dict]] = defaultdict(list)
        self.sla_violations: List[Dict] = []
        self.tenant_contexts: Dict[str, Dict[str, Any]] = {}
        
        # Sustainability tracking
        self.total_carbon_savings_kg = 0.0
        self.total_helium_savings_l = 0.0
        self.sustainability_score = 0.0
        self.biomass_mobilized_count = 0
        
        # Pipelines
        self.pipelines = {
            'standard': self._standard_pipeline,
            'quantum_enhanced': self._quantum_pipeline,
            'helium_optimized': self._helium_pipeline,
            'meta_cognitive': self._meta_cognitive_pipeline,
            'batched': self._batched_pipeline,
            'checkpointed': self._checkpointed_pipeline,
            'bio_optimized': self._bio_optimized_pipeline,
            'hybrid_quantum_classical': self._hybrid_pipeline  # NEW
        }
        
        # Start background tasks
        self._start_background_tasks()
        
        logger.info(
            f"Enhanced Work Integrator v6.0.0 initialized: "
            f"bio_integration={self.enable_bio_integration}, "
            f"carbon_intensity={self.enable_carbon_intensity}, "
            f"predictive={self.enable_predictive}, "
            f"state_persistence={self.enable_state_persistence}, "
            f"dynamic_pricing={self.enable_dynamic_pricing}, "
            f"hybrid_pipeline={self.enable_hybrid_pipeline}, "
            f"sustainability_dashboard={self.enable_sustainability_dashboard}"
        )
    
    def _start_background_tasks(self):
        asyncio.create_task(self._cleanup_loop())
        asyncio.create_task(self._sla_monitor_loop())
        if self.enable_bio_integration:
            asyncio.create_task(self._biomass_mobilization_loop())
            asyncio.create_task(self._token_expiration_loop())
        if self.enable_carbon_intensity:
            asyncio.create_task(self._carbon_update_loop())
        if self.enable_state_persistence:
            asyncio.create_task(self._persistence_cleanup_loop())
        if self.enable_sustainability_dashboard:
            asyncio.create_task(self._dashboard_update_loop())
    
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
    # Bio-Inspired Methods (Enhanced)
    # ========================================================================
    
    async def _allocate_ecoatp_for_work(self, work_id: str, ecoatp_required: float, priority: int = 0) -> Tuple[bool, float]:
        if not self.token_manager:
            return True, 0.0
        
        # Get dynamic price if enabled
        if self.enable_dynamic_pricing and self.dynamic_pricing:
            price = await self.dynamic_pricing.get_price(
                WorkPriority(priority) if priority < len(WorkPriority) else WorkPriority.MEDIUM
            )
            ecoatp_required *= price
        
        if self.scheduler:
            success = self.scheduler.schedule_execution(task_id=work_id, eco_atp_required=ecoatp_required, priority=priority)
            if success:
                return True, ecoatp_required
        
        account_id = f"work_{work_id}"
        success, token_ids = self.token_manager.reserve_tokens(
            account_id=account_id,
            amount=ecoatp_required,
            consumer=EcoATPConsumer.EXPERT_EXECUTION
        )
        return success, ecoatp_required if success else 0.0
    
    def _store_work_as_biomass(self, work: Dict[str, Any], ecoatp_cost: float,
                               guarantee: GuaranteeLevel = GuaranteeLevel.SILVER) -> Optional[str]:
        if not self.biomass_storage:
            return None
        stored, token_id = self.biomass_storage.store_task(
            task_data=work,
            ecoatp_cost=ecoatp_cost,
            guarantee=guarantee,
            initial_tier=StorageTier.GLYCOGEN_QUEUE
        )
        if stored:
            logger.info(f"Work stored as biomass: {token_id}")
        return token_id if stored else None
    
    def _get_gradient_aware_priority(self, base_priority: WorkPriority) -> WorkPriority:
        if not self.gradient_manager:
            return base_priority
        carbon = self.gradient_manager.fields.get('carbon')
        opportunity = self.gradient_manager.fields.get('opportunity')
        priority_value = base_priority.value
        if carbon and carbon.gradient_strength > 0.7 and priority_value > 1:
            priority_value = min(5, priority_value + 1)
        if opportunity and opportunity.gradient_strength > 0.6 and priority_value > 0:
            priority_value = max(0, priority_value - 1)
        priority_map = {0: WorkPriority.CRITICAL, 1: WorkPriority.HIGH, 2: WorkPriority.MEDIUM,
                        3: WorkPriority.LOW, 4: WorkPriority.BACKGROUND, 5: WorkPriority.DEFERRABLE}
        return priority_map.get(priority_value, base_priority)
    
    def _recover_tokens_on_failure(self, work_id: str, completion_percentage: float) -> float:
        if not self.token_manager:
            return 0.0
        recovered = self.token_manager.recover_tokens(
            token_ids=[f"work_{work_id}"],
            completion_percentage=completion_percentage
        )
        if recovered > 0:
            logger.info(f"Recovered {recovered:.1f} Eco-ATP from failed work {work_id}")
        return recovered
    
    def _check_compartment_availability(self, expert_type: str) -> Tuple[bool, Optional[str]]:
        if not self.compartment_manager:
            return True, None
        compartment = self.compartment_manager.find_best_compartment(expert_type)
        if compartment and compartment.is_viable:
            return True, compartment.compartment_id
        return False, None
    
    def _get_ecoatp_cost_estimate(self, work: Dict[str, Any]) -> float:
        base_cost = work.get('complexity', 0.5) * 10.0
        if work.get('quantum_capable', False):
            base_cost *= 5.0
        data_size = work.get('meta_cognitive_state', {}).get('data_size_mb', 1.0)
        base_cost *= (1.0 + data_size / 1000.0)
        return base_cost
    
    # ========================================================================
    # Background Loops (Enhanced)
    # ========================================================================
    
    async def _biomass_mobilization_loop(self):
        while True:
            try:
                if not self.enable_bio_integration or not self.biomass_storage:
                    await asyncio.sleep(60); continue
                mobilize = False
                if self.gradient_manager:
                    carbon = self.gradient_manager.fields.get('carbon')
                    if carbon and carbon.gradient_strength < 0.3:
                        mobilize = True
                if mobilize:
                    stats = self.biomass_storage.get_storage_stats()
                    glycogen_count = stats.get('tiers', {}).get('glycogen_queue', 0)
                    if glycogen_count > 0:
                        mobilized = min(10, glycogen_count)
                        logger.info(f"Mobilizing {mobilized} tasks from biomass storage")
                        self.biomass_mobilized_count += mobilized
                await asyncio.sleep(30)
            except Exception as e:
                logger.error(f"Biomass mobilization error: {str(e)}")
                await asyncio.sleep(60)
    
    async def _token_expiration_loop(self):
        while True:
            try:
                if not self.enable_bio_integration or not self.token_manager:
                    await asyncio.sleep(300); continue
                now = datetime.utcnow()
                for work_id, work in list(self.active_works.items()):
                    if work.tokens_allocated > 0 and work.state == WorkState.TOKENS_ALLOCATED:
                        if work.started_at is None:
                            wait_time = (now - work.created_at).total_seconds()
                            if wait_time > 3600:
                                logger.warning(f"Work {work_id} token timeout - recovering tokens")
                                recovered = self._recover_tokens_on_failure(work_id, 0.1)
                                work.tokens_recovered = recovered
                                work.tokens_allocated = 0
                                work.transition_to(WorkState.FAILED)
                await asyncio.sleep(300)
            except Exception as e:
                logger.error(f"Token expiration error: {str(e)}")
                await asyncio.sleep(600)
    
    async def _carbon_update_loop(self):
        while True:
            try:
                await self.carbon_manager.update_carbon_intensity()
                await asyncio.sleep(self.carbon_manager.update_interval)
            except Exception as e:
                logger.error(f"Carbon update error: {str(e)}")
                await asyncio.sleep(60)
    
    async def _sla_monitor_loop(self):
        while True:
            try:
                if not self.enable_sla_tracking:
                    await asyncio.sleep(60); continue
                for work_id, work in list(self.active_works.items()):
                    if work.sla and work.sla.deadline:
                        remaining = work.sla.time_until_deadline()
                        if remaining is not None and remaining <= 0:
                            logger.warning(f"SLA deadline exceeded for {work_id}")
                            self._record_sla_violation(work, float('inf'))
                        elif remaining is not None and remaining < 30:
                            work.priority = WorkPriority.CRITICAL
                            # Update SLA health
                            if work.sla:
                                work.sla.update_health(0.8)
                await asyncio.sleep(5)
            except Exception as e:
                logger.error(f"SLA monitor error: {str(e)}")
                await asyncio.sleep(30)
    
    async def _cleanup_loop(self):
        while True:
            try:
                now = datetime.utcnow()
                max_age = timedelta(hours=24)
                for wid in [wid for wid, work in self.completed_works.items()
                           if now - datetime.fromisoformat(work['timestamp']) > max_age]:
                    del self.completed_works[wid]
                for wid in [wid for wid, work in self.failed_works.items()
                           if now - datetime.fromisoformat(work['timestamp']) > max_age]:
                    del self.failed_works[wid]
                # Cleanup persistence
                if self.enable_state_persistence and self.state_persistence:
                    # Clean up old states
                    pass
                await asyncio.sleep(300)
            except Exception as e:
                logger.error(f"Cleanup error: {str(e)}")
                await asyncio.sleep(60)
    
    async def _persistence_cleanup_loop(self):
        """Clean up old persisted states"""
        while True:
            try:
                if not self.enable_state_persistence or not self.state_persistence:
                    await asyncio.sleep(3600); continue
                # Implementation would clean up old files
                await asyncio.sleep(3600)
            except Exception as e:
                logger.error(f"Persistence cleanup error: {str(e)}")
                await asyncio.sleep(600)
    
    async def _dashboard_update_loop(self):
        """Periodically update sustainability dashboard"""
        while True:
            try:
                if self.enable_sustainability_dashboard and self.sustainability_dashboard:
                    # Update dashboard with current metrics
                    for work_id, work in list(self.completed_works.items())[-10:]:
                        metrics = {
                            'sustainability_score': work.get('sustainability_score', 0.5),
                            'carbon_intensity': work.get('carbon_intensity', 400),
                            'helium_usage': work.get('helium_usage', 0.5),
                            'token_efficiency': work.get('token_efficiency', 0.5),
                            'success_rate': 1.0 if work.get('success', False) else 0.0
                        }
                        await self.sustainability_dashboard.update_metrics(work_id, metrics)
                await asyncio.sleep(60)
            except Exception as e:
                logger.error(f"Dashboard update error: {str(e)}")
                await asyncio.sleep(120)
    
    # ========================================================================
    # Primary Work Processing (Enhanced)
    # ========================================================================
    
    async def process_work(
        self,
        work_request: Dict[str, Any],
        pipeline_type: str = 'standard',
        dependencies: Optional[List[str]] = None,
        tenant_id: str = "default"
    ) -> Dict[str, Any]:
        # Create context
        context = self._create_work_context(work_request, tenant_id)
        
        # Update carbon intensity
        if self.enable_carbon_intensity:
            carbon_data = await self.carbon_manager.update_carbon_intensity()
            context.meta_cognitive_state['carbon_intensity'] = carbon_data.get('intensity', 400)
            context.meta_cognitive_state['carbon_price'] = carbon_data.get('price_usd_per_ton', 50.0)
        
        # Get dynamic pricing
        if self.enable_dynamic_pricing and self.dynamic_pricing:
            context.dynamic_token_price = await self.dynamic_pricing.get_price(
                context.priority
            )
            # Update scarcity factors
            await self.dynamic_pricing.update_scarcity('carbon', context.carbon_zone / 10)
            await self.dynamic_pricing.update_scarcity('helium', context.helium_dependency)
        
        # Adjust priority based on gradients
        if self.enable_bio_integration:
            context.priority = self._get_gradient_aware_priority(context.priority)
        
        # Add to workflow DAG
        self.workflow_dag.add_node(context.task_id, work=context)
        if dependencies:
            for dep_id in dependencies:
                if dep_id in self.workflow_dag:
                    self.workflow_dag.add_edge(dep_id, context.task_id)
                    context.depends_on.append(dep_id)
        
        if not context.transition_to(WorkState.VALIDATED):
            return self._create_error_response(context, "Invalid state transition")
        
        # Check SLA deadline
        if self.enable_sla_tracking and context.sla and context.sla.is_deadline_critical():
            context.priority = WorkPriority.CRITICAL
        
        # Allocate Eco-ATP
        ecoatp_required = 0.0
        if self.enable_bio_integration:
            ecoatp_required = self._get_ecoatp_cost_estimate(work_request)
            if self.enable_dynamic_pricing and self.dynamic_pricing:
                ecoatp_required *= context.dynamic_token_price
            success, allocated = await self._allocate_ecoatp_for_work(
                context.task_id, ecoatp_required, context.priority.value
            )
            if success:
                context.tokens_allocated = allocated
                context.transition_to(WorkState.TOKENS_ALLOCATED)
                context.add_event('tokens_allocated', {'amount': allocated, 'source': 'eco_atp_pool'})
            else:
                biomass_token = self._store_work_as_biomass(work_request, ecoatp_required,
                    GuaranteeLevel.GOLD if context.priority in [WorkPriority.CRITICAL, WorkPriority.HIGH] else GuaranteeLevel.SILVER)
                if biomass_token:
                    context.biomass_storage_token = biomass_token
                    context.transition_to(WorkState.STORED_AS_BIOMASS)
                    return {'success': True, 'status': 'stored_as_biomass', 'task_id': context.task_id,
                            'biomass_token': biomass_token, 'ecoatp_required': ecoatp_required}
                else:
                    context.transition_to(WorkState.QUEUED)
        
        # Check compartment availability
        if self.enable_bio_integration:
            available, compartment_id = self._check_compartment_availability(work_request.get('task_type', 'general'))
            if available and compartment_id:
                context.compartment_id = compartment_id
            elif not available:
                biomass_token = self._store_work_as_biomass(work_request, ecoatp_required)
                if biomass_token:
                    context.biomass_storage_token = biomass_token
                    context.transition_to(WorkState.STORED_AS_BIOMASS)
                    return {'success': True, 'status': 'stored_as_biomass', 'task_id': context.task_id,
                            'biomass_token': biomass_token, 'reason': 'No viable compartment available'}
        
        # Execute pipeline
        if not context.transition_to(WorkState.EXECUTING):
            return self._create_error_response(context, "Cannot start execution")
        
        context.started_at = datetime.utcnow()
        context.execution_attempts += 1
        self.active_works[context.task_id] = context
        
        # Save initial state
        if self.enable_state_persistence and self.state_persistence:
            await self.state_persistence.save_work_state(context.task_id, context)
        
        try:
            pipeline = self.pipelines.get(pipeline_type, self._standard_pipeline)
            result = await pipeline(context)
            
            # Consume tokens on success
            if self.enable_bio_integration and context.tokens_allocated > 0:
                self.token_manager.consume_tokens(
                    token_ids=[f"work_{context.task_id}"],
                    consumer=EcoATPConsumer.EXPERT_EXECUTION,
                    operation_success=result.get('success', False)
                )
                context.tokens_consumed = context.tokens_allocated
            
            # Create checkpoint
            if self.enable_checkpointing:
                await self._create_checkpoint(context, result)
            
            # Complete work
            context.transition_to(WorkState.COMPLETED)
            context.completed_at = datetime.utcnow()
            self.workflow_dag.nodes[context.task_id]['completed'] = True
            
            # Calculate sustainability metrics
            carbon_savings = result.get('carbon_savings_kg', 0)
            self.total_carbon_savings_kg += carbon_savings
            context.carbon_savings_kg = carbon_savings
            context.sustainability_score = self._calculate_sustainability_score(context, result)
            
            # Update predictive analyzer
            if self.enable_predictive:
                self.predictive_analyzer.update_history({
                    'success_rate': 0.9,
                    'avg_latency_ms': (context.completed_at - context.started_at).total_seconds() * 1000,
                    'carbon_intensity': context.meta_cognitive_state.get('carbon_intensity', 400),
                    'token_efficiency': context.tokens_consumed / max(context.tokens_allocated, 1) if context.tokens_allocated > 0 else 0.5,
                    'workload': context.complexity
                })
                await self.predictive_analyzer.train_forecast_model()
            
            # Cross-domain knowledge transfer
            if self.enable_cross_domain:
                self.cross_domain_transfer.transfer_knowledge(
                    'work', 'energy',
                    'efficiency_strategies',
                    {'tokens_consumed': context.tokens_consumed, 'sustainability_score': self.sustainability_score}
                )
            
            # Update sustainability dashboard
            if self.enable_sustainability_dashboard and self.sustainability_dashboard:
                await self.sustainability_dashboard.update_metrics(
                    context.task_id,
                    {
                        'sustainability_score': context.sustainability_score,
                        'carbon_intensity': context.meta_cognitive_state.get('carbon_intensity', 400),
                        'helium_usage': context.helium_dependency,
                        'token_efficiency': context.tokens_consumed / max(context.tokens_allocated, 1) if context.tokens_allocated > 0 else 0.5,
                        'success_rate': 1.0
                    }
                )
            
            # Record completion
            result['sustainability_score'] = context.sustainability_score
            result['carbon_savings_kg'] = carbon_savings
            self.completed_works[context.task_id] = {
                'context': context, 'result': result,
                'timestamp': datetime.utcnow().isoformat(),
                'sustainability_score': context.sustainability_score,
                'carbon_savings_kg': carbon_savings
            }
            
            # Check SLA
            if self.enable_sla_tracking and context.sla:
                execution_time = (context.completed_at - context.started_at).total_seconds() * 1000
                if context.sla.is_violated(execution_time):
                    self._record_sla_violation(context, execution_time)
                else:
                    # Update SLA health
                    context.sla.update_health(0.1)
            
            # Update metrics
            self._update_work_metrics(context.task_id, result)
            
            # Add bio-inspired metadata
            result['bio_metadata'] = {
                'ecoatp_allocated': context.tokens_allocated,
                'ecoatp_consumed': context.tokens_consumed,
                'ecoatp_recovered': context.tokens_recovered,
                'compartment_id': context.compartment_id,
                'biomass_stored': context.biomass_storage_token is not None,
                'gradient_priority': context.priority.value,
                'bio_integration_active': self.enable_bio_integration,
                'sustainability_score': context.sustainability_score,
                'dynamic_token_price': context.dynamic_token_price,
                'state_persisted': self.enable_state_persistence
            }
            
            # Delete persisted state on success
            if self.enable_state_persistence and self.state_persistence:
                await self.state_persistence.delete_work_state(context.task_id)
            
            return result
            
        except Exception as e:
            logger.error(f"Work processing failed for {context.task_id}: {str(e)}")
            context.transition_to(WorkState.FAILED)
            
            if self.enable_bio_integration and context.tokens_allocated > 0:
                completion = 0.5 if context.checkpoints else 0.1
                recovered = self._recover_tokens_on_failure(context.task_id, completion)
                context.tokens_recovered = recovered
            
            if self.enable_rollback:
                await self._rollback_work(context)
            
            if context.can_retry():
                context.transition_to(WorkState.QUEUED)
                return await self.process_work(work_request, pipeline_type, dependencies, tenant_id)
            
            self.failed_works[context.task_id] = {
                'context': context, 'error': str(e),
                'timestamp': datetime.utcnow().isoformat()
            }
            return self._create_error_response(context, str(e))
        
        finally:
            self.active_works.pop(context.task_id, None)
    
    def _calculate_sustainability_score(self, context: EnhancedWorkContext, result: Dict) -> float:
        """Calculate sustainability score based on work execution"""
        carbon_factor = 1.0 - (context.meta_cognitive_state.get('carbon_intensity', 400) / 800)
        token_efficiency = context.tokens_consumed / max(context.tokens_allocated, 1) if context.tokens_allocated > 0 else 0.5
        success_factor = 1.0 if result.get('success', False) else 0.0
        
        # Include dynamic pricing if available
        price_factor = 1.0
        if self.enable_dynamic_pricing and self.dynamic_pricing:
            price_factor = 1.0 / (1.0 + context.dynamic_token_price * 0.1)
        
        score = (carbon_factor * 0.25 + token_efficiency * 0.25 + success_factor * 0.3 + price_factor * 0.2)
        return min(1.0, max(0.0, score))
    
    # ========================================================================
    # Pipelines (Enhanced)
    # ========================================================================
    
    async def _standard_pipeline(self, context: EnhancedWorkContext) -> Dict[str, Any]:
        if self.meta_cognitive:
            context = await self._apply_meta_cognition(context)
        
        symbolic_constraints = None
        if self.neuro_symbolic:
            symbolic_constraints = await self._extract_symbolic_constraints(context)
        
        dual_axis_context = self._build_dual_axis_context(context)
        
        if self.enable_bio_integration and self.gradient_manager:
            dual_axis_context['gradient_levels'] = self.gradient_manager.get_field_strengths()
        
        routing_result = self.router.route_and_execute(
            workload_profile=context.to_routing_context(),
            meta_cognitive_state=context.meta_cognitive_state,
            dual_axis_context=dual_axis_context,
            symbolic_constraints=symbolic_constraints
        )
        
        result = self._post_process_result(routing_result, context)
        result['work_metadata'] = {
            'task_id': context.task_id, 'work_type': context.work_type,
            'priority': context.priority.name, 'state': context.state.value,
            'attempt': context.execution_attempts, 'tenant_id': context.tenant_id,
            'compartment_id': context.compartment_id
        }
        return result
    
    async def _hybrid_pipeline(self, context: EnhancedWorkContext) -> Dict[str, Any]:
        """Quantum-classical hybrid pipeline"""
        if not self.enable_hybrid_pipeline or not self.hybrid_pipeline:
            return await self._standard_pipeline(context)
        
        # Use hybrid pipeline
        result = await self.hybrid_pipeline.execute(
            context,
            self._standard_pipeline,
            quantum_threshold=0.7
        )
        result['pipeline_type'] = 'hybrid_quantum_classical'
        return result
    
    async def _bio_optimized_pipeline(self, context: EnhancedWorkContext) -> Dict[str, Any]:
        gradient_levels = {}
        if self.gradient_manager:
            gradient_levels = self.gradient_manager.get_field_strengths()
        if gradient_levels.get('carbon', 0) > 0.7:
            context.meta_cognitive_state['prefer_efficiency'] = True
        if gradient_levels.get('opportunity', 0) > 0.6:
            context.meta_cognitive_state['exploration_budget'] = 0.2
        result = await self._standard_pipeline(context)
        result['bio_optimized'] = True
        result['gradient_levels'] = gradient_levels
        result['token_efficiency'] = context.tokens_consumed / max(context.tokens_allocated, 1) if context.tokens_allocated > 0 else 0
        return result
    
    async def _quantum_pipeline(self, context: EnhancedWorkContext) -> Dict[str, Any]:
        if not context.quantum_capable or not self.quantum_module:
            return await self._standard_pipeline(context)
        result = await self._standard_pipeline(context)
        result['quantum_enhanced'] = True
        return result
    
    async def _helium_pipeline(self, context: EnhancedWorkContext) -> Dict[str, Any]:
        if context.helium_dependency > 0.7:
            context.max_carbon_budget *= 0.5
        return await self._standard_pipeline(context)
    
    async def _meta_cognitive_pipeline(self, context: EnhancedWorkContext) -> Dict[str, Any]:
        result = await self._standard_pipeline(context)
        result['meta_cognitive_enhanced'] = True
        return result
    
    async def _batched_pipeline(self, context: EnhancedWorkContext) -> Dict[str, Any]:
        result = await self._standard_pipeline(context)
        result['batched'] = True
        result['batch_group'] = context.batch_group
        return result
    
    async def _checkpointed_pipeline(self, context: EnhancedWorkContext) -> Dict[str, Any]:
        if context.resume_from_checkpoint:
            checkpoint = next((c for c in context.checkpoints if c.checkpoint_id == context.resume_from_checkpoint), None)
            if checkpoint:
                context.transition_to(WorkState.RESUMED)
                context.add_event('resumed_from_checkpoint', {'checkpoint_id': checkpoint.checkpoint_id})
        result = await self._standard_pipeline(context)
        await self._create_checkpoint(context, result)
        return result
    
    # ========================================================================
    # Helper Methods (Enhanced)
    # ========================================================================
    
    async def _apply_meta_cognition(self, context: EnhancedWorkContext) -> EnhancedWorkContext:
        if not self.meta_cognitive:
            return context
        try:
            meta_state = await self.meta_cognitive.get_state(context.task_id)
            context.meta_cognitive_state.update({
                'historical_success_rate': meta_state.get('success_rate', 0.9),
                'carbon_budget_remaining': meta_state.get('carbon_budget', context.max_carbon_budget),
                'helium_budget_remaining': meta_state.get('helium_budget', context.max_helium_budget),
                'latency_budget_ms': meta_state.get('latency_budget', context.max_latency_ms),
                'preferred_experts': meta_state.get('preferred_experts', []),
                'avoided_experts': meta_state.get('avoided_experts', [])
            })
        except Exception as e:
            logger.warning(f"Meta-cognitive processing failed: {str(e)}")
        return context
    
    async def _extract_symbolic_constraints(self, context: EnhancedWorkContext) -> Optional[Dict[str, Any]]:
        if not self.neuro_symbolic:
            return None
        try:
            return await self.neuro_symbolic.query_graph(
                task_type=context.work_type,
                carbon_zone=context.carbon_zone,
                helium_dependency=context.helium_dependency
            )
        except Exception:
            return None
    
    def _build_dual_axis_context(self, context: EnhancedWorkContext) -> Dict[str, Any]:
        return {
            'carbon_zone': context.carbon_zone,
            'helium_scarcity': context.helium_dependency,
            'carbon_weight': 0.6,
            'helium_weight': 0.4,
            'execution_constraints': {
                'max_carbon': context.max_carbon_budget,
                'max_helium': context.max_helium_budget,
                'max_latency': context.max_latency_ms,
                'max_ecoatp': context.max_ecoatp_budget
            }
        }
    
    def _post_process_result(self, routing_result: Dict[str, Any], context: EnhancedWorkContext) -> Dict[str, Any]:
        routing_result['work_context'] = {
            'task_id': context.task_id,
            'task_type': context.work_type,
            'priority': context.priority.name,
            'helium_dependency': context.helium_dependency
        }
        routing_result['compliance'] = {
            'carbon_compliant': True,
            'helium_compliant': True,
            'latency_compliant': True,
            'ecoatp_compliant': context.tokens_consumed <= context.max_ecoatp_budget
        }
        routing_result['carbon_savings_kg'] = 0.01 * (1 - context.carbon_zone / 10)
        return routing_result
    
    async def _create_checkpoint(self, context: EnhancedWorkContext, result: Dict[str, Any]):
        if not self.enable_checkpointing:
            return
        checkpoint = WorkCheckpoint(
            checkpoint_id=f"ckpt_{context.task_id}_{datetime.utcnow().timestamp()}",
            work_id=context.task_id,
            state=context.state,
            progress=0.5,
            intermediate_results=result,
            resource_usage={
                'carbon_kg': result.get('final_plan', {}).get('aggregate_carbon_kg', 0),
                'helium_units': result.get('final_plan', {}).get('aggregate_helium', 0)
            },
            ecoatp_consumed=context.tokens_consumed,
            carbon_footprint_at_checkpoint=context.meta_cognitive_state.get('carbon_intensity', 400),
            helium_usage_at_checkpoint=context.helium_dependency
        )
        context.add_checkpoint(checkpoint)
        if context.state == WorkState.EXECUTING:
            context.transition_to(WorkState.CHECKPOINTED)
        
        # Save checkpoint to persistence
        if self.enable_state_persistence and self.state_persistence:
            await self.state_persistence.save_work_state(context.task_id, context)
    
    async def _rollback_work(self, context: EnhancedWorkContext):
        if not self.enable_rollback:
            return
        context.transition_to(WorkState.ROLLING_BACK)
        for action in reversed(context.compensation_actions):
            try:
                await action() if asyncio.iscoroutinefunction(action) else action()
            except Exception as e:
                logger.error(f"Compensation action failed: {str(e)}")
        context.transition_to(WorkState.ROLLED_BACK)
    
    def _create_work_context(self, request: Dict[str, Any], tenant_id: str = "default") -> EnhancedWorkContext:
        sla = None
        if request.get('sla_level'):
            sla_level = SLALevel(request['sla_level'])
            sla_configs = {
                SLALevel.PLATINUM: (10, 0.9999), SLALevel.GOLD: (50, 0.999),
                SLALevel.SILVER: (200, 0.99), SLALevel.BRONZE: (1000, 0.95),
                SLALevel.BEST_EFFORT: (5000, 0.0)
            }
            max_latency, min_availability = sla_configs.get(sla_level, (1000, 0.95))
            sla = WorkSLA(
                level=sla_level,
                max_latency_ms=request.get('max_latency_ms', max_latency),
                min_availability=min_availability,
                max_carbon_kg=request.get('max_carbon_budget'),
                max_helium_units=request.get('max_helium_budget'),
                max_ecoatp_cost=request.get('max_ecoatp_budget'),
                deadline=request.get('deadline')
            )
        return EnhancedWorkContext(
            task_id=request.get('task_id', str(uuid.uuid4())),
            work_type=request.get('task_type', 'inference'),
            priority=WorkPriority[request.get('priority', 'MEDIUM').upper()],
            sla=sla,
            complexity=request.get('complexity', 0.5),
            estimated_duration_ms=request.get('estimated_duration_ms', 100),
            helium_dependency=request.get('helium_dependency', 0.0),
            meta_cognitive_state=request.get('meta_cognitive_state', {}),
            carbon_zone=request.get('carbon_zone', 0),
            quantum_capable=request.get('quantum_capable', False),
            max_carbon_budget=request.get('max_carbon_budget', float('inf')),
            max_helium_budget=request.get('max_helium_budget', float('inf')),
            max_latency_ms=request.get('max_latency_ms', 1000.0),
            max_ecoatp_budget=request.get('max_ecoatp_budget', float('inf')),
            can_batch=request.get('can_batch', True),
            tenant_id=tenant_id
        )
    
    def _create_error_response(self, context: EnhancedWorkContext, error: str) -> Dict[str, Any]:
        context.add_event("error", {'error': error})
        return {'success': False, 'error': error, 'task_id': context.task_id,
                'state': context.state.value, 'attempt': context.execution_attempts,
                'can_retry': context.can_retry()}
    
    def _update_work_metrics(self, task_id: str, result: Dict[str, Any]):
        self.work_metrics[task_id].append({
            'timestamp': datetime.utcnow().isoformat(),
            'success': result.get('success', False),
            'action': result.get('final_plan', {}).get('action', 'unknown'),
            'execution_time': result.get('execution_time_ms', 0)
        })
    
    def _record_sla_violation(self, context: EnhancedWorkContext, actual_latency_ms: float):
        violation = {
            'work_id': context.task_id,
            'sla_level': context.sla.level.value,
            'max_latency_ms': context.sla.max_latency_ms,
            'actual_latency_ms': actual_latency_ms,
            'violated_at': datetime.utcnow().isoformat(),
            'tenant_id': context.tenant_id
        }
        self.sla_violations.append(violation)
        context.sla.violations += 1
        context.sla.update_health(0.9)
    
    # ========================================================================
    # Statistics (Enhanced)
    # ========================================================================
    
    def get_work_statistics(self) -> Dict[str, Any]:
        stats = {
            'total_works': len(self.completed_works) + len(self.failed_works) + len(self.active_works),
            'active_works': len(self.active_works),
            'completed_works': len(self.completed_works),
            'failed_works': len(self.failed_works),
            'success_rate': len(self.completed_works) / max(len(self.completed_works) + len(self.failed_works), 1),
            'sla_violations': len(self.sla_violations),
            'bio_integration_active': self.enable_bio_integration,
            'carbon_intensity_active': self.enable_carbon_intensity,
            'predictive_active': self.enable_predictive,
            'cross_domain_active': self.enable_cross_domain,
            'sustainability_scoring_active': self.enable_sustainability_scoring,
            'state_persistence_active': self.enable_state_persistence,
            'dynamic_pricing_active': self.enable_dynamic_pricing,
            'hybrid_pipeline_active': self.enable_hybrid_pipeline,
            'sustainability_dashboard_active': self.enable_sustainability_dashboard,
            'biomass_mobilized': self.biomass_mobilized_count,
            'total_carbon_savings_kg': self.total_carbon_savings_kg,
            'sustainability_score': self.sustainability_score,
            'pipeline_distribution': {
                pipeline: sum(1 for w in self.completed_works.values()
                            if w['result'].get('pipeline_type') == pipeline)
                for pipeline in self.pipelines.keys()
            }
        }
        
        if self.enable_dynamic_pricing and self.dynamic_pricing:
            stats['pricing_stats'] = self.dynamic_pricing.get_pricing_stats()
        
        if self.enable_hybrid_pipeline and self.hybrid_pipeline:
            stats['hybrid_stats'] = self.hybrid_pipeline.get_hybrid_stats()
        
        if self.enable_sustainability_dashboard and self.sustainability_dashboard:
            stats['dashboard_stats'] = {
                'total_works_tracked': len(self.sustainability_dashboard.scores),
                'history_samples': len(self.sustainability_dashboard.history)
            }
        
        return stats
    
    def get_work_state(self, task_id: str) -> Optional[Dict[str, Any]]:
        if task_id in self.active_works:
            work = self.active_works[task_id]
            return {'task_id': task_id, 'state': work.state.value, 'priority': work.priority.name,
                    'tokens_allocated': work.tokens_allocated, 'tokens_consumed': work.tokens_consumed,
                    'biomass_stored': work.biomass_storage_token is not None,
                    'compartment_id': work.compartment_id, 'sustainability_score': work.sustainability_score,
                    'dynamic_token_price': work.dynamic_token_price}
        if task_id in self.completed_works:
            return {'task_id': task_id, 'state': 'completed', 
                    'sustainability_score': self.completed_works[task_id].get('sustainability_score', 0)}
        if task_id in self.failed_works:
            return {'task_id': task_id, 'state': 'failed'}
        return None
    
    def cancel_work(self, task_id: str) -> bool:
        if task_id in self.active_works:
            work = self.active_works[task_id]
            work.transition_to(WorkState.CANCELLED)
            if work.tokens_allocated > 0:
                self._recover_tokens_on_failure(task_id, 0.0)
            del self.active_works[task_id]
            return True
        return False
    
    def get_sustainability_report(self) -> Dict[str, Any]:
        report = {
            'timestamp': datetime.utcnow().isoformat(),
            'sustainability_score': self.sustainability_score,
            'total_carbon_savings_kg': self.total_carbon_savings_kg,
            'total_helium_savings_l': self.total_helium_savings_l,
            'active_works': len(self.active_works),
            'bio_integration_active': self.enable_bio_integration,
            'predictive_forecast': self.predictive_analyzer.predict_work_trend() if self.enable_predictive else {},
            'recommendations': self._generate_sustainability_recommendations()
        }
        
        if self.enable_sustainability_dashboard and self.sustainability_dashboard:
            # Get dashboard data
            dashboard_data = asyncio.run(self.sustainability_dashboard.get_dashboard_data())
            report['dashboard'] = dashboard_data
        
        if self.enable_dynamic_pricing and self.dynamic_pricing:
            report['pricing_stats'] = self.dynamic_pricing.get_pricing_stats()
        
        if self.enable_hybrid_pipeline and self.hybrid_pipeline:
            report['hybrid_stats'] = self.hybrid_pipeline.get_hybrid_stats()
        
        return report
    
    def _generate_sustainability_recommendations(self) -> List[str]:
        recommendations = []
        if self.sustainability_score < 0.5:
            recommendations.append("Increase token efficiency for better sustainability")
            recommendations.append("Optimize carbon-aware scheduling")
        if self.total_carbon_savings_kg < 10:
            recommendations.append("Implement more aggressive carbon reduction strategies")
        if self.enable_bio_integration and self.biomass_mobilized_count < 10:
            recommendations.append("Increase biomass mobilization for backlog processing")
        
        # Add dynamic pricing recommendations
        if self.enable_dynamic_pricing and self.dynamic_pricing:
            pricing_stats = self.dynamic_pricing.get_pricing_stats()
            for resource, factor in pricing_stats.get('scarcity_factors', {}).items():
                if factor > 1.5:
                    recommendations.append(f"High scarcity for {resource} - consider reducing usage")
        
        return recommendations or ["Work integration sustainability is on track"]
    
    async def shutdown(self):
        logger.info("Shutting down Enhanced Work Integrator")
        await self.carbon_manager.close()
        logger.info("Shutdown complete")
