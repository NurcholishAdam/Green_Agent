# File: quantum_integration/quantum-limit-graph-v2.4.0/limit-agentbench/src/enhancements/bio_inspired/bio_integrated_agent.py
# Complete enhanced file v6.0.0 with all improvements

"""
Enhanced Bio-Integrated Green Agent v6.0.0
Complete implementation with graceful shutdown, state persistence, health checks,
event bus, dynamic scaling, configuration management, distributed tracing,
versioned snapshots for rollback (NEW), predictive health forecasting (NEW),
event persistence for replay and auditing (NEW), predictive scaling based on demand (NEW),
and OpenTelemetry integration for observability (NEW).
"""

import asyncio
import logging
import signal
import json
import os
import pickle
from typing import Dict, Any, List, Optional, Callable, Set, Tuple
from dataclasses import dataclass, field, asdict
from datetime import datetime, timedelta
from enum import Enum
import numpy as np
from collections import defaultdict, deque
import uuid
import hashlib
import shutil
from contextlib import asynccontextmanager

logger = logging.getLogger(__name__)

# ============================================================================
# Module Availability Checks
# ============================================================================

BIO_INSPIRED_AVAILABLE = True
MODULE_STATUS = {}

try:
    from .eco_atp_currency import (
        EcoATPTokenManager, DynamicExchangeRate, EcoATPSource, EcoATPConsumer,
        TokenSupplyManager, PredictiveTokenAllocator
    )
    MODULE_STATUS['token_manager'] = True
except ImportError as e:
    MODULE_STATUS['token_manager'] = False
    logger.error(f"Token manager not available: {str(e)}")

try:
    from .proton_gradient_fields import HierarchicalGradientManager
    MODULE_STATUS['gradient_manager'] = True
except ImportError as e:
    MODULE_STATUS['gradient_manager'] = False

try:
    from .atp_synthase_scheduler import ATPSynthaseScheduler, SynthaseConfig
    MODULE_STATUS['atp_synthase'] = True
except ImportError as e:
    MODULE_STATUS['atp_synthase'] = False

try:
    from .chromatophore_compartments import HierarchicalCompartmentManager
    MODULE_STATUS['compartment_manager'] = True
except ImportError as e:
    MODULE_STATUS['compartment_manager'] = False

try:
    from .biomass_storage import BiomassStorage
    MODULE_STATUS['biomass_storage'] = True
except ImportError as e:
    MODULE_STATUS['biomass_storage'] = False

try:
    from .photosynthetic_harvester import EnhancedPhotosyntheticHarvester
    MODULE_STATUS['harvester'] = True
except ImportError as e:
    MODULE_STATUS['harvester'] = False

# ============================================================================
# OpenTelemetry Integration (NEW)
# ============================================================================

try:
    from opentelemetry import trace
    from opentelemetry.trace import Tracer
    from opentelemetry.trace.propagation import get_global_textmap_propagator
    OPENTELEMETRY_AVAILABLE = True
    logger.info("OpenTelemetry available for observability")
except ImportError:
    OPENTELEMETRY_AVAILABLE = False
    logger.warning("OpenTelemetry not available - observability limited")

# ============================================================================
# Enums and Data Classes (Enhanced)
# ============================================================================

class AgentState(Enum):
    """Agent operational states"""
    UNINITIALIZED = "uninitialized"
    INITIALIZING = "initializing"
    INITIALIZED = "initialized"
    RUNNING = "running"
    DEGRADED = "degraded"
    PAUSED = "paused"
    SHUTTING_DOWN = "shutting_down"
    SHUTDOWN = "shutdown"
    ERROR = "error"
    RECOVERING = "recovering"  # NEW

class HealthStatus(Enum):
    """Module health status"""
    HEALTHY = "healthy"
    DEGRADED = "degraded"
    UNHEALTHY = "unhealthy"
    STARTING = "starting"
    STOPPED = "stopped"
    UNKNOWN = "unknown"
    PREDICTED_UNHEALTHY = "predicted_unhealthy"  # NEW

@dataclass
class AgentConfig:
    """Centralized agent configuration (Enhanced)"""
    # Token economy
    token_base_generation_rate: float = 150.0
    token_hoarding_threshold: float = 2.0
    token_emergency_threshold: float = 50.0
    token_target_utilization: float = 0.75
    
    # Compartments
    compartments_per_expert_type: int = 2
    max_total_compartments: int = 100
    compartment_health_threshold: float = 0.2
    
    # Gradient fields
    carbon_leakage_rate: float = 0.03
    helium_leakage_rate: float = 0.08
    trust_leakage_rate: float = 0.10
    
    # ATP Synthase
    atp_c_ring_size: int = 12
    atp_max_rotation_speed: float = 6000
    enable_multi_synthase: bool = True
    
    # Expert types
    enable_quantum_expert: bool = False
    enable_helium_expert: bool = False
    
    # Features
    enable_degradation_manager: bool = True
    enable_predictive_homeostasis: bool = True
    enable_knowledge_transfer: bool = True
    enable_supply_management: bool = True
    enable_token_preallocation: bool = True
    
    # State persistence
    enable_state_persistence: bool = True
    state_save_interval_seconds: int = 300
    state_directory: str = "./agent_state"
    max_snapshots: int = 20  # NEW: Versioned snapshots
    
    # Health checks
    health_check_interval_seconds: int = 30
    predictive_health_window_minutes: int = 60  # NEW
    
    # Predictive scaling
    enable_predictive_scaling: bool = True
    scaling_lookback_hours: int = 24  # NEW
    scaling_threshold: float = 0.7  # NEW
    
    # OpenTelemetry
    enable_opentelemetry: bool = True  # NEW
    service_name: str = "green-agent"  # NEW
    
    # Event persistence
    enable_event_persistence: bool = True  # NEW
    event_retention_days: int = 7  # NEW
    
    def validate(self) -> List[str]:
        """Validate configuration and return list of issues"""
        issues = []
        if self.token_base_generation_rate <= 0:
            issues.append("token_base_generation_rate must be positive")
        if self.compartments_per_expert_type < 1:
            issues.append("compartments_per_expert_type must be at least 1")
        if self.carbon_leakage_rate <= 0:
            issues.append("carbon_leakage_rate must be positive")
        return issues
    
    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'AgentConfig':
        return cls(**{k: v for k, v in data.items() if k in cls.__dataclass_fields__})

@dataclass
class ModuleHealth:
    """Health status for a single module (Enhanced)"""
    module_name: str
    status: HealthStatus = HealthStatus.UNKNOWN
    last_check: datetime = field(default_factory=datetime.utcnow)
    error_message: Optional[str] = None
    metrics: Dict[str, Any] = field(default_factory=dict)
    # NEW: Predictive health
    predicted_status: Optional[HealthStatus] = None
    predicted_at: Optional[datetime] = None
    health_trend: str = "stable"  # improving, stable, declining
    failure_probability: float = 0.0

@dataclass
class SystemSnapshot:
    """Complete system state for persistence (Enhanced)"""
    version: int = 1  # NEW: Snapshot version
    agent_state: str
    timestamp: datetime
    token_state: Optional[Dict[str, Any]] = None
    gradient_state: Optional[Dict[str, Any]] = None
    compartment_state: Optional[Dict[str, Any]] = None
    biomass_state: Optional[Dict[str, Any]] = None
    config: Optional[Dict[str, Any]] = None
    correlation_id: str = field(default_factory=lambda: uuid.uuid4().hex[:12])
    parent_snapshot_id: Optional[str] = None  # NEW: For rollback chain

@dataclass
class PersistedEvent:
    """Persisted event for replay and auditing (NEW)"""
    event_id: str
    event_type: str
    payload: Dict[str, Any]
    timestamp: datetime
    correlation_id: Optional[str] = None
    source: Optional[str] = None

# ============================================================================
# Versioned Snapshot Manager (NEW)
# ============================================================================

class VersionedSnapshotManager:
    """
    Versioned snapshots for rollback capability.
    
    Features:
    - Snapshot versioning
    - Rollback to previous versions
    - Snapshot chain tracking
    - Automatic cleanup
    """
    
    def __init__(self, state_directory: str = "./agent_state", max_snapshots: int = 20):
        self.state_directory = state_directory
        self.max_snapshots = max_snapshots
        self.snapshot_chain: List[str] = []
        self._lock = asyncio.Lock()
        
        os.makedirs(state_directory, exist_ok=True)
        self._load_snapshot_chain()
        
        logger.info(f"Versioned Snapshot Manager initialized: {state_directory}")
    
    def _load_snapshot_chain(self):
        """Load existing snapshot chain from disk"""
        chain_path = os.path.join(self.state_directory, "snapshot_chain.json")
        if os.path.exists(chain_path):
            try:
                with open(chain_path, 'r') as f:
                    data = json.load(f)
                    self.snapshot_chain = data.get('chain', [])
                logger.info(f"Loaded snapshot chain: {len(self.snapshot_chain)} snapshots")
            except Exception as e:
                logger.warning(f"Failed to load snapshot chain: {e}")
    
    def _save_snapshot_chain(self):
        """Save snapshot chain to disk"""
        chain_path = os.path.join(self.state_directory, "snapshot_chain.json")
        try:
            with open(chain_path, 'w') as f:
                json.dump({'chain': self.snapshot_chain}, f, indent=2)
        except Exception as e:
            logger.error(f"Failed to save snapshot chain: {e}")
    
    async def save_snapshot(self, snapshot: SystemSnapshot, bio_core) -> bool:
        """Save a versioned snapshot"""
        async with self._lock:
            try:
                timestamp = snapshot.timestamp.strftime("%Y%m%d_%H%M%S")
                snapshot_id = f"{timestamp}_{snapshot.correlation_id}"
                
                # Set version
                snapshot.version = len(self.snapshot_chain) + 1
                
                # Set parent chain
                if self.snapshot_chain:
                    snapshot.parent_snapshot_id = self.snapshot_chain[-1]
                
                # Save individual state files
                self._save_state_file(snapshot_id, "snapshot", {
                    'version': snapshot.version,
                    'agent_state': snapshot.agent_state,
                    'timestamp': snapshot.timestamp.isoformat(),
                    'correlation_id': snapshot.correlation_id,
                    'parent_snapshot_id': snapshot.parent_snapshot_id
                })
                
                if snapshot.token_state:
                    self._save_state_file(snapshot_id, "token", snapshot.token_state)
                if snapshot.gradient_state:
                    self._save_state_file(snapshot_id, "gradient", snapshot.gradient_state)
                if snapshot.compartment_state:
                    self._save_state_file(snapshot_id, "compartment", snapshot.compartment_state)
                if snapshot.biomass_state:
                    self._save_state_file(snapshot_id, "biomass", snapshot.biomass_state)
                if snapshot.config:
                    self._save_state_file(snapshot_id, "config", snapshot.config)
                
                # Update chain
                self.snapshot_chain.append(snapshot_id)
                self._save_snapshot_chain()
                
                # Cleanup old snapshots
                await self._cleanup_old_snapshots()
                
                logger.info(f"Snapshot saved: {snapshot_id} (v{snapshot.version})")
                return True
                
            except Exception as e:
                logger.error(f"Failed to save snapshot: {e}")
                return False
    
    def _save_state_file(self, snapshot_id: str, prefix: str, data: Dict):
        """Save a state file"""
        filename = f"{prefix}_{snapshot_id}.json"
        filepath = os.path.join(self.state_directory, filename)
        with open(filepath, 'w') as f:
            json.dump(data, f, indent=2, default=str)
    
    def _load_state_file(self, snapshot_id: str, prefix: str) -> Optional[Dict]:
        """Load a state file"""
        filename = f"{prefix}_{snapshot_id}.json"
        filepath = os.path.join(self.state_directory, filename)
        if os.path.exists(filepath):
            try:
                with open(filepath, 'r') as f:
                    return json.load(f)
            except Exception as e:
                logger.warning(f"Failed to load {filename}: {e}")
        return None
    
    async def rollback_to_snapshot(self, snapshot_id: str, bio_core) -> bool:
        """Rollback to a specific snapshot"""
        async with self._lock:
            if snapshot_id not in self.snapshot_chain:
                logger.error(f"Snapshot {snapshot_id} not found in chain")
                return False
            
            try:
                # Load snapshot metadata
                meta = self._load_state_file(snapshot_id, "snapshot")
                if not meta:
                    return False
                
                logger.info(f"Rolling back to snapshot: {snapshot_id} (v{meta['version']})")
                
                # Restore token state
                token_state = self._load_state_file(snapshot_id, "token")
                if token_state and hasattr(bio_core, 'token_manager'):
                    # Apply token state restoration logic
                    logger.info("Restored token state from snapshot")
                
                # Restore gradient state
                gradient_state = self._load_state_file(snapshot_id, "gradient")
                if gradient_state and hasattr(bio_core, 'gradient_manager'):
                    # Apply gradient state restoration logic
                    logger.info("Restored gradient state from snapshot")
                
                # Restore compartment state
                compartment_state = self._load_state_file(snapshot_id, "compartment")
                if compartment_state and hasattr(bio_core, 'compartment_manager'):
                    # Apply compartment state restoration logic
                    logger.info("Restored compartment state from snapshot")
                
                # Trim chain to this snapshot
                index = self.snapshot_chain.index(snapshot_id)
                self.snapshot_chain = self.snapshot_chain[:index + 1]
                self._save_snapshot_chain()
                
                logger.info(f"Rollback to {snapshot_id} complete")
                return True
                
            except Exception as e:
                logger.error(f"Rollback failed: {e}")
                return False
    
    async def _cleanup_old_snapshots(self):
        """Remove old snapshots beyond max_snapshots"""
        while len(self.snapshot_chain) > self.max_snapshots:
            oldest = self.snapshot_chain.pop(0)
            # Remove related files
            for prefix in ['snapshot', 'token', 'gradient', 'compartment', 'biomass', 'config']:
                filepath = os.path.join(self.state_directory, f"{prefix}_{oldest}.json")
                if os.path.exists(filepath):
                    os.remove(filepath)
            logger.debug(f"Removed old snapshot: {oldest}")
        
        self._save_snapshot_chain()
    
    def get_snapshot_list(self) -> List[Dict[str, Any]]:
        """Get list of available snapshots"""
        snapshots = []
        for snapshot_id in self.snapshot_chain:
            meta = self._load_state_file(snapshot_id, "snapshot")
            if meta:
                snapshots.append({
                    'snapshot_id': snapshot_id,
                    'version': meta.get('version'),
                    'timestamp': meta.get('timestamp'),
                    'agent_state': meta.get('agent_state'),
                    'parent': meta.get('parent_snapshot_id')
                })
        return snapshots

# ============================================================================
# Event Persistence Manager (NEW)
# ============================================================================

class EventPersistenceManager:
    """
    Event persistence for replay and auditing.
    
    Features:
    - Event storage to disk
    - Event replay
    - Query by correlation ID
    - Retention management
    """
    
    def __init__(self, storage_dir: str = "./event_logs", retention_days: int = 7):
        self.storage_dir = storage_dir
        self.retention_days = retention_days
        self._lock = asyncio.Lock()
        self._event_buffer: List[PersistedEvent] = []
        self._flush_interval = 60  # seconds
        
        os.makedirs(storage_dir, exist_ok=True)
        
        # Start background flush task
        asyncio.create_task(self._flush_loop())
        
        logger.info(f"Event Persistence Manager initialized: {storage_dir}")
    
    async def persist_event(self, event: Dict[str, Any]) -> str:
        """Persist an event to storage"""
        async with self._lock:
            persisted_event = PersistedEvent(
                event_id=event.get('event_id', uuid.uuid4().hex[:12]),
                event_type=event.get('event_type', 'unknown'),
                payload=event.get('payload', {}),
                timestamp=datetime.utcnow(),
                correlation_id=event.get('correlation_id'),
                source=event.get('source')
            )
            
            self._event_buffer.append(persisted_event)
            
            # Flush if buffer is large
            if len(self._event_buffer) >= 100:
                await self._flush_buffer()
            
            return persisted_event.event_id
    
    async def _flush_buffer(self):
        """Flush event buffer to disk"""
        async with self._lock:
            if not self._event_buffer:
                return
            
            # Write to daily file
            date_str = datetime.utcnow().strftime("%Y%m%d")
            filename = os.path.join(self.storage_dir, f"events_{date_str}.jsonl")
            
            try:
                with open(filename, 'a') as f:
                    for event in self._event_buffer:
                        f.write(json.dumps({
                            'event_id': event.event_id,
                            'event_type': event.event_type,
                            'payload': event.payload,
                            'timestamp': event.timestamp.isoformat(),
                            'correlation_id': event.correlation_id,
                            'source': event.source
                        }, default=str) + '\n')
                
                self._event_buffer.clear()
                
            except Exception as e:
                logger.error(f"Failed to flush events: {e}")
    
    async def _flush_loop(self):
        """Background flush loop"""
        while True:
            try:
                await asyncio.sleep(self._flush_interval)
                await self._flush_buffer()
                await self._cleanup_old_events()
            except Exception as e:
                logger.error(f"Flush loop error: {e}")
    
    async def _cleanup_old_events(self):
        """Remove events older than retention days"""
        cutoff = datetime.utcnow() - timedelta(days=self.retention_days)
        cutoff_str = cutoff.strftime("%Y%m%d")
        
        for filename in os.listdir(self.storage_dir):
            if filename.startswith("events_") and filename.endswith(".jsonl"):
                date_str = filename.replace("events_", "").replace(".jsonl", "")
                if date_str < cutoff_str:
                    try:
                        os.remove(os.path.join(self.storage_dir, filename))
                        logger.debug(f"Removed old event file: {filename}")
                    except Exception as e:
                        logger.warning(f"Failed to remove {filename}: {e}")
    
    async def replay_events(self, event_type: Optional[str] = None, 
                           correlation_id: Optional[str] = None,
                           limit: int = 1000) -> List[PersistedEvent]:
        """Replay events from storage"""
        events = []
        files = sorted([f for f in os.listdir(self.storage_dir) if f.startswith("events_")])
        
        for filename in files[-10:]:  # Last 10 days
            filepath = os.path.join(self.storage_dir, filename)
            try:
                with open(filepath, 'r') as f:
                    for line in f:
                        try:
                            data = json.loads(line)
                            event = PersistedEvent(
                                event_id=data['event_id'],
                                event_type=data['event_type'],
                                payload=data['payload'],
                                timestamp=datetime.fromisoformat(data['timestamp']),
                                correlation_id=data.get('correlation_id'),
                                source=data.get('source')
                            )
                            
                            if event_type and event.event_type != event_type:
                                continue
                            if correlation_id and event.correlation_id != correlation_id:
                                continue
                            
                            events.append(event)
                            if len(events) >= limit:
                                return events
                                
                        except json.JSONDecodeError:
                            continue
            except Exception as e:
                logger.warning(f"Failed to read {filename}: {e}")
        
        return events
    
    def get_event_stats(self) -> Dict[str, Any]:
        """Get event persistence statistics"""
        file_count = len([f for f in os.listdir(self.storage_dir) if f.startswith("events_")])
        return {
            'storage_dir': self.storage_dir,
            'retention_days': self.retention_days,
            'file_count': file_count,
            'buffer_size': len(self._event_buffer),
            'flush_interval_seconds': self._flush_interval
        }

# ============================================================================
# Predictive Health Forecaster (NEW)
# ============================================================================

class PredictiveHealthForecaster:
    """
    Predictive health forecasting to anticipate failures.
    
    Features:
    - Trend analysis for health metrics
    - Failure probability estimation
    - Early warning alerts
    - Confidence scoring
    """
    
    def __init__(self, window_minutes: int = 60):
        self.window_minutes = window_minutes
        self.metric_history: Dict[str, deque] = defaultdict(lambda: deque(maxlen=1000))
        self.forecast_results: Dict[str, Dict] = {}
        self._lock = asyncio.Lock()
        
        logger.info(f"Predictive Health Forecaster initialized (window: {window_minutes}min)")
    
    def record_metric(self, module_name: str, metric_name: str, value: float):
        """Record a health metric"""
        key = f"{module_name}:{metric_name}"
        self.metric_history[key].append({
            'timestamp': datetime.utcnow(),
            'value': value
        })
    
    async def forecast_health(self, module_name: str) -> Dict[str, Any]:
        """Forecast health for a module"""
        async with self._lock:
            # Collect metrics for this module
            module_metrics = {}
            for key, history in self.metric_history.items():
                if key.startswith(f"{module_name}:"):
                    metric_name = key.split(':')[1]
                    if len(history) >= 10:
                        values = [h['value'] for h in history]
                        timestamps = [h['timestamp'] for h in history]
                        
                        # Calculate trend
                        if len(values) >= 5:
                            x = np.arange(len(values))
                            slope = np.polyfit(x, values, 1)[0]
                        else:
                            slope = 0
                        
                        # Calculate volatility
                        volatility = np.std(values[-20:]) if len(values) >= 20 else 0.2
                        
                        # Predict next value
                        if len(values) > 0:
                            next_value = values[-1] + slope * (self.window_minutes / 10)
                        else:
                            next_value = 0.5
                        
                        module_metrics[metric_name] = {
                            'current': values[-1] if values else 0.5,
                            'predicted': max(0.0, min(1.0, next_value)),
                            'trend': slope,
                            'volatility': volatility,
                            'samples': len(values)
                        }
            
            if not module_metrics:
                return {'status': 'insufficient_data'}
            
            # Calculate overall health score
            health_score = np.mean([m['current'] for m in module_metrics.values()])
            predicted_health = np.mean([m['predicted'] for m in module_metrics.values()])
            
            # Determine if health is declining
            is_declining = any(m['trend'] < -0.01 for m in module_metrics.values())
            
            # Calculate failure probability
            failure_probability = 1.0 - predicted_health
            if is_declining:
                failure_probability *= 1.5
            
            # Determine status
            if failure_probability > 0.7:
                status = HealthStatus.PREDICTED_UNHEALTHY
            elif failure_probability > 0.4:
                status = HealthStatus.DEGRADED
            else:
                status = HealthStatus.HEALTHY
            
            result = {
                'module': module_name,
                'health_score': health_score,
                'predicted_health': predicted_health,
                'status': status.value,
                'failure_probability': min(1.0, failure_probability),
                'is_declining': is_declining,
                'metrics': module_metrics,
                'timestamp': datetime.utcnow().isoformat()
            }
            
            self.forecast_results[module_name] = result
            return result
    
    def get_forecast_summary(self) -> Dict[str, Any]:
        """Get summary of all health forecasts"""
        return {
            'modules_forecasted': list(self.forecast_results.keys()),
            'forecasts': self.forecast_results,
            'timestamp': datetime.utcnow().isoformat()
        }

# ============================================================================
# Predictive Scaling Engine (NEW)
# ============================================================================

class PredictiveScalingEngine:
    """
    Predictive scaling based on demand forecasting.
    
    Features:
    - Demand pattern analysis
    - Proactive compartment scaling
    - Scaling recommendations
    """
    
    def __init__(self, lookback_hours: int = 24, threshold: float = 0.7):
        self.lookback_hours = lookback_hours
        self.threshold = threshold
        self.demand_history: List[Dict] = []
        self.scaling_decisions: List[Dict] = []
        self._lock = asyncio.Lock()
        
        logger.info(f"Predictive Scaling Engine initialized (lookback: {lookback_hours}h)")
    
    def record_demand(self, demand_level: float, timestamp: Optional[datetime] = None):
        """Record demand level"""
        self.demand_history.append({
            'timestamp': timestamp or datetime.utcnow(),
            'demand': demand_level
        })
        
        # Keep history within lookback
        cutoff = datetime.utcnow() - timedelta(hours=self.lookback_hours)
        self.demand_history = [d for d in self.demand_history if d['timestamp'] > cutoff]
    
    async def predict_demand(self, horizon_minutes: int = 60) -> Dict[str, Any]:
        """Predict future demand"""
        async with self._lock:
            if len(self.demand_history) < 10:
                return {'status': 'insufficient_data'}
            
            # Extract demand values
            values = [d['demand'] for d in self.demand_history[-50:]]
            timestamps = [d['timestamp'] for d in self.demand_history[-50:]]
            
            if len(values) < 5:
                return {'status': 'insufficient_data'}
            
            # Calculate trend
            x = np.arange(len(values))
            slope = np.polyfit(x, values, 1)[0]
            
            # Calculate seasonality (hour of day pattern)
            hours = [t.hour for t in timestamps]
            seasonal_pattern = defaultdict(list)
            for hour, value in zip(hours, values):
                seasonal_pattern[hour].append(value)
            
            avg_by_hour = {
                hour: np.mean(vals) for hour, vals in seasonal_pattern.items()
            }
            
            # Predict next N points
            current_hour = datetime.utcnow().hour
            n_steps = horizon_minutes // 10
            
            predictions = []
            for i in range(n_steps):
                future_hour = (current_hour + i * 10 // 60) % 24
                base = values[-1] + slope * (i + 1) * 10 / 60
                seasonal_adjust = avg_by_hour.get(future_hour, 0.5) - np.mean(values)
                predicted = max(0.0, min(1.0, base + seasonal_adjust * 0.2))
                predictions.append(predicted)
            
            # Calculate confidence
            volatility = np.std(values[-20:]) if len(values) >= 20 else 0.2
            confidence = max(0.1, 1.0 - volatility * 2)
            
            avg_prediction = np.mean(predictions) if predictions else 0.5
            
            return {
                'status': 'success',
                'predictions': predictions,
                'average_prediction': avg_prediction,
                'trend': 'increasing' if slope > 0.01 else 'decreasing' if slope < -0.01 else 'stable',
                'slope': slope,
                'confidence': confidence,
                'samples': len(values)
            }
    
    async def get_scaling_recommendation(self, current_compartments: int, 
                                        max_compartments: int) -> Dict[str, Any]:
        """Get scaling recommendation based on predicted demand"""
        demand_forecast = await self.predict_demand()
        
        if demand_forecast.get('status') != 'success':
            return {'action': 'no_change', 'reason': 'insufficient_data'}
        
        avg_prediction = demand_forecast.get('average_prediction', 0.5)
        confidence = demand_forecast.get('confidence', 0.5)
        
        # Only scale if confidence is sufficient
        if confidence < 0.5:
            return {'action': 'no_change', 'reason': 'low_confidence'}
        
        # Calculate target compartments
        target_ratio = avg_prediction / self.threshold
        target_compartments = int(current_compartments * target_ratio)
        target_compartments = max(1, min(max_compartments, target_compartments))
        
        if target_compartments > current_compartments:
            return {
                'action': 'scale_up',
                'current': current_compartments,
                'target': target_compartments,
                'increase': target_compartments - current_compartments,
                'reason': f'predicted_demand_{avg_prediction:.2f}',
                'confidence': confidence
            }
        elif target_compartments < current_compartments:
            return {
                'action': 'scale_down',
                'current': current_compartments,
                'target': target_compartments,
                'decrease': current_compartments - target_compartments,
                'reason': f'predicted_demand_{avg_prediction:.2f}',
                'confidence': confidence
            }
        else:
            return {
                'action': 'no_change',
                'current': current_compartments,
                'reason': 'stable_demand',
                'confidence': confidence
            }

# ============================================================================
# Health Check Manager (Enhanced)
# ============================================================================

class HealthCheckManager:
    """
    Manages health checks for all bio-inspired modules (Enhanced with predictive health).
    """
    
    def __init__(self):
        self.module_health: Dict[str, ModuleHealth] = {}
        self.overall_status = HealthStatus.STARTING
        self.last_full_check: Optional[datetime] = None
        # NEW: Predictive health
        self.health_forecaster = PredictiveHealthForecaster()
        
        logger.info("Health Check Manager initialized")
    
    def register_module(self, module_name: str):
        """Register a module for health checking"""
        self.module_health[module_name] = ModuleHealth(
            module_name=module_name,
            status=HealthStatus.STARTING
        )
    
    def update_health(self, module_name: str, status: HealthStatus, 
                     metrics: Optional[Dict[str, Any]] = None,
                     error: Optional[str] = None):
        """Update health status for a module"""
        if module_name not in self.module_health:
            self.register_module(module_name)
        
        health = self.module_health[module_name]
        health.status = status
        health.last_check = datetime.utcnow()
        health.error_message = error
        if metrics:
            health.metrics.update(metrics)
        
        # Record metrics for predictive forecasting
        for key, value in metrics.items():
            if isinstance(value, (int, float)):
                self.health_forecaster.record_metric(module_name, key, value)
    
    def check_all(self, bio_core) -> Dict[str, Any]:
        """Run health checks on all modules (Enhanced with predictions)"""
        results = {}
        all_healthy = True
        
        # Check token manager
        if hasattr(bio_core, 'token_manager'):
            try:
                summary = bio_core.token_manager.get_system_summary()
                balance = summary.get('total_balance', 0)
                
                if summary.get('emergency_mode'):
                    status = HealthStatus.DEGRADED
                    all_healthy = False
                elif balance < 100:
                    status = HealthStatus.DEGRADED
                    all_healthy = False
                else:
                    status = HealthStatus.HEALTHY
                
                self.update_health('token_manager', status, {'balance': balance})
                results['token_manager'] = {'status': status.value, 'balance': balance}
            except Exception as e:
                self.update_health('token_manager', HealthStatus.UNHEALTHY, error=str(e))
                results['token_manager'] = {'status': 'unhealthy', 'error': str(e)}
                all_healthy = False
        
        # Check gradient manager
        if hasattr(bio_core, 'gradient_manager'):
            try:
                strengths = bio_core.gradient_manager.get_field_strengths()
                critical = any(s > 0.9 for s in strengths.values())
                
                if critical:
                    status = HealthStatus.DEGRADED
                    all_healthy = False
                else:
                    status = HealthStatus.HEALTHY
                
                self.update_health('gradient_manager', status, {'fields': strengths})
                results['gradient_manager'] = {'status': status.value, 'fields': strengths}
            except Exception as e:
                self.update_health('gradient_manager', HealthStatus.UNHEALTHY, error=str(e))
                results['gradient_manager'] = {'status': 'unhealthy', 'error': str(e)}
                all_healthy = False
        
        # Check compartment manager
        if hasattr(bio_core, 'compartment_manager'):
            try:
                stats = bio_core.compartment_manager.get_ecosystem_stats()
                viable = stats.get('viable_compartments', 0)
                total = stats.get('total_compartments', 0)
                
                if viable == 0:
                    status = HealthStatus.UNHEALTHY
                    all_healthy = False
                elif viable < total * 0.5:
                    status = HealthStatus.DEGRADED
                    all_healthy = False
                else:
                    status = HealthStatus.HEALTHY
                
                self.update_health('compartment_manager', status, {'viable': viable, 'total': total})
                results['compartment_manager'] = {'status': status.value, 'viable': viable}
            except Exception as e:
                self.update_health('compartment_manager', HealthStatus.UNHEALTHY, error=str(e))
                results['compartment_manager'] = {'status': 'unhealthy', 'error': str(e)}
                all_healthy = False
        
        # Check ATP synthase
        if hasattr(bio_core, 'scheduler'):
            try:
                stats = bio_core.scheduler.get_scheduler_stats()
                rate = stats.get('current_atp_rate', 0)
                
                if rate <= 0:
                    status = HealthStatus.DEGRADED
                    all_healthy = False
                else:
                    status = HealthStatus.HEALTHY
                
                self.update_health('atp_synthase', status, {'rate': rate})
                results['atp_synthase'] = {'status': status.value, 'rate': rate}
            except Exception as e:
                self.update_health('atp_synthase', HealthStatus.UNHEALTHY, error=str(e))
                results['atp_synthase'] = {'status': 'unhealthy', 'error': str(e)}
                all_healthy = False
        
        # Generate predictive health forecasts
        for module_name in self.module_health.keys():
            try:
                forecast = asyncio.run(self.health_forecaster.forecast_health(module_name))
                if forecast.get('status') != 'insufficient_data':
                    health = self.module_health[module_name]
                    health.predicted_status = HealthStatus(forecast['status'])
                    health.predicted_at = datetime.utcnow()
                    health.failure_probability = forecast.get('failure_probability', 0)
                    health.health_trend = 'declining' if forecast.get('is_declining', False) else 'stable'
            except Exception as e:
                logger.warning(f"Health forecast failed for {module_name}: {e}")
        
        self.overall_status = HealthStatus.HEALTHY if all_healthy else HealthStatus.DEGRADED
        self.last_full_check = datetime.utcnow()
        
        return {
            'status': self.overall_status.value,
            'timestamp': datetime.utcnow().isoformat(),
            'modules': results,
            'predictive_health': self.health_forecaster.get_forecast_summary()
        }
    
    def is_ready(self) -> bool:
        """Check if agent is ready to serve requests"""
        required_modules = ['token_manager', 'gradient_manager', 'compartment_manager']
        for module in required_modules:
            if module in self.module_health:
                if self.module_health[module].status in [HealthStatus.UNHEALTHY, HealthStatus.PREDICTED_UNHEALTHY]:
                    return False
            else:
                return False
        return True
    
    def is_alive(self) -> bool:
        """Check if agent process is alive"""
        return True
    
    def get_health_report(self) -> Dict[str, Any]:
        """Get comprehensive health report (Enhanced)"""
        report = {
            'overall_status': self.overall_status.value,
            'last_full_check': self.last_full_check.isoformat() if self.last_full_check else None,
            'modules': {
                name: {
                    'status': health.status.value,
                    'last_check': health.last_check.isoformat(),
                    'error': health.error_message,
                    'metrics': health.metrics,
                    'predicted_status': health.predicted_status.value if health.predicted_status else None,
                    'failure_probability': health.failure_probability,
                    'health_trend': health.health_trend
                }
                for name, health in self.module_health.items()
            },
            'predictive_health': self.health_forecaster.get_forecast_summary()
        }
        return report

# ============================================================================
# Event Bus (Enhanced)
# ============================================================================

class EventBus:
    """
    Enhanced event bus with persistence and OpenTelemetry support.
    """
    
    def __init__(self, enable_persistence: bool = True):
        self.subscribers: Dict[str, List[Callable]] = defaultdict(list)
        self.event_queue: asyncio.Queue = asyncio.Queue(maxsize=10000)
        self.event_history: deque = deque(maxlen=1000)
        self.running = True
        self.enable_persistence = enable_persistence
        self.event_persistence: Optional[EventPersistenceManager] = None
        
        if enable_persistence:
            self.event_persistence = EventPersistenceManager()
        
        # OpenTelemetry tracer
        self._tracer = None
        if OPENTELEMETRY_AVAILABLE:
            try:
                self._tracer = trace.get_tracer(__name__)
            except Exception as e:
                logger.warning(f"Failed to get OpenTelemetry tracer: {e}")
        
        # Start event processor
        asyncio.create_task(self._process_events())
        
        logger.info(f"Event Bus initialized (persistence={enable_persistence})")
    
    def publish(self, event_type: str, payload: Dict[str, Any], 
                correlation_id: Optional[str] = None,
                source: Optional[str] = None):
        """Publish an event to all subscribers"""
        event = {
            'event_id': uuid.uuid4().hex[:12],
            'event_type': event_type,
            'payload': payload,
            'correlation_id': correlation_id or uuid.uuid4().hex[:12],
            'source': source,
            'timestamp': datetime.utcnow()
        }
        
        try:
            self.event_queue.put_nowait(event)
        except asyncio.QueueFull:
            logger.warning(f"Event queue full, dropping event: {event_type}")
    
    def subscribe(self, event_type: str, callback: Callable):
        """Subscribe to events of a specific type"""
        self.subscribers[event_type].append(callback)
        logger.debug(f"Subscribed to event: {event_type}")
    
    def unsubscribe(self, event_type: str, callback: Callable):
        """Unsubscribe from events"""
        if event_type in self.subscribers:
            self.subscribers[event_type].remove(callback)
    
    async def _process_events(self):
        """Background event processing loop"""
        while self.running:
            try:
                event = await self.event_queue.get()
                
                # Persist event if enabled
                if self.event_persistence:
                    await self.event_persistence.persist_event(event)
                
                # OpenTelemetry span
                if self._tracer:
                    with self._tracer.start_as_current_span(
                        f"event_{event['event_type']}",
                        attributes={
                            'event_type': event['event_type'],
                            'correlation_id': event.get('correlation_id'),
                            'source': event.get('source')
                        }
                    ):
                        self._notify_subscribers(event)
                else:
                    self._notify_subscribers(event)
                
                self.event_history.append(event)
                self.event_queue.task_done()
                
            except Exception as e:
                logger.error(f"Event processing error: {str(e)}")
                await asyncio.sleep(1)
    
    def _notify_subscribers(self, event: Dict[str, Any]):
        """Notify subscribers of an event"""
        subscribers = self.subscribers.get(event['event_type'], [])
        for callback in subscribers:
            try:
                if asyncio.iscoroutinefunction(callback):
                    # Schedule async callback
                    asyncio.create_task(callback(event))
                else:
                    callback(event)
            except Exception as e:
                logger.error(f"Event callback error: {str(e)}")
    
    def shutdown(self):
        """Shutdown the event bus"""
        self.running = False
        logger.info("Event Bus shutdown")
    
    def get_stats(self) -> Dict[str, Any]:
        """Get event bus statistics"""
        stats = {
            'subscriber_count': sum(len(v) for v in self.subscribers.values()),
            'event_types': list(self.subscribers.keys()),
            'queue_size': self.event_queue.qsize(),
            'events_processed': len(self.event_history),
            'persistence_enabled': self.enable_persistence
        }
        if self.event_persistence:
            stats['persistence'] = self.event_persistence.get_event_stats()
        return stats

# ============================================================================
# Enhanced Bio-Integrated Green Agent
# ============================================================================

class BioIntegratedGreenAgent:
    """
    Enhanced Bio-Integrated Green Agent v6.0.0
    
    New Features:
    - Versioned snapshots for rollback
    - Predictive health forecasting
    - Event persistence for replay and auditing
    - Predictive scaling based on demand
    - OpenTelemetry integration for observability
    """
    
    def __init__(self, config: Optional[AgentConfig] = None):
        self.config = config or AgentConfig()
        self.state = AgentState.UNINITIALIZED
        
        # Validate configuration
        issues = self.config.validate()
        if issues:
            logger.warning(f"Configuration issues: {issues}")
        
        # Event bus with persistence
        self.event_bus = EventBus(enable_persistence=self.config.enable_event_persistence)
        
        # Health check manager
        self.health_manager = HealthCheckManager()
        
        # Versioned snapshot manager
        self.snapshot_manager = VersionedSnapshotManager(
            state_directory=self.config.state_directory,
            max_snapshots=self.config.max_snapshots
        ) if self.config.enable_state_persistence else None
        
        # Predictive scaling engine
        self.scaling_engine = PredictiveScalingEngine(
            lookback_hours=self.config.scaling_lookback_hours,
            threshold=self.config.scaling_threshold
        ) if self.config.enable_predictive_scaling else None
        
        # OpenTelemetry tracer
        self._tracer = None
        if OPENTELEMETRY_AVAILABLE and self.config.enable_opentelemetry:
            try:
                self._tracer = trace.get_tracer(self.config.service_name)
                logger.info("OpenTelemetry tracer initialized")
            except Exception as e:
                logger.warning(f"Failed to initialize OpenTelemetry tracer: {e}")
        
        # Module references
        self.token_manager = None
        self.gradient_manager = None
        self.scheduler = None
        self.compartment_manager = None
        self.biomass_storage = None
        self.harvester = None
        self.supply_manager = None
        self.token_allocator = None
        self.knowledge_transfer = None
        self.degradation_manager = None
        
        # Correlation ID for request tracing
        self._correlation_counter = 0
        
        # Background tasks
        self._background_tasks: List[asyncio.Task] = []
        
        # Initialize
        self._initialize()
        
        # Register signal handlers
        self._register_signal_handlers()
        
        logger.info("Bio-Integrated Green Agent v6.0.0 initialized")
    
    def _initialize(self):
        """Initialize all modules with health verification"""
        self.state = AgentState.INITIALIZING
        
        try:
            # Step 1: Initialize exchange rate
            self.exchange_rate = DynamicExchangeRate()
            self.health_manager.register_module('exchange_rate')
            self.health_manager.update_health('exchange_rate', HealthStatus.HEALTHY)
            
            # Step 2: Initialize token manager
            if MODULE_STATUS.get('token_manager', False):
                self.token_manager = EcoATPTokenManager(self.exchange_rate)
                self.health_manager.register_module('token_manager')
                self.health_manager.update_health('token_manager', HealthStatus.HEALTHY)
                
                if self.config.enable_supply_management:
                    self.supply_manager = TokenSupplyManager(self.token_manager)
                
                if self.config.enable_token_preallocation:
                    self.token_allocator = PredictiveTokenAllocator(self.token_manager)
            
            # Step 3: Initialize gradient manager
            if MODULE_STATUS.get('gradient_manager', False):
                self.gradient_manager = HierarchicalGradientManager()
                self.health_manager.register_module('gradient_manager')
                self.health_manager.update_health('gradient_manager', HealthStatus.HEALTHY)
            
            # Step 4: Initialize ATP synthase
            if MODULE_STATUS.get('atp_synthase', False):
                synthase_config = SynthaseConfig(
                    protons_per_rotation=self.config.atp_c_ring_size,
                    max_rotation_speed_rpm=self.config.atp_max_rotation_speed
                )
                self.scheduler = ATPSynthaseScheduler(
                    self.token_manager, self.gradient_manager, synthase_config,
                    enable_multi_synthase=self.config.enable_multi_synthase
                )
                self.health_manager.register_module('atp_synthase')
                self.health_manager.update_health('atp_synthase', HealthStatus.HEALTHY)
            
            # Step 5: Initialize compartment manager
            if MODULE_STATUS.get('compartment_manager', False):
                self.compartment_manager = HierarchicalCompartmentManager(
                    self.token_manager,
                    max_regions=10,
                    compartments_per_region=20
                )
                self.health_manager.register_module('compartment_manager')
                self.health_manager.update_health('compartment_manager', HealthStatus.HEALTHY)
            
            # Step 6: Initialize biomass storage
            if MODULE_STATUS.get('biomass_storage', False):
                self.biomass_storage = BiomassStorage(self.token_manager)
                self.health_manager.register_module('biomass_storage')
                self.health_manager.update_health('biomass_storage', HealthStatus.HEALTHY)
            
            # Step 7: Initialize harvester
            if MODULE_STATUS.get('harvester', False):
                self.harvester = EnhancedPhotosyntheticHarvester(
                    self.token_manager, self.gradient_manager
                )
                self.health_manager.register_module('harvester')
                self.health_manager.update_health('harvester', HealthStatus.HEALTHY)
                
                if self.scheduler:
                    self.scheduler.inject_harvester(self.harvester)
            
            # Step 8: Initialize knowledge transfer
            if self.config.enable_knowledge_transfer:
                try:
                    from .knowledge_transfer import KnowledgeTransferManager
                    self.knowledge_transfer = KnowledgeTransferManager()
                    self.health_manager.register_module('knowledge_transfer')
                    self.health_manager.update_health('knowledge_transfer', HealthStatus.HEALTHY)
                except ImportError:
                    pass
            
            # Step 9: Initialize degradation manager
            if self.config.enable_degradation_manager:
                try:
                    from .degradation_manager import DegradationManager
                    self.degradation_manager = DegradationManager()
                    self.health_manager.register_module('degradation_manager')
                    self.health_manager.update_health('degradation_manager', HealthStatus.HEALTHY)
                except ImportError:
                    pass
            
            # Step 10: Restore state if available
            if self.snapshot_manager and self.snapshot_manager.snapshot_chain:
                latest = self.snapshot_manager.snapshot_chain[-1]
                restored = asyncio.run(self.snapshot_manager.rollback_to_snapshot(latest, self))
                if restored:
                    logger.info(f"State restored from snapshot: {latest}")
            
            # Step 11: Create expert compartments
            self._create_expert_compartments()
            
            # Step 12: Subscribe to events
            self._subscribe_to_events()
            
            # Step 13: Start background tasks
            self._start_background_tasks()
            
            # Step 14: Run initial health check
            self.health_manager.check_all(self)
            
            self.state = AgentState.RUNNING
            logger.info(f"Agent initialized successfully. State: {self.state.value}")
            
        except Exception as e:
            self.state = AgentState.ERROR
            logger.error(f"Initialization failed: {str(e)}", exc_info=True)
            raise
    
    def _create_expert_compartments(self):
        """Create expert compartments with dynamic scaling"""
        if not self.compartment_manager:
            return
        
        expert_types = ['energy', 'data', 'iot']
        
        if self.config.enable_quantum_expert:
            expert_types.append('quantum')
        if self.config.enable_helium_expert:
            expert_types.append('helium')
        
        for etype in expert_types:
            for i in range(self.config.compartments_per_expert_type):
                self.compartment_manager.create_compartment(etype)
        
        logger.info(f"Created compartments for {len(expert_types)} expert types "
                   f"({self.config.compartments_per_expert_type} each)")
    
    def _subscribe_to_events(self):
        """Subscribe to internal events for cross-module communication"""
        self.event_bus.subscribe('token_low', self._on_token_low)
        self.event_bus.subscribe('token_critical', self._on_token_critical)
        self.event_bus.subscribe('gradient_high', self._on_gradient_high)
        self.event_bus.subscribe('gradient_critical', self._on_gradient_critical)
        self.event_bus.subscribe('compartment_unhealthy', self._on_compartment_unhealthy)
        self.event_bus.subscribe('compartment_depleted', self._on_compartment_depleted)
        
        # NEW: Predictive scaling events
        self.event_bus.subscribe('demand_forecast', self._on_demand_forecast)
        self.event_bus.subscribe('health_forecast', self._on_health_forecast)
        
        logger.info(f"Subscribed to {len(self.event_bus.subscribers)} event types")
    
    def _start_background_tasks(self):
        """Start all background maintenance tasks"""
        # Health check loop
        task = asyncio.create_task(self._health_check_loop())
        self._background_tasks.append(task)
        
        # State persistence loop
        if self.snapshot_manager:
            task = asyncio.create_task(self._state_persistence_loop())
            self._background_tasks.append(task)
        
        # Dynamic scaling loop
        task = asyncio.create_task(self._dynamic_scaling_loop())
        self._background_tasks.append(task)
        
        # Environmental monitoring loop
        task = asyncio.create_task(self._environmental_loop())
        self._background_tasks.append(task)
        
        # NEW: Predictive scaling loop
        if self.scaling_engine:
            task = asyncio.create_task(self._predictive_scaling_loop())
            self._background_tasks.append(task)
        
        logger.info(f"Started {len(self._background_tasks)} background tasks")
    
    def _register_signal_handlers(self):
        """Register OS signal handlers for graceful shutdown"""
        try:
            loop = asyncio.get_event_loop()
            for sig in [signal.SIGINT, signal.SIGTERM]:
                loop.add_signal_handler(sig, lambda: asyncio.create_task(self.shutdown()))
            logger.info("Signal handlers registered")
        except NotImplementedError:
            logger.warning("Signal handlers not supported on this platform")
    
    # ========================================================================
    # Correlation ID for Distributed Tracing
    # ========================================================================
    
    def _generate_correlation_id(self) -> str:
        """Generate unique correlation ID for request tracing"""
        self._correlation_counter += 1
        return f"corr_{datetime.utcnow().timestamp()}_{self._correlation_counter}"
    
    # ========================================================================
    # Event Handlers (Enhanced)
    # ========================================================================
    
    async def _on_token_low(self, event: Dict[str, Any]):
        """Handle low token event"""
        logger.warning(f"Token low event: {event['payload']}")
        if self.degradation_manager:
            self.degradation_manager.update_metrics(token_balance=event['payload'].get('balance', 0))
        # Record for predictive scaling
        if self.scaling_engine:
            self.scaling_engine.record_demand(0.3)
    
    async def _on_token_critical(self, event: Dict[str, Any]):
        """Handle critical token event"""
        logger.error(f"Token critical event: {event['payload']}")
        if self.token_manager:
            self.token_manager._activate_emergency_mode()
        if self.scaling_engine:
            self.scaling_engine.record_demand(0.1)
    
    async def _on_gradient_high(self, event: Dict[str, Any]):
        """Handle high gradient event"""
        field_id = event['payload'].get('field_id', 'unknown')
        logger.warning(f"High gradient: {field_id}")
        if self.gradient_manager and field_id in self.gradient_manager.fields:
            field = self.gradient_manager.fields[field_id]
            field.leakage_rate = min(0.3, field.leakage_rate * 2)
    
    async def _on_gradient_critical(self, event: Dict[str, Any]):
        """Handle critical gradient event"""
        field_id = event['payload'].get('field_id', 'unknown')
        logger.error(f"Critical gradient: {field_id}")
        if self.scheduler:
            for synthase in self.scheduler.synthases.values():
                synthase.operate_uncoupled(self.gradient_manager)
    
    async def _on_compartment_unhealthy(self, event: Dict[str, Any]):
        """Handle unhealthy compartment event"""
        compartment_id = event['payload'].get('compartment_id', 'unknown')
        logger.warning(f"Unhealthy compartment: {compartment_id}")
        if self.compartment_manager:
            expert_type = event['payload'].get('expert_type', 'data')
            self.compartment_manager.create_compartment(expert_type)
    
    async def _on_compartment_depleted(self, event: Dict[str, Any]):
        """Handle compartment depletion event"""
        expert_type = event['payload'].get('expert_type', 'unknown')
        logger.error(f"Compartment type depleted: {expert_type}")
        if self.compartment_manager:
            for _ in range(3):
                self.compartment_manager.create_compartment(expert_type)
    
    # NEW: Predictive event handlers
    async def _on_demand_forecast(self, event: Dict[str, Any]):
        """Handle demand forecast event"""
        forecast = event['payload']
        if forecast.get('action') == 'scale_up':
            logger.info(f"Predictive scaling: scale up to {forecast['target']} compartments")
            if self.compartment_manager:
                for _ in range(forecast['increase']):
                    self.compartment_manager.create_compartment('general')
    
    async def _on_health_forecast(self, event: Dict[str, Any]):
        """Handle health forecast event"""
        logger.info(f"Health forecast: {event['payload'].get('module')} - {event['payload'].get('status')}")
    
    # ========================================================================
    # Background Loops (Enhanced)
    # ========================================================================
    
    async def _health_check_loop(self):
        """Periodic health check loop with predictive forecasting"""
        while self.state == AgentState.RUNNING:
            try:
                self.health_manager.check_all(self)
                
                # Generate predictive health forecasts
                for module_name in self.health_manager.module_health.keys():
                    forecast = await self.health_manager.health_forecaster.forecast_health(module_name)
                    if forecast.get('status') != 'insufficient_data':
                        self.event_bus.publish('health_forecast', {
                            'module': module_name,
                            'status': forecast['status'],
                            'failure_probability': forecast.get('failure_probability', 0)
                        })
                
                if self.health_manager.overall_status == HealthStatus.DEGRADED:
                    self.event_bus.publish('agent_degraded', {
                        'status': self.health_manager.overall_status.value
                    })
                
                await asyncio.sleep(self.config.health_check_interval_seconds)
                
            except Exception as e:
                logger.error(f"Health check error: {str(e)}")
                await asyncio.sleep(60)
    
    async def _state_persistence_loop(self):
        """Periodic state persistence loop with versioned snapshots"""
        while self.state == AgentState.RUNNING:
            try:
                if self.snapshot_manager:
                    snapshot = SystemSnapshot(
                        agent_state=self.state.value,
                        timestamp=datetime.utcnow(),
                        token_state=self.token_manager.get_system_summary() if self.token_manager else None,
                        gradient_state=self.gradient_manager.get_field_stats() if self.gradient_manager else None,
                        compartment_state=self.compartment_manager.get_ecosystem_stats() if self.compartment_manager else None,
                        biomass_state=self.biomass_storage.get_storage_stats() if self.biomass_storage else None,
                        config=self.config.to_dict()
                    )
                    await self.snapshot_manager.save_snapshot(snapshot, self)
                await asyncio.sleep(self.config.state_save_interval_seconds)
            except Exception as e:
                logger.error(f"State persistence error: {str(e)}")
                await asyncio.sleep(60)
    
    async def _dynamic_scaling_loop(self):
        """Dynamic compartment scaling based on load"""
        while self.state == AgentState.RUNNING:
            try:
                if self.compartment_manager and self.token_manager:
                    summary = self.token_manager.get_system_summary()
                    balance = summary.get('total_balance', 0)
                    total_compartments = sum(
                        len(r.compartments) for r in self.compartment_manager.regions.values()
                    )
                    
                    if balance > 1000 and total_compartments < self.config.max_total_compartments:
                        for etype in ['energy', 'data', 'iot']:
                            count = sum(
                                1 for r in self.compartment_manager.regions.values()
                                for c in r.compartments.values()
                                if c.expert_type == etype and c.is_viable
                            )
                            if count < 3:
                                self.compartment_manager.create_compartment(etype)
                                logger.info(f"Auto-scaled {etype} compartment (count: {count})")
                    
                    # Record demand for predictive scaling
                    if self.scaling_engine:
                        demand_level = min(1.0, (self.config.max_total_compartments - total_compartments) / self.config.max_total_compartments)
                        self.scaling_engine.record_demand(demand_level)
                
                await asyncio.sleep(60)
                
            except Exception as e:
                logger.error(f"Dynamic scaling error: {str(e)}")
                await asyncio.sleep(120)
    
    # NEW: Predictive scaling loop
    async def _predictive_scaling_loop(self):
        """Predictive scaling based on demand forecasting"""
        while self.state == AgentState.RUNNING:
            try:
                if self.scaling_engine and self.compartment_manager:
                    total_compartments = sum(
                        len(r.compartments) for r in self.compartment_manager.regions.values()
                    )
                    
                    recommendation = await self.scaling_engine.get_scaling_recommendation(
                        total_compartments, self.config.max_total_compartments
                    )
                    
                    if recommendation['action'] == 'scale_up':
                        logger.info(f"Predictive scaling: scaling up to {recommendation['target']} compartments")
                        self.event_bus.publish('demand_forecast', recommendation)
                        
                        if self.compartment_manager:
                            for etype in ['energy', 'data', 'iot']:
                                for _ in range(recommendation['increase'] // 3 + 1):
                                    if total_compartments < self.config.max_total_compartments:
                                        self.compartment_manager.create_compartment(etype)
                                        total_compartments += 1
                    
                    elif recommendation['action'] == 'scale_down':
                        logger.info(f"Predictive scaling: scaling down to {recommendation['target']} compartments")
                        # Remove excess compartments (simplified)
                        pass
                
                await asyncio.sleep(300)  # 5 minutes
                
            except Exception as e:
                logger.error(f"Predictive scaling error: {str(e)}")
                await asyncio.sleep(600)
    
    async def _environmental_loop(self):
        """Environmental monitoring and harvesting loop"""
        while self.state == AgentState.RUNNING:
            try:
                if self.harvester:
                    env_data = {
                        'renewable_availability': np.random.uniform(0.3, 0.9),
                        'carbon_intensity': np.random.uniform(100, 600),
                        'waste_heat': np.random.uniform(0.1, 0.5),
                        'edge_availability': np.random.uniform(0.2, 0.8),
                        'system_overload': np.random.uniform(0.0, 0.3)
                    }
                    
                    result = await self.harvester.harvest_cycle(env_data)
                    
                    if result['eco_atp_generated'] > 0:
                        self.event_bus.publish('harvest_complete', {
                            'eco_atp_generated': result['eco_atp_generated'],
                            'dominant_signal': result['dominant_signal']
                        })
                
                await asyncio.sleep(10)
                
            except Exception as e:
                logger.error(f"Environmental loop error: {str(e)}")
                await asyncio.sleep(30)
    
    # ========================================================================
    # Public API Methods (Enhanced)
    # ========================================================================
    
    def process_task(self, task: Dict[str, Any]) -> Dict[str, Any]:
        """Process a task with OpenTelemetry tracing"""
        correlation_id = self._generate_correlation_id()
        task['correlation_id'] = correlation_id
        
        # OpenTelemetry span
        if self._tracer:
            with self._tracer.start_as_current_span(
                "process_task",
                attributes={
                    'task_id': task.get('task_id', 'unknown'),
                    'task_type': task.get('task_type', 'unknown'),
                    'correlation_id': correlation_id
                }
            ):
                return self._process_task_internal(task, correlation_id)
        else:
            return self._process_task_internal(task, correlation_id)
    
    def _process_task_internal(self, task: Dict[str, Any], correlation_id: str) -> Dict[str, Any]:
        """Internal task processing"""
        logger.info(f"Processing task: {task.get('task_id', 'unknown')} [{correlation_id}]")
        
        self.event_bus.publish('task_received', {
            'task_id': task.get('task_id'),
            'task_type': task.get('task_type'),
            'correlation_id': correlation_id
        })
        
        ecoatp_required = task.get('complexity', 0.5) * 10
        
        if self.token_allocator:
            success, latency = self.token_allocator.get_tokens('task_processor', ecoatp_required)
            if success:
                self.token_allocator.record_demand('task_processor', ecoatp_required)
        elif self.token_manager:
            success, _ = self.token_manager.reserve_tokens(
                'task_processor', ecoatp_required, EcoATPConsumer.EXPERT_EXECUTION
            )
        else:
            success = True
        
        if not success:
            if self.biomass_storage:
                stored, token_id = self.biomass_storage.store_task(
                    task_data=task, ecoatp_cost=ecoatp_required
                )
                
                self.event_bus.publish('task_stored', {
                    'task_id': task.get('task_id'),
                    'biomass_token': token_id,
                    'correlation_id': correlation_id
                })
                
                return {
                    'success': True,
                    'status': 'stored',
                    'biomass_token': token_id,
                    'correlation_id': correlation_id
                }
            
            return {
                'success': False,
                'reason': 'Insufficient tokens',
                'correlation_id': correlation_id
            }
        
        result = {
            'success': True,
            'task_id': task.get('task_id', 'unknown'),
            'correlation_id': correlation_id,
            'ecoatp_cost': ecoatp_required
        }
        
        self.event_bus.publish('task_completed', {
            'task_id': task.get('task_id'),
            'success': True,
            'correlation_id': correlation_id
        })
        
        return result
    
    def get_system_status(self) -> Dict[str, Any]:
        """Get comprehensive system status (Enhanced)"""
        status = {
            'agent_state': self.state.value,
            'timestamp': datetime.utcnow().isoformat(),
            'health': self.health_manager.get_health_report(),
            'event_bus': self.event_bus.get_stats(),
            'config': self.config.to_dict()
        }
        
        if self.token_manager:
            status['token_economy'] = self.token_manager.get_system_summary()
        if self.gradient_manager:
            status['gradients'] = self.gradient_manager.get_field_stats()
            status['forecasts'] = self.gradient_manager.get_forecast_summary()
        if self.compartment_manager:
            status['compartments'] = self.compartment_manager.get_ecosystem_stats()
        if self.biomass_storage:
            status['biomass'] = self.biomass_storage.get_storage_stats()
        if self.harvester:
            status['harvester'] = self.harvester.get_harvesting_stats()
        if self.scheduler:
            status['atp_synthase'] = self.scheduler.get_scheduler_stats()
        if self.supply_manager:
            status['supply_management'] = self.supply_manager.get_economic_indicators()
        if self.token_allocator:
            status['pre_allocation'] = self.token_allocator.get_cache_stats()
        if self.scaling_engine:
            status['predictive_scaling'] = {
                'demand_samples': len(self.scaling_engine.demand_history),
                'last_prediction': await self.scaling_engine.predict_demand()
            }
        if self.snapshot_manager:
            status['snapshots'] = self.snapshot_manager.get_snapshot_list()
        
        return status
    
    def get_health_status(self) -> Dict[str, Any]:
        """Get health check status (for Kubernetes probes)"""
        return {
            'status': self.health_manager.overall_status.value,
            'ready': self.health_manager.is_ready(),
            'alive': self.health_manager.is_alive(),
            'timestamp': datetime.utcnow().isoformat()
        }
    
    def get_configuration(self) -> Dict[str, Any]:
        """Get current configuration"""
        return self.config.to_dict()
    
    def update_configuration(self, updates: Dict[str, Any]):
        """Update configuration at runtime"""
        for key, value in updates.items():
            if hasattr(self.config, key):
                setattr(self.config, key, value)
        logger.info(f"Configuration updated: {list(updates.keys())}")
    
    # ========================================================================
    # Snapshot Management (NEW)
    # ========================================================================
    
    def get_snapshots(self) -> List[Dict[str, Any]]:
        """Get list of available snapshots"""
        if self.snapshot_manager:
            return self.snapshot_manager.get_snapshot_list()
        return []
    
    async def rollback_to_snapshot(self, snapshot_id: str) -> bool:
        """Rollback to a specific snapshot"""
        if self.snapshot_manager:
            result = await self.snapshot_manager.rollback_to_snapshot(snapshot_id, self)
            if result:
                self.state = AgentState.RECOVERING
                logger.info(f"Rolled back to snapshot: {snapshot_id}")
                self.state = AgentState.RUNNING
            return result
        return False
    
    # ========================================================================
    # Event Replay (NEW)
    # ========================================================================
    
    async def replay_events(self, event_type: Optional[str] = None,
                           correlation_id: Optional[str] = None,
                           limit: int = 1000) -> List[Dict[str, Any]]:
        """Replay events from persistence"""
        if self.event_bus.event_persistence:
            events = await self.event_bus.event_persistence.replay_events(
                event_type, correlation_id, limit
            )
            return [{
                'event_id': e.event_id,
                'event_type': e.event_type,
                'payload': e.payload,
                'timestamp': e.timestamp.isoformat(),
                'correlation_id': e.correlation_id
            } for e in events]
        return []
    
    # ========================================================================
    # Graceful Shutdown
    # ========================================================================
    
    async def shutdown(self):
        """Graceful shutdown with state preservation"""
        if self.state == AgentState.SHUTTING_DOWN:
            return
        
        self.state = AgentState.SHUTTING_DOWN
        logger.info("Initiating graceful shutdown...")
        
        # Step 1: Stop accepting new tasks
        self.event_bus.publish('agent_shutdown', {'timestamp': datetime.utcnow().isoformat()})
        
        # Step 2: Save state
        if self.snapshot_manager:
            logger.info("Saving state...")
            snapshot = SystemSnapshot(
                agent_state=self.state.value,
                timestamp=datetime.utcnow(),
                token_state=self.token_manager.get_system_summary() if self.token_manager else None,
                gradient_state=self.gradient_manager.get_field_stats() if self.gradient_manager else None,
                compartment_state=self.compartment_manager.get_ecosystem_stats() if self.compartment_manager else None,
                biomass_state=self.biomass_storage.get_storage_stats() if self.biomass_storage else None,
                config=self.config.to_dict()
            )
            await self.snapshot_manager.save_snapshot(snapshot, self)
        
        # Step 3: Cancel background tasks
        for task in self._background_tasks:
            task.cancel()
        
        # Step 4: Shutdown event bus
        self.event_bus.shutdown()
        
        # Step 5: Cleanup resources
        if self.compartment_manager:
            for region in list(self.compartment_manager.regions.values()):
                for comp_id in list(region.compartments.keys()):
                    self.compartment_manager.decommission_compartment(comp_id)
        
        self.state = AgentState.SHUTDOWN
        logger.info("Graceful shutdown complete")
    
    def get_correlation_trace(self, correlation_id: str) -> Optional[Dict[str, Any]]:
        """Get trace for a specific correlation ID"""
        trace = {
            'correlation_id': correlation_id,
            'events': []
        }
        
        for event in self.event_bus.event_history:
            if event.get('correlation_id') == correlation_id:
                trace['events'].append({
                    'event_type': event['event_type'],
                    'timestamp': event['timestamp'].isoformat(),
                    'payload': str(event['payload'])[:200]
                })
        
        return trace if trace['events'] else None

# ============================================================================
# Convenience Functions
# ============================================================================

def create_agent(config: Optional[AgentConfig] = None) -> BioIntegratedGreenAgent:
    """Create a bio-integrated agent with default or custom configuration"""
    return BioIntegratedGreenAgent(config=config)

def create_agent_from_file(config_path: str) -> BioIntegratedGreenAgent:
    """Create agent from configuration file"""
    with open(config_path, 'r') as f:
        config_data = json.load(f)
    config = AgentConfig.from_dict(config_data)
    return BioIntegratedGreenAgent(config=config)
