# File: quantum_integration/quantum-limit-graph-v2.4.0/limit-agentbench/src/enhancements/bio_inspired/enhanced_bio_core.py
# Complete enhanced file v6.1.0 with:
# - Genetic Optimizer for core parameters (health check interval, circuit breaker thresholds, etc.)
# - Decentralized decision-making via module‑local event handling and autonomous adjustments
# - Module marketplace for competition (replacement of underperforming modules)

"""
Enhanced Bio-Inspired Core v6.1.0
Complete implementation with graceful shutdown, module registry, lifecycle management,
health dashboard, configuration validation, module isolation, dynamic module loading,
predictive health forecasting, configuration versioning,
anomaly detection in performance metrics, event-driven communication,
Genetic Optimizer for core parameters, Decentralized decision-making,
and Module Marketplace for competition.
"""

import asyncio
import logging
import signal
import time
import random
from typing import Dict, Any, List, Optional, Tuple, Protocol, Callable, Set
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
import numpy as np
from collections import defaultdict, deque
import json
import os
import importlib
import hashlib
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler

logger = logging.getLogger(__name__)

# ============================================================================
# Service Protocols (Enhanced)
# ============================================================================

class TokenServiceProtocol(Protocol):
    def get_system_summary(self) -> Dict[str, Any]: ...
    def get_account_summary(self, account_id: str) -> Dict[str, Any]: ...
    def reserve_tokens(self, account_id: str, amount: float, consumer: Any, tenant_id: str, priority: int) -> Tuple[bool, List[str]]: ...
    def generate_tokens(self, account_id: str, source: Any, **kwargs) -> List[Any]: ...
    def consume_tokens(self, token_ids: List[str], consumer: Any, operation_success: bool) -> float: ...
    def recover_tokens(self, token_ids: List[str], completion_percentage: float) -> float: ...
    def create_account(self, account_id: str) -> Any: ...

class GradientServiceProtocol(Protocol):
    def get_field_strengths(self) -> Dict[str, float]: ...
    def pump_field(self, field_id: str, amount: float, source: str) -> None: ...
    def discharge_field(self, field_id: str, amount: float) -> float: ...
    def get_dominant_field(self) -> Tuple[str, float]: ...
    def get_field_stats(self) -> Dict[str, Any]: ...
    def find_root_cause(self, anomaly_field: str, max_depth: int) -> Dict[str, Any]: ...
    def explain_gradient_state(self, field_id: str) -> Dict[str, Any]: ...
    def forecast(self, field_id: str, horizon_seconds: float) -> Dict[str, Any]: ...
    def get_forecast_summary(self) -> Dict[str, Any]: ...

class CompartmentServiceProtocol(Protocol):
    def find_best_compartment(self, expert_type: str, task_complexity: float) -> Any: ...
    def get_ecosystem_stats(self) -> Dict[str, Any]: ...
    def create_compartment(self, expert_type: str, expert_instance: Any, resources: Any, parent_id: str) -> Any: ...
    def decommission_compartment(self, compartment_id: str) -> Dict[str, Any]: ...

class BiomassServiceProtocol(Protocol):
    def store_task(self, task_data: Dict[str, Any], ecoatp_cost: float, guarantee: Any, deadline: Any, initial_tier: Any) -> Tuple[bool, Optional[str]]: ...
    def retrieve_task(self, token_id: str) -> Tuple[Optional[Dict[str, Any]], float]: ...
    def get_storage_stats(self) -> Dict[str, Any]: ...
    def simulate_storage_scenario(self, scenario: Dict[str, Any]) -> Dict[str, Any]: ...

# ============================================================================
# Lifecycle Management
# ============================================================================

class LifecyclePhase(Enum):
    """Module lifecycle phases"""
    UNREGISTERED = "unregistered"
    REGISTERED = "registered"
    INITIALIZING = "initializing"
    INITIALIZED = "initialized"
    HEALTH_CHECKING = "health_checking"
    RUNNING = "running"
    PAUSING = "pausing"
    PAUSED = "paused"
    STOPPING = "stopping"
    STOPPED = "stopped"
    ERROR = "error"
    LOADING = "loading"

@dataclass
class ModuleEntry:
    """Module registry entry with lifecycle management (Enhanced)"""
    name: str
    module: Any = None
    phase: LifecyclePhase = LifecyclePhase.REGISTERED
    dependencies: List[str] = field(default_factory=list)
    dependents: List[str] = field(default_factory=list)
    health_check: Optional[Callable] = None
    init_timeout: float = 30.0
    shutdown_timeout: float = 10.0
    init_started: Optional[datetime] = None
    init_completed: Optional[datetime] = None
    error_message: Optional[str] = None
    health_status: str = "unknown"
    circuit_breaker_state: str = "closed"
    failure_count: int = 0
    last_failure: Optional[datetime] = None
    metrics: Dict[str, Any] = field(default_factory=dict)
    module_path: Optional[str] = None
    version: str = "1.0.0"
    loaded_at: Optional[datetime] = None
    predicted_health: Optional[float] = None
    failure_probability: float = 0.0
    health_trend: str = "stable"

# ============================================================================
# Event Bus for Event-Driven Communication
# ============================================================================

@dataclass
class CoreEvent:
    """Event for event-driven communication between services"""
    event_type: str
    source: str
    payload: Dict[str, Any]
    timestamp: datetime = field(default_factory=datetime.utcnow)
    correlation_id: Optional[str] = None

class CoreEventBus:
    """
    Event bus for event-driven communication between bio-inspired services.
    
    Features:
    - Publish/subscribe pattern
    - Event filtering
    - Priority queuing
    - Event correlation
    """
    
    def __init__(self):
        self.subscribers: Dict[str, List[Callable]] = defaultdict(list)
        self.event_history: deque = deque(maxlen=10000)
        self.event_queue: asyncio.PriorityQueue = asyncio.PriorityQueue()
        self._lock = asyncio.Lock()
        self._running = True
        self._processor_task = None
        
        # Start event processing
        self._processor_task = asyncio.create_task(self._process_events())
        
        logger.info("Core Event Bus initialized")
    
    def subscribe(self, event_type: str, callback: Callable):
        """Subscribe to an event type"""
        self.subscribers[event_type].append(callback)
        logger.debug(f"Subscribed to {event_type}")
    
    def unsubscribe(self, event_type: str, callback: Callable):
        """Unsubscribe from an event type"""
        if event_type in self.subscribers:
            self.subscribers[event_type].remove(callback)
    
    async def publish(self, event: CoreEvent):
        """Publish an event"""
        async with self._lock:
            await self.event_queue.put((0, event))
            self.event_history.append(event)
            logger.debug(f"Event published: {event.event_type} from {event.source}")
    
    async def _process_events(self):
        """Process events from the queue"""
        while self._running:
            try:
                priority, event = await self.event_queue.get()
                
                if event.event_type in self.subscribers:
                    for callback in self.subscribers[event.event_type]:
                        try:
                            if asyncio.iscoroutinefunction(callback):
                                await callback(event)
                            else:
                                callback(event)
                        except Exception as e:
                            logger.error(f"Event callback error: {str(e)}")
                
                self.event_queue.task_done()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Event processing error: {str(e)}")
    
    def shutdown(self):
        """Shutdown the event bus"""
        self._running = False
        if self._processor_task:
            self._processor_task.cancel()
        logger.info("Core Event Bus shutdown")
    
    def get_event_stats(self) -> Dict[str, Any]:
        """Get event bus statistics"""
        return {
            'total_events': len(self.event_history),
            'subscribers': {k: len(v) for k, v in self.subscribers.items()},
            'queue_size': self.event_queue.qsize(),
            'is_running': self._running
        }

# ============================================================================
# Predictive Health Forecasting
# ============================================================================

class PredictiveHealthForecaster:
    """
    Predictive health forecasting for proactive intervention.
    
    Features:
    - ML-based health prediction
    - Failure probability estimation
    - Trend analysis
    - Confidence scoring
    """
    
    def __init__(self):
        self.model = IsolationForest(contamination=0.1, random_state=42)
        self.scaler = StandardScaler()
        self.is_trained = False
        self.history: List[Dict] = []
        self.predictions: Dict[str, Dict] = {}
        self._lock = asyncio.Lock()
        
        logger.info("Predictive Health Forecaster initialized")
    
    def record_health_data(self, module_name: str, metrics: Dict[str, float]):
        """Record health data for training"""
        self.history.append({
            'module': module_name,
            'timestamp': datetime.utcnow(),
            **metrics
        })
        if len(self.history) > 1000:
            self.history = self.history[-1000:]
    
    async def train(self):
        """Train the health prediction model"""
        if len(self.history) < 50:
            return {'status': 'insufficient_data', 'samples': len(self.history)}
        
        async with self._lock:
            # Prepare features
            X = []
            for i in range(10, len(self.history) - 1):
                features = []
                for j in range(10):
                    data = self.history[i - j]
                    features.extend([
                        data.get('health_score', 0.5),
                        data.get('success_rate', 0.5),
                        data.get('token_balance', 500) / 1000,
                        data.get('error_rate', 0.01)
                    ])
                X.append(features)
            
            if len(X) < 20:
                return {'status': 'insufficient_training_data', 'samples': len(X)}
            
            X = np.array(X)
            X_scaled = self.scaler.fit_transform(X)
            
            self.model.fit(X_scaled)
            self.is_trained = True
            
            logger.info(f"Health forecaster trained on {len(X)} samples")
            return {'status': 'success', 'samples': len(X)}
    
    async def predict_health(self, current_metrics: Dict[str, float]) -> Dict[str, Any]:
        """Predict future health"""
        if not self.is_trained:
            return {'predicted_health': 0.5, 'failure_probability': 0.0, 'confidence': 0.0}
        
        async with self._lock:
            # Prepare features
            features = []
            for key in ['health_score', 'success_rate', 'token_balance', 'error_rate']:
                if key in current_metrics:
                    features.append(current_metrics[key])
                else:
                    features.append(0.5)
            
            # Ensure correct feature count
            while len(features) < 4:
                features.append(0.5)
            
            features_array = np.array([features])
            features_scaled = self.scaler.transform(features_array)
            
            # Predict anomaly
            prediction = self.model.predict(features_scaled)[0]
            is_anomalous = prediction == -1
            
            # Calculate confidence
            decision_function = self.model.decision_function(features_scaled)[0]
            confidence = abs(decision_function) / (abs(decision_function) + 1)
            
            # Determine trend
            if len(self.history) > 20:
                recent_health = [h.get('health_score', 0.5) for h in self.history[-20:]]
                trend_slope = np.polyfit(range(len(recent_health)), recent_health, 1)[0]
                trend = 'improving' if trend_slope > 0.01 else 'declining' if trend_slope < -0.01 else 'stable'
            else:
                trend = 'stable'
            
            result = {
                'predicted_health': 0.3 if is_anomalous else 0.7,
                'failure_probability': 0.8 if is_anomalous else 0.2,
                'trend': trend,
                'confidence': confidence,
                'is_anomalous': is_anomalous
            }
            
            self.predictions[str(datetime.utcnow().timestamp())] = result
            return result

# ============================================================================
# Configuration Version Manager
# ============================================================================

class ConfigurationVersionManager:
    """
    Configuration versioning for rollback capability.
    
    Features:
    - Version tracking
    - Rollback to previous versions
    - Configuration history
    - Diff capability
    """
    
    def __init__(self, storage_dir: str = "./config_versions"):
        self.storage_dir = storage_dir
        self.versions: List[Dict] = []
        self.current_version: Optional[str] = None
        self._lock = asyncio.Lock()
        
        os.makedirs(storage_dir, exist_ok=True)
        self._load_version_history()
        
        logger.info("Configuration Version Manager initialized")
    
    def _load_version_history(self):
        """Load version history from disk"""
        history_path = os.path.join(self.storage_dir, "version_history.json")
        if os.path.exists(history_path):
            try:
                with open(history_path, 'r') as f:
                    data = json.load(f)
                    self.versions = data.get('versions', [])
                    self.current_version = data.get('current_version')
                logger.info(f"Loaded {len(self.versions)} configuration versions")
            except Exception as e:
                logger.warning(f"Failed to load version history: {e}")
    
    def _save_version_history(self):
        """Save version history to disk"""
        history_path = os.path.join(self.storage_dir, "version_history.json")
        try:
            with open(history_path, 'w') as f:
                json.dump({
                    'versions': self.versions,
                    'current_version': self.current_version
                }, f, indent=2)
        except Exception as e:
            logger.error(f"Failed to save version history: {e}")
    
    def save_version(self, config: Dict[str, Any], description: str = "") -> str:
        """Save a new configuration version"""
        version_id = hashlib.md5(
            f"{datetime.utcnow().timestamp()}".encode()
        ).hexdigest()[:12]
        
        version_data = {
            'version_id': version_id,
            'timestamp': datetime.utcnow().isoformat(),
            'config': config,
            'description': description,
            'parent': self.current_version
        }
        
        # Save to file
        version_path = os.path.join(self.storage_dir, f"config_{version_id}.json")
        with open(version_path, 'w') as f:
            json.dump(version_data, f, indent=2)
        
        self.versions.append({
            'version_id': version_id,
            'timestamp': version_data['timestamp'],
            'description': description,
            'parent': self.current_version
        })
        
        self.current_version = version_id
        self._save_version_history()
        
        logger.info(f"Configuration version saved: {version_id}")
        return version_id
    
    def rollback_to_version(self, version_id: str) -> Optional[Dict[str, Any]]:
        """Rollback to a specific configuration version"""
        version_path = os.path.join(self.storage_dir, f"config_{version_id}.json")
        if not os.path.exists(version_path):
            logger.error(f"Version {version_id} not found")
            return None
        
        try:
            with open(version_path, 'r') as f:
                version_data = json.load(f)
            
            self.current_version = version_id
            self._save_version_history()
            
            logger.info(f"Rolled back to configuration version: {version_id}")
            return version_data.get('config')
            
        except Exception as e:
            logger.error(f"Failed to rollback to version {version_id}: {e}")
            return None
    
    def get_version_history(self, limit: int = 10) -> List[Dict]:
        """Get version history"""
        return self.versions[-limit:]
    
    def get_version_diff(self, version_a: str, version_b: str) -> Dict[str, Any]:
        """Get diff between two versions"""
        config_a = self._load_version_data(version_a)
        config_b = self._load_version_data(version_b)
        
        if not config_a or not config_b:
            return {'error': 'Version not found'}
        
        diff = {
            'added': {},
            'removed': {},
            'changed': {}
        }
        
        all_keys = set(config_a.keys()) | set(config_b.keys())
        
        for key in all_keys:
            if key not in config_a:
                diff['added'][key] = config_b[key]
            elif key not in config_b:
                diff['removed'][key] = config_a[key]
            elif config_a[key] != config_b[key]:
                diff['changed'][key] = {'from': config_a[key], 'to': config_b[key]}
        
        return diff
    
    def _load_version_data(self, version_id: str) -> Optional[Dict]:
        """Load version data from disk"""
        version_path = os.path.join(self.storage_dir, f"config_{version_id}.json")
        if os.path.exists(version_path):
            try:
                with open(version_path, 'r') as f:
                    data = json.load(f)
                    return data.get('config')
            except Exception:
                return None
        return None

# ============================================================================
# Performance Anomaly Detector
# ============================================================================

class PerformanceAnomalyDetector:
    """
    Anomaly detection in performance metrics.
    
    Features:
    - Statistical anomaly detection
    - Trend-based anomaly detection
    - Alert generation
    - Historical analysis
    """
    
    def __init__(self):
        self.metric_history: Dict[str, List[float]] = defaultdict(list)
        self.anomalies: List[Dict] = []
        self._lock = asyncio.Lock()
        self.zscore_threshold = 3.0
        self.trend_threshold = 0.2
        
        logger.info("Performance Anomaly Detector initialized")
    
    def record_metric(self, metric_name: str, value: float):
        """Record a metric value"""
        self.metric_history[metric_name].append(value)
        if len(self.metric_history[metric_name]) > 1000:
            self.metric_history[metric_name] = self.metric_history[metric_name][-1000:]
    
    async def detect_anomalies(self, metric_name: str) -> List[Dict]:
        """Detect anomalies in a metric"""
        if metric_name not in self.metric_history or len(self.metric_history[metric_name]) < 10:
            return []
        
        values = self.metric_history[metric_name][-50:]
        
        # Z-score detection
        mean = np.mean(values)
        std = np.std(values)
        anomalies = []
        
        if std > 0:
            z_scores = [(v - mean) / std for v in values[-10:]]
            for i, zscore in enumerate(z_scores):
                if abs(zscore) > self.zscore_threshold:
                    anomalies.append({
                        'metric': metric_name,
                        'value': values[-10 + i],
                        'zscore': zscore,
                        'timestamp': datetime.utcnow().isoformat(),
                        'type': 'zscore'
                    })
        
        # Trend detection
        if len(values) > 20:
            recent = values[-20:]
            slope = np.polyfit(range(len(recent)), recent, 1)[0]
            if abs(slope) > self.trend_threshold:
                anomalies.append({
                    'metric': metric_name,
                    'slope': slope,
                    'timestamp': datetime.utcnow().isoformat(),
                    'type': 'trend',
                    'direction': 'increasing' if slope > 0 else 'decreasing'
                })
        
        return anomalies
    
    async def get_anomaly_report(self) -> Dict[str, Any]:
        """Get anomaly detection report"""
        report = {'timestamp': datetime.utcnow().isoformat(), 'anomalies': []}
        
        for metric_name in self.metric_history:
            anomalies = await self.detect_anomalies(metric_name)
            if anomalies:
                report['anomalies'].extend(anomalies)
        
        return report

# ============================================================================
# NEW: Genetic Optimizer for Core Parameters
# ============================================================================

class CoreGeneticOptimizer:
    """
    Genetic optimizer for core parameters:
    - health_check_interval_seconds
    - circuit_breaker_threshold (failure count before opening)
    - predictive_health_retrain_interval
    - anomaly_zscore_threshold
    - module_retirement_threshold (health score below which a module is considered for replacement)
    """
    
    def __init__(self, core: 'EnhancedBioInspiredCore'):
        self.core = core
        self.population_size = 20
        self.mutation_rate = 0.2
        self.crossover_rate = 0.7
        self.generations = 10
        self.tournament_size = 3
        self.best_individual = None
        self.best_fitness = -float('inf')
        self.evolution_history = []
        
        # Parameter bounds
        self.param_bounds = {
            'health_check_interval_seconds': (10, 120),
            'circuit_breaker_threshold': (3, 10),
            'predictive_health_retrain_interval': (120, 900),
            'anomaly_zscore_threshold': (2.0, 5.0),
            'module_retirement_threshold': (0.1, 0.4)
        }
        logger.info("Core Genetic Optimizer initialized")
    
    def _initialize_individual(self) -> Dict:
        """Generate random parameter set."""
        ind = {}
        for key, (low, high) in self.param_bounds.items():
            ind[key] = random.uniform(low, high)
        # Ensure integer parameters
        ind['circuit_breaker_threshold'] = int(ind['circuit_breaker_threshold'])
        ind['health_check_interval_seconds'] = int(ind['health_check_interval_seconds'])
        ind['predictive_health_retrain_interval'] = int(ind['predictive_health_retrain_interval'])
        return ind
    
    def _initialize_population(self) -> List[Dict]:
        return [self._initialize_individual() for _ in range(self.population_size)]
    
    def _fitness(self, individual: Dict) -> float:
        """Fitness based on system health, uptime, and response time."""
        # Temporarily apply parameters
        self._apply_individual(individual)
        # Evaluate fitness
        status = self.core.get_system_status()
        modules_health = self.core.registry.health_check_all()
        # Components:
        # - Average health score of modules
        health_scores = [1.0 if s['status'] == 'healthy' else 0.5 if s['status'] == 'degraded' else 0.0 for s in modules_health.values()]
        avg_health = np.mean(health_scores) if health_scores else 0.5
        
        # - Uptime (longer is better)
        uptime = status.get('uptime_seconds', 0)
        uptime_score = min(1.0, uptime / 86400)  # 1 day = 1.0
        
        # - Circuit breaker open count (fewer open is better)
        open_circuits = sum(1 for m in self.core.registry.modules.values() if m.circuit_breaker_state == 'open')
        circuit_score = max(0, 1.0 - open_circuits / max(1, len(self.core.registry.modules) * 0.5))
        
        # - Anomaly count (fewer is better)
        anomaly_report = asyncio.run(self.core._anomaly_detector.get_anomaly_report())
        anomaly_count = len(anomaly_report.get('anomalies', []))
        anomaly_score = max(0, 1.0 - anomaly_count / 20)
        
        # Combine
        fitness = 0.4 * avg_health + 0.3 * uptime_score + 0.2 * circuit_score + 0.1 * anomaly_score
        self._restore_original_parameters()
        return fitness
    
    def _apply_individual(self, individual: Dict):
        """Temporarily apply parameters to core."""
        self._original_params = {
            'health_check_interval_seconds': self.core.config.health_check_interval_seconds,
            'circuit_breaker_threshold': self.core.registry._circuit_breaker_threshold if hasattr(self.core.registry, '_circuit_breaker_threshold') else 5,
            'predictive_health_retrain_interval': self.core._predictive_health_retrain_interval if hasattr(self.core, '_predictive_health_retrain_interval') else 300,
            'anomaly_zscore_threshold': self.core._anomaly_detector.zscore_threshold if hasattr(self.core._anomaly_detector, 'zscore_threshold') else 3.0,
            'module_retirement_threshold': self.core._module_retirement_threshold if hasattr(self.core, '_module_retirement_threshold') else 0.2
        }
        # Apply new values
        self.core.config.health_check_interval_seconds = individual['health_check_interval_seconds']
        if hasattr(self.core.registry, '_circuit_breaker_threshold'):
            self.core.registry._circuit_breaker_threshold = individual['circuit_breaker_threshold']
        self.core._predictive_health_retrain_interval = individual['predictive_health_retrain_interval']
        if hasattr(self.core._anomaly_detector, 'zscore_threshold'):
            self.core._anomaly_detector.zscore_threshold = individual['anomaly_zscore_threshold']
        self.core._module_retirement_threshold = individual['module_retirement_threshold']
    
    def _restore_original_parameters(self):
        if hasattr(self, '_original_params'):
            self.core.config.health_check_interval_seconds = self._original_params['health_check_interval_seconds']
            if hasattr(self.core.registry, '_circuit_breaker_threshold'):
                self.core.registry._circuit_breaker_threshold = self._original_params['circuit_breaker_threshold']
            self.core._predictive_health_retrain_interval = self._original_params['predictive_health_retrain_interval']
            if hasattr(self.core._anomaly_detector, 'zscore_threshold'):
                self.core._anomaly_detector.zscore_threshold = self._original_params['anomaly_zscore_threshold']
            self.core._module_retirement_threshold = self._original_params['module_retirement_threshold']
    
    def _select(self, population: List[Dict], fitness_scores: List[float]) -> Dict:
        tournament = random.sample(range(len(population)), self.tournament_size)
        best_idx = max(tournament, key=lambda i: fitness_scores[i])
        return population[best_idx]
    
    def _crossover(self, parent1: Dict, parent2: Dict) -> Dict:
        child = {}
        for key in parent1:
            if random.random() < 0.5:
                child[key] = parent1[key]
            else:
                child[key] = parent2[key]
            if random.random() < 0.3:
                child[key] = (parent1[key] + parent2[key]) / 2
        # Ensure integer params
        child['circuit_breaker_threshold'] = int(child['circuit_breaker_threshold'])
        child['health_check_interval_seconds'] = int(child['health_check_interval_seconds'])
        child['predictive_health_retrain_interval'] = int(child['predictive_health_retrain_interval'])
        return child
    
    def _mutate(self, individual: Dict) -> Dict:
        mutated = individual.copy()
        for key, (low, high) in self.param_bounds.items():
            if random.random() < self.mutation_rate:
                delta = random.uniform(-(high-low)*0.1, (high-low)*0.1)
                mutated[key] = max(low, min(high, mutated[key] + delta))
        # Ensure integer params
        mutated['circuit_breaker_threshold'] = int(mutated['circuit_breaker_threshold'])
        mutated['health_check_interval_seconds'] = int(mutated['health_check_interval_seconds'])
        mutated['predictive_health_retrain_interval'] = int(mutated['predictive_health_retrain_interval'])
        return mutated
    
    def _evolve_one_generation(self, population: List[Dict]) -> List[Dict]:
        fitness_scores = [self._fitness(ind) for ind in population]
        new_population = []
        # Elitism
        best_idx = max(range(len(population)), key=lambda i: fitness_scores[i])
        new_population.append(population[best_idx])
        while len(new_population) < self.population_size:
            if random.random() < self.crossover_rate:
                parent1 = self._select(population, fitness_scores)
                parent2 = self._select(population, fitness_scores)
                child = self._crossover(parent1, parent2)
                child = self._mutate(child)
                new_population.append(child)
            else:
                parent = self._select(population, fitness_scores)
                new_population.append(parent.copy())
        return new_population
    
    async def evolve(self, generations: Optional[int] = None) -> Dict:
        if generations is None:
            generations = self.generations
        population = self._initialize_population()
        best_fitness = -float('inf')
        best_ind = None
        for gen in range(generations):
            population = self._evolve_one_generation(population)
            fitness_scores = [self._fitness(ind) for ind in population]
            gen_best = max(range(len(population)), key=lambda i: fitness_scores[i])
            if fitness_scores[gen_best] > best_fitness:
                best_fitness = fitness_scores[gen_best]
                best_ind = population[gen_best]
            logger.debug(f"Gen {gen+1}: best fitness = {fitness_scores[gen_best]:.4f}")
        if best_fitness > self.best_fitness:
            self.best_fitness = best_fitness
            self.best_individual = best_ind
            self._apply_individual(best_ind)
            logger.info(f"Applied best individual with fitness {best_fitness:.4f}")
        self.evolution_history.append({
            'timestamp': datetime.utcnow(),
            'best_fitness': best_fitness
        })
        return {'best_fitness': best_fitness, 'best_individual': best_ind}
    
    def get_status(self) -> Dict:
        return {
            'best_fitness': self.best_fitness,
            'best_individual': self.best_individual,
            'history': self.evolution_history[-10:]
        }

# ============================================================================
# NEW: Decentralized Module Base Class
# ============================================================================

class DecentralizedModule:
    """
    Base class for modules that can make local decisions based on events.
    """
    def __init__(self, module_name: str, core: 'EnhancedBioInspiredCore'):
        self.module_name = module_name
        self.core = core
        self.local_state: Dict[str, Any] = {}
        self.event_subscriptions: List[str] = []
    
    async def on_event(self, event: CoreEvent):
        """Handle an event. Override in subclasses."""
        pass
    
    async def local_decision(self, context: Dict[str, Any]) -> Dict[str, Any]:
        """Make a local decision based on context. Override in subclasses."""
        return {'action': 'noop', 'reason': 'Base class'}

# ============================================================================
# NEW: Module Marketplace for Competition
# ============================================================================

class ModuleMarketplace:
    """
    Marketplace where modules compete for resources.
    Underperforming modules can be replaced by better alternatives.
    """
    
    def __init__(self, core: 'EnhancedBioInspiredCore'):
        self.core = core
        self.module_scores: Dict[str, float] = {}
        self.replacement_history: deque = deque(maxlen=100)
        self.competition_interval = 3600  # 1 hour
        self._lock = asyncio.Lock()
        logger.info("Module Marketplace initialized")
    
    async def evaluate_modules(self) -> Dict[str, float]:
        """Evaluate all modules and assign a score (0-1, higher is better)."""
        scores = {}
        for name, entry in self.core.registry.modules.items():
            # Score based on health status, failure count, and predicted health
            health = 1.0 if entry.health_status == 'healthy' else 0.5 if entry.health_status == 'degraded' else 0.0
            failure_penalty = min(1.0, entry.failure_count / 10)
            predicted = entry.predicted_health if entry.predicted_health is not None else 0.5
            score = 0.4 * health + 0.3 * (1 - failure_penalty) + 0.3 * predicted
            scores[name] = score
        self.module_scores = scores
        return scores
    
    async def run_competition(self):
        """
        Identify underperforming modules and suggest replacements.
        """
        async with self._lock:
            scores = await self.evaluate_modules()
            if not scores:
                return
            
            # Find modules with score below threshold
            retirement_threshold = getattr(self.core, '_module_retirement_threshold', 0.2)
            underperformers = [name for name, score in scores.items() if score < retirement_threshold]
            
            # For each underperformer, try to find a replacement
            replacements = []
            for name in underperformers:
                # Check if there are any modules with same dependencies that are better
                entry = self.core.registry.modules.get(name)
                if not entry:
                    continue
                # Find alternative: a module with similar function (e.g., same dependency pattern)
                alternatives = []
                for other_name, other_entry in self.core.registry.modules.items():
                    if other_name == name:
                        continue
                    if other_entry.dependencies == entry.dependencies:
                        alternatives.append((other_name, scores.get(other_name, 0.5)))
                if alternatives:
                    # Choose best alternative that is not underperforming
                    best_alt = max(alternatives, key=lambda x: x[1])
                    if best_alt[1] > scores[name]:
                        replacements.append({
                            'old_module': name,
                            'new_module': best_alt[0],
                            'score_old': scores[name],
                            'score_new': best_alt[1]
                        })
            
            if replacements:
                logger.info(f"Module marketplace: {len(replacements)} replacements suggested")
                self.replacement_history.extend(replacements)
                # We don't automatically replace; we suggest to the core admin
                # The core can call `apply_replacement()` manually or via event.
                # For now, we'll publish an event.
                for rep in replacements:
                    await self.core.event_bus.publish(CoreEvent(
                        event_type='module_replacement_suggested',
                        source='module_marketplace',
                        payload=rep
                    ))
    
    def get_marketplace_stats(self) -> Dict[str, Any]:
        return {
            'module_scores': self.module_scores,
            'replacement_history': list(self.replacement_history)[-10:],
            'competition_interval': self.competition_interval
        }

# ============================================================================
# Module Registry (Enhanced)
# ============================================================================

class ModuleRegistry:
    """
    Dynamic module registry with lifecycle management, health checking,
    circuit breaker protection, and dynamic loading.
    Enhanced with evolvable circuit breaker threshold.
    """
    
    def __init__(self):
        self.modules: Dict[str, ModuleEntry] = {}
        self.startup_order: List[str] = []
        self.shutdown_order: List[str] = []
        self._initialized = False
        self._init_lock = asyncio.Lock()
        self.loaded_modules: Set[str] = set()
        self.module_paths: Dict[str, str] = {}
        self.health_forecaster = PredictiveHealthForecaster()
        self._circuit_breaker_threshold = 5  # evolvable
        logger.info("Module Registry initialized")
    
    def register(self, name: str, module: Any = None, dependencies: List[str] = None,
                health_check: Callable = None, init_timeout: float = 30.0,
                shutdown_timeout: float = 10.0,
                module_path: Optional[str] = None) -> 'ModuleEntry':
        """Register a module with the registry"""
        if name in self.modules:
            logger.warning(f"Module {name} already registered, updating")
        
        entry = ModuleEntry(
            name=name,
            module=module,
            dependencies=dependencies or [],
            health_check=health_check,
            init_timeout=init_timeout,
            shutdown_timeout=shutdown_timeout,
            module_path=module_path
        )
        
        self.modules[name] = entry
        
        # Update dependency graph
        for dep in entry.dependencies:
            if dep in self.modules:
                self.modules[dep].dependents.append(name)
        
        logger.info(f"Module registered: {name} (deps: {entry.dependencies})")
        return entry
    
    async def load_module(self, name: str, module_path: str) -> bool:
        """Dynamically load a module at runtime"""
        if name in self.loaded_modules:
            logger.warning(f"Module {name} already loaded")
            return True
        
        try:
            # Import module dynamically
            spec = importlib.util.spec_from_file_location(name, module_path)
            if not spec or not spec.loader:
                logger.error(f"Failed to load module {name}: invalid spec")
                return False
            
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
            
            # Register module
            entry = self.register(name, module, module_path=module_path)
            entry.phase = LifecyclePhase.LOADING
            entry.loaded_at = datetime.utcnow()
            
            # Initialize if has initialize method
            if hasattr(module, 'initialize'):
                await module.initialize()
            
            entry.phase = LifecyclePhase.INITIALIZED
            self.loaded_modules.add(name)
            self.module_paths[name] = module_path
            
            logger.info(f"Dynamic module loaded: {name}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to load module {name}: {str(e)}")
            return False
    
    def unload_module(self, name: str) -> bool:
        """Unload a dynamically loaded module"""
        if name not in self.loaded_modules:
            logger.warning(f"Module {name} not loaded")
            return False
        
        try:
            entry = self.modules.get(name)
            if entry:
                if hasattr(entry.module, 'shutdown'):
                    asyncio.run(entry.module.shutdown())
                
                entry.phase = LifecyclePhase.STOPPED
                self.loaded_modules.remove(name)
                del self.module_paths[name]
                del self.modules[name]
                
                logger.info(f"Dynamic module unloaded: {name}")
                return True
            
        except Exception as e:
            logger.error(f"Failed to unload module {name}: {str(e)}")
            return False
    
    def get(self, name: str) -> Optional[Any]:
        """Get module instance by name"""
        entry = self.modules.get(name)
        return entry.module if entry else None
    
    def get_entry(self, name: str) -> Optional[ModuleEntry]:
        """Get module entry by name"""
        return self.modules.get(name)
    
    def list_modules(self) -> List[str]:
        """List all registered modules"""
        return list(self.modules.keys())
    
    def get_dependency_graph(self) -> Dict[str, List[str]]:
        """Get module dependency graph"""
        return {
            name: entry.dependencies
            for name, entry in self.modules.items()
        }
    
    def get_startup_order(self) -> List[str]:
        """Calculate topological startup order"""
        visited = set()
        order = []
        
        def visit(name):
            if name in visited:
                return
            visited.add(name)
            entry = self.modules.get(name)
            if entry:
                for dep in entry.dependencies:
                    if dep in self.modules:
                        visit(dep)
            order.append(name)
        
        for name in self.modules:
            visit(name)
        
        self.startup_order = order
        return order
    
    def get_shutdown_order(self) -> List[str]:
        """Calculate reverse topological shutdown order"""
        startup = self.get_startup_order()
        self.shutdown_order = list(reversed(startup))
        return self.shutdown_order
    
    async def initialize_all(self, parallel: bool = False) -> Dict[str, bool]:
        """Initialize all modules in dependency order"""
        async with self._init_lock:
            if self._initialized:
                logger.warning("Modules already initialized")
                return {}
            
            order = self.get_startup_order()
            results = {}
            
            for name in order:
                entry = self.modules[name]
                
                if entry.phase == LifecyclePhase.INITIALIZED:
                    results[name] = True
                    continue
                
                try:
                    entry.phase = LifecyclePhase.INITIALIZING
                    entry.init_started = datetime.utcnow()
                    
                    if hasattr(entry.module, 'initialize'):
                        await asyncio.wait_for(
                            entry.module.initialize(),
                            timeout=entry.init_timeout
                        )
                    
                    entry.phase = LifecyclePhase.INITIALIZED
                    entry.init_completed = datetime.utcnow()
                    
                    if entry.health_check:
                        try:
                            is_healthy = entry.health_check()
                            entry.health_status = "healthy" if is_healthy else "degraded"
                        except Exception:
                            entry.health_status = "unknown"
                    
                    results[name] = True
                    logger.info(f"Module {name} initialized successfully")
                    
                except asyncio.TimeoutError:
                    entry.phase = LifecyclePhase.ERROR
                    entry.error_message = f"Initialization timeout ({entry.init_timeout}s)"
                    results[name] = False
                    logger.error(f"Module {name} initialization timed out")
                    
                except Exception as e:
                    entry.phase = LifecyclePhase.ERROR
                    entry.error_message = str(e)
                    results[name] = False
                    logger.error(f"Module {name} initialization failed: {str(e)}")
            
            all_ok = all(results.values())
            if all_ok:
                self._initialized = True
                logger.info("All modules initialized successfully")
            else:
                failed = [name for name, ok in results.items() if not ok]
                logger.warning(f"Some modules failed to initialize: {failed}")
            
            return results
    
    async def shutdown_all(self) -> Dict[str, bool]:
        """Shutdown all modules in reverse dependency order"""
        order = self.get_shutdown_order()
        results = {}
        
        for name in order:
            entry = self.modules[name]
            
            if entry.phase == LifecyclePhase.STOPPED:
                results[name] = True
                continue
            
            try:
                entry.phase = LifecyclePhase.STOPPING
                
                if hasattr(entry.module, 'shutdown'):
                    await asyncio.wait_for(
                        entry.module.shutdown(),
                        timeout=entry.shutdown_timeout
                    )
                
                entry.phase = LifecyclePhase.STOPPED
                results[name] = True
                logger.info(f"Module {name} shutdown successfully")
                
            except asyncio.TimeoutError:
                entry.phase = LifecyclePhase.ERROR
                results[name] = False
                logger.error(f"Module {name} shutdown timed out")
                
            except Exception as e:
                entry.phase = LifecyclePhase.ERROR
                results[name] = False
                logger.error(f"Module {name} shutdown failed: {str(e)}")
        
        self._initialized = False
        return results
    
    def health_check_all(self) -> Dict[str, Dict[str, Any]]:
        """Run health checks on all modules"""
        results = {}
        
        for name, entry in self.modules.items():
            if entry.health_check:
                try:
                    is_healthy = entry.health_check()
                    entry.health_status = "healthy" if is_healthy else "degraded"
                except Exception as e:
                    entry.health_status = "error"
                    entry.error_message = str(e)
            else:
                entry.health_status = "unknown"
            
            results[name] = {
                'status': entry.health_status,
                'phase': entry.phase.value,
                'error': entry.error_message,
                'circuit_breaker': entry.circuit_breaker_state,
                'uptime': (datetime.utcnow() - entry.init_completed).total_seconds() if entry.init_completed else 0,
                'predicted_health': entry.predicted_health,
                'failure_probability': entry.failure_probability,
                'health_trend': entry.health_trend
            }
        
        return results
    
    def record_failure(self, name: str):
        """Record a module failure for circuit breaker (using evolvable threshold)"""
        entry = self.modules.get(name)
        if not entry:
            return
        
        entry.failure_count += 1
        entry.last_failure = datetime.utcnow()
        
        if entry.failure_count >= self._circuit_breaker_threshold and entry.circuit_breaker_state == "closed":
            entry.circuit_breaker_state = "open"
            logger.warning(f"Circuit breaker OPEN for module {name} ({entry.failure_count} failures)")
    
    def record_success(self, name: str):
        """Record a module success for circuit breaker"""
        entry = self.modules.get(name)
        if not entry:
            return
        
        if entry.circuit_breaker_state == "half_open":
            entry.circuit_breaker_state = "closed"
            entry.failure_count = 0
            logger.info(f"Circuit breaker CLOSED for module {name}")
    
    def update_predictive_health(self, name: str, metrics: Dict[str, float]):
        """Update predictive health for a module"""
        entry = self.modules.get(name)
        if not entry:
            return
        
        # Record health data
        self.health_forecaster.record_health_data(name, metrics)
        
        # Get prediction
        prediction = asyncio.run(self.health_forecaster.predict_health(metrics))
        entry.predicted_health = prediction.get('predicted_health', 0.5)
        entry.failure_probability = prediction.get('failure_probability', 0.0)
        entry.health_trend = prediction.get('trend', 'stable')
    
    def get_registry_stats(self) -> Dict[str, Any]:
        """Get registry statistics"""
        return {
            'total_modules': len(self.modules),
            'initialized': sum(1 for m in self.modules.values() if m.phase == LifecyclePhase.INITIALIZED),
            'running': sum(1 for m in self.modules.values() if m.phase == LifecyclePhase.RUNNING),
            'error': sum(1 for m in self.modules.values() if m.phase == LifecyclePhase.ERROR),
            'circuit_breakers_open': sum(1 for m in self.modules.values() if m.circuit_breaker_state == "open"),
            'loaded_modules': len(self.loaded_modules),
            'modules': {
                name: {
                    'phase': entry.phase.value,
                    'health': entry.health_status,
                    'circuit_breaker': entry.circuit_breaker_state,
                    'dependencies': entry.dependencies,
                    'dependents': entry.dependents,
                    'predicted_health': entry.predicted_health,
                    'failure_probability': entry.failure_probability,
                    'health_trend': entry.health_trend
                }
                for name, entry in self.modules.items()
            }
        }

# ============================================================================
# Configuration Manager (Enhanced)
# ============================================================================

@dataclass
class CoreConfig:
    """Core configuration with validation"""
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
    enable_chaos_engineering: bool = False
    
    # State persistence
    enable_state_persistence: bool = True
    state_save_interval_seconds: int = 300
    state_directory: str = "./agent_state"
    
    # Health checks
    health_check_interval_seconds: int = 30
    
    # Versioning
    version: str = "1.0.0"
    version_description: str = ""
    
    def validate(self) -> Tuple[bool, List[str]]:
        """Validate configuration and return (is_valid, issues)"""
        issues = []
        
        if self.token_base_generation_rate <= 0:
            issues.append("token_base_generation_rate must be positive")
        if self.token_hoarding_threshold < 1.0:
            issues.append("token_hoarding_threshold should be at least 1.0")
        if self.compartments_per_expert_type < 1:
            issues.append("compartments_per_expert_type must be at least 1")
        if self.carbon_leakage_rate <= 0:
            issues.append("carbon_leakage_rate must be positive")
        if self.atp_c_ring_size < 8 or self.atp_c_ring_size > 17:
            issues.append("atp_c_ring_size should be between 8 and 17")
        if self.state_save_interval_seconds < 60:
            issues.append("state_save_interval_seconds should be at least 60")
        
        return len(issues) == 0, issues
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return {
            'token_base_generation_rate': self.token_base_generation_rate,
            'token_hoarding_threshold': self.token_hoarding_threshold,
            'token_emergency_threshold': self.token_emergency_threshold,
            'token_target_utilization': self.token_target_utilization,
            'compartments_per_expert_type': self.compartments_per_expert_type,
            'max_total_compartments': self.max_total_compartments,
            'compartment_health_threshold': self.compartment_health_threshold,
            'carbon_leakage_rate': self.carbon_leakage_rate,
            'helium_leakage_rate': self.helium_leakage_rate,
            'trust_leakage_rate': self.trust_leakage_rate,
            'atp_c_ring_size': self.atp_c_ring_size,
            'atp_max_rotation_speed': self.atp_max_rotation_speed,
            'enable_multi_synthase': self.enable_multi_synthase,
            'enable_quantum_expert': self.enable_quantum_expert,
            'enable_helium_expert': self.enable_helium_expert,
            'enable_degradation_manager': self.enable_degradation_manager,
            'enable_predictive_homeostasis': self.enable_predictive_homeostasis,
            'enable_knowledge_transfer': self.enable_knowledge_transfer,
            'enable_supply_management': self.enable_supply_management,
            'enable_token_preallocation': self.enable_token_preallocation,
            'enable_chaos_engineering': self.enable_chaos_engineering,
            'enable_state_persistence': self.enable_state_persistence,
            'state_save_interval_seconds': self.state_save_interval_seconds,
            'state_directory': self.state_directory,
            'health_check_interval_seconds': self.health_check_interval_seconds,
            'version': self.version,
            'version_description': self.version_description
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'CoreConfig':
        """Create from dictionary"""
        valid_keys = {f.name for f in cls.__dataclass_fields__.values()}
        filtered = {k: v for k, v in data.items() if k in valid_keys}
        return cls(**filtered)
    
    @classmethod
    def from_file(cls, path: str) -> 'CoreConfig':
        """Load configuration from JSON file"""
        with open(path, 'r') as f:
            data = json.load(f)
        return cls.from_dict(data)
    
    def save_to_file(self, path: str):
        """Save configuration to JSON file"""
        with open(path, 'w') as f:
            json.dump(self.to_dict(), f, indent=2)

# ============================================================================
# Enhanced Bio-Inspired Core (with all new features integrated)
# ============================================================================

class EnhancedBioInspiredCore:
    """
    Enhanced Bio-Inspired Core v6.1.0
    
    New Features:
    - Dynamic module loading for runtime extensibility
    - Predictive health forecasting for proactive intervention
    - Configuration versioning for rollback capability
    - Anomaly detection in performance metrics
    - Event-driven communication between services
    - Genetic optimizer for core parameters
    - Decentralized decision-making via module-local events
    - Module marketplace for competition and replacement
    """
    
    def __init__(self, config: Optional[CoreConfig] = None, config_path: Optional[str] = None):
        # Load configuration
        if config_path:
            self.config = CoreConfig.from_file(config_path)
        else:
            self.config = config or CoreConfig()
        
        # Validate configuration
        is_valid, issues = self.config.validate()
        if not is_valid:
            logger.warning(f"Configuration issues: {issues}")
        
        # Module registry
        self.registry = ModuleRegistry()
        
        # Module references (populated during init)
        self._token_manager = None
        self._gradient_manager = None
        self._scheduler = None
        self._compartment_manager = None
        self._biomass_storage = None
        self._harvester = None
        self._supply_manager = None
        self._token_allocator = None
        self._knowledge_transfer = None
        self._degradation_manager = None
        self._api = None
        
        # NEW: Event bus
        self._event_bus = CoreEventBus()
        
        # NEW: Configuration version manager
        self._version_manager = ConfigurationVersionManager()
        self._save_initial_config()
        
        # NEW: Performance anomaly detector
        self._anomaly_detector = PerformanceAnomalyDetector()
        
        # NEW: Genetic optimizer
        self._genetic_optimizer = CoreGeneticOptimizer(self)
        
        # NEW: Module marketplace
        self._marketplace = ModuleMarketplace(self)
        
        # NEW: Decentralized modules registry
        self._decentralized_modules: Dict[str, DecentralizedModule] = {}
        
        # NEW: Evolvable parameters (used by genetic optimizer)
        self._module_retirement_threshold = 0.2
        self._predictive_health_retrain_interval = 300
        
        # Exchange rate
        self.exchange_rate = None
        
        # Lifecycle state
        self._lifecycle_phase = LifecyclePhase.UNREGISTERED
        self._start_time: Optional[datetime] = None
        self._shutdown_requested = False
        
        # Performance metrics
        self._perf_metrics: Dict[str, deque] = defaultdict(lambda: deque(maxlen=100))
        
        # Register signal handlers
        self._register_signal_handlers()
        
        logger.info("Enhanced Bio-Inspired Core v6.1.0 created")
    
    def _save_initial_config(self):
        """Save initial configuration version"""
        self._version_manager.save_version(
            self.config.to_dict(),
            description="Initial configuration"
        )
    
    # ========================================================================
    # Lifecycle Management (Enhanced)
    # ========================================================================
    
    async def initialize(self) -> bool:
        """Initialize all modules with health verification"""
        if self._lifecycle_phase == LifecyclePhase.RUNNING:
            logger.warning("Core already initialized")
            return True
        
        self._lifecycle_phase = LifecyclePhase.INITIALIZING
        self._start_time = datetime.utcnow()
        
        try:
            # Step 1: Validate configuration
            is_valid, issues = self.config.validate()
            if not is_valid:
                logger.error(f"Configuration invalid: {issues}")
                self._lifecycle_phase = LifecyclePhase.ERROR
                return False
            
            # Step 2: Initialize exchange rate
            from .eco_atp_currency import DynamicExchangeRate
            self.exchange_rate = DynamicExchangeRate()
            self.registry.register('exchange_rate', self.exchange_rate, 
                                  health_check=lambda: True)
            
            # Step 3: Initialize token manager
            from .eco_atp_currency import EcoATPTokenManager, TokenSupplyManager, PredictiveTokenAllocator
            self._token_manager = EcoATPTokenManager(self.exchange_rate)
            self.registry.register('token_manager', self._token_manager,
                                  dependencies=['exchange_rate'],
                                  health_check=lambda: self._token_manager.get_system_summary().get('total_balance', 0) > 0)
            
            # Step 4: Initialize gradient manager
            from .proton_gradient_fields import HierarchicalGradientManager
            self._gradient_manager = HierarchicalGradientManager()
            self.registry.register('gradient_manager', self._gradient_manager,
                                  health_check=lambda: len(self._gradient_manager.get_field_strengths()) > 0)
            
            # Step 5: Initialize ATP synthase scheduler
            from .atp_synthase_scheduler import ATPSynthaseScheduler, SynthaseConfig
            synthase_config = SynthaseConfig(
                protons_per_rotation=self.config.atp_c_ring_size,
                max_rotation_speed_rpm=self.config.atp_max_rotation_speed
            )
            self._scheduler = ATPSynthaseScheduler(
                self._token_manager, self._gradient_manager, synthase_config,
                enable_multi_synthase=self.config.enable_multi_synthase
            )
            self.registry.register('atp_synthase', self._scheduler,
                                  dependencies=['token_manager', 'gradient_manager'],
                                  health_check=lambda: self._scheduler.calculate_gradient_driving_force() >= 0)
            
            # Step 6: Initialize compartment manager
            from .chromatophore_compartments import HierarchicalCompartmentManager
            self._compartment_manager = HierarchicalCompartmentManager(
                self._token_manager,
                max_regions=10,
                compartments_per_region=20
            )
            self.registry.register('compartment_manager', self._compartment_manager,
                                  dependencies=['token_manager'],
                                  health_check=lambda: self._compartment_manager.get_ecosystem_stats().get('viable_compartments', 0) > 0)
            
            # Step 7: Initialize biomass storage
            from .biomass_storage import BiomassStorage
            self._biomass_storage = BiomassStorage(self._token_manager, self._gradient_manager)
            self.registry.register('biomass_storage', self._biomass_storage,
                                  dependencies=['token_manager', 'gradient_manager'],
                                  health_check=lambda: self._biomass_storage.get_storage_stats().get('total_stored', -1) >= 0)
            
            # Step 8: Initialize harvester
            from .photosynthetic_harvester import EnhancedPhotosyntheticHarvester
            self._harvester = EnhancedPhotosyntheticHarvester(
                self._token_manager, self._gradient_manager
            )
            self.registry.register('harvester', self._harvester,
                                  dependencies=['token_manager', 'gradient_manager'],
                                  health_check=lambda: self._harvester.get_harvesting_stats().get('total_harvested', -1) >= 0)
            
            # Wire harvester to scheduler
            if self._scheduler:
                self._scheduler.inject_harvester(self._harvester)
            
            # Step 9: Initialize supply management
            if self.config.enable_supply_management:
                self._supply_manager = TokenSupplyManager(self._token_manager)
                self.registry.register('supply_manager', self._supply_manager,
                                      dependencies=['token_manager'])
            
            # Step 10: Initialize token pre-allocation
            if self.config.enable_token_preallocation:
                self._token_allocator = PredictiveTokenAllocator(self._token_manager)
                self.registry.register('token_allocator', self._token_allocator,
                                      dependencies=['token_manager'])
            
            # Step 11: Initialize knowledge transfer
            if self.config.enable_knowledge_transfer:
                try:
                    from .knowledge_transfer import KnowledgeTransferManager
                    self._knowledge_transfer = KnowledgeTransferManager()
                    self.registry.register('knowledge_transfer', self._knowledge_transfer)
                except ImportError:
                    logger.warning("Knowledge transfer not available")
            
            # Step 12: Initialize degradation manager
            if self.config.enable_degradation_manager:
                try:
                    from .degradation_manager import DegradationManager
                    self._degradation_manager = DegradationManager(event_bus=self._event_bus)
                    self.registry.register('degradation_manager', self._degradation_manager)
                    
                    # Wire initial metrics
                    self._degradation_manager.update_metrics(
                        token_balance=self._token_manager.get_system_summary().get('total_balance', 500)
                    )
                except ImportError:
                    logger.warning("Degradation manager not available")
            
            # Step 13: Run health checks on all modules
            health_results = self.registry.health_check_all()
            unhealthy = [name for name, status in health_results.items() if status['status'] not in ('healthy', 'unknown')]
            
            if unhealthy:
                logger.warning(f"Some modules unhealthy after init: {unhealthy}")
            
            # Step 14: Start background monitoring
            asyncio.create_task(self._health_monitoring_loop())
            asyncio.create_task(self._performance_monitoring_loop())
            asyncio.create_task(self._predictive_health_loop())
            asyncio.create_task(self._anomaly_detection_loop())
            asyncio.create_task(self._competition_loop())
            asyncio.create_task(self._genetic_optimization_loop())
            
            self._lifecycle_phase = LifecyclePhase.RUNNING
            
            init_time = (datetime.utcnow() - self._start_time).total_seconds()
            logger.info(f"Bio-Inspired Core initialized successfully in {init_time:.1f}s")
            logger.info(f"Registered modules: {self.registry.list_modules()}")
            
            return True
            
        except Exception as e:
            self._lifecycle_phase = LifecyclePhase.ERROR
            logger.error(f"Initialization failed: {str(e)}", exc_info=True)
            return False
    
    async def shutdown(self) -> bool:
        """Graceful shutdown of all modules"""
        if self._lifecycle_phase == LifecyclePhase.STOPPED:
            return True
        
        self._lifecycle_phase = LifecyclePhase.STOPPING
        self._shutdown_requested = True
        logger.info("Initiating graceful shutdown...")
        
        # Save state if enabled
        if self.config.enable_state_persistence:
            self._save_state()
        
        # Shutdown event bus
        self._event_bus.shutdown()
        
        # Shutdown all modules in reverse order
        results = await self.registry.shutdown_all()
        
        all_ok = all(results.values())
        if all_ok:
            self._lifecycle_phase = LifecyclePhase.STOPPED
            logger.info("Graceful shutdown complete")
        else:
            failed = [name for name, ok in results.items() if not ok]
            logger.warning(f"Some modules failed to shutdown: {failed}")
        
        return all_ok
    
    def _save_state(self):
        """Save system state for recovery"""
        try:
            state_dir = self.config.state_directory
            os.makedirs(state_dir, exist_ok=True)
            
            state = {
                'timestamp': datetime.utcnow().isoformat(),
                'config': self.config.to_dict(),
                'token_summary': self._token_manager.get_system_summary() if self._token_manager else {},
                'gradient_strengths': self._gradient_manager.get_field_strengths() if self._gradient_manager else {},
                'compartment_stats': self._compartment_manager.get_ecosystem_stats() if self._compartment_manager else {},
                'biomass_stats': self._biomass_storage.get_storage_stats() if self._biomass_storage else {}
            }
            
            path = os.path.join(state_dir, f"state_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.json")
            with open(path, 'w') as f:
                json.dump(state, f, indent=2, default=str)
            
            logger.info(f"State saved to {path}")
            
        except Exception as e:
            logger.error(f"Failed to save state: {str(e)}")
    
    def _register_signal_handlers(self):
        """Register OS signal handlers for graceful shutdown"""
        try:
            loop = asyncio.get_event_loop()
            for sig in [signal.SIGINT, signal.SIGTERM]:
                loop.add_signal_handler(
                    sig,
                    lambda: asyncio.create_task(self.shutdown())
                )
            logger.info("Signal handlers registered")
        except NotImplementedError:
            logger.warning("Signal handlers not supported on this platform")
    
    # ========================================================================
    # Monitoring Loops (Enhanced)
    # ========================================================================
    
    async def _health_monitoring_loop(self):
        """Periodic health monitoring loop"""
        while self._lifecycle_phase == LifecyclePhase.RUNNING:
            try:
                health_results = self.registry.health_check_all()
                
                # Log unhealthy modules
                unhealthy = [
                    name for name, status in health_results.items()
                    if status['status'] not in ('healthy', 'unknown')
                ]
                
                if unhealthy:
                    logger.warning(f"Unhealthy modules: {unhealthy}")
                
                # Update degradation manager
                if self._degradation_manager and self._token_manager:
                    summary = self._token_manager.get_system_summary()
                    gradients = self._gradient_manager.get_field_strengths() if self._gradient_manager else {}
                    
                    self._degradation_manager.update_metrics(
                        token_balance=summary.get('total_balance', 500),
                        carbon_gradient=gradients.get('carbon', 0.5),
                        compartment_health=self._get_avg_compartment_health()
                    )
                
                await asyncio.sleep(self.config.health_check_interval_seconds)
                
            except Exception as e:
                logger.error(f"Health monitoring error: {str(e)}")
                await asyncio.sleep(60)
    
    async def _performance_monitoring_loop(self):
        """Periodic performance monitoring loop"""
        while self._lifecycle_phase == LifecyclePhase.RUNNING:
            try:
                # Record token metrics
                if self._token_manager:
                    summary = self._token_manager.get_system_summary()
                    self._perf_metrics['token_balance'].append(summary.get('total_balance', 0))
                    self._perf_metrics['token_efficiency'].append(summary.get('system_efficiency', 0))
                    self._anomaly_detector.record_metric('token_balance', summary.get('total_balance', 0))
                
                # Record gradient metrics
                if self._gradient_manager:
                    strengths = self._gradient_manager.get_field_strengths()
                    for field_id, strength in strengths.items():
                        self._perf_metrics[f'gradient_{field_id}'].append(strength)
                        self._anomaly_detector.record_metric(f'gradient_{field_id}', strength)
                
                # Record compartment metrics
                if self._compartment_manager:
                    stats = self._compartment_manager.get_ecosystem_stats()
                    self._perf_metrics['viable_compartments'].append(stats.get('viable_compartments', 0))
                    self._anomaly_detector.record_metric('viable_compartments', stats.get('viable_compartments', 0))
                
                await asyncio.sleep(60)
                
            except Exception as e:
                logger.error(f"Performance monitoring error: {str(e)}")
                await asyncio.sleep(60)
    
    async def _predictive_health_loop(self):
        """Predictive health forecasting loop"""
        while self._lifecycle_phase == LifecyclePhase.RUNNING:
            try:
                # Collect health data for all modules
                for name, entry in self.registry.modules.items():
                    metrics = {
                        'health_score': 0.5 if entry.health_status == 'unknown' else 0.8 if entry.health_status == 'healthy' else 0.3,
                        'success_rate': 1.0 - (entry.failure_count / max(1, entry.failure_count + 1)),
                        'token_balance': self._token_manager.get_system_summary().get('total_balance', 500) / 1000 if self._token_manager else 0.5,
                        'error_rate': 0.01
                    }
                    self.registry.update_predictive_health(name, metrics)
                
                # Train health forecaster
                await self.registry.health_forecaster.train()
                
                await asyncio.sleep(self._predictive_health_retrain_interval)
                
            except Exception as e:
                logger.error(f"Predictive health loop error: {str(e)}")
                await asyncio.sleep(60)
    
    async def _anomaly_detection_loop(self):
        """Anomaly detection loop for performance metrics"""
        while self._lifecycle_phase == LifecyclePhase.RUNNING:
            try:
                report = await self._anomaly_detector.get_anomaly_report()
                if report['anomalies']:
                    logger.warning(f"Performance anomalies detected: {report['anomalies']}")
                    
                    # Publish anomaly event
                    if self._event_bus:
                        for anomaly in report['anomalies']:
                            await self._event_bus.publish(CoreEvent(
                                event_type='performance_anomaly',
                                source='anomaly_detector',
                                payload=anomaly
                            ))
                
                await asyncio.sleep(60)
                
            except Exception as e:
                logger.error(f"Anomaly detection loop error: {str(e)}")
                await asyncio.sleep(120)
    
    async def _competition_loop(self):
        """Periodic module competition loop"""
        while self._lifecycle_phase == LifecyclePhase.RUNNING:
            try:
                await self._marketplace.run_competition()
                await asyncio.sleep(self._marketplace.competition_interval)
            except Exception as e:
                logger.error(f"Competition loop error: {str(e)}")
                await asyncio.sleep(300)
    
    async def _genetic_optimization_loop(self):
        """Periodic genetic optimization loop"""
        while self._lifecycle_phase == LifecyclePhase.RUNNING:
            try:
                if len(self.registry.modules) >= 5:
                    logger.info("Starting genetic optimization cycle...")
                    result = await self._genetic_optimizer.evolve(generations=10)
                    logger.info(f"Genetic optimization complete: best fitness {result['best_fitness']:.4f}")
                await asyncio.sleep(86400)  # every 24 hours
            except Exception as e:
                logger.error(f"Genetic optimization loop error: {str(e)}")
                await asyncio.sleep(3600)
    
    def _get_avg_compartment_health(self) -> float:
        """Get average compartment health"""
        if not self._compartment_manager:
            return 0.5
        compartments = self._compartment_manager.compartments
        if not compartments:
            return 0.5
        return np.mean([c.health_score for c in compartments.values()])
    
    # ========================================================================
    # Protocol-Compliant Service Accessors
    # ========================================================================
    
    @property
    def token_service(self) -> Optional[TokenServiceProtocol]:
        return self._token_manager
    
    @property
    def gradient_service(self) -> Optional[GradientServiceProtocol]:
        return self._gradient_manager
    
    @property
    def compartment_service(self) -> Optional[CompartmentServiceProtocol]:
        return self._compartment_manager
    
    @property
    def biomass_service(self) -> Optional[BiomassServiceProtocol]:
        return self._biomass_storage
    
    @property
    def event_bus(self) -> CoreEventBus:
        return self._event_bus
    
    @property
    def version_manager(self) -> ConfigurationVersionManager:
        return self._version_manager
    
    @property
    def anomaly_detector(self) -> PerformanceAnomalyDetector:
        return self._anomaly_detector
    
    # Legacy accessors
    @property
    def token_manager(self): return self._token_manager
    @property
    def gradient_manager(self): return self._gradient_manager
    @property
    def scheduler(self): return self._scheduler
    @property
    def compartment_manager(self): return self._compartment_manager
    @property
    def biomass_storage(self): return self._biomass_storage
    @property
    def harvester(self): return self._harvester
    @property
    def supply_manager(self): return self._supply_manager
    @property
    def token_allocator(self): return self._token_allocator
    @property
    def knowledge_transfer(self): return self._knowledge_transfer
    @property
    def degradation_manager(self): return self._degradation_manager
    
    # ========================================================================
    # Dynamic Module Loading
    # ========================================================================
    
    async def load_module(self, name: str, module_path: str) -> bool:
        """Dynamically load a module at runtime"""
        return await self.registry.load_module(name, module_path)
    
    def unload_module(self, name: str) -> bool:
        """Unload a dynamically loaded module"""
        return self.registry.unload_module(name)
    
    def get_loaded_modules(self) -> List[str]:
        """Get list of dynamically loaded modules"""
        return list(self.registry.loaded_modules)
    
    # ========================================================================
    # Configuration Versioning
    # ========================================================================
    
    def save_configuration_version(self, description: str = "") -> str:
        """Save current configuration as a new version"""
        return self._version_manager.save_version(
            self.config.to_dict(),
            description=description
        )
    
    def rollback_configuration(self, version_id: str) -> bool:
        """Rollback to a configuration version"""
        config_data = self._version_manager.rollback_to_version(version_id)
        if config_data:
            # Apply configuration
            for key, value in config_data.items():
                if hasattr(self.config, key):
                    setattr(self.config, key, value)
            logger.info(f"Configuration rolled back to version: {version_id}")
            return True
        return False
    
    def get_config_version_history(self, limit: int = 10) -> List[Dict]:
        """Get configuration version history"""
        return self._version_manager.get_version_history(limit)
    
    def get_config_version_diff(self, version_a: str, version_b: str) -> Dict[str, Any]:
        """Get diff between two configuration versions"""
        return self._version_manager.get_version_diff(version_a, version_b)
    
    # ========================================================================
    # Decentralized Module Registration (NEW)
    # ========================================================================
    
    def register_decentralized_module(self, name: str, module: DecentralizedModule):
        """Register a decentralized module that can make local decisions."""
        self._decentralized_modules[name] = module
        # Subscribe to events
        for event_type in module.event_subscriptions:
            self._event_bus.subscribe(event_type, module.on_event)
    
    async def trigger_local_decision(self, module_name: str, context: Dict[str, Any]) -> Dict[str, Any]:
        """Trigger a local decision for a decentralized module."""
        if module_name in self._decentralized_modules:
            return await self._decentralized_modules[module_name].local_decision(context)
        return {'action': 'noop', 'reason': 'Module not found'}
    
    # ========================================================================
    # Module Marketplace API (NEW)
    # ========================================================================
    
    def apply_module_replacement(self, old_name: str, new_name: str) -> bool:
        """Replace an underperforming module with another."""
        if old_name not in self.registry.modules or new_name not in self.registry.modules:
            return False
        # Transfer dependencies and health checks
        old_entry = self.registry.modules[old_name]
        new_entry = self.registry.modules[new_name]
        # Decommission old module
        asyncio.run(self.registry.shutdown_all())
        # Remove old, re-register new with same dependencies
        self.registry.modules[old_name] = new_entry
        # Update dependency graph
        for dep in old_entry.dependencies:
            if dep in self.registry.modules:
                self.registry.modules[dep].dependents.remove(old_name)
                self.registry.modules[dep].dependents.append(new_name)
        logger.info(f"Module replacement: {old_name} → {new_name}")
        return True
    
    def get_marketplace_status(self) -> Dict:
        return self._marketplace.get_marketplace_stats()
    
    def get_genetic_status(self) -> Dict:
        return self._genetic_optimizer.get_status()
    
    # ========================================================================
    # System Status and Reporting (Enhanced)
    # ========================================================================
    
    def get_system_status(self) -> Dict[str, Any]:
        """Get comprehensive system status"""
        status = {
            'lifecycle_phase': self._lifecycle_phase.value,
            'uptime_seconds': (datetime.utcnow() - self._start_time).total_seconds() if self._start_time else 0,
            'timestamp': datetime.utcnow().isoformat(),
            'config': self.config.to_dict(),
            'modules': self.registry.get_registry_stats(),
            'event_bus': self._event_bus.get_event_stats(),
            'config_version': self._version_manager.current_version,
            'loaded_modules': self.get_loaded_modules(),
            'genetic_optimizer': self._genetic_optimizer.get_status(),
            'marketplace': self._marketplace.get_marketplace_stats(),
            'module_retirement_threshold': self._module_retirement_threshold,
            'predictive_health_retrain_interval': self._predictive_health_retrain_interval
        }
        
        # Module-specific status
        if self._token_manager:
            status['token_economy'] = self._token_manager.get_system_summary()
        
        if self._gradient_manager:
            status['gradients'] = self._gradient_manager.get_field_stats()
            status['gradient_forecasts'] = self._gradient_manager.get_forecast_summary()
        
        if self._compartment_manager:
            status['compartments'] = self._compartment_manager.get_ecosystem_stats()
        
        if self._biomass_storage:
            status['biomass'] = self._biomass_storage.get_storage_stats()
        
        if self._harvester:
            status['harvester'] = self._harvester.get_harvesting_stats()
        
        if self._scheduler:
            status['atp_synthase'] = self._scheduler.get_scheduler_stats()
        
        if self._supply_manager:
            status['token_economy']['supply_management'] = self._supply_manager.get_economic_indicators()
        
        if self._token_allocator:
            status['token_economy']['pre_allocation'] = self._token_allocator.get_cache_stats()
        
        if self._degradation_manager:
            status['degradation'] = self._degradation_manager.get_tier_status()
        
        # Performance metrics
        status['performance'] = {
            name: {
                'current': list(values)[-1] if values else None,
                'avg_1min': np.mean(list(values)[-60:]) if len(values) >= 10 else None,
                'trend': 'stable'
            }
            for name, values in self._perf_metrics.items()
        }
        
        # Anomaly report
        status['anomalies'] = asyncio.run(self._anomaly_detector.get_anomaly_report())
        
        return status
    
    def get_health_dashboard(self) -> Dict[str, Any]:
        """Get health dashboard for all modules"""
        health = self.registry.health_check_all()
        
        # Calculate overall health
        healthy_count = sum(1 for s in health.values() if s['status'] == 'healthy')
        total = len(health)
        
        return {
            'overall_health': 'healthy' if healthy_count == total else 'degraded' if healthy_count > total // 2 else 'unhealthy',
            'healthy_modules': healthy_count,
            'total_modules': total,
            'modules': health,
            'circuit_breakers': {
                name: entry.circuit_breaker_state
                for name, entry in self.registry.modules.items()
            },
            'predictive_health': {
                name: {
                    'predicted_health': entry.predicted_health,
                    'failure_probability': entry.failure_probability,
                    'trend': entry.health_trend
                }
                for name, entry in self.registry.modules.items()
            },
            'dependency_graph': self.registry.get_dependency_graph(),
            'timestamp': datetime.utcnow().isoformat()
        }
    
    def get_economic_report(self) -> Dict[str, Any]:
        """Get economic health report"""
        report = {
            'timestamp': datetime.utcnow().isoformat()
        }
        
        if self._token_manager:
            report['token_economy'] = self._token_manager.get_system_summary()
        
        if self._supply_manager:
            report['supply_management'] = self._supply_manager.get_economic_indicators()
        
        if self._token_allocator:
            report['pre_allocation'] = self._token_allocator.get_cache_stats()
        
        # Health assessment
        indicators = report.get('supply_management', {})
        utilization = indicators.get('utilization', 0.5)
        inflation = indicators.get('inflation_pressure', 0)
        
        if 0.6 < utilization < 0.9 and abs(inflation) < 0.2:
            report['health'] = 'healthy'
        elif utilization < 0.4:
            report['health'] = 'deflationary'
        elif utilization > 0.95:
            report['health'] = 'inflationary'
        else:
            report['health'] = 'stable'
        
        recs = []
        if utilization < 0.4:
            recs.append("Economy under-utilized. Increase task throughput.")
        if utilization > 0.95:
            recs.append("Economy over-heating. Add capacity or reduce load.")
        if inflation > 0.3:
            recs.append("High inflation pressure. Token burning recommended.")
        report['recommendations'] = recs if recs else ["Economy is healthy."]
        
        return report
    
    def get_performance_report(self) -> Dict[str, Any]:
        """Get performance metrics report with anomaly detection"""
        report = {'timestamp': datetime.utcnow().isoformat()}
        
        for name, values in self._perf_metrics.items():
            if values:
                arr = np.array(list(values))
                report[name] = {
                    'current': float(arr[-1]) if len(arr) > 0 else None,
                    'mean': float(np.mean(arr[-60:])) if len(arr) >= 10 else None,
                    'min': float(np.min(arr[-60:])) if len(arr) >= 10 else None,
                    'max': float(np.max(arr[-60:])) if len(arr) >= 10 else None,
                    'std': float(np.std(arr[-60:])) if len(arr) >= 10 else None,
                    'trend': 'improving' if len(arr) >= 10 and arr[-1] > np.mean(arr[-10:-5]) else 'stable'
                }
        
        # Add anomaly report
        report['anomalies'] = asyncio.run(self._anomaly_detector.get_anomaly_report())
        
        return report
    
    # ========================================================================
    # Configuration Management (Enhanced)
    # ========================================================================
    
    def update_configuration(self, updates: Dict[str, Any], description: str = "") -> Tuple[bool, str]:
        """Update configuration with validation and versioning"""
        # Create temporary config with updates
        temp_config = CoreConfig.from_dict({**self.config.to_dict(), **updates})
        
        # Validate
        is_valid, issues = temp_config.validate()
        if not is_valid:
            return False, f"Invalid configuration: {'; '.join(issues)}"
        
        # Apply updates
        for key, value in updates.items():
            if hasattr(self.config, key):
                setattr(self.config, key, value)
        
        # Save version
        self.save_configuration_version(description or f"Updated: {', '.join(updates.keys())}")
        
        logger.info(f"Configuration updated: {list(updates.keys())}")
        return True, "Configuration updated successfully"
    
    def reload_configuration(self, path: str) -> Tuple[bool, str]:
        """Reload configuration from file with versioning"""
        try:
            new_config = CoreConfig.from_file(path)
            is_valid, issues = new_config.validate()
            
            if not is_valid:
                return False, f"Invalid configuration: {'; '.join(issues)}"
            
            # Save current version before reload
            self.save_configuration_version(f"Before reload from {path}")
            
            self.config = new_config
            
            # Save new version
            self.save_configuration_version(f"Reloaded from {path}")
            
            logger.info(f"Configuration reloaded from {path}")
            return True, "Configuration reloaded successfully"
            
        except Exception as e:
            return False, f"Failed to reload configuration: {str(e)}"
    
    # ========================================================================
    # Task Processing (Enhanced)
    # ========================================================================
    
    def process_task(self, task: Dict[str, Any]) -> Dict[str, Any]:
        """Process a task through the bio-inspired system with event publishing"""
        if self._lifecycle_phase != LifecyclePhase.RUNNING:
            return {'success': False, 'reason': f'System not running (phase: {self._lifecycle_phase.value})'}
        
        # If task specifies a decentralized module, let it decide locally
        if 'module' in task and task['module'] in self._decentralized_modules:
            asyncio.create_task(self._decentralized_modules[task['module']].local_decision(task))
        
        ecoatp_required = task.get('complexity', 0.5) * 10
        
        # Publish task received event
        asyncio.create_task(self._event_bus.publish(CoreEvent(
            event_type='task_received',
            source='core',
            payload={'task_id': task.get('task_id'), 'complexity': task.get('complexity', 0.5)}
        )))
        
        # Try token pre-allocation first
        if self._token_allocator:
            success, _ = self._token_allocator.get_tokens('task_processor', ecoatp_required)
            if success:
                self._token_allocator.record_demand('task_processor', ecoatp_required)
        elif self._token_manager:
            success, _ = self._token_manager.reserve_tokens(
                'task_processor', ecoatp_required, None
            )
        else:
            success = True
        
        if not success:
            # Store in biomass
            if self._biomass_storage:
                stored, token_id = self._biomass_storage.store_task(
                    task_data=task, ecoatp_cost=ecoatp_required
                )
                asyncio.create_task(self._event_bus.publish(CoreEvent(
                    event_type='task_stored',
                    source='core',
                    payload={'task_id': task.get('task_id'), 'biomass_token': token_id}
                )))
                return {'success': True, 'status': 'stored', 'biomass_token': token_id}
            
            return {'success': False, 'reason': 'Insufficient tokens'}
        
        asyncio.create_task(self._event_bus.publish(CoreEvent(
            event_type='task_processed',
            source='core',
            payload={'task_id': task.get('task_id'), 'ecoatp_cost': ecoatp_required}
        )))
        
        return {'success': True, 'task_id': task.get('task_id', 'unknown'), 'ecoatp_cost': ecoatp_required}
    
    # ========================================================================
    # Lifecycle Status
    # ========================================================================
    
    @property
    def is_running(self) -> bool:
        return self._lifecycle_phase == LifecyclePhase.RUNNING
    
    @property
    def is_healthy(self) -> bool:
        health = self.registry.health_check_all()
        return all(s['status'] != 'error' for s in health.values())
    
    @property
    def lifecycle_phase(self) -> LifecyclePhase:
        return self._lifecycle_phase
    
    def get_lifecycle_status(self) -> Dict[str, Any]:
        """Get lifecycle status"""
        return {
            'phase': self._lifecycle_phase.value,
            'is_running': self.is_running,
            'is_healthy': self.is_healthy,
            'uptime_seconds': (datetime.utcnow() - self._start_time).total_seconds() if self._start_time else 0,
            'start_time': self._start_time.isoformat() if self._start_time else None,
            'shutdown_requested': self._shutdown_requested,
            'module_count': len(self.registry.modules),
            'loaded_modules_count': len(self.registry.loaded_modules),
            'config_version': self._version_manager.current_version
        }

# ============================================================================
# Convenience Functions
# ============================================================================

def create_core(config: Optional[CoreConfig] = None, config_path: Optional[str] = None) -> EnhancedBioInspiredCore:
    """Create an enhanced bio-inspired core"""
    return EnhancedBioInspiredCore(config=config, config_path=config_path)

async def create_and_initialize(config: Optional[CoreConfig] = None) -> EnhancedBioInspiredCore:
    """Create and initialize a bio-inspired core"""
    core = EnhancedBioInspiredCore(config=config)
    success = await core.initialize()
    
    if not success:
        raise RuntimeError("Failed to initialize Bio-Inspired Core")
    
    return core
